# 本地持久化异步任务队列（async-job-queue-go）

用**本地文件存储模拟消息代理（broker）**的异步任务队列，纯 Go 标准库实现、零外部依赖。
目标是让任务在**并发消费、失败重试、进程重启**三类场景下的投递与状态语义可预期。

- 状态机与进度落盘（`pending → leased → succeeded | dead`），写盘采用「临时文件 + fsync + 原子 rename」，崩溃不会留下半写文件。
- 投递语义：**至少一次投递（at-least-once delivery）+ 终态唯一**。
- 可见性窗口（租约/lease）：未在窗口内确认的投递会被重新投递；迟到的 ack 一律失效。
- 重试与死信：区分「可重试失败」与「不可恢复失败」，重试预算耗尽进入死信并保留可追溯原因；死信不会再被普通消费者取出。
- 多消费者并发：同一任务在可见性窗口内不会被两个消费者同时成功处理。
- 优雅关闭：取消进行中的消费，未完成任务立即释放回 `pending`，重启后可继续投递。

---

## 1. 快速开始

```go
store, err := jobqueue.OpenStore("./data/queue.json") // 不存在会自动创建
if err != nil { log.Fatal(err) }

q := jobqueue.New(store, jobqueue.Config{
    Workers:     4,                // 并发消费者数
    LeaseTTL:    30 * time.Second, // 可见性窗口
    MaxAttempts: 3,                // 最大投递次数（1 = 不重试）
    Backoff: jobqueue.ExponentialBackoff{
        BaseDelay: 100 * time.Millisecond,
        MaxDelay:  5 * time.Second,
    },
    Handler: func(ctx context.Context, d *jobqueue.Delivery) error {
        if err := d.UpdateProgress(50); err != nil { // 进度持久化
            return err
        }
        switch {
        case badPayload(d.Job):
            return jobqueue.TerminalError("missing field user_id") // 不可恢复：立即死信
        case downstreamFlaky():
            return jobqueue.RetryError("http 500")                  // 可重试：退避后再投递
        default:
            return nil                                              // nil = ack 成功
        }
    },
})

if err := q.Enqueue("job-1", map[string]any{"url": "https://example.com"}); err != nil {
    log.Fatal(err)
}
q.Start()
defer q.Shutdown(context.Background())
```

运行本地演示（入队 / 重试 / 死信 / 重启恢复）：

```bash
go run ./cmd/demo            # 首次运行：消费并落盘
go run ./cmd/demo -restart   # 重新打开同一文件：恢复并展示一致终态
```

---

## 2. 任务状态机

```
                 Enqueue
                   │
                   ▼
              ┌─────────┐  claim 成功（获得 lease）  ┌────────┐
              │ pending │ ───────────────────────▶ │ leased │
              └─────────┘                            └────────┘
                   ▲                                  │  │  │
   重试退避到期 /   │                                  │  │  └─ handler 返回 TerminalError
   租约超时回收 /   │  Retry（可重试失败）             │  │     或 Attempts==MaxAttempts 仍失败
   关闭释放         └──────────────────────────────────┘  │     或租约超时耗尽
                                                          ▼
                                              成功 ack → succeeded（终态，持久）
                                                         dead（死信，终态，不再投递）
```

| 状态 | 含义 | 是否可被 Claim |
| --- | --- | --- |
| `pending` | 已入队、等待投递（可能带退避到期时间 `AvailableAt`） | 是（到期后） |
| `leased` | 已投递给某消费者，处于可见性窗口内，持有唯一 `LeaseID` | 否（窗口过期后可被重新投递） |
| `succeeded` | 已显式确认完成，落盘持久 | 否 |
| `dead` | 死信，保留 `DeadReason` | **永否**，只能由运维显式 `RequeueDead` |

每个 `Job` 持久化字段：`id, payload, status, attempts, max_attempts, progress,
available_at, lease_id, lease_owner, lease_expires_at, last_error, dead_reason,
created_at, updated_at`。

---

## 3. 投递语义：至少一次 + 终态唯一

- **Claim 是原子的**：存储层用互斥锁保护，`pending` 到期任务或**已过期的 lease** 才会被选中；
  同一时刻只有一个消费者拿到某任务的新 `LeaseID`。
- **Ack 必须携带 lease token**：`Complete(jobID, leaseID)` 会校验租约。窗口过期后任务被别人拿走，
  旧持有者迟到的 ack 会得到 `ErrLeaseLost` 并被**忽略**——不会把已被重新投递的任务误标成功。
- 因此：网络/崩溃/卡顿可能导致**重复投递**（at-least-once），但一个任务的**成功终态只会落一次**。
  消费逻辑需按 job id 自行做幂等。
- **租约超时重投**：`LeaseExpiresAt < now` 的 `leased` 任务可被任意消费者重新 Claim，
  `Attempts` 加 1，并记录 `basis=lease-expired decision=redeliver`。
- **保护阀（防止静默无限重投）**：若消费者每次都在 ack/nack 前崩溃，任务只能靠租约超时重投；
  当重投次数也超过 `MaxAttempts` 时，队列依据 `basis=lease-timeout-exhausted` 将其放入死信，
  并记录「lease expired without ack N times」。

---

## 4. 失败分类、重试与退避

Handler 返回值即判定依据：

| Handler 返回 | 判定 (`basis`) | 队列动作 |
| --- | --- | --- |
| `nil` | `handler-success` | ack → `succeeded`（progress=100） |
| `jobqueue.RetryError(reason)` 或任意普通 `error` | `retryable-error` | `Attempts < MaxAttempts`：按退避回到 `pending`；否则 → 死信 |
| `jobqueue.TerminalError(reason)` | `terminal-error` | **当次**即 → 死信，不消耗重试预算 |

- **退避规则**：指数退避 `delay = min(MaxDelay, BaseDelay * 2^(attempt-1))`
  （attempt 从 1 开始；`ExponentialBackoff{BaseDelay:100ms, MaxDelay:5s}` → 100ms, 200ms, 400ms, 800ms, 1.6s, 3.2s, 5s, 5s …）。
  退避期间任务保持 `pending` 但 `AvailableAt` 未到，Claim 会跳过。
- **重试预算耗尽**：`attempt >= max_attempts` 的可重试失败 → `dead`，
  `DeadReason = "retries exhausted after N/M deliveries: <原始原因>"`。
- 所有失败原因都会同时写入 `last_error`，死信另存 `dead_reason`，重启后仍可追溯。

---

## 5. 死信处理（DLQ）

- `succeeded` 与 `dead` 都是持久终态；普通 `Claim` 永不返回死信任务（测试中以直接 Claim 探测验证）。
- 死信原因可通过 `queue.Get(id).DeadReason` / `queue.All()` 离线排查。
- **唯一离开死信的途径是显式运维操作** `queue.RequeueDead(id)`：重置为 `pending`、清零投递计数、
  保留历史原因到 `last_error`（形如 `requeued after dead-letter; <旧原因>`）。
  典型流程：修复 bug / 数据后，重启新进程，再调用重放。

---

## 6. 持久化与进程重启恢复

- 每次状态变更（入队、claim、ack、retry、dead、release、进度）都**同步重写快照并 fsync**，
  通过同目录临时文件 + `rename` 原子发布，保证磁盘上要么是旧版本、要么是新版本，不会撕裂。
- **打开即恢复**（`OpenStore`）：磁盘上残留的任何 `leased` 记录都意味着旧进程已死亡
  （租约只在单进程内存中有效），统一重置为可立即投递的 `pending`，并在 `last_error`
  追加 `process restart: stale lease recovered`。已 `succeeded` / `dead` 的任务原样保留。
- 崩溃时的投递计入 `Attempts`（崩溃不等于白跑），因此一个一直崩溃的任务最终也会触发保护阀进入死信。
- **优雅关闭** `Shutdown(ctx)`：
  1. 取消消费者 context，worker 停止拉新；
  2. 尊重取消的在途 handler 返回错误时，任务**立即**（无退避）释放回 `pending`；
  3. 超过 `ctx` 截止仍未退出的在途任务，强制释放其 lease，保证重启后可继续；
  4. 关闭存储。重启后 `OpenStore` 的恢复逻辑同样兜底。

---

## 7. 可观测性：判定日志

队列对每次投递/判定打印一行结构化日志，**包含任务标识、投递次数（当前/上限）与判定依据**：

```
job=job-x attempts=1/3 basis=claim decision=deliver owner=consumer-1
job=job-x attempts=1/3 basis=retryable-error decision=retry backoff=2ms reason="http 500"
job=job-x attempts=2/3 basis=lease-expired decision=redeliver owner=consumer-2
job=job-x attempts=2/3 basis=ack decision=ignored reason=lease-lost (...)
job=job-x attempts=3/3 basis=retry-budget-exhausted decision=dead-letter reason="retries exhausted ..."
job=job-x attempts=1/1 basis=terminal-error decision=dead-letter reason="terminal failure: ..."
job=job-x attempts=3/2 basis=lease-timeout-exhausted decision=dead-letter reason="..."
job=job-x basis=shutdown decision=release-to-pending
```

`Config.Logger` 可注入自定义 `*log.Logger`（测试中输出到 `t.Log`/stdout）。

---

## 8. 本地验证方法

```bash
# 全部单元测试（建议带竞态检测）
go test ./... -race -count=1 -v

# 仅看场景
go test ./... -race -v -run 'TestRetryExhaustedEntersDeadLetter'
go test ./... -race -v -run 'TestLeaseExpiryRedeliveryAndStaleAckIgnored'
go test ./... -race -v -run 'TestConcurrentConsumersExclusiveDelivery'
go test ./... -race -v -run 'TestCrashRecoveryRedeliversUnfinished'
go test ./... -race -v -run 'TestGracefulShutdownReleasesInflight'

# 端到端演示
go run ./cmd/demo && go run ./cmd/demo -restart

# 直接检查落盘内容
cat ./.demo/queue.json | jq .
```

测试覆盖的场景：

| 测试 | 覆盖语义 |
| --- | --- |
| `TestAckPersistsAcrossRestart` | ack 与进度持久化；重启后不丢、不重投 |
| `TestRetryBackoffThenSucceed` | 可重试失败退避后成功，投递次数准确 |
| `TestRetryExhaustedEntersDeadLetter` | 重试预算耗尽 → 死信；原因可追溯；此后不再被消费 |
| `TestTerminalFailureDeadLettersImmediately` | 不可恢复失败首次即死信，不重试 |
| `TestLeaseExpiryRedeliveryAndStaleAckIgnored` | 租约超时重投；迟到 ack 被忽略；成功终态唯一 |
| `TestConcurrentConsumersExclusiveDelivery` | 4 消费者 × 20 任务：并行处理且每任务恰好成功一次（`-race`） |
| `TestCrashRecoveryRedeliversUnfinished` | 不优雅退出后重开文件：在途任务恢复 pending 并被处理，终态保持 |
| `TestGracefulShutdownReleasesInflight` | 优雅关闭释放在途任务，新进程立即可投递并完成 |
| `TestLeaseTimeoutExhaustionDeadLetters` | 崩溃循环保护阀：租约超时次数超限也进死信 |
| `TestRequeueDeadIsExplicitOnly` | 死信不会自动复活；运维显式重放后可被修复后的消费者完成 |
| `TestExponentialBackoff` | 退避曲线与封顶 |

---

## 9. 设计边界与注意事项

- 本地文件存储面向**单进程**使用（嵌入式 broker 模拟）；多进程并发写同一文件不在支持范围内。
- 投递保证是**至少一次**：消费者必须对副作用做幂等。
- 每次变更全量重写 JSON 快照，适合中小规模任务演示与验证；生产级吞吐可在 `Store` 接口后替换为 WAL/SQLite。
- 时钟使用 UTC 墙钟；`Config.Now` 可注入自定义时钟便于测试。
