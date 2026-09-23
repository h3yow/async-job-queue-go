# 本地持久化异步任务队列（Go）

用**本地 JSON 文件存储模拟消息代理**的异步任务队列，提供可预期的投递、重试、死信与重启恢复语义。仅依赖 Go 标准库。

## 目录结构

| 路径 | 说明 |
| --- | --- |
| `queue/task.go` | 任务模型、状态机、失败分类（可重试 / 不可恢复） |
| `queue/store.go` | `Store` 接口与 `FileStore`：临时文件 + fsync + rename 原子落盘 |
| `queue/queue.go` | 队列核心：入队、投递、租约、Ack/Nack、退避、死信、重启恢复、优雅退回 |
| `queue/consumer.go` | 多 worker 并发消费者与优雅关闭 |
| `cmd/demo` | 本地手工验证用的命令行演示 |

## 任务状态机

```
                 Enqueue
                   │
                   ▼
              ┌─────────┐  Nack(retryable, 额度未满)   ┌─────────┐
              │ pending │ ◄────────────────────────── │ running │
              └─────────┘        +backoff VisibleAt   └─────────┘
                   │                     ▲   │  │
   VisibleAt 到期   │        租约超时重投  │   │  │ Ack（令牌匹配）
                   └─────────────────────┘   │  ├──────────────► ┌───────────┐
                                             │  │ Nack(不可恢复   │ succeeded │ 终态
                                             │  │  或重试额度耗尽) └───────────┘
                                             │  ▼
                                             │ ┌──────┐
                                             └─┤ dead │ 终态（死信，不再投递）
                                               └──────┘
```

- `pending`：等待投递。新任务、退避中的任务、被优雅关闭退回的任务都处于该状态；`VisibleAt` 未到的任务不会被取出。
- `running`：已投递给某个消费者实例，处于租约（可见性窗口）保护期，记录 `LeaseOwner`、`LeaseToken`、`LeaseExpires`。
- `succeeded` / `dead`：终态，持久化后进程重启仍然保持，**永远不会再被投递**。

## 投递语义

### 1. 持久化时机（at-least-once 投递 + 不丢结论）

队列在单进程内以互斥锁串行化所有状态变更，**每次变更（Enqueue / Dequeue / Ack / Nack / 退回）都把全量快照原子写入数据文件**（写临时文件 → `fsync` → `rename` 替换 → `fsync` 目录）：

- `Dequeue` 成功返回前，"任务已投递、投递次数 +1" 已落盘；
- `Ack` 成功返回后，`succeeded` 结论不会因崩溃或重启丢失；
- 写入过程中崩溃只会留下/保留旧文件，不会出现半截数据。

因此整体语义是 **at-least-once（至少一次）投递 + 终态结论 exactly-once 生效**：任务可能被投递多次（租约超时、崩溃恢复），但"成功"只会被确认一次。

### 2. 租约 / 可见性窗口

- 每次投递生成新的随机 `LeaseToken`，任务在 `LeaseExpires` 之前不会被再次投递；
- `Ack/Nack` 必须携带当前令牌。**租约超时后任务被重新投递、令牌轮换**，旧持有者迟到的 Ack/Nack 返回 `ErrInvalidLease` 并被拒绝，不会覆盖新租约的处理结论；
- 这保证同一任务在可见性窗口内不会被两个消费者同时成功处理。

### 3. 进程重启恢复

`queue.New` 启动时加载数据文件：

- 上次进程遗留的 `running` 任务（崩溃、被 `kill -9`，未走优雅关闭）一律重置为 `pending`、清空租约、立即可重新投递——不存在静默丢弃；
- `succeeded` / `dead` 原样保留（含死信原因）；
- `Attempts`（已投递次数）**只增不减、跨重启保留**，恢复重投继续累计，用于判定重试额度。

### 4. 优雅关闭

`Consumer.Run` 在 `ctx` 取消后：

1. worker 停止拉取新任务，在途 handler 的 context 被取消；
2. 等待所有 worker 退出（关闭前已处理成功的任务仍会正常 Ack 落盘）；
3. 调用 `RequeueOwned` 把本实例仍持有的未完成租约**立即退回 `pending`**（退避为 0），重启后或其他实例无需等待租约超时即可接管。

## 失败分类、重试与退避

Handler 通过返回值声明失败类型：

| 返回值 | 行为 |
| --- | --- |
| `nil` | 成功，Ack → `succeeded` |
| `queue.Retryable(err)` | 可重试：未达 `MaxAttempts` 时退回 `pending`，按指数退避延后投递；额度耗尽 → 死信 |
| `queue.NonRetryable(err)` | 不可恢复：**首次即进入死信**，不消耗重试预算 |
| 其他 `error` | 按可重试失败处理 |

退避公式（第 `attempt` 次失败，attempt 从 1 起）：

```
delay = min(BaseBackoff * BackoffMultiplier^(attempt-1), MaxBackoff)
Jitter=true 时再乘 [0.5, 1.5) 的随机系数（防重试雷鸣群）
```

默认配置（`DefaultConfig`）：`MaxAttempts=5`、租约 30s、初始退避 1s、倍率 2、上限 30s、无抖动。

## 死信处理

- 进入条件：不可恢复失败，或可重试失败在第 `MaxAttempts` 次投递后仍失败；
- 死信任务保留可追溯的 `DeadReason`，形如：
  - `non-retryable failure on attempt 1/5: <错误信息>`
  - `retry budget exhausted after 5/5 attempts, last error: <错误信息>`
- 死信是终态，`Dequeue` 的候选只包含到期的 `pending` 和超时的 `running`，**死信永远不会被消费者再次取出**；只能通过 `Queue.Snapshot()` / `Queue.Get()` 查看与排障。

## 并发模型

- 一个 `Queue` 实例可挂多个 worker（`NewConsumer(q, workers, h)`），内部互斥锁保证投递、确认的原子性；
- 每个数据文件在任一时刻应由一个进程打开读写；多进程/多机消费超出本地存储模拟范围（跨进程接管通过"崩溃后用同一数据文件重开"验证）。

## 本地验证

### 运行测试

```bash
go test ./... -race -count=1 -v
```

覆盖场景：

| 测试 | 场景 |
| --- | --- |
| `TestEnqueueDequeueAck` | 基本投递、重复 Ack 被拒、终态不再投递 |
| `TestRetryExhaustionEntersDeadLetter` | 可重试失败退避、重试耗尽进死信且保留原因、死信不再投递 |
| `TestNonRetryableGoesStraightToDead` | 不可恢复失败首次即死信 |
| `TestLeaseExpiryRedelivery` | 可见性窗口内不重复投递、租约超时重投、旧令牌 Ack 被拒 |
| `TestBackoffHidesTaskUntilVisible` | 退避到期前不可见 |
| `TestRestartRecoversUnfinishedRunningTask` | 崩溃遗留 running 任务重启后重投，不静默丢弃 |
| `TestRestartPreservesTerminalStates` | 重启后 succeeded/dead 及死信原因保持 |
| `TestGracefulShutdownRequeuesOwnedLeases` | 优雅关闭退回未完成任务，新实例立即接管 |
| `TestGracefulShutdownKeepsAckedTask` | 已 Ack 任务关闭时不退回 |
| `TestConcurrentConsumersProcessEachTaskOnce` | 8 worker × 50 任务，每任务恰好成功一次 |
| `TestConsumerRetriesThenSucceeds` | 消费侧重试退避后成功，投递次数准确 |
| `TestLeaseTakeoverAcrossInstances` | 慢 handler 租约超时被重投，迟到 Ack 被拒，结论唯一 |
| `TestConsumerShutdownRequeuesInflight` | 在途任务随优雅关闭退回 pending |
| `TestFileStoreRoundTrip` / `TestFileStoreNoLeftoverTempFiles` | 持久化往返与原子替换 |

单测日志（`-v` 可见）统一打印 **任务标识、投递次数（attempt N/M）与判定依据（basis）**，例如：

```
[queue-log] worker=0 deliver task=task-xxxx attempt=2/5 basis="pending-visible-or-lease-expired"
[queue-log] worker=0 stale-ack task=task-xxxx attempt=1 verdict=rejected basis="lease token mismatch (visibility timeout)"
[assert] task=task-xxxx attempts=5 verdict=dead reason="retry budget exhausted ..." basis="retryable failure and attempts == max -> dead letter"
```

### 命令行演示

```bash
# 生产几条任务（payload 语义：ok: 成功；fail:<n>: 前 n 次失败后成功；fatal: 立即死信；其他一直失败）
go run ./cmd/demo -data /tmp/ajq-demo/q.json -run=-1s -enqueue 'ok:first'
go run ./cmd/demo -data /tmp/ajq-demo/q.json -run=-1s -enqueue 'fail:2:retry-me'
go run ./cmd/demo -data /tmp/ajq-demo/q.json -run=-1s -enqueue 'fatal:bad'
go run ./cmd/demo -data /tmp/ajq-demo/q.json -run=-1s -enqueue 'always-boom'

# 消费（短时长模拟一次进程生命周期，Ctrl-C 触发优雅关闭）
go run ./cmd/demo -data /tmp/ajq-demo/q.json -workers 3 -run 3s

# 再次运行即"进程重启"：succeeded/dead 保持，退避任务继续重试直至死信
go run ./cmd/demo -data /tmp/ajq-demo/q.json -run 3s
```

## 使用示例

```go
store := queue.NewFileStore("data/queue.json")
q, _ := queue.New(queue.DefaultConfig(), store, "instance-1") // 启动即恢复

task, _ := q.Enqueue(ctx, payload)

consumer := queue.NewConsumer(q, 4, func(ctx context.Context, t *queue.Task) error {
    if err := doWork(ctx, t.Payload); err != nil {
        if permanent(err) {
            return queue.NonRetryable(err) // 直接死信
        }
        return queue.Retryable(err)       // 退避重试
    }
    return nil
})
consumer.Run(ctx) // ctx 取消后优雅关闭，未完成任务自动退回可投递
```
