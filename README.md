# 本地持久化异步任务队列（Go）

用**本地 JSON 文件存储模拟消息代理**的异步任务队列，提供可预期的投递、重试、死信与重启恢复语义。仅依赖 Go 标准库。

## 目录结构

| 路径 | 说明 |
| --- | --- |
| `queue/task.go` | 任务模型、状态机、失败分类（可重试 / 不可恢复）、进度模型 |
| `queue/store.go` | `Store` 接口与 `FileStore`：临时文件 + fsync + rename 原子落盘 |
| `queue/queue.go` | 队列核心：入队、投递、租约、Ack/Nack、退避、死信、重启恢复、优雅退回、进度上报与查询 |
| `queue/consumer.go` | 多 worker 并发消费者、进度上报器注入、带截止时间的优雅关闭 |
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

### 4. 优雅关闭（等待上限只从关闭时刻起算）

**核心约定：`shutdownTimeout` 是"关闭等待上限"，不是"运行寿命上限"。** 它只在 `Consumer.Run` 的 `ctx` 被取消（真正收到关闭信号）那一刻才开始计时，与消费者此前已经运行了多久**完全无关**。因此：

- **正常运行期间它从不触发**：消费者是常驻进程，单个任务跑几分钟、跑多久都不会自我终止，也不会提前放掉手上的在途租约；
- **收到关闭后重新获得一份完整的等待上限**：不会因为进程之前已经跑了很久而几乎没有等待余地。

`Consumer.Run` 在 `ctx` 取消后：

1. worker 立即停止拉取新任务；**在途 handler 不会被顺手掐断**——其处理上下文不随父 ctx 取消，而是保留一份从关闭时刻才开始计的、完整的宽限期；
2. 等待所有 worker 退出，**最长等待 `shutdownTimeout`**（默认 30s，`WithShutdownTimeout` 可调；`<=0` 表示无限等待），该截止时间从关闭那一刻起算。在宽限期内跑完、返回 nil 的任务仍会正常 Ack 落盘、判成功，**不会被提前掐断重投**；
3. 宽限到期后调用 `RequeueOwned` 把本实例仍持有的未完成租约**立即退回 `pending`**（退避为 0），重启后或其他实例无需等待租约超时即可接管。

**截止时间到期仍有 handler 不退出**（不理会取消、一直跑）时，Run 不再无限阻塞：

- 先取消在途 handler 的处理上下文（响应取消的 handler 可据此收尾），随后强制退回这些在途租约并直接返回，任务回到 `pending` **重新可投递、不会丢**；
- 强制退回会轮换租约令牌，失控 handler 之后迟到的 Ack/Nack/进度上报都会被拒绝（`ErrInvalidLease`），不会覆盖新一轮投递的结论与进度；
- 已经 Ack 成功的任务是终态，`RequeueOwned` 不会触碰，**不会因超时释放被再次投递**；
- 注意这是 at-least-once 语义的固有取舍：被强制退回的任务可能在新一轮投递中与失控的旧 handler 短暂并发执行，handler 应具备幂等性。

> 关闭前后两种上下文是分开的：拉取循环使用随关闭立即取消的上下文（停止拉新），单次投递的 handler 使用在宽限期内仍然有效的上下文（让在途任务跑完）。这也是"长任务正常运行不受影响、关闭后还能完整等一等"的实现基础。

## 进度上报与查询（长耗时任务）

### 上报语义

长任务 handler 通过消费者注入的进度上报器上报进度（整体百分比 + 当前阶段说明）：

```go
handler := func(ctx context.Context, t *queue.Task) error {
    pr, ok := queue.ProgressReporterFromContext(ctx)
    for i, seg := range segments {
        if err := process(ctx, seg); err != nil { return queue.Retryable(err) }
        if ok {
            if err := pr.Report((i+1)*100/len(segments), fmt.Sprintf("第 %d/%d 段", i+1, len(segments))); err != nil {
                return nil // 租约已易主（ErrInvalidLease），旧执行应放弃
            }
        }
    }
    return nil
}
```

也可以绕过消费者直接调用 `Queue.ReportProgress(ctx, taskID, leaseToken, percent, message)`。语义：

- **仅当前租约持有者可上报**：任务处于 `running` 且令牌匹配才接受；租约超时被重新投递后令牌轮换，**旧执行迟到的上报返回 `ErrInvalidLease`，不会覆盖新一轮租约的进度**；终态（succeeded/dead）后任何上报同样被拒绝；
- **成功即持久**：每次成功上报随全量快照原子落盘（与其他状态变更同一机制），`percent` 钳制到 `[0,100]`；
- **重启不回退**：进程重启/崩溃恢复后读到的是最后一次成功上报的进度与上报时间，不会变回零值、也不会倒退；
- **频率建议**：上报是同步落盘的（一次小文件原子重写，毫秒级），与队列其他状态变更串行化的时间很短，不会阻塞其他 worker 的处理逻辑本身；但建议按阶段或固定节奏（如每几百毫秒）上报，不要每处理一条记录上报一次。

### 查询语义

```go
p, ok := q.GetProgress(taskID) // 或 q.Get(taskID) 后读 Task.Progress
```

- 任何状态（处理中、succeeded、dead）都可查询；任务进入终态后**最后上报的进度原样保留**，用于事后排查；
- 读到的是内存中已落盘的最新值，与数据文件一致，**重启后再查还是同一条**；
- `Progress.Reported()` 可区分"从未上报"（零值）与"上报过 0%"。

### 旧数据文件兼容

旧版本写出的数据文件没有 `progress` 字段，新版本**可以直接打开**：缺失字段解码为零值（视为"从未上报"），其余字段与恢复语义（running 重置、终态保留、Attempts 累计）完全不变；旧记录被重新投递后即可正常上报进度。反向不保证：新版写出的文件含 `progress` 字段，旧版本代码读取时会忽略未知字段，可正常加载但看不到进度。


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
| `TestProgressPersistsAcrossRestart` | 进度上报落盘，重启后保留最后上报值、不倒退 |
| `TestStaleLeaseProgressReportRejected` | 租约轮换后旧执行的迟到进度上报被拒，不覆盖新进度 |
| `TestProgressRetainedAfterTerminalStates` | succeeded/dead 终态后进度仍可查，重启后一致 |
| `TestOldDataFileWithoutProgressLoads` | 无 progress 字段的旧数据文件直接加载，功能不受影响 |
| `TestShutdownTimeoutForceRequeuesInflight` | 关闭截止到期强制释放在途租约，任务重新可投递、已 Ack 任务不受影响 |
| `TestShutdownTimeoutStartsOnlyAfterShutdown` | 关闭等待上限只从关闭起算：正常运行远超上限不自我终止/不放在途任务；关闭后宽限内跑完判成功、到点不退出才释放重投，旧执行迟到上报/确认被拒 |
| `TestProgressReportingDoesNotBlockProcessing` | 长任务频繁上报进度时其他任务正常完成 |
| `TestFileStoreRoundTrip` / `TestFileStoreNoLeftoverTempFiles` | 持久化往返与原子替换 |

单测日志（`-v` 可见）统一打印 **任务标识、投递次数（attempt N/M）与判定依据（basis）**，例如：

```
[queue-log] worker=0 deliver task=task-xxxx attempt=2/5 basis="pending-visible-or-lease-expired"
[queue-log] worker=0 stale-ack task=task-xxxx attempt=1 verdict=rejected basis="lease token mismatch (visibility timeout)"
[assert] task=task-xxxx attempts=5 verdict=dead reason="retry budget exhausted ..." basis="retryable failure and attempts == max -> dead letter"
```

### 命令行演示

```bash
# 生产几条任务（payload 语义：ok: 成功；slow:<秒>: 长任务并上报进度；
# fail:<n>: 前 n 次失败后成功；fatal: 立即死信；其他一直失败）
go run ./cmd/demo -data /tmp/ajq-demo/q.json -run=-1s -enqueue 'ok:first'
go run ./cmd/demo -data /tmp/ajq-demo/q.json -run=-1s -enqueue 'slow:30:export'
go run ./cmd/demo -data /tmp/ajq-demo/q.json -run=-1s -enqueue 'fail:2:retry-me'
go run ./cmd/demo -data /tmp/ajq-demo/q.json -run=-1s -enqueue 'fatal:bad'

# 消费（短时长模拟一次进程生命周期，Ctrl-C 触发优雅关闭；
# -shutdown-timeout 控制等待在途 handler 的截止时间，到期强制退回租约）
go run ./cmd/demo -data /tmp/ajq-demo/q.json -workers 3 -run 3s -shutdown-timeout 1s

# 再次运行即"进程重启"：succeeded/dead 保持，长任务进度保留在最后上报值并继续推进
go run ./cmd/demo -data /tmp/ajq-demo/q.json -run 3s
```

### 本地复现"正常运行不受关闭上限影响"与"关闭后完整等一等"

**自动化复现（推荐，稳定可重复）**：一条用例同时覆盖两种情况——先不关闭地正常运行一段明显超过关闭等待上限的时间（测试里上限 150ms、正常运行 750ms），再触发关闭观察宽限语义：

```bash
go test ./queue/ -run TestShutdownTimeoutStartsOnlyAfterShutdown -race -count=1 -v
```

`-v` 日志可依次看到任务标识、第几次投递与判定依据：

- 正常运行 750ms（≈5 倍上限）后仍输出 `verdict=alive basis="shutdownTimeout timer is created only after ctx cancellation"`，三条在途任务保持 `running attempts=1`（`verdict=no-early-release`）；
- 长任务在未关闭时放行后 `verdict=succeeded ... attempt=1`（跑多久都不重投）；
- 关闭后宽限内（50ms < 150ms）跑完的任务 `verdict=succeeded basis="completed within grace period whose deadline starts at shutdown"`；
- 到点（150ms）仍不退出的失控任务 `verdict=redeliverable`，重新投递为 `attempt=2`，其旧执行的迟到上报/确认 `verdict=rejected`。

**命令行手动复现**（用 Ctrl-C 作为关闭信号，`-run 0` 表示收到信号前一直运行）：

```bash
# 一条要跑 30s 的长任务，关闭等待上限仅 2s
go run ./cmd/demo -data /tmp/ajq-demo/grace.json -run=-1s -enqueue 'slow:30:export'
# 常驻消费：不按 Ctrl-C，就让它跑远超 2s —— 观察进度持续推进、任务保持 running，
# 消费者绝不会因为"运行超过 2s"而自己退出。
go run ./cmd/demo -data /tmp/ajq-demo/grace.json -run 0 -shutdown-timeout 2s
# 跑过若干秒后按 Ctrl-C：此时才开始 2s 宽限。
#   - 若任务在 2s 内完成 -> 日志 ack ... verdict=succeeded；
#   - 若 2s 到点仍在跑 -> deadline exceeded, force-releasing，任务退回 pending。
# 再启动一次即可看到被退回的任务立即重新投递（attempt 递增）：
go run ./cmd/demo -data /tmp/ajq-demo/grace.json -run 0 -shutdown-timeout 2s
```

## 使用示例

```go
store := queue.NewFileStore("data/queue.json")
q, _ := queue.New(queue.DefaultConfig(), store, "instance-1") // 启动即恢复

task, _ := q.Enqueue(ctx, payload)

consumer := queue.NewConsumer(q, 4, func(ctx context.Context, t *queue.Task) error {
    pr, hasProgress := queue.ProgressReporterFromContext(ctx) // 长任务进度上报
    if err := doWork(ctx, t.Payload, func(pct int, stage string) {
        if hasProgress {
            _ = pr.Report(pct, stage) // 成功即落盘；ErrInvalidLease 表示租约已易主
        }
    }); err != nil {
        if permanent(err) {
            return queue.NonRetryable(err) // 直接死信
        }
        return queue.Retryable(err)       // 退避重试
    }
    return nil
}, queue.WithShutdownTimeout(10*time.Second)) // 关闭后等待在途 handler 的截止时间（只从关闭时刻起算，正常运行不触发）
consumer.Run(ctx) // 常驻消费直到 ctx 取消；取消后给在途任务完整宽限，到期未完成才退回可投递

// 随时查询进度（含终态任务的最后进度），重启后结果一致
progress, _ := q.GetProgress(task.ID)
```
