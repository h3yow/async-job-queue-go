# 本地持久化异步任务队列（Go）

用**本地 JSON 文件存储模拟消息代理**的异步任务队列，提供可预期的投递、重试、死信与重启恢复语义。仅依赖 Go 标准库。

## 目录结构

| 路径 | 说明 |
| --- | --- |
| `queue/task.go` | 任务模型、状态机、失败分类（可重试 / 不可恢复）、进度字段 |
| `queue/store.go` | `Store` 接口与 `FileStore`：临时文件 + fsync + rename 原子落盘 |
| `queue/queue.go` | 队列核心：入队、投递、租约、Ack/Nack、**进度上报**、退避、死信、重启恢复、优雅退回 |
| `queue/progress.go` | 异步进度上报器 `ProgressReporter`：非阻塞、节流合并落盘，经 handler context 取用 |
| `queue/consumer.go` | 多 worker 并发消费者、优雅关闭与**关闭截止时间（强制退回）** |
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

### 4. 优雅关闭与关闭截止时间

`Consumer.Run` 在 `ctx` 取消后：

1. worker 停止拉取新任务，在途 handler 的 context 被取消；
2. 等待在途 handler 退出，**从取消时刻起**最多等待 `ShutdownGrace`（`WithShutdownGrace` 设置，默认 10s，传 `0` 表示不设上限、一直等）；
3. 在截止时间内退出的，按 handler 结果正常 Ack/Nack；全部退出后调用 `RequeueOwned` 把本实例仍持有的未完成租约**立即退回 `pending`**（退避为 0），重启后或其他实例无需等待租约超时即可接管；
4. **到点仍有 handler 不理会取消、一直跑**：不再等待，立即调用 `RequeueOwned` 强制释放其租约（任务不丢、重新变成可投递），`Run` 返回可由 `errors.Is(err, queue.ErrShutdownDeadlineExceeded)` 判定的错误。被遗弃的 handler 之后无论返回成功/失败，其 Ack/Nack 与进度上报都因租约令牌已清空而被拒绝（`ErrInvalidLease`），**无法造成重复投递，也无法覆盖新一轮的进度**；
5. 关闭前已 Ack 的任务是 `succeeded` 终态，强制退回只触碰本实例仍持有的 running 租约，**已成功任务不会被再投递一次**。

注意：超时强制释放只保证“任务不丢、重新可投递、关闭有界”，并不能强行杀死用户的 goroutine——不响应取消的 handler 仍可能继续占用 CPU/IO 直到自己结束（其任何后续操作都会被令牌校验挡下）。

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

## 长任务进度上报与查询

面向批量导出、分段处理这类一次跑几分钟以上的任务，每条任务的持久化记录新增三个字段：

| 字段 | 含义 |
| --- | --- |
| `ProgressPercent` | 整体百分比，取值 `[0,100]` |
| `ProgressStage` | 当前阶段的文字说明（由 handler 自由填写） |
| `ProgressReportedAt` | 最近一次**被接受并落盘**的上报时间 |

### 上报方式

handler 从 context 取出**本次投递专属**的上报器，调用 `Report` 即可：

```go
consumer := queue.NewConsumer(q, 4, func(ctx context.Context, t *queue.Task) error {
    reporter := queue.ProgressFromContext(ctx)
    for i, shard := range shards {
        if err := process(ctx, shard); err != nil {
            return queue.Retryable(err)
        }
        reporter.Report(100*(i+1)/len(shards), fmt.Sprintf("exported shard %d/%d", i+1, len(shards)))
    }
    return nil
},
    queue.WithProgressFlushInterval(200*time.Millisecond), // 默认 200ms
)
```

- `Report(percent, stage)` **永远非阻塞**：只做一次内存加锁写入，后台单个 goroutine 按最小落盘间隔（默认 200ms）把**窗口内的多次上报合并为最新一条**原子落盘。慢磁盘、高频上报都不会把任务处理拖到不可用；
- handler 返回后、Ack/Nack 之前，消费者会同步把最后一条未落盘进度 flush 一次，因此成功/失败终态前最后一次上报不会丢；
- 不经过 Consumer、直接使用 `Queue` 时可调用同步方法 `Queue.ReportProgress(ctx, taskID, token, percent, stage)`：成功返回即已落盘，语义与上面的落盘路径完全一致。

### 语义与边界（重启、租约变化、终态）

1. **归属校验（令牌）**：进度与 Ack/Nack 一样必须携带当前租约令牌。任务因租约超时被重新投递、崩溃恢复重投或关闭退回后，**上一轮仍在跑的旧执行再上报会得到 `ErrInvalidLease` 并被拒绝**——即使它报的百分比更高，也不能覆盖新一轮的进度。
2. **单调只增（防倒退）**：只接受 `percent >= 当前已落盘值` 的上报；更低的值返回 `ErrProgressRegressed`，任务状态不变。因此**进程重启或崩溃恢复后读到的是最后一次成功上报的进度，不会变回 0，也不会倒退**；重新投递后的新一轮应从 `task.ProgressPercent` 接着报。`percent` 相同的上报允许通过（用于只刷新阶段文字/上报时间）；`percent` 越界（`<0` 或 `>100`）返回 `ErrInvalidProgress`。
3. **不随状态流转清零**：退避重试、租约超时重投、崩溃恢复、关闭退回都**保留**进度字段；进入 succeeded/dead 终态后进度同样保留，可用于排查。
4. **查询一致性**：`Queue.Get(id)` / `Queue.Snapshot()` 返回的进度就是已落盘快照的内容——未被接受的上报（令牌失效、倒退、越界）不会留在内存态里，因此查询结果与数据文件一致，重启后再查仍是同一条。
5. **节流中的崩溃窗口**：两次落盘间隔（默认 200ms）内、尚未落盘的最后几条上报在进程被 `kill -9` 时会丢；恢复后读到的是上一次成功落盘的值（不旧于它、不会倒退）。需要“返回即持久”的关键节点可直接用 `Queue.ReportProgress` 同步上报。

### 旧数据文件兼容

旧版本写出的数据文件没有 `progress_percent` / `progress_stage` / `progress_reported_at` 字段。`FileStore` 直接按 JSON 反序列化：

- 缺失字段自动取零值（`0%` / 空阶段 / 零时间），**加载不报错、无需迁移**，队列正常启动并可立即投递、上报；
- 兼容范围是“旧文件 → 新版本”：旧文件中既有的状态、租约、退避、死信原因等字段含义不变；新版本写出的文件含新字段，旧版本程序无法识别这些未知字段（Go JSON 默认忽略未知字段，读取本身不报错，但旧程序重新保存会丢掉进度）。

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
| `TestProgressPersistsAcrossRestart` | 进度多次上报落盘、倒退被拒、崩溃重启后保留最后进度、死信与二次重启后仍可查 |
| `TestStaleProgressFromOldAttemptRejected` | 租约超时重投后旧执行迟到上报被拒，无法覆盖新一轮进度；非法百分比被拒 |
| `TestProgressReporterIsNonBlockingUnderSlowStore` | 慢盘下 1000 次高频上报不阻塞 handler，合并落盘、close 刷最后一条 |
| `TestLoadsLegacyDataFileWithoutProgressFields` | 旧版本数据文件（无 progress 字段）直接加载，零值起步并可正常上报 |
| `TestShutdownDeadlineForceRequeuesInflight` | handler 不理会取消时关闭在截止时间返回、在途任务强制退回可重投，已 Ack 任务不重投，旧执行迟到 ack/进度被拒 |
| `TestShutdownWithinGraceReturnsNil` | 配合取消的 handler 在截止时间内退出时正常优雅关闭 |
| `TestFileStoreRoundTrip` / `TestFileStoreNoLeftoverTempFiles` | 持久化往返与原子替换 |

单测日志（`-v` 可见）统一打印 **任务标识、投递次数（attempt N/M）与判定依据（basis）**，例如：

```
[queue-log] worker=0 deliver task=task-xxxx attempt=2/5 basis="pending-visible-or-lease-expired"
[queue-log] worker=0 stale-ack task=task-xxxx attempt=1 verdict=rejected basis="lease token mismatch (visibility timeout)"
[assert] task=task-xxxx attempts=5 verdict=dead reason="retry budget exhausted ..." basis="retryable failure and attempts == max -> dead letter"
[queue-log] progress task=task-xxxx attempt=1 percent=99 verdict=rejected err="invalid or expired lease: task=... status=running"
[assert] task=task-xxxx after-restart progress=80% stage="upload" verdict=retained basis="progress fields are part of the persisted snapshot and never reset on recovery"
[assert] task=task-xxxx attempt=1 after-deadline verdict=pending visible=now progress=35% basis="force-requeued owned unfinished lease; progress snapshot untouched"
```

### 命令行演示

```bash
# 生产几条任务（payload 语义：ok: 成功；fail:<n>: 前 n 次失败后成功；fatal: 立即死信；
#                long:<秒数>: 长任务，执行中持续上报进度；其他一直失败）
go run ./cmd/demo -data /tmp/ajq-demo/q.json -run=-1s -enqueue 'ok:first'
go run ./cmd/demo -data /tmp/ajq-demo/q.json -run=-1s -enqueue 'fail:2:retry-me'
go run ./cmd/demo -data /tmp/ajq-demo/q.json -run=-1s -enqueue 'fatal:bad'
go run ./cmd/demo -data /tmp/ajq-demo/q.json -run=-1s -enqueue 'long:30:batch-export'
go run ./cmd/demo -data /tmp/ajq-demo/q.json -run=-1s -enqueue 'always-boom'

# 消费（短时长模拟一次进程生命周期；-shutdown-grace 设置关闭截止时间，Ctrl-C 触发优雅关闭）
go run ./cmd/demo -data /tmp/ajq-demo/q.json -workers 3 -run 3s -shutdown-grace 5s

# 再次运行即"进程重启"：succeeded/dead 保持，退避任务继续重试直至死信；
# long: 任务从上次持久化的百分比继续（进度不倒退）
go run ./cmd/demo -data /tmp/ajq-demo/q.json -run 3s
```

手工验证新能力的建议路径：

```bash
# 1) 进度在多次运行（重启）间保留、并从上次值续跑
go run ./cmd/demo -data /tmp/ajq-demo/p.json -run=-1s -enqueue 'long:60:export'
go run ./cmd/demo -data /tmp/ajq-demo/p.json -run 2s   # 跑一小段，结束时 final 行带 progress=N%
go run ./cmd/demo -data /tmp/ajq-demo/p.json -run 2s   # 日志里 progress 从上轮的 N% 继续，不回 0

# 2) 关闭截止时间：长任务运行中 Ctrl-C，超过 -shutdown-grace 未退出时日志出现
#    "shutdown grace=... exceeded, force-requeued=1"，任务保持 pending 可再投
go run ./cmd/demo -data /tmp/ajq-demo/p.json -run 0s -shutdown-grace 2s

# 3) 旧数据兼容：保留/拷贝一份升级前的 queue.json，直接用新版本打开，不报错、任务正常投递
```

## 使用示例

```go
store := queue.NewFileStore("data/queue.json")
q, _ := queue.New(queue.DefaultConfig(), store, "instance-1") // 启动即恢复

task, _ := q.Enqueue(ctx, payload)

consumer := queue.NewConsumer(q, 4, func(ctx context.Context, t *queue.Task) error {
    reporter := queue.ProgressFromContext(ctx) // 长任务：非阻塞进度上报
    for i, shard := range shards {
        if err := doWork(ctx, shard); err != nil {
            if permanent(err) {
                return queue.NonRetryable(err) // 直接死信
            }
            return queue.Retryable(err)       // 退避重试；t.ProgressPercent 是上次落盘进度
        }
        reporter.Report(100*(i+1)/len(shards), fmt.Sprintf("shard %d/%d", i+1, len(shards)))
    }
    return nil
},
    queue.WithShutdownGrace(30*time.Second),       // 关闭截止时间：到点强制退回在途任务
    queue.WithProgressFlushInterval(200*time.Millisecond),
)
err := consumer.Run(ctx) // ctx 取消后优雅关闭，未完成任务自动退回可投递
errors.Is(err, queue.ErrShutdownDeadlineExceeded) // 有 handler 超时未退出、已强制退回
```
