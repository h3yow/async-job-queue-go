package queue

import (
	"context"
	"errors"
	"log"
	"os"
	"sync"
	"time"
)

// Logger 队列运行日志接口（测试可替换为 testing.T）。
type Logger interface {
	Printf(format string, args ...any)
}

// Handler 处理单条任务：
//   - 返回 nil：任务成功，消费者调用 Ack；
//   - 返回 *TaskError（Retryable/NonRetryable）：按声明语义 Nack；
//   - 返回其他 error：按可重试失败处理。
//
// 长耗时任务可在处理过程中通过 ProgressReporterFromContext 取出
// 进度上报器并调用 Report 上报进度（见 Queue.ReportProgress 的语义）。
type Handler func(ctx context.Context, task *Task) error

// ProgressReporter 绑定一次具体投递（任务 + 租约令牌）的进度上报器。
// 租约超时被重新投递后，旧 reporter 的上报会被队列拒绝（ErrInvalidLease），
// 不会覆盖新一轮租约的进度。
type ProgressReporter struct {
	queue  *Queue
	taskID string
	token  string
}

// Report 上报整体百分比与当前阶段说明；成功返回即已持久化。
func (r *ProgressReporter) Report(percent int, message string) error {
	return r.queue.ReportProgress(context.Background(), r.taskID, r.token, percent, message)
}

type progressReporterKey struct{}

// ProgressReporterFromContext 取出消费者注入 handler 上下文的进度上报器；
// 在消费者之外（如直接调用 handler 的测试）返回 (nil, false)。
func ProgressReporterFromContext(ctx context.Context) (*ProgressReporter, bool) {
	r, ok := ctx.Value(progressReporterKey{}).(*ProgressReporter)
	return r, ok
}

// Consumer 从队列拉取任务并以固定数量 worker 并发处理。
type Consumer struct {
	queue           *Queue
	handler         Handler
	workers         int
	instance        string
	pollInterval    time.Duration
	shutdownTimeout time.Duration
	logger          Logger
}

// ConsumerOption 配置可选项。
type ConsumerOption func(*Consumer)

// WithPollInterval 设置空闲轮询间隔，默认 100ms。
func WithPollInterval(d time.Duration) ConsumerOption {
	return func(c *Consumer) { c.pollInterval = d }
}

// WithShutdownTimeout 设置优雅关闭时等待在途 handler 退出的截止时间。
// 该截止时间只从 Run 的 ctx 被取消（真正收到关闭）那一刻开始计时，
// 与消费者此前已经运行了多久无关；正常消费期间它完全不起作用。
// 到期仍未退出的 handler 不再被等待：其租约被强制退回 pending，
// 任务重新可投递（不丢任务）；handler 之后迟到的 Ack/Nack 会因令牌
// 已轮换而被拒绝。默认 30s；<=0 表示无限等待（不推荐）。
func WithShutdownTimeout(d time.Duration) ConsumerOption {
	return func(c *Consumer) { c.shutdownTimeout = d }
}

// WithLogger 替换日志输出。
func WithLogger(l Logger) ConsumerOption {
	return func(c *Consumer) {
		if l != nil {
			c.logger = l
		}
	}
}

// NewConsumer 创建消费者；workers 为并发处理数。
func NewConsumer(q *Queue, workers int, h Handler, opts ...ConsumerOption) *Consumer {
	if workers <= 0 {
		workers = 1
	}
	c := &Consumer{
		queue:           q,
		handler:         h,
		workers:         workers,
		instance:        q.InstanceID(),
		pollInterval:    100 * time.Millisecond,
		shutdownTimeout: 30 * time.Second,
		logger:          log.New(os.Stderr, "[consumer] ", log.LstdFlags|log.Lmsgprefix),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Run 启动 worker 循环，直到 ctx 被取消才进入关闭流程：
//
// 正常消费阶段（ctx 未取消）没有任何时长上限：消费者是常驻进程，
// 单个任务跑多久都不会触发自我终止，也不会放掉手上的在途租约。
//
// ctx 取消（真正收到关闭）之后：
//  1. worker 立即停止拉取新任务；
//  2. 在途 handler 不被提前掐断，而是拿到一份从关闭时刻才开始计的、
//     完整的 shutdownTimeout 宽限时间继续运行。在此期间返回 nil 的
//     handler 仍会正常 Ack 落盘，任务判成功，不会被重投；
//  3. 所有 worker 在截止时间内退出则正常结束；截止到期仍不退出的
//     handler（不理会取消）不再被等待，其上下文被取消、仍持有的租约
//     一次性退回 pending，重启后/其他消费者处立即可重新投递（不丢任务）。
//
// 截止时间只从关闭那一刻起算，与进程此前已运行多久无关。
// 已 Ack 成功的任务是终态，不会被退回；失控 handler 之后迟到的
// Ack/Nack/进度上报会因租约令牌已轮换而被拒绝。
func (c *Consumer) Run(ctx context.Context) error {
	// 在途 handler 的上下文不继承父 ctx：关闭只表示停止拉新，
	// 不应顺手掐断手上的任务；它的截止时间从关闭时刻才开始。
	handlerCtx, cancelHandlers := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelHandlers()

	// worker 立即启动并正常消费；pollCtx 即父 ctx，未关闭前不设任何时长上限。
	var wg sync.WaitGroup
	for i := 0; i < c.workers; i++ {
		wg.Add(1)
		go c.worker(ctx, handlerCtx, i, &wg)
	}

	workersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(workersDone)
	}()

	// 正常消费阶段没有任何时限：消费者是常驻进程，跑多久都不会自我终止。
	// 一直阻塞到真正收到关闭（ctx 取消）为止。
	<-ctx.Done()
	c.logger.Printf("instance=%s shutdown signaled, stop pulling new tasks; inflight handlers keep a full grace period of %s",
		c.instance, c.shutdownTimeoutLabel())

	timedOut := false
	if c.shutdownTimeout > 0 {
		// 关键：计时器在收到关闭之后才创建，消费者此前运行多久都不消耗宽限。
		timer := time.NewTimer(c.shutdownTimeout)
		defer timer.Stop()
		select {
		case <-workersDone:
			c.logger.Printf("instance=%s all workers exited within shutdown deadline %s (measured from shutdown)",
				c.instance, c.shutdownTimeout)
		case <-timer.C:
			timedOut = true
			// 截止到点仍有 handler 不退出：取消其上下文（响应取消的 handler 可据此收尾），
			// 但不再等待它们，Run 有界地返回。
			cancelHandlers()
			c.logger.Printf("instance=%s shutdown deadline %s exceeded (measured from shutdown), "+
				"force-releasing inflight leases without waiting for runaway handler(s)",
				c.instance, c.shutdownTimeout)
		}
	} else {
		<-workersDone
	}

	// 用独立上下文执行退回，避免已取消的 ctx 阻止落盘。
	requeued, err := c.queue.RequeueOwned(context.Background())
	if err != nil {
		c.logger.Printf("instance=%s shutdown requeue failed: %v", c.instance, err)
		return err
	}
	basis := "all inflight handlers exited within grace period"
	if timedOut {
		basis = "runaway handler(s) ignored grace-period cancellation; leases force-released"
	}
	c.logger.Printf("instance=%s graceful shutdown complete, requeued=%d unfinished task(s) basis=%q",
		c.instance, requeued, basis)
	return nil
}

// shutdownTimeoutLabel 仅用于日志展示关闭等待上限。
func (c *Consumer) shutdownTimeoutLabel() string {
	if c.shutdownTimeout > 0 {
		return c.shutdownTimeout.String()
	}
	return "unlimited"
}

// worker 循环拉取并处理任务。
//   - pollCtx 取消（收到关闭）后立即停止拉取新任务并退出循环；
//   - 已在途的一次投递使用 handlerCtx：它在关闭后的完整宽限期内不会被
//     取消，handler 只要在宽限内跑完就能正常给出终局判定。
func (c *Consumer) worker(pollCtx, handlerCtx context.Context, id int, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-pollCtx.Done():
			return
		default:
		}

		lease, err := c.queue.Dequeue(pollCtx)
		if err != nil {
			if pollCtx.Err() != nil {
				return
			}
			c.logger.Printf("worker=%d dequeue error: %v", id, err)
			if !sleep(pollCtx, c.pollInterval) {
				return
			}
			continue
		}
		if lease == nil {
			if !sleep(pollCtx, c.pollInterval) {
				return
			}
			continue
		}
		c.handle(handlerCtx, pollCtx, id, lease)
	}
}

// handle 处理一次投递并给出终局判定；所有判定都会打印任务标识、投递次数与依据。
//
// handlerCtx 是本次投递的处理上下文（关闭后仍享有完整宽限期）；
// pollCtx 仅用于判断是否已经收到关闭：收到关闭后 handler 才失败返回时，
// 不再走带退避的 Nack（退避没有意义），交给 RequeueOwned 立即退回可投递。
func (c *Consumer) handle(handlerCtx, pollCtx context.Context, workerID int, lease *Lease) {
	task := lease.Task
	c.logger.Printf("worker=%d deliver task=%s attempt=%d/%d basis=%q",
		workerID, task.ID, task.Attempts, task.MaxAttempts,
		"pending-visible-or-lease-expired")

	err := c.handler(withProgressReporter(handlerCtx, c.queue, task.ID, lease.Token), task.Clone())

	switch {
	case err == nil:
		// 即便正在关闭，已处理成功的结果也要确认落盘，避免重复投递。
		if ackErr := c.queue.Ack(context.Background(), task.ID, lease.Token); ackErr != nil {
			if errors.Is(ackErr, ErrInvalidLease) {
				// 租约已超时并被重新投递：本次确认作废，任务以最新租约为准。
				c.logger.Printf("worker=%d stale-ack task=%s attempt=%d verdict=rejected basis=%q",
					workerID, task.ID, task.Attempts, "lease token mismatch (visibility timeout)")
				return
			}
			c.logger.Printf("worker=%d ack-error task=%s attempt=%d err=%v",
				workerID, task.ID, task.Attempts, ackErr)
			return
		}
		c.logger.Printf("worker=%d ack task=%s attempt=%d verdict=succeeded basis=%q",
			workerID, task.ID, task.Attempts, "handler returned nil and lease token matched")

	default:
		// 已收到关闭：不再 Nack（退避没有意义），交给 RequeueOwned 立即退回。
		// 注意这里看的是 pollCtx（关闭信号）而非 handlerCtx：关闭后的宽限期内
		// handlerCtx 仍未取消，handler 在宽限内跑完返回 nil 仍会正常 Ack 成功。
		if pollCtx.Err() != nil {
			c.logger.Printf("worker=%d abandon task=%s attempt=%d verdict=requeued-on-shutdown basis=%q",
				workerID, task.ID, task.Attempts, "context canceled before nack")
			return
		}
		te, ok := err.(*TaskError)
		if !ok {
			te = Retryable(err)
		}
		if nackErr := c.queue.Nack(context.Background(), task.ID, lease.Token, te); nackErr != nil {
			if errors.Is(nackErr, ErrInvalidLease) {
				c.logger.Printf("worker=%d stale-nack task=%s attempt=%d verdict=rejected basis=%q",
					workerID, task.ID, task.Attempts, "lease token mismatch (visibility timeout)")
				return
			}
			c.logger.Printf("worker=%d nack-error task=%s attempt=%d err=%v",
				workerID, task.ID, task.Attempts, nackErr)
			return
		}
		c.logger.Printf("worker=%d nack task=%s attempt=%d kind=%s verdict=accepted basis=%q",
			workerID, task.ID, task.Attempts, te.Kind,
			"handler classified failure; retry/backoff or dead-letter applied")
	}
}

// withProgressReporter 把绑定本次投递租约的进度上报器注入 handler 上下文。
func withProgressReporter(ctx context.Context, q *Queue, taskID, token string) context.Context {
	return context.WithValue(ctx, progressReporterKey{}, &ProgressReporter{
		queue:  q,
		taskID: taskID,
		token:  token,
	})
}

// sleep 在 ctx 取消时立即返回 false。
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
