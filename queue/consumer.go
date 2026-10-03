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

// WithShutdownTimeout 设置收到关闭（Run 的 ctx 被取消）后等待在途 handler
// 退出的截止时间。该时长**只在关闭后才开始计时**：正常运行期间不计时，
// 消费者常驻多久、任务跑多久都不会触发它；关闭后无论进程此前运行了多久，
// 都会重新拿到这份完整的等待时间。
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

// Run 启动 worker 循环并持续消费，直到 ctx 被取消才开始优雅关闭。
// 正常运行期间没有任何时间上限：消费者可以常驻数小时、单个任务可以
// 运行任意长时间，shutdownTimeout 绝不参与正常运行阶段。
//
// ctx 取消后的关闭流程：
//  1. worker 停止拉取新任务，在途任务的处理上下文被取消；
//  2. 从关闭这一刻起重新计时，等待所有 worker 退出，
//     最长等待 shutdownTimeout（默认 30s，WithShutdownTimeout 可调），
//     等待时长与进程此前已运行多久无关——关闭后总是拿到完整的等待额度，
//     在途 handler 只要在此期间跑完，其成功结果仍会正常 Ack 落盘；
//  3. 将本实例仍持有（未 Ack/Nack）的租约一次性退回 pending，
//     使其在重启后/其他消费者处立即可重新投递。
//
// 关闭前已成功完成的任务仍会正常 Ack 落盘，不会被退回。
// 只有从关闭起算、等满 shutdownTimeout 仍不退出（不理会取消）的失控
// handler，Run 才不再等待：强制退回这些在途租约后直接返回，任务不会丢失；
// 失控 handler 后续迟到的 Ack/Nack/进度上报会因租约令牌已轮换而被拒绝。
func (c *Consumer) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < c.workers; i++ {
		wg.Add(1)
		go c.worker(ctx, i, &wg)
	}

	// 正常运行阶段：只等待关闭信号，不设任何定时器。
	// 进程常驻多久、单个任务跑多久都不会触发自我终止或释放租约。
	<-ctx.Done()
	c.logger.Printf("instance=%s shutdown requested, waiting up to %s for inflight handler(s)",
		c.instance, c.shutdownLabel())

	workersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(workersDone)
	}()

	// 截止时间从关闭这一刻才开始起算，与 Run 此前的运行时长无关。
	if c.shutdownTimeout > 0 {
		timer := time.NewTimer(c.shutdownTimeout)
		defer timer.Stop()
		select {
		case <-workersDone:
			c.logger.Printf("instance=%s all workers exited within shutdown deadline %s",
				c.instance, c.shutdownTimeout)
		case <-timer.C:
			c.logger.Printf("instance=%s shutdown deadline %s exceeded, "+
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
	c.logger.Printf("instance=%s graceful shutdown complete, requeued=%d unfinished task(s)",
		c.instance, requeued)
	return nil
}

func (c *Consumer) worker(ctx context.Context, id int, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		lease, err := c.queue.Dequeue(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.logger.Printf("worker=%d dequeue error: %v", id, err)
			if !sleep(ctx, c.pollInterval) {
				return
			}
			continue
		}
		if lease == nil {
			if !sleep(ctx, c.pollInterval) {
				return
			}
			continue
		}
		c.handle(ctx, id, lease)
	}
}

// handle 处理一次投递并给出终局判定；所有判定都会打印任务标识、投递次数与依据。
func (c *Consumer) handle(ctx context.Context, workerID int, lease *Lease) {
	task := lease.Task
	c.logger.Printf("worker=%d deliver task=%s attempt=%d/%d basis=%q",
		workerID, task.ID, task.Attempts, task.MaxAttempts,
		"pending-visible-or-lease-expired")

	err := c.handler(withProgressReporter(ctx, c.queue, task.ID, lease.Token), task.Clone())

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
		// 关闭进行中：不再 Nack（退避没有意义），交给 RequeueOwned 立即退回。
		if ctx.Err() != nil {
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

// shutdownLabel 返回关闭等待额度的可读描述（<=0 为无限等待）。
func (c *Consumer) shutdownLabel() string {
	if c.shutdownTimeout > 0 {
		return c.shutdownTimeout.String()
	}
	return "forever"
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
