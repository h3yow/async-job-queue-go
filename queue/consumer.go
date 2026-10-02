package queue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// 常见关闭结果，调用方可用 errors.Is 判定。
var (
	// ErrShutdownDeadlineExceeded 优雅关闭超过截止时间：仍有 handler 未退出，
	// 队列已强制把其租约退回 pending（任务不丢、可重新投递），Run 返回本错误。
	ErrShutdownDeadlineExceeded = errors.New("shutdown deadline exceeded; in-flight leases force-requeued")
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
// 长耗时任务可用 ProgressFromContext(ctx) 拿到本次投递专属的进度上报器。
type Handler func(ctx context.Context, task *Task) error

// Consumer 从队列拉取任务并以固定数量 worker 并发处理。
type Consumer struct {
	queue              *Queue
	handler            Handler
	workers            int
	instance           string
	pollInterval       time.Duration
	shutdownGrace      time.Duration // 关闭时等待在途 handler 的截止时间
	progressFlushEvery time.Duration // 进度上报最小落盘间隔
	logger             Logger
}

// ConsumerOption 配置可选项。
type ConsumerOption func(*Consumer)

// WithPollInterval 设置空闲轮询间隔，默认 100ms。
func WithPollInterval(d time.Duration) ConsumerOption {
	return func(c *Consumer) { c.pollInterval = d }
}

// WithShutdownGrace 设置优雅关闭的截止时间：ctx 取消后，已在途的 handler
// 最多再等多久；到点仍未退出则强制退回其租约（Run 返回
// ErrShutdownDeadlineExceeded）。默认 10s；传 0 表示无截止（一直等，
// 与旧版本行为一致）。
func WithShutdownGrace(d time.Duration) ConsumerOption {
	return func(c *Consumer) { c.shutdownGrace = d }
}

// WithProgressFlushInterval 设置进度上报的最小落盘间隔，默认 200ms。
// 间隔内的多次上报合并为最新一条落盘，handler 侧调用始终非阻塞。
func WithProgressFlushInterval(d time.Duration) ConsumerOption {
	return func(c *Consumer) { c.progressFlushEvery = d }
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
		queue:              q,
		handler:            h,
		workers:            workers,
		instance:           q.InstanceID(),
		pollInterval:       100 * time.Millisecond,
		shutdownGrace:      10 * time.Second,
		progressFlushEvery: DefaultProgressFlushInterval,
		logger:             log.New(os.Stderr, "[consumer] ", log.LstdFlags|log.Lmsgprefix),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Run 启动 worker 循环直到 ctx 被取消。关闭流程：
//  1. 停止拉取新任务，取消在途任务的处理上下文；
//  2. 等待在途 handler 退出，最多等待 ShutdownGrace（0 表示无限等待）：
//     在截止时间内退出的，按其结果正常 Ack/Nack；
//  3. 到点仍有 handler 未退出时，不再等待——调用 RequeueOwned 强制把本实例
//     仍持有的租约退回 pending，任务立即可重新投递且不丢失，Run 返回
//     ErrShutdownDeadlineExceeded；之后迟到的 handler 无论 Ack/Nack 还是
//     进度上报，都会因令牌失效被拒绝，无法造成重复投递或覆盖新进度。
//
// 关闭前已成功 Ack 的任务是终态，不受强制退回影响，不会被再投递一次。
func (c *Consumer) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < c.workers; i++ {
		wg.Add(1)
		go c.worker(ctx, i, &wg)
	}

	workersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(workersDone)
	}()

	// 正常运行阶段：等待关闭请求；所有 worker 恰好在此前自行退出也算正常结束。
	select {
	case <-ctx.Done():
	case <-workersDone:
	}

	// 截止时间从“请求关闭”这一刻起算，而不是从启动起算，
	// 否则长任务（可能经历租约超时重投）会在无人请求关闭时被误杀。
	var graceCh <-chan time.Time
	if c.shutdownGrace > 0 {
		timer := time.NewTimer(c.shutdownGrace)
		defer timer.Stop()
		graceCh = timer.C
	}

	select {
	case <-workersDone:
		// 所有 handler 都在截止时间内结束，走常规优雅关闭。
	case <-graceCh:
		// 有 handler 不理会取消、一直跑：强制释放其租约，不再等待。
		requeued, err := c.queue.RequeueOwned(context.Background())
		if err != nil {
			c.logger.Printf("instance=%s shutdown deadline force-requeue failed: %v", c.instance, err)
			return err
		}
		c.logger.Printf("instance=%s shutdown grace=%s exceeded, force-requeued=%d unfinished task(s); "+
			"late ack/nack/progress from abandoned handlers will be rejected",
			c.instance, c.shutdownGrace, requeued)
		return fmt.Errorf("%w (grace=%s, requeued=%d)",
			ErrShutdownDeadlineExceeded, c.shutdownGrace, requeued)
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

	// 每次投递一个独立上报器：只有持本次令牌的上报才会被队列接受。
	reporter := newProgressReporter(c.queue, lease, c.progressFlushEvery, c.logger)
	hctx := withProgressReporter(ctx, reporter)

	err := c.handler(hctx, task.Clone())

	// 先收尾上报器（把最后一条进度同步落盘）；若租约已失效（超时重投/关闭强制退回），
	// 这里与后续 Ack/Nack 一样会得到 ErrInvalidLease，旧执行无法覆盖新进度。
	if perr := reporter.close(context.Background()); perr != nil {
		if errors.Is(perr, ErrInvalidLease) {
			c.logger.Printf("worker=%d stale-progress task=%s attempt=%d verdict=rejected basis=%q",
				workerID, task.ID, task.Attempts, "lease token mismatch (redelivered or force-requeued)")
		} else {
			c.logger.Printf("worker=%d progress-flush-error task=%s attempt=%d err=%v",
				workerID, task.ID, task.Attempts, perr)
		}
	}

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
