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
type Handler func(ctx context.Context, task *Task) error

// Consumer 从队列拉取任务并以固定数量 worker 并发处理。
type Consumer struct {
	queue        *Queue
	handler      Handler
	workers      int
	instance     string
	pollInterval time.Duration
	logger       Logger
}

// ConsumerOption 配置可选项。
type ConsumerOption func(*Consumer)

// WithPollInterval 设置空闲轮询间隔，默认 100ms。
func WithPollInterval(d time.Duration) ConsumerOption {
	return func(c *Consumer) { c.pollInterval = d }
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
		queue:        q,
		handler:      h,
		workers:      workers,
		instance:     q.InstanceID(),
		pollInterval: 100 * time.Millisecond,
		logger:       log.New(os.Stderr, "[consumer] ", log.LstdFlags|log.Lmsgprefix),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Run 启动 worker 循环直到 ctx 被取消：
//  1. 停止拉取新任务；
//  2. 取消在途任务的处理上下文，等待所有 worker 退出；
//  3. 将本实例仍持有（未 Ack/Nack）的租约一次性退回 pending，
//     使其在重启后/其他消费者处立即可重新投递。
//
// 关闭前已成功完成的任务仍会正常 Ack 落盘，不会被退回。
func (c *Consumer) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < c.workers; i++ {
		wg.Add(1)
		go c.worker(ctx, i, &wg)
	}
	wg.Wait()

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

	err := c.handler(ctx, task.Clone())

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
