// Command demo 演示本地持久化异步任务队列的典型用法。
//
// 数据文件默认 /tmp/ajq-demo/queue.json，可用 -data 指定。
// 重复运行（重启）即可观察恢复语义：已完成任务保持 succeeded，
// 上次未完成的任务会被重新投递。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/h3yow/async-job-queue-go/queue"
)

func main() {
	dataPath := flag.String("data", "/tmp/ajq-demo/queue.json", "持久化数据文件路径")
	enqueue := flag.String("enqueue", "", "入队一条 payload（前缀 slow:<秒>: 模拟长任务并上报进度，fatal: 不可恢复失败，fail:<n>: 前 n 次可重试失败）")
	workers := flag.Int("workers", 2, "消费者并发数")
	duration := flag.Duration("run", 3*time.Second, "消费者运行时长（0 表示直到信号）")
	shutdownTimeout := flag.Duration("shutdown-timeout", 5*time.Second, "优雅关闭等待在途 handler 的截止时间，到期强制退回租约")
	flag.Parse()

	logger := log.New(os.Stderr, "[demo] ", log.LstdFlags|log.Lmsgprefix)

	store := queue.NewFileStore(*dataPath)
	cfg := queue.DefaultConfig()
	cfg.LeaseDuration = 2 * time.Second
	cfg.BaseBackoff = 200 * time.Millisecond
	cfg.MaxBackoff = 2 * time.Second

	q, err := queue.New(cfg, store, "demo-instance")
	if err != nil {
		logger.Fatalf("create queue: %v", err)
	}

	if *enqueue != "" {
		t, err := q.Enqueue(context.Background(), *enqueue)
		if err != nil {
			logger.Fatalf("enqueue: %v", err)
		}
		logger.Printf("enqueued task=%s payload=%q", t.ID, t.Payload)
	}

	if *enqueue != "" && *duration < 0 {
		// 仅入队模式（-run=-1）：入队后立即退出，模拟生产者。
		return
	}

	// 任务处理器：
	//   "ok:..."              总是成功
	//   "slow:<秒>:..."       模拟长耗时任务：每 200ms 推进一步并上报进度（百分比 + 阶段说明）
	//   "fatal:..."           不可恢复失败，直接死信
	//   "fail:<n>:..."        前 n 次可重试失败，之后成功
	//   其他                   总是可重试失败，最终耗尽进入死信
	handler := func(ctx context.Context, t *queue.Task) error {
		switch {
		case strings.HasPrefix(t.Payload, "ok:"):
			return nil
		case strings.HasPrefix(t.Payload, "slow:"):
			var secs int
			var rest string
			if _, err := fmt.Sscanf(t.Payload, "slow:%d:%s", &secs, &rest); err != nil || secs <= 0 {
				return queue.NonRetryable(fmt.Errorf("bad slow payload %q", t.Payload))
			}
			pr, _ := queue.ProgressReporterFromContext(ctx)
			steps := secs * 5 // 每步 200ms
			for i := 1; i <= steps; i++ {
				select {
				case <-ctx.Done():
					return ctx.Err() // 关闭宽限到期仍未完成才被取消，租约由消费者退回
				case <-time.After(200 * time.Millisecond):
				}
				if pr != nil {
					pct := i * 100 / steps
					if err := pr.Report(pct, fmt.Sprintf("阶段 %d/%d", i, steps)); err != nil {
						logger.Printf("task=%s progress report rejected: %v", t.ID, err)
						return nil // 租约已易主，旧执行直接放弃
					}
					logger.Printf("task=%s attempt=%d progress=%d%%", t.ID, t.Attempts, pct)
				}
			}
			return nil
		case strings.HasPrefix(t.Payload, "fatal:"):
			return queue.NonRetryable(errors.New("fatal payload: " + t.Payload))
		case strings.HasPrefix(t.Payload, "fail:"):
			var n int
			var rest string
			if _, err := fmt.Sscanf(t.Payload, "fail:%d:%s", &n, &rest); err == nil && t.Attempts <= n {
				return queue.Retryable(fmt.Errorf("transient failure attempt %d/%d", t.Attempts, n))
			}
			return nil
		default:
			return queue.Retryable(errors.New("always failing payload"))
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	consumer := queue.NewConsumer(q, *workers, handler,
		queue.WithShutdownTimeout(*shutdownTimeout))
	logger.Printf("consumer starting workers=%d lease=%s shutdown-timeout=%s data=%s",
		*workers, cfg.LeaseDuration, *shutdownTimeout, *dataPath)
	if err := consumer.Run(ctx); err != nil {
		logger.Fatalf("consumer stopped with error: %v", err)
	}

	for _, t := range q.Snapshot() {
		logger.Printf("final task=%s status=%s attempts=%d progress=%d%% stage=%q visibleAt=%s reason=%q",
			t.ID, t.Status, t.Attempts, t.Progress.Percent, t.Progress.Message,
			t.VisibleAt.Format(time.RFC3339), t.DeadReason)
	}
}
