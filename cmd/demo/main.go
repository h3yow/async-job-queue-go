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
	enqueue := flag.String("enqueue", "", "入队一条 payload（payload 前缀 fatal: 表示不可恢复失败，fail: 表示前几次可重试失败，long:<秒数> 表示会上报进度的长任务）")
	workers := flag.Int("workers", 2, "消费者并发数")
	duration := flag.Duration("run", 3*time.Second, "消费者运行时长（0 表示直到信号）")
	shutdownGrace := flag.Duration("shutdown-grace", 5*time.Second, "优雅关闭截止时间：到点未退出的在途处理强制退回（0 表示一直等）")
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
	//   "fatal:..."           不可恢复失败，直接死信
	//   "fail:<n>:..."        前 n 次可重试失败，之后成功
	//   "long:<秒数>:..."     长任务：执行期间每 200ms 上报一次百分比与阶段，
	//                         进度持久化在数据文件里（重启后仍可查询）
	//   其他                   总是可重试失败，最终耗尽进入死信
	handler := func(ctx context.Context, t *queue.Task) error {
		switch {
		case strings.HasPrefix(t.Payload, "ok:"):
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
		case strings.HasPrefix(t.Payload, "long:"):
			var secs int
			var rest string
			if _, err := fmt.Sscanf(t.Payload, "long:%d:%s", &secs, &rest); err != nil {
				secs = 2
			}
			if secs <= 0 {
				secs = 2
			}
			reporter := queue.ProgressFromContext(ctx)
			startFrom := t.ProgressPercent // 重启/重投后从上次进度继续，不倒退
			steps := secs * 5              // 每 200ms 一步
			for i := 0; i <= steps; i++ {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(200 * time.Millisecond):
				}
				pct := startFrom + (100-startFrom)*i/steps
				stage := fmt.Sprintf("processing %s (%d/%d ticks)", rest, i, steps)
				if reporter != nil {
					reporter.Report(pct, stage) // 非阻塞；后台节流落盘
				}
				logger.Printf("tick task=%s attempt=%d progress=%d%% stage=%q", t.ID, t.Attempts, pct, stage)
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
		queue.WithShutdownGrace(*shutdownGrace))
	logger.Printf("consumer starting workers=%d lease=%s shutdownGrace=%s data=%s",
		*workers, cfg.LeaseDuration, *shutdownGrace, *dataPath)
	runErr := consumer.Run(ctx)
	if runErr != nil && !errors.Is(runErr, queue.ErrShutdownDeadlineExceeded) {
		logger.Fatalf("consumer stopped with error: %v", runErr)
	}
	if errors.Is(runErr, queue.ErrShutdownDeadlineExceeded) {
		logger.Printf("consumer stopped: %v（未完成任务已强制退回，可重新投递）", runErr)
	}

	for _, t := range q.Snapshot() {
		logger.Printf("final task=%s status=%s attempts=%d progress=%d%% stage=%q reportedAt=%s visibleAt=%s reason=%q",
			t.ID, t.Status, t.Attempts, t.ProgressPercent, t.ProgressStage,
			t.ProgressReportedAt.Format(time.RFC3339), t.VisibleAt.Format(time.RFC3339), t.DeadReason)
	}
}
