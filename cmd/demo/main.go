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
	enqueue := flag.String("enqueue", "", "入队一条 payload（payload 前缀 fatal: 表示不可恢复失败，fail: 表示前几次可重试失败）")
	workers := flag.Int("workers", 2, "消费者并发数")
	duration := flag.Duration("run", 3*time.Second, "消费者运行时长（0 表示直到信号）")
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
	//   其他                   总是可重试失败，最终耗尽进入死信
	handler := func(_ context.Context, t *queue.Task) error {
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

	consumer := queue.NewConsumer(q, *workers, handler)
	logger.Printf("consumer starting workers=%d lease=%s data=%s", *workers, cfg.LeaseDuration, *dataPath)
	if err := consumer.Run(ctx); err != nil {
		logger.Fatalf("consumer stopped with error: %v", err)
	}

	for _, t := range q.Snapshot() {
		logger.Printf("final task=%s status=%s attempts=%d visibleAt=%s reason=%q",
			t.ID, t.Status, t.Attempts, t.VisibleAt.Format(time.RFC3339), t.DeadReason)
	}
}
