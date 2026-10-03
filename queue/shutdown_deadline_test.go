package queue

import (
	"context"
	"errors"
	"testing"
	"time"
)

// 场景（本 bug 的稳定复现）：
//
//	阶段一：消费者正常运行（不触发关闭）的时长明显超过 shutdownTimeout，
//	       期间一个长任务一直处于在途。消费者既不能自动退出，也不能提前
//	       放掉手上的在途租约（任务必须保持 running、不能变 pending）。
//	阶段二：随后才触发关闭。
//	       - 一个在途任务虽不响应取消，但能在 shutdownTimeout 内跑完，
//	         必须正常判 succeeded 落盘，不能被提前掐断重投；
//	       - 另一个到点仍不退出的失控任务，才被强制退回 pending、
//	         立即可重新投递，且它事后迟到的 Ack/进度上报被令牌轮换拒绝。
//
// 运行日志可看出任务标识、第几次投递（attempt）与最终判定依据（basis）。
func TestShutdownDeadlineStartsOnlyAfterShutdown(t *testing.T) {
	path := t.TempDir() + "/queue.json"
	cfg := consumerConfig()
	cfg.LeaseDuration = time.Hour // 排除租约自然超时，只验证关闭路径
	cfg.MaxAttempts = 5

	const (
		shutdownTimeout = 200 * time.Millisecond
		// 给"关闭耗时 >= 完整上限"断言预留的调度容差。
		deadlineTolerance = 40 * time.Millisecond
	)

	q, err := New(cfg, NewFileStore(path), "inst-deadline")
	if err != nil {
		t.Fatal(err)
	}

	// phase1: 正常运行期间的长任务，跨越"远超关闭等待上限"的时段仍在途。
	phase1 := mustEnqueue(t, q, "long-task-during-normal-run")
	// earlyOk: 阶段一期间正常完成的普通任务，证明消费者这段时间一直在消费。
	earlyOk := mustEnqueue(t, q, "quick-ok-during-normal-run")
	// withinDeadline: 关闭后不响应取消、但能在上限内跑完的任务。
	withinDeadline := mustEnqueue(t, q, "finishes-within-deadline-after-shutdown")
	// runaway: 关闭后到点仍不退出的失控任务。
	runaway := mustEnqueue(t, q, "runaway-after-shutdown")

	normalRunStart := make(chan struct{})
	phase1Entered := make(chan struct{})
	withinEntered := make(chan struct{})
	runawayEntered := make(chan struct{})
	finishWithin := make(chan struct{}) // 关闭后才放行 withinDeadline
	releaseRunaway := make(chan struct{})

	handler := func(ctx context.Context, tk *Task) error {
		switch tk.ID {
		case phase1.ID:
			// 长任务：正常运行期间持续在途，收到关闭后才收工。
			closeOnce(phase1Entered)
			<-normalRunStart
			<-ctx.Done()
			return ctx.Err()
		case withinDeadline.ID:
			// 关闭后不响应取消信号，直到测试放行；放行后在上限内跑完并返回成功。
			closeOnce(withinEntered)
			<-finishWithin
			return nil
		case runaway.ID:
			// 失控任务：完全不理会取消，到 shutdownTimeout 仍不退出。
			closeOnce(runawayEntered)
			<-releaseRunaway
			return nil
		default:
			return nil // earlyOk 立即成功
		}
	}

	// 3 个 worker：phase1 长期占住一个，另外两个分别处理 earlyOk 与后续任务。
	consumer := NewConsumer(q, 3, handler,
		WithPollInterval(2*time.Millisecond),
		WithShutdownTimeout(shutdownTimeout),
		WithLogger(newTestLogger(t)))

	ctx, cancel := context.WithCancel(t.Context())
	doneCh := runAsync(t, consumer, ctx)

	<-phase1Entered
	// 阶段一：正常运行持续 4 倍 shutdownTimeout，期间不触发关闭。
	const normalRun = 4 * shutdownTimeout
	t.Logf("[phase=normal-run] task=%s attempt=1 stays inflight for %s (> shutdownTimeout=%s), "+
		"no shutdown requested basis=%q",
		phase1.ID, normalRun, shutdownTimeout,
		"shutdown deadline is not armed until shutdown is requested")
	time.Sleep(normalRun)

	// 断言一：正常跑超上限后消费者仍在运行，长任务仍是本次租约（running, attempt=1），
	// 没有被提前退回 pending、没有重复投递。
	select {
	case runErr := <-doneCh:
		t.Fatalf("consumer self-terminated during normal run after %s, Run returned: %v (should keep running)",
			normalRun, runErr)
	default:
	}
	if g := statusOf(t, q, phase1.ID); g.Status != StatusRunning || g.Attempts != 1 {
		t.Fatalf("task=%s during normal run status=%s attempts=%d, want running/attempt=1 basis=%q",
			phase1.ID, g.Status, g.Attempts, "no shutdown => inflight lease must not be released")
	}
	t.Logf("[assert phase=normal-run] task=%s attempt=1 status=running verdict=kept-inflight basis=%q",
		phase1.ID, "consumer stays up and never releases leases before shutdown is requested")

	// 期间普通任务照常消费完成，证明消费者一直正常工作。
	waitFor(t, func() bool {
		g, ok := q.Get(earlyOk.ID)
		return ok && g.Status == StatusSucceeded
	}, "quick task succeeds during the long normal run")
	t.Logf("[assert phase=normal-run] task=%s verdict=succeeded basis=%q",
		earlyOk.ID, "consumer keeps consuming regardless of process uptime")

	// 让 phase1 在途任务在关闭到来后响应取消收工。
	close(normalRunStart)

	// 等待关闭后两个关键在途任务都已开始处理，并记下失控任务的旧租约令牌。
	<-withinEntered
	<-runawayEntered
	oldRunaway := statusOf(t, q, runaway.ID)
	if oldRunaway.Status != StatusRunning || oldRunaway.LeaseToken == "" {
		t.Fatalf("runaway precondition: status=%s token=%q, want running with token",
			oldRunaway.Status, oldRunaway.LeaseToken)
	}
	oldToken := oldRunaway.LeaseToken

	// 阶段二：此刻才请求关闭，关闭应从这一刻起拿到完整的 shutdownTimeout。
	cancel()
	shutdownStart := time.Now()

	// 关闭后等待约一半上限再放行 withinDeadline：
	// 它不响应取消，但只要在上限内跑完就必须判成功，不能被提前掐断。
	time.Sleep(shutdownTimeout / 2)
	t.Logf("[phase=shutdown] task=%s attempt=1 allowed to finish at %s into deadline %s basis=%q",
		withinDeadline.ID, (shutdownTimeout / 2).String(), shutdownTimeout,
		"full deadline window is granted starting from shutdown request")
	close(finishWithin)

	if err := <-doneCh; err != nil {
		t.Fatalf("Run after shutdown: %v", err)
	}
	elapsed := time.Since(shutdownStart)

	// 关闭耗时应接近一个完整上限（等满才强制释放 runaway），
	// 而不是因为进程此前已跑很久而几乎没有等待余地；同时必须有上界。
	if elapsed < shutdownTimeout-deadlineTolerance {
		t.Fatalf("shutdown elapsed=%s, want >= ~%s (full deadline measured from shutdown request)",
			elapsed, shutdownTimeout)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("shutdown elapsed=%s, want bounded near %s", elapsed, shutdownTimeout)
	}
	t.Logf("[assert phase=shutdown] shutdown elapsed=%s verdict=bounded basis=%q",
		elapsed.Round(time.Millisecond),
		"deadline starts at shutdown request; runaway handler is force-released only at the deadline")

	// 断言二：关闭后在上限内跑完的任务判成功落盘，没有被重投。
	if g := statusOf(t, q, withinDeadline.ID); g.Status != StatusSucceeded || g.Attempts != 1 {
		t.Fatalf("task=%s status=%s attempts=%d, want succeeded/attempt=1 basis=%q",
			withinDeadline.ID, g.Status, g.Attempts,
			"handler finished within deadline after shutdown => ack and persist success")
	}
	t.Logf("[assert phase=shutdown] task=%s attempt=1 verdict=succeeded basis=%q",
		withinDeadline.ID,
		"finished within the post-shutdown deadline; ack persisted, never redelivered")

	// 断言三：到点仍不退出的失控任务被强制退回 pending、立即可重新投递。
	// 注意：阶段一的长任务 phase1 未完成，关闭时同样被退回 pending，
	// 因此这里要在可投递集合里找到 runaway，而不能假设它排在最前。
	if g := statusOf(t, q, runaway.ID); g.Status != StatusPending {
		t.Fatalf("task=%s status=%s, want pending after forced release", runaway.ID, g.Status)
	}
	var lease2 *Lease
	for {
		l, derr := q.Dequeue(context.Background())
		if derr != nil {
			t.Fatalf("Dequeue after shutdown: %v", derr)
		}
		if l == nil {
			t.Fatalf("runaway task=%s not redeliverable after forced release", runaway.ID)
		}
		if l.Task.ID == runaway.ID {
			lease2 = l
			break
		}
		// 其他被退回的未完成任务（如 phase1）也应是 attempt 递增的重新投递。
		if l.Task.Attempts < 2 {
			t.Fatalf("requeued task=%s attempts=%d, want >=2", l.Task.ID, l.Task.Attempts)
		}
		t.Logf("[assert phase=shutdown] task=%s attempt=%d verdict=also-redeliverable basis=%q",
			l.Task.ID, l.Task.Attempts, "unfinished inflight task returned to pending on graceful stop")
	}
	if lease2.Task.Attempts != 2 {
		t.Fatalf("task=%s attempts=%d, want 2", runaway.ID, lease2.Task.Attempts)
	}
	if lease2.Token == oldToken {
		t.Fatalf("task=%s redelivery reused old lease token %q, want rotated token", runaway.ID, oldToken)
	}
	t.Logf("[assert phase=shutdown] task=%s attempt=2 verdict=redeliverable basis=%q",
		runaway.ID,
		"only handlers still running after the full deadline are force-released; task is not lost")

	// 断言四：被释放的旧执行事后迟到的 Ack 与进度上报都因令牌轮换被拒绝，
	// 不会把新一轮投递再确认一次、也不会覆盖新进度。
	if err := q.Ack(context.Background(), runaway.ID, oldToken); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("task=%s late ack from old execution err=%v, want ErrInvalidLease", runaway.ID, err)
	}
	if err := q.ReportProgress(context.Background(), runaway.ID, oldToken, 99, "旧执行迟到上报"); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("task=%s late progress from old execution err=%v, want ErrInvalidLease", runaway.ID, err)
	}
	t.Logf("[assert phase=shutdown] task=%s late-ack/progress-from-attempt=1 verdict=rejected basis=%q",
		runaway.ID, "forced release rotated the lease token; stale confirmation and report are rejected")

	// 放行真正的失控 handler：消费者已返回，其迟到 Ack 同样被拒，
	// attempt=2 的新租约始终是权威结论。
	close(releaseRunaway)
	waitFor(t, func() bool {
		g, ok := q.Get(runaway.ID)
		return ok && g.Status == StatusRunning && g.Attempts == 2
	}, "redelivered lease stays authoritative over the released old execution")
	t.Logf("[assert phase=shutdown] task=%s attempt=2 status=running verdict=authoritative basis=%q",
		runaway.ID, "late ack from the released handler is rejected; the new lease decides the outcome")
}
