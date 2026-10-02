package queue

import (
	"context"
	"errors"
	"testing"
	"time"
)

// 场景（必覆盖）：handler 不理会取消一直跑，优雅关闭到达截止时间后：
//   - Run 在有界时间内返回 ErrShutdownDeadlineExceeded；
//   - 在途任务被强制退回 pending、立即可投递，重新开队列即可再次投递（attempt=2）；
//   - 关闭前已 Ack 的任务保持 succeeded，不会被这次强制释放再投递；
//   - 旧执行随后返回并尝试的 Ack / 进度上报被拒，最终结论仍是 attempt=2 的 succeeded。
func TestShutdownDeadlineForceRequeuesInflight(t *testing.T) {
	path := t.TempDir() + "/queue.json"
	cfg := consumerConfig()
	cfg.LeaseDuration = time.Hour // 不能依赖租约超时，强制退回必须由关闭截止时间触发
	q, err := New(cfg, NewFileStore(path), "inst-deadline-A")
	if err != nil {
		t.Fatal(err)
	}

	inflight := mustEnqueue(t, q, "inflight-ignores-cancel")
	done := mustEnqueue(t, q, "done-before-shutdown")

	blocked := make(chan struct{}) // 从不理会取消的在途 handler：等这个通道才返回
	entered := make(chan struct{})
	handlerReturned := make(chan struct{})
	flog := &failCountLogger{t: t} // 捕获旧执行迟到 ack/progress 被拒的日志

	handler := func(ctx context.Context, tk *Task) error {
		if tk.ID == inflight.ID {
			closeOnce(entered)
			reporter := ProgressFromContext(ctx)
			if reporter == nil {
				t.Errorf("task=%s progress reporter missing from handler context", tk.ID)
			}
			reporter.Report(35, "long batch export running")
			<-blocked // 故意忽略 ctx.Done()，模拟不配合取消的处理逻辑
			reporter.Report(99, "old execution late report")
			close(handlerReturned)
			return nil // 旧执行随后以为自己成功了
		}
		return nil
	}

	consumer := NewConsumer(q, 2, handler,
		WithPollInterval(2*time.Millisecond),
		WithShutdownGrace(120*time.Millisecond),
		WithProgressFlushInterval(30*time.Millisecond),
		WithLogger(flog))

	ctx, cancel := context.WithCancel(t.Context())
	runErr := runAsync(t, consumer, ctx)

	<-entered
	// 等另一条任务正常处理完并 Ack（关闭前的成功结论）。
	waitFor(t, func() bool {
		g, ok := q.Get(done.ID)
		return ok && g.Status == StatusSucceeded
	}, "second task acked before shutdown")

	cancel()
	select {
	case err := <-runErr:
		if !errors.Is(err, ErrShutdownDeadlineExceeded) {
			t.Fatalf("Run err=%v, want ErrShutdownDeadlineExceeded basis=handler ignored cancellation past grace", err)
		}
		t.Logf("[assert] shutdown returned err=%v basis=%q", err, "grace deadline elapsed while handler still running")
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not respect shutdown deadline")
	}

	g := statusOf(t, q, inflight.ID)
	if g.Status != StatusPending || g.LeaseToken != "" {
		t.Fatalf("task=%s after deadline status=%s token=%q, want pending with empty lease",
			inflight.ID, g.Status, g.LeaseToken)
	}
	if g.VisibleAt.After(time.Now()) {
		t.Fatalf("task=%s VisibleAt=%s in the future, want immediately deliverable", inflight.ID, g.VisibleAt)
	}
	if g.ProgressPercent != 35 || g.ProgressStage != "long batch export running" {
		t.Fatalf("task=%s progress=%d%%/%q, want 35%% retained through force-requeue",
			inflight.ID, g.ProgressPercent, g.ProgressStage)
	}
	t.Logf("[assert] task=%s attempt=1 after-deadline verdict=pending visible=now progress=%d%% basis=%q",
		inflight.ID, g.ProgressPercent, "force-requeued owned unfinished lease; progress snapshot untouched")

	if d := statusOf(t, q, done.ID); d.Status != StatusSucceeded || d.Attempts != 1 {
		t.Fatalf("task=%s status=%s attempts=%d, want succeeded/1 basis=acked before deadline",
			done.ID, d.Status, d.Attempts)
	}
	t.Logf("[assert] task=%s verdict=succeeded attempts=1 basis=%q",
		done.ID, "terminal ack is not requeued by deadline-driven release")

	// 新进程/新实例立刻接管在途任务，无需等租约超时；这是第 2 次投递。
	q2 := reopenQueue(t, path, "inst-deadline-B", cfg)
	lease2 := leaseCheck(t, mustDequeue(t, q2), inflight.ID, 2)
	if err := q2.Ack(context.Background(), inflight.ID, lease2.Token); err != nil {
		t.Fatalf("redelivery ack: %v", err)
	}
	t.Logf("[assert] task=%s attempt=2 redelivered-to=inst-deadline-B verdict=succeeded basis=%q",
		inflight.ID, "force-requeued lease immediately visible after restart")

	// 旧执行终于结束：它在 q1 内存里的迟到 Ack/进度上报都会因令牌已被清空而失败。
	close(blocked)
	select {
	case <-handlerReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("abandoned handler did not return after release")
	}
	// 确定性等待旧执行的 stale-ack 与 stale-progress 日志都出现。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) &&
		(!flog.linesContain("stale-ack task="+inflight.ID) ||
			!flog.linesContain("stale-progress task="+inflight.ID)) {
		time.Sleep(5 * time.Millisecond)
	}
	if !flog.linesContain("stale-ack task=" + inflight.ID) {
		t.Fatalf("task=%s stale ack from abandoned attempt not rejected, logs=%v", inflight.ID, flog.snapshotLines())
	}
	if !flog.linesContain("stale-progress task=" + inflight.ID) {
		t.Fatalf("task=%s stale progress from abandoned attempt not rejected, logs=%v", inflight.ID, flog.snapshotLines())
	}
	t.Logf("[assert] task=%s attempt=1 late ack/progress verdict=rejected basis=%q",
		inflight.ID, "force-requeue cleared token before old execution finished")

	final := statusOf(t, q2, inflight.ID)
	if final.Status != StatusSucceeded || final.Attempts != 2 {
		t.Fatalf("task=%s final=%s attempts=%d, want succeeded/2 basis=stale ack from attempt 1 rejected",
			inflight.ID, final.Status, final.Attempts)
	}
	t.Logf("[assert] task=%s final verdict=succeeded attempts=2 basis=%q",
		inflight.ID, "old execution token cleared by force-requeue; its late progress/ack rejected")
	if d := statusOf(t, q2, done.ID); d.Status != StatusSucceeded {
		t.Fatalf("previously acked task changed: %s", d.Status)
	}
}

// 场景：在途 handler 响应取消、在截止时间内退出时，走正常优雅关闭，
// 返回 nil，任务退回 pending（不把正常关闭误判成超时）。
func TestShutdownWithinGraceReturnsNil(t *testing.T) {
	q, err := New(consumerConfig(), NewFileStore(t.TempDir()+"/queue.json"), "inst-grace-ok")
	if err != nil {
		t.Fatal(err)
	}
	task := mustEnqueue(t, q, "cooperative")
	entered := make(chan struct{})
	release := make(chan struct{})
	handler := func(ctx context.Context, _ *Task) error {
		closeOnce(entered)
		<-ctx.Done()
		<-release
		return ctx.Err()
	}
	consumer := NewConsumer(q, 1, handler,
		WithShutdownGrace(5*time.Second),
		WithLogger(newTestLogger(t)))
	ctx, cancel := context.WithCancel(t.Context())
	runErr := runAsync(t, consumer, ctx)
	<-entered
	cancel()
	close(release)
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("graceful shutdown err=%v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not finish after cooperative handler exit")
	}
	g := statusOf(t, q, task.ID)
	if g.Status != StatusPending {
		t.Fatalf("task=%s status=%s, want pending basis=normal requeue within grace", task.ID, g.Status)
	}
	t.Logf("[assert] task=%s shutdown verdict=pending err=nil basis=%q",
		task.ID, "all handlers exited before grace deadline")
}
