package queue

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// 场景：长任务处理中上报的进度随快照落盘，进程重启（用同一数据文件重开队列）
// 后读到的仍是最后一次成功上报的进度，不回退为零值；继续上报推进后再次重启依然保留。
func TestProgressPersistsAcrossRestart(t *testing.T) {
	path := t.TempDir() + "/queue.json"
	cfg := testConfig()

	q, err := New(cfg, NewFileStore(path), "inst-1")
	if err != nil {
		t.Fatal(err)
	}
	task := mustEnqueue(t, q, "export-job")
	lease := leaseCheck(t, mustDequeue(t, q), task.ID, 1)

	reportAt := time.Now()
	if err := q.ReportProgress(t.Context(), task.ID, lease.Token, 40, "导出第 2/5 段"); err != nil {
		t.Fatalf("ReportProgress: %v", err)
	}
	p, ok := q.GetProgress(task.ID)
	if !ok || !p.Reported() || p.Percent != 40 || p.Message != "导出第 2/5 段" {
		t.Fatalf("task=%s progress=%+v ok=%v, want 40%% reported", task.ID, p, ok)
	}
	if p.UpdatedAt.Before(reportAt) {
		t.Fatalf("task=%s progress UpdatedAt=%s before report moment %s", task.ID, p.UpdatedAt, reportAt)
	}
	t.Logf("[assert] task=%s attempt=%d progress=%d%% message=%q verdict=reported basis=%q",
		task.ID, lease.Task.Attempts, p.Percent, p.Message, "report with matching lease token persisted")

	// 模拟进程崩溃/重启：任务还停在 running，用同一数据文件重开队列。
	q2, err := New(cfg, NewFileStore(path), "inst-2")
	if err != nil {
		t.Fatal(err)
	}
	p2, ok := q2.GetProgress(task.ID)
	if !ok || p2.Percent != 40 || p2.Message != "导出第 2/5 段" || !p2.UpdatedAt.Equal(p.UpdatedAt) {
		t.Fatalf("task=%s progress after restart=%+v, want 40%% with same UpdatedAt=%s",
			task.ID, p2, p.UpdatedAt)
	}
	t.Logf("[assert] task=%s after-restart progress=%d%% updatedAt=%s verdict=preserved basis=%q",
		task.ID, p2.Percent, p2.UpdatedAt.Format(time.RFC3339Nano),
		"progress persisted with snapshot; reload restores last reported value")

	// 重启后任务被重新投递（running 遗留被重置），新一轮执行继续推进进度。
	lease2 := leaseCheck(t, mustDequeue(t, q2), task.ID, 2)
	if err := q2.ReportProgress(t.Context(), task.ID, lease2.Token, 75, "导出第 4/5 段"); err != nil {
		t.Fatalf("ReportProgress after restart: %v", err)
	}

	// 再次重启：进度只能停在新值，不允许倒退。
	q3, err := New(cfg, NewFileStore(path), "inst-3")
	if err != nil {
		t.Fatal(err)
	}
	p3, _ := q3.GetProgress(task.ID)
	if p3.Percent != 75 || p3.Message != "导出第 4/5 段" {
		t.Fatalf("task=%s progress after 2nd restart=%+v, want 75%% (no regression)", task.ID, p3)
	}
	t.Logf("[assert] task=%s after-2nd-restart progress=%d%% verdict=no-regression basis=%q",
		task.ID, p3.Percent, "every successful report is durable; reload never reverts to older value")
}

// 场景：租约超时任务被重新投递后，上一轮旧执行迟到的进度上报必须被拒绝，
// 不能覆盖新一轮租约期间上报的进度。
func TestStaleLeaseProgressReportRejected(t *testing.T) {
	cfg := testConfig()
	cfg.LeaseDuration = 30 * time.Millisecond
	q, err := New(cfg, NewFileStore(t.TempDir()+"/queue.json"), "inst-1")
	if err != nil {
		t.Fatal(err)
	}
	task := mustEnqueue(t, q, "segmented-export")

	lease1 := leaseCheck(t, mustDequeue(t, q), task.ID, 1)
	if err := q.ReportProgress(t.Context(), task.ID, lease1.Token, 30, "第 1 段"); err != nil {
		t.Fatalf("ReportProgress attempt=1: %v", err)
	}

	// 旧执行卡住超过租约，任务被重新投递（新一轮租约、新令牌）。
	time.Sleep(45 * time.Millisecond)
	lease2 := leaseCheck(t, mustDequeue(t, q), task.ID, 2)
	if err := q.ReportProgress(t.Context(), task.ID, lease2.Token, 55, "第 2 段（重投后）"); err != nil {
		t.Fatalf("ReportProgress attempt=2: %v", err)
	}

	// 旧执行迟到上报：必须拒绝，且不得覆盖新进度。
	err = q.ReportProgress(t.Context(), task.ID, lease1.Token, 5, "旧执行迟到上报")
	if !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("stale report err=%v, want ErrInvalidLease", err)
	}
	p, _ := q.GetProgress(task.ID)
	if p.Percent != 55 || p.Message != "第 2 段（重投后）" {
		t.Fatalf("task=%s progress=%+v after stale report, want 55%% unchanged", task.ID, p)
	}
	t.Logf("[assert] task=%s stale-report-from-attempt=1 verdict=rejected progress-kept=%d%% basis=%q",
		task.ID, p.Percent, "lease token rotated on redelivery; old execution cannot overwrite progress")

	// 未持有租约的胡乱令牌同样拒绝。
	if err = q.ReportProgress(t.Context(), task.ID, "lease-forged", 99, "x"); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("forged-token report err=%v, want ErrInvalidLease", err)
	}
	// 不存在的任务返回 ErrTaskNotFound。
	if err = q.ReportProgress(t.Context(), "task-missing", "lease-x", 1, "x"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("missing-task report err=%v, want ErrTaskNotFound", err)
	}
}

// 场景：任务进入 succeeded / dead 终态后，最后上报的进度仍然保留可查询，
// 重启后查询结果不变，用于事后排查。
func TestProgressRetainedAfterTerminalStates(t *testing.T) {
	path := t.TempDir() + "/queue.json"
	cfg := testConfig()
	q, err := New(cfg, NewFileStore(path), "inst-1")
	if err != nil {
		t.Fatal(err)
	}

	done := mustEnqueue(t, q, "export-ok")
	l1 := leaseCheck(t, mustDequeue(t, q), done.ID, 1)
	if err := q.ReportProgress(t.Context(), done.ID, l1.Token, 100, "全部完成"); err != nil {
		t.Fatal(err)
	}
	if err := q.Ack(t.Context(), done.ID, l1.Token); err != nil {
		t.Fatal(err)
	}

	dead := mustEnqueue(t, q, "export-bad")
	l2 := leaseCheck(t, mustDequeue(t, q), dead.ID, 1)
	if err := q.ReportProgress(t.Context(), dead.ID, l2.Token, 33, "第 1 段发现脏数据"); err != nil {
		t.Fatal(err)
	}
	if err := q.Nack(t.Context(), dead.ID, l2.Token, NonRetryable(errors.New("corrupt row 4096"))); err != nil {
		t.Fatal(err)
	}

	// 终态后进度仍可读。
	if p, _ := q.GetProgress(done.ID); p.Percent != 100 || p.Message != "全部完成" {
		t.Fatalf("task=%s succeeded progress=%+v, want 100%%", done.ID, p)
	}
	if p, _ := q.GetProgress(dead.ID); p.Percent != 33 || p.Message != "第 1 段发现脏数据" {
		t.Fatalf("task=%s dead progress=%+v, want 33%%", dead.ID, p)
	}
	t.Logf("[assert] task=%s status=succeeded progress=100%% verdict=queryable basis=%q",
		done.ID, "terminal states keep last reported progress for postmortem")
	t.Logf("[assert] task=%s status=dead progress=33%% verdict=queryable basis=%q",
		dead.ID, "terminal states keep last reported progress for postmortem")

	// 终态后任何上报都被拒绝（租约已清空）。
	if err := q.ReportProgress(t.Context(), done.ID, l1.Token, 101, "late"); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("report after ack err=%v, want ErrInvalidLease", err)
	}

	// 重启后查到的仍是同一条进度。
	q2, err := New(cfg, NewFileStore(path), "inst-2")
	if err != nil {
		t.Fatal(err)
	}
	if p, _ := q2.GetProgress(done.ID); p.Percent != 100 {
		t.Fatalf("task=%s progress after restart=%+v, want 100%%", done.ID, p)
	}
	if p, _ := q2.GetProgress(dead.ID); p.Percent != 33 {
		t.Fatalf("task=%s progress after restart=%+v, want 33%%", dead.ID, p)
	}
	if _, ok := q2.GetProgress("task-missing"); ok {
		t.Fatal("GetProgress for unknown task must return ok=false")
	}
	t.Logf("[assert] after-restart terminal-progress unchanged (100%%/33%%) basis=%q",
		"progress persisted in the same snapshot as the terminal state")
}

// 场景：旧版本写出的数据文件没有 progress 字段，新版本必须能直接加载，
// 加载后进度为零值（未上报），且各状态迁移与上报功能不受影响。
func TestOldDataFileWithoutProgressLoads(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "queue.json")
	// 旧格式：无 progress 字段。覆盖 pending / running / dead 三种记录。
	oldJSON := `[
  {
    "id": "task-old-pending",
    "payload": "legacy-pending",
    "status": "pending",
    "attempts": 1,
    "max_attempts": 5,
    "enqueued_at": "2026-01-01T00:00:00Z",
    "visible_at": "2026-01-01T00:00:00Z",
    "lease_owner": "",
    "lease_token": "",
    "lease_expires": "0001-01-01T00:00:00Z",
    "dead_reason": "",
    "updated_at": "2026-01-01T00:00:00Z"
  },
  {
    "id": "task-old-running",
    "payload": "legacy-running",
    "status": "running",
    "attempts": 2,
    "max_attempts": 5,
    "enqueued_at": "2026-01-01T00:00:00Z",
    "visible_at": "2026-01-01T00:00:00Z",
    "lease_owner": "inst-old",
    "lease_token": "lease-old",
    "lease_expires": "2026-01-01T00:00:30Z",
    "dead_reason": "",
    "updated_at": "2026-01-01T00:00:00Z"
  },
  {
    "id": "task-old-dead",
    "payload": "legacy-dead",
    "status": "dead",
    "attempts": 5,
    "max_attempts": 5,
    "enqueued_at": "2026-01-01T00:00:00Z",
    "visible_at": "2026-01-01T00:00:00Z",
    "lease_owner": "",
    "lease_token": "",
    "lease_expires": "0001-01-01T00:00:00Z",
    "dead_reason": "retry budget exhausted after 5/5 attempts, last error: boom",
    "updated_at": "2026-01-01T00:00:00Z"
  }
]`
	if err := os.WriteFile(path, []byte(oldJSON), 0o644); err != nil {
		t.Fatal(err)
	}

	q, err := New(testConfig(), NewFileStore(path), "inst-new")
	if err != nil {
		t.Fatalf("New on old data file: %v", err)
	}
	t.Logf("[assert] old-data-file loaded records=3 verdict=compatible basis=%q",
		"missing progress field decodes to zero value")

	// 旧记录进度为零值（从未上报）。
	for _, id := range []string{"task-old-pending", "task-old-running", "task-old-dead"} {
		p, ok := q.GetProgress(id)
		if !ok {
			t.Fatalf("task=%s missing after loading old file", id)
		}
		if p.Reported() || p.Percent != 0 || p.Message != "" {
			t.Fatalf("task=%s progress=%+v, want zero value for legacy record", id, p)
		}
	}

	// 旧文件遗留的 running 任务按既有恢复语义重置为 pending 并可重新投递；
	// 新执行可以正常上报进度。（两条 pending 按 ID 序投递，先 pending 后 running。）
	leaseCheck(t, mustDequeue(t, q), "task-old-pending", 2)
	lease := leaseCheck(t, mustDequeue(t, q), "task-old-running", 3)
	if err := q.ReportProgress(t.Context(), "task-old-running", lease.Token, 10, "恢复后继续"); err != nil {
		t.Fatalf("ReportProgress on legacy task: %v", err)
	}
	p, _ := q.GetProgress("task-old-running")
	if p.Percent != 10 {
		t.Fatalf("legacy task progress=%+v, want 10%%", p)
	}
	t.Logf("[assert] task=task-old-running attempt=3 progress=10%% verdict=upgraded-in-place basis=%q",
		"legacy record recovered from stale running and accepts progress reports")

	// 旧死信记录保持终态、不被投递，进度为零值也可查询。
	if l, _ := q.Dequeue(t.Context()); l != nil && l.Task.ID == "task-old-dead" {
		t.Fatal("legacy dead task must not be delivered")
	}
	if g := statusOf(t, q, "task-old-dead"); g.Status != StatusDead || g.DeadReason == "" {
		t.Fatalf("legacy dead task status=%s reason=%q", g.Status, g.DeadReason)
	}
}

// 场景：关闭时 handler 不理会取消一直跑，等待到截止时间后强制释放在途租约，
// Run 有明确时间上限地返回；在途任务立即重新可投递（不丢），
// 已确认成功的任务不受影响；旧执行迟到的上报/确认被拒绝。
func TestShutdownTimeoutForceRequeuesInflight(t *testing.T) {
	path := t.TempDir() + "/queue.json"
	cfg := consumerConfig()
	cfg.LeaseDuration = time.Hour // 排除租约自然超时，只验证关闭路径
	q, err := New(cfg, NewFileStore(path), "inst-shutdown-timeout")
	if err != nil {
		t.Fatal(err)
	}
	blocking := mustEnqueue(t, q, "runaway-handler")
	acked := mustEnqueue(t, q, "quick-ok")

	entered := make(chan struct{})
	release := make(chan struct{})
	reporterCh := make(chan *ProgressReporter, 1)
	handler := func(ctx context.Context, tk *Task) error {
		if tk.ID == blocking.ID {
			if pr, ok := ProgressReporterFromContext(ctx); ok {
				reporterCh <- pr
			}
			closeOnce(entered)
			<-release // 故意不理会 ctx 取消，模拟失控 handler
			return nil
		}
		return nil // quick-ok 立即成功
	}
	consumer := NewConsumer(q, 2, handler,
		WithPollInterval(2*time.Millisecond),
		WithShutdownTimeout(100*time.Millisecond),
		WithLogger(newTestLogger(t)))
	ctx, cancel := context.WithCancel(t.Context())
	doneCh := runAsync(t, consumer, ctx)

	<-entered // 失控任务已进入 handler
	waitFor(t, func() bool {
		g, ok := q.Get(acked.ID)
		return ok && g.Status == StatusSucceeded
	}, "quick task acked before shutdown")

	cancel()
	shutdownStart := time.Now()
	if err := <-doneCh; err != nil {
		t.Fatalf("Run: %v", err)
	}
	elapsed := time.Since(shutdownStart)
	if elapsed > 3*time.Second {
		t.Fatalf("shutdown took %s, want bounded by shutdown timeout 100ms", elapsed)
	}
	t.Logf("[assert] shutdown elapsed=%s verdict=bounded basis=%q",
		elapsed.Round(time.Millisecond), "inflight handler ignored cancel; deadline force-released its lease")

	// 在途任务被强制退回 pending，立即可重新投递（不丢）。
	got := statusOf(t, q, blocking.ID)
	if got.Status != StatusPending {
		t.Fatalf("task=%s status=%s, want pending after forced release", blocking.ID, got.Status)
	}
	lease := leaseCheck(t, mustDequeue(t, q), blocking.ID, 2)
	t.Logf("[assert] task=%s attempt=%d verdict=redeliverable basis=%q",
		blocking.ID, lease.Task.Attempts, "force-released lease returned task to pending without loss")

	// 已确认成功的任务不受超时释放影响。
	if g := statusOf(t, q, acked.ID); g.Status != StatusSucceeded {
		t.Fatalf("task=%s status=%s, want succeeded (acked before shutdown)", acked.ID, g.Status)
	}
	t.Logf("[assert] task=%s status=succeeded verdict=untouched basis=%q",
		acked.ID, "RequeueOwned only touches running leases owned by this instance")

	// 失控 handler 恢复后，其迟到的进度上报与 Ack 都因令牌轮换被拒绝。
	reporter := <-reporterCh
	if err := reporter.Report(99, "失控执行的迟到上报"); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("runaway report err=%v, want ErrInvalidLease", err)
	}
	close(release)
	waitFor(t, func() bool {
		g, ok := q.Get(blocking.ID)
		return ok && g.Status == StatusRunning && g.Attempts == 2
	}, "redelivered lease stays authoritative over runaway handler")
	t.Logf("[assert] task=%s runaway-report verdict=rejected basis=%q",
		blocking.ID, "lease token rotated by forced requeue; late report/ack from old execution rejected")
}

// 场景：handler 在处理中频繁上报进度，不影响其他任务的正常投递与完成。
func TestProgressReportingDoesNotBlockProcessing(t *testing.T) {
	path := t.TempDir() + "/queue.json"
	q, err := New(consumerConfig(), NewFileStore(path), "inst-progress")
	if err != nil {
		t.Fatal(err)
	}
	long := mustEnqueue(t, q, "long-export")
	others := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		others = append(others, mustEnqueue(t, q, fmt.Sprintf("quick-%d", i)).ID)
	}

	handler := func(ctx context.Context, tk *Task) error {
		if tk.ID != long.ID {
			return nil
		}
		pr, ok := ProgressReporterFromContext(ctx)
		if !ok {
			return NonRetryable(errors.New("progress reporter missing from handler context"))
		}
		for i := 1; i <= 10; i++ {
			if err := pr.Report(i*10, fmt.Sprintf("第 %d/10 段", i)); err != nil {
				return NonRetryable(fmt.Errorf("report %d: %w", i, err))
			}
		}
		return nil
	}
	consumer := NewConsumer(q, 3, handler,
		WithPollInterval(2*time.Millisecond), WithLogger(newTestLogger(t)))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	doneCh := runAsync(t, consumer, ctx)

	waitFor(t, func() bool {
		for _, id := range append(others, long.ID) {
			g, ok := q.Get(id)
			if !ok || g.Status != StatusSucceeded {
				return false
			}
		}
		return true
	}, "all tasks succeed while long task reports progress")
	cancel()
	if err := <-doneCh; err != nil {
		t.Fatal(err)
	}

	p, _ := q.GetProgress(long.ID)
	if p.Percent != 100 || p.Message != "第 10/10 段" {
		t.Fatalf("task=%s final progress=%+v, want 100%%", long.ID, p)
	}
	t.Logf("[assert] task=%s reports=10 final=100%% others-succeeded=%d verdict=no-interference basis=%q",
		long.ID, len(others), "progress reports serialize with queue ops as short atomic writes")
}
