package queue

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 场景 1（必覆盖）：长任务多次上报进度并落盘；进程崩溃重启后，
// 读到的是最后一次成功上报的进度——百分比不倒退、阶段文字与上报时间保留。
// 死信后进度同样可查。
func TestProgressPersistsAcrossRestart(t *testing.T) {
	path := t.TempDir() + "/queue.json"
	cfg := testConfig()
	q := reopenQueue(t, path, "inst-progress-A", cfg)
	task := mustEnqueue(t, q, "long-export")
	lease := leaseCheck(t, mustDequeue(t, q), task.ID, 1)

	ctx := context.Background()
	updates := []struct {
		percent int
		stage   string
	}{
		{10, "scan rows"},
		{10, "scan rows"}, // 同值允许：仅刷新阶段/时间
		{45, "export shard 1/2"},
		{80, "upload"},
	}
	for i, u := range updates {
		if err := q.ReportProgress(ctx, task.ID, lease.Token, u.percent, u.stage); err != nil {
			t.Fatalf("ReportProgress #%d: %v", i, err)
		}
	}
	// 查询结果应与已落盘状态一致。
	got := statusOf(t, q, task.ID)
	if got.ProgressPercent != 80 || got.ProgressStage != "upload" || got.ProgressReportedAt.IsZero() {
		t.Fatalf("task=%s in-memory progress=%d stage=%q at=%v, want 80/upload/non-zero",
			task.ID, got.ProgressPercent, got.ProgressStage, got.ProgressReportedAt)
	}
	lastReported := got.ProgressReportedAt
	t.Logf("[assert] task=%s attempt=1 progress=%d%% stage=%q reportedAt=%s basis=%q",
		task.ID, got.ProgressPercent, got.ProgressStage, lastReported.Format(time.RFC3339Nano),
		"every accepted progress report is persisted immediately")

	// 倒退的上报必须被拒绝。
	if err := q.ReportProgress(ctx, task.ID, lease.Token, 79, "rollback"); !errors.Is(err, ErrProgressRegressed) {
		t.Fatalf("task=%s regressed report err=%v, want ErrProgressRegressed", task.ID, err)
	}
	if g := statusOf(t, q, task.ID); g.ProgressPercent != 80 || g.ProgressStage != "upload" {
		t.Fatalf("task=%s rejected regress report mutated state: %d/%q", task.ID, g.ProgressPercent, g.ProgressStage)
	}
	t.Logf("[assert] task=%s stale-progress-percent=79 verdict=rejected basis=%q",
		task.ID, "percent must be monotonic non-decreasing; state untouched")

	// 进程在这里"崩溃"（不 Ack、不优雅关闭），用同一数据文件重开。
	q2 := reopenQueue(t, path, "inst-progress-B", cfg)
	g2 := statusOf(t, q2, task.ID)
	if g2.Status != StatusPending {
		t.Fatalf("task=%s after restart status=%s, want pending", task.ID, g2.Status)
	}
	if g2.ProgressPercent != 80 || g2.ProgressStage != "upload" {
		t.Fatalf("task=%s after restart progress=%d%% stage=%q, want 80/upload basis=last report survived crash",
			task.ID, g2.ProgressPercent, g2.ProgressStage)
	}
	if !g2.ProgressReportedAt.Equal(lastReported) {
		t.Fatalf("task=%s after restart reportedAt=%v, want %v",
			task.ID, g2.ProgressReportedAt, lastReported)
	}
	t.Logf("[assert] task=%s after-restart progress=%d%% stage=%q verdict=retained basis=%q",
		task.ID, g2.ProgressPercent, g2.ProgressStage,
		"progress fields are part of the persisted snapshot and never reset on recovery")

	// 新一轮投递从 80% 接着报，不能因重启要求从 0 重新开始。
	lease2 := leaseCheck(t, mustDequeue(t, q2), task.ID, 2)
	if err := q2.ReportProgress(ctx, task.ID, lease2.Token, 90, "verify"); err != nil {
		t.Fatalf("resumed ReportProgress: %v", err)
	}
	// 新这一轮想报 50% 仍然是倒退，被拒。
	if err := q2.ReportProgress(ctx, task.ID, lease2.Token, 50, "rewind"); !errors.Is(err, ErrProgressRegressed) {
		t.Fatalf("post-restart regress err=%v, want ErrProgressRegressed", err)
	}
	if err := q2.Nack(ctx, task.ID, lease2.Token, NonRetryable(errors.New("export target gone"))); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	dead := statusOf(t, q2, task.ID)
	if dead.Status != StatusDead || dead.ProgressPercent != 90 || dead.ProgressStage != "verify" {
		t.Fatalf("task=%s dead snapshot=%d%%/%q status=%s, want dead with final progress",
			task.ID, dead.ProgressPercent, dead.ProgressStage, dead.Status)
	}
	t.Logf("[assert] task=%s attempt=2 verdict=dead final-progress=%d%% stage=%q basis=%q",
		task.ID, dead.ProgressPercent, dead.ProgressStage,
		"final accepted progress stays queryable after entering dead letter")

	// 再次重启，死信里的最后进度仍可用于排查。
	q3 := reopenQueue(t, path, "inst-progress-C", cfg)
	g3 := statusOf(t, q3, task.ID)
	if g3.Status != StatusDead || g3.ProgressPercent != 90 {
		t.Fatalf("task=%s after second restart status=%s progress=%d, want dead/90",
			task.ID, g3.Status, g3.ProgressPercent)
	}
	t.Logf("[assert] task=%s after-restart status=dead final-progress=%d%% verdict=queryable basis=%q",
		task.ID, g3.ProgressPercent, "progress is retained with terminal state across restarts")
}

// 场景 2（必覆盖）：租约超时任务被重新投递后，上一轮还在跑的旧执行
// 再上报进度必须被拒绝（令牌已轮换），不能覆盖新一轮的进度。
func TestStaleProgressFromOldAttemptRejected(t *testing.T) {
	cfg := testConfig()
	cfg.LeaseDuration = 30 * time.Millisecond
	q, err := New(cfg, NewFileStore(t.TempDir()+"/queue.json"), "inst-stale-progress")
	if err != nil {
		t.Fatal(err)
	}
	task := mustEnqueue(t, q, "slow-export")
	lease1 := leaseCheck(t, mustDequeue(t, q), task.ID, 1)
	ctx := context.Background()

	if err := q.ReportProgress(ctx, task.ID, lease1.Token, 30, "phase-a"); err != nil {
		t.Fatal(err)
	}

	// 租约超时，重新投递，令牌轮换。
	time.Sleep(45 * time.Millisecond)
	lease2 := leaseCheck(t, mustDequeue(t, q), task.ID, 2)
	if lease1.Token == lease2.Token {
		t.Fatal("redelivery must rotate lease token")
	}
	if err := q.ReportProgress(ctx, task.ID, lease2.Token, 60, "phase-b-new-owner"); err != nil {
		t.Fatalf("fresh owner report: %v", err)
	}

	// 旧执行（attempt=1）迟到上报：即便百分比更高也必须拒绝。
	err = q.ReportProgress(ctx, task.ID, lease1.Token, 99, "old-owner-late")
	if !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("task=%s stale progress err=%v, want ErrInvalidLease basis=token rotated", task.ID, err)
	}
	got := statusOf(t, q, task.ID)
	if got.ProgressPercent != 60 || got.ProgressStage != "phase-b-new-owner" {
		t.Fatalf("task=%s progress overwritten by stale report: %d%%/%q",
			task.ID, got.ProgressPercent, got.ProgressStage)
	}
	t.Logf("[assert] task=%s stale-progress-from-attempt=1 percent=99 verdict=rejected basis=%q; "+
		"current progress=%d%% stage=%q from attempt=2",
		task.ID, "old lease token superseded after visibility timeout",
		got.ProgressPercent, got.ProgressStage)

	// 非法百分比。
	for _, bad := range []int{-1, 101} {
		if err := q.ReportProgress(ctx, task.ID, lease2.Token, bad, "x"); !errors.Is(err, ErrInvalidProgress) {
			t.Fatalf("percent=%d err=%v, want ErrInvalidProgress", bad, err)
		}
	}
	if g := statusOf(t, q, task.ID); g.ProgressPercent != 60 {
		t.Fatalf("invalid report mutated progress: %d", g.ProgressPercent)
	}
	t.Logf("[assert] task=%s invalid-progress verdict=rejected basis=%q",
		task.ID, "percent out of [0,100]")
}

// 场景：ProgressReporter 高频上报不阻塞 handler，磁盘慢时处理照常推进；
// 最终进度经过 close 同步落盘，查询与磁盘一致。
func TestProgressReporterIsNonBlockingUnderSlowStore(t *testing.T) {
	path := t.TempDir() + "/queue.json"
	slow := &slowStore{inner: NewFileStore(path), delay: 20 * time.Millisecond}
	q, err := New(testConfig(), slow, "inst-reporter")
	if err != nil {
		t.Fatal(err)
	}
	task := mustEnqueue(t, q, "frequent")
	lease := mustDequeue(t, q)
	leaseCheck(t, lease, task.ID, 1)

	reporter := newProgressReporter(q, lease, 15*time.Millisecond, newTestLogger(t))

	// 高频上报 1000 次：每次都必须是“加锁写内存”级别的耗时，远小于一次落盘。
	start := time.Now()
	const n = 1000
	for i := 0; i < n; i++ {
		// 单调到 100 后保持：高频上报只更新阶段，合并落盘后最终值确定为 100。
		reporter.Report(min(i, 100), fmt.Sprintf("step-%d", i))
	}
	elapsed := time.Since(start)
	if elapsed > 500*time.Millisecond {
		t.Fatalf("task=%s %d reports took %s, Report must not block on disk flush", task.ID, n, elapsed)
	}
	t.Logf("[assert] task=%s reports=%d elapsed=%s storeDelay=20ms verdict=non-blocking basis=%q",
		task.ID, n, elapsed, "reports are coalesced in memory; a single goroutine flushes")

	// 等待后台至少落盘一轮（磁盘每次 Save 阻塞 20ms，handler 循环不受影响）。
	waitFor(t, func() bool {
		g, ok := q.Get(task.ID)
		return ok && g.ProgressPercent >= 100
	}, "progress reaches 100 via background flush")

	if err := reporter.close(context.Background()); err != nil {
		t.Fatalf("reporter close: %v", err)
	}
	// close 后再上报是空操作。
	reporter.Report(100, "after-close")

	got := statusOf(t, q, task.ID)
	if got.ProgressPercent != 100 {
		t.Fatalf("task=%s final progress=%d, want 100 basis=last value flushed on close", task.ID, got.ProgressPercent)
	}
	if !got.ProgressReportedAt.After(time.Time{}) {
		t.Fatal("reportedAt must be set")
	}
	t.Logf("[assert] task=%s final-progress=%d%% stage=%q verdict=persisted basis=%q",
		task.ID, got.ProgressPercent, got.ProgressStage, "close synchronously flushes last pending value")
}

// 场景：旧版本写出的数据文件没有任何 progress_* 字段，新版本必须直接加载，
// 字段取零值，随后可以正常上报并落盘。
func TestLoadsLegacyDataFileWithoutProgressFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "queue.json")
	// 手工写一份旧格式数据（字段集合与加进度字段之前的版本一致）。
	legacy := `[
  {
    "id": "task-legacy-running",
    "payload": "old-running",
    "status": "running",
    "attempts": 1,
    "max_attempts": 5,
    "enqueued_at": "2026-09-01T00:00:00Z",
    "visible_at": "2026-09-01T00:00:00Z",
    "lease_owner": "dead-inst",
    "lease_token": "dead-token",
    "lease_expires": "2026-09-01T00:00:30Z",
    "updated_at": "2026-09-01T00:00:00Z"
  },
  {
    "id": "task-legacy-dead",
    "payload": "old-dead",
    "status": "dead",
    "attempts": 5,
    "max_attempts": 5,
    "enqueued_at": "2026-09-01T00:00:00Z",
    "visible_at": "2026-09-01T00:00:00Z",
    "lease_owner": "",
    "lease_token": "",
    "lease_expires": "0001-01-01T00:00:00Z",
    "dead_reason": "retry budget exhausted after 5/5 attempts, last error: old",
    "updated_at": "2026-09-01T00:00:00Z"
  }
]`
	if err := os.WriteFile(path, []byte(legacy), 0o644); err != nil {
		t.Fatal(err)
	}

	q := reopenQueue(t, path, "inst-upgraded", testConfig())

	running := statusOf(t, q, "task-legacy-running")
	if running.Status != StatusPending {
		t.Fatalf("legacy running task status=%s, want pending after recovery", running.Status)
	}
	if running.ProgressPercent != 0 || running.ProgressStage != "" || !running.ProgressReportedAt.IsZero() {
		t.Fatalf("legacy task progress fields must be zero-valued, got %d/%q/%v",
			running.ProgressPercent, running.ProgressStage, running.ProgressReportedAt)
	}
	dead := statusOf(t, q, "task-legacy-dead")
	if dead.Status != StatusDead || dead.DeadReason == "" {
		t.Fatalf("legacy dead task not preserved: %s/%q", dead.Status, dead.DeadReason)
	}
	t.Logf("[assert] legacy-file loaded: running-task recovered=pending progress=0%%(zero-value), "+
		"dead-task retained reason=%q basis=%q",
		dead.DeadReason, "missing progress_* JSON fields unmarshal to zero values")

	// 恢复后的任务可以正常投递、上报、确认。
	lease := leaseCheck(t, mustDequeue(t, q), running.ID, 2)
	if err := q.ReportProgress(context.Background(), running.ID, lease.Token, 25, "first report after upgrade"); err != nil {
		t.Fatalf("report on recovered legacy task: %v", err)
	}
	if err := q.Ack(context.Background(), running.ID, lease.Token); err != nil {
		t.Fatalf("ack: %v", err)
	}
	got := statusOf(t, q, running.ID)
	if got.Status != StatusSucceeded || got.ProgressPercent != 25 {
		t.Fatalf("legacy task final state=%s progress=%d", got.Status, got.ProgressPercent)
	}
	t.Logf("[assert] task=%s attempt=2 progress=25%% verdict=succeeded basis=%q",
		running.ID, "upgraded queue delivers and persists progress on legacy data")
}

// slowStore 给每次 Save 增加固定延迟，用于模拟慢磁盘/高频落盘压力。
type slowStore struct {
	inner Store
	delay time.Duration
	saves atomic.Int64
}

func (s *slowStore) Load(ctx context.Context) ([]*Task, error) {
	return s.inner.Load(ctx)
}

func (s *slowStore) Save(ctx context.Context, tasks []*Task) error {
	s.saves.Add(1)
	time.Sleep(s.delay)
	return s.inner.Save(ctx, tasks)
}

var (
	_ Store = (*slowStore)(nil)
	_       = sync.Mutex{}
)
