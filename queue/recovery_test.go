package queue

import (
	"context"
	"errors"
	"testing"
	"time"
)

// 模拟"进程崩溃后重启"：不调用优雅关闭，直接用同一数据文件 New 一个新队列。
func reopenQueue(t *testing.T, path, instanceID string, cfg Config) *Queue {
	t.Helper()
	q, err := New(cfg, NewFileStore(path), instanceID)
	if err != nil {
		t.Fatalf("reopen queue: %v", err)
	}
	return q
}

// 场景：投递后崩溃（running 未落 ack）-> 重启后任务重新可投递，
// 且不会出现静默丢失；投递次数在重投时递增。
func TestRestartRecoversUnfinishedRunningTask(t *testing.T) {
	path := t.TempDir() + "/queue.json"
	cfg := testConfig()

	q := reopenQueue(t, path, "inst-A", cfg)
	task := mustEnqueue(t, q, "will-crash")
	leaseCheck(t, mustDequeue(t, q), task.ID, 1)

	// 进程在这里"崩溃"：没有 Ack，也没有 RequeueOwned。
	// 用新实例重开（等价于进程重启）。
	q2 := reopenQueue(t, path, "inst-B", cfg)
	got := statusOf(t, q2, task.ID)
	if got.Status != StatusPending {
		t.Fatalf("task=%s after restart status=%s, want pending basis=orphan running reset",
			task.ID, got.Status)
	}
	if got.LeaseOwner != "" || got.LeaseToken != "" {
		t.Fatalf("task=%s stale lease survived restart: owner=%q", task.ID, got.LeaseOwner)
	}
	t.Logf("[assert] task=%s after-restart status=pending attempts=%d basis=%q",
		task.ID, got.Attempts, "orphan running from crashed process reset to pending")

	// 重启后立即可以重新投递；Attempts 保留为 1，下一次投递变 2。
	lease2 := leaseCheck(t, mustDequeue(t, q2), task.ID, 2)
	if err := q2.Ack(context.Background(), task.ID, lease2.Token); err != nil {
		t.Fatalf("Ack after restart: %v", err)
	}
	got = statusOf(t, q2, task.ID)
	if got.Status != StatusSucceeded {
		t.Fatalf("task=%s status=%s want succeeded", task.ID, got.Status)
	}
	t.Logf("[assert] task=%s after-restart-redelivery verdict=succeeded attempts=%d basis=%q",
		task.ID, got.Attempts, "redelivered and acked by new instance")
}

// 场景：重启后 succeeded 与 dead 终态原样保留（已确认完成的任务不得丢失/重投），
// 死信原因同样保留。
func TestRestartPreservesTerminalStates(t *testing.T) {
	path := t.TempDir() + "/queue.json"
	cfg := testConfig()
	q := reopenQueue(t, path, "inst-A", cfg)

	done := mustEnqueue(t, q, "done")
	l1 := mustDequeue(t, q)
	if err := q.Ack(context.Background(), done.ID, l1.Token); err != nil {
		t.Fatal(err)
	}

	dead := mustEnqueue(t, q, "dead")
	l2 := mustDequeue(t, q)
	if err := q.Nack(context.Background(), dead.ID, l2.Token,
		NonRetryable(errors.New("permanent bad data"))); err != nil {
		t.Fatal(err)
	}

	pending := mustEnqueue(t, q, "pending-job")

	// 重启。
	q2 := reopenQueue(t, path, "inst-B", cfg)

	if g := statusOf(t, q2, done.ID); g.Status != StatusSucceeded {
		t.Fatalf("done task status after restart=%s, want succeeded", g.Status)
	}
	t.Logf("[assert] task=%s after-restart status=succeeded basis=%q",
		done.ID, "terminal state persisted on disk before crash")

	g := statusOf(t, q2, dead.ID)
	if g.Status != StatusDead || g.DeadReason == "" {
		t.Fatalf("dead task after restart status=%s reason=%q, want dead with reason",
			g.Status, g.DeadReason)
	}
	t.Logf("[assert] task=%s after-restart status=dead reason=%q basis=%q",
		dead.ID, g.DeadReason, "dead letter and its traceable reason persisted")

	if g := statusOf(t, q2, pending.ID); g.Status != StatusPending {
		t.Fatalf("pending task after restart status=%s", g.Status)
	}

	// 队列里恰好只有 pending-job 一条可投递任务，死信与完成任务不被取出。
	lease := mustDequeue(t, q2)
	if lease.Task.ID != pending.ID {
		t.Fatalf("first delivery after restart=%s, want %s", lease.Task.ID, pending.ID)
	}
	if l, _ := q2.Dequeue(context.Background()); l != nil {
		t.Fatalf("no further deliverable tasks, terminal tasks must stay hidden")
	}
	t.Logf("[assert] task=%s delivered-after-restart attempt=%d basis=%q; "+
		"succeeded/dead not redelivered",
		pending.ID, lease.Task.Attempts, "only pending tasks are candidates")
}

// 场景：优雅关闭把未完成任务退回 pending，立即用新实例接管即可继续消费，无需等租约超时。
func TestGracefulShutdownRequeuesOwnedLeases(t *testing.T) {
	path := t.TempDir() + "/queue.json"
	cfg := testConfig()
	q := reopenQueue(t, path, "inst-A", cfg)
	task := mustEnqueue(t, q, "inflight")
	leaseCheck(t, mustDequeue(t, q), task.ID, 1)

	n, err := q.RequeueOwned(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("RequeueOwned n=%d err=%v, want 1,nil", n, err)
	}
	got := statusOf(t, q, task.ID)
	if got.Status != StatusPending || got.LeaseToken != "" {
		t.Fatalf("task=%s status=%s token=%q, want pending with empty lease",
			task.ID, got.Status, got.LeaseToken)
	}
	t.Logf("[assert] task=%s shutdown verdict=pending attempts=%d basis=%q",
		task.ID, got.Attempts, "graceful shutdown requeues owned unfinished lease immediately")

	// 新进程立刻接管，不需要等待一个租约周期。
	q2 := reopenQueue(t, path, "inst-B", cfg)
	lease2 := leaseCheck(t, mustDequeue(t, q2), task.ID, 2)
	if err := q2.Ack(context.Background(), task.ID, lease2.Token); err != nil {
		t.Fatal(err)
	}
	t.Logf("[assert] task=%s taken-over-by=inst-B verdict=succeeded basis=%q",
		task.ID, "requeued task visible immediately on restart")
}

// 场景：关闭前已 Ack 的任务不会被 RequeueOwned 退回（结论唯一，不重复处理）。
func TestGracefulShutdownKeepsAckedTask(t *testing.T) {
	q := newTestQueue(t)
	task := mustEnqueue(t, q, "finished")
	lease := leaseCheck(t, mustDequeue(t, q), task.ID, 1)
	if err := q.Ack(context.Background(), task.ID, lease.Token); err != nil {
		t.Fatal(err)
	}
	n, err := q.RequeueOwned(context.Background())
	if err != nil || n != 0 {
		t.Fatalf("RequeueOwned n=%d err=%v, want 0,nil basis=acked task is terminal", n, err)
	}
	if statusOf(t, q, task.ID).Status != StatusSucceeded {
		t.Fatal("acked task must stay succeeded after shutdown")
	}
	t.Logf("[assert] task=%s requeued=0 status=succeeded basis=%q",
		task.ID, "RequeueOwned only touches running leases owned by this instance")
}

// 兜底：time 包在其他测试配置中使用，保持导入一致。
var _ = time.Millisecond
