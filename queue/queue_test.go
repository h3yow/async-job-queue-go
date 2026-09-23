package queue

import (
	"context"
	"errors"
	"testing"
	"time"
)

func testConfig() Config {
	return Config{
		MaxAttempts:       3,
		LeaseDuration:     time.Hour, // 默认租约很长，超时场景单独构造短租约
		BaseBackoff:       time.Millisecond,
		MaxBackoff:        10 * time.Millisecond,
		BackoffMultiplier: 2,
		Jitter:            false,
	}
}

func newTestQueue(t *testing.T) *Queue {
	t.Helper()
	q, err := New(testConfig(), NewFileStore(t.TempDir()+"/queue.json"), "inst-1")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return q
}

// 场景1：基本投递 + ack 终态 + 重复 ack 被拒（旧令牌不得二次生效）。
func TestEnqueueDequeueAck(t *testing.T) {
	q := newTestQueue(t)
	task := mustEnqueue(t, q, "hello")

	lease := leaseCheck(t, mustDequeue(t, q), task.ID, 1)

	if err := q.Ack(t.Context(), task.ID, lease.Token); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	got := statusOf(t, q, task.ID)
	if got.Status != StatusSucceeded {
		t.Fatalf("task=%s status=%s, want succeeded basis=ack with matching token",
			task.ID, got.Status)
	}
	t.Logf("[assert] task=%s attempts=%d status=succeeded basis=%q",
		task.ID, got.Attempts, "ack token matched -> terminal succeeded")

	// succeeded 任务永远不会再被投递。
	if l, err := q.Dequeue(t.Context()); err != nil || l != nil {
		t.Fatalf("succeeded task must not be redelivered, got lease=%v err=%v", l, err)
	}
	// 用旧令牌重复 ack 必须被拒绝，且状态保持终态。
	err := q.Ack(t.Context(), task.ID, lease.Token)
	if !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("duplicate ack err=%v, want ErrInvalidLease", err)
	}
	t.Logf("[assert] task=%s duplicate-ack verdict=rejected basis=%q",
		task.ID, "task no longer running / token not active")
	if statusOf(t, q, task.ID).Status != StatusSucceeded {
		t.Fatalf("status changed by rejected ack")
	}
}

// 场景2：可重试失败在 MaxAttempts 次后进入死信，退避期间不可投递，死信保留原因且不再被取出。
func TestRetryExhaustionEntersDeadLetter(t *testing.T) {
	q := newTestQueue(t)
	task := mustEnqueue(t, q, "flaky")

	// 第 1、2 次可重试失败 -> pending + 退避；第 3 次失败 -> dead。
	for attempt := 1; attempt <= 3; attempt++ {
		lease := leaseCheck(t, mustDequeueAfterBackoff(t, q), task.ID, attempt)
		beforeNack := time.Now()
		err := q.Nack(t.Context(), task.ID, lease.Token,
			Retryable(errors.New("simulated transient error")))
		if err != nil {
			t.Fatalf("Nack attempt=%d: %v", attempt, err)
		}
		got := statusOf(t, q, task.ID)
		if attempt < 3 {
			if got.Status != StatusPending {
				t.Fatalf("task=%s after attempt=%d status=%s, want pending basis=retryable",
					task.ID, attempt, got.Status)
			}
			if !got.VisibleAt.After(beforeNack) {
				t.Fatalf("task=%s backoff VisibleAt=%s should be after nack moment %s",
					task.ID, got.VisibleAt, beforeNack)
			}
			t.Logf("[assert] task=%s attempt=%d verdict=retry-scheduled visibleAt=%s basis=%q",
				task.ID, attempt, got.VisibleAt.Format(time.RFC3339Nano),
				"retryable failure and attempts < max -> backoff")
		} else {
			if got.Status != StatusDead {
				t.Fatalf("task=%s status=%s, want dead basis=budget exhausted",
					task.ID, got.Status)
			}
			if got.DeadReason == "" {
				t.Fatalf("dead task must retain reason")
			}
			t.Logf("[assert] task=%s attempt=%d verdict=dead reason=%q basis=%q",
				task.ID, attempt, got.DeadReason,
				"retryable failure and attempts == max -> dead letter")
		}
	}

	// 死信任务无论等多久都不会再被投递。
	if l, err := q.Dequeue(t.Context()); err != nil || l != nil {
		t.Fatalf("dead letter must not be delivered, got lease=%v err=%v", l, err)
	}
	t.Logf("[assert] task=%s verdict=never-redelivered basis=%q",
		task.ID, "dead is terminal and excluded from dequeue candidates")
}

// 场景3：不可恢复失败首次即进入死信，不消耗重试预算。
func TestNonRetryableGoesStraightToDead(t *testing.T) {
	q := newTestQueue(t)
	task := mustEnqueue(t, q, "poison")
	lease := leaseCheck(t, mustDequeue(t, q), task.ID, 1)

	if err := q.Nack(t.Context(), task.ID, lease.Token,
		NonRetryable(errors.New("unparseable payload"))); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	got := statusOf(t, q, task.ID)
	if got.Status != StatusDead || got.Attempts != 1 {
		t.Fatalf("status=%s attempts=%d, want dead/1 basis=non-retryable on first attempt",
			got.Status, got.Attempts)
	}
	t.Logf("[assert] task=%s attempts=%d verdict=dead reason=%q basis=%q",
		task.ID, got.Attempts, got.DeadReason, "non-retryable failure -> immediate dead letter")
}

// 场景4：租约超时后任务可被重新投递；旧持有者的过期 ack/nack 被拒，
// 且不会影响新租约（同一任务不会被两个有效持有者同时处理成功）。
func TestLeaseExpiryRedelivery(t *testing.T) {
	cfg := testConfig()
	cfg.LeaseDuration = 30 * time.Millisecond
	path := t.TempDir() + "/queue.json"
	q, err := New(cfg, NewFileStore(path), "inst-1")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	task := mustEnqueue(t, q, "slow-handler")

	lease1 := leaseCheck(t, mustDequeue(t, q), task.ID, 1)
	// 租约窗口内不可重复投递。
	if l, _ := q.Dequeue(t.Context()); l != nil {
		t.Fatalf("task=%s delivered twice within visibility window", task.ID)
	}
	t.Logf("[assert] task=%s verdict=not-redelivered basis=%q",
		task.ID, "lease still valid within visibility window")

	time.Sleep(45 * time.Millisecond)
	// 超时后重新投递，Attempts 递增。
	lease2 := leaseCheck(t, mustDequeue(t, q), task.ID, 2)
	if lease1.Token == lease2.Token {
		t.Fatalf("redelivery must issue a new lease token")
	}

	// 旧持有者迟到的 ack/nack 一律拒绝。
	if err := q.Ack(t.Context(), task.ID, lease1.Token); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("stale ack err=%v, want ErrInvalidLease basis=old token superseded", err)
	}
	t.Logf("[assert] task=%s stale-ack-from-attempt=1 verdict=rejected basis=%q",
		task.ID, "token rotated after redelivery")

	// 新持有者确认成功，任务终态只可能有一个确定结论。
	if err := q.Ack(t.Context(), task.ID, lease2.Token); err != nil {
		t.Fatalf("Ack with fresh token: %v", err)
	}
	got := statusOf(t, q, task.ID)
	if got.Status != StatusSucceeded || got.Attempts != 2 {
		t.Fatalf("status=%s attempts=%d, want succeeded/2", got.Status, got.Attempts)
	}
	t.Logf("[assert] task=%s attempts=%d verdict=succeeded basis=%q",
		task.ID, got.Attempts, "fresh lease token after visibility timeout acked")
}

// 场景5：退避到期前不可投递，到期后可投递。
func TestBackoffHidesTaskUntilVisible(t *testing.T) {
	cfg := testConfig()
	cfg.BaseBackoff = 80 * time.Millisecond
	cfg.MaxBackoff = 80 * time.Millisecond
	q, err := New(cfg, NewFileStore(t.TempDir()+"/queue.json"), "inst-1")
	if err != nil {
		t.Fatal(err)
	}
	task := mustEnqueue(t, q, "later")
	lease := leaseCheck(t, mustDequeue(t, q), task.ID, 1)
	if err := q.Nack(t.Context(), task.ID, lease.Token, Retryable(errors.New("x"))); err != nil {
		t.Fatal(err)
	}
	if l, _ := q.Dequeue(t.Context()); l != nil {
		t.Fatalf("task=%s delivered before backoff VisibleAt", task.ID)
	}
	time.Sleep(90 * time.Millisecond)
	leaseCheck(t, mustDequeueAfterBackoff(t, q), task.ID, 2)
}

// 场景6：空队列 Dequeue 返回 nil 而非错误或 panic。
func TestDequeueEmpty(t *testing.T) {
	q := newTestQueue(t)
	l, err := q.Dequeue(t.Context())
	if err != nil || l != nil {
		t.Fatalf("empty queue: lease=%v err=%v, want nil,nil", l, err)
	}
}

func mustDequeue(t *testing.T, q *Queue) *Lease {
	t.Helper()
	lease, err := q.Dequeue(t.Context())
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	return lease
}

// mustDequeueAfterBackoff 轮询等待退避到期并取到任务。
func mustDequeueAfterBackoff(t *testing.T, q *Queue) *Lease {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	for {
		lease, err := q.Dequeue(ctx)
		if err != nil {
			t.Fatalf("Dequeue: %v", err)
		}
		if lease != nil {
			return lease
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for redelivery after backoff: %v", ctx.Err())
		case <-time.After(2 * time.Millisecond):
		}
	}
}
