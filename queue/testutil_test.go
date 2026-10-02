package queue

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

// testLogger 把运行日志写入 testing.T，单测日志中可见任务标识、投递次数与判定依据。
type testLogger struct{ t *testing.T }

func newTestLogger(t *testing.T) *testLogger {
	t.Helper()
	return &testLogger{t: t}
}

func (l *testLogger) Printf(format string, args ...any) {
	l.t.Logf("[queue-log] "+format, args...)
}

// mustEnqueue 入队一条任务并在失败时终止测试，返回任务快照。
func mustEnqueue(t *testing.T, q *Queue, payload string) *Task {
	t.Helper()
	task, err := q.Enqueue(t.Context(), payload)
	if err != nil {
		t.Fatalf("Enqueue(%q): %v", payload, err)
	}
	t.Logf("[action] enqueue task=%s payload=%q basis=%q",
		task.ID, payload, "new task persisted as pending")
	return task
}

// leaseCheck 断言一次 Dequeue 的结果。
func leaseCheck(t *testing.T, lease *Lease, wantID string, wantAttempt int) *Lease {
	t.Helper()
	if lease == nil {
		t.Fatalf("Dequeue returned no lease, want task=%s attempt=%d", wantID, wantAttempt)
	}
	if lease.Task.ID != wantID {
		t.Fatalf("Dequeue task=%s, want %s", lease.Task.ID, wantID)
	}
	if lease.Task.Attempts != wantAttempt {
		t.Fatalf("task=%s attempts=%d, want %d", wantID, lease.Task.Attempts, wantAttempt)
	}
	t.Logf("[action] deliver task=%s attempt=%d verdict=delivered basis=%q",
		wantID, wantAttempt, "pending visible / lease expired")
	return lease
}

func statusOf(t *testing.T, q *Queue, id string) *Task {
	t.Helper()
	task, ok := q.Get(id)
	if !ok {
		t.Fatalf("task %s not found", id)
	}
	return task
}

// failCountLogger 同时记录日志条数（用于验证消费过程确实输出了判定依据）。
// mu 保护 lines：日志由 worker goroutine 写入，断言由测试 goroutine 读取。
type failCountLogger struct {
	t     *testing.T
	mu    sync.Mutex
	lines []string
}

func (l *failCountLogger) Printf(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	l.mu.Lock()
	l.lines = append(l.lines, line)
	l.mu.Unlock()
	l.t.Logf("[queue-log] %s", line)
}

// snapshotLines 返回已记录日志的拷贝。
func (l *failCountLogger) snapshotLines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.lines...)
}

// linesContain 报告已记录日志中是否存在包含 sub 的行。
func (l *failCountLogger) linesContain(sub string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		if strings.Contains(line, sub) {
			return true
		}
	}
	return false
}
