package queue

import (
	"context"
	"os"
	"testing"
)

// FileStore 在空文件状态下 Load 返回空切片；Save 后数据可读且一致。
func TestFileStoreRoundTrip(t *testing.T) {
	path := t.TempDir() + "/nested/queue.json"
	store := NewFileStore(path)

	tasks, err := store.Load(context.Background())
	if err != nil || len(tasks) != 0 {
		t.Fatalf("initial Load: tasks=%d err=%v, want empty,nil", len(tasks), err)
	}

	q, err := New(testConfig(), store, "inst-store")
	if err != nil {
		t.Fatal(err)
	}
	t1 := mustEnqueue(t, q, "persisted-1")
	t2 := mustEnqueue(t, q, "persisted-2")
	l := mustDequeue(t, q)
	if err := q.Ack(context.Background(), t1.ID, l.Token); err != nil {
		t.Fatal(err)
	}

	// 用全新 store 实例重新读取，模拟另一个进程打开同一文件。
	reloaded, err := NewFileStore(path).Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	byID := map[string]*Task{}
	for _, tk := range reloaded {
		byID[tk.ID] = tk
	}
	if g := byID[t1.ID]; g == nil || g.Status != StatusSucceeded {
		t.Fatalf("t1 on disk: %+v, want succeeded basis=ack persisted before reopen", g)
	}
	if g := byID[t2.ID]; g == nil || g.Status != StatusPending {
		t.Fatalf("t2 on disk: %+v, want pending", g)
	}
	t.Logf("[assert] disk snapshot contains task=%s(succeeded), task=%s(pending) basis=%q",
		t1.ID, t2.ID, "full snapshot atomically written after every state change")
}

// Save 不应留下临时文件（原子替换后目录整洁）。
func TestFileStoreNoLeftoverTempFiles(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/queue.json"
	store := NewFileStore(path)
	if err := store.Save(context.Background(), []*Task{}); err != nil {
		t.Fatal(err)
	}
	entries, err := readDirNames(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range entries {
		if name != "queue.json" {
			t.Fatalf("unexpected leftover file: %s", name)
		}
	}
}

func readDirNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names, nil
}
