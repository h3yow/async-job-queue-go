package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// Store 是本地持久化存储的抽象（用本地文件模拟消息代理）。
type Store interface {
	// Load 读取全部任务记录；数据文件不存在时返回空切片。
	Load(ctx context.Context) ([]*Task, error)
	// Save 以全量快照覆盖方式原子落盘。实现必须保证：
	//   - 写入过程中崩溃不会产生半截文件（临时文件 + rename）；
	//   - Save 成功返回后数据已刷盘（文件与目录均 fsync）。
	Save(ctx context.Context, tasks []*Task) error
}

// FileStore 基于单个 JSON 文件的本地存储。
// 写入流程：写临时文件 -> fsync 文件 -> rename 原子替换 -> fsync 目录。
type FileStore struct {
	path string
}

// NewFileStore 创建文件存储，数据文件不存在时视为空队列。
func NewFileStore(path string) *FileStore {
	return &FileStore{path: path}
}

func (s *FileStore) Load(ctx context.Context) ([]*Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return []*Task{}, nil
		}
		return nil, fmt.Errorf("load queue data: %w", err)
	}
	if len(data) == 0 {
		return []*Task{}, nil
	}
	var tasks []*Task
	if err := json.Unmarshal(data, &tasks); err != nil {
		return nil, fmt.Errorf("parse queue data %q: %w", s.path, err)
	}
	return tasks, nil
}

func (s *FileStore) Save(ctx context.Context, tasks []*Task) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if tasks == nil {
		tasks = []*Task{}
	}
	data, err := json.MarshalIndent(tasks, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal queue data: %w", err)
	}

	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	tmp, err := os.CreateTemp(dir, ".queue-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmp.Name()
	// 任何失败路径都尝试清理临时文件。
	cleanup := func() { _ = os.Remove(tmpName) }

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("fsync temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close temp file: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		cleanup()
		return fmt.Errorf("rename temp file: %w", err)
	}
	// fsync 目录，保证 rename 在掉电后仍然生效。
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

var _ Store = (*FileStore)(nil)
