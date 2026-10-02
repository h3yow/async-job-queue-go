package queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"sync"
	"time"
)

// 常见错误，调用方可用 errors.Is 判定。
var (
	// ErrTaskNotFound 任务不存在。
	ErrTaskNotFound = errors.New("task not found")
	// ErrInvalidLease 租约无效：任务不在 running 状态，或令牌不匹配
	//（过期后被重新投递给他人、或重复 ack/nack 时返回）。
	ErrInvalidLease = errors.New("invalid or expired lease")
	// ErrInvalidProgress 进度参数非法：百分比超出 [0,100]。
	ErrInvalidProgress = errors.New("invalid progress")
	// ErrProgressRegressed 进度倒退：上报百分比低于已持久化的值（含重启恢复后），拒绝。
	ErrProgressRegressed = errors.New("progress would regress")
)

// Config 队列配置。
type Config struct {
	MaxAttempts       int           // 最大投递次数，重试耗尽进入死信
	LeaseDuration     time.Duration // 可见性窗口/租约时长
	BaseBackoff       time.Duration // 第 1 次失败后的初始退避
	MaxBackoff        time.Duration // 退避上限
	BackoffMultiplier float64       // 退避倍率（指数退避）
	Jitter            bool          // 是否对退避加 [0.5,1.5) 的随机抖动
}

// DefaultConfig 默认配置：最多 5 次投递、30 秒租约、指数退避 1s 起、上限 30s、无抖动。
func DefaultConfig() Config {
	return Config{
		MaxAttempts:       5,
		LeaseDuration:     30 * time.Second,
		BaseBackoff:       time.Second,
		MaxBackoff:        30 * time.Second,
		BackoffMultiplier: 2,
		Jitter:            false,
	}
}

func (c *Config) normalize() {
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 5
	}
	if c.LeaseDuration <= 0 {
		c.LeaseDuration = 30 * time.Second
	}
	if c.BaseBackoff <= 0 {
		c.BaseBackoff = time.Second
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = 30 * time.Second
	}
	if c.BackoffMultiplier <= 1 {
		c.BackoffMultiplier = 2
	}
}

// Lease 表示一次成功投递的租约凭据；ack/nack 必须带回 Token。
type Lease struct {
	Task  *Task
	Token string
}

// Queue 本地持久化异步任务队列。
// 单进程内以互斥锁串行化状态变更，每次变更全量快照原子落盘，
// 因此 Dequeue/Ack/Nack 一旦成功返回，对应状态对崩溃重启即有定义。
type Queue struct {
	cfg        Config
	store      Store
	instanceID string

	mu    sync.Mutex
	tasks map[string]*Task
}

// New 创建队列并从存储恢复历史状态：
// 上次进程遗留的 running 任务（崩溃或被杀死，未走优雅关闭）
// 一律退回 pending，租约清空、立即可重新投递；终态任务原样保留。
func New(cfg Config, store Store, instanceID string) (*Queue, error) {
	cfg.normalize()
	if store == nil {
		return nil, errors.New("nil store")
	}
	if instanceID == "" {
		instanceID = newToken("inst")
	}
	q := &Queue{
		cfg:        cfg,
		store:      store,
		instanceID: instanceID,
		tasks:      make(map[string]*Task),
	}
	loaded, err := store.Load(context.Background())
	if err != nil {
		return nil, err
	}
	now := time.Now()
	recovered := 0
	for _, t := range loaded {
		if t.Status == StatusRunning {
			t.Status = StatusPending
			t.LeaseOwner = ""
			t.LeaseToken = ""
			t.LeaseExpires = time.Time{}
			t.VisibleAt = now
			t.UpdatedAt = now
			recovered++
		}
		q.tasks[t.ID] = t
	}
	if recovered > 0 {
		if err := q.persistLocked(context.Background()); err != nil {
			return nil, fmt.Errorf("persist recovered state: %w", err)
		}
	}
	return q, nil
}

// InstanceID 返回本队列实例标识（即租约 owner 标识）。
func (q *Queue) InstanceID() string { return q.instanceID }

// Enqueue 任务入队，状态为 pending 且立即可投递。
func (q *Queue) Enqueue(ctx context.Context, payload string) (*Task, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
	t := &Task{
		ID:          newToken("task"),
		Payload:     payload,
		Status:      StatusPending,
		Attempts:    0,
		MaxAttempts: q.cfg.MaxAttempts,
		EnqueuedAt:  now,
		VisibleAt:   now,
		UpdatedAt:   now,
	}
	q.tasks[t.ID] = t
	if err := q.persistLocked(ctx); err != nil {
		delete(q.tasks, t.ID)
		return nil, err
	}
	return t.Clone(), nil
}

// Dequeue 拉取一条可投递任务：
//   - pending 且 VisibleAt 已到（含新任务和退避到期任务）；
//   - running 但租约已超时（消费者宕机/超时未 ack），重新投递给当前实例。
//
// 没有可用任务时返回 (nil, nil)。投递成功会立即持久化，
// 保证"已投递"这一事实在崩溃后可追溯（Attempts 只增不减）。
func (q *Queue) Dequeue(ctx context.Context) (*Lease, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
	candidates := make([]*Task, 0, 1)
	for _, t := range q.tasks {
		switch t.Status {
		case StatusPending:
			if !t.VisibleAt.After(now) {
				candidates = append(candidates, t)
			}
		case StatusRunning:
			if !t.LeaseExpires.After(now) {
				candidates = append(candidates, t)
			}
		}
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	// 确定性选择：按"最早具备投递资格"排序，行为可预期、便于测试。
	sort.Slice(candidates, func(i, j int) bool {
		ti, tj := candidates[i], candidates[j]
		ei, ej := eligibleAt(ti), eligibleAt(tj)
		if !ei.Equal(ej) {
			return ei.Before(ej)
		}
		return ti.ID < tj.ID
	})
	t := candidates[0]

	t.Status = StatusRunning
	t.Attempts++
	t.LeaseOwner = q.instanceID
	t.LeaseToken = newToken("lease")
	t.LeaseExpires = now.Add(q.cfg.LeaseDuration)
	t.UpdatedAt = now
	if err := q.persistLocked(ctx); err != nil {
		// 持久化失败：内存中的投递不落盘，交由下一轮重试。
		t.Status = StatusPending
		t.Attempts--
		t.LeaseOwner = ""
		t.LeaseToken = ""
		t.LeaseExpires = time.Time{}
		t.UpdatedAt = now
		return nil, err
	}
	return &Lease{Task: t.Clone(), Token: t.LeaseToken}, nil
}

// Ack 确认任务完成。仅当任务仍处于 running 且令牌匹配当前持有者时成功，
// 随后进入 succeeded 终态并持久化。令牌不匹配（租约超时后已重新投递、
// 重复确认等）返回 ErrInvalidLease，且不会改动任务当前状态。
func (q *Queue) Ack(ctx context.Context, taskID, token string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	t, ok := q.tasks[taskID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
	}
	if t.Status != StatusRunning || t.LeaseToken != token {
		return fmt.Errorf("%w: task=%s status=%s", ErrInvalidLease, taskID, t.Status)
	}

	now := time.Now()
	t.Status = StatusSucceeded
	t.LeaseOwner = ""
	t.LeaseToken = ""
	t.LeaseExpires = time.Time{}
	t.UpdatedAt = now
	return q.persistLocked(ctx)
}

// Nack 报告任务处理失败：
//   - 不可恢复失败：立即进入死信；
//   - 可重试失败且 Attempts 已达 MaxAttempts：进入死信，记录重试耗尽原因；
//   - 否则退回 pending，按指数退避设置 VisibleAt。
//
// 死信任务保留 DeadReason（失败类型、次数、最后一条错误信息），可追溯，
// 且 Dequeue 永远不会再选中它。
func (q *Queue) Nack(ctx context.Context, taskID, token string, taskErr *TaskError) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	t, ok := q.tasks[taskID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
	}
	if t.Status != StatusRunning || t.LeaseToken != token {
		return fmt.Errorf("%w: task=%s status=%s", ErrInvalidLease, taskID, t.Status)
	}
	if taskErr == nil {
		taskErr = Retryable(errors.New("nack without error"))
	}

	now := time.Now()
	reason := ""
	if taskErr.Err != nil {
		reason = taskErr.Err.Error()
	}

	switch {
	case taskErr.Kind == FailureNonRetryable:
		q.markDeadLocked(t, now, fmt.Sprintf(
			"non-retryable failure on attempt %d/%d: %s",
			t.Attempts, t.MaxAttempts, reason))
	case t.Attempts >= t.MaxAttempts:
		q.markDeadLocked(t, now, fmt.Sprintf(
			"retry budget exhausted after %d/%d attempts, last error: %s",
			t.Attempts, t.MaxAttempts, reason))
	default:
		backoff := q.backoff(t.Attempts)
		t.Status = StatusPending
		t.LeaseOwner = ""
		t.LeaseToken = ""
		t.LeaseExpires = time.Time{}
		t.VisibleAt = now.Add(backoff)
		t.UpdatedAt = now
	}
	return q.persistLocked(ctx)
}

// ReportProgress 由当前租约持有者上报执行进度，并在成功返回前原子落盘：
//   - 任务必须处于 running 且 token 为当前租约令牌；任务被重新投递（租约超时、
//     重启恢复、关闭退回）后，上一轮旧执行持有的旧令牌一律返回 ErrInvalidLease，
//     其迟到上报不会覆盖新一轮的进度；
//   - percent 必须落在 [0,100]，否则返回 ErrInvalidProgress；
//   - 进度百分比单调只增：percent 低于已持久化值（包括进程重启后读到的值）时
//     返回 ErrProgressRegressed，任务状态不变，重启不会让进度倒退；
//   - percent 相等的上报允许通过，用于只更新阶段文字；stage 原样记录。
//
// 落盘成功后 UpdatedAt/ProgressReportedAt 都会更新。Get/Snapshot 读到的
// 进度与数据文件中的内容一致（未被接受的上报不会留在内存态里）。
func (q *Queue) ReportProgress(ctx context.Context, taskID, token string, percent int, stage string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	t, ok := q.tasks[taskID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
	}
	if t.Status != StatusRunning || t.LeaseToken != token {
		return fmt.Errorf("%w: task=%s status=%s", ErrInvalidLease, taskID, t.Status)
	}
	if percent < 0 || percent > 100 {
		return fmt.Errorf("%w: task=%s percent=%d", ErrInvalidProgress, taskID, percent)
	}
	if percent < t.ProgressPercent {
		return fmt.Errorf("%w: task=%s reported=%d persisted=%d",
			ErrProgressRegressed, taskID, percent, t.ProgressPercent)
	}

	now := time.Now()
	t.ProgressPercent = percent
	t.ProgressStage = stage
	t.ProgressReportedAt = now
	t.UpdatedAt = now
	return q.persistLocked(ctx)
}

// RequeueOwned 在优雅关闭时把本实例仍持有的 running 任务全部退回 pending，
// 清空租约并立即可被重新投递（退避为 0），Attempts 保留不回退。
// 返回退回的任务数。其他实例持有的租约不受影响。
func (q *Queue) RequeueOwned(ctx context.Context) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
	n := 0
	for _, t := range q.tasks {
		if t.Status == StatusRunning && t.LeaseOwner == q.instanceID {
			t.Status = StatusPending
			t.LeaseOwner = ""
			t.LeaseToken = ""
			t.LeaseExpires = time.Time{}
			t.VisibleAt = now
			t.UpdatedAt = now
			n++
		}
	}
	if n == 0 {
		return 0, nil
	}
	return n, q.persistLocked(ctx)
}

// Get 按 ID 返回任务快照，不存在返回 (nil, false)。
func (q *Queue) Get(taskID string) (*Task, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	t, ok := q.tasks[taskID]
	return t.Clone(), ok
}

// Snapshot 返回全部任务的深拷贝（按 ID 排序），供测试断言与可观测使用。
func (q *Queue) Snapshot() []*Task {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]*Task, 0, len(q.tasks))
	for _, t := range q.tasks {
		out = append(out, t.Clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// markDeadLocked 将任务置为死信终态并记录原因。
func (q *Queue) markDeadLocked(t *Task, now time.Time, reason string) {
	t.Status = StatusDead
	t.DeadReason = reason
	t.LeaseOwner = ""
	t.LeaseToken = ""
	t.LeaseExpires = time.Time{}
	t.UpdatedAt = now
}

// backoff 计算第 attempt 次失败（attempt 从 1 起）后的退避时长。
func (q *Queue) backoff(attempt int) time.Duration {
	d := float64(q.cfg.BaseBackoff) * math.Pow(q.cfg.BackoffMultiplier, float64(attempt-1))
	if d > float64(q.cfg.MaxBackoff) || math.IsInf(d, 0) {
		d = float64(q.cfg.MaxBackoff)
	}
	dur := time.Duration(d)
	if q.cfg.Jitter {
		// [0.5, 1.5) 倍抖动，避免大量任务同时重试形成雷鸣群。
		if n, err := rand.Int(rand.Reader, big.NewInt(1000)); err == nil {
			factor := 0.5 + float64(n.Int64())/1000.0
			dur = time.Duration(float64(dur) * factor)
		}
	}
	if dur < 0 {
		dur = 0
	}
	return dur
}

// persistLocked 在持锁状态下把全量快照原子落盘。
func (q *Queue) persistLocked(ctx context.Context) error {
	snapshot := make([]*Task, 0, len(q.tasks))
	for _, t := range q.tasks {
		snapshot = append(snapshot, t)
	}
	sort.Slice(snapshot, func(i, j int) bool { return snapshot[i].ID < snapshot[j].ID })
	return q.store.Save(ctx, snapshot)
}

// eligibleAt 返回任务最早具备投递资格的时刻，仅用于候选排序。
func eligibleAt(t *Task) time.Time {
	if t.Status == StatusRunning {
		return t.LeaseExpires
	}
	return t.VisibleAt
}

// newToken 生成带前缀的随机十六进制标识。
func newToken(prefix string) string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		// rand.Read 极少失败；退化为时间戳保证唯一性不回落到固定值。
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return prefix + "-" + hex.EncodeToString(buf)
}
