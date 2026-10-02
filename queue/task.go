package queue

import "time"

// TaskStatus 任务生命周期状态。
type TaskStatus string

const (
	// StatusPending 等待投递：新入队、退避中或被退回的任务都处于该状态。
	StatusPending TaskStatus = "pending"
	// StatusRunning 已投递给某个消费者，处于租约（可见性窗口）保护期内。
	StatusRunning TaskStatus = "running"
	// StatusSucceeded 已被消费者确认完成，终态，不会再被投递。
	StatusSucceeded TaskStatus = "succeeded"
	// StatusDead 死信：重试耗尽或不可恢复失败，终态，不会再被消费取出。
	StatusDead TaskStatus = "dead"
)

// FailureKind 区分失败类型，决定任务是退避重试还是直接进入死信。
type FailureKind string

const (
	// FailureRetryable 可重试失败：未超过 MaxAttempts 时按退避策略重新投递。
	FailureRetryable FailureKind = "retryable"
	// FailureNonRetryable 不可恢复失败：立即进入死信，不再投递。
	FailureNonRetryable FailureKind = "non_retryable"
)

// TaskError 由任务处理器返回，用于声明失败是否可重试及原因。
type TaskError struct {
	Kind FailureKind
	Err  error
}

func (e *TaskError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err != nil {
		return string(e.Kind) + ": " + e.Err.Error()
	}
	return string(e.Kind)
}

// Unwrap 支持 errors.Is/errors.As 访问被包装的底层错误。
func (e *TaskError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// Retryable 构造一个可重试失败。
func Retryable(err error) *TaskError {
	return &TaskError{Kind: FailureRetryable, Err: err}
}

// NonRetryable 构造一个不可恢复失败。
func NonRetryable(err error) *TaskError {
	return &TaskError{Kind: FailureNonRetryable, Err: err}
}

// Progress 长耗时任务的执行进度，随任务记录一起持久化。
// 零值（Percent=0、Message=""、UpdatedAt 为零时间）表示从未上报过；
// 旧版本写出的数据文件没有该字段，加载后即为零值，语义兼容。
type Progress struct {
	Percent   int       `json:"percent"`    // 整体完成百分比，取值 [0,100]（上报时会被钳制到该区间）
	Message   string    `json:"message"`    // 当前阶段的文字说明，如 "导出第 3/8 段"
	UpdatedAt time.Time `json:"updated_at"` // 最近一次成功上报（已落盘）的时刻
}

// Reported 报告该任务是否至少成功上报过一次进度。
func (p Progress) Reported() bool {
	return !p.UpdatedAt.IsZero()
}

// Task 是队列中一条任务的持久化记录（JSON 落盘，进程重启后据此恢复）。
type Task struct {
	ID           string     `json:"id"`
	Payload      string     `json:"payload"`
	Status       TaskStatus `json:"status"`
	Attempts     int        `json:"attempts"`     // 已投递次数（含当前这次）
	MaxAttempts  int        `json:"max_attempts"` // 最大投递次数
	EnqueuedAt   time.Time  `json:"enqueued_at"`
	VisibleAt    time.Time  `json:"visible_at"`    // 退避到期时刻，早于该时刻不投递
	LeaseOwner   string     `json:"lease_owner"`   // 当前租约持有者（消费者实例ID）
	LeaseToken   string     `json:"lease_token"`   // 租约令牌，ack/nack 时校验归属
	LeaseExpires time.Time  `json:"lease_expires"` // 租约超时时刻
	DeadReason   string     `json:"dead_reason"`   // 进入死信的可追溯原因
	// Progress 最后一次成功上报的进度。任务进入 succeeded/dead 终态后保留，
	// 用于事后排查；重启后从数据文件原样恢复，不会回退。
	Progress  Progress  `json:"progress"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Terminal 报告任务是否处于不再投递的终态。
func (t *Task) Terminal() bool {
	return t.Status == StatusSucceeded || t.Status == StatusDead
}

// Clone 返回任务的深拷贝，避免外部代码修改队列内部状态。
func (t *Task) Clone() *Task {
	if t == nil {
		return nil
	}
	cp := *t
	return &cp
}
