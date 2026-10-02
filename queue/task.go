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
	// 进度信息（长耗时任务由当前租约持有者上报，单调只增）。
	// 旧版本写出的数据文件没有这些字段，加载后为零值（0% / 空阶段 / 零时刻）。
	ProgressPercent    int       `json:"progress_percent"`     // 整体百分比 [0,100]，只接受不小于已有值的上报
	ProgressStage      string    `json:"progress_stage"`       // 当前阶段的文字说明
	ProgressReportedAt time.Time `json:"progress_reported_at"` // 最近一次成功（被接受并落盘）上报的时刻
	UpdatedAt          time.Time `json:"updated_at"`
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
