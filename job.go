// Package jobqueue implements a locally persisted asynchronous job queue.
// A JSON snapshot file on disk plays the role of the message broker so that
// delivery and state semantics stay predictable across retries, concurrent
// consumers and process restarts.
package jobqueue

import (
	"context"
	"errors"
	"time"
)

// Status is the persisted lifecycle state of a job.
type Status string

const (
	// StatusPending means the job is queued and waiting for (re)delivery.
	StatusPending Status = "pending"
	// StatusLeased means the job has been delivered to a consumer and is
	// inside its visibility window.
	StatusLeased Status = "leased"
	// StatusSucceeded means the job was explicitly acknowledged after
	// successful processing. The state is durable.
	StatusSucceeded Status = "succeeded"
	// StatusDead means the job was moved to the dead-letter set. It must
	// never be delivered to a normal consumer again.
	StatusDead Status = "dead"
)

// Job is the persisted record of one asynchronous task.
type Job struct {
	ID          string         `json:"id"`
	Payload     map[string]any `json:"payload,omitempty"`
	Status      Status         `json:"status"`
	Attempts    int            `json:"attempts"`
	MaxAttempts int            `json:"max_attempts"`
	Progress    int            `json:"progress"`
	// AvailableAt holds the job back until this time (retry backoff).
	AvailableAt time.Time `json:"available_at,omitempty"`
	// Lease fields, meaningful only while Status == leased.
	LeaseID        string    `json:"lease_id,omitempty"`
	LeaseOwner     string    `json:"lease_owner,omitempty"`
	LeaseExpiresAt time.Time `json:"lease_expires_at,omitempty"`
	// LastError is the traceable reason for the latest failure / dead-letter.
	LastError string `json:"last_error,omitempty"`
	// DeadReason is preserved when the job enters the dead-letter set.
	DeadReason string    `json:"dead_reason,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// Sentinel errors.
var (
	// ErrNotFound is returned when no job exists for the given ID.
	ErrNotFound = errors.New("jobqueue: job not found")
	// ErrLeaseLost means the lease token no longer owns the job (it
	// expired, the job completed elsewhere, or it was dead-lettered).
	ErrLeaseLost = errors.New("jobqueue: lease lost or expired")
	// ErrDead is returned when a normal consumer tries to claim or touch a
	// job that is already in the dead-letter set.
	ErrDead = errors.New("jobqueue: job is dead-lettered")
	// ErrClosed is returned after the queue/store has been closed.
	ErrClosed = errors.New("jobqueue: queue is closed")
)

// Decision tells the queue how a handler failure should be treated.
type Decision string

const (
	// DecisionRetry marks a transient failure: redeliver after backoff
	// until MaxAttempts is exhausted.
	DecisionRetry Decision = "retry"
	// DecisionTerminal marks an unrecoverable failure: dead-letter now.
	DecisionTerminal Decision = "terminal"
)

// JobError is the structured failure returned by a Handler. It lets the
// handler distinguish retryable failures from unrecoverable ones.
type JobError struct {
	Decision Decision
	Reason   string
}

// Error implements error.
func (e *JobError) Error() string {
	if e == nil {
		return ""
	}
	return string(e.Decision) + ": " + e.Reason
}

// RetryError wraps a transient failure. The reason is persisted on the job
// for traceability.
func RetryError(reason string) *JobError {
	return &JobError{Decision: DecisionRetry, Reason: reason}
}

// TerminalError wraps an unrecoverable failure. The job is dead-lettered
// immediately with the given reason preserved.
func TerminalError(reason string) *JobError {
	return &JobError{Decision: DecisionTerminal, Reason: reason}
}

// classifyError turns a handler result into (decision, reason). A nil error
// is the caller's responsibility (ack); any plain error is retryable.
func classifyError(err error) (Decision, string) {
	if err == nil {
		return "", ""
	}
	var je *JobError
	if errors.As(err, &je) {
		return je.Decision, je.Reason
	}
	return DecisionRetry, err.Error()
}

// Delivery is one leased delivery of a job to a consumer.
type Delivery struct {
	Job     *Job
	LeaseID string
	Attempt int
	q       *Queue
}

// UpdateProgress persists progress (0-100) for this delivery. It returns
// ErrLeaseLost if the visibility window has already expired.
func (d *Delivery) UpdateProgress(progress int) error {
	if d == nil || d.q == nil {
		return ErrLeaseLost
	}
	return d.q.UpdateProgress(d, progress)
}

// Handler processes one delivery. Returning nil acknowledges the job.
// Returning a *JobError selects retry vs. terminal semantics; any other
// non-nil error is treated as retryable.
type Handler func(ctx context.Context, d *Delivery) error
