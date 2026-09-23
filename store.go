package jobqueue

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Store is the local persistence boundary that stands in for a message
// broker. All mutating operations are durable before they return, and Claim
// only delivers jobs that are currently deliverable.
type Store interface {
	// Enqueue inserts one or more new jobs (status pending).
	Enqueue(jobs ...*Job) error
	// Claim atomically selects one deliverable job (pending and past its
	// AvailableAt, or leased with an expired visibility timeout), marks it
	// leased and returns the new delivery. Returns ErrNotFound when idle.
	//
	// A reclaimed expired lease is represented by reclaimed == true so the
	// caller can apply the max-delivery safety rule.
	Claim(owner string, leaseTTL time.Duration, now time.Time) (d *Delivery, reclaimed bool, err error)
	// Complete marks a leased job succeeded, but only if leaseID still owns
	// it. Returns ErrLeaseLost otherwise.
	Complete(jobID, leaseID string, now time.Time) error
	// Retry releases a leased job back to pending with a backoff and stores
	// the failure reason. Lease ownership is validated.
	Retry(jobID, leaseID, reason string, availableAt, now time.Time) error
	// Dead moves a leased job into the dead-letter set, preserving the
	// reason. Lease ownership is validated.
	Dead(jobID, leaseID, reason string, now time.Time) error
	// Release returns an in-flight leased job to pending immediately. It is
	// used on graceful shutdown so unfinished work stays redeliverable.
	Release(jobID, leaseID string, now time.Time) error
	// UpdateProgress persists handler progress for a leased delivery.
	UpdateProgress(jobID, leaseID string, progress int, now time.Time) error
	// Get returns a snapshot of one job.
	Get(jobID string) (*Job, error)
	// All returns snapshots of every persisted job.
	All() ([]*Job, error)
	// RecoverExpired is used at startup: jobs left leased by a crashed
	// process are reset to pending so they can be redelivered. It returns
	// the recovered job IDs.
	RecoverExpired(now time.Time) ([]string, error)
	// RequeueDead explicitly moves a dead-lettered job back to pending
	// (operator action).
	RequeueDead(jobID string, now time.Time) error
	// Close flushes and releases the store.
	Close() error
}

// snapshotVersion is the on-disk format version.
const snapshotVersion = 1

type snapshot struct {
	Version int    `json:"version"`
	Jobs    []*Job `json:"jobs"`
}

// FileStore persists every mutation to a local JSON snapshot using
// temp-file + fsync + atomic rename, so a crash leaves either the previous
// or the new complete state (never a torn file).
type FileStore struct {
	mu     sync.Mutex
	path   string
	jobs   map[string]*Job
	closed bool
}

// OpenStore opens (or creates) the file-backed store at path and performs
// crash recovery: stale leases are reset to pending and become deliverable
// again; succeeded and dead jobs are left untouched.
func OpenStore(path string) (*FileStore, error) {
	s := &FileStore{path: path, jobs: map[string]*Job{}}
	if err := s.load(); err != nil {
		return nil, err
	}
	if _, err := s.RecoverExpired(time.Now().UTC()); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *FileStore) load() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("jobqueue: read store: %w", err)
	}
	if len(data) == 0 {
		return nil
	}
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("jobqueue: corrupt store file %q: %w", s.path, err)
	}
	for _, j := range snap.Jobs {
		s.jobs[j.ID] = j
	}
	return nil
}

// persistLocked must be called with s.mu held.
func (s *FileStore) persistLocked() error {
	if s.closed {
		return ErrClosed
	}
	jobs := make([]*Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		jobs = append(jobs, cloneJob(j))
	}
	sort.Slice(jobs, func(i, k int) bool {
		if !jobs[i].CreatedAt.Equal(jobs[k].CreatedAt) {
			return jobs[i].CreatedAt.Before(jobs[k].CreatedAt)
		}
		return jobs[i].ID < jobs[k].ID
	})
	data, err := json.MarshalIndent(snapshot{Version: snapshotVersion, Jobs: jobs}, "", "  ")
	if err != nil {
		return fmt.Errorf("jobqueue: marshal snapshot: %w", err)
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("jobqueue: create store dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".jobqueue-*.tmp")
	if err != nil {
		return fmt.Errorf("jobqueue: create temp snapshot: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("jobqueue: write snapshot: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("jobqueue: fsync snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("jobqueue: close snapshot: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("jobqueue: publish snapshot: %w", err)
	}
	return nil
}

func cloneJob(j *Job) *Job {
	if j == nil {
		return nil
	}
	c := *j
	if j.Payload != nil {
		c.Payload = make(map[string]any, len(j.Payload))
		for k, v := range j.Payload {
			c.Payload[k] = v
		}
	}
	return &c
}

func newLeaseID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failure is not recoverable in practice.
		panic(fmt.Errorf("jobqueue: cannot generate lease id: %w", err))
	}
	return hex.EncodeToString(b[:])
}

// Enqueue implements Store.
func (s *FileStore) Enqueue(jobs ...*Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	now := time.Now().UTC()
	for _, j := range jobs {
		if j == nil || j.ID == "" {
			return fmt.Errorf("jobqueue: enqueue requires a non-empty job id")
		}
		if _, exists := s.jobs[j.ID]; exists {
			return fmt.Errorf("jobqueue: duplicate job id %q", j.ID)
		}
		if j.Status == "" {
			j.Status = StatusPending
		}
		if j.MaxAttempts <= 0 {
			j.MaxAttempts = 1
		}
		if j.CreatedAt.IsZero() {
			j.CreatedAt = now
		}
		j.UpdatedAt = now
		s.jobs[j.ID] = cloneJob(j)
	}
	return s.persistLocked()
}

// Claim implements Store.
func (s *FileStore) Claim(owner string, leaseTTL time.Duration, now time.Time) (*Delivery, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, false, ErrClosed
	}
	ids := make([]string, 0, len(s.jobs))
	for id := range s.jobs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var chosen *Job
	reclaimed := false
	for _, id := range ids {
		j := s.jobs[id]
		switch {
		case j.Status == StatusPending && (j.AvailableAt.IsZero() || !now.Before(j.AvailableAt)):
			chosen = j
		case j.Status == StatusLeased && !now.Before(j.LeaseExpiresAt):
			chosen = j
			reclaimed = true
		}
		if chosen != nil {
			break
		}
	}
	if chosen == nil {
		return nil, false, ErrNotFound
	}
	chosen.Attempts++
	chosen.Status = StatusLeased
	chosen.LeaseID = newLeaseID()
	chosen.LeaseOwner = owner
	chosen.LeaseExpiresAt = now.Add(leaseTTL)
	chosen.AvailableAt = time.Time{}
	chosen.UpdatedAt = now
	if err := s.persistLocked(); err != nil {
		return nil, false, err
	}
	return &Delivery{Job: cloneJob(chosen), LeaseID: chosen.LeaseID, Attempt: chosen.Attempts}, reclaimed, nil
}

// ownedLeaseLocked must be called with s.mu held.
func (s *FileStore) ownedLeaseLocked(jobID, leaseID string) (*Job, error) {
	j, ok := s.jobs[jobID]
	if !ok {
		return nil, ErrNotFound
	}
	switch j.Status {
	case StatusDead:
		return nil, ErrDead
	case StatusSucceeded:
		return nil, ErrLeaseLost
	case StatusLeased:
		if j.LeaseID != leaseID {
			return nil, ErrLeaseLost
		}
		return j, nil
	default:
		return nil, ErrLeaseLost
	}
}

func (s *FileStore) Complete(jobID, leaseID string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.ownedLeaseLocked(jobID, leaseID)
	if err != nil {
		return err
	}
	j.Status = StatusSucceeded
	j.LeaseID = ""
	j.LeaseOwner = ""
	j.LeaseExpiresAt = time.Time{}
	j.Progress = 100
	j.UpdatedAt = now
	return s.persistLocked()
}

func (s *FileStore) Retry(jobID, leaseID, reason string, availableAt, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.ownedLeaseLocked(jobID, leaseID)
	if err != nil {
		return err
	}
	j.Status = StatusPending
	j.LeaseID = ""
	j.LeaseOwner = ""
	j.LeaseExpiresAt = time.Time{}
	j.AvailableAt = availableAt
	j.LastError = reason
	j.UpdatedAt = now
	return s.persistLocked()
}

func (s *FileStore) Dead(jobID, leaseID, reason string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[jobID]
	if !ok {
		return ErrNotFound
	}
	if j.Status == StatusDead {
		return nil // idempotent
	}
	if j.Status == StatusSucceeded {
		return ErrLeaseLost
	}
	if j.Status == StatusLeased && j.LeaseID != leaseID {
		return ErrLeaseLost
	}
	j.Status = StatusDead
	j.DeadReason = reason
	j.LastError = reason
	j.LeaseID = ""
	j.LeaseOwner = ""
	j.LeaseExpiresAt = time.Time{}
	j.AvailableAt = time.Time{}
	j.UpdatedAt = now
	return s.persistLocked()
}

func (s *FileStore) Release(jobID, leaseID string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.ownedLeaseLocked(jobID, leaseID)
	if err != nil {
		return err
	}
	j.Status = StatusPending
	j.LeaseID = ""
	j.LeaseOwner = ""
	j.LeaseExpiresAt = time.Time{}
	j.AvailableAt = time.Time{} // redeliver immediately
	j.UpdatedAt = now
	return s.persistLocked()
}

func (s *FileStore) UpdateProgress(jobID, leaseID string, progress int, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.ownedLeaseLocked(jobID, leaseID)
	if err != nil {
		return err
	}
	j.Progress = progress
	j.UpdatedAt = now
	return s.persistLocked()
}

func (s *FileStore) Get(jobID string) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[jobID]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneJob(j), nil
}

func (s *FileStore) All() ([]*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		out = append(out, cloneJob(j))
	}
	sort.Slice(out, func(i, k int) bool {
		if !out[i].CreatedAt.Equal(out[k].CreatedAt) {
			return out[i].CreatedAt.Before(out[k].CreatedAt)
		}
		return out[i].ID < out[k].ID
	})
	return out, nil
}

func (s *FileStore) RecoverExpired(now time.Time) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var recovered []string
	for _, j := range s.jobs {
		// Any lease surviving an Open is stale: its owner process is gone.
		if j.Status == StatusLeased {
			j.Status = StatusPending
			j.LeaseID = ""
			j.LeaseOwner = ""
			j.LeaseExpiresAt = time.Time{}
			j.AvailableAt = time.Time{}
			j.LastError = appendReason(j.LastError, "process restart: stale lease recovered")
			j.UpdatedAt = now
			recovered = append(recovered, j.ID)
		}
	}
	sort.Strings(recovered)
	if len(recovered) > 0 {
		if err := s.persistLocked(); err != nil {
			return nil, err
		}
	}
	return recovered, nil
}

func appendReason(prev, add string) string {
	if prev == "" {
		return add
	}
	return prev + "; " + add
}

func (s *FileStore) RequeueDead(jobID string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[jobID]
	if !ok {
		return ErrNotFound
	}
	if j.Status != StatusDead {
		return fmt.Errorf("jobqueue: job %q is not dead (status=%s)", jobID, j.Status)
	}
	prevReason := j.DeadReason
	j.Status = StatusPending
	j.Attempts = 0
	j.Progress = 0
	j.DeadReason = ""
	j.LastError = appendReason("requeued after dead-letter", prevReason)
	j.AvailableAt = time.Time{}
	j.UpdatedAt = now
	return s.persistLocked()
}

func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// Compile-time assertion that FileStore satisfies Store.
var _ Store = (*FileStore)(nil)
