package queue

import (
	"context"
	"sync"
	"time"
)

// DefaultProgressFlushInterval 是 ProgressReporter 的默认最小落盘间隔。
const DefaultProgressFlushInterval = 200 * time.Millisecond

// ProgressReporter 面向单个任务的一次投递（一个租约）：
// handler 在长耗时任务中调用 Report 上报进度，调用永远非阻塞——
// 最新值先进内存（加锁写入后立刻返回），由后台 goroutine 按节流间隔
// （interval 内多次上报只落盘最新一条）调用 Queue.ReportProgress 原子落盘，
// 因而高频上报不会把任务处理拖到不可用。
//
// 进度的归属与单调约束仍由 Queue.ReportProgress 把关：租约令牌不匹配的
// 上报（旧执行迟到上报、任务已被重新投递）会在落盘时被拒绝并记日志。
type ProgressReporter struct {
	q        *Queue
	taskID   string
	token    string
	attempt  int
	interval time.Duration
	logger   Logger

	mu      sync.Mutex
	pending *progressState // 待落盘的最新值；nil 表示没有待落盘内容
	closed  bool

	wake chan struct{} // 容量 1，通知 flusher 有新值
	stop chan struct{} // close 时关闭，flusher 立即退出
	done chan struct{} // flusher 已退出
	once sync.Once
}

type progressState struct {
	percent int
	stage   string
}

// newProgressReporter 创建上报器并启动后台落盘协程。
func newProgressReporter(q *Queue, lease *Lease, interval time.Duration, logger Logger) *ProgressReporter {
	if interval <= 0 {
		interval = DefaultProgressFlushInterval
	}
	r := &ProgressReporter{
		q:        q,
		taskID:   lease.Task.ID,
		token:    lease.Token,
		attempt:  lease.Task.Attempts,
		interval: interval,
		logger:   logger,
		wake:     make(chan struct{}, 1),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go r.flushLoop()
	return r
}

// Report 记录一条进度。永不阻塞：仅加锁写入内存并唤醒后台落盘，
// 即使落盘很慢或上报器已关闭也不会卡住 handler。百分比非法时直接忽略。
func (r *ProgressReporter) Report(percent int, stage string) {
	if percent < 0 || percent > 100 {
		return
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.pending = &progressState{percent: percent, stage: stage}
	r.mu.Unlock()

	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// flushLoop 串行落盘。pending 是唯一事实来源，wake 仅用于唤醒；
// 两次落盘之间至少间隔 interval（窗口内的上报合并为最新一条）。
func (r *ProgressReporter) flushLoop() {
	defer close(r.done)
	for {
		select {
		case <-r.wake:
		case <-r.stop:
			return
		}
		if r.isClosed() {
			return
		}

		r.mu.Lock()
		st := r.pending
		r.mu.Unlock()
		if st != nil {
			if err := r.q.ReportProgress(context.Background(), r.taskID, r.token, st.percent, st.stage); err != nil {
				// 存储错误 / 令牌失效 / 进度倒退：记录并保留 pending，
				// 下个窗口用最新值重试（令牌失效类错误会一直被拒，直到 close）。
				r.logRejected(st, err)
			} else {
				// 仅当 pending 仍是这一条时才清除；落盘期间更新的值必须保留。
				r.mu.Lock()
				if r.pending == st {
					r.pending = nil
				}
				r.mu.Unlock()
			}

			// 固定合并窗口：期间 wake 信号不消费（留给下一轮），关闭则立即退出。
			// 只在确实落过盘后等待——没有进度要写时立即回到阻塞，
			// 避免给不上报进度的任务平白增加一次 interval 的关闭延迟。
			timer := time.NewTimer(r.interval)
			select {
			case <-timer.C:
			case <-r.stop:
				timer.Stop()
				return
			}
		}
	}
}

func (r *ProgressReporter) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

func (r *ProgressReporter) logRejected(st *progressState, err error) {
	if r.logger == nil {
		return
	}
	r.logger.Printf("progress task=%s attempt=%d percent=%d verdict=rejected err=%v",
		r.taskID, r.attempt, st.percent, err)
}

// close 停止后台协程，并把最后一条尚未落盘的进度同步落盘一次
// （通常在 handler 返回后、Ack/Nack 之前调用）。关闭后 Report 变为空操作。
// 可重复调用。
func (r *ProgressReporter) close(ctx context.Context) error {
	r.once.Do(func() { close(r.stop) })
	<-r.done

	r.mu.Lock()
	last := r.pending
	r.closed = true
	r.pending = nil
	r.mu.Unlock()

	if last == nil {
		return nil
	}
	return r.q.ReportProgress(ctx, r.taskID, r.token, last.percent, last.stage)
}

// progressCtxKey 在 context 中携带本次投递专属的 ProgressReporter。
type progressCtxKey struct{}

func withProgressReporter(ctx context.Context, r *ProgressReporter) context.Context {
	return context.WithValue(ctx, progressCtxKey{}, r)
}

// ProgressFromContext 取出 handler context 中的进度上报器。
// 仅在 Consumer 直接调用 handler 的场景存在；脱离消费者直接使用 Queue 时
// 返回 nil（这种场景可调用 Queue.ReportProgress 自行同步上报）。
func ProgressFromContext(ctx context.Context) *ProgressReporter {
	if ctx == nil {
		return nil
	}
	r, _ := ctx.Value(progressCtxKey{}).(*ProgressReporter)
	return r
}
