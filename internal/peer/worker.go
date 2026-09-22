package peer

import (
	"context"
	"errors"
	"net"
	"sync"

	"github.com/gus-ceraso/Leech/internal/limits"
)

var (
	ErrWorkerClosed    = errors.New("peer connection worker is closed")
	ErrWorkerQueueFull = errors.New("peer connection command queue is full")
	ErrEventQueueFull  = errors.New("peer connection event queue is full")
	ErrWorkerConfig    = errors.New("invalid peer connection worker configuration")
)

// PeerEvent is the only output produced by a connection worker. The worker
// never mutates PeerState; a coordinator consumes Message events and applies
// them in its own goroutine.
type PeerEvent struct {
	Message Message
	Err     error
}

func (e PeerEvent) HasMessage() bool {
	return e.Err == nil && !e.Message.KeepAlive
}

// ConnectionWorker owns one connected net.Conn and two bounded queues. A
// reader and writer are joined by Close, Run, or Wait. The connection itself
// is closed to unblock a raw Read when cancellation occurs.
type ConnectionWorker struct {
	conn       net.Conn
	options    ReadOptions
	commands   chan Message
	events     chan PeerEvent
	finishOnce sync.Once
	wg         sync.WaitGroup
	done       chan struct{}

	mu       sync.Mutex
	cancel   context.CancelFunc
	started  bool
	closing  bool
	terminal error
}

// NewConnectionWorker creates a worker with supported queue bounds. Start or
// SendContext starts it; construction itself does not create goroutines.
func NewConnectionWorker(conn net.Conn, options ReadOptions) *ConnectionWorker {
	worker, _ := NewConnectionWorkerWithCaps(conn, options, limits.PeerCommands, limits.PeerCommands)
	return worker
}

func NewConnectionWorkerWithCaps(conn net.Conn, options ReadOptions, commandCap, eventCap int) (*ConnectionWorker, error) {
	if conn == nil || commandCap < 1 || commandCap > limits.PeerCommands || eventCap < 1 || eventCap > limits.PeerCommands {
		return nil, ErrWorkerConfig
	}
	return &ConnectionWorker{
		conn:     conn,
		options:  options,
		commands: make(chan Message, commandCap),
		events:   make(chan PeerEvent, eventCap),
		done:     make(chan struct{}),
	}, nil
}

// Start begins I/O using a caller-owned context. It is idempotent.
func (w *ConnectionWorker) Start(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	w.mu.Lock()
	if w.started || w.closing {
		w.mu.Unlock()
		return
	}
	workerCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	w.started = true
	// Register every worker before releasing mu. Close can therefore either
	// prevent this block entirely or wait for all registered goroutines.
	w.wg.Add(3)
	go w.readLoop(workerCtx)
	go w.writeLoop(workerCtx)
	go w.cancelWatcher(workerCtx)
	go func() {
		w.wg.Wait()
		w.finish()
	}()
	w.mu.Unlock()
}

// Run starts the worker and waits for both I/O loops. Its returned error is
// the first worker failure, or nil after an explicit Close.
func (w *ConnectionWorker) Run(ctx context.Context) error {
	w.Start(ctx)
	<-w.done
	return w.Err()
}

func (w *ConnectionWorker) Events() <-chan PeerEvent { return w.events }

// SendContext queues a permitted local command. It waits for queue capacity
// only while ctx remains live; it never closes the queue from the producer.
func (w *ConnectionWorker) SendContext(ctx context.Context, message Message) error {
	if _, err := EncodeOutbound(message); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	w.Start(context.Background())
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.done:
		return ErrWorkerClosed
	case w.commands <- message:
		return nil
	}
}

// Send is the ordinary coordinator path. It remains cancel-safe through
// Close; use SendContext when a full queue needs a caller deadline.
func (w *ConnectionWorker) Send(message Message) error {
	return w.SendContext(context.Background(), message)
}

// Close cancels both loops and closes the owned transport to unblock reads.
// It is idempotent and waits for all worker goroutines before returning.
func (w *ConnectionWorker) Close() error {
	w.mu.Lock()
	if !w.closing {
		w.closing = true
	}
	started := w.started
	cancel := w.cancel
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	_ = w.conn.Close()
	if started {
		<-w.done
	} else {
		// Closing before Start permanently seals the worker. Start observes
		// closing under the same mutex and cannot launch a later goroutine.
		w.finish()
	}
	return w.Err()
}

func (w *ConnectionWorker) Wait() error {
	if !w.isStarted() {
		return ErrWorkerClosed
	}
	<-w.done
	return w.Err()
}

func (w *ConnectionWorker) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.terminal
}

func (w *ConnectionWorker) finish() {
	w.finishOnce.Do(func() {
		close(w.done)
		close(w.events)
	})
}

func (w *ConnectionWorker) isStarted() bool {
	w.mu.Lock()
	started := w.started
	w.mu.Unlock()
	return started
}

func (w *ConnectionWorker) cancelWatcher(ctx context.Context) {
	defer w.wg.Done()
	<-ctx.Done()
	_ = w.conn.Close()
}

func (w *ConnectionWorker) readLoop(ctx context.Context) {
	defer w.wg.Done()
	for {
		message, err := ReadMessageWithOptions(w.conn, w.options)
		if err != nil {
			if !w.isClosing(ctx) {
				w.fail(err, true)
			}
			return
		}
		if !w.emit(PeerEvent{Message: message}) {
			return
		}
	}
}

func (w *ConnectionWorker) writeLoop(ctx context.Context) {
	defer w.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case message := <-w.commands:
			if err := WriteMessage(w.conn, message); err != nil {
				if !w.isClosing(ctx) {
					w.fail(err, true)
				}
				return
			}
		}
	}
}

func (w *ConnectionWorker) emit(event PeerEvent) bool {
	select {
	case w.events <- event:
		return true
	default:
		w.fail(ErrEventQueueFull, false)
		return false
	}
}

func (w *ConnectionWorker) fail(err error, notify bool) {
	w.mu.Lock()
	first := w.terminal == nil
	if w.terminal == nil {
		w.terminal = err
	}
	cancel := w.cancel
	closing := w.closing
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	_ = w.conn.Close()
	if notify && first && !closing {
		select {
		case w.events <- PeerEvent{Err: err}:
		default:
			w.mu.Lock()
			if w.terminal == err {
				w.terminal = ErrEventQueueFull
			}
			w.mu.Unlock()
		}
	}
}

func (w *ConnectionWorker) isClosing(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closing
}
