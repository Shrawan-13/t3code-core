package t3

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sync"

	"github.com/t3tools/t3go/internal/rpc"
)

// ThreadEventKind classifies thread stream updates.
type ThreadEventKind string

const (
	ThreadEventSnapshot     ThreadEventKind = "snapshot"
	ThreadEventSynchronized ThreadEventKind = "synchronized"
	ThreadEventEvent        ThreadEventKind = "event"
	ThreadEventUnknown      ThreadEventKind = "unknown-event"
	ThreadEventClosed       ThreadEventKind = "closed"
)

// ThreadEvent is one update from a thread subscription.
type ThreadEvent struct {
	Kind      ThreadEventKind `json:"kind"`
	Sequence  int64           `json:"sequence,omitempty"`
	Snapshot  *ThreadState    `json:"snapshot,omitempty"`
	Event     *DomainEvent    `json:"event,omitempty"`
	EventType string          `json:"eventType,omitempty"`
	Err       string          `json:"error,omitempty"`
}

// ThreadWatcher maintains a live thread projection.
type ThreadWatcher struct {
	client  *Client
	threadID string

	mu     sync.RWMutex
	state  ThreadState
	partial bool
	latestLocalOrdinal *int

	events chan ThreadEvent
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

// WatchThread subscribes to a thread. If afterSequence is negative the server
// sends a full snapshot first.
func (c *Client) WatchThread(ctx context.Context, threadID string, afterSequence int64) (*ThreadWatcher, error) {
	return c.WatchThreadPartial(ctx, threadID, afterSequence, false)
}

// WatchThreadPartial subscribes to a thread, optionally requesting the bounded
// timeline optimization used by mobile clients.
func (c *Client) WatchThreadPartial(ctx context.Context, threadID string, afterSequence int64, acceptBounded bool) (*ThreadWatcher, error) {
	payload := map[string]any{"threadId": threadID}
	if afterSequence >= 0 {
		payload["afterSequence"] = afterSequence
	}
	if acceptBounded {
		payload["acceptBoundedSnapshot"] = true
		payload["acceptCompactTurnItems"] = true
	}
	stream, err := c.Stream(ctx, "orchestration.subscribeThread", payload)
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	w := &ThreadWatcher{
		client:   c,
		threadID: threadID,
		events:   make(chan ThreadEvent, 256),
		cancel:   cancel,
		done:     make(chan struct{}),
	}
	go w.run(runCtx, stream)
	return w, nil
}

func (w *ThreadWatcher) run(ctx context.Context, stream *rpc.Stream) {
	defer close(w.done)
	for {
		values, done, err := stream.Next(ctx)
		if err != nil || done {
			w.mu.Lock()
			w.err = err
			w.mu.Unlock()
			ev := ThreadEvent{Kind: ThreadEventClosed}
			if err != nil {
				ev.Err = err.Error()
			}
			w.emit(ev)
			return
		}
		for _, raw := range values {
			var item struct {
				Kind             string          `json:"kind"`
				Sequence         int64           `json:"sequence"`
				SnapshotSequence int64           `json:"snapshotSequence"`
				Projection       *ThreadProjection `json:"projection"`
				HistoryCursor    *string         `json:"historyCursor"`
				HasMoreHistory   *bool           `json:"hasMoreHistory"`
				LatestLocalTurnOrdinal *int      `json:"latestLocalTurnOrdinal"`
				TurnItemsOmitLocalVisible bool  `json:"turnItemsOmitLocalVisible"`
				Event            *DomainEvent    `json:"event"`
				EventType        string          `json:"eventType"`
			}
			if err := json.Unmarshal(raw, &item); err != nil {
				continue
			}
			switch item.Kind {
			case "snapshot":
				if item.Projection == nil {
					continue
				}
				w.mu.Lock()
				w.state = ThreadState{
					Projection:     *item.Projection,
					Sequence:       item.SnapshotSequence,
					HistoryCursor:  item.HistoryCursor,
					HasMoreHistory: item.HasMoreHistory != nil && *item.HasMoreHistory,
					Partial:        item.TurnItemsOmitLocalVisible,
				}
				w.partial = item.TurnItemsOmitLocalVisible
				stateCopy := w.state
				w.mu.Unlock()
				w.emit(ThreadEvent{Kind: ThreadEventSnapshot, Snapshot: &stateCopy})
				// The latest local ordinal applies to subsequent events.
				w.latestLocalOrdinal = item.LatestLocalTurnOrdinal
			case "synchronized":
				w.emit(ThreadEvent{Kind: ThreadEventSynchronized})
			case "event":
				if item.Event == nil {
					continue
				}
				w.mu.Lock()
				w.state.Sequence = item.Sequence
				w.state.Apply(*item.Event, w.partial, w.latestLocalOrdinal)
				w.mu.Unlock()
				w.emit(ThreadEvent{Kind: ThreadEventEvent, Sequence: item.Sequence, Event: item.Event})
			case "unknown-event":
				w.mu.Lock()
				w.state.Sequence = item.Sequence
				w.mu.Unlock()
				w.emit(ThreadEvent{Kind: ThreadEventUnknown, Sequence: item.Sequence, EventType: item.EventType})
			}
		}
	}
}

func (w *ThreadWatcher) emit(ev ThreadEvent) {
	select {
	case w.events <- ev:
	default:
		select {
		case <-w.events:
		default:
		}
		select {
		case w.events <- ev:
		default:
		}
	}
}

// Events returns the update channel.
func (w *ThreadWatcher) Events() <-chan ThreadEvent { return w.events }

// Close cancels the subscription.
func (w *ThreadWatcher) Close() { w.cancel() }

// Done is closed when the subscription ends.
func (w *ThreadWatcher) Done() <-chan struct{} { return w.done }

// Err returns the terminal error.
func (w *ThreadWatcher) Err() error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.err
}

// State returns a copy of the current thread state.
func (w *ThreadWatcher) State() ThreadState {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.state
}

// StateJSON serializes the current thread state safely.
func (w *ThreadWatcher) StateJSON() ([]byte, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return json.Marshal(w.state)
}

// Projection returns the current projection.
func (w *ThreadWatcher) Projection() ThreadProjection {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.state.Projection
}

// WaitSnapshot blocks until the first projection snapshot arrives.
func (w *ThreadWatcher) WaitSnapshot(ctx context.Context) (*ThreadState, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-w.done:
			if err := w.Err(); err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("thread stream ended before snapshot")
		case ev, ok := <-w.events:
			if !ok {
				return nil, fmt.Errorf("thread stream closed")
			}
			if ev.Kind == ThreadEventSnapshot {
				return ev.Snapshot, nil
			}
			if ev.Kind == ThreadEventClosed && ev.Err != "" {
				return nil, fmt.Errorf("%s", ev.Err)
			}
		}
	}
}

// GetThread fetches a thread projection over RPC.
func (c *Client) GetThread(ctx context.Context, threadID string) (*ThreadProjection, error) {
	var out ThreadProjection
	if err := c.call(ctx, "orchestration.getThreadProjection", map[string]any{"threadId": threadID}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ArchivedShell fetches the archived thread snapshot over RPC.
func (c *Client) ArchivedShell(ctx context.Context) (*ShellSnapshot, error) {
	var out ShellSnapshot
	if err := c.call(ctx, "orchestration.getArchivedShellSnapshot", map[string]any{}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ThreadHistoryPage is one page of older thread history.
type ThreadHistoryPage struct {
	SnapshotSequence int64             `json:"snapshotSequence"`
	Items            []VisibleTurnItem `json:"items"`
	NextCursor       *string           `json:"nextCursor"`
	HasMoreHistory   bool              `json:"hasMoreHistory"`
}

// ThreadHistory fetches older timeline rows over HTTP.
func (c *Client) ThreadHistory(ctx context.Context, threadID, cursor string) (*ThreadHistoryPage, error) {
	path := "/api/orchestration/threads/" + url.PathEscape(threadID) + "/history?cursor=" + url.QueryEscape(cursor)
	var out ThreadHistoryPage
	if err := c.orchestrationJSON(ctx, "GET", path, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
