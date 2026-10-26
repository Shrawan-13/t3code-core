package t3

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/t3tools/t3go/internal/rpc"
)

// ShellEventKind classifies shell stream updates.
type ShellEventKind string

const (
	ShellEventSnapshot      ShellEventKind = "snapshot"
	ShellEventSynchronized  ShellEventKind = "synchronized"
	ShellEventProjectUpdate ShellEventKind = "project.updated"
	ShellEventProjectRemove ShellEventKind = "project.removed"
	ShellEventThreadUpdate  ShellEventKind = "thread.updated"
	ShellEventThreadRemove  ShellEventKind = "thread.removed"
	ShellEventClosed        ShellEventKind = "closed"
)

// ShellEvent is one update from the shell subscription.
type ShellEvent struct {
	Kind      ShellEventKind `json:"kind"`
	Sequence  int64          `json:"sequence,omitempty"`
	Snapshot  *ShellSnapshot `json:"snapshot,omitempty"`
	Project   *Project       `json:"project,omitempty"`
	ProjectID string         `json:"projectId,omitempty"`
	Thread    *ThreadShell   `json:"thread,omitempty"`
	ThreadID  string         `json:"threadId,omitempty"`
	Archived  bool           `json:"archived,omitempty"`
	// ResolvedRepositoryIdentityRoots marks a project-metadata refresh. Those
	// snapshots intentionally carry empty thread lists and must not replace
	// the live shell's threads.
	ResolvedRepositoryIdentityRoots []string `json:"resolvedRepositoryIdentityRoots,omitempty"`
	Err                             string   `json:"error,omitempty"`
}

// ShellWatcher maintains a live shell snapshot.
type ShellWatcher struct {
	client  *Client
	mu      sync.RWMutex
	state   ShellSnapshot
	hasSnap bool
	seq     int64

	events chan ShellEvent
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

// WatchShell subscribes to shell updates. It returns immediately; the first
// snapshot arrives as a ShellEventSnapshot.
func (c *Client) WatchShell(ctx context.Context) (*ShellWatcher, error) {
	stream, err := c.Stream(ctx, "orchestration.subscribeShell", map[string]any{})
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	w := &ShellWatcher{
		client: c,
		events: make(chan ShellEvent, 128),
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go w.run(runCtx, stream)
	return w, nil
}

func (w *ShellWatcher) run(ctx context.Context, stream *rpc.Stream) {
	defer close(w.done)
	// Closing a watcher cancels ctx, but the RPC stream has its own request
	// slot. Cancel it here as well so the server stops publishing and the
	// client stops buffering events for a subscription nobody will read.
	defer stream.Cancel()
	for {
		values, done, err := stream.Next(ctx)
		if err != nil || done {
			if err != nil {
				w.mu.Lock()
				w.err = err
				w.mu.Unlock()
				w.emit(ShellEvent{Kind: ShellEventClosed, Err: err.Error()})
			} else {
				w.emit(ShellEvent{Kind: ShellEventClosed})
			}
			return
		}
		for _, raw := range values {
			var item struct {
				Kind                            string         `json:"kind"`
				Sequence                        int64          `json:"sequence"`
				Snapshot                        *ShellSnapshot `json:"snapshot"`
				Project                         *Project       `json:"project"`
				ProjectID                       string         `json:"projectId"`
				Thread                          *ThreadShell   `json:"thread"`
				ThreadID                        string         `json:"threadId"`
				Location                        string         `json:"location"`
				ResolvedRepositoryIdentityRoots []string       `json:"resolvedRepositoryIdentityRoots"`
			}
			if err := json.Unmarshal(raw, &item); err != nil {
				continue
			}
			switch item.Kind {
			case "snapshot":
				if item.Snapshot == nil {
					continue
				}
				w.mu.Lock()
				if len(item.ResolvedRepositoryIdentityRoots) > 0 && w.hasSnap {
					// The server emits a second, project-only snapshot while
					// repository identities resolve. It deliberately has no
					// threads; treating it as authoritative wipes the chat list.
					merged := mergeShellProjectSnapshot(w.state, *item.Snapshot)
					w.state = merged
					w.emit(ShellEvent{
						Kind:                            ShellEventSnapshot,
						Snapshot:                        &merged,
						ResolvedRepositoryIdentityRoots: append([]string(nil), item.ResolvedRepositoryIdentityRoots...),
					})
				} else {
					w.state = *item.Snapshot
					w.hasSnap = true
					w.seq = item.Snapshot.SnapshotSequence
					w.emit(ShellEvent{
						Kind:                            ShellEventSnapshot,
						Snapshot:                        item.Snapshot,
						ResolvedRepositoryIdentityRoots: append([]string(nil), item.ResolvedRepositoryIdentityRoots...),
					})
				}
				w.mu.Unlock()
			case "synchronized":
				w.emit(ShellEvent{Kind: ShellEventSynchronized})
			case "project.updated":
				if item.Project == nil {
					continue
				}
				w.mu.Lock()
				w.upsertProjectLocked(item.Project, item.Sequence)
				w.mu.Unlock()
				w.emit(ShellEvent{Kind: ShellEventProjectUpdate, Project: item.Project, Sequence: item.Sequence})
			case "project.removed":
				w.mu.Lock()
				w.removeProjectLocked(item.ProjectID, item.Sequence)
				w.mu.Unlock()
				w.emit(ShellEvent{Kind: ShellEventProjectRemove, ProjectID: item.ProjectID, Sequence: item.Sequence})
			case "thread.updated":
				if item.Thread == nil {
					continue
				}
				archived := item.Location == "archive"
				w.mu.Lock()
				w.upsertThreadLocked(item.Thread, archived, item.Sequence)
				w.mu.Unlock()
				w.emit(ShellEvent{Kind: ShellEventThreadUpdate, Thread: item.Thread, Archived: archived, Sequence: item.Sequence})
			case "thread.removed":
				archived := item.Location == "archive"
				w.mu.Lock()
				w.removeThreadLocked(item.ThreadID, archived, item.Sequence)
				w.mu.Unlock()
				w.emit(ShellEvent{Kind: ShellEventThreadRemove, ThreadID: item.ThreadID, Archived: archived, Sequence: item.Sequence})
			}
		}
	}
}

func (w *ShellWatcher) emit(ev ShellEvent) {
	select {
	case w.events <- ev:
	default:
		// Drop old notifications rather than stalling the stream; consumers can
		// always re-read Snapshot.
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

// Events returns the update channel. It is closed when the stream ends.
func (w *ShellWatcher) Events() <-chan ShellEvent { return w.events }

// Close cancels the subscription.
func (w *ShellWatcher) Close() {
	w.cancel()
}

// Done is closed when the subscription ends.
func (w *ShellWatcher) Done() <-chan struct{} { return w.done }

// Err returns the terminal subscription error.
func (w *ShellWatcher) Err() error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.err
}

// Snapshot returns the latest shell state.
func (w *ShellWatcher) Snapshot() ShellSnapshot {
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := w.state
	out.Projects = append([]Project(nil), w.state.Projects...)
	out.Threads = append([]ThreadShell(nil), w.state.Threads...)
	out.ArchivedThreads = append([]ThreadShell(nil), w.state.ArchivedThreads...)
	return out
}

// SnapshotJSON serializes the latest state safely.
func (w *ShellWatcher) SnapshotJSON() ([]byte, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return json.Marshal(w.state)
}

// WaitSnapshot blocks until the first snapshot arrives.
func (w *ShellWatcher) WaitSnapshot(ctx context.Context) (*ShellSnapshot, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-w.done:
			if err := w.Err(); err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("shell stream ended before snapshot")
		case ev, ok := <-w.events:
			if !ok {
				return nil, fmt.Errorf("shell stream closed")
			}
			if ev.Kind == ShellEventSnapshot {
				return ev.Snapshot, nil
			}
			if ev.Kind == ShellEventClosed && ev.Err != "" {
				return nil, fmt.Errorf("%s", ev.Err)
			}
		}
	}
}

func (w *ShellWatcher) upsertProjectLocked(p *Project, seq int64) {
	for i := range w.state.Projects {
		if w.state.Projects[i].ID == p.ID {
			w.state.Projects[i] = retainRepositoryIdentity(w.state.Projects[i], *p)
			w.bumpSeq(seq)
			return
		}
	}
	w.state.Projects = append(w.state.Projects, *p)
	w.bumpSeq(seq)
}

// mergeShellProjectSnapshot applies an enrichment-only snapshot without
// dropping threads. The server sends these after the authoritative snapshot so
// project repository identities can resolve asynchronously.
func mergeShellProjectSnapshot(previous, next ShellSnapshot) ShellSnapshot {
	merged := previous
	projects := append([]Project(nil), previous.Projects...)
	for _, project := range next.Projects {
		index := -1
		for i := range projects {
			if projects[i].ID == project.ID {
				index = i
				break
			}
		}
		if index >= 0 {
			projects[index] = retainRepositoryIdentity(projects[index], project)
		} else {
			projects = append(projects, project)
		}
	}
	merged.Projects = projects
	return merged
}

func retainRepositoryIdentity(previous, next Project) Project {
	if isNullJSON(next.RepositoryIdentity) {
		next.RepositoryIdentity = previous.RepositoryIdentity
	}
	return next
}

func isNullJSON(raw json.RawMessage) bool {
	trimmed := strings.TrimSpace(string(raw))
	return trimmed == "" || trimmed == "null"
}

func (w *ShellWatcher) removeProjectLocked(id string, seq int64) {
	out := w.state.Projects[:0]
	for _, p := range w.state.Projects {
		if p.ID != id {
			out = append(out, p)
		}
	}
	w.state.Projects = out
	w.bumpSeq(seq)
}

func (w *ShellWatcher) upsertThreadLocked(t *ThreadShell, archived bool, seq int64) {
	if archived {
		w.state.Threads = removeThread(w.state.Threads, t.ID)
		for i := range w.state.ArchivedThreads {
			if w.state.ArchivedThreads[i].ID == t.ID {
				w.state.ArchivedThreads[i] = *t
				w.bumpSeq(seq)
				return
			}
		}
		w.state.ArchivedThreads = append(w.state.ArchivedThreads, *t)
		w.bumpSeq(seq)
		return
	}
	w.state.ArchivedThreads = removeThread(w.state.ArchivedThreads, t.ID)
	for i := range w.state.Threads {
		if w.state.Threads[i].ID == t.ID {
			w.state.Threads[i] = *t
			w.bumpSeq(seq)
			return
		}
	}
	w.state.Threads = append(w.state.Threads, *t)
	w.bumpSeq(seq)
}

func (w *ShellWatcher) removeThreadLocked(id string, archived bool, seq int64) {
	if archived {
		w.state.ArchivedThreads = removeThread(w.state.ArchivedThreads, id)
	} else {
		w.state.Threads = removeThread(w.state.Threads, id)
	}
	w.bumpSeq(seq)
}

func (w *ShellWatcher) bumpSeq(seq int64) {
	if seq > w.seq {
		w.seq = seq
	}
	w.state.SnapshotSequence = w.seq
}

func removeThread(list []ThreadShell, id string) []ThreadShell {
	out := list[:0]
	for _, t := range list {
		if t.ID != id {
			out = append(out, t)
		}
	}
	return out
}
