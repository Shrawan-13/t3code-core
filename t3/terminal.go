package t3

import (
	"context"
	"encoding/json"
	"fmt"

)

// TerminalSession is the state of one shell inside a thread.
type TerminalSession struct {
	ThreadID      string  `json:"threadId"`
	TerminalID    string  `json:"terminalId"`
	Cwd           string  `json:"cwd"`
	WorktreePath  *string `json:"worktreePath"`
	Status        string  `json:"status"`
	Pid           *int    `json:"pid"`
	History       string  `json:"history"`
	ExitCode      *int    `json:"exitCode"`
	ExitSignal    *int    `json:"exitSignal"`
	Label         string  `json:"label"`
	UpdatedAt     string  `json:"updatedAt"`
	Sequence      *int64  `json:"sequence,omitempty"`
	HasRunningSubprocess *bool `json:"hasRunningSubprocess,omitempty"`
}

// TerminalEvent is one terminal stream update.
type TerminalEvent struct {
	Type       string           `json:"type"`
	ThreadID   string           `json:"threadId"`
	TerminalID string           `json:"terminalId"`
	Sequence   *int64           `json:"sequence,omitempty"`
	Data       string           `json:"data,omitempty"`
	Snapshot   *TerminalSession `json:"snapshot,omitempty"`
	Message    string           `json:"message,omitempty"`
	ExitCode   *int             `json:"exitCode,omitempty"`
	ExitSignal *int             `json:"exitSignal,omitempty"`
	HasRunningSubprocess *bool  `json:"hasRunningSubprocess,omitempty"`
	Label      string           `json:"label,omitempty"`
}

// OpenTerminal starts a shell in a thread.
func (c *Client) OpenTerminal(ctx context.Context, threadID, terminalID, cwd string, cols, rows int, worktreePath string) (*TerminalSession, error) {
	if terminalID == "" {
		terminalID = "term-1"
	}
	if cols <= 0 {
		cols = 80
	}
	if rows <= 0 {
		rows = 24
	}
	payload := map[string]any{
		"threadId": threadID, "terminalId": terminalID, "cwd": cwd,
		"cols": cols, "rows": rows,
	}
	if worktreePath != "" {
		payload["worktreePath"] = worktreePath
	}
	var out TerminalSession
	if err := c.call(ctx, "terminal.open", payload, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// TerminalSubscription streams output for one terminal.
type TerminalSubscription struct {
	Events <-chan TerminalEvent
	cancel func()
}

// Close stops the subscription.
func (t *TerminalSubscription) Close() { t.cancel() }

// AttachTerminal subscribes to a terminal, starting it when needed.
func (c *Client) AttachTerminal(ctx context.Context, threadID, terminalID, cwd string, cols, rows int, worktreePath string) (*TerminalSubscription, error) {
	if terminalID == "" {
		terminalID = "term-1"
	}
	payload := map[string]any{
		"threadId": threadID, "terminalId": terminalID,
		"restartIfNotRunning": false,
	}
	if cwd != "" {
		payload["cwd"] = cwd
	}
	if cols > 0 {
		payload["cols"] = cols
	}
	if rows > 0 {
		payload["rows"] = rows
	}
	if worktreePath != "" {
		payload["worktreePath"] = worktreePath
	}
	return c.watchTerminal(ctx, "terminal.attach", payload)
}

// ObserveTerminal streams output without starting a shell.
func (c *Client) ObserveTerminal(ctx context.Context, threadID, terminalID string) (*TerminalSubscription, error) {
	return c.watchTerminal(ctx, "terminal.observe", map[string]any{
		"threadId": threadID, "terminalId": terminalID,
	})
}

func (c *Client) watchTerminal(ctx context.Context, method string, payload map[string]any) (*TerminalSubscription, error) {
	stream, err := c.Stream(ctx, method, payload)
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	events := make(chan TerminalEvent, 256)
	go func() {
		defer close(events)
		defer stream.Cancel()
		for {
			values, done, err := stream.Next(runCtx)
			if err != nil || done {
				return
			}
			for _, raw := range values {
				var ev TerminalEvent
				if json.Unmarshal(raw, &ev) != nil {
					continue
				}
				select {
				case events <- ev:
				case <-runCtx.Done():
					return
				}
			}
		}
	}()
	return &TerminalSubscription{Events: events, cancel: cancel}, nil
}

// WriteTerminal sends input to a shell.
func (c *Client) WriteTerminal(ctx context.Context, threadID, terminalID, data string) error {
	return c.call(ctx, "terminal.write", map[string]any{
		"threadId": threadID, "terminalId": terminalID, "data": data,
	}, nil)
}

// ResizeTerminal updates the pty size.
func (c *Client) ResizeTerminal(ctx context.Context, threadID, terminalID string, cols, rows int) error {
	if cols < 1 || rows < 1 {
		return fmt.Errorf("cols and rows must be positive")
	}
	return c.call(ctx, "terminal.resize", map[string]any{
		"threadId": threadID, "terminalId": terminalID, "cols": cols, "rows": rows,
	}, nil)
}

// ClearTerminal clears the scrollback.
func (c *Client) ClearTerminal(ctx context.Context, threadID, terminalID string) error {
	return c.call(ctx, "terminal.clear", map[string]any{
		"threadId": threadID, "terminalId": terminalID,
	}, nil)
}

// RestartTerminal restarts a shell.
func (c *Client) RestartTerminal(ctx context.Context, threadID, terminalID, cwd string, cols, rows int, worktreePath string) (*TerminalSession, error) {
	if cols <= 0 {
		cols = 80
	}
	if rows <= 0 {
		rows = 24
	}
	payload := map[string]any{
		"threadId": threadID, "terminalId": terminalID, "cwd": cwd,
		"cols": cols, "rows": rows,
	}
	if worktreePath != "" {
		payload["worktreePath"] = worktreePath
	}
	var out TerminalSession
	if err := c.call(ctx, "terminal.restart", payload, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// CloseTerminal closes one terminal or all terminals of a thread.
func (c *Client) CloseTerminal(ctx context.Context, threadID, terminalID string, deleteHistory bool) error {
	payload := map[string]any{"threadId": threadID}
	if terminalID != "" {
		payload["terminalId"] = terminalID
	}
	if deleteHistory {
		payload["deleteHistory"] = true
	}
	return c.call(ctx, "terminal.close", payload, nil)
}

// TerminalMetadataEvent describes terminal list changes across threads.
type TerminalMetadataEvent struct {
	Type     string             `json:"type"`
	Terminals []TerminalSession `json:"terminals,omitempty"`
	Terminal *TerminalSession   `json:"terminal,omitempty"`
	ThreadID string             `json:"threadId,omitempty"`
	TerminalID string           `json:"terminalId,omitempty"`
}

// SubscribeTerminalMetadata streams the set of running terminals.
func (c *Client) SubscribeTerminalMetadata(ctx context.Context) (<-chan TerminalMetadataEvent, func(), error) {
	stream, err := c.Stream(ctx, "subscribeTerminalMetadata", map[string]any{})
	if err != nil {
		return nil, nil, err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	ch := make(chan TerminalMetadataEvent, 64)
	go func() {
		defer close(ch)
		defer stream.Cancel()
		for {
			values, done, err := stream.Next(runCtx)
			if err != nil || done {
				return
			}
			for _, raw := range values {
				var ev TerminalMetadataEvent
				if json.Unmarshal(raw, &ev) != nil {
					continue
				}
				select {
				case ch <- ev:
				case <-runCtx.Done():
					return
				}
			}
		}
	}()
	return ch, cancel, nil
}

// SubscribeTerminalEvents streams every terminal event on the server.
func (c *Client) SubscribeTerminalEvents(ctx context.Context) (<-chan TerminalEvent, func(), error) {
	stream, err := c.Stream(ctx, "subscribeTerminalEvents", map[string]any{})
	if err != nil {
		return nil, nil, err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	ch := make(chan TerminalEvent, 256)
	go func() {
		defer close(ch)
		defer stream.Cancel()
		for {
			values, done, err := stream.Next(runCtx)
			if err != nil || done {
				return
			}
			for _, raw := range values {
				var ev TerminalEvent
				if json.Unmarshal(raw, &ev) != nil {
					continue
				}
				select {
				case ch <- ev:
				case <-runCtx.Done():
					return
				}
			}
		}
	}()
	return ch, cancel, nil
}
