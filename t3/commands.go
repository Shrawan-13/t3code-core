package t3

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// NewID returns a random RFC 4122 version 4 UUID.
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(b[0:4]), hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]), hex.EncodeToString(b[8:10]),
		hex.EncodeToString(b[10:16]))
}

// Command is an orchestration command dispatched to the server.
type Command map[string]any

// Dispatch sends one orchestration command and returns the resulting sequence.
func (c *Client) Dispatch(ctx context.Context, cmd Command) (int64, error) {
	var out struct {
		Sequence int64 `json:"sequence"`
	}
	if err := c.call(ctx, "orchestration.dispatchCommand", cmd, &out); err != nil {
		return 0, err
	}
	return out.Sequence, nil
}

func withActor(cmd Command, source string) Command {
	cmd["commandId"] = NewID()
	cmd["createdBy"] = "user"
	cmd["creationSource"] = source
	return cmd
}

// DispatchMode controls how a message is delivered to a thread.
type DispatchMode string

const (
	DispatchStartImmediately DispatchMode = "start_immediately"
	DispatchQueueAfterActive DispatchMode = "queue_after_active"
	DispatchSteerActive      DispatchMode = "steer_active"
)

// SendMessageInput describes a user message to dispatch.
type SendMessageInput struct {
	ThreadID    string
	Text        string
	Attachments []json.RawMessage // ChatAttachment objects
	ModelSelection *ModelSelection
	Mode        DispatchMode
	TargetRunID string // required for steer_active
	Context     json.RawMessage
	TitleSeed   string
}

// SendMessage dispatches a user message to an existing thread.
func (c *Client) SendMessage(ctx context.Context, in SendMessageInput) (int64, error) {
	if in.ThreadID == "" {
		return 0, fmt.Errorf("threadId is required")
	}
	if in.Attachments == nil {
		in.Attachments = []json.RawMessage{}
	}
	cmd := Command{
		"type":         "message.dispatch",
		"threadId":     in.ThreadID,
		"messageId":    NewID(),
		"text":         in.Text,
		"attachments":  in.Attachments,
		"createdBy":    "user",
		"creationSource": "mobile",
		"commandId":    NewID(),
	}
	if in.ModelSelection != nil {
		cmd["modelSelection"] = in.ModelSelection
	}
	if in.Context != nil {
		cmd["context"] = in.Context
	}
	if in.TitleSeed != "" {
		cmd["titleSeed"] = in.TitleSeed
	}
	switch in.Mode {
	case "", DispatchStartImmediately:
		cmd["dispatchMode"] = map[string]any{"type": "start_immediately"}
	case DispatchQueueAfterActive:
		cmd["dispatchMode"] = map[string]any{"type": "queue_after_active"}
	case DispatchSteerActive:
		target := in.TargetRunID
		if target == "" {
			return 0, fmt.Errorf("steer_active requires TargetRunID")
		}
		cmd["dispatchMode"] = map[string]any{"type": "steer_active", "targetRunId": target}
	}
	return c.Dispatch(ctx, cmd)
}

// ThreadLaunchInput creates and starts a new thread.
type ThreadLaunchInput struct {
	ProjectID      string
	Title          string
	ModelSelection ModelSelection
	RuntimeMode    string
	InteractionMode string
	Workspace      WorkspaceStrategy
	InitialMessage *string
	Attachments    []json.RawMessage
	ThreadID       string // optional client-chosen id
	GenerateTitle  bool
}

// WorkspaceStrategy selects where a new thread's worktree lives.
type WorkspaceStrategy struct {
	Type            string  `json:"type"` // root, existing_worktree, worktree
	Branch          *string `json:"branch,omitempty"`
	WorktreePath    *string `json:"worktreePath,omitempty"`
	BaseRef         *string `json:"baseRef,omitempty"`
	StartFromOrigin *bool   `json:"startFromOrigin,omitempty"`
}

// LaunchThread creates a thread, optionally sending its first message.
func (c *Client) LaunchThread(ctx context.Context, in ThreadLaunchInput) (*ThreadLaunchResult, error) {
	if in.ProjectID == "" {
		return nil, fmt.Errorf("projectId is required")
	}
	if in.Title == "" {
		in.Title = "New thread"
	}
	if in.RuntimeMode == "" {
		in.RuntimeMode = "full-access"
	}
	if in.InteractionMode == "" {
		in.InteractionMode = "default"
	}
	if in.Workspace.Type == "" {
		in.Workspace = WorkspaceStrategy{Type: "root"}
	}
	payload := map[string]any{
		"commandId":      NewID(),
		"creationSource": "mobile",
		"projectId":      in.ProjectID,
		"title":          in.Title,
		"modelSelection": in.ModelSelection,
		"runtimeMode":    in.RuntimeMode,
		"interactionMode": in.InteractionMode,
		"workspaceStrategy": in.Workspace,
	}
	if in.ThreadID != "" {
		payload["threadId"] = in.ThreadID
	}
	if in.InitialMessage != nil {
		atts := in.Attachments
		if atts == nil {
			atts = []json.RawMessage{}
		}
		payload["initialMessage"] = map[string]any{
			"messageId":   NewID(),
			"text":        *in.InitialMessage,
			"attachments": atts,
		}
	}
	if in.GenerateTitle {
		payload["generateTitle"] = true
	}
	var out ThreadLaunchResult
	if err := c.call(ctx, "orchestration.launchThread", payload, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ThreadLaunchResult is returned by LaunchThread.
type ThreadLaunchResult struct {
	ThreadID   string           `json:"threadId"`
	Projection ThreadProjection `json:"projection"`
	Resumed    bool             `json:"resumed"`
}

// StopRun interrupts the active run of a thread.
func (c *Client) StopRun(ctx context.Context, threadID, runID, reason string) error {
	cmd := Command{"type": "run.interrupt", "commandId": NewID(), "threadId": threadID}
	if runID != "" {
		cmd["runId"] = runID
	}
	if reason != "" {
		cmd["reason"] = reason
	}
	_, err := c.Dispatch(ctx, cmd)
	return err
}

// StopThread interrupts the thread and any running work.
func (c *Client) StopThread(ctx context.Context, threadID, reason string) error {
	cmd := Command{"type": "thread.stop", "commandId": NewID(), "threadId": threadID}
	if reason != "" {
		cmd["reason"] = reason
	}
	_, err := c.Dispatch(ctx, cmd)
	return err
}

// RespondToRuntimeRequest answers an approval or user-input request.
func (c *Client) RespondToRuntimeRequest(ctx context.Context, threadID, requestID string, decision string, feedback string, answers map[string]any) error {
	cmd := Command{
		"type":      "runtime-request.respond",
		"commandId": NewID(),
		"threadId":  threadID,
		"requestId": requestID,
	}
	if decision != "" {
		cmd["decision"] = decision
	}
	if feedback != "" {
		cmd["feedback"] = feedback
	}
	if answers != nil {
		cmd["answers"] = answers
	}
	_, err := c.Dispatch(ctx, cmd)
	return err
}

// DismissUserInput dismisses a pending user-input request.
func (c *Client) DismissUserInput(ctx context.Context, threadID, requestID string) error {
	_, err := c.Dispatch(ctx, Command{
		"type":      "thread.user-input.dismiss",
		"commandId": NewID(),
		"threadId":  threadID,
		"requestId": requestID,
	})
	return err
}

// ArchiveThread archives a thread.
func (c *Client) ArchiveThread(ctx context.Context, threadID string) error {
	_, err := c.Dispatch(ctx, Command{"type": "thread.archive", "commandId": NewID(), "threadId": threadID})
	return err
}

// UnarchiveThread restores an archived thread.
func (c *Client) UnarchiveThread(ctx context.Context, threadID string) error {
	_, err := c.Dispatch(ctx, Command{"type": "thread.unarchive", "commandId": NewID(), "threadId": threadID})
	return err
}

// DeleteThread deletes a thread.
func (c *Client) DeleteThread(ctx context.Context, threadID string) error {
	_, err := c.Dispatch(ctx, Command{"type": "thread.delete", "commandId": NewID(), "threadId": threadID})
	return err
}

// RenameThread updates a thread title.
func (c *Client) RenameThread(ctx context.Context, threadID, title string) error {
	return c.UpdateThreadMetadata(ctx, threadID, map[string]any{"title": title})
}

// UpdateThreadMetadata applies arbitrary thread.metadata.update fields.
func (c *Client) UpdateThreadMetadata(ctx context.Context, threadID string, fields map[string]any) error {
	cmd := Command{"type": "thread.metadata.update", "commandId": NewID(), "threadId": threadID}
	for k, v := range fields {
		cmd[k] = v
	}
	_, err := c.Dispatch(ctx, cmd)
	return err
}

// SetModelSelection changes a thread's model.
func (c *Client) SetModelSelection(ctx context.Context, threadID string, sel ModelSelection) error {
	_, err := c.Dispatch(ctx, Command{
		"type": "thread.model-selection.set", "commandId": NewID(),
		"threadId": threadID, "modelSelection": sel,
	})
	return err
}

// SetRuntimeMode changes the approval mode for a thread.
func (c *Client) SetRuntimeMode(ctx context.Context, threadID, mode string) error {
	_, err := c.Dispatch(ctx, Command{
		"type": "thread.runtime-mode.set", "commandId": NewID(),
		"threadId": threadID, "runtimeMode": mode,
	})
	return err
}

// SetInteractionMode switches between default and plan mode.
func (c *Client) SetInteractionMode(ctx context.Context, threadID, mode string) error {
	_, err := c.Dispatch(ctx, Command{
		"type": "thread.interaction-mode.set", "commandId": NewID(),
		"threadId": threadID, "interactionMode": mode,
	})
	return err
}

// MarkThreadRead records a visit at the given time.
func (c *Client) MarkThreadRead(ctx context.Context, threadID string) error {
	_, err := c.Dispatch(ctx, Command{
		"type": "thread.visit", "commandId": NewID(),
		"threadId": threadID, "visitedAt": time.Now().UTC().Format(time.RFC3339Nano),
	})
	return err
}

// MarkThreadUnread marks a thread unread.
func (c *Client) MarkThreadUnread(ctx context.Context, threadID string) error {
	_, err := c.Dispatch(ctx, Command{"type": "thread.mark-unread", "commandId": NewID(), "threadId": threadID})
	return err
}

// SnoozeThread snoozes a thread until the given time.
func (c *Client) SnoozeThread(ctx context.Context, threadID string, until time.Time) error {
	_, err := c.Dispatch(ctx, Command{
		"type": "thread.snooze", "commandId": NewID(),
		"threadId": threadID, "snoozedUntil": until.UTC().Format(time.RFC3339Nano),
	})
	return err
}

// UnsnoozeThread clears a snooze.
func (c *Client) UnsnoozeThread(ctx context.Context, threadID string) error {
	_, err := c.Dispatch(ctx, Command{
		"type": "thread.unsnooze", "commandId": NewID(), "threadId": threadID, "reason": "user",
	})
	return err
}

// SettleThread marks a thread settled.
func (c *Client) SettleThread(ctx context.Context, threadID string) error {
	_, err := c.Dispatch(ctx, Command{"type": "thread.settle", "commandId": NewID(), "threadId": threadID})
	return err
}

// UnsettleThread marks a thread active again.
func (c *Client) UnsettleThread(ctx context.Context, threadID string) error {
	_, err := c.Dispatch(ctx, Command{
		"type": "thread.unsettle", "commandId": NewID(), "threadId": threadID, "reason": "user",
	})
	return err
}

// PinThread pins a thread; an empty orderKey appends it.
func (c *Client) PinThread(ctx context.Context, threadID, orderKey string) error {
	cmd := Command{"type": "thread.pin", "commandId": NewID(), "threadId": threadID}
	if orderKey != "" {
		cmd["orderKey"] = orderKey
	}
	_, err := c.Dispatch(ctx, cmd)
	return err
}

// UnpinThread unpins a thread.
func (c *Client) UnpinThread(ctx context.Context, threadID string) error {
	_, err := c.Dispatch(ctx, Command{"type": "thread.unpin", "commandId": NewID(), "threadId": threadID})
	return err
}

// CancelQueuedRun removes a queued run from a thread.
func (c *Client) CancelQueuedRun(ctx context.Context, threadID, runID string) error {
	_, err := c.Dispatch(ctx, Command{
		"type": "queued-run.cancel", "commandId": NewID(), "threadId": threadID, "runId": runID,
	})
	return err
}

// StartQueuedRun starts a queued run immediately.
func (c *Client) StartQueuedRun(ctx context.Context, threadID, runID string) error {
	_, err := c.Dispatch(ctx, Command{
		"type": "queued-run.start", "commandId": NewID(), "threadId": threadID, "runId": runID,
	})
	return err
}

// EditQueuedRun rewrites the text of a queued message.
func (c *Client) EditQueuedRun(ctx context.Context, threadID, runID, text string) error {
	_, err := c.Dispatch(ctx, Command{
		"type": "queued-run.edit", "commandId": NewID(), "threadId": threadID, "runId": runID, "text": text,
	})
	return err
}

// ResumeQueue releases a held queue.
func (c *Client) ResumeQueue(ctx context.Context, threadID string) error {
	_, err := c.Dispatch(ctx, Command{
		"type": "queue.resume", "commandId": NewID(), "threadId": threadID,
	})
	return err
}

// RollbackCheckpoint restores files to a checkpoint.
func (c *Client) RollbackCheckpoint(ctx context.Context, threadID, scopeID, checkpointID string, restoreFiles bool) error {
	_, err := c.Dispatch(ctx, Command{
		"type": "checkpoint.rollback", "commandId": NewID(), "threadId": threadID,
		"scopeId": scopeID, "checkpointId": checkpointID, "restoreFiles": restoreFiles,
	})
	return err
}

// SearchThreads searches thread titles and message text.
type ThreadSearchResult struct {
	Threads []json.RawMessage `json:"threads"`
}

// SearchThreads runs a server-side thread search. Query shape follows the
// server's orchestration.searchThreads input.
func (c *Client) SearchThreads(ctx context.Context, query string, limit int) (json.RawMessage, error) {
	payload := map[string]any{"query": query}
	if limit > 0 {
		payload["limit"] = limit
	}
	return c.callRaw(ctx, "orchestration.searchThreads", payload)
}
