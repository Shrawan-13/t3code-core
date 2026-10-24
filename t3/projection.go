package t3

import (
	"encoding/json"
	"sort"
	"strings"
)

// Run is a queued or executed agent run in a thread.
type Run struct {
	ID               string  `json:"id"`
	ThreadID         string  `json:"threadId"`
	Ordinal          int     `json:"ordinal"`
	ProviderInstanceID string `json:"providerInstanceId"`
	Status           string  `json:"status"`
	UserMessageID    string  `json:"userMessageId"`
	RootNodeID       *string `json:"rootNodeId"`
	ActiveAttemptID  *string `json:"activeAttemptId"`
	QueuePosition    *int    `json:"queuePosition"`
	QueueHeld        *bool   `json:"queueHeld"`
	RequestedAt      string  `json:"requestedAt"`
	StartedAt        *string `json:"startedAt"`
	CompletedAt      *string `json:"completedAt"`
}

// RunAttempt is one attempt to execute a run.
type RunAttempt struct {
	ID           string  `json:"id"`
	RunID        string  `json:"runId"`
	AttemptOrdinal int   `json:"attemptOrdinal"`
	RootNodeID   string  `json:"rootNodeId"`
	Status       string  `json:"status"`
	ProviderThreadID string `json:"providerThreadId"`
}

var unfinishedRunStatuses = map[string]bool{
	"preparing": true,
	"queued":    true,
	"starting":  true,
	"running":   true,
	"waiting":   true,
}

// DomainEvent is a decoded thread stream event.
type DomainEvent struct {
	Type       string          `json:"type"`
	ThreadID   string          `json:"threadId"`
	RunID      *string         `json:"runId,omitempty"`
	NodeID     *string         `json:"nodeId,omitempty"`
	OccurredAt string          `json:"occurredAt,omitempty"`
	Payload    json.RawMessage `json:"payload"`
}

// ThreadState is a live thread projection plus its resume cursor.
type ThreadState struct {
	Projection    ThreadProjection
	Sequence      int64
	HistoryCursor *string
	HasMoreHistory bool
	Partial       bool
}

// Apply merges one domain event into the projection.
func (s *ThreadState) Apply(ev DomainEvent, partial bool, latestLocalOrdinal *int) {
	if ev.ThreadID != "" && s.Projection.Thread.ID != "" && ev.ThreadID != s.Projection.Thread.ID {
		return
	}
	if ev.OccurredAt != "" {
		s.Projection.UpdatedAt = ev.OccurredAt
	}
	switch ev.Type {
	case "thread.created", "thread.archived", "thread.unarchived", "thread.deleted",
		"thread.settled", "thread.unsettled", "thread.snoozed", "thread.unsnoozed",
		"thread.pinned", "thread.auto-settle-set", "thread.unpinned", "thread.pin-reordered",
		"thread.active-reordered", "thread.visited", "thread.marked-unread",
		"thread.metadata-updated", "thread.pull-request-synced",
		"thread.runtime-mode-updated", "thread.interaction-mode-updated",
		"thread.model-selection-updated", "thread.provider-switched":
		var shell ThreadShell
		if json.Unmarshal(ev.Payload, &shell) == nil && shell.ID != "" {
			s.Projection.Thread = shell
		}
	case "run.created", "run.updated":
		var run Run
		if json.Unmarshal(ev.Payload, &run) == nil && run.ID != "" {
			s.Projection.Runs = upsertRaw(s.Projection.Runs, run.ID, ev.Payload)
			s.renumberVisible()
		}
	case "run-attempt.created", "run-attempt.updated":
		var attempt RunAttempt
		if json.Unmarshal(ev.Payload, &attempt) == nil && attempt.ID != "" {
			s.Projection.Attempts = upsertRaw(s.Projection.Attempts, attempt.ID, ev.Payload)
			s.renumberVisible()
		}
	case "node.updated":
		s.Projection.Nodes = upsertRaw(s.Projection.Nodes, idOf(ev.Payload), ev.Payload)
	case "subagent.updated":
		s.Projection.Subagents = upsertRaw(s.Projection.Subagents, idOf(ev.Payload), ev.Payload)
	case "provider-session.attached", "provider-session.updated":
		s.Projection.ProviderSessions = upsertRaw(s.Projection.ProviderSessions, idOf(ev.Payload), ev.Payload)
	case "provider-session.detached":
		var d struct {
			ProviderSessionID string `json:"providerSessionId"`
		}
		if json.Unmarshal(ev.Payload, &d) == nil {
			s.Projection.ProviderSessions = removeRaw(s.Projection.ProviderSessions, d.ProviderSessionID)
		}
	case "provider-thread.updated":
		s.Projection.ProviderThreads = upsertRaw(s.Projection.ProviderThreads, idOf(ev.Payload), ev.Payload)
	case "provider-turn.updated":
		s.Projection.ProviderTurns = upsertRaw(s.Projection.ProviderTurns, idOf(ev.Payload), ev.Payload)
	case "runtime-request.updated":
		var req RuntimeRequest
		if json.Unmarshal(ev.Payload, &req) == nil && req.ID != "" {
			s.Projection.RuntimeRequests = upsertRequest(s.Projection.RuntimeRequests, req)
		}
	case "message.updated":
		var msg Message
		if json.Unmarshal(ev.Payload, &msg) == nil && msg.ID != "" {
			s.Projection.Messages = upsertMessage(s.Projection.Messages, msg)
		}
	case "plan.updated":
		s.Projection.Plans = upsertRaw(s.Projection.Plans, idOf(ev.Payload), ev.Payload)
	case "checkpoint-scope.created":
		s.Projection.CheckpointScopes = upsertRaw(s.Projection.CheckpointScopes, idOf(ev.Payload), ev.Payload)
	case "checkpoint.captured":
		s.Projection.Checkpoints = upsertRaw(s.Projection.Checkpoints, idOf(ev.Payload), ev.Payload)
	case "context-handoff.updated":
		s.Projection.ContextHandoffs = upsertRaw(s.Projection.ContextHandoffs, idOf(ev.Payload), ev.Payload)
	case "context-transfer.created", "context-transfer.updated":
		s.Projection.ContextTransfers = upsertRaw(s.Projection.ContextTransfers, idOf(ev.Payload), ev.Payload)
	case "turn-item.updated":
		var item TurnItem
		if json.Unmarshal(ev.Payload, &item) == nil && item.ID != "" {
			s.applyTurnItem(item, partial, latestLocalOrdinal)
		}
	}
}

func (s *ThreadState) applyTurnItem(item TurnItem, partial bool, latestLocalOrdinal *int) {
	idx := -1
	prevType := ""
	for i := len(s.Projection.TurnItems) - 1; i >= 0; i-- {
		if s.Projection.TurnItems[i].ID == item.ID {
			idx = i
			prevType = s.Projection.TurnItems[i].Type
			break
		}
	}
	if idx == -1 {
		s.Projection.TurnItems = append(s.Projection.TurnItems, item)
	} else {
		s.Projection.TurnItems[idx] = item
	}
	if item.Type == "run_interrupt_request" || prevType == "run_interrupt_request" {
		s.renumberVisible()
	}
	if s.isVisible(item) {
		s.upsertVisible(item, partial, latestLocalOrdinal)
	} else {
		s.removeVisible(item.ID)
	}
}

func (s *ThreadState) runsByID() map[string]string {
	m := make(map[string]string, len(s.Projection.Runs))
	for _, raw := range s.Projection.Runs {
		var r Run
		if json.Unmarshal(raw, &r) == nil {
			m[r.ID] = r.Status
		}
	}
	return m
}

func (s *ThreadState) isVisible(item TurnItem) bool {
	statuses := s.runsByID()
	if item.RunID != "" {
		if statuses[item.RunID] == "rolled_back" {
			return false
		}
		if statuses[item.RunID] == "cancelled" && item.Type == "user_message" && inputIntent(item.Raw) == "queued_turn" {
			return false
		}
	}
	// Superseded interrupt results are hidden unless the run has any interrupt request.
	if item.Type == "run_interrupt_result" && item.RunID != "" && item.NodeID != "" {
		superseded := false
		for _, raw := range s.Projection.Attempts {
			var a RunAttempt
			if json.Unmarshal(raw, &a) == nil && a.RunID == item.RunID && a.Status == "superseded" && a.RootNodeID == item.NodeID {
				superseded = true
				break
			}
		}
		if superseded && !s.hasInterruptRequest(item.RunID) {
			return false
		}
	}
	return true
}

func (s *ThreadState) hasInterruptRequest(runID string) bool {
	for _, it := range s.Projection.TurnItems {
		if it.RunID == runID && it.Type == "run_interrupt_request" {
			return true
		}
	}
	return false
}

func (s *ThreadState) renumberVisible() {
	rows := s.Projection.VisibleTurnItems
	next := make([]VisibleTurnItem, 0, len(rows))
	for _, row := range rows {
		if row.Visibility == "local" && !s.isVisible(row.Item) {
			continue
		}
		row.Position = len(next)
		next = append(next, row)
	}
	s.Projection.VisibleTurnItems = next
}

func (s *ThreadState) upsertVisible(item TurnItem, partial bool, latestLocalOrdinal *int) {
	rows := s.Projection.VisibleTurnItems
	idx := -1
	for i := len(rows) - 1; i >= 0; i-- {
		if rows[i].SourceItemID == item.ID {
			idx = i
			break
		}
	}
	if idx >= 0 {
		rows[idx].Item = item
		s.Projection.VisibleTurnItems = rows
		return
	}
	if partial && s.shouldDropMissingPartialTurnItem(item, latestLocalOrdinal) {
		return
	}
	next := VisibleTurnItem{
		Visibility:     "local",
		SourceThreadID: item.ThreadID,
		SourceItemID:   item.ID,
		Item:           item,
	}
	insertion := len(rows)
	for i, row := range rows {
		if row.Visibility != "local" {
			continue
		}
		if row.Item.Ordinal > item.Ordinal || (row.Item.Ordinal == item.Ordinal && row.Item.ID > item.ID) {
			insertion = i
			break
		}
	}
	rows = append(rows, VisibleTurnItem{})
	copy(rows[insertion+1:], rows[insertion:])
	rows[insertion] = next
	for i := range rows {
		rows[i].Position = i
	}
	s.Projection.VisibleTurnItems = rows
}

func (s *ThreadState) shouldDropMissingPartialTurnItem(item TurnItem, latestLocalOrdinal *int) bool {
	for _, row := range s.Projection.VisibleTurnItems {
		if row.SourceItemID == item.ID {
			return false
		}
	}
	statuses := s.runsByID()
	if item.RunID == "" {
		return s.isBelowPartialWindow(item, latestLocalOrdinal)
	}
	if unfinishedRunStatuses[statuses[item.RunID]] {
		return false
	}
	for _, row := range s.Projection.VisibleTurnItems {
		if row.Visibility == "local" && row.Item.RunID == item.RunID && row.Item.Ordinal <= item.Ordinal {
			return false
		}
	}
	return s.isBelowPartialWindow(item, latestLocalOrdinal)
}

func (s *ThreadState) isBelowPartialWindow(item TurnItem, latestLocalOrdinal *int) bool {
	if latestLocalOrdinal != nil && item.Ordinal <= *latestLocalOrdinal {
		return true
	}
	oldest := -1
	for _, row := range s.Projection.VisibleTurnItems {
		if row.Visibility != "local" {
			continue
		}
		if oldest == -1 || row.Item.Ordinal < oldest {
			oldest = row.Item.Ordinal
		}
	}
	return oldest != -1 && item.Ordinal < oldest
}

func (s *ThreadState) removeVisible(itemID string) {
	rows := s.Projection.VisibleTurnItems
	idx := -1
	for i, row := range rows {
		if row.SourceItemID == itemID {
			idx = i
			break
		}
	}
	if idx < 0 {
		return
	}
	rows = append(rows[:idx], rows[idx+1:]...)
	for i := range rows {
		rows[i].Position = i
	}
	s.Projection.VisibleTurnItems = rows
}

func idOf(raw json.RawMessage) string {
	var v struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(raw, &v)
	return v.ID
}

func inputIntent(raw json.RawMessage) string {
	var v struct {
		InputIntent string `json:"inputIntent"`
	}
	_ = json.Unmarshal(raw, &v)
	return v.InputIntent
}

func upsertRaw(items []json.RawMessage, id string, raw json.RawMessage) []json.RawMessage {
	for i, existing := range items {
		if idOf(existing) == id {
			next := make([]json.RawMessage, len(items))
			copy(next, items)
			next[i] = append(json.RawMessage(nil), raw...)
			return next
		}
	}
	return append(items, append(json.RawMessage(nil), raw...))
}

func removeRaw(items []json.RawMessage, id string) []json.RawMessage {
	out := items[:0]
	for _, existing := range items {
		if idOf(existing) != id {
			out = append(out, existing)
		}
	}
	return out
}

func upsertMessage(items []Message, msg Message) []Message {
	for i := range items {
		if items[i].ID == msg.ID {
			next := make([]Message, len(items))
			copy(next, items)
			next[i] = msg
			return next
		}
	}
	return append(items, msg)
}

func upsertRequest(items []RuntimeRequest, req RuntimeRequest) []RuntimeRequest {
	for i := range items {
		if items[i].ID == req.ID {
			next := make([]RuntimeRequest, len(items))
			copy(next, items)
			next[i] = req
			return next
		}
	}
	return append(items, req)
}

// SortVisibleByPosition returns rows ordered by position.
func SortVisibleByPosition(rows []VisibleTurnItem) []VisibleTurnItem {
	out := make([]VisibleTurnItem, len(rows))
	copy(out, rows)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Position < out[j].Position })
	return out
}

// TranscriptText flattens visible rows into plain text for notifications or
// accessibility. Tool output is omitted.
func TranscriptText(rows []VisibleTurnItem) string {
	var b strings.Builder
	for _, row := range SortVisibleByPosition(rows) {
		switch row.Item.Type {
		case "user_message", "assistant_message", "reasoning":
			if row.Item.Text != "" {
				b.WriteString(row.Item.Text)
				b.WriteString("\n")
			}
		}
	}
	return b.String()
}
