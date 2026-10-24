package t3_test

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/t3tools/t3go/t3"
)

// TestLaunchStreamTerminalAndDelete exercises the write path end to end. It is
// opt-in because it starts a real agent run on the server.
func TestLaunchStreamTerminalAndDelete(t *testing.T) {
	if os.Getenv("T3_TEST_WRITE") != "1" {
		t.Skip("set T3_TEST_WRITE=1 to run write-path tests")
	}
	c := liveClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	snap, err := c.ShellHTTP(ctx)
	if err != nil {
		t.Fatalf("shell: %v", err)
	}
	cfg := c.Config()
	var project *t3.Project
	for i := range snap.Projects {
		p := &snap.Projects[i]
		if p.WorkspaceRoot == cfg.ScratchWorkspaceRoot || strings.EqualFold(p.Title, "no project") {
			project = p
			break
		}
	}
	if project == nil {
		t.Skip("no scratch project available")
	}

	var instanceID, model string
	for _, p := range cfg.Providers {
		if !p.Enabled || !p.Installed || p.Status != "ready" || len(p.Models) == 0 {
			continue
		}
		candidate := p.Models[0].Key()
		if p.InstanceID == "opencode" {
			for _, m := range p.Models {
				if strings.Contains(m.Key(), "flash-free") || strings.Contains(m.Key(), "minimax") {
					candidate = m.Key()
					break
				}
			}
		}
		instanceID, model = p.InstanceID, candidate
		if p.InstanceID == "opencode" {
			break
		}
	}
	if instanceID == "" || model == "" {
		t.Skip("no installed provider with models")
	}
	t.Logf("using project=%s model=%s/%s", project.Title, instanceID, model)

	launchCtx, launchCancel := context.WithTimeout(ctx, 60*time.Second)
	title := "go core smoke test (auto-delete) " + time.Now().Format("150405")
	initial := "Reply with exactly: T3_CORE_OK. Do not call any tools."
	res, err := c.LaunchThread(launchCtx, t3.ThreadLaunchInput{
		ProjectID:      project.ID,
		Title:          title,
		ModelSelection: t3.ModelSelection{InstanceID: instanceID, Model: model},
		RuntimeMode:    "full-access",
		Workspace:      t3.WorkspaceStrategy{Type: "root"},
		InitialMessage: &initial,
	})
	launchCancel()
	if err != nil {
		t.Fatalf("launch: %v", err)
	}
	if res.ThreadID == "" {
		t.Fatalf("launch returned no thread id")
	}
	threadID := res.ThreadID
	t.Logf("launched thread %s", threadID)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cleanupCancel()
		_ = c.StopThread(cleanupCtx, threadID, "smoke test cleanup")
		_ = c.ArchiveThread(cleanupCtx, threadID)
		if err := c.DeleteThread(cleanupCtx, threadID); err != nil {
			t.Logf("cleanup delete thread: %v", err)
		}
	})

	w, err := c.WatchThread(ctx, threadID, -1)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	defer w.Close()
	state, err := w.WaitSnapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if state.Projection.Thread.ID != threadID {
		t.Fatalf("snapshot thread mismatch")
	}

	waitFor := func(what string, timeout time.Duration, cond func(t3.ThreadProjection) bool) t3.ThreadProjection {
		t.Helper()
		deadline := time.Now().Add(timeout)
		for {
			proj := w.Projection()
			if cond(proj) {
				return proj
			}
			if time.Now().After(deadline) {
				b, _ := json.Marshal(proj.Thread)
				t.Fatalf("timed out waiting for %s; thread=%s", what, b)
			}
			select {
			case ev, ok := <-w.Events():
				if !ok {
					t.Fatalf("thread stream closed while waiting for %s", what)
				}
				if ev.Err != "" {
					t.Logf("thread event error: %s", ev.Err)
				}
			case <-time.After(time.Second):
			}
		}
	}

	sawUser := waitFor("initial user message", 60*time.Second, func(p t3.ThreadProjection) bool {
		for _, m := range p.Messages {
			if m.Role == "user" && strings.Contains(m.Text, "T3_CORE_OK") {
				return true
			}
		}
		return false
	})
	if len(sawUser.VisibleTurnItems) == 0 {
		t.Fatalf("expected visible timeline rows")
	}
	final := waitFor("assistant reply", 10*time.Minute, func(p t3.ThreadProjection) bool {
		for _, m := range p.Messages {
			if m.Role == "assistant" && strings.TrimSpace(m.Text) != "" {
				return true
			}
		}
		return false
	})
	var assistantText strings.Builder
	for _, m := range final.Messages {
		if m.Role == "assistant" {
			assistantText.WriteString(m.Text)
		}
	}
	if !strings.Contains(assistantText.String(), "T3_CORE_OK") {
		t.Logf("assistant reply did not echo marker (model-dependent): %q", assistantText.String())
	}
	t.Logf("final status=%s messages=%d visible=%d", final.Thread.Status, len(final.Messages), len(final.VisibleTurnItems))

	// Terminal round trip on the same thread.
	termID := "term-smoke"
	if _, err := c.OpenTerminal(ctx, threadID, termID, project.WorkspaceRoot, 100, 30, ""); err != nil {
		t.Fatalf("open terminal: %v", err)
	}
	sub, err := c.AttachTerminal(ctx, threadID, termID, project.WorkspaceRoot, 100, 30, "")
	if err != nil {
		t.Fatalf("attach terminal: %v", err)
	}
	defer func() {
		sub.Close()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer closeCancel()
		_ = c.CloseTerminal(closeCtx, threadID, termID, true)
	}()
	if err := c.WriteTerminal(ctx, threadID, termID, "echo T3_TERM_OK\n"); err != nil {
		t.Fatalf("write terminal: %v", err)
	}
	if err := c.ResizeTerminal(ctx, threadID, termID, 90, 28); err != nil {
		t.Fatalf("resize terminal: %v", err)
	}
	termDeadline := time.After(30 * time.Second)
	var buf strings.Builder
	for strings.Count(buf.String(), "T3_TERM_OK") < 2 {
		select {
		case ev, ok := <-sub.Events:
			if !ok {
				t.Fatalf("terminal stream closed, output=%q", buf.String())
			}
			if ev.Type == "error" {
				t.Fatalf("terminal error: %s", ev.Message)
			}
			if ev.Type == "output" {
				buf.WriteString(ev.Data)
			}
		case <-termDeadline:
			t.Fatalf("terminal marker not seen, output=%q", buf.String())
		}
	}
	t.Logf("terminal output: %q", strings.TrimSpace(buf.String()))

	// A follow-up message must dispatch into the same thread.
	msgCtx, msgCancel := context.WithTimeout(ctx, 60*time.Second)
	seq, err := c.SendMessage(msgCtx, t3.SendMessageInput{
		ThreadID: threadID,
		Text:     "Reply with exactly: SECOND_OK",
		Mode:     t3.DispatchStartImmediately,
	})
	msgCancel()
	if err != nil {
		t.Fatalf("send message: %v", err)
	}
	if seq == 0 {
		t.Fatalf("send message returned zero sequence")
	}
}
