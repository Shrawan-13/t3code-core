package t3_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/t3tools/t3go/t3"
)

func liveClient(t *testing.T) *t3.Client {
	t.Helper()
	base := os.Getenv("T3_TEST_BASE")
	if base == "" {
		base = "http://127.0.0.1:3773"
	}
	token := os.Getenv("T3_TEST_TOKEN")
	if token == "" {
		if data, err := os.ReadFile("../../.secrets/token.txt"); err == nil {
			token = strings.TrimSpace(string(data))
		}
	}
	if token == "" {
		t.Skip("T3_TEST_TOKEN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	c, err := t3.Connect(ctx, t3.Options{BaseURL: base, Token: token, ClientLabel: "go-integration-test"})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func TestDescriptorAndConfig(t *testing.T) {
	c := liveClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	desc, err := c.Descriptor(ctx)
	if err != nil {
		t.Fatalf("descriptor: %v", err)
	}
	if desc.EnvironmentID == "" || desc.ServerVersion == "" {
		t.Fatalf("incomplete descriptor: %+v", desc)
	}
	cfg := c.Config()
	if cfg == nil || len(cfg.Providers) == 0 {
		t.Fatalf("expected providers in config")
	}
	if cfg.Environment.OrchestrationProtocolVersion != 2 {
		t.Fatalf("expected protocol 2, got %d", cfg.Environment.OrchestrationProtocolVersion)
	}
	session, err := c.Session(ctx)
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	if !session.Authenticated {
		t.Fatalf("expected authenticated session: %+v", session)
	}
}

func TestShellHTTPAndStream(t *testing.T) {
	c := liveClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	snap, err := c.ShellHTTP(ctx)
	if err != nil {
		t.Fatalf("shell http: %v", err)
	}
	if snap.SnapshotSequence == 0 {
		t.Fatalf("expected non-zero snapshot sequence")
	}
	if len(snap.Projects) == 0 {
		t.Fatalf("expected at least one project")
	}
	if len(snap.Threads) == 0 {
		t.Fatalf("expected at least one thread")
	}
	for _, th := range snap.Threads {
		if th.ID == "" || th.ProjectID == "" || th.Title == "" {
			t.Fatalf("thread missing required fields: %+v", th)
		}
	}

	w, err := c.WatchShell(ctx)
	if err != nil {
		t.Fatalf("watch shell: %v", err)
	}
	defer w.Close()
	initial, err := w.WaitSnapshot(ctx)
	if err != nil {
		t.Fatalf("wait snapshot: %v", err)
	}
	if len(initial.Threads) != len(snap.Threads) && len(snap.Threads)-len(initial.Threads) > 1 {
		t.Logf("thread count moved between HTTP and WS: %d -> %d", len(snap.Threads), len(initial.Threads))
	}
	if initial.SnapshotSequence < snap.SnapshotSequence {
		t.Fatalf("stream snapshot older than HTTP snapshot")
	}
	current := w.Snapshot()
	if len(current.Projects) != len(initial.Projects) {
		t.Fatalf("cached projects drifted")
	}
}

func TestThreadFetchAndWatch(t *testing.T) {
	c := liveClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	snap, err := c.ShellHTTP(ctx)
	if err != nil {
		t.Fatalf("shell: %v", err)
	}
	var threadID string
	for _, th := range snap.Threads {
		if len(th.Title) > 0 {
			threadID = th.ID
			break
		}
	}
	if threadID == "" {
		t.Skip("no threads")
	}

	detail, err := c.ThreadHTTP(ctx, threadID)
	if err != nil {
		t.Fatalf("thread http: %v", err)
	}
	proj := detail.Projection
	if proj.Thread.ID != threadID {
		t.Fatalf("thread id mismatch: %s", proj.Thread.ID)
	}
	if len(proj.VisibleTurnItems) > 0 {
		last := -1
		for _, row := range proj.VisibleTurnItems {
			if row.Position < last {
				t.Fatalf("visible items not ordered by position")
			}
			last = row.Position
			if row.Item.ID == "" || row.Item.Type == "" {
				t.Fatalf("visible row missing item id/type")
			}
		}
	}

	// RPC projection must agree with the HTTP detail snapshot.
	rpcProj, err := c.GetThread(ctx, threadID)
	if err != nil {
		t.Fatalf("get thread rpc: %v", err)
	}
	if rpcProj.Thread.ID != threadID {
		t.Fatalf("rpc projection thread id mismatch")
	}
	if len(rpcProj.Messages) != len(proj.Messages) {
		t.Logf("message count differs between endpoints: rpc=%d http=%d", len(rpcProj.Messages), len(proj.Messages))
	}

	w, err := c.WatchThread(ctx, threadID, -1)
	if err != nil {
		t.Fatalf("watch thread: %v", err)
	}
	defer w.Close()
	state, err := w.WaitSnapshot(ctx)
	if err != nil {
		t.Fatalf("thread snapshot: %v", err)
	}
	if state.Projection.Thread.ID != threadID {
		t.Fatalf("watched thread id mismatch")
	}
	if len(state.Projection.Messages) == 0 && len(proj.Messages) > 0 {
		t.Fatalf("watch snapshot lost messages")
	}
	jsonState, err := w.StateJSON()
	if err != nil || !json.Valid(jsonState) {
		t.Fatalf("state json invalid: %v", err)
	}

	if detail.HasMoreHistory != nil && *detail.HasMoreHistory && detail.HistoryCursor != nil {
		page, err := c.ThreadHistory(ctx, threadID, *detail.HistoryCursor)
		if err != nil {
			t.Fatalf("history: %v", err)
		}
		if len(page.Items) == 0 {
			t.Fatalf("expected history items")
		}
	}
}

func TestVcsAndDiff(t *testing.T) {
	c := liveClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	repo := os.Getenv("T3_TEST_REPO")
	if repo == "" {
		root, err := filepath.Abs("../..")
		if err != nil {
			t.Fatalf("workspace root: %v", err)
		}
		repo, err = os.MkdirTemp(root, ".t3testrepo-")
		if err != nil {
			t.Fatalf("temp repo: %v", err)
		}
		t.Cleanup(func() { os.RemoveAll(repo) })
		runGit(t, repo, "init", "-q", "-b", "main")
		runGit(t, repo, "config", "user.email", "test@example.com")
		runGit(t, repo, "config", "user.name", "T3 Test")
		if err := os.WriteFile(repo+"/hello.txt", []byte("hello\n"), 0o644); err != nil {
			t.Fatalf("write file: %v", err)
		}
		runGit(t, repo, "add", "hello.txt")
		runGit(t, repo, "commit", "-q", "-m", "initial")
		if err := os.WriteFile(repo+"/hello.txt", []byte("hello world\n"), 0o644); err != nil {
			t.Fatalf("write file: %v", err)
		}
		if err := os.WriteFile(repo+"/new.txt", []byte("new\n"), 0o644); err != nil {
			t.Fatalf("write file: %v", err)
		}
	}
	status, err := c.RefreshVcsStatus(ctx, repo)
	if err != nil {
		t.Fatalf("vcs status: %v", err)
	}
	if !status.IsRepo {
		t.Fatalf("expected reference clone to be a repo: %+v", status)
	}
	if status.RefName == nil || *status.RefName == "" {
		t.Fatalf("expected a current ref")
	}
	preview, err := c.GetDiffPreview(ctx, repo, "", false)
	if err != nil {
		t.Fatalf("diff preview: %v", err)
	}
	if preview.Sources == nil {
		t.Fatalf("expected sources slice")
	}
	if len(preview.Sources) == 0 || preview.Sources[0].Diff == "" {
		t.Fatalf("expected a working-tree diff, got %+v", preview.Sources)
	}
	refs, err := c.ListRefs(ctx, repo, "", "local", 20)
	if err != nil {
		t.Fatalf("list refs: %v", err)
	}
	if !refs.IsRepo || len(refs.Refs) == 0 {
		t.Logf("ref list returned isRepo=%v refs=%d", refs.IsRepo, len(refs.Refs))
	}
}

func TestUsageSettingsAndTasks(t *testing.T) {
	c := liveClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	settings, err := c.GetSettings(ctx)
	if err != nil {
		t.Fatalf("settings: %v", err)
	}
	if !json.Valid(settings) {
		t.Fatalf("settings not valid json")
	}
	usage, err := c.GetUsageSummary(ctx, "", "", "UTC", "", false)
	if err != nil {
		t.Fatalf("usage: %v", err)
	}
	if usage.ContractVersion == 0 {
		t.Fatalf("usage contract version missing")
	}
	if _, err := c.ListScheduledTasks(ctx); err != nil {
		t.Fatalf("scheduled tasks: %v", err)
	}
	cfg := c.Config()
	if cfg.ScratchWorkspaceRoot == "" {
		t.Fatalf("scratch workspace root missing")
	}
}

func TestGenericCallError(t *testing.T) {
	c := liveClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err := c.CallMethod(ctx, "does.not.exist", map[string]any{})
	if err == nil {
		t.Fatalf("expected an error for unknown method")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "unknown") && !strings.Contains(err.Error(), "Rpc") {
		t.Logf("unexpected error shape: %v", err)
	}
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	if out, err := runGitCmd(dir, args...).CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

func runGitCmd(dir string, args ...string) *exec.Cmd {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	return cmd
}

func TestPairingFlow(t *testing.T) {
	c := liveClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	label := "go-integration-pair-" + time.Now().Format("150405")
	cred, err := c.CreatePairingCredential(ctx, label, nil)
	if err != nil {
		t.Fatalf("create pairing credential: %v", err)
	}
	if cred.ID == "" || cred.Credential == "" || cred.ExpiresAt == "" {
		t.Fatalf("incomplete pairing credential: %+v", cred)
	}
	t.Cleanup(func() {
		revokeCtx, revokeCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer revokeCancel()
		_ = c.RevokePairingLink(revokeCtx, cred.ID)
	})

	base := os.Getenv("T3_TEST_BASE")
	if base == "" {
		base = "http://127.0.0.1:3773"
	}
	tok, err := t3.Pair(ctx, t3.PairingInput{
		BaseURL: base, Code: cred.Credential, ClientLabel: label,
		DeviceType: "mobile", OS: "Android",
	})
	if err != nil {
		t.Fatalf("pair: %v", err)
	}
	if tok.AccessToken == "" || tok.TokenType != "Bearer" {
		t.Fatalf("unexpected token: %+v", tok)
	}

	pc, err := t3.Connect(ctx, t3.Options{BaseURL: base, Token: tok.AccessToken, ClientLabel: label})
	if err != nil {
		t.Fatalf("connect with paired token: %v", err)
	}
	defer pc.Close()
	desc, err := pc.Descriptor(ctx)
	if err != nil || desc.EnvironmentID == "" {
		t.Fatalf("paired descriptor: %v %+v", err, desc)
	}
	session, err := pc.Session(ctx)
	if err != nil || !session.Authenticated {
		t.Fatalf("paired session: %v", err)
	}

	sessions, err := c.ListSessions(ctx)
	if err != nil {
		t.Fatalf("list sessions: %v", err)
	}
	var pairedSessionID string
	for _, s := range sessions {
		if s.Client.Label == label {
			pairedSessionID = s.SessionID
			break
		}
	}
	if pairedSessionID == "" {
		t.Fatalf("paired session %q not found among %d sessions", label, len(sessions))
	}
	if err := c.RevokeSession(ctx, pairedSessionID); err != nil {
		t.Fatalf("revoke paired session: %v", err)
	}
	after, err := pc.Session(ctx)
	if err == nil && after.Authenticated {
		t.Fatalf("expected revoked token to be unauthenticated")
	}
}
