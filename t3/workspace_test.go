package t3_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/t3tools/t3go/t3"
)

func tempRepo(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("workspace root: %v", err)
	}
	dir, err := os.MkdirTemp(root, ".t3wstest-")
	if err != nil {
		t.Fatalf("temp repo: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	runGit(t, dir, "init", "-q", "-b", "main")
	runGit(t, dir, "config", "user.email", "test@example.com")
	runGit(t, dir, "config", "user.name", "T3 Test")
	if err := os.WriteFile(filepath.Join(dir, "app.txt"), []byte("alpha\nbeta\ngamma\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	runGit(t, dir, "add", "app.txt")
	runGit(t, dir, "commit", "-q", "-m", "initial")
	return dir
}

func TestFilesProjectsAttachments(t *testing.T) {
	c := liveClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	dir := tempRepo(t)
	title := "go-workspace-test-" + time.Now().Format("150405")

	project, err := c.CreateProject(ctx, title, dir, true, nil)
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	if project.ID == "" || project.WorkspaceRoot != dir {
		t.Fatalf("unexpected project: %+v", project)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := c.DeleteProject(cleanup, project.ID, true); err != nil {
			t.Logf("cleanup project: %v", err)
		}
	})

	snap, err := c.ProjectSnapshot(ctx)
	if err != nil {
		t.Fatalf("project snapshot: %v", err)
	}
	found := false
	for _, p := range snap.Projects {
		if p.ID == project.ID {
			found = true
		}
	}
	if !found {
		t.Fatalf("created project missing from snapshot")
	}

	entries, err := c.ListEntries(ctx, dir, "")
	if err != nil {
		t.Fatalf("list entries: %v", err)
	}
	if len(entries.Entries) == 0 {
		t.Fatalf("expected entries")
	}
	read, err := c.ReadFile(ctx, dir, "app.txt", false)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if !strings.Contains(read.Contents, "beta") {
		t.Fatalf("unexpected contents: %+v", read)
	}
	if err := c.WriteFile(ctx, dir, "new.txt", "hello search\n"); err != nil {
		t.Fatalf("write file: %v", err)
	}
	search, err := c.SearchEntries(ctx, dir, "new", 10)
	if err != nil {
		t.Fatalf("search entries: %v", err)
	}
	if len(search.Entries) == 0 {
		t.Fatalf("expected search hit")
	}
	hits, err := c.SearchContents(ctx, dir, "hello search", 10, false, false, false)
	if err != nil {
		t.Fatalf("search contents: %v", err)
	}
	if len(hits.Matches) == 0 {
		t.Fatalf("expected content match: %+v", hits)
	}
	meta, err := c.FileMetadata(ctx, dir)
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	if meta == nil || meta.Path == "" {
		t.Fatalf("empty metadata")
	}
	if _, err := c.BrowseFiles(ctx, dir, dir); err != nil {
		t.Fatalf("browse: %v", err)
	}
	caps := c.Config().Environment.TypedCapabilities()
	if caps.IgnoredFileListing {
		if _, err := c.ListIgnoredFiles(ctx, dir); err != nil {
			t.Fatalf("ignored files: %v", err)
		}
	}

	att, err := c.UploadAttachment(ctx, "file", "t3-core-test.txt", "text/plain", []byte("attachment body\n"))
	if err != nil {
		t.Fatalf("upload attachment: %v", err)
	}
	if att.ID == "" || att.SizeBytes == 0 {
		t.Fatalf("unexpected attachment: %+v", att)
	}
	if err := c.DeleteAttachment(ctx, att.ID); err != nil {
		t.Fatalf("delete attachment: %v", err)
	}
}

func TestGitCommitRefsAndWorktree(t *testing.T) {
	c := liveClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	dir := tempRepo(t)

	if err := os.WriteFile(filepath.Join(dir, "app.txt"), []byte("alpha\nbeta\ngamma\ndelta\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	status, err := c.RefreshVcsStatus(ctx, dir)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if !status.HasWorkingTreeChanges {
		t.Fatalf("expected working tree changes")
	}

	if err := c.RunGitAction(ctx, dir, "commit", "go core commit test", false, nil); err != nil {
		t.Fatalf("commit action: %v", err)
	}
	out := runGitOutput(t, dir, "log", "-1", "--pretty=%s")
	if strings.TrimSpace(out) != "go core commit test" {
		t.Fatalf("unexpected commit subject %q", out)
	}
	clean, err := c.RefreshVcsStatus(ctx, dir)
	if err != nil {
		t.Fatalf("status after commit: %v", err)
	}
	if clean.HasWorkingTreeChanges {
		t.Fatalf("expected clean tree after commit")
	}

	if err := c.CreateRef(ctx, dir, "go-core-test-branch", true); err != nil {
		t.Fatalf("create ref: %v", err)
	}
	branch := strings.TrimSpace(runGitOutput(t, dir, "rev-parse", "--abbrev-ref", "HEAD"))
	if branch != "go-core-test-branch" {
		t.Fatalf("expected switched branch, got %q", branch)
	}
	if err := c.SwitchRef(ctx, dir, "main"); err != nil {
		t.Fatalf("switch ref: %v", err)
	}

	wtPath := dir + "-wt"
	t.Cleanup(func() { os.RemoveAll(wtPath) })
	if _, err := c.CreateWorktree(ctx, dir, "go-core-test-branch", "", "", wtPath); err != nil {
		t.Fatalf("create worktree: %v", err)
	}
	if _, err := os.Stat(filepath.Join(wtPath, "app.txt")); err != nil {
		t.Fatalf("worktree not populated: %v", err)
	}
	if err := c.RemoveWorktree(ctx, dir, wtPath, true); err != nil {
		t.Fatalf("remove worktree: %v", err)
	}
	if _, err := os.Stat(wtPath); !os.IsNotExist(err) {
		t.Logf("worktree path still present after removal: %v", err)
	}
}

func runGitOutput(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := runGitCmd(dir, args...)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git %v: %v", args, err)
	}
	return string(out)
}

var _ = t3.Options{}
