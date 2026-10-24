package t3

import (
	"context"
	"encoding/json"
	"fmt"
)

// WorkingTreeFile is one changed file in a worktree.
type WorkingTreeFile struct {
	Path           string `json:"path"`
	PreviousPath   *string `json:"previousPath"`
	Status         string `json:"status"`
	Staged         bool   `json:"staged,omitempty"`
	Additions      int    `json:"additions,omitempty"`
	Deletions      int    `json:"deletions,omitempty"`
	Untracked      bool   `json:"untracked,omitempty"`
}

// WorkingTree is the changed-file set for a repo.
type WorkingTree struct {
	Root  string            `json:"root,omitempty"`
	Files []WorkingTreeFile `json:"files"`
}

// ChangeRequest is a server-known PR for the current branch.
type ChangeRequest struct {
	Number   int    `json:"number"`
	Title    string `json:"title"`
	URL      string `json:"url"`
	BaseRef  string `json:"baseRef"`
	HeadRef  string `json:"headRef"`
	State    string `json:"state"`
	IsDraft  *bool  `json:"isDraft,omitempty"`
	UpdatedAt *string `json:"updatedAt,omitempty"`
}

// VcsStatus is the source-control state of a workspace.
type VcsStatus struct {
	IsRepo                bool            `json:"isRepo"`
	SourceControlProvider json.RawMessage `json:"sourceControlProvider,omitempty"`
	HasPrimaryRemote      bool            `json:"hasPrimaryRemote"`
	HasUpstream           bool            `json:"hasUpstream,omitempty"`
	IsDefaultRef          bool            `json:"isDefaultRef"`
	RefName               *string         `json:"refName"`
	HasWorkingTreeChanges bool            `json:"hasWorkingTreeChanges"`
	WorkingTree           json.RawMessage `json:"workingTree"`
	AheadCount            *int            `json:"aheadCount,omitempty"`
	AheadOfDefaultCount   *int            `json:"aheadOfDefaultCount,omitempty"`
	BehindCount           *int            `json:"behindCount,omitempty"`
	PR                    *ChangeRequest  `json:"pr,omitempty"`
	Remote                json.RawMessage `json:"remote,omitempty"`
}

// VcsStatusEvent is one tagged status stream event.
type VcsStatusEvent struct {
	Tag    string          `json:"_tag"`
	Local  json.RawMessage `json:"local,omitempty"`
	Remote json.RawMessage `json:"remote,omitempty"`
}

// RefreshVcsStatus asks the server to compute fresh git status.
func (c *Client) RefreshVcsStatus(ctx context.Context, cwd string) (*VcsStatus, error) {
	var out VcsStatus
	if err := c.call(ctx, "vcs.refreshStatus", map[string]any{"cwd": cwd}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// SubscribeVcsStatus streams status updates for a workspace.
func (c *Client) SubscribeVcsStatus(ctx context.Context, cwd string, includeRemote bool) (<-chan VcsStatusEvent, func(), error) {
	payload := map[string]any{"cwd": cwd, "includeRemote": includeRemote}
	stream, err := c.Stream(ctx, "subscribeVcsStatus", payload)
	if err != nil {
		return nil, nil, err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	ch := make(chan VcsStatusEvent, 16)
	go func() {
		defer close(ch)
		defer stream.Cancel()
		for {
			values, done, err := stream.Next(runCtx)
			if err != nil || done {
				return
			}
			for _, raw := range values {
				var ev VcsStatusEvent
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

// Pull runs git pull in a workspace.
func (c *Client) Pull(ctx context.Context, cwd string) (json.RawMessage, error) {
	return c.callRaw(ctx, "vcs.pull", map[string]any{"cwd": cwd})
}

// Ref is a branch or tag reference.
type Ref struct {
	Name      string  `json:"name"`
	Kind      string  `json:"kind"`
	IsCurrent bool    `json:"isCurrent,omitempty"`
	Remote    *string `json:"remote,omitempty"`
	UpdatedAt *string `json:"updatedAt,omitempty"`
}

// ListRefsResult lists refs with paging.
type ListRefsResult struct {
	Refs               []Ref `json:"refs"`
	NextCursor         *int  `json:"nextCursor,omitempty"`
	IsRepo             bool  `json:"isRepo"`
	IsAtRepositoryRoot bool  `json:"isAtRepositoryRoot,omitempty"`
	HasPrimaryRemote   bool  `json:"hasPrimaryRemote,omitempty"`
	TotalCount         int   `json:"totalCount,omitempty"`
}

// ListRefs lists branches/tags for a workspace.
func (c *Client) ListRefs(ctx context.Context, cwd, query, refKind string, limit int) (*ListRefsResult, error) {
	payload := map[string]any{"cwd": cwd}
	if query != "" {
		payload["query"] = query
	}
	if refKind != "" {
		payload["refKind"] = refKind
	}
	if limit > 0 {
		payload["limit"] = limit
	}
	var out ListRefsResult
	if err := c.call(ctx, "vcs.listRefs", payload, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// CreateWorktree adds a git worktree.
func (c *Client) CreateWorktree(ctx context.Context, cwd, refName, newRefName, baseRefName, path string) (json.RawMessage, error) {
	payload := map[string]any{"cwd": cwd, "refName": refName}
	if newRefName != "" {
		payload["newRefName"] = newRefName
	}
	if baseRefName != "" {
		payload["baseRefName"] = baseRefName
	}
	if path != "" {
		payload["path"] = path
	} else {
		payload["path"] = nil
	}
	return c.callRaw(ctx, "vcs.createWorktree", payload)
}

// RemoveWorktree removes a git worktree.
func (c *Client) RemoveWorktree(ctx context.Context, cwd, path string, force bool) error {
	return c.call(ctx, "vcs.removeWorktree", map[string]any{
		"cwd": cwd, "path": path, "force": force,
	}, nil)
}

// CreateRef creates a branch and optionally switches to it.
func (c *Client) CreateRef(ctx context.Context, cwd, refName string, switchRef bool) error {
	return c.call(ctx, "vcs.createRef", map[string]any{
		"cwd": cwd, "refName": refName, "switchRef": switchRef,
	}, nil)
}

// SwitchRef checks out a branch.
func (c *Client) SwitchRef(ctx context.Context, cwd, refName string) error {
	return c.call(ctx, "vcs.switchRef", map[string]any{"cwd": cwd, "refName": refName}, nil)
}

// InitRepo initializes a git repository.
func (c *Client) InitRepo(ctx context.Context, cwd string) error {
	return c.call(ctx, "vcs.init", map[string]any{"cwd": cwd}, nil)
}

// GitActionProgress is one event from a stacked git action.
type GitActionProgress struct {
	ActionID string `json:"actionId"`
	Kind     string `json:"kind"`
	Phase    string `json:"phase,omitempty"`
	Message  string `json:"message,omitempty"`
	Stream   string `json:"stream,omitempty"`
	Output   string `json:"output,omitempty"`
	Error    string `json:"error,omitempty"`
	Commit   json.RawMessage `json:"commit,omitempty"`
	Push     json.RawMessage `json:"push,omitempty"`
	PullRequest json.RawMessage `json:"pullRequest,omitempty"`
}

// RunGitAction runs commit/push/PR flows and streams progress. onEvent may be
// nil. The call blocks until the action completes or ctx ends.
func (c *Client) RunGitAction(ctx context.Context, cwd, action, commitMessage string, featureBranch bool, filePaths []string) error {
	actionID := NewID()
	payload := map[string]any{
		"actionId": actionID, "cwd": cwd, "action": action,
	}
	if commitMessage != "" {
		payload["commitMessage"] = commitMessage
	}
	if featureBranch {
		payload["featureBranch"] = true
	}
	if len(filePaths) > 0 {
		payload["filePaths"] = filePaths
	}
	stream, err := c.Stream(ctx, "git.runStackedAction", payload)
	if err != nil {
		return err
	}
	defer stream.Cancel()
	for {
		values, done, err := stream.Next(ctx)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		for _, raw := range values {
			var ev GitActionProgress
			if json.Unmarshal(raw, &ev) == nil && ev.Kind == "action_failed" {
				if ev.Error != "" {
					return fmt.Errorf("git action failed: %s", ev.Error)
				}
				if ev.Message != "" {
					return fmt.Errorf("git action failed: %s", ev.Message)
				}
			}
		}
	}
}

// DiffFileStat is one file summary in a diff preview.
type DiffFileStat struct {
	Path         string  `json:"path"`
	PreviousPath *string `json:"previousPath"`
	Additions    int     `json:"additions"`
	Deletions    int     `json:"deletions"`
}

// DiffPreviewSource is one diff (working tree or branch range).
type DiffPreviewSource struct {
	ID       string         `json:"id"`
	Kind     string         `json:"kind"`
	Title    string         `json:"title"`
	BaseRef  *string        `json:"baseRef"`
	HeadRef  *string        `json:"headRef"`
	Diff     string         `json:"diff"`
	DiffHash string         `json:"diffHash"`
	Truncated bool          `json:"truncated"`
	Files    []DiffFileStat `json:"files,omitempty"`
}

// DiffPreview is the result of review.getDiffPreview.
type DiffPreview struct {
	Sources []DiffPreviewSource `json:"sources"`
}

// GetDiffPreview fetches working-tree and branch-range diffs for a workspace.
func (c *Client) GetDiffPreview(ctx context.Context, cwd, baseRef string, ignoreWhitespace bool) (*DiffPreview, error) {
	payload := map[string]any{"cwd": cwd}
	if baseRef != "" {
		payload["baseRef"] = baseRef
	}
	if ignoreWhitespace {
		payload["ignoreWhitespace"] = true
	}
	var out DiffPreview
	if err := c.call(ctx, "review.getDiffPreview", payload, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// DiffFileContents is the old/new content of a changed file.
type DiffFileContents struct {
	OldContents *string `json:"oldContents"`
	NewContents *string `json:"newContents"`
	Language    string  `json:"language,omitempty"`
	IsBinary    bool    `json:"isBinary,omitempty"`
}

// GetDiffFileContents loads both versions of a changed file.
func (c *Client) GetDiffFileContents(ctx context.Context, cwd, sourceKind, changeType, baseRef, headRef, oldPath, newPath string) (*DiffFileContents, error) {
	payload := map[string]any{
		"cwd": cwd, "sourceKind": sourceKind, "changeType": changeType,
		"oldPath": oldPath, "newPath": newPath,
	}
	if baseRef != "" {
		payload["baseRef"] = baseRef
	}
	if headRef != "" {
		payload["headRef"] = headRef
	}
	var out DiffFileContents
	if err := c.call(ctx, "review.getDiffFileContents", payload, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
