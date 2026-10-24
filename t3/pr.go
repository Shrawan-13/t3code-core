package t3

import (
	"context"
	"encoding/json"
	"fmt"
)

// PullRequestRef identifies one pull request in a project's repository.
type PullRequestRef struct {
	ProjectID        string `json:"projectId"`
	Host             string `json:"host,omitempty"`
	ExpectedAccountID string `json:"expectedAccountId,omitempty"`
	AllowStale       bool   `json:"allowStale,omitempty"`
	Repository       string `json:"repository"`
	Number           int    `json:"number"`
}

func (r PullRequestRef) fields() map[string]any {
	m := map[string]any{
		"projectId": r.ProjectID, "repository": r.Repository, "number": r.Number,
	}
	if r.Host != "" {
		m["host"] = r.Host
	}
	if r.ExpectedAccountID != "" {
		m["expectedAccountId"] = r.ExpectedAccountID
	}
	if r.AllowStale {
		m["allowStale"] = true
	}
	return m
}

// ListPullRequests lists pull requests with optional filters. state is one of
// "open", "closed", "merged", "all".
func (c *Client) ListPullRequests(ctx context.Context, state, involvement, query, host string, projectIDs []string, limit int) (json.RawMessage, error) {
	payload := map[string]any{"state": state}
	if involvement != "" {
		payload["involvement"] = involvement
	}
	if query != "" {
		payload["query"] = query
	}
	if host != "" {
		payload["host"] = host
	}
	if len(projectIDs) > 0 {
		payload["projectIds"] = projectIDs
	}
	if limit > 0 {
		payload["limit"] = limit
	}
	return c.callRaw(ctx, "pullRequests.list", payload)
}

// PullRequestRouting resolves where a PR's repository lives on this server.
func (c *Client) PullRequestRouting(ctx context.Context, ref PullRequestRef) (json.RawMessage, error) {
	return c.callRaw(ctx, "pullRequests.routing", ref.fields())
}

// PullRequestSummary fetches the PR row summary.
func (c *Client) PullRequestSummary(ctx context.Context, ref PullRequestRef) (json.RawMessage, error) {
	return c.callRaw(ctx, "pullRequests.summary", ref.fields())
}

// PullRequestDetail fetches the full PR detail.
func (c *Client) PullRequestDetail(ctx context.Context, ref PullRequestRef) (json.RawMessage, error) {
	return c.callRaw(ctx, "pullRequests.detail", ref.fields())
}

// PullRequestChecks fetches CI checks for a PR.
func (c *Client) PullRequestChecks(ctx context.Context, ref PullRequestRef) (json.RawMessage, error) {
	return c.callRaw(ctx, "pullRequests.checks", ref.fields())
}

// PullRequestActivity fetches the PR timeline.
func (c *Client) PullRequestActivity(ctx context.Context, ref PullRequestRef) (json.RawMessage, error) {
	return c.callRaw(ctx, "pullRequests.activity", ref.fields())
}

// PullRequestStack fetches a native stack context for a PR, if any.
func (c *Client) PullRequestStack(ctx context.Context, ref PullRequestRef) (json.RawMessage, error) {
	return c.callRaw(ctx, "pullRequests.stack", ref.fields())
}

// PullRequestLinkedThreads lists threads linked to a PR.
func (c *Client) PullRequestLinkedThreads(ctx context.Context, ref PullRequestRef) (json.RawMessage, error) {
	return c.callRaw(ctx, "pullRequests.linkedThreads", ref.fields())
}

// PullRequestFilesViewed fetches the viewed state for PR files.
func (c *Client) PullRequestFilesViewed(ctx context.Context, ref PullRequestRef) (json.RawMessage, error) {
	return c.callRaw(ctx, "pullRequests.filesViewed", ref.fields())
}

// PullRequestSetFilesViewed marks files viewed/unviewed.
func (c *Client) PullRequestSetFilesViewed(ctx context.Context, ref PullRequestRef, files map[string]bool) error {
	payload := ref.fields()
	list := make([]map[string]any, 0, len(files))
	for path, viewed := range files {
		list = append(list, map[string]any{"path": path, "viewed": viewed})
	}
	payload["files"] = list
	_, err := c.callRaw(ctx, "pullRequests.setFilesViewed", payload)
	return err
}

// PullRequestChangeBreakdown fetches per-file change stats.
func (c *Client) PullRequestChangeBreakdown(ctx context.Context, ref PullRequestRef) (json.RawMessage, error) {
	return c.callRaw(ctx, "pullRequests.changeBreakdown", ref.fields())
}

// PullRequestRunAction performs a PR action: "merge", "close", "reopen",
// "mark_ready", "convert_to_draft", "approve", ... plus optional mergeMethod.
func (c *Client) PullRequestRunAction(ctx context.Context, ref PullRequestRef, action, mergeMethod string) error {
	payload := ref.fields()
	payload["action"] = action
	if mergeMethod != "" {
		payload["mergeMethod"] = mergeMethod
	}
	_, err := c.callRaw(ctx, "pullRequests.runAction", payload)
	return err
}

// UpdatePullRequest edits a PR title/body.
func (c *Client) UpdatePullRequest(ctx context.Context, ref PullRequestRef, title, body string) error {
	payload := ref.fields()
	if title != "" {
		payload["title"] = title
	}
	if body != "" {
		payload["body"] = body
	}
	_, err := c.callRaw(ctx, "pullRequests.update", payload)
	return err
}

// CommentPullRequest posts a conversation comment.
func (c *Client) CommentPullRequest(ctx context.Context, ref PullRequestRef, body string) error {
	payload := ref.fields()
	payload["body"] = body
	_, err := c.callRaw(ctx, "pullRequests.comment", payload)
	return err
}

// PullRequestThreadComments lists review-thread comments.
func (c *Client) PullRequestThreadComments(ctx context.Context, ref PullRequestRef, threadID, cursor string) (json.RawMessage, error) {
	payload := ref.fields()
	payload["threadId"] = threadID
	if cursor != "" {
		payload["cursor"] = cursor
	}
	return c.callRaw(ctx, "pullRequests.threadComments", payload)
}

// ReplyPullRequestThread replies in a review thread.
func (c *Client) ReplyPullRequestThread(ctx context.Context, ref PullRequestRef, threadID, body string) error {
	payload := ref.fields()
	payload["threadId"] = threadID
	payload["body"] = body
	_, err := c.callRaw(ctx, "pullRequests.replyToThread", payload)
	return err
}

// ResolvePullRequestThread resolves or reopens a review thread.
func (c *Client) ResolvePullRequestThread(ctx context.Context, ref PullRequestRef, threadID string, resolved bool) error {
	payload := ref.fields()
	payload["threadId"] = threadID
	payload["resolved"] = resolved
	_, err := c.callRaw(ctx, "pullRequests.setThreadResolution", payload)
	return err
}

// SubmitPullRequestReview submits a verdict with optional inline comments.
func (c *Client) SubmitPullRequestReview(ctx context.Context, ref PullRequestRef, verdict, body string, comments json.RawMessage) error {
	payload := ref.fields()
	payload["verdict"] = verdict
	payload["body"] = body
	if len(comments) == 0 {
		comments = json.RawMessage("[]")
	}
	payload["comments"] = comments
	_, err := c.callRaw(ctx, "pullRequests.submitReview", payload)
	return err
}

// InvalidatePullRequest clears cached PR data.
func (c *Client) InvalidatePullRequest(ctx context.Context, ref PullRequestRef) error {
	_, err := c.callRaw(ctx, "pullRequests.invalidate", ref.fields())
	return err
}

// PullRequestReviewerCandidates lists assignable reviewers.
func (c *Client) PullRequestReviewerCandidates(ctx context.Context, ref PullRequestRef) (json.RawMessage, error) {
	return c.callRaw(ctx, "pullRequests.reviewerCandidates", ref.fields())
}

// RequestPullRequestReviewers requests reviews from users.
func (c *Client) RequestPullRequestReviewers(ctx context.Context, ref PullRequestRef, reviewers []string) error {
	payload := ref.fields()
	payload["reviewers"] = reviewers
	_, err := c.callRaw(ctx, "pullRequests.requestReviewers", payload)
	return err
}

// PullRequestLabelCandidates lists available labels.
func (c *Client) PullRequestLabelCandidates(ctx context.Context, ref PullRequestRef) (json.RawMessage, error) {
	return c.callRaw(ctx, "pullRequests.labelCandidates", ref.fields())
}

// SetPullRequestLabels replaces the PR labels.
func (c *Client) SetPullRequestLabels(ctx context.Context, ref PullRequestRef, labels []string) error {
	payload := ref.fields()
	payload["labels"] = labels
	_, err := c.callRaw(ctx, "pullRequests.setLabels", payload)
	return err
}

// SubscribePullRequestRefreshes streams refresh notifications.
func (c *Client) SubscribePullRequestRefreshes(ctx context.Context) (*ConfigSubscription, error) {
	return c.rawSubscription(ctx, "pullRequests.subscribeRefreshes", map[string]any{})
}

// ResolvePullRequestFromGit finds a PR from a URL/number/branch reference.
func (c *Client) ResolvePullRequestFromGit(ctx context.Context, cwd string, reference json.RawMessage) (json.RawMessage, error) {
	return c.callRaw(ctx, "git.resolvePullRequest", map[string]any{"cwd": cwd, "reference": reference})
}

// PreparePullRequestThread creates a checkout thread for a PR.
func (c *Client) PreparePullRequestThread(ctx context.Context, cwd string, reference json.RawMessage, mode, threadID string) (json.RawMessage, error) {
	payload := map[string]any{"cwd": cwd, "reference": reference, "mode": mode}
	if threadID != "" {
		payload["threadId"] = threadID
	}
	return c.callRaw(ctx, "git.preparePullRequestThread", payload)
}

// PreviewTab is a browser preview session.
type PreviewTab struct {
	ThreadID  string  `json:"threadId"`
	TabID     string  `json:"tabId"`
	URL       string  `json:"url,omitempty"`
	Title     string  `json:"title,omitempty"`
	Status    string  `json:"status,omitempty"`
	CreatedAt string  `json:"createdAt,omitempty"`
}

// OpenPreview opens a browser preview tab for a thread.
func (c *Client) OpenPreview(ctx context.Context, threadID, rawURL string, reveal bool) (json.RawMessage, error) {
	payload := map[string]any{"threadId": threadID}
	if rawURL != "" {
		payload["url"] = rawURL
	}
	if reveal {
		payload["reveal"] = true
	}
	return c.callRaw(ctx, "preview.open", payload)
}

// ListPreviews lists preview tabs for a thread.
func (c *Client) ListPreviews(ctx context.Context, threadID string) (json.RawMessage, error) {
	return c.callRaw(ctx, "preview.list", map[string]any{"threadId": threadID})
}

// NavigatePreview navigates an open preview tab.
func (c *Client) NavigatePreview(ctx context.Context, threadID, tabID, rawURL string) error {
	_, err := c.callRaw(ctx, "preview.navigate", map[string]any{
		"threadId": threadID, "tabId": tabID, "url": rawURL,
	})
	return err
}

// RefreshPreview reloads a preview tab.
func (c *Client) RefreshPreview(ctx context.Context, threadID, tabID string) error {
	_, err := c.callRaw(ctx, "preview.refresh", map[string]any{"threadId": threadID, "tabId": tabID})
	return err
}

// ClosePreview closes one or all preview tabs of a thread.
func (c *Client) ClosePreview(ctx context.Context, threadID, tabID string) error {
	payload := map[string]any{"threadId": threadID}
	if tabID != "" {
		payload["tabId"] = tabID
	}
	_, err := c.callRaw(ctx, "preview.close", payload)
	return err
}

// SubscribePreviewEvents streams preview events.
func (c *Client) SubscribePreviewEvents(ctx context.Context) (*ConfigSubscription, error) {
	return c.rawSubscription(ctx, "subscribePreviewEvents", map[string]any{})
}

// ScheduledTask is a recurring agent task.
type ScheduledTask struct {
	ID             string          `json:"id"`
	Title          string          `json:"title"`
	Prompt         string          `json:"prompt"`
	Enabled        bool            `json:"enabled"`
	Schedule       json.RawMessage `json:"schedule"`
	ProjectID      string          `json:"projectId"`
	ThreadID       *string         `json:"threadId"`
	ModelSelection ModelSelection  `json:"modelSelection"`
	RuntimeMode    string          `json:"runtimeMode"`
	NextRunAt      *string         `json:"nextRunAt"`
	LastRunAt      *string         `json:"lastRunAt"`
	LastRunStatus  string          `json:"lastRunStatus"`
	LastRunError   *string         `json:"lastRunError"`
	RunCount       int             `json:"runCount"`
	CreatedAt      string          `json:"createdAt"`
	UpdatedAt      string          `json:"updatedAt"`
}

// ListScheduledTasks lists recurring tasks.
func (c *Client) ListScheduledTasks(ctx context.Context) ([]ScheduledTask, error) {
	var out struct {
		Tasks []ScheduledTask `json:"tasks"`
	}
	if err := c.call(ctx, "scheduledTasks.list", map[string]any{}, &out); err != nil {
		return nil, err
	}
	return out.Tasks, nil
}

// UpsertScheduledTask creates or updates a recurring task. Input follows the
// server's ScheduledTaskUpsertInput schema.
func (c *Client) UpsertScheduledTask(ctx context.Context, input json.RawMessage) (*ScheduledTask, error) {
	raw, err := c.CallJSON(ctx, "scheduledTasks.upsert", input)
	if err != nil {
		return nil, err
	}
	var out ScheduledTask
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// SetScheduledTaskEnabled enables/disables a task.
func (c *Client) SetScheduledTaskEnabled(ctx context.Context, id string, enabled bool) error {
	_, err := c.callRaw(ctx, "scheduledTasks.setEnabled", map[string]any{"id": id, "enabled": enabled})
	return err
}

// DeleteScheduledTask removes a task.
func (c *Client) DeleteScheduledTask(ctx context.Context, id string) error {
	_, err := c.callRaw(ctx, "scheduledTasks.delete", map[string]any{"id": id})
	return err
}

// RunScheduledTaskNow triggers an immediate run.
func (c *Client) RunScheduledTaskNow(ctx context.Context, id string) (json.RawMessage, error) {
	return c.callRaw(ctx, "scheduledTasks.runNow", map[string]any{"id": id})
}

// RotateScheduledTaskWebhookToken issues a new webhook secret.
func (c *Client) RotateScheduledTaskWebhookToken(ctx context.Context, id string) (json.RawMessage, error) {
	return c.callRaw(ctx, "scheduledTasks.rotateWebhookToken", map[string]any{"id": id})
}

// SubscribeScheduledTasks streams task list changes.
func (c *Client) SubscribeScheduledTasks(ctx context.Context) (*ConfigSubscription, error) {
	return c.rawSubscription(ctx, "scheduledTasks.subscribe", map[string]any{})
}

// ListDevices lists connected devices available for preview/run.
func (c *Client) ListDevices(ctx context.Context) (json.RawMessage, error) {
	return c.callRaw(ctx, "device.list", map[string]any{})
}

// DeviceDetail fetches one device's detail.
func (c *Client) DeviceDetail(ctx context.Context, deviceID string) (json.RawMessage, error) {
	return c.callRaw(ctx, "device.detail", map[string]any{"deviceId": deviceID})
}

// OpenDevice opens an app/URL on a device.
func (c *Client) OpenDevice(ctx context.Context, input json.RawMessage) error {
	_, err := c.CallJSON(ctx, "device.open", input)
	return err
}

// CloseDevice closes an app on a device.
func (c *Client) CloseDevice(ctx context.Context, deviceID string) error {
	_, err := c.callRaw(ctx, "device.close", map[string]any{"deviceId": deviceID})
	return err
}

// RunDeviceAction runs a named action on a device.
func (c *Client) RunDeviceAction(ctx context.Context, deviceID, action string, args json.RawMessage) (json.RawMessage, error) {
	payload := map[string]any{"deviceId": deviceID, "action": action}
	if len(args) > 0 {
		payload["args"] = args
	}
	return c.callRaw(ctx, "device.action", payload)
}

// CloudRelayStatus reports the remote-access relay state.
func (c *Client) CloudRelayStatus(ctx context.Context) (json.RawMessage, error) {
	return c.callRaw(ctx, "cloud.getRelayClientStatus", map[string]any{})
}

// InstallCloudRelay installs the relay client.
func (c *Client) InstallCloudRelay(ctx context.Context) (json.RawMessage, error) {
	return c.callRaw(ctx, "cloud.installRelayClient", map[string]any{})
}

// CallMethod is an escape hatch for any server RPC method. payload may be nil.
func (c *Client) CallMethod(ctx context.Context, method string, payload any) (json.RawMessage, error) {
	return c.callRaw(ctx, method, payload)
}

// Str is a tiny helper for building optional payload fields.
func Str(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// RequireInt validates a positive number field.
func RequireInt(name string, v int) error {
	if v <= 0 {
		return fmt.Errorf("%s must be positive", name)
	}
	return nil
}
