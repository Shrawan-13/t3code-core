package t3

import (
	"context"
	"encoding/json"
)

// This file covers the long tail of server RPCs that are primarily used by
// desktop/admin surfaces or that carry large, provider-specific payloads. The
// typed core methods live in their own files; these take/return raw JSON so
// every remaining endpoint is still reachable from Android without a schema
// copy for each one.

// AgentSessionsScan discovers resumable provider sessions on disk.
func (c *Client) AgentSessionsScan(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "agentSessions.scan", input)
}

// AgentSessionsImport imports a discovered provider session as a thread.
func (c *Client) AgentSessionsImport(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "agentSessions.import", input)
}

// AssetsCreateURL builds an authenticated URL for a stored asset.
func (c *Client) AssetsCreateURL(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "assets.createUrl", input)
}

// PersistChatAttachments copies draft attachments into permanent storage.
func (c *Client) PersistChatAttachments(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "assets.persistChatAttachments", input)
}

// CreateAttachmentUploadURL starts a direct upload.
func (c *Client) CreateAttachmentUploadURL(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "attachments.createUploadUrl", input)
}

// CloudRelayInstall installs the relay client. Alias of InstallCloudRelay.
func (c *Client) CloudRelayInstall(ctx context.Context) (json.RawMessage, error) {
	return c.InstallCloudRelay(ctx)
}

// MCPAppCallTool invokes a tool exposed by an MCP app.
func (c *Client) MCPAppCallTool(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "mcpApps.callTool", input)
}

// MCPAppToolInfo describes an MCP app tool.
func (c *Client) MCPAppToolInfo(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "mcpApps.toolInfo", input)
}

// MCPAppReadResource reads an MCP app resource.
func (c *Client) MCPAppReadResource(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "mcpApps.readResource", input)
}

// MCPAppUpdateModelContext lets an MCP app contribute model context.
func (c *Client) MCPAppUpdateModelContext(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "mcpApps.updateModelContext", input)
}

// PreviewResize resizes a preview viewport.
func (c *Client) PreviewResize(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "preview.resize", input)
}

// PreviewAdjust applies a viewport adjustment (zoom, device preset, ...).
func (c *Client) PreviewAdjust(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "preview.adjust", input)
}

// PreviewReportProfiles reports the browser profiles available on a client.
func (c *Client) PreviewReportProfiles(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "preview.reportProfiles", input)
}

// PreviewReportStatus reports navigation state for a preview tab.
func (c *Client) PreviewReportStatus(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "preview.reportStatus", input)
}

// PreviewClearProfile clears stored data for a browser profile.
func (c *Client) PreviewClearProfile(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "preview.clearProfile", input)
}

// DeviceConfigure stores configuration for a device.
func (c *Client) DeviceConfigure(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "device.configure", input)
}

// DeviceEASAccount returns the configured Expo/EAS account for a device.
func (c *Client) DeviceEASAccount(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "device.easAccount", input)
}

// DeviceTestHost checks that a device host is reachable.
func (c *Client) DeviceTestHost(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "device.testHost", input)
}

// DeviceShutdown powers off a device.
func (c *Client) DeviceShutdown(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "device.shutdown", input)
}

// ProviderUploadFeedback uploads a provider feedback report.
func (c *Client) ProviderUploadFeedback(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "provider.uploadFeedback", input)
}

// ProviderConsumeResetCredit consumes a usage-limit reset credit.
func (c *Client) ProviderConsumeResetCredit(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "provider.consumeResetCredit", input)
}

// SubscribeChatGPTHandoff streams ChatGPT handoff state changes.
func (c *Client) SubscribeChatGPTHandoff(ctx context.Context, input json.RawMessage) (*ConfigSubscription, error) {
	return c.rawSubscription(ctx, "provider.chatgpt.handoff.subscribe", input)
}

// SecretsAnswerRequest answers a secret request from a provider.
func (c *Client) SecretsAnswerRequest(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "secrets.answerRequest", input)
}

// ServerProbe probes the server for its current capabilities.
func (c *Client) ServerProbe(ctx context.Context) (json.RawMessage, error) {
	return c.callRaw(ctx, "server.probe", map[string]any{})
}

// ProcessResourceHistory returns sampled resource usage of managed processes.
func (c *Client) ProcessResourceHistory(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "server.getProcessResourceHistory", input)
}

// ResourceTelemetryHistory returns sampled host telemetry.
func (c *Client) ResourceTelemetryHistory(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "server.getResourceTelemetryHistory", input)
}

// RetryResourceTelemetry asks the server to restart telemetry collection.
func (c *Client) RetryResourceTelemetry(ctx context.Context) (json.RawMessage, error) {
	return c.callRaw(ctx, "server.retryResourceTelemetry", map[string]any{})
}

// CheckOtlpEndpoint validates an OTLP endpoint.
func (c *Client) CheckOtlpEndpoint(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "server.checkOtlpEndpoint", input)
}

// DiscoverSourceControl detects available source-control CLIs.
func (c *Client) DiscoverSourceControl(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "server.discoverSourceControl", input)
}

// UpsertKeybinding sets a keybinding.
func (c *Client) UpsertKeybinding(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "server.upsertKeybinding", input)
}

// RemoveKeybinding removes a keybinding.
func (c *Client) RemoveKeybinding(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "server.removeKeybinding", input)
}

// ScheduledTaskWebhookDeliveries lists webhook deliveries for a task.
func (c *Client) ScheduledTaskWebhookDeliveries(ctx context.Context, id string, limit int) (json.RawMessage, error) {
	payload := map[string]any{"id": id}
	if limit > 0 {
		payload["limit"] = limit
	}
	return c.callRaw(ctx, "scheduledTasks.listWebhookDeliveries", payload)
}

// ScheduledTaskWebhookDelivery fetches one webhook delivery.
func (c *Client) ScheduledTaskWebhookDelivery(ctx context.Context, id, deliveryID string) (json.RawMessage, error) {
	return c.callRaw(ctx, "scheduledTasks.getWebhookDelivery", map[string]any{
		"id": id, "deliveryId": deliveryID,
	})
}

// PullRequestCall invokes any pull-request RPC by suffix (for example
// "listStats" or "diffFileContents") with the shared PR identity plus extra
// fields.
func (c *Client) PullRequestCall(ctx context.Context, suffix string, ref PullRequestRef, extra map[string]any) (json.RawMessage, error) {
	payload := ref.fields()
	for k, v := range extra {
		payload[k] = v
	}
	return c.callRaw(ctx, "pullRequests."+suffix, payload)
}

// PullRequestListStats returns aggregate counts for the PR list.
func (c *Client) PullRequestListStats(ctx context.Context, filters map[string]any) (json.RawMessage, error) {
	if filters == nil {
		filters = map[string]any{}
	}
	return c.callRaw(ctx, "pullRequests.listStats", filters)
}

// PullRequestDiffFileContents loads one file's diff contents in a PR.
func (c *Client) PullRequestDiffFileContents(ctx context.Context, ref PullRequestRef, path, previousPath, sourceKind string) (json.RawMessage, error) {
	extra := map[string]any{"path": path}
	if previousPath != "" {
		extra["previousPath"] = previousPath
	}
	if sourceKind != "" {
		extra["sourceKind"] = sourceKind
	}
	return c.PullRequestCall(ctx, "diffFileContents", ref, extra)
}

// PullRequestPreview returns a lightweight PR preview.
func (c *Client) PullRequestPreview(ctx context.Context, ref PullRequestRef) (json.RawMessage, error) {
	return c.PullRequestCall(ctx, "preview", ref, nil)
}

// PullRequestReviewGuide returns the stored review guide for a PR.
func (c *Client) PullRequestReviewGuide(ctx context.Context, ref PullRequestRef) (json.RawMessage, error) {
	return c.PullRequestCall(ctx, "reviewGuide", ref, nil)
}

// GeneratePullRequestReviewGuide asks the server to build a review guide.
func (c *Client) GeneratePullRequestReviewGuide(ctx context.Context, ref PullRequestRef, extra map[string]any) (json.RawMessage, error) {
	return c.PullRequestCall(ctx, "generateReviewGuide", ref, extra)
}

// PullRequestRoutingIdentity resolves the authenticated GitHub identity.
func (c *Client) PullRequestRoutingIdentity(ctx context.Context, host string) (json.RawMessage, error) {
	return c.callRaw(ctx, "pullRequests.routingIdentity", map[string]any{"host": host})
}

// PullRequestReportState reports client-side PR view state.
func (c *Client) PullRequestReportState(ctx context.Context, ref PullRequestRef, extra map[string]any) (json.RawMessage, error) {
	return c.PullRequestCall(ctx, "reportState", ref, extra)
}

// PullRequestSetReaction toggles a reaction on a PR comment.
func (c *Client) PullRequestSetReaction(ctx context.Context, ref PullRequestRef, commentID, reaction string, active bool) (json.RawMessage, error) {
	return c.PullRequestCall(ctx, "setReaction", ref, map[string]any{
		"commentId": commentID, "reaction": reaction, "active": active,
	})
}

// PullRequestUpdateComment edits a PR comment.
func (c *Client) PullRequestUpdateComment(ctx context.Context, ref PullRequestRef, commentID, body string) (json.RawMessage, error) {
	return c.PullRequestCall(ctx, "updateComment", ref, map[string]any{
		"commentId": commentID, "body": body,
	})
}
