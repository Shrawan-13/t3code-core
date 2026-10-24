package t3

import (
	"context"
	"encoding/json"
)

// Settings is the raw server settings object.
type Settings = json.RawMessage

// GetSettings fetches server settings.
func (c *Client) GetSettings(ctx context.Context) (Settings, error) {
	return c.callRaw(ctx, "server.getSettings", map[string]any{})
}

// UpdateSettings patches server settings. providerMutation is optional raw JSON
// for adding/updating provider instances.
func (c *Client) UpdateSettings(ctx context.Context, patch json.RawMessage, providerMutation json.RawMessage) (Settings, error) {
	payload := map[string]any{"patch": patch}
	if len(providerMutation) > 0 {
		payload["providerInstanceMutation"] = providerMutation
	}
	return c.callRaw(ctx, "server.updateSettings", payload)
}

// RefreshProviders asks the server to refresh provider status and models.
func (c *Client) RefreshProviders(ctx context.Context, instanceID, cwd string, fresh, refreshModels bool) (json.RawMessage, error) {
	payload := map[string]any{}
	if instanceID != "" {
		payload["instanceId"] = instanceID
	}
	if cwd != "" {
		payload["cwd"] = cwd
	}
	if fresh {
		payload["fresh"] = true
	}
	if refreshModels {
		payload["refreshModels"] = true
	}
	return c.callRaw(ctx, "server.refreshProviders", payload)
}

// UpdateProvider creates or edits a provider instance.
func (c *Client) UpdateProvider(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "server.updateProvider", input)
}

// ServerUpdateInfo describes an available server update.
type ServerUpdateInfo struct {
	CurrentVersion  string `json:"currentVersion,omitempty"`
	LatestVersion   string `json:"latestVersion,omitempty"`
	UpdateAvailable bool   `json:"updateAvailable,omitempty"`
	Channel         string `json:"channel,omitempty"`
	ReleaseNotes    string `json:"releaseNotes,omitempty"`
}

// CheckServerUpdate triggers the update check/install path. The exact payload
// follows the server's ServerUpdateInput schema; pass raw JSON.
func (c *Client) CheckServerUpdate(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return c.CallJSON(ctx, "server.updateServer", input)
}

// UpdateServerWithProgress installs a server update and streams progress.
func (c *Client) UpdateServerWithProgress(ctx context.Context, input json.RawMessage, onEvent func(json.RawMessage)) error {
	var p any
	if err := json.Unmarshal(input, &p); err != nil {
		return err
	}
	stream, err := c.Stream(ctx, "server.updateServerWithProgress", p)
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
		if onEvent != nil {
			for _, v := range values {
				onEvent(v)
			}
		}
	}
}

// StorageCleanupReport is the server's storage usage report.
func (c *Client) StorageCleanupReport(ctx context.Context) (json.RawMessage, error) {
	return c.callRaw(ctx, "server.getStorageCleanupReport", map[string]any{})
}

// RunStorageCleanup deletes reclaimable data.
func (c *Client) RunStorageCleanup(ctx context.Context, dryRun bool) (json.RawMessage, error) {
	return c.callRaw(ctx, "server.runStorageCleanup", map[string]any{"dryRun": dryRun})
}

// DatabaseStorage reports database table sizes.
func (c *Client) DatabaseStorage(ctx context.Context) (json.RawMessage, error) {
	return c.callRaw(ctx, "server.getDatabaseStorage", map[string]any{})
}

// TraceDiagnostics returns recent trace diagnostics.
func (c *Client) TraceDiagnostics(ctx context.Context) (json.RawMessage, error) {
	return c.callRaw(ctx, "server.getTraceDiagnostics", map[string]any{})
}

// ProcessDiagnostics returns server process diagnostics.
func (c *Client) ProcessDiagnostics(ctx context.Context) (json.RawMessage, error) {
	return c.callRaw(ctx, "server.getProcessDiagnostics", map[string]any{})
}

// HostResources returns current host resource usage.
func (c *Client) HostResources(ctx context.Context) (json.RawMessage, error) {
	return c.callRaw(ctx, "server.getHostResources", map[string]any{})
}

// SignalProcess sends a signal to a managed process.
func (c *Client) SignalProcess(ctx context.Context, input json.RawMessage) error {
	_, err := c.CallJSON(ctx, "server.signalProcess", input)
	return err
}

// ReportClientActivity reports foreground/background state for push decisions.
func (c *Client) ReportClientActivity(ctx context.Context, state string) error {
	_, err := c.callRaw(ctx, "server.reportClientActivity", map[string]any{"state": state})
	return err
}

// ReportHostPowerState reports host sleep/wake state.
func (c *Client) ReportHostPowerState(ctx context.Context, state string) error {
	_, err := c.callRaw(ctx, "server.reportHostPowerState", map[string]any{"state": state})
	return err
}

// BackgroundPolicy is the server's background execution policy.
func (c *Client) BackgroundPolicy(ctx context.Context) (json.RawMessage, error) {
	return c.callRaw(ctx, "server.getBackgroundPolicy", map[string]any{})
}

// ConfigSubscription streams updated server configurations.
type ConfigSubscription struct {
	Events <-chan json.RawMessage
	cancel func()
}

// Close cancels the subscription.
func (s *ConfigSubscription) Close() { s.cancel() }

// SubscribeServerConfig streams config changes.
func (c *Client) SubscribeServerConfig(ctx context.Context) (*ConfigSubscription, error) {
	return c.rawSubscription(ctx, "subscribeServerConfig", map[string]any{})
}

// ServerLifecycleEvent is one lifecycle notification (start/stop/update).
type ServerLifecycleEvent = json.RawMessage

// SubscribeServerLifecycle streams server lifecycle changes.
func (c *Client) SubscribeServerLifecycle(ctx context.Context) (*ConfigSubscription, error) {
	return c.rawSubscription(ctx, "subscribeServerLifecycle", map[string]any{})
}

// SubscribeAuthAccess streams auth access changes for this session.
func (c *Client) SubscribeAuthAccess(ctx context.Context) (*ConfigSubscription, error) {
	return c.rawSubscription(ctx, "subscribeAuthAccess", map[string]any{})
}

// SubscribeResourceTelemetry streams host resource samples.
func (c *Client) SubscribeResourceTelemetry(ctx context.Context) (*ConfigSubscription, error) {
	return c.rawSubscription(ctx, "subscribeResourceTelemetry", map[string]any{})
}

// DiscoveredLocalServer is a server announced on the local network.
type DiscoveredLocalServer struct {
	URL           string `json:"url"`
	Label         string `json:"label,omitempty"`
	EnvironmentID string `json:"environmentId,omitempty"`
	Version       string `json:"version,omitempty"`
	Host          string `json:"host,omitempty"`
	Port          int    `json:"port,omitempty"`
}

// SubscribeDiscoveredLocalServers streams LAN discovery results.
func (c *Client) SubscribeDiscoveredLocalServers(ctx context.Context) (*ConfigSubscription, error) {
	return c.rawSubscription(ctx, "subscribeDiscoveredLocalServers", map[string]any{})
}

func (c *Client) rawSubscription(ctx context.Context, method string, payload any) (*ConfigSubscription, error) {
	stream, err := c.Stream(ctx, method, payload)
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	ch := make(chan json.RawMessage, 64)
	go func() {
		defer close(ch)
		defer stream.Cancel()
		for {
			values, done, err := stream.Next(runCtx)
			if err != nil || done {
				return
			}
			for _, v := range values {
				select {
				case ch <- append(json.RawMessage(nil), v...):
				case <-runCtx.Done():
					return
				}
			}
		}
	}()
	return &ConfigSubscription{Events: ch, cancel: cancel}, nil
}

// OpenInEditor opens a path in a configured editor on the host.
func (c *Client) OpenInEditor(ctx context.Context, cwd, path, editor string) error {
	payload := map[string]any{"cwd": cwd, "path": path}
	if editor != "" {
		payload["editor"] = editor
	}
	_, err := c.callRaw(ctx, "shell.openInEditor", payload)
	return err
}

// SearchThread searches one thread's history for text.
func (c *Client) SearchThread(ctx context.Context, threadID, query string, limit int) (json.RawMessage, error) {
	payload := map[string]any{"threadId": threadID, "query": query}
	if limit > 0 {
		payload["limit"] = limit
	}
	return c.callRaw(ctx, "orchestration.searchThread", payload)
}

// SearchThreadStream streams thread search hits.
func (c *Client) SearchThreadStream(ctx context.Context, threadID, query string) (*ConfigSubscription, error) {
	return c.rawSubscription(ctx, "orchestration.searchThreadStream", map[string]any{
		"threadId": threadID, "query": query,
	})
}

// GetTurnItem loads one timeline row by id.
func (c *Client) GetTurnItem(ctx context.Context, threadID, itemID string) (json.RawMessage, error) {
	return c.callRaw(ctx, "orchestration.getTurnItem", map[string]any{
		"threadId": threadID, "itemId": itemID,
	})
}

// GetTurnDiff fetches the diff produced by one turn.
func (c *Client) GetTurnDiff(ctx context.Context, threadID, itemID string) (json.RawMessage, error) {
	return c.callRaw(ctx, "orchestration.getTurnDiff", map[string]any{
		"threadId": threadID, "itemId": itemID,
	})
}

// GetFullThreadDiff fetches the full diff for a thread.
func (c *Client) GetFullThreadDiff(ctx context.Context, threadID string) (json.RawMessage, error) {
	return c.callRaw(ctx, "orchestration.getFullThreadDiff", map[string]any{"threadId": threadID})
}

// CommandOutput streams live output for a long-running command row.
func (c *Client) CommandOutput(ctx context.Context, threadID, itemID string, onFrame func(kind, text string, truncated, running bool)) error {
	stream, err := c.Stream(ctx, "orchestration.subscribeCommandOutput", map[string]any{
		"threadId": threadID, "itemId": itemID,
	})
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
			var f struct {
				Kind      string `json:"kind"`
				Text      string `json:"text"`
				Truncated bool   `json:"truncated"`
				Running   bool   `json:"running"`
			}
			if json.Unmarshal(raw, &f) == nil && onFrame != nil {
				onFrame(f.Kind, f.Text, f.Truncated, f.Running)
			}
		}
	}
}

// WorkflowScript returns the workflow script for a thread (orchestration mode).
func (c *Client) WorkflowScript(ctx context.Context, threadID string) (json.RawMessage, error) {
	return c.callRaw(ctx, "orchestration.getWorkflowScript", map[string]any{"threadId": threadID})
}

// StopWorkflow stops a running orchestration workflow.
func (c *Client) StopWorkflow(ctx context.Context, threadID string) error {
	_, err := c.callRaw(ctx, "orchestration.stopWorkflow", map[string]any{"threadId": threadID})
	return err
}
