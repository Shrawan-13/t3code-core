package t3

import (
	"context"
	"encoding/json"
	"time"
)

// TokenTotals is the token accounting for a usage bucket.
type TokenTotals struct {
	UncachedInputTokens int64 `json:"uncachedInputTokens"`
	CachedInputTokens   int64 `json:"cachedInputTokens"`
	CacheCreationTokens int64 `json:"cacheCreationTokens"`
	OutputTokens        int64 `json:"outputTokens"`
	ReasoningTokens     int64 `json:"reasoningTokens"`
}

// UsageBucket is one provider/model/day aggregation.
type UsageBucket struct {
	Day          string      `json:"day"`
	HourStart    *string     `json:"hourStart,omitempty"`
	Provider     string      `json:"provider"`
	Model        string      `json:"model"`
	SourcePath   *string     `json:"sourcePath,omitempty"`
	Totals       TokenTotals `json:"totals"`
	CostUSD      float64     `json:"costUsd"`
	CacheSavingsUSD float64  `json:"cacheSavingsUsd"`
	CostSource   string      `json:"costSource"`
	Records      int64       `json:"records"`
	UnpricedRecords int64    `json:"unpricedRecords"`
	Sessions     int64       `json:"sessions"`
}

// UsageSummary is the response of server.getUsageSummary.
type UsageSummary struct {
	ContractVersion int             `json:"contractVersion"`
	ReadAt          string          `json:"readAt"`
	TimeZone        string          `json:"timeZone"`
	SinceDay        string          `json:"sinceDay"`
	UntilDay        string          `json:"untilDay"`
	Buckets         []UsageBucket   `json:"buckets"`
	Sources         json.RawMessage `json:"sources,omitempty"`
	Pricing         json.RawMessage `json:"pricing,omitempty"`
	ScanDurationMS  int64           `json:"scanDurationMs"`
}

// TotalTokens sums all token categories.
func (s *UsageSummary) TotalTokens() TokenTotals {
	var t TokenTotals
	for _, b := range s.Buckets {
		t.UncachedInputTokens += b.Totals.UncachedInputTokens
		t.CachedInputTokens += b.Totals.CachedInputTokens
		t.CacheCreationTokens += b.Totals.CacheCreationTokens
		t.OutputTokens += b.Totals.OutputTokens
		t.ReasoningTokens += b.Totals.ReasoningTokens
	}
	return t
}

// TotalCost sums bucket costs in USD.
func (s *UsageSummary) TotalCost() float64 {
	var c float64
	for _, b := range s.Buckets {
		c += b.CostUSD
	}
	return c
}

// GetUsageSummary fetches usage for an inclusive day window. When since/until
// are empty, the last 7 days are used. timeZone defaults to UTC.
func (c *Client) GetUsageSummary(ctx context.Context, sinceDay, untilDay, timeZone string, resolution string, awaitRefresh bool) (*UsageSummary, error) {
	if timeZone == "" {
		timeZone = "UTC"
	}
	if untilDay == "" {
		untilDay = time.Now().UTC().Format("2006-01-02")
	}
	if sinceDay == "" {
		sinceDay = time.Now().UTC().AddDate(0, 0, -6).Format("2006-01-02")
	}
	payload := map[string]any{
		"sinceDay": sinceDay, "untilDay": untilDay, "timeZone": timeZone,
	}
	if resolution != "" {
		payload["resolution"] = resolution
	}
	if awaitRefresh {
		payload["awaitRefresh"] = true
	}
	var out UsageSummary
	if err := c.call(ctx, "server.getUsageSummary", payload, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// RefreshUsageRates refreshes model pricing data.
func (c *Client) RefreshUsageRates(ctx context.Context) (json.RawMessage, error) {
	return c.callRaw(ctx, "server.refreshUsageRates", map[string]any{})
}

// ProviderAuthMethod is one way to authenticate a provider.
type ProviderAuthMethod struct {
	ID          string  `json:"id"`
	AccountEmail *string `json:"accountEmail,omitempty"`
	Name        string  `json:"name"`
	Description *string `json:"description"`
	Type        string  `json:"type"`
}

// ProviderAuthState is the progress of a provider login flow.
type ProviderAuthState struct {
	InstanceID       string                  `json:"instanceId"`
	Phase            string                  `json:"phase"`
	FlowID           *string                 `json:"flowId"`
	AuthorizationURL *string                 `json:"authorizationUrl"`
	ExpiresAt        *string                 `json:"expiresAt"`
	Message          *string                 `json:"message"`
	Methods          []ProviderAuthMethod    `json:"methods,omitempty"`
	Interaction      json.RawMessage         `json:"interaction,omitempty"`
	CredentialOwner  *string                 `json:"credentialOwner,omitempty"`
}

// StartProviderAuth begins a login flow.
func (c *Client) StartProviderAuth(ctx context.Context, instanceID, methodID, returnURL, callbackMode string) (*ProviderAuthState, error) {
	payload := map[string]any{"instanceId": instanceID}
	if methodID != "" {
		payload["methodId"] = methodID
	}
	if returnURL != "" {
		payload["returnUrl"] = returnURL
	}
	if callbackMode != "" {
		payload["callbackMode"] = callbackMode
	}
	var out ProviderAuthState
	if err := c.call(ctx, "provider.auth.start", payload, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// RespondProviderAuth answers an interactive auth prompt.
func (c *Client) RespondProviderAuth(ctx context.Context, instanceID, flowID, interactionID string, response json.RawMessage) (*ProviderAuthState, error) {
	payload := map[string]any{
		"instanceId": instanceID, "flowId": flowID,
		"interactionId": interactionID, "response": response,
	}
	var out ProviderAuthState
	if err := c.call(ctx, "provider.auth.respond", payload, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// CompleteProviderAuth completes a callback-based login.
func (c *Client) CompleteProviderAuth(ctx context.Context, instanceID, flowID, callbackURL string) (*ProviderAuthState, error) {
	var out ProviderAuthState
	if err := c.call(ctx, "provider.auth.complete", map[string]any{
		"instanceId": instanceID, "flowId": flowID, "callbackUrl": callbackURL,
	}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// CancelProviderAuth cancels a login flow.
func (c *Client) CancelProviderAuth(ctx context.Context, instanceID, flowID string) error {
	_, err := c.callRaw(ctx, "provider.auth.cancel", map[string]any{
		"instanceId": instanceID, "flowId": flowID,
	})
	return err
}

// LogoutProvider clears provider credentials.
func (c *Client) LogoutProvider(ctx context.Context, instanceID string) (*ProviderAuthState, error) {
	var out ProviderAuthState
	if err := c.call(ctx, "provider.auth.logout", map[string]any{"instanceId": instanceID}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// SubscribeProviderAuth streams auth state changes for a provider.
func (c *Client) SubscribeProviderAuth(ctx context.Context, instanceID string) (*ConfigSubscription, error) {
	return c.rawSubscription(ctx, "provider.auth.subscribe", map[string]any{"instanceId": instanceID})
}

// StartProviderInstall installs a provider's CLI/runtime.
func (c *Client) StartProviderInstall(ctx context.Context, instanceID string) (json.RawMessage, error) {
	return c.callRaw(ctx, "provider.install.start", map[string]any{"instanceId": instanceID})
}

// CancelProviderInstall cancels an install.
func (c *Client) CancelProviderInstall(ctx context.Context, instanceID string) (json.RawMessage, error) {
	return c.callRaw(ctx, "provider.install.cancel", map[string]any{"instanceId": instanceID})
}

// SubscribeProviderInstall streams install progress.
func (c *Client) SubscribeProviderInstall(ctx context.Context, instanceID string) (*ConfigSubscription, error) {
	return c.rawSubscription(ctx, "provider.install.subscribe", map[string]any{"instanceId": instanceID})
}

// RemoveProviderInstall uninstalls a provider runtime.
func (c *Client) RemoveProviderInstall(ctx context.Context, instanceID string) (json.RawMessage, error) {
	return c.callRaw(ctx, "provider.install.remove", map[string]any{"instanceId": instanceID})
}
