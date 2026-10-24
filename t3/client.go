// Package t3 is a Go client SDK for the T3 Code server. It implements the
// Effect RPC WebSocket protocol plus the HTTP endpoints used for snapshots,
// auth and uploads, and exposes typed access to projects, threads, terminals,
// files, git, pull requests and provider configuration.
package t3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/t3tools/t3go/internal/rpc"
)

const (
	// DefaultTimeout bounds RPC calls that do not carry their own deadline.
	DefaultTimeout = 90 * time.Second
)

// Options configures a connection to a T3 Code server.
type Options struct {
	// BaseURL is the server origin, e.g. http://127.0.0.1:3773 or a LAN address.
	BaseURL string
	// Token is a bearer access token. It may be empty only for servers that
	// allow unauthenticated loopback access.
	Token string
	// ClientLabel is recorded by the server in its connections list.
	ClientLabel string
	// OnEvent receives connection lifecycle notifications (optional).
	OnEvent func(Event)
}

// EventKind classifies connection lifecycle events.
type EventKind string

const (
	EventConnected    EventKind = "connected"
	EventDisconnected EventKind = "disconnected"
)

// Event is a connection lifecycle notification.
type Event struct {
	Kind    EventKind
	Message string
	Err     error
}

// Client is a connected T3 Code client. It is safe for concurrent use.
type Client struct {
	baseURL string
	token   string
	label   string

	rpc  *rpc.Client
	http *http.Client

	mu    sync.RWMutex
	cfg   *ServerConfig
	onEvent func(Event)
}

// Connect dials the server, verifies the orchestration protocol and loads the
// server configuration. The returned client must be closed by the caller.
func Connect(ctx context.Context, opts Options) (*Client, error) {
	base := strings.TrimSpace(opts.BaseURL)
	if base == "" {
		return nil, errors.New("t3: BaseURL is required")
	}
	base = strings.TrimRight(base, "/")
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		base = "http://" + base
	}

	c := &Client{
		baseURL: base,
		token:   strings.TrimSpace(opts.Token),
		label:   opts.ClientLabel,
		http:    &http.Client{Timeout: 60 * time.Second},
		onEvent: opts.OnEvent,
	}
	if err := c.checkProtocol(ctx); err != nil {
		return nil, err
	}
	rpcConn, err := rpc.Dial(ctx, base, c.token)
	if err != nil {
		return nil, err
	}
	c.rpc = rpcConn
	rpcConn.OnClose = func(err error) {
		if c.onEvent != nil {
			c.onEvent(Event{Kind: EventDisconnected, Message: "connection closed", Err: err})
		}
	}
	if c.onEvent != nil {
		c.onEvent(Event{Kind: EventConnected, Message: base})
	}
	cfg, err := c.LoadConfig(ctx)
	if err != nil {
		c.Close()
		return nil, err
	}
	c.mu.Lock()
	c.cfg = cfg
	c.mu.Unlock()
	return c, nil
}

// BaseURL is the normalized server origin.
func (c *Client) BaseURL() string { return c.baseURL }

// Close terminates the connection.
func (c *Client) Close() {
	if c.rpc != nil {
		c.rpc.Close()
	}
}

// Done is closed when the connection ends.
func (c *Client) Done() <-chan struct{} {
	if c.rpc == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return c.rpc.Done()
}

// Err reports the terminal connection error, if any.
func (c *Client) Err() error {
	if c.rpc == nil {
		return nil
	}
	return c.rpc.Err()
}

// Config returns the cached server configuration.
func (c *Client) Config() *ServerConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cfg
}

func (c *Client) setConfig(cfg *ServerConfig) {
	c.mu.Lock()
	c.cfg = cfg
	c.mu.Unlock()
}

// LoadConfig fetches server.getConfig.
func (c *Client) LoadConfig(ctx context.Context) (*ServerConfig, error) {
	var cfg ServerConfig
	if err := c.call(ctx, "server.getConfig", map[string]any{}, &cfg); err != nil {
		return nil, err
	}
	c.setConfig(&cfg)
	return &cfg, nil
}

// Call invokes an RPC method and decodes its success value into out (which may
// be nil).
func (c *Client) Call(ctx context.Context, method string, payload any, out any) error {
	raw, err := c.callRaw(ctx, method, payload)
	if err != nil {
		return err
	}
	if out == nil || len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	return json.Unmarshal(raw, out)
}

// CallJSON invokes an RPC method with raw JSON payload and returns raw JSON.
func (c *Client) CallJSON(ctx context.Context, method string, payload json.RawMessage) (json.RawMessage, error) {
	var p any
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, fmt.Errorf("invalid payload: %w", err)
		}
	}
	return c.callRaw(ctx, method, p)
}

func (c *Client) callRaw(ctx context.Context, method string, payload any) (json.RawMessage, error) {
	if c.rpc == nil {
		return nil, errors.New("t3: not connected")
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, DefaultTimeout)
		defer cancel()
	}
	return c.rpc.Call(ctx, method, payload)
}

func (c *Client) call(ctx context.Context, method string, payload, out any) error {
	raw, err := c.callRaw(ctx, method, payload)
	if err != nil {
		return err
	}
	if out == nil || len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	return json.Unmarshal(raw, out)
}

// Stream opens a streaming RPC. The caller owns the returned stream.
func (c *Client) Stream(ctx context.Context, method string, payload any) (*rpc.Stream, error) {
	if c.rpc == nil {
		return nil, errors.New("t3: not connected")
	}
	return c.rpc.Stream(ctx, method, payload)
}

// --- HTTP helpers -----------------------------------------------------------

func (c *Client) doJSON(ctx context.Context, method, path string, body any, out any) error {
	var rdr io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, rdr)
	if err != nil {
		return err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return &HTTPError{Status: resp.StatusCode, Body: string(data)}
	}
	if out == nil {
		return nil
	}
	if len(data) == 0 {
		return nil
	}
	return json.Unmarshal(data, out)
}

// HTTPError is a non-2xx response from an HTTP endpoint.
type HTTPError struct {
	Status int
	Body   string
}

func (e *HTTPError) Error() string {
	msg := strings.TrimSpace(e.Body)
	if len(msg) > 300 {
		msg = msg[:300]
	}
	if msg == "" {
		return fmt.Sprintf("http %d", e.Status)
	}
	return fmt.Sprintf("http %d: %s", e.Status, msg)
}

func (c *Client) checkProtocol(ctx context.Context) error {
	var desc EnvironmentDescriptor
	if err := c.doJSON(ctx, http.MethodGet, "/.well-known/t3/environment", nil, &desc); err != nil {
		var herr *HTTPError
		if errors.As(err, &herr) && herr.Status == http.StatusUnauthorized {
			return fmt.Errorf("server requires authentication: %w", err)
		}
		return fmt.Errorf("read environment descriptor: %w", err)
	}
	if desc.OrchestrationProtocolVersion != 0 && desc.OrchestrationProtocolVersion != 2 {
		return fmt.Errorf("server orchestration protocol %d is not supported (need 2)", desc.OrchestrationProtocolVersion)
	}
	return nil
}

// Descriptor fetches the environment descriptor over HTTP.
func (c *Client) Descriptor(ctx context.Context) (*EnvironmentDescriptor, error) {
	var d EnvironmentDescriptor
	if err := c.doJSON(ctx, http.MethodGet, "/.well-known/t3/environment", nil, &d); err != nil {
		return nil, err
	}
	return &d, nil
}

// SessionState is the response of /api/auth/session.
type SessionState struct {
	Authenticated bool            `json:"authenticated"`
	Auth          json.RawMessage `json:"auth,omitempty"`
	Scopes        []string        `json:"scopes,omitempty"`
	SessionMethod string          `json:"sessionMethod,omitempty"`
	ExpiresAt     string          `json:"expiresAt,omitempty"`
}

// Session fetches the auth session state for the current credentials.
func (c *Client) Session(ctx context.Context) (*SessionState, error) {
	var s SessionState
	if err := c.doJSON(ctx, http.MethodGet, "/api/auth/session", nil, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// ShellHTTP fetches the current shell snapshot over HTTP.
func (c *Client) ShellHTTP(ctx context.Context) (*ShellSnapshot, error) {
	var s ShellSnapshot
	if err := c.orchestrationJSON(ctx, http.MethodGet, "/api/orchestration/shell", nil, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// ThreadHTTP fetches a full thread projection over HTTP.
func (c *Client) ThreadHTTP(ctx context.Context, threadID string) (*ThreadDetailSnapshot, error) {
	var s ThreadDetailSnapshot
	path := "/api/orchestration/threads/" + url.PathEscape(threadID)
	if err := c.orchestrationJSON(ctx, http.MethodGet, path, nil, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

func (c *Client) orchestrationJSON(ctx context.Context, method, path string, body, out any) error {
	var rdr io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, rdr)
	if err != nil {
		return err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	req.Header.Set("x-t3-orchestration-protocol", "2")
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return &HTTPError{Status: resp.StatusCode, Body: string(data)}
	}
	if out == nil || len(data) == 0 {
		return nil
	}
	return json.Unmarshal(data, out)
}
