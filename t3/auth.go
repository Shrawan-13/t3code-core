package t3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	grantTypeTokenExchange = "urn:ietf:params:oauth:grant-type:token-exchange"
	accessTokenType        = "urn:ietf:params:oauth:token-type:access_token"
	bootstrapTokenType     = "urn:t3:params:oauth:token-type:environment-bootstrap"
)

// PairingInput describes a pairing-code redemption.
type PairingInput struct {
	BaseURL     string
	Code        string
	Scopes      []string
	ClientLabel string
	DeviceType  string // mobile, desktop, tablet, unknown
	OS          string // Android, iOS, ...
}

// AccessToken is the result of exchanging a pairing code.
type AccessToken struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
	Scope       string `json:"scope"`
}

// Pair redeems a one-time pairing code for a bearer access token.
func Pair(ctx context.Context, in PairingInput) (*AccessToken, error) {
	base := strings.TrimRight(strings.TrimSpace(in.BaseURL), "/")
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		base = "http://" + base
	}
	form := url.Values{}
	form.Set("grant_type", grantTypeTokenExchange)
	form.Set("subject_token", strings.TrimSpace(in.Code))
	form.Set("subject_token_type", bootstrapTokenType)
	form.Set("requested_token_type", accessTokenType)
	if len(in.Scopes) > 0 {
		form.Set("scope", strings.Join(in.Scopes, " "))
	}
	if in.ClientLabel != "" {
		form.Set("client_label", in.ClientLabel)
	}
	if in.DeviceType != "" {
		form.Set("client_device_type", in.DeviceType)
	}
	if in.OS != "" {
		form.Set("client_os", in.OS)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/oauth/token", strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	client := &http.Client{Timeout: 45 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return nil, &HTTPError{Status: resp.StatusCode, Body: string(data)}
	}
	var tok AccessToken
	if err := json.Unmarshal(data, &tok); err != nil {
		return nil, err
	}
	if tok.AccessToken == "" {
		return nil, errors.New("pairing failed: server returned no access token")
	}
	return &tok, nil
}

// PairingURL is a parsed t3 pairing link.
type PairingURL struct {
	BaseURL string
	Code    string
}

// ParsePairingURL accepts a full pairing link (t3.codes/pair#token=...,
// http://host:3773/#token=..., or t3code://...?pairingUrl=...) and extracts
// the server origin and one-time code.
func ParsePairingURL(raw string) (*PairingURL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, errors.New("empty pairing link")
	}
	if strings.HasPrefix(raw, "t3code:") {
		if u, err := url.Parse(raw); err == nil {
			if inner := u.Query().Get("pairingUrl"); inner != "" {
				return ParsePairingURL(inner)
			}
		}
	}
	u, err := url.Parse(raw)
	if err != nil {
		// Allow bare host:port.
		return &PairingURL{BaseURL: raw}, nil
	}
	code := ""
	if u.Fragment != "" {
		if vals, err := url.ParseQuery(u.Fragment); err == nil {
			code = vals.Get("token")
		}
	}
	if code == "" {
		code = u.Query().Get("token")
	}
	u.Fragment = ""
	u.RawQuery = ""
	u.Path = "/"
	return &PairingURL{BaseURL: strings.TrimRight(u.String(), "/"), Code: code}, nil
}

// PairAndConnect redeems a pairing code and connects with the new token.
func PairAndConnect(ctx context.Context, in PairingInput) (*Client, *AccessToken, error) {
	tok, err := Pair(ctx, in)
	if err != nil {
		return nil, nil, err
	}
	client, err := Connect(ctx, Options{BaseURL: in.BaseURL, Token: tok.AccessToken, ClientLabel: in.ClientLabel})
	if err != nil {
		return nil, tok, err
	}
	return client, tok, nil
}

// Probe checks a candidate server origin and returns its descriptor.
func Probe(ctx context.Context, baseURL string) (*EnvironmentDescriptor, error) {
	base := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		base = "http://" + base
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/.well-known/t3/environment", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, &HTTPError{Status: resp.StatusCode}
	}
	var d EnvironmentDescriptor
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&d); err != nil {
		return nil, err
	}
	if d.EnvironmentID == "" {
		return nil, errors.New("not a T3 Code server")
	}
	return &d, nil
}

// Discover scans a host on the common T3 ports and returns the first server
// that answers. Ports are tried in order and failures are ignored.
func Discover(ctx context.Context, host string, ports []int) (*EnvironmentDescriptor, string, error) {
	if len(ports) == 0 {
		ports = []int{3773, 3000, 8080}
	}
	host = strings.TrimSpace(host)
	host = strings.TrimPrefix(host, "http://")
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimSuffix(host, "/")
	type result struct {
		desc *EnvironmentDescriptor
		base string
		err  error
	}
	ch := make(chan result, len(ports))
	for _, port := range ports {
		go func(port int) {
			base := fmt.Sprintf("http://%s:%d", host, port)
			desc, err := Probe(ctx, base)
			ch <- result{desc: desc, base: base, err: err}
		}(port)
	}
	var firstErr error
	for range ports {
		r := <-ch
		if r.err == nil {
			return r.desc, r.base, nil
		}
		if firstErr == nil {
			firstErr = r.err
		}
	}
	if firstErr == nil {
		firstErr = errors.New("no T3 Code server found")
	}
	return nil, "", firstErr
}

// ClientSession is one connected/authorized client session (HTTP auth API).
type ClientSession struct {
	SessionID       string     `json:"sessionId"`
	Subject         string     `json:"subject"`
	Method          string     `json:"method,omitempty"`
	Scopes          []string   `json:"scopes,omitempty"`
	Permissions     []string   `json:"permissions,omitempty"`
	Client          ClientInfo `json:"client"`
	IssuedAt        string     `json:"issuedAt,omitempty"`
	LastConnectedAt string     `json:"lastConnectedAt,omitempty"`
	ExpiresAt       string     `json:"expiresAt,omitempty"`
	Connected       bool       `json:"connected,omitempty"`
	Current         bool       `json:"current,omitempty"`
}

// ClientInfo describes the app that owns a session.
type ClientInfo struct {
	Label      string `json:"label,omitempty"`
	IPAddress  string `json:"ipAddress,omitempty"`
	UserAgent  string `json:"userAgent,omitempty"`
	DeviceType string `json:"deviceType,omitempty"`
	OS         string `json:"os,omitempty"`
}

// Label returns the human label for the session.
func (s ClientSession) LabelOrSubject() string {
	if s.Client.Label != "" {
		return s.Client.Label
	}
	return s.Subject
}

// ListSessions lists client sessions authorized on the server.
func (c *Client) ListSessions(ctx context.Context) ([]ClientSession, error) {
	var out []ClientSession
	if err := c.doJSON(ctx, http.MethodGet, "/api/auth/clients", nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// RevokeSession revokes one client session by id.
func (c *Client) RevokeSession(ctx context.Context, sessionID string) error {
	return c.doJSON(ctx, http.MethodPost, "/api/auth/clients/revoke", map[string]any{"sessionId": sessionID}, nil)
}

// RevokeOtherSessions revokes every session except the current one.
func (c *Client) RevokeOtherSessions(ctx context.Context) (int, error) {
	var out struct {
		RevokedCount int `json:"revokedCount"`
	}
	if err := c.doJSON(ctx, http.MethodPost, "/api/auth/clients/revoke-others", map[string]any{}, &out); err != nil {
		return 0, err
	}
	return out.RevokedCount, nil
}

// PairingLink is an active one-time pairing credential (secret omitted).
type PairingLink struct {
	ID        string   `json:"id"`
	Subject   string   `json:"subject,omitempty"`
	Label     string   `json:"label,omitempty"`
	Scopes    []string `json:"scopes,omitempty"`
	CreatedAt string   `json:"createdAt,omitempty"`
	ExpiresAt string   `json:"expiresAt,omitempty"`
}

// ListPairingLinks lists active pairing credentials.
func (c *Client) ListPairingLinks(ctx context.Context) ([]PairingLink, error) {
	var out []PairingLink
	if err := c.doJSON(ctx, http.MethodGet, "/api/auth/pairing-links", nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// PairingCredential is a freshly minted one-time pairing credential.
type PairingCredential struct {
	ID         string `json:"id"`
	Credential string `json:"credential"`
	Label      string `json:"label,omitempty"`
	ExpiresAt  string `json:"expiresAt"`
}

// CreatePairingCredential mints a new one-time pairing credential.
func (c *Client) CreatePairingCredential(ctx context.Context, label string, scopes []string) (*PairingCredential, error) {
	body := map[string]any{}
	if label != "" {
		body["label"] = label
	}
	if len(scopes) > 0 {
		body["scopes"] = scopes
	}
	var out PairingCredential
	if err := c.doJSON(ctx, http.MethodPost, "/api/auth/pairing-token", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// RevokePairingLink invalidates an unused pairing credential.
func (c *Client) RevokePairingLink(ctx context.Context, id string) error {
	return c.doJSON(ctx, http.MethodPost, "/api/auth/pairing-links/revoke", map[string]any{"id": id}, nil)
}
