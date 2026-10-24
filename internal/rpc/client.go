// Package rpc implements the Effect RPC wire protocol used by the T3 Code
// server: JSON envelopes over a single WebSocket, with request/response
// correlation, streamed chunks, acknowledgements and keepalive pings.
package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

const (
	PingInterval = 5 * time.Second
	PingTimeout  = 20 * time.Second
)

// Error is a typed RPC failure returned by the server.
type Error struct {
	Tag     string          `json:"_tag"`
	Message string          `json:"message"`
	Raw     json.RawMessage `json:"-"`
}

func (e *Error) Error() string {
	if e.Message != "" {
		return e.Tag + ": " + e.Message
	}
	if e.Tag != "" {
		return e.Tag
	}
	return "rpc call failed"
}

type envelope struct {
	Tag       string          `json:"_tag"`
	ID        json.RawMessage `json:"id,omitempty"`
	RequestID json.RawMessage `json:"requestId,omitempty"`
	Method    string          `json:"tag,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Headers   [][2]string     `json:"headers,omitempty"`
	Values    []json.RawMessage `json:"values,omitempty"`
	Exit      *exitEnvelope   `json:"exit,omitempty"`
	Defect    json.RawMessage `json:"defect,omitempty"`
	Error     json.RawMessage `json:"error,omitempty"`
}

type exitEnvelope struct {
	Tag   string       `json:"_tag"`
	Value json.RawMessage `json:"value,omitempty"`
	Cause []causeEntry `json:"cause,omitempty"`
}

type causeEntry struct {
	Tag    string          `json:"_tag"`
	Error  json.RawMessage `json:"error,omitempty"`
	Defect json.RawMessage `json:"defect,omitempty"`
}

type pending struct {
	ch     chan frame
	stream bool
}

// frame is one decoded server message for a request.
type frame struct {
	Chunk []json.RawMessage
	Done  bool
	Value json.RawMessage
	Err   error
}

// Client is a single Effect RPC connection over WebSocket.
type Client struct {
	conn   *websocket.Conn
	write  chan []byte
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.Mutex
	pending map[string]*pending
	nextID  atomic.Int64

	closed  atomic.Bool
	closeMu sync.Mutex
	err     error

	// OnServerRequest handles server-originated requests (rare, used for
	// preview automation and notifications). It must return a JSON value.
	OnServerRequest func(method string, payload json.RawMessage) (json.RawMessage, error)
	// OnClose is called once when the connection ends.
	OnClose func(error)
}

// Dial opens a connection. token may be empty for unauthenticated loopback
// servers; the Authorization header is set only when non-empty.
func Dial(ctx context.Context, baseURL, token string) (*Client, error) {
	wsURL := toWebSocketURL(baseURL)
	hdr := http.Header{}
	if token != "" {
		hdr.Set("Authorization", "Bearer "+token)
	}
	hdr.Set("User-Agent", "t3go/0.1")
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPHeader: hdr})
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", wsURL, err)
	}
	conn.SetReadLimit(128 << 20)

	runCtx, cancel := context.WithCancel(context.Background())
	c := &Client{
		conn:    conn,
		write:   make(chan []byte, 256),
		ctx:     runCtx,
		cancel:  cancel,
		pending: make(map[string]*pending),
	}
	c.wg.Add(2)
	go c.readLoop()
	go c.writeLoop()
	go c.pingLoop()
	return c, nil
}

func toWebSocketURL(base string) string {
	// Accept http(s):// or ws(s):// bases.
	switch {
	case len(base) >= 5 && base[:5] == "ws://":
	case len(base) >= 6 && base[:6] == "wss://":
	case len(base) >= 7 && base[:7] == "http://":
		base = "ws://" + base[7:]
	case len(base) >= 8 && base[:8] == "https://":
		base = "wss://" + base[8:]
	}
	// Trim trailing slash and append the RPC route + protocol marker.
	for len(base) > 0 && base[len(base)-1] == '/' {
		base = base[:len(base)-1]
	}
	return base + "/ws?orchestrationProtocol=2"
}

func (c *Client) pingLoop() {
	t := time.NewTicker(PingInterval)
	defer t.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-t.C:
			if c.closed.Load() {
				return
			}
			c.sendJSON(map[string]any{"_tag": "Ping"})
		}
	}
}

func (c *Client) writeLoop() {
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		case data := <-c.write:
			wctx, cancel := context.WithTimeout(c.ctx, 20*time.Second)
			err := c.conn.Write(wctx, websocket.MessageText, data)
			cancel()
			if err != nil {
				c.fail(fmt.Errorf("write: %w", err))
				return
			}
		}
	}
}

func (c *Client) sendJSON(v any) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	select {
	case c.write <- data:
	case <-c.ctx.Done():
	}
}

func (c *Client) readLoop() {
	defer c.wg.Done()
	for {
		_, data, err := c.conn.Read(c.ctx)
		if err != nil {
			c.fail(fmt.Errorf("read: %w", err))
			return
		}
		var envs []envelope
		trimmed := data
		if len(trimmed) > 0 && trimmed[0] == '[' {
			if err := json.Unmarshal(trimmed, &envs); err != nil {
				continue
			}
		} else {
			var one envelope
			if err := json.Unmarshal(trimmed, &one); err != nil {
				continue
			}
			envs = []envelope{one}
		}
		for i := range envs {
			c.handle(&envs[i])
		}
	}
}

func (c *Client) handle(env *envelope) {
	switch env.Tag {
	case "Pong":
		return
	case "ClientProtocolError":
		return
	case "Defect":
		c.fail(fmt.Errorf("server defect: %s", string(env.Defect)))
		return
	case "Request":
		// Server-originated request: answer so the peer does not hang.
		go func() {
			var out json.RawMessage = json.RawMessage("null")
			if c.OnServerRequest != nil {
				if v, err := c.OnServerRequest(env.Method, env.Payload); err == nil {
					out = v
				}
			}
			resp := map[string]any{
				"_tag":      "Exit",
				"requestId": json.RawMessage(env.ID),
				"exit": map[string]any{
					"_tag":  "Success",
					"value": out,
				},
			}
			c.sendJSON(resp)
		}()
		return
	case "Chunk":
		id := rawID(env.RequestID)
		c.mu.Lock()
		p := c.pending[id]
		c.mu.Unlock()
		if p != nil {
			p.ch <- frame{Chunk: env.Values}
			// Acknowledge so the server can free the chunk.
			c.sendJSON(map[string]any{"_tag": "Ack", "requestId": json.RawMessage(env.RequestID)})
		}
		return
	case "Exit":
		id := rawID(env.RequestID)
		c.mu.Lock()
		p := c.pending[id]
		if p != nil {
			delete(c.pending, id)
		}
		c.mu.Unlock()
		if p == nil {
			return
		}
		if env.Exit == nil {
			p.ch <- frame{Done: true}
			close(p.ch)
			return
		}
		if env.Exit.Tag == "Success" {
			p.ch <- frame{Done: true, Value: env.Exit.Value}
			close(p.ch)
			return
		}
		p.ch <- frame{Done: true, Err: decodeCause(env.Exit.Cause)}
		close(p.ch)
	}
}

func rawID(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	if raw[0] == '"' {
		var s string
		_ = json.Unmarshal(raw, &s)
		return s
	}
	return string(raw)
}

func decodeCause(cause []causeEntry) error {
	for _, entry := range cause {
		switch entry.Tag {
		case "Fail":
			var e Error
			if err := json.Unmarshal(entry.Error, &e); err == nil && e.Tag != "" {
				e.Raw = entry.Error
				return &e
			}
			return errors.New(string(entry.Error))
		case "Die":
			var s string
			if err := json.Unmarshal(entry.Defect, &s); err == nil {
				return fmt.Errorf("server defect: %s", s)
			}
			return fmt.Errorf("server defect: %s", string(entry.Defect))
		case "Interrupt":
			return errors.New("request interrupted")
		}
	}
	return errors.New("rpc call failed")
}

func (c *Client) fail(err error) {
	c.closeMu.Lock()
	if c.err != nil {
		c.closeMu.Unlock()
		return
	}
	c.err = err
	c.closeMu.Unlock()
	if c.closed.CompareAndSwap(false, true) {
		c.cancel()
		_ = c.conn.Close(websocket.StatusNormalClosure, "")
		c.mu.Lock()
		for id, p := range c.pending {
			select {
			case p.ch <- frame{Done: true, Err: err}:
			default:
			}
			close(p.ch)
			delete(c.pending, id)
		}
		c.mu.Unlock()
		if c.OnClose != nil {
			c.OnClose(err)
		}
	}
}

// Err returns the terminal connection error, if any.
func (c *Client) Err() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	return c.err
}

// Close terminates the connection.
func (c *Client) Close() {
	c.fail(context.Canceled)
	c.wg.Wait()
}

// Call performs an RPC and returns the final success value.
func (c *Client) Call(ctx context.Context, method string, payload any) (json.RawMessage, error) {
	s, err := c.start(ctx, method, payload)
	if err != nil {
		return nil, err
	}
	defer s.Cancel()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case f, ok := <-s.frames:
			if !ok {
				return nil, c.Err()
			}
			if f.Err != nil {
				return nil, f.Err
			}
			if f.Done {
				return f.Value, nil
			}
		}
	}
}

// Stream represents a server stream of values for one request.
type Stream struct {
	frames <-chan frame
	cancel func()
}

// Next returns the next chunk of values. done is true when the stream ended
// successfully.
func (s *Stream) Next(ctx context.Context) (values []json.RawMessage, done bool, err error) {
	select {
	case <-ctx.Done():
		return nil, false, ctx.Err()
	case f, ok := <-s.frames:
		if !ok {
			return nil, true, nil
		}
		if f.Err != nil {
			return nil, true, f.Err
		}
		if f.Done {
			return nil, true, nil
		}
		return f.Chunk, false, nil
	}
}

// Cancel interrupts the stream and releases its request slot.
func (s *Stream) Cancel() { s.cancel() }

// Stream starts a streaming RPC.
func (c *Client) Stream(ctx context.Context, method string, payload any) (*Stream, error) {
	return c.start(ctx, method, payload)
}

func (c *Client) start(ctx context.Context, method string, payload any) (*Stream, error) {
	if c.closed.Load() {
		if err := c.Err(); err != nil {
			return nil, err
		}
		return nil, errors.New("connection closed")
	}
	if payload == nil {
		payload = map[string]any{}
	}
	id := strconv.FormatInt(c.nextID.Add(1), 10)
	req := map[string]any{
		"_tag":    "Request",
		"id":      id,
		"tag":     method,
		"payload": payload,
		"headers": [][2]string{},
	}
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	p := &pending{ch: make(chan frame, 64), stream: true}
	c.mu.Lock()
	c.pending[id] = p
	c.mu.Unlock()

	select {
	case c.write <- data:
	case <-c.ctx.Done():
		return nil, c.Err()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	s := &Stream{frames: p.ch}
	s.cancel = func() {
		c.mu.Lock()
		_, ok := c.pending[id]
		delete(c.pending, id)
		c.mu.Unlock()
		if ok {
			c.sendJSON(map[string]any{"_tag": "Interrupt", "requestId": id})
		}
	}
	return s, nil
}

// Done reports whether the connection has ended.
func (c *Client) Done() <-chan struct{} { return c.ctx.Done() }
