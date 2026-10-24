package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/coder/websocket"
)

func main() {
	tokenBytes, err := os.ReadFile("/root/t3c-and/.secrets/token.txt")
	if err != nil {
		panic(err)
	}
	token := strings.TrimSpace(string(tokenBytes))

	method := "server.getConfig"
	payload := "{}"
	if len(os.Args) > 1 {
		method = os.Args[1]
	}
	if len(os.Args) > 2 {
		payload = os.Args[2]
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	url := "ws://127.0.0.1:3773/ws?orchestrationProtocol=2"
	opts := &websocket.DialOptions{
		HTTPHeader: map[string][]string{
			"Authorization": {"Bearer " + token},
			"User-Agent":    {"t3go-probe/0.1"},
		},
	}
	conn, _, err := websocket.Dial(ctx, url, opts)
	if err != nil {
		fmt.Println("dial error:", err)
		os.Exit(1)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(64 << 20)

	req := map[string]any{
		"_tag":   "Request",
		"id":     "1",
		"tag":    method,
		"payload": json.RawMessage(payload),
		"headers": []any{},
	}
	data, _ := json.Marshal(req)
	fmt.Println(">>", string(data))
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
		fmt.Println("write error:", err)
		os.Exit(1)
	}

	for {
		_, msg, err := conn.Read(ctx)
		if err != nil {
			fmt.Println("read error:", err)
			return
		}
		fmt.Println("<<", string(msg))
		if strings.Contains(string(msg), `"_tag":"Exit"`) {
			return
		}
	}
}
