// Command t3ctl is a small terminal client used to exercise the Go core
// against a live T3 Code server.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/t3tools/t3go/t3"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	base := os.Getenv("T3_URL")
	if base == "" {
		base = "http://127.0.0.1:3773"
	}
	token := os.Getenv("T3_TOKEN")
	if token == "" {
		if data, err := os.ReadFile("/root/t3c-and/.secrets/token.txt"); err == nil {
			token = strings.TrimSpace(string(data))
		}
	}
	ctx := context.Background()

	switch os.Args[1] {
	case "pair":
		if len(os.Args) < 3 {
			fatal("usage: t3ctl pair <url>")
		}
		pu, err := t3.ParsePairingURL(os.Args[2])
		check(err)
		tok, err := t3.Pair(ctx, t3.PairingInput{BaseURL: pu.BaseURL, Code: pu.Code, ClientLabel: "t3ctl", DeviceType: "desktop", OS: "Linux"})
		check(err)
		fmt.Println(tok.AccessToken)
	case "probe":
		if len(os.Args) < 3 {
			fatal("usage: t3ctl probe <host>")
		}
		desc, base, err := t3.Discover(ctx, os.Args[2], []int{3773, 3000, 8080})
		check(err)
		fmt.Printf("%s %s (%s)\n", base, desc.Label, desc.EnvironmentID)
	case "call":
		if len(os.Args) < 3 {
			fatal("usage: t3ctl call <method> [jsonPayload]")
		}
		client := connect(base, token)
		defer client.Close()
		payload := "{}"
		if len(os.Args) > 3 {
			payload = os.Args[3]
		}
		raw, err := client.CallJSON(ctx, os.Args[2], json.RawMessage(payload))
		check(err)
		fmt.Println(pretty(raw))
	case "stream":
		client := connect(base, token)
		defer client.Close()
		method := os.Args[2]
		payload := "{}"
		if len(os.Args) > 3 {
			payload = os.Args[3]
		}
		var p any
		check(json.Unmarshal([]byte(payload), &p))
		stream, err := client.Stream(ctx, method, p)
		check(err)
		for {
			vals, done, err := stream.Next(ctx)
			if err != nil || done {
				fmt.Println("done:", err)
				return
			}
			for _, v := range vals {
				fmt.Println(pretty(v))
			}
		}
	case "shell":
		client := connect(base, token)
		defer client.Close()
		snap, err := client.ShellHTTP(ctx)
		check(err)
		fmt.Printf("projects=%d threads=%d archived=%d seq=%d\n", len(snap.Projects), len(snap.Threads), len(snap.ArchivedThreads), snap.SnapshotSequence)
		for _, p := range snap.Projects {
			fmt.Printf("  P %s  %s\n", shorten(p.ID), p.Title)
		}
		for _, t := range snap.Threads {
			fmt.Printf("  T %s  %-10s %-6s %s\n", shorten(t.ID), t.Status, truncate(t.ModelSelection.Model, 24), t.Title)
		}
	case "threads":
		client := connect(base, token)
		defer client.Close()
		snap, err := client.ShellHTTP(ctx)
		check(err)
		for _, t := range snap.Threads {
			fmt.Printf("%s\t%s\t%s\n", t.ID, t.Status, t.Title)
		}
	case "thread":
		if len(os.Args) < 3 {
			fatal("usage: t3ctl thread <threadId>")
		}
		client := connect(base, token)
		defer client.Close()
		snap, err := client.ThreadHTTP(ctx, os.Args[2])
		check(err)
		p := snap.Projection
		fmt.Printf("thread: %s  status=%s  model=%s/%s\n", p.Thread.Title, p.Thread.Status, p.Thread.ModelSelection.InstanceID, p.Thread.ModelSelection.Model)
		fmt.Printf("messages=%d turnItems=%d visible=%d runs=%d requests=%d\n", len(p.Messages), len(p.TurnItems), len(p.VisibleTurnItems), len(p.Runs), len(p.RuntimeRequests))
		fmt.Println("--- transcript ---")
		for _, row := range t3.SortVisibleByPosition(p.VisibleTurnItems) {
			item := row.Item
			switch item.Type {
			case "user_message":
				fmt.Printf("\n[user] %s\n", item.Text)
			case "assistant_message":
				fmt.Printf("\n[assistant] %s\n", item.Text)
			case "reasoning":
				fmt.Printf("[reasoning] %s\n", truncate(item.Text, 200))
			case "command_execution":
				fmt.Printf("[cmd] %s exit=%v\n", item.Title, deref(item.ExitCode))
			}
		}
	case "watch":
		if len(os.Args) < 3 {
			fatal("usage: t3ctl watch <threadId>")
		}
		client := connect(base, token)
		defer client.Close()
		w, err := client.WatchThread(ctx, os.Args[2], -1)
		check(err)
		st, err := w.WaitSnapshot(ctx)
		check(err)
		fmt.Printf("snapshot seq=%d visible=%d\n", st.Sequence, len(st.Projection.VisibleTurnItems))
		timeout := time.After(30 * time.Second)
		for {
			select {
			case <-timeout:
				return
			case ev := <-w.Events():
				if ev.Kind == t3.ThreadEventClosed {
					fmt.Println("closed", ev.Err)
					return
				}
				if ev.Kind == t3.ThreadEventEvent {
					fmt.Printf("event seq=%d type=%s\n", ev.Sequence, ev.Event.Type)
				}
			}
		}
	case "send":
		if len(os.Args) < 4 {
			fatal("usage: t3ctl send <threadId> <text>")
		}
		client := connect(base, token)
		defer client.Close()
		seq, err := client.SendMessage(ctx, t3.SendMessageInput{ThreadID: os.Args[2], Text: strings.Join(os.Args[3:], " ")})
		check(err)
		fmt.Println("dispatched at seq", seq)
	case "stop":
		client := connect(base, token)
		defer client.Close()
		check(client.StopThread(ctx, os.Args[2], "stopped by t3ctl"))
		fmt.Println("stopped")
	case "archive", "unarchive", "delete":
		client := connect(base, token)
		defer client.Close()
		switch os.Args[1] {
		case "archive":
			check(client.ArchiveThread(ctx, os.Args[2]))
		case "unarchive":
			check(client.UnarchiveThread(ctx, os.Args[2]))
		case "delete":
			check(client.DeleteThread(ctx, os.Args[2]))
		}
		fmt.Println("ok")
	case "providers":
		client := connect(base, token)
		defer client.Close()
		cfg := client.Config()
		for _, p := range cfg.Providers {
			fmt.Printf("%-14s %-10s enabled=%v installed=%v status=%s models=%d\n", p.InstanceID, p.Driver, p.Enabled, p.Installed, p.Status, len(p.Models))
		}
	default:
		usage()
	}
}

func connect(base, token string) *t3.Client {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := t3.Connect(ctx, t3.Options{BaseURL: base, Token: token, ClientLabel: "t3ctl"})
	check(err)
	return c
}

func pretty(raw json.RawMessage) string {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return string(raw)
	}
	b, _ := json.MarshalIndent(v, "", "  ")
	return string(b)
}

func shorten(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

func deref(p *int) any {
	if p == nil {
		return nil
	}
	return *p
}

func check(err error) {
	if err != nil {
		fatal(err.Error())
	}
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, "error:", msg)
	os.Exit(1)
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: t3ctl <command> [args]

  probe <host>              discover a server
  pair <pairingUrl>         redeem a pairing link and print a token
  call <method> [json]      raw RPC call
  stream <method> [json]    raw RPC stream
  shell                     print projects and threads
  thread <threadId>         print a thread transcript
  watch <threadId>          stream live thread events for 30s
  send <threadId> <text>    dispatch a user message
  stop <threadId>           stop a thread
  archive|unarchive|delete <threadId>
  providers                 list provider instances

env: T3_URL (default http://127.0.0.1:3773), T3_TOKEN`)
	os.Exit(2)
}

var _ = flag.CommandLine
