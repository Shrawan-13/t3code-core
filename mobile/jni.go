// Package main builds the Android JNI bridge for the T3 Code Go core.
//
// It is intentionally a pull-based API: Kotlin starts watches/streams and then
// polls next(handle, timeoutMs) on a background dispatcher. That keeps JNI free
// of callback thread-attachment problems and lets the Kotlin side use ordinary
// coroutines/Flow.
//
// Every exported function returns a JSON string. Errors are encoded as
// {"__error":"...","__tag":"..."} so Kotlin can throw a typed exception.
package main

/*
#include <jni.h>
#include <stdlib.h>
#include <string.h>

static char* t3_go_string(JNIEnv* env, jstring s) {
	if (s == NULL) return NULL;
	const char* c = (*env)->GetStringUTFChars(env, s, NULL);
	if (c == NULL) return NULL;
	char* out = strdup(c);
	(*env)->ReleaseStringUTFChars(env, s, c);
	return out;
}

static jstring t3_new_string(JNIEnv* env, const char* s) {
	return (*env)->NewStringUTF(env, s == NULL ? "" : s);
}

static unsigned char* t3_go_bytes(JNIEnv* env, jbyteArray arr, jsize* len) {
	if (arr == NULL) { *len = 0; return NULL; }
	*len = (*env)->GetArrayLength(env, arr);
	unsigned char* out = (unsigned char*)malloc((size_t)*len);
	if (out == NULL) return NULL;
	(*env)->GetByteArrayRegion(env, arr, 0, *len, (jbyte*)out);
	return out;
}
*/
import "C"

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/t3tools/t3go/t3"
)

// --- handle registries ------------------------------------------------------

var (
	registryMu sync.Mutex
	nextHandle int64 = 1

	clients   = map[int64]*t3.Client{}
	streams   = map[int64]*streamHandle{}
	shells    = map[int64]*t3.ShellWatcher{}
	threads   = map[int64]*t3.ThreadWatcher{}
	terminals = map[int64]*t3.TerminalSubscription{}
)

type streamHandle struct {
	stream *t3Stream
	close  func()
}

// t3Stream is the subset of *rpc.Stream we need, kept behind a tiny interface
// so this package does not depend on internal types in its signatures.
type t3Stream struct {
	next  func(ctx context.Context) ([]json.RawMessage, bool, error)
	close func()
}

func newHandle() int64 {
	registryMu.Lock()
	defer registryMu.Unlock()
	h := nextHandle
	nextHandle++
	return h
}

func putClient(c *t3.Client) int64 {
	h := newHandle()
	registryMu.Lock()
	clients[h] = c
	registryMu.Unlock()
	return h
}

func getClient(h int64) (*t3.Client, error) {
	registryMu.Lock()
	defer registryMu.Unlock()
	c, ok := clients[h]
	if !ok {
		return nil, fmt.Errorf("unknown client handle %d", h)
	}
	return c, nil
}

func takeClient(h int64) *t3.Client {
	registryMu.Lock()
	defer registryMu.Unlock()
	c := clients[h]
	delete(clients, h)
	return c
}

// --- JNI helpers ------------------------------------------------------------

func goString(env *C.JNIEnv, s C.jstring) string {
	if s == 0 {
		return ""
	}
	cs := C.t3_go_string(env, s)
	if cs == nil {
		return ""
	}
	defer C.free(unsafe.Pointer(cs))
	return C.GoString(cs)
}

func javaString(env *C.JNIEnv, s string) C.jstring {
	cs := C.CString(s)
	defer C.free(unsafe.Pointer(cs))
	return C.t3_new_string(env, cs)
}

func okJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return `{"__error":"encoding failed"}`
	}
	return string(b)
}

func errJSON(err error) string {
	tag := ""
	if e, ok := err.(*t3Error); ok {
		tag = e.tag
	}
	b, _ := json.Marshal(map[string]any{"__error": err.Error(), "__tag": tag})
	return string(b)
}

type t3Error struct {
	tag string
	err error
}

func (e *t3Error) Error() string { return e.err.Error() }

func wrap(method func() (any, error)) string {
	v, err := method()
	if err != nil {
		return errJSON(err)
	}
	if v == nil {
		return `{"ok":true}`
	}
	return okJSON(v)
}

func ctxTimeout(ms int) (context.Context, context.CancelFunc) {
	if ms <= 0 {
		ms = 30000
	}
	return context.WithTimeout(context.Background(), time.Duration(ms)*time.Millisecond)
}

// --- connection -------------------------------------------------------------

//export Java_com_t3codes_core_Native_nativeConnect
func Java_com_t3codes_core_Native_nativeConnect(env *C.JNIEnv, clazz C.jclass, base C.jstring, token C.jstring, label C.jstring) C.jstring {
	baseURL := goString(env, base)
	tok := goString(env, token)
	lbl := goString(env, label)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client, err := t3.Connect(ctx, t3.Options{BaseURL: baseURL, Token: tok, ClientLabel: lbl})
	if err != nil {
		return javaString(env, errJSON(err))
	}
	h := putClient(client)
	cfg := client.Config()
	return javaString(env, okJSON(map[string]any{"handle": h, "config": cfg}))
}

//export Java_com_t3codes_core_Native_nativeClose
func Java_com_t3codes_core_Native_nativeClose(env *C.JNIEnv, clazz C.jclass, handle C.jlong) C.jstring {
	if c := takeClient(int64(handle)); c != nil {
		c.Close()
	}
	return javaString(env, `{"ok":true}`)
}

//export Java_com_t3codes_core_Native_nativeConfig
func Java_com_t3codes_core_Native_nativeConfig(env *C.JNIEnv, clazz C.jclass, handle C.jlong) C.jstring {
	c, err := getClient(int64(handle))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	return javaString(env, okJSON(c.Config()))
}

//export Java_com_t3codes_core_Native_nativeCall
func Java_com_t3codes_core_Native_nativeCall(env *C.JNIEnv, clazz C.jclass, handle C.jlong, method C.jstring, payload C.jstring) C.jstring {
	c, err := getClient(int64(handle))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	ctx, cancel := ctxTimeout(120000)
	defer cancel()
	raw, err := c.CallJSON(ctx, goString(env, method), json.RawMessage(goString(env, payload)))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	if len(raw) == 0 {
		return javaString(env, `{"ok":true}`)
	}
	return javaString(env, string(raw))
}

// --- generic raw streams ----------------------------------------------------

//export Java_com_t3codes_core_Native_nativeStreamOpen
func Java_com_t3codes_core_Native_nativeStreamOpen(env *C.JNIEnv, clazz C.jclass, handle C.jlong, method C.jstring, payload C.jstring) C.jstring {
	c, err := getClient(int64(handle))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	var p any
	if s := goString(env, payload); s != "" && s != "null" {
		if err := json.Unmarshal([]byte(s), &p); err != nil {
			return javaString(env, errJSON(err))
		}
	}
	stream, err := c.Stream(context.Background(), goString(env, method), p)
	if err != nil {
		return javaString(env, errJSON(err))
	}
	h := newHandle()
	registryMu.Lock()
	streams[h] = &streamHandle{
		stream: &t3Stream{
			next: func(ctx context.Context) ([]json.RawMessage, bool, error) {
				return stream.Next(ctx)
			},
			close: stream.Cancel,
		},
	}
	registryMu.Unlock()
	return javaString(env, okJSON(map[string]any{"handle": h}))
}

//export Java_com_t3codes_core_Native_nativeStreamNext
func Java_com_t3codes_core_Native_nativeStreamNext(env *C.JNIEnv, clazz C.jclass, handle C.jlong, timeoutMs C.jint) C.jstring {
	registryMu.Lock()
	s := streams[int64(handle)]
	registryMu.Unlock()
	if s == nil {
		return javaString(env, errJSON(fmt.Errorf("unknown stream handle %d", int64(handle))))
	}
	ctx, cancel := ctxTimeout(int(timeoutMs))
	defer cancel()
	values, done, err := s.stream.next(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return javaString(env, `{"values":[],"done":false,"timeout":true}`)
		}
		return javaString(env, errJSON(err))
	}
	return javaString(env, okJSON(map[string]any{"values": values, "done": done}))
}

//export Java_com_t3codes_core_Native_nativeStreamClose
func Java_com_t3codes_core_Native_nativeStreamClose(env *C.JNIEnv, clazz C.jclass, handle C.jlong) C.jstring {
	registryMu.Lock()
	s := streams[int64(handle)]
	delete(streams, int64(handle))
	registryMu.Unlock()
	if s != nil && s.stream.close != nil {
		s.stream.close()
	}
	return javaString(env, `{"ok":true}`)
}

// --- shell ------------------------------------------------------------------

//export Java_com_t3codes_core_Native_nativeShell
func Java_com_t3codes_core_Native_nativeShell(env *C.JNIEnv, clazz C.jclass, handle C.jlong) C.jstring {
	c, err := getClient(int64(handle))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	ctx, cancel := ctxTimeout(60000)
	defer cancel()
	snap, err := c.ShellHTTP(ctx)
	if err != nil {
		return javaString(env, errJSON(err))
	}
	return javaString(env, okJSON(snap))
}

//export Java_com_t3codes_core_Native_nativeShellWatch
func Java_com_t3codes_core_Native_nativeShellWatch(env *C.JNIEnv, clazz C.jclass, handle C.jlong) C.jstring {
	c, err := getClient(int64(handle))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	w, err := c.WatchShell(context.Background())
	if err != nil {
		return javaString(env, errJSON(err))
	}
	h := newHandle()
	registryMu.Lock()
	shells[h] = w
	registryMu.Unlock()
	return javaString(env, okJSON(map[string]any{"handle": h}))
}

//export Java_com_t3codes_core_Native_nativeShellNext
func Java_com_t3codes_core_Native_nativeShellNext(env *C.JNIEnv, clazz C.jclass, handle C.jlong, timeoutMs C.jint) C.jstring {
	registryMu.Lock()
	w := shells[int64(handle)]
	registryMu.Unlock()
	if w == nil {
		return javaString(env, errJSON(fmt.Errorf("unknown shell handle %d", int64(handle))))
	}
	if timeoutMs <= 0 {
		timeoutMs = 30000
	}
	timer := time.NewTimer(time.Duration(timeoutMs) * time.Millisecond)
	defer timer.Stop()
	select {
	case ev, ok := <-w.Events():
		if !ok {
			return javaString(env, `{"kind":"closed","done":true}`)
		}
		return javaString(env, okJSON(ev))
	case <-timer.C:
		return javaString(env, `{"timeout":true}`)
	}
}

//export Java_com_t3codes_core_Native_nativeShellState
func Java_com_t3codes_core_Native_nativeShellState(env *C.JNIEnv, clazz C.jclass, handle C.jlong) C.jstring {
	registryMu.Lock()
	w := shells[int64(handle)]
	registryMu.Unlock()
	if w == nil {
		return javaString(env, errJSON(fmt.Errorf("unknown shell handle %d", int64(handle))))
	}
	b, err := w.SnapshotJSON()
	if err != nil {
		return javaString(env, errJSON(err))
	}
	return javaString(env, string(b))
}

//export Java_com_t3codes_core_Native_nativeShellClose
func Java_com_t3codes_core_Native_nativeShellClose(env *C.JNIEnv, clazz C.jclass, handle C.jlong) C.jstring {
	registryMu.Lock()
	w := shells[int64(handle)]
	delete(shells, int64(handle))
	registryMu.Unlock()
	if w != nil {
		w.Close()
	}
	return javaString(env, `{"ok":true}`)
}

// --- threads ----------------------------------------------------------------

//export Java_com_t3codes_core_Native_nativeThread
func Java_com_t3codes_core_Native_nativeThread(env *C.JNIEnv, clazz C.jclass, handle C.jlong, threadID C.jstring) C.jstring {
	c, err := getClient(int64(handle))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	ctx, cancel := ctxTimeout(60000)
	defer cancel()
	snap, err := c.ThreadHTTP(ctx, goString(env, threadID))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	return javaString(env, okJSON(snap))
}

//export Java_com_t3codes_core_Native_nativeThreadWatch
func Java_com_t3codes_core_Native_nativeThreadWatch(env *C.JNIEnv, clazz C.jclass, handle C.jlong, threadID C.jstring, afterSequence C.jlong, acceptBounded C.jboolean) C.jstring {
	c, err := getClient(int64(handle))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	w, err := c.WatchThreadPartial(context.Background(), goString(env, threadID), int64(afterSequence), acceptBounded != 0)
	if err != nil {
		return javaString(env, errJSON(err))
	}
	h := newHandle()
	registryMu.Lock()
	threads[h] = w
	registryMu.Unlock()
	return javaString(env, okJSON(map[string]any{"handle": h}))
}

//export Java_com_t3codes_core_Native_nativeThreadNext
func Java_com_t3codes_core_Native_nativeThreadNext(env *C.JNIEnv, clazz C.jclass, handle C.jlong, timeoutMs C.jint) C.jstring {
	registryMu.Lock()
	w := threads[int64(handle)]
	registryMu.Unlock()
	if w == nil {
		return javaString(env, errJSON(fmt.Errorf("unknown thread handle %d", int64(handle))))
	}
	if timeoutMs <= 0 {
		timeoutMs = 30000
	}
	timer := time.NewTimer(time.Duration(timeoutMs) * time.Millisecond)
	defer timer.Stop()
	select {
	case ev, ok := <-w.Events():
		if !ok {
			return javaString(env, `{"kind":"closed","done":true}`)
		}
		return javaString(env, okJSON(ev))
	case <-timer.C:
		return javaString(env, `{"timeout":true}`)
	}
}

//export Java_com_t3codes_core_Native_nativeThreadState
func Java_com_t3codes_core_Native_nativeThreadState(env *C.JNIEnv, clazz C.jclass, handle C.jlong) C.jstring {
	registryMu.Lock()
	w := threads[int64(handle)]
	registryMu.Unlock()
	if w == nil {
		return javaString(env, errJSON(fmt.Errorf("unknown thread handle %d", int64(handle))))
	}
	b, err := w.StateJSON()
	if err != nil {
		return javaString(env, errJSON(err))
	}
	return javaString(env, string(b))
}

//export Java_com_t3codes_core_Native_nativeThreadHistory
func Java_com_t3codes_core_Native_nativeThreadHistory(env *C.JNIEnv, clazz C.jclass, handle C.jlong, threadID C.jstring, cursor C.jstring) C.jstring {
	c, err := getClient(int64(handle))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	ctx, cancel := ctxTimeout(60000)
	defer cancel()
	page, err := c.ThreadHistory(ctx, goString(env, threadID), goString(env, cursor))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	return javaString(env, okJSON(page))
}

//export Java_com_t3codes_core_Native_nativeThreadClose
func Java_com_t3codes_core_Native_nativeThreadClose(env *C.JNIEnv, clazz C.jclass, handle C.jlong) C.jstring {
	registryMu.Lock()
	w := threads[int64(handle)]
	delete(threads, int64(handle))
	registryMu.Unlock()
	if w != nil {
		w.Close()
	}
	return javaString(env, `{"ok":true}`)
}

// --- actions ----------------------------------------------------------------

//export Java_com_t3codes_core_Native_nativeSendMessage
func Java_com_t3codes_core_Native_nativeSendMessage(env *C.JNIEnv, clazz C.jclass, handle C.jlong, threadID C.jstring, text C.jstring, attachmentsJSON C.jstring, mode C.jstring, targetRunID C.jstring) C.jstring {
	c, err := getClient(int64(handle))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	var atts []json.RawMessage
	if s := goString(env, attachmentsJSON); s != "" && s != "null" {
		if err := json.Unmarshal([]byte(s), &atts); err != nil {
			return javaString(env, errJSON(err))
		}
	}
	ctx, cancel := ctxTimeout(60000)
	defer cancel()
	seq, err := c.SendMessage(ctx, t3.SendMessageInput{
		ThreadID:       goString(env, threadID),
		Text:           goString(env, text),
		Attachments:    atts,
		Mode:           t3.DispatchMode(goString(env, mode)),
		TargetRunID:    goString(env, targetRunID),
	})
	if err != nil {
		return javaString(env, errJSON(err))
	}
	return javaString(env, okJSON(map[string]any{"ok": true, "sequence": seq}))
}

//export Java_com_t3codes_core_Native_nativeLaunchThread
func Java_com_t3codes_core_Native_nativeLaunchThread(env *C.JNIEnv, clazz C.jclass, handle C.jlong, projectID C.jstring, title C.jstring, modelJSON C.jstring, workspaceJSON C.jstring, initialText C.jstring, runtimeMode C.jstring, interactionMode C.jstring) C.jstring {
	c, err := getClient(int64(handle))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	var model t3.ModelSelection
	if s := goString(env, modelJSON); s != "" && s != "null" {
		if err := json.Unmarshal([]byte(s), &model); err != nil {
			return javaString(env, errJSON(err))
		}
	}
	ws := t3.WorkspaceStrategy{Type: "root"}
	if s := goString(env, workspaceJSON); s != "" && s != "null" {
		if err := json.Unmarshal([]byte(s), &ws); err != nil {
			return javaString(env, errJSON(err))
		}
	}
	in := t3.ThreadLaunchInput{
		ProjectID:       goString(env, projectID),
		Title:           goString(env, title),
		ModelSelection:  model,
		Workspace:       ws,
		RuntimeMode:     goString(env, runtimeMode),
		InteractionMode: goString(env, interactionMode),
	}
	if txt := goString(env, initialText); txt != "" {
		in.InitialMessage = &txt
	}
	ctx, cancel := ctxTimeout(120000)
	defer cancel()
	res, err := c.LaunchThread(ctx, in)
	if err != nil {
		return javaString(env, errJSON(err))
	}
	return javaString(env, okJSON(res))
}

//export Java_com_t3codes_core_Native_nativeStopThread
func Java_com_t3codes_core_Native_nativeStopThread(env *C.JNIEnv, clazz C.jclass, handle C.jlong, threadID C.jstring) C.jstring {
	c, err := getClient(int64(handle))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	ctx, cancel := ctxTimeout(30000)
	defer cancel()
	if err := c.StopThread(ctx, goString(env, threadID), "stopped from Android"); err != nil {
		return javaString(env, errJSON(err))
	}
	return javaString(env, `{"ok":true}`)
}

//export Java_com_t3codes_core_Native_nativeRespond
func Java_com_t3codes_core_Native_nativeRespond(env *C.JNIEnv, clazz C.jclass, handle C.jlong, threadID C.jstring, requestID C.jstring, decision C.jstring, feedback C.jstring, answersJSON C.jstring) C.jstring {
	c, err := getClient(int64(handle))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	var answers map[string]any
	if s := goString(env, answersJSON); s != "" && s != "null" {
		if err := json.Unmarshal([]byte(s), &answers); err != nil {
			return javaString(env, errJSON(err))
		}
	}
	ctx, cancel := ctxTimeout(60000)
	defer cancel()
	if err := c.RespondToRuntimeRequest(ctx, goString(env, threadID), goString(env, requestID), goString(env, decision), goString(env, feedback), answers); err != nil {
		return javaString(env, errJSON(err))
	}
	return javaString(env, `{"ok":true}`)
}

//export Java_com_t3codes_core_Native_nativeDismissUserInput
func Java_com_t3codes_core_Native_nativeDismissUserInput(env *C.JNIEnv, clazz C.jclass, handle C.jlong, threadID C.jstring, requestID C.jstring) C.jstring {
	c, err := getClient(int64(handle))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	ctx, cancel := ctxTimeout(60000)
	defer cancel()
	if err := c.DismissUserInput(ctx, goString(env, threadID), goString(env, requestID)); err != nil {
		return javaString(env, errJSON(err))
	}
	return javaString(env, `{"ok":true}`)
}

//export Java_com_t3codes_core_Native_nativeThreadAction
func Java_com_t3codes_core_Native_nativeThreadAction(env *C.JNIEnv, clazz C.jclass, handle C.jlong, threadID C.jstring, action C.jstring, value C.jstring) C.jstring {
	c, err := getClient(int64(handle))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	ctx, cancel := ctxTimeout(60000)
	defer cancel()
	tid := goString(env, threadID)
	val := goString(env, value)
	switch goString(env, action) {
	case "archive":
		err = c.ArchiveThread(ctx, tid)
	case "unarchive":
		err = c.UnarchiveThread(ctx, tid)
	case "delete":
		err = c.DeleteThread(ctx, tid)
	case "rename":
		err = c.RenameThread(ctx, tid, val)
	case "pin":
		err = c.PinThread(ctx, tid, val)
	case "unpin":
		err = c.UnpinThread(ctx, tid)
	case "markRead":
		err = c.MarkThreadRead(ctx, tid)
	case "markUnread":
		err = c.MarkThreadUnread(ctx, tid)
	case "settle":
		err = c.SettleThread(ctx, tid)
	case "unsettle":
		err = c.UnsettleThread(ctx, tid)
	case "unsnooze":
		err = c.UnsnoozeThread(ctx, tid)
	case "snooze":
		when, perr := time.Parse(time.RFC3339, val)
		if perr != nil {
			return javaString(env, errJSON(perr))
		}
		err = c.SnoozeThread(ctx, tid, when)
	case "setModel":
		var model t3.ModelSelection
		if jerr := json.Unmarshal([]byte(val), &model); jerr != nil {
			return javaString(env, errJSON(jerr))
		}
		err = c.SetModelSelection(ctx, tid, model)
	case "setRuntimeMode":
		err = c.SetRuntimeMode(ctx, tid, val)
	case "setInteractionMode":
		err = c.SetInteractionMode(ctx, tid, val)
	case "resumeQueue":
		err = c.ResumeQueue(ctx, tid)
	default:
		err = fmt.Errorf("unknown thread action %q", val)
	}
	if err != nil {
		return javaString(env, errJSON(err))
	}
	return javaString(env, `{"ok":true}`)
}

// --- terminal ---------------------------------------------------------------

//export Java_com_t3codes_core_Native_nativeTerminalOpen
func Java_com_t3codes_core_Native_nativeTerminalOpen(env *C.JNIEnv, clazz C.jclass, handle C.jlong, threadID C.jstring, terminalID C.jstring, cwd C.jstring, cols C.jint, rows C.jint) C.jstring {
	c, err := getClient(int64(handle))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	ctx, cancel := ctxTimeout(30000)
	defer cancel()
	sess, err := c.OpenTerminal(ctx, goString(env, threadID), goString(env, terminalID), goString(env, cwd), int(cols), int(rows), "")
	if err != nil {
		return javaString(env, errJSON(err))
	}
	return javaString(env, okJSON(sess))
}

//export Java_com_t3codes_core_Native_nativeTerminalAttach
func Java_com_t3codes_core_Native_nativeTerminalAttach(env *C.JNIEnv, clazz C.jclass, handle C.jlong, threadID C.jstring, terminalID C.jstring, cwd C.jstring, cols C.jint, rows C.jint) C.jstring {
	c, err := getClient(int64(handle))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	sub, err := c.AttachTerminal(context.Background(), goString(env, threadID), goString(env, terminalID), goString(env, cwd), int(cols), int(rows), "")
	if err != nil {
		return javaString(env, errJSON(err))
	}
	h := newHandle()
	registryMu.Lock()
	terminals[h] = sub
	registryMu.Unlock()
	return javaString(env, okJSON(map[string]any{"handle": h}))
}

//export Java_com_t3codes_core_Native_nativeTerminalNext
func Java_com_t3codes_core_Native_nativeTerminalNext(env *C.JNIEnv, clazz C.jclass, handle C.jlong, timeoutMs C.jint) C.jstring {
	registryMu.Lock()
	sub := terminals[int64(handle)]
	registryMu.Unlock()
	if sub == nil {
		return javaString(env, errJSON(fmt.Errorf("unknown terminal handle %d", int64(handle))))
	}
	if timeoutMs <= 0 {
		timeoutMs = 30000
	}
	timer := time.NewTimer(time.Duration(timeoutMs) * time.Millisecond)
	defer timer.Stop()
	select {
	case ev, ok := <-sub.Events:
		if !ok {
			return javaString(env, `{"type":"closed","done":true}`)
		}
		return javaString(env, okJSON(ev))
	case <-timer.C:
		return javaString(env, `{"timeout":true}`)
	}
}

//export Java_com_t3codes_core_Native_nativeTerminalWrite
func Java_com_t3codes_core_Native_nativeTerminalWrite(env *C.JNIEnv, clazz C.jclass, handle C.jlong, threadID C.jstring, terminalID C.jstring, data C.jstring) C.jstring {
	c, err := getClient(int64(handle))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	ctx, cancel := ctxTimeout(30000)
	defer cancel()
	if err := c.WriteTerminal(ctx, goString(env, threadID), goString(env, terminalID), goString(env, data)); err != nil {
		return javaString(env, errJSON(err))
	}
	return javaString(env, `{"ok":true}`)
}

//export Java_com_t3codes_core_Native_nativeTerminalResize
func Java_com_t3codes_core_Native_nativeTerminalResize(env *C.JNIEnv, clazz C.jclass, handle C.jlong, threadID C.jstring, terminalID C.jstring, cols C.jint, rows C.jint) C.jstring {
	c, err := getClient(int64(handle))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	ctx, cancel := ctxTimeout(30000)
	defer cancel()
	if err := c.ResizeTerminal(ctx, goString(env, threadID), goString(env, terminalID), int(cols), int(rows)); err != nil {
		return javaString(env, errJSON(err))
	}
	return javaString(env, `{"ok":true}`)
}

//export Java_com_t3codes_core_Native_nativeTerminalClose
func Java_com_t3codes_core_Native_nativeTerminalClose(env *C.JNIEnv, clazz C.jclass, handle C.jlong, threadID C.jstring, terminalID C.jstring) C.jstring {
	c, err := getClient(int64(handle))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	ctx, cancel := ctxTimeout(30000)
	defer cancel()
	if err := c.CloseTerminal(ctx, goString(env, threadID), goString(env, terminalID), false); err != nil {
		return javaString(env, errJSON(err))
	}
	return javaString(env, `{"ok":true}`)
}

//export Java_com_t3codes_core_Native_nativeTerminalCloseHandle
func Java_com_t3codes_core_Native_nativeTerminalCloseHandle(env *C.JNIEnv, clazz C.jclass, handle C.jlong) C.jstring {
	registryMu.Lock()
	sub := terminals[int64(handle)]
	delete(terminals, int64(handle))
	registryMu.Unlock()
	if sub != nil {
		sub.Close()
	}
	return javaString(env, `{"ok":true}`)
}

// --- attachments ------------------------------------------------------------

//export Java_com_t3codes_core_Native_nativeAttachmentUpload
func Java_com_t3codes_core_Native_nativeAttachmentUpload(env *C.JNIEnv, clazz C.jclass, handle C.jlong, typ C.jstring, name C.jstring, mime C.jstring, data C.jbyteArray) C.jstring {
	c, err := getClient(int64(handle))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	var length C.jsize
	ptr := C.t3_go_bytes(env, data, &length)
	if ptr == nil && length > 0 {
		return javaString(env, errJSON(fmt.Errorf("could not read attachment bytes")))
	}
	defer C.free(unsafe.Pointer(ptr))
	bytes := C.GoBytes(unsafe.Pointer(ptr), C.int(length))
	ctx, cancel := ctxTimeout(120000)
	defer cancel()
	att, err := c.UploadAttachment(ctx, goString(env, typ), goString(env, name), goString(env, mime), bytes)
	if err != nil {
		return javaString(env, errJSON(err))
	}
	return javaString(env, okJSON(att))
}

// --- reads ------------------------------------------------------------------

//export Java_com_t3codes_core_Native_nativeUsage
func Java_com_t3codes_core_Native_nativeUsage(env *C.JNIEnv, clazz C.jclass, handle C.jlong, since C.jstring, until C.jstring, timeZone C.jstring) C.jstring {
	c, err := getClient(int64(handle))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	ctx, cancel := ctxTimeout(60000)
	defer cancel()
	sum, err := c.GetUsageSummary(ctx, goString(env, since), goString(env, until), goString(env, timeZone), "", false)
	if err != nil {
		return javaString(env, errJSON(err))
	}
	return javaString(env, okJSON(sum))
}

//export Java_com_t3codes_core_Native_nativeVcsStatus
func Java_com_t3codes_core_Native_nativeVcsStatus(env *C.JNIEnv, clazz C.jclass, handle C.jlong, cwd C.jstring) C.jstring {
	c, err := getClient(int64(handle))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	ctx, cancel := ctxTimeout(60000)
	defer cancel()
	status, err := c.RefreshVcsStatus(ctx, goString(env, cwd))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	return javaString(env, okJSON(status))
}

//export Java_com_t3codes_core_Native_nativeDiffPreview
func Java_com_t3codes_core_Native_nativeDiffPreview(env *C.JNIEnv, clazz C.jclass, handle C.jlong, cwd C.jstring) C.jstring {
	c, err := getClient(int64(handle))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	ctx, cancel := ctxTimeout(120000)
	defer cancel()
	preview, err := c.GetDiffPreview(ctx, goString(env, cwd), "", false)
	if err != nil {
		return javaString(env, errJSON(err))
	}
	return javaString(env, okJSON(preview))
}

//export Java_com_t3codes_core_Native_nativeListEntries
func Java_com_t3codes_core_Native_nativeListEntries(env *C.JNIEnv, clazz C.jclass, handle C.jlong, cwd C.jstring, dir C.jstring) C.jstring {
	c, err := getClient(int64(handle))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	ctx, cancel := ctxTimeout(60000)
	defer cancel()
	res, err := c.ListEntries(ctx, goString(env, cwd), goString(env, dir))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	return javaString(env, okJSON(res))
}

//export Java_com_t3codes_core_Native_nativeReadFile
func Java_com_t3codes_core_Native_nativeReadFile(env *C.JNIEnv, clazz C.jclass, handle C.jlong, cwd C.jstring, path C.jstring) C.jstring {
	c, err := getClient(int64(handle))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	ctx, cancel := ctxTimeout(60000)
	defer cancel()
	res, err := c.ReadFile(ctx, goString(env, cwd), goString(env, path), false)
	if err != nil {
		return javaString(env, errJSON(err))
	}
	return javaString(env, okJSON(res))
}

//export Java_com_t3codes_core_Native_nativePair
func Java_com_t3codes_core_Native_nativePair(env *C.JNIEnv, clazz C.jclass, base C.jstring, code C.jstring, label C.jstring) C.jstring {
	ctx, cancel := ctxTimeout(60000)
	defer cancel()
	baseURL := goString(env, base)
	codeValue := goString(env, code)
	if strings.Contains(codeValue, "://") || strings.Contains(codeValue, "#token=") {
		if parsed, perr := t3.ParsePairingURL(codeValue); perr == nil && parsed.Code != "" {
			codeValue = parsed.Code
			if baseURL == "" {
				baseURL = parsed.BaseURL
			}
		}
	}
	tok, err := t3.Pair(ctx, t3.PairingInput{
		BaseURL: baseURL, Code: codeValue,
		ClientLabel: goString(env, label), DeviceType: "mobile", OS: "Android",
	})
	if err != nil {
		return javaString(env, errJSON(err))
	}
	return javaString(env, okJSON(tok))
}

//export Java_com_t3codes_core_Native_nativeProbe
func Java_com_t3codes_core_Native_nativeProbe(env *C.JNIEnv, clazz C.jclass, base C.jstring) C.jstring {
	ctx, cancel := ctxTimeout(8000)
	defer cancel()
	desc, err := t3.Probe(ctx, goString(env, base))
	if err != nil {
		return javaString(env, errJSON(err))
	}
	return javaString(env, okJSON(desc))
}

func main() {}
