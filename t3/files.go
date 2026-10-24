package t3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// ProjectEntry is a file or directory below a workspace root.
type ProjectEntry struct {
	Path    string `json:"path"`
	Kind    string `json:"kind"`
	Ignored bool   `json:"ignored,omitempty"`
}

// ListEntriesResult is returned by ListEntries.
type ListEntriesResult struct {
	Entries   []ProjectEntry `json:"entries"`
	Truncated bool           `json:"truncated"`
}

// ListEntries lists files and directories under cwd.
func (c *Client) ListEntries(ctx context.Context, cwd, dir string) (*ListEntriesResult, error) {
	payload := map[string]any{"cwd": cwd}
	if dir != "" {
		payload["directoryPath"] = dir
	}
	var out ListEntriesResult
	if err := c.call(ctx, "projects.listEntries", payload, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ReadFileResult is a text file read.
type ReadFileResult struct {
	RelativePath string `json:"relativePath"`
	Contents     string `json:"contents"`
	ByteLength   int    `json:"byteLength"`
	Truncated    bool   `json:"truncated"`
	Exists       *bool  `json:"exists,omitempty"`
}

// ReadFile reads a workspace file.
func (c *Client) ReadFile(ctx context.Context, cwd, relativePath string, allowMissing bool) (*ReadFileResult, error) {
	payload := map[string]any{"cwd": cwd, "relativePath": relativePath}
	if allowMissing {
		payload["allowMissing"] = true
	}
	var out ReadFileResult
	if err := c.call(ctx, "projects.readFile", payload, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// WriteFile writes a workspace file.
func (c *Client) WriteFile(ctx context.Context, cwd, relativePath, contents string) error {
	return c.call(ctx, "projects.writeFile", map[string]any{
		"cwd": cwd, "relativePath": relativePath, "contents": contents,
	}, nil)
}

// SearchEntries searches file names in a workspace.
func (c *Client) SearchEntries(ctx context.Context, cwd, query string, limit int) (*ListEntriesResult, error) {
	if limit <= 0 {
		limit = 50
	}
	var out ListEntriesResult
	if err := c.call(ctx, "projects.searchEntries", map[string]any{
		"cwd": cwd, "query": query, "limit": limit,
	}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ContentMatch is one matched line.
type ContentMatch struct {
	Path        string `json:"path"`
	LineNumber  int    `json:"lineNumber"`
	LineContent string `json:"lineContent"`
	MatchRanges []struct {
		Start int `json:"start"`
		End   int `json:"end"`
	} `json:"matchRanges"`
}

// SearchContentsResult is returned by SearchContents.
type SearchContentsResult struct {
	Matches            []ContentMatch `json:"matches"`
	Truncated          bool           `json:"truncated"`
	RegexFallbackError string         `json:"regexFallbackError,omitempty"`
}

// SearchContents greps a workspace.
func (c *Client) SearchContents(ctx context.Context, cwd, query string, limit int, caseSensitive, wholeWord, useRegex bool) (*SearchContentsResult, error) {
	if limit <= 0 {
		limit = 50
	}
	var out SearchContentsResult
	err := c.call(ctx, "projects.searchContents", map[string]any{
		"cwd": cwd, "query": query, "limit": limit,
		"caseSensitive": caseSensitive, "wholeWord": wholeWord, "useRegex": useRegex,
	}, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// ListIgnoredFiles lists ignored files (e.g. build output) in a workspace.
func (c *Client) ListIgnoredFiles(ctx context.Context, cwd string) ([]map[string]any, error) {
	var out struct {
		Files     []map[string]any `json:"files"`
		Truncated bool             `json:"truncated"`
	}
	if err := c.call(ctx, "projects.listIgnoredFiles", map[string]any{"cwd": cwd}, &out); err != nil {
		return nil, err
	}
	return out.Files, nil
}

// ProjectSnapshot is the HTTP project snapshot.
type ProjectSnapshot struct {
	Projects []Project `json:"projects"`
}

// ProjectSnapshot fetches all projects over HTTP.
func (c *Client) ProjectSnapshot(ctx context.Context) (*ProjectSnapshot, error) {
	var out ProjectSnapshot
	if err := c.doJSON(ctx, http.MethodGet, "/api/projects", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// CreateProject adds an existing directory as a project.
func (c *Client) CreateProject(ctx context.Context, title, workspaceRoot string, createIfMissing bool, model *ModelSelection) (*Project, error) {
	payload := map[string]any{
		"type":        "project.create",
		"commandId":   NewID(),
		"projectId":   NewID(),
		"title":       title,
		"workspaceRoot": workspaceRoot,
	}
	if createIfMissing {
		payload["createWorkspaceRootIfMissing"] = true
	}
	if model != nil {
		payload["defaultModelSelection"] = model
	}
	var out Project
	if err := c.call(ctx, "projects.mutate", payload, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// UpdateProject patches project settings.
func (c *Client) UpdateProject(ctx context.Context, projectID string, fields map[string]any) (*Project, error) {
	payload := map[string]any{"type": "project.update", "commandId": NewID(), "projectId": projectID}
	for k, v := range fields {
		payload[k] = v
	}
	var out Project
	if err := c.call(ctx, "projects.mutate", payload, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// DeleteProject removes a project.
func (c *Client) DeleteProject(ctx context.Context, projectID string, force bool) error {
	return c.call(ctx, "projects.mutate", map[string]any{
		"type": "project.delete", "commandId": NewID(), "projectId": projectID, "force": force,
	}, nil)
}

// EnsureScratchProject creates or returns the server's scratch project.
func (c *Client) EnsureScratchProject(ctx context.Context) (string, error) {
	var out struct {
		ProjectID string `json:"projectId"`
	}
	if err := c.call(ctx, "projects.ensureScratch", map[string]any{}, &out); err != nil {
		return "", err
	}
	return out.ProjectID, nil
}

// CreateNewProject creates a directory and registers it as a project.
func (c *Client) CreateNewProject(ctx context.Context, name string) (*Project, error) {
	var out struct {
		Project Project `json:"project"`
	}
	if err := c.call(ctx, "projects.createNew", map[string]any{"name": name}, &out); err != nil {
		return &out.Project, err
	}
	return &out.Project, nil
}

// Attachment is a chat attachment accepted by message.dispatch.
type Attachment struct {
	Type      string `json:"type"`
	ID        string `json:"id"`
	Name      string `json:"name"`
	MimeType  string `json:"mimeType"`
	SizeBytes int    `json:"sizeBytes"`
	Source    any    `json:"source,omitempty"`
}

// UploadAttachment uploads bytes and returns the attachment descriptor to send
// with a message. The server issues a short-lived signed upload URL first.
func (c *Client) UploadAttachment(ctx context.Context, typ, name, mimeType string, data []byte) (*Attachment, error) {
	input := map[string]any{
		"name": name, "mimeType": mimeType, "sizeBytes": len(data),
	}
	if typ == "image" {
		input["type"] = "image"
	} else {
		input["type"] = "file"
	}
	var created struct {
		AttachmentID string `json:"attachmentId"`
		RelativeURL  string `json:"relativeUrl"`
		ExpiresAt    int64  `json:"expiresAt"`
	}
	if err := c.call(ctx, "attachments.createUploadUrl", input, &created); err != nil {
		return nil, err
	}
	uploadURL := created.RelativeURL
	if strings.HasPrefix(uploadURL, "/") {
		uploadURL = c.baseURL + uploadURL
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, uploadURL, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", mimeType)
	req.Header.Set("Content-Length", fmt.Sprintf("%d", len(data)))
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return nil, &HTTPError{Status: resp.StatusCode, Body: string(body)}
	}
	return &Attachment{
		Type: typ, ID: created.AttachmentID, Name: name, MimeType: mimeType, SizeBytes: len(data),
	}, nil
}

// DeleteAttachment removes an uploaded attachment.
func (c *Client) DeleteAttachment(ctx context.Context, attachmentID string) error {
	return c.call(ctx, "attachments.delete", map[string]any{"attachmentId": attachmentID}, nil)
}

// AssetURL builds an absolute URL for a server-relative asset path.
func (c *Client) AssetURL(relative string) string {
	if strings.HasPrefix(relative, "http://") || strings.HasPrefix(relative, "https://") {
		return relative
	}
	if !strings.HasPrefix(relative, "/") {
		relative = "/" + relative
	}
	return c.baseURL + relative
}

// AssetURLFor resolves an asset id and path to a signed URL.
func (c *Client) AssetURLFor(ctx context.Context, assetID, path string) (string, error) {
	var out struct {
		URL string `json:"url"`
	}
	if err := c.call(ctx, "assets.createUrl", map[string]any{"assetId": assetID, "path": path}, &out); err != nil {
		return "", err
	}
	if out.URL == "" {
		return "", fmt.Errorf("server returned no asset url")
	}
	return out.URL, nil
}

// FileMetadata describes a filesystem entry. Missing is true when the server
// could not read the path.
type FileMetadata struct {
	Path       string `json:"path"`
	Kind       string `json:"kind"`
	ByteLength int64  `json:"byteLength,omitempty"`
	MimeType   string `json:"mimeType,omitempty"`
	Missing    bool   `json:"missing,omitempty"`
}

// FileMetadataBatch fetches metadata for several host paths in one call.
func (c *Client) FileMetadataBatch(ctx context.Context, paths []string) ([]*FileMetadata, error) {
	var out struct {
		Entries []*struct {
			Kind       string  `json:"kind"`
			ByteLength *int64  `json:"byteLength"`
			MimeType   *string `json:"mimeType"`
		} `json:"entries"`
	}
	if err := c.call(ctx, "filesystem.getMetadata", map[string]any{"paths": paths}, &out); err != nil {
		return nil, err
	}
	result := make([]*FileMetadata, len(paths))
	for i, path := range paths {
		meta := &FileMetadata{Path: path}
		if i < len(out.Entries) && out.Entries[i] != nil {
			meta.Kind = out.Entries[i].Kind
			if out.Entries[i].ByteLength != nil {
				meta.ByteLength = *out.Entries[i].ByteLength
			}
			if out.Entries[i].MimeType != nil {
				meta.MimeType = *out.Entries[i].MimeType
			}
		} else {
			meta.Missing = true
		}
		result[i] = meta
	}
	return result, nil
}

// FileMetadata fetches metadata for one host path.
func (c *Client) FileMetadata(ctx context.Context, path string) (*FileMetadata, error) {
	all, err := c.FileMetadataBatch(ctx, []string{path})
	if err != nil {
		return nil, err
	}
	if len(all) == 0 {
		return nil, fmt.Errorf("no metadata returned")
	}
	return all[0], nil
}

// BrowseFiles completes a partial host path into a directory listing. cwd is
// the workspace anchor for relative paths.
func (c *Client) BrowseFiles(ctx context.Context, partialPath, cwd string) (json.RawMessage, error) {
	payload := map[string]any{"partialPath": partialPath}
	if cwd != "" {
		payload["cwd"] = cwd
	}
	return c.callRaw(ctx, "filesystem.browse", payload)
}

// AttachmentFileURL builds the download URL for a chat attachment on a
// workspace.
func (c *Client) AttachmentFileURL(projectID, attachmentID, name string) string {
	q := url.Values{}
	q.Set("projectId", projectID)
	q.Set("name", name)
	return c.baseURL + "/api/attachments/" + url.PathEscape(attachmentID) + "?" + q.Encode()
}
