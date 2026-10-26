package t3

import (
	"encoding/json"
	"testing"
)

func TestMergeShellProjectSnapshotKeepsThreads(t *testing.T) {
	previous := ShellSnapshot{
		SnapshotSequence: 41,
		Threads: []ThreadShell{
			{ID: "active-1", Title: "Active chat"},
			{ID: "active-2", Title: "Another chat"},
		},
		ArchivedThreads: []ThreadShell{{ID: "archived-1", Title: "Old chat"}},
		Projects: []Project{
			{
				ID:                 "project-1",
				WorkspaceRoot:      "/work/one",
				RepositoryIdentity: json.RawMessage(`{"owner":"acme","repo":"one"}`),
			},
		},
	}
	enrichment := ShellSnapshot{
		SnapshotSequence: 42,
		Threads:          []ThreadShell{},
		ArchivedThreads:  []ThreadShell{},
		Projects: []Project{
			{
				ID:                 "project-1",
				WorkspaceRoot:      "/work/one",
				RepositoryIdentity: json.RawMessage(`{"owner":"acme","repo":"one"}`),
			},
			{ID: "project-2", WorkspaceRoot: "/work/two"},
		},
	}

	merged := mergeShellProjectSnapshot(previous, enrichment)
	if len(merged.Threads) != 2 {
		t.Fatalf("expected active threads to survive, got %d", len(merged.Threads))
	}
	if len(merged.ArchivedThreads) != 1 {
		t.Fatalf("expected archived threads to survive, got %d", len(merged.ArchivedThreads))
	}
	if len(merged.Projects) != 2 {
		t.Fatalf("expected project metadata to merge, got %d", len(merged.Projects))
	}
	if merged.SnapshotSequence != 41 {
		t.Fatalf("project enrichment must not advance the thread cursor: %d", merged.SnapshotSequence)
	}
}

func TestMergeShellProjectSnapshotKeepsResolvedIdentity(t *testing.T) {
	previous := ShellSnapshot{
		Projects: []Project{{
			ID:                 "project-1",
			WorkspaceRoot:      "/work/one",
			RepositoryIdentity: json.RawMessage(`{"owner":"acme","repo":"one"}`),
		}},
	}
	enrichment := ShellSnapshot{
		Projects: []Project{{
			ID:            "project-1",
			WorkspaceRoot: "/work/one",
		}},
	}

	merged := mergeShellProjectSnapshot(previous, enrichment)
	if isNullJSON(merged.Projects[0].RepositoryIdentity) {
		t.Fatalf("expected a null metadata refresh to retain the resolved identity")
	}
}
