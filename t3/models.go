package t3

import (
	"encoding/json"
	"time"
)

// ModelOption is one provider-specific model option (e.g. reasoning effort).
type ModelOption struct {
	ID    string `json:"id"`
	Value any    `json:"value"`
}

// ModelSelection identifies a provider instance and model with optional options.
type ModelSelection struct {
	InstanceID string        `json:"instanceId"`
	Model      string        `json:"model"`
	Options    []ModelOption `json:"options,omitempty"`
}

// Platform describes the host OS/arch serving the environment.
type Platform struct {
	OS   string `json:"os"`
	Arch string `json:"arch"`
}

// EnvironmentDescriptor is the identity and capability set of a server.
type EnvironmentDescriptor struct {
	EnvironmentID               string          `json:"environmentId"`
	Label                       string          `json:"label"`
	Platform                    Platform        `json:"platform"`
	ServerVersion               string          `json:"serverVersion"`
	OrchestrationProtocolVersion int            `json:"orchestrationProtocolVersion"`
	Capabilities                json.RawMessage `json:"capabilities,omitempty"`
}

// Capabilities is the typed subset of environment capability flags used for
// feature gating in clients. Unknown/absent flags stay false.
type Capabilities struct {
	RepositoryIdentity          bool `json:"repositoryIdentity,omitempty"`
	ConnectionProbe             bool `json:"connectionProbe,omitempty"`
	AttachmentUploads           bool `json:"attachmentUploads,omitempty"`
	QuestionAttachments         bool `json:"questionAttachments,omitempty"`
	PullRequests                bool `json:"pullRequests,omitempty"`
	PullRequestChecks           bool `json:"pullRequestChecks,omitempty"`
	PullRequestStackActions     bool `json:"pullRequestStackActions,omitempty"`
	InlineMessageContext        bool `json:"inlineMessageContext,omitempty"`
	RequiredWorktreeBootstrap   bool `json:"requiredWorktreeBootstrap,omitempty"`
	ThreadSettlement            bool `json:"threadSettlement,omitempty"`
	ThreadAutoSettlement        bool `json:"threadAutoSettlement,omitempty"`
	ThreadSnooze                bool `json:"threadSnooze,omitempty"`
	ThreadPinning               bool `json:"threadPinning,omitempty"`
	ThreadPinReorder            bool `json:"threadPinReorder,omitempty"`
	ThreadActiveReorder         bool `json:"threadActiveReorder,omitempty"`
	ThreadAutoSettleOptOut      bool `json:"threadAutoSettleOptOut,omitempty"`
	ThreadTitleRegeneration     bool `json:"threadTitleRegeneration,omitempty"`
	ThreadVisitedTracking       bool `json:"threadVisitedTracking,omitempty"`
	ThreadPullRequestLinking    bool `json:"threadPullRequestLinking,omitempty"`
	ThreadPullRequests          bool `json:"threadPullRequests,omitempty"`
	ThreadPullRequestWatch      bool `json:"threadPullRequestWatch,omitempty"`
	ThreadRestartContinuation   bool `json:"threadRestartContinuation,omitempty"`
	StorageCleanup              bool `json:"storageCleanup,omitempty"`
	StorageCleanupRun           bool `json:"storageCleanupRun,omitempty"`
	ProjectWorktreeCleanup      bool `json:"projectWorktreeCleanup,omitempty"`
	SettledWorktreeCleanup      bool `json:"settledWorktreeCleanup,omitempty"`
	WorktreesDirectory          bool `json:"worktreesDirectory,omitempty"`
	EnvironmentThemes           bool `json:"environmentThemes,omitempty"`
	EnvironmentIcon             bool `json:"environmentIcon,omitempty"`
	UsageLimitSources           bool `json:"usageLimitSources,omitempty"`
	UsagePriceOverrides         bool `json:"usagePriceOverrides,omitempty"`
	UsageModelAliases           bool `json:"usageModelAliases,omitempty"`
	IgnoredFileListing          bool `json:"ignoredFileListing,omitempty"`
	CommandOutputStreaming      bool `json:"commandOutputStreaming,omitempty"`
	ProjectCloneTracking        bool `json:"projectCloneTracking,omitempty"`
	ProjectSettingsOverrides    bool `json:"projectSettingsOverrides,omitempty"`
	ProjectProviderOverrides    bool `json:"projectProviderInstanceOverrides,omitempty"`
	ServerBrowser               bool `json:"serverBrowser,omitempty"`
	ServerSelfUpdateProgress    bool `json:"serverSelfUpdateProgress,omitempty"`
	DesktopAppUpdate            bool `json:"desktopAppUpdate,omitempty"`
	AgentActivityPublishing     bool `json:"agentActivityPublishing,omitempty"`
}

// TypedCapabilities decodes the capability flags in a descriptor.
func (d *EnvironmentDescriptor) TypedCapabilities() *Capabilities {
	var caps Capabilities
	if len(d.Capabilities) > 0 {
		_ = json.Unmarshal(d.Capabilities, &caps)
	}
	return &caps
}

// ProjectIcon is a stored project icon reference.
type ProjectIcon struct {
	Kind string `json:"kind"`
	Path string `json:"path,omitempty"`
	URL  string `json:"url,omitempty"`
}

// ProjectScript is a user-defined project script surfaced in the composer.
type ProjectScript struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Command     string `json:"command,omitempty"`
	Description string `json:"description,omitempty"`
	Icon        string `json:"icon,omitempty"`
}

// Project is a workspace the agent can operate on.
type Project struct {
	ID                    string          `json:"id"`
	Title                 string          `json:"title"`
	WorkspaceRoot         string          `json:"workspaceRoot"`
	RepositoryIdentity    json.RawMessage `json:"repositoryIdentity,omitempty"`
	DefaultModelSelection *ModelSelection `json:"defaultModelSelection,omitempty"`
	DefaultThreadEnvMode  *string         `json:"defaultThreadEnvMode,omitempty"`
	AutoPull              *bool           `json:"autoPull,omitempty"`
	FaviconPath           *string         `json:"faviconPath,omitempty"`
	ProjectIcon           *ProjectIcon    `json:"projectIcon,omitempty"`
	Scripts               []ProjectScript `json:"scripts,omitempty"`
	CreatedAt             string          `json:"createdAt,omitempty"`
	UpdatedAt             string          `json:"updatedAt,omitempty"`
}

// ThreadLineage describes fork relationships between threads.
type ThreadLineage struct {
	ParentThreadID       *string `json:"parentThreadId,omitempty"`
	RelationshipToParent *string `json:"relationshipToParent,omitempty"`
	RootThreadID         string  `json:"rootThreadId,omitempty"`
}

// PendingRuntimeRequestSummary is the compact form attached to a thread shell.
type PendingRuntimeRequestSummary struct {
	ID        string `json:"id"`
	Kind      string `json:"kind"`
	CreatedAt string `json:"createdAt,omitempty"`
	Blocking  *bool  `json:"blocking,omitempty"`
}

// LatestVisibleMessageSummary is the newest message preview for a thread.
type LatestVisibleMessageSummary struct {
	ID        string `json:"id"`
	Role      string `json:"role"`
	Text      string `json:"text"`
	UpdatedAt string `json:"updatedAt,omitempty"`
}

// PendingBackgroundTask describes work that outlives a turn.
type PendingBackgroundTask struct {
	ID     string `json:"id,omitempty"`
	Kind   string `json:"kind,omitempty"`
	Title  string `json:"title,omitempty"`
	Status string `json:"status,omitempty"`
}

// ThreadShell is the list-level view of a thread.
type ThreadShell struct {
	CreatedBy               string                        `json:"createdBy"`
	CreationSource          string                        `json:"creationSource"`
	ID                      string                        `json:"id"`
	ProjectID               string                        `json:"projectId"`
	Title                   string                        `json:"title"`
	ProviderInstanceID      string                        `json:"providerInstanceId"`
	ModelSelection          ModelSelection                `json:"modelSelection"`
	RuntimeMode             string                        `json:"runtimeMode"`
	InteractionMode         string                        `json:"interactionMode"`
	Branch                  *string                       `json:"branch"`
	WorktreePath            *string                       `json:"worktreePath"`
	PullRequests            []json.RawMessage             `json:"pullRequests,omitempty"`
	LinkedPullRequest       json.RawMessage               `json:"linkedPullRequest,omitempty"`
	Lineage                 ThreadLineage                 `json:"lineage"`
	ActiveProviderThreadID  *string                       `json:"activeProviderThreadId"`
	LatestRunID             *string                       `json:"latestRunId,omitempty"`
	LatestRunRequestedAt    *string                       `json:"latestRunRequestedAt,omitempty"`
	LatestRunStartedAt      *string                       `json:"latestRunStartedAt,omitempty"`
	LatestRunCompletedAt    *string                       `json:"latestRunCompletedAt,omitempty"`
	ActiveRunID             *string                       `json:"activeRunId,omitempty"`
	ActivityRunStartedAt    *string                       `json:"activityRunStartedAt,omitempty"`
	ActivityRunStatus       *string                       `json:"activityRunStatus,omitempty"`
	BlockedOnThreadWait     *bool                         `json:"blockedOnThreadWait,omitempty"`
	Status                  string                        `json:"status"`
	LastError               *string                       `json:"lastError,omitempty"`
	LastErrorClass          *string                       `json:"lastErrorClass,omitempty"`
	LastErrorCode           *string                       `json:"lastErrorCode,omitempty"`
	UsageLimitResetAt       *string                       `json:"usageLimitResetAt,omitempty"`
	PendingRuntimeRequest   *PendingRuntimeRequestSummary `json:"pendingRuntimeRequest"`
	LatestVisibleMessage    *LatestVisibleMessageSummary  `json:"latestVisibleMessage"`
	LatestUserMessageAt     *string                       `json:"latestUserMessageAt,omitempty"`
	LatestUserAuthoredMsgAt *string                       `json:"latestUserAuthoredMessageAt,omitempty"`
	HasActionablePlan       bool                          `json:"hasActionableProposedPlan,omitempty"`
	PendingBackgroundTasks  []PendingBackgroundTask       `json:"pendingBackgroundTasks,omitempty"`
	ProviderInstanceHistory []string                      `json:"providerInstanceHistory,omitempty"`
	ItemCount               int                           `json:"itemCount"`
	VisibleItemCount        int                           `json:"visibleItemCount"`
	CreatedAt               string                        `json:"createdAt"`
	UpdatedAt               string                        `json:"updatedAt"`
	ArchivedAt              *string                       `json:"archivedAt"`
	SettledOverride         *string                       `json:"settledOverride,omitempty"`
	SettledAt               *string                       `json:"settledAt,omitempty"`
	UnsettledAt             *string                       `json:"unsettledAt,omitempty"`
	SnoozedUntil            *string                       `json:"snoozedUntil,omitempty"`
	SnoozedAt               *string                       `json:"snoozedAt,omitempty"`
	PinnedAt                *string                       `json:"pinnedAt,omitempty"`
	AutoSettleDisabledAt    *string                       `json:"autoSettleDisabledAt,omitempty"`
	PinOrderKey             *string                       `json:"pinOrderKey,omitempty"`
	ActiveOrderKey          *string                       `json:"activeOrderKey,omitempty"`
	LastVisitedAt           *string                       `json:"lastVisitedAt,omitempty"`
	DeletedAt               *string                       `json:"deletedAt"`
}

// IsRunning reports whether the thread currently has an active run.
func (t ThreadShell) IsRunning() bool {
	return t.ActiveRunID != nil && *t.ActiveRunID != ""
}

// AwaitingInput reports whether the thread is blocked on a runtime request.
func (t ThreadShell) AwaitingInput() bool { return t.PendingRuntimeRequest != nil }

// ShellSnapshot is the full list state: projects, active and archived threads.
type ShellSnapshot struct {
	SchemaVersion    int           `json:"schemaVersion"`
	SnapshotSequence int64         `json:"snapshotSequence"`
	Threads          []ThreadShell `json:"threads"`
	ArchivedThreads  []ThreadShell `json:"archivedThreads"`
	Projects         []Project     `json:"projects"`
}

// TurnItem is one entry in a thread timeline. Only the fields shared by every
// item type are typed; Type-specific data stays available as Raw.
type TurnItem struct {
	ID                 string          `json:"id"`
	ThreadID           string          `json:"threadId"`
	RunID              string          `json:"runId,omitempty"`
	NodeID             string          `json:"nodeId,omitempty"`
	ProviderThreadID   string          `json:"providerThreadId,omitempty"`
	ProviderTurnID     string          `json:"providerTurnId,omitempty"`
	ParentItemID       *string         `json:"parentItemId,omitempty"`
	Ordinal            int             `json:"ordinal"`
	Status             string          `json:"status,omitempty"`
	Title              string          `json:"title,omitempty"`
	StartedAt          string          `json:"startedAt,omitempty"`
	CompletedAt        string          `json:"completedAt,omitempty"`
	UpdatedAt          string          `json:"updatedAt,omitempty"`
	Type               string          `json:"type"`
	Text               string          `json:"text,omitempty"`
	Streaming          *bool           `json:"streaming,omitempty"`
	ExitCode           *int            `json:"exitCode,omitempty"`
	ToolName           string          `json:"toolName,omitempty"`
	Input              json.RawMessage `json:"input,omitempty"`
	Output             json.RawMessage `json:"output,omitempty"`
	OutputOmitted      *bool           `json:"outputOmitted,omitempty"`
	Raw                json.RawMessage `json:"-"`
}

// UnmarshalJSON keeps both the typed view and the raw object.
func (t *TurnItem) UnmarshalJSON(b []byte) error {
	type plain TurnItem
	var p plain
	if err := json.Unmarshal(b, &p); err != nil {
		return err
	}
	*t = TurnItem(p)
	t.Raw = append(t.Raw[:0], b...)
	return nil
}

// VisibleTurnItem is a timeline row with visibility metadata.
type VisibleTurnItem struct {
	Position       int       `json:"position"`
	Visibility     string    `json:"visibility,omitempty"`
	SourceThreadID string    `json:"sourceThreadId,omitempty"`
	SourceItemID   string    `json:"sourceItemId,omitempty"`
	Item           TurnItem  `json:"item"`
}

// Message is a conversation message (user, assistant, system).
type Message struct {
	CreatedBy      string `json:"createdBy,omitempty"`
	CreationSource string `json:"creationSource,omitempty"`
	ID             string `json:"id"`
	ThreadID       string `json:"threadId"`
	RunID          string `json:"runId,omitempty"`
	NodeID         string `json:"nodeId,omitempty"`
	Role           string `json:"role"`
	Text           string `json:"text"`
	CreatedAt      string `json:"createdAt,omitempty"`
	UpdatedAt      string `json:"updatedAt,omitempty"`
}

// RuntimeRequest is an approval, user-input or auth request from an agent.
type RuntimeRequest struct {
	ID                string          `json:"id"`
	NodeID            string          `json:"nodeId"`
	ProviderTurnID    *string         `json:"providerTurnId,omitempty"`
	Kind              string          `json:"kind"`
	Status            string          `json:"status"`
	ResponseCapability json.RawMessage `json:"responseCapability,omitempty"`
	CreatedAt         string          `json:"createdAt,omitempty"`
	ResolvedAt        *string         `json:"resolvedAt,omitempty"`
	Decision          *string         `json:"decision,omitempty"`
	Feedback          *string         `json:"feedback,omitempty"`
	Answers           json.RawMessage `json:"answers,omitempty"`
}

// ThreadProjection is the full state of one thread.
type ThreadProjection struct {
	Thread            ThreadShell      `json:"thread"`
	Runs              []json.RawMessage `json:"runs"`
	Attempts          []json.RawMessage `json:"attempts"`
	Nodes             []json.RawMessage `json:"nodes"`
	Subagents         []json.RawMessage `json:"subagents"`
	ProviderSessions  []json.RawMessage `json:"providerSessions"`
	ProviderThreads   []json.RawMessage `json:"providerThreads"`
	ProviderTurns     []json.RawMessage `json:"providerTurns"`
	RuntimeRequests   []RuntimeRequest  `json:"runtimeRequests"`
	Messages          []Message         `json:"messages"`
	Plans             []json.RawMessage `json:"plans"`
	TurnItems         []TurnItem        `json:"turnItems"`
	CheckpointScopes  []json.RawMessage `json:"checkpointScopes"`
	Checkpoints       []json.RawMessage `json:"checkpoints"`
	ContextHandoffs   []json.RawMessage `json:"contextHandoffs"`
	ContextTransfers  []json.RawMessage `json:"contextTransfers"`
	VisibleTurnItems  []VisibleTurnItem `json:"visibleTurnItems"`
	UpdatedAt         string            `json:"updatedAt"`
}

// PendingRequests returns runtime requests that still need a client answer.
func (p *ThreadProjection) PendingRequests() []RuntimeRequest {
	out := make([]RuntimeRequest, 0, 1)
	for _, r := range p.RuntimeRequests {
		if r.Status == "pending" {
			out = append(out, r)
		}
	}
	return out
}

// ThreadDetailSnapshot is returned by the HTTP thread endpoint.
type ThreadDetailSnapshot struct {
	SnapshotSequence      int64            `json:"snapshotSequence"`
	Projection            ThreadProjection `json:"projection"`
	HistoryCursor         *string          `json:"historyCursor,omitempty"`
	HasMoreHistory        *bool            `json:"hasMoreHistory,omitempty"`
	LatestLocalTurnOrdinal *int            `json:"latestLocalTurnOrdinal,omitempty"`
}

// Provider describes an agent provider instance and its models.
type Provider struct {
	InstanceID   string          `json:"instanceId"`
	Driver       string          `json:"driver"`
	DisplayName  string          `json:"displayName"`
	Enabled      bool            `json:"enabled"`
	Installed    bool            `json:"installed"`
	Version      *string         `json:"version"`
	Status       string          `json:"status"`
	Auth         json.RawMessage `json:"auth,omitempty"`
	Message      *string         `json:"message,omitempty"`
	Models       []Model         `json:"models"`
	SlashCommands []json.RawMessage `json:"slashCommands,omitempty"`
	Skills        []json.RawMessage `json:"skills,omitempty"`
}

// Model is one selectable model for a provider.
type Model struct {
	ID            string          `json:"id"`
	Slug          string          `json:"slug,omitempty"`
	Name          string          `json:"name,omitempty"`
	DisplayName   string          `json:"displayName,omitempty"`
	Description   string          `json:"description,omitempty"`
	ContextWindow *int            `json:"contextWindow,omitempty"`
	Options       json.RawMessage `json:"options,omitempty"`
	DefaultOption *ModelOption    `json:"defaultOption,omitempty"`
	Capabilities  json.RawMessage `json:"capabilities,omitempty"`
	IsCustom      bool            `json:"isCustom,omitempty"`
}

// Key returns the canonical model identifier used in a selection.
func (m Model) Key() string {
	if m.Slug != "" {
		return m.Slug
	}
	if m.ID != "" {
		return m.ID
	}
	return m.Name
}

// ServerConfig is the large configuration payload returned by server.getConfig.
type ServerConfig struct {
	Environment             EnvironmentDescriptor `json:"environment"`
	Auth                    json.RawMessage       `json:"auth,omitempty"`
	Cwd                     string                `json:"cwd"`
	Providers               []Provider            `json:"providers"`
	Settings                json.RawMessage       `json:"settings,omitempty"`
	Keybindings             []json.RawMessage     `json:"keybindings,omitempty"`
	Issues                  []json.RawMessage     `json:"issues,omitempty"`
	AvailableEditors        []json.RawMessage     `json:"availableEditors,omitempty"`
	RemoteOpenTargets       []json.RawMessage     `json:"remoteOpenTargets,omitempty"`
	ScratchWorkspaceRoot    string                `json:"scratchWorkspaceRoot,omitempty"`
	NewProjectsRoot         string                `json:"newProjectsRoot,omitempty"`
	ThreadSnapshotPagination *bool                `json:"threadSnapshotPagination,omitempty"`
	ThreadFind              *bool                 `json:"threadFind,omitempty"`
	ThreadFindProgressive   *bool                 `json:"threadFindProgressive,omitempty"`
}

// ModelFor finds a model across providers.
func (c *ServerConfig) ModelFor(instanceID, modelID string) *Model {
	for _, p := range c.Providers {
		if p.InstanceID != instanceID {
			continue
		}
		for i := range p.Models {
			if p.Models[i].ID == modelID {
				return &p.Models[i]
			}
		}
	}
	return nil
}

// ProviderFor finds a provider instance by id.
func (c *ServerConfig) ProviderFor(instanceID string) *Provider {
	for i := range c.Providers {
		if c.Providers[i].InstanceID == instanceID {
			return &c.Providers[i]
		}
	}
	return nil
}

// ParseTime parses a server ISO timestamp, returning the zero time on failure.
func ParseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}
	}
	return t
}
