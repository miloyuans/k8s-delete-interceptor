package main

import (
	"encoding/json"
	"time"
)

const (
	DecisionAuditOnly       = "audit_only"
	DecisionAllowSilent     = "allow_silent"
	DecisionAllowNotify     = "allow_notify"
	DecisionRequireApproval = "require_approval"
	DecisionBlock           = "block"

	RollbackNone                = "none"
	RollbackRestoreOldObject    = "restore_old_object"
	RollbackDeleteCreatedObject = "delete_created_object"
)

type RuntimeConfig struct {
	Version               int64                  `json:"version" yaml:"version" bson:"version"`
	ClusterName           string                 `json:"cluster_name" yaml:"cluster_name" bson:"cluster_name"`
	Enabled               bool                   `json:"enabled" yaml:"enabled" bson:"enabled"`
	Defaults              DefaultPolicy          `json:"defaults" yaml:"defaults" bson:"defaults"`
	Storage               SharedStorageConfig    `json:"storage" yaml:"storage" bson:"storage"`
	DataSources           []DataSource           `json:"data_sources" yaml:"data_sources" bson:"data_sources"`
	ResourceScopes        []ResourceScope        `json:"resource_scopes" yaml:"resource_scopes" bson:"resource_scopes"`
	ActorGroups           []ActorGroup           `json:"actor_groups" yaml:"actor_groups" bson:"actor_groups"`
	Rules                 []PolicyRule           `json:"rules" yaml:"rules" bson:"rules"`
	Telegram              TelegramConfig         `json:"telegram" yaml:"telegram" bson:"telegram"`
	NotificationTemplates []NotificationTemplate `json:"notification_templates" yaml:"notification_templates" bson:"notification_templates"`
	Rollback              RollbackConfig         `json:"rollback" yaml:"rollback" bson:"rollback"`
	Audit                 AuditConfig            `json:"audit" yaml:"audit" bson:"audit"`
	SystemProtection      SystemProtectionConfig `json:"system_protection" yaml:"system_protection" bson:"system_protection"`
	Web                   WebSettings            `json:"web" yaml:"web" bson:"web"`
	WebRoles              []WebRole              `json:"web_roles" yaml:"web_roles" bson:"web_roles"`
	WebUsers              []WebUser              `json:"web_users" yaml:"web_users" bson:"web_users"`
}

type DefaultPolicy struct {
	Unmatched DecisionByOperation `json:"unmatched" yaml:"unmatched" bson:"unmatched"`
}

type DecisionByOperation struct {
	Create string `json:"create" yaml:"create" bson:"create"`
	Update string `json:"update" yaml:"update" bson:"update"`
	Delete string `json:"delete" yaml:"delete" bson:"delete"`
}

type SharedStorageConfig struct {
	RootDir                   string `json:"root_dir" yaml:"root_dir" bson:"root_dir"`
	CleanupSyncedAfter        string `json:"cleanup_synced_after" yaml:"cleanup_synced_after" bson:"cleanup_synced_after"`
	WarnUsagePercent          int    `json:"warn_usage_percent" yaml:"warn_usage_percent" bson:"warn_usage_percent"`
	CriticalUsagePercent      int    `json:"critical_usage_percent" yaml:"critical_usage_percent" bson:"critical_usage_percent"`
	CompressSyncedEvents      bool   `json:"compress_synced_events" yaml:"compress_synced_events" bson:"compress_synced_events"`
	UseSharedApprovalFallback bool   `json:"use_shared_approval_fallback" yaml:"use_shared_approval_fallback" bson:"use_shared_approval_fallback"`
}

type DataSource struct {
	ID         string `json:"id" yaml:"id" bson:"id"`
	Name       string `json:"name" yaml:"name" bson:"name"`
	Type       string `json:"type" yaml:"type" bson:"type"` // internal_mongodb | external_mongodb
	Enabled    bool   `json:"enabled" yaml:"enabled" bson:"enabled"`
	Active     bool   `json:"active" yaml:"active" bson:"active"`
	URI        string `json:"uri,omitempty" yaml:"uri,omitempty" bson:"uri,omitempty"`
	URIEnv     string `json:"uri_env,omitempty" yaml:"uri_env,omitempty" bson:"uri_env,omitempty"`
	Database   string `json:"database" yaml:"database" bson:"database"`
	Namespace  string `json:"namespace,omitempty" yaml:"namespace,omitempty" bson:"namespace,omitempty"`
	Service    string `json:"service,omitempty" yaml:"service,omitempty" bson:"service,omitempty"`
	SecretName string `json:"secret_name,omitempty" yaml:"secret_name,omitempty" bson:"secret_name,omitempty"`
	ReplicaSet string `json:"replica_set,omitempty" yaml:"replica_set,omitempty" bson:"replica_set,omitempty"`
}

type ResourceScope struct {
	ID         string   `json:"id" yaml:"id" bson:"id"`
	Name       string   `json:"name" yaml:"name" bson:"name"`
	Enabled    bool     `json:"enabled" yaml:"enabled" bson:"enabled"`
	APIGroups  []string `json:"api_groups" yaml:"api_groups" bson:"api_groups"`
	Resources  []string `json:"resources" yaml:"resources" bson:"resources"`
	Kinds      []string `json:"kinds" yaml:"kinds" bson:"kinds"`
	Namespaces []string `json:"namespaces" yaml:"namespaces" bson:"namespaces"`
	Names      []string `json:"names" yaml:"names" bson:"names"`
}

type ActorGroup struct {
	ID              string   `json:"id" yaml:"id" bson:"id"`
	Name            string   `json:"name" yaml:"name" bson:"name"`
	Enabled         bool     `json:"enabled" yaml:"enabled" bson:"enabled"`
	Users           []string `json:"users" yaml:"users" bson:"users"`
	Groups          []string `json:"groups" yaml:"groups" bson:"groups"`
	ServiceAccounts []string `json:"service_accounts" yaml:"service_accounts" bson:"service_accounts"`
}

type PolicyRule struct {
	ID             string              `json:"id" yaml:"id" bson:"id"`
	Name           string              `json:"name" yaml:"name" bson:"name"`
	Enabled        bool                `json:"enabled" yaml:"enabled" bson:"enabled"`
	Priority       int                 `json:"priority" yaml:"priority" bson:"priority"`
	ScopeIDs       []string            `json:"scope_ids" yaml:"scope_ids" bson:"scope_ids"`
	Operations     []string            `json:"operations" yaml:"operations" bson:"operations"`
	ActorGroupIDs  []string            `json:"actor_group_ids" yaml:"actor_group_ids" bson:"actor_group_ids"`
	ChangeClasses  []string            `json:"change_classes" yaml:"change_classes" bson:"change_classes"`
	Decision       string              `json:"decision" yaml:"decision" bson:"decision"`
	Reason         string              `json:"reason" yaml:"reason" bson:"reason"`
	Notify         NotificationBinding `json:"notify" yaml:"notify" bson:"notify"`
	Rollback       RollbackBinding     `json:"rollback" yaml:"rollback" bson:"rollback"`
	Approval       ApprovalBinding     `json:"approval" yaml:"approval" bson:"approval"`
	ControllerSafe ControllerSafeRule  `json:"controller_safe" yaml:"controller_safe" bson:"controller_safe"`
}

type ControllerSafeRule struct {
	RequireOwnerReference           bool     `json:"require_owner_reference" yaml:"require_owner_reference" bson:"require_owner_reference"`
	RequireControllerOwnerReference bool     `json:"require_controller_owner_reference" yaml:"require_controller_owner_reference" bson:"require_controller_owner_reference"`
	AllowedOwnerKinds               []string `json:"allowed_owner_kinds" yaml:"allowed_owner_kinds" bson:"allowed_owner_kinds"`
	RequireNodeUserMatchesPodNode   bool     `json:"require_node_user_matches_pod_node" yaml:"require_node_user_matches_pod_node" bson:"require_node_user_matches_pod_node"`
}

type NotificationBinding struct {
	Enabled         bool     `json:"enabled" yaml:"enabled" bson:"enabled"`
	TemplateID      string   `json:"template_id" yaml:"template_id" bson:"template_id"`
	TelegramBotIDs  []string `json:"telegram_bot_ids" yaml:"telegram_bot_ids" bson:"telegram_bot_ids"`
	TelegramChatIDs []string `json:"telegram_chat_ids" yaml:"telegram_chat_ids" bson:"telegram_chat_ids"`
	TelegramUserIDs []string `json:"telegram_user_ids" yaml:"telegram_user_ids" bson:"telegram_user_ids"`
}

type ApprovalBinding struct {
	Enabled               bool     `json:"enabled" yaml:"enabled" bson:"enabled"`
	Mode                  string   `json:"mode" yaml:"mode" bson:"mode"` // telegram | web | both
	TTLSeconds            int      `json:"ttl_seconds" yaml:"ttl_seconds" bson:"ttl_seconds"`
	ApproverTelegramUsers []string `json:"approver_telegram_users" yaml:"approver_telegram_users" bson:"approver_telegram_users"`
	FailWhenStoreDown     bool     `json:"fail_when_store_down" yaml:"fail_when_store_down" bson:"fail_when_store_down"`
}

type RollbackBinding struct {
	Enabled        bool   `json:"enabled" yaml:"enabled" bson:"enabled"`
	Mode           string `json:"mode" yaml:"mode" bson:"mode"`
	ShowInTelegram bool   `json:"show_in_telegram" yaml:"show_in_telegram" bson:"show_in_telegram"`
	ShowInWeb      bool   `json:"show_in_web" yaml:"show_in_web" bson:"show_in_web"`
}

type RollbackConfig struct {
	Enabled              bool `json:"enabled" yaml:"enabled" bson:"enabled"`
	CreateDeleteRollback bool `json:"create_delete_rollback" yaml:"create_delete_rollback" bson:"create_delete_rollback"`
	RequireDryRun        bool `json:"require_dry_run" yaml:"require_dry_run" bson:"require_dry_run"`
}

type AuditConfig struct {
	Enabled               bool `json:"enabled" yaml:"enabled" bson:"enabled"`
	WriteAdmissionRequest bool `json:"write_admission_request" yaml:"write_admission_request" bson:"write_admission_request"`
	RecordNoiseEvents     bool `json:"record_noise_events" yaml:"record_noise_events" bson:"record_noise_events"`
	PreferMongo           bool `json:"prefer_mongo" yaml:"prefer_mongo" bson:"prefer_mongo"`
}

type SystemProtectionConfig struct {
	Enabled                     bool     `json:"enabled" yaml:"enabled" bson:"enabled"`
	SystemResourceLabel         string   `json:"system_resource_label" yaml:"system_resource_label" bson:"system_resource_label"`
	InternalMongoResourceValue  string   `json:"internal_mongodb_resource_value" yaml:"internal_mongodb_resource_value" bson:"internal_mongodb_resource_value"`
	ProtectedNamespaces         []string `json:"protected_namespaces" yaml:"protected_namespaces" bson:"protected_namespaces"`
	AllowWebhookSelfMaintenance bool     `json:"allow_webhook_self_maintenance" yaml:"allow_webhook_self_maintenance" bson:"allow_webhook_self_maintenance"`
}

type TelegramConfig struct {
	ID        string         `json:"id,omitempty" yaml:"id,omitempty" bson:"id,omitempty"`
	Enabled   bool           `json:"enabled" yaml:"enabled" bson:"enabled"`
	Bots      []TelegramBot  `json:"bots" yaml:"bots" bson:"bots"`
	Chats     []TelegramChat `json:"chats" yaml:"chats" bson:"chats"`
	Users     []TelegramUser `json:"users" yaml:"users" bson:"users"`
	Version   int64          `json:"version,omitempty" yaml:"version,omitempty" bson:"version,omitempty"`
	UpdatedAt time.Time      `json:"updated_at,omitempty" yaml:"updated_at,omitempty" bson:"updated_at,omitempty"`
	UpdatedBy string         `json:"updated_by,omitempty" yaml:"updated_by,omitempty" bson:"updated_by,omitempty"`
}

type TelegramBot struct {
	ID        string   `json:"id" yaml:"id" bson:"id"`
	Name      string   `json:"name" yaml:"name" bson:"name"`
	Enabled   bool     `json:"enabled" yaml:"enabled" bson:"enabled"`
	Token     string   `json:"token,omitempty" yaml:"token,omitempty" bson:"token,omitempty"`
	TokenEnv  string   `json:"token_env,omitempty" yaml:"token_env,omitempty" bson:"token_env,omitempty"`
	Tokens    []string `json:"tokens,omitempty" yaml:"tokens,omitempty" bson:"tokens,omitempty"`
	TokenEnvs []string `json:"token_envs,omitempty" yaml:"token_envs,omitempty" bson:"token_envs,omitempty"`
}

type TelegramChat struct {
	ID      string `json:"id" yaml:"id" bson:"id"`
	Name    string `json:"name" yaml:"name" bson:"name"`
	Enabled bool   `json:"enabled" yaml:"enabled" bson:"enabled"`
	BotID   string `json:"bot_id" yaml:"bot_id" bson:"bot_id"`
	ChatID  string `json:"chat_id" yaml:"chat_id" bson:"chat_id"`
}

type TelegramUser struct {
	ID             string   `json:"id" yaml:"id" bson:"id"`
	TelegramID     string   `json:"telegram_id" yaml:"telegram_id" bson:"telegram_id"`
	Alias          string   `json:"alias" yaml:"alias" bson:"alias"`
	Username       string   `json:"username" yaml:"username" bson:"username"`
	DisplayName    string   `json:"display_name" yaml:"display_name" bson:"display_name"`
	MentionEnabled bool     `json:"mention_enabled" yaml:"mention_enabled" bson:"mention_enabled"`
	Roles          []string `json:"roles" yaml:"roles" bson:"roles"`
	Enabled        bool     `json:"enabled" yaml:"enabled" bson:"enabled"`
}

type NotificationTemplate struct {
	ID        string `json:"id" yaml:"id" bson:"id"`
	Name      string `json:"name" yaml:"name" bson:"name"`
	Channel   string `json:"channel" yaml:"channel" bson:"channel"`
	ParseMode string `json:"parse_mode" yaml:"parse_mode" bson:"parse_mode"`
	Enabled   bool   `json:"enabled" yaml:"enabled" bson:"enabled"`
	Body      string `json:"body" yaml:"body" bson:"body"`
}

type WebSettings struct {
	SiteName        string `json:"site_name" yaml:"site_name" bson:"site_name"`
	SiteSubtitle    string `json:"site_subtitle" yaml:"site_subtitle" bson:"site_subtitle"`
	SiteIcon        string `json:"site_icon" yaml:"site_icon" bson:"site_icon"`
	DefaultTimezone string `json:"default_timezone" yaml:"default_timezone" bson:"default_timezone"`
	Theme           string `json:"theme" yaml:"theme" bson:"theme"`
}

type WebRole struct {
	ID          string   `json:"id" yaml:"id" bson:"id"`
	Name        string   `json:"name" yaml:"name" bson:"name"`
	Description string   `json:"description" yaml:"description" bson:"description"`
	Permissions []string `json:"permissions" yaml:"permissions" bson:"permissions"`
	Builtin     bool     `json:"builtin" yaml:"builtin" bson:"builtin"`
}

type WebUser struct {
	Username     string    `json:"username" yaml:"username" bson:"username"`
	DisplayName  string    `json:"display_name" yaml:"display_name" bson:"display_name"`
	PasswordHash string    `json:"password_hash,omitempty" yaml:"password_hash,omitempty" bson:"password_hash,omitempty"`
	Roles        []string  `json:"roles" yaml:"roles" bson:"roles"`
	Enabled      bool      `json:"enabled" yaml:"enabled" bson:"enabled"`
	CreatedAt    time.Time `json:"created_at,omitempty" yaml:"created_at,omitempty" bson:"created_at,omitempty"`
	UpdatedAt    time.Time `json:"updated_at,omitempty" yaml:"updated_at,omitempty" bson:"updated_at,omitempty"`
}

type TelegramMessageRef struct {
	BotID     string `json:"bot_id" bson:"bot_id"`
	ChatID    string `json:"chat_id" bson:"chat_id"`
	MessageID int64  `json:"message_id" bson:"message_id"`
}

const (
	NotifyKindAdmissionEvent = "admission_event"
	NotifyKindConfigChange   = "config_change"
	NotifyStatusPending      = "pending"
	NotifyStatusSending      = "sending"
	NotifyStatusSent         = "sent"
	NotifyStatusFailed       = "failed"
)

type TelegramNotificationEvent struct {
	ID            string         `json:"id" bson:"id"`
	Kind          string         `json:"kind" bson:"kind"`
	EventID       string         `json:"event_id" bson:"event_id"`
	ChangeID      string         `json:"change_id,omitempty" bson:"change_id,omitempty"`
	RuleID        string         `json:"rule_id,omitempty" bson:"rule_id,omitempty"`
	RuleName      string         `json:"rule_name,omitempty" bson:"rule_name,omitempty"`
	BotID         string         `json:"bot_id" bson:"bot_id"`
	ChatID        string         `json:"chat_id" bson:"chat_id"`
	TargetName    string         `json:"target_name,omitempty" bson:"target_name,omitempty"`
	Text          string         `json:"text" bson:"text"`
	ParseMode     string         `json:"parse_mode,omitempty" bson:"parse_mode,omitempty"`
	ReplyMarkup   map[string]any `json:"reply_markup,omitempty" bson:"reply_markup,omitempty"`
	Status        string         `json:"status" bson:"status"`
	Attempts      int            `json:"attempts" bson:"attempts"`
	MaxAttempts   int            `json:"max_attempts" bson:"max_attempts"`
	NextAttemptAt time.Time      `json:"next_attempt_at" bson:"next_attempt_at"`
	ClaimedBy     string         `json:"claimed_by,omitempty" bson:"claimed_by,omitempty"`
	ClaimedAt     time.Time      `json:"claimed_at,omitempty" bson:"claimed_at,omitempty"`
	CreatedAt     time.Time      `json:"created_at" bson:"created_at"`
	SentAt        time.Time      `json:"sent_at,omitempty" bson:"sent_at,omitempty"`
	MessageID     int64          `json:"message_id,omitempty" bson:"message_id,omitempty"`
	ViewedAt      time.Time      `json:"viewed_at,omitempty" bson:"viewed_at,omitempty"`
	ViewedBy      string         `json:"viewed_by,omitempty" bson:"viewed_by,omitempty"`
	ViewCount     int            `json:"view_count,omitempty" bson:"view_count,omitempty"`
	LastError     string         `json:"last_error,omitempty" bson:"last_error,omitempty"`
}

type ConfigChangeRequest struct {
	ID                   string               `json:"id" bson:"id"`
	EventID              string               `json:"event_id" bson:"event_id"`
	BaseHash             string               `json:"base_hash,omitempty" bson:"base_hash,omitempty"`
	TargetHash           string               `json:"target_hash,omitempty" bson:"target_hash,omitempty"`
	Kind                 string               `json:"kind" bson:"kind"`
	Summary              string               `json:"summary" bson:"summary"`
	DiffSummary          []string             `json:"diff_summary" bson:"diff_summary"`
	Status               string               `json:"status" bson:"status"`
	BaseVersion          int64                `json:"base_version" bson:"base_version"`
	TargetVersion        int64                `json:"target_version" bson:"target_version"`
	CreatedBy            string               `json:"created_by" bson:"created_by"`
	CreatedAt            time.Time            `json:"created_at" bson:"created_at"`
	DecidedBy            string               `json:"decided_by,omitempty" bson:"decided_by,omitempty"`
	DecidedAt            time.Time            `json:"decided_at,omitempty" bson:"decided_at,omitempty"`
	DecisionNote         string               `json:"decision_note,omitempty" bson:"decision_note,omitempty"`
	NotificationMessages []TelegramMessageRef `json:"notification_messages,omitempty" bson:"notification_messages,omitempty"`
	Config               RuntimeConfig        `json:"config" bson:"config"`
}

type ConfigAuditEvent struct {
	ID            string    `json:"id" bson:"id"`
	EventID       string    `json:"event_id" bson:"event_id"`
	Kind          string    `json:"kind" bson:"kind"`
	Category      string    `json:"category" bson:"category"`
	Summary       string    `json:"summary" bson:"summary"`
	DiffSummary   []string  `json:"diff_summary,omitempty" bson:"diff_summary,omitempty"`
	BaseVersion   int64     `json:"base_version" bson:"base_version"`
	TargetVersion int64     `json:"target_version" bson:"target_version"`
	Actor         string    `json:"actor" bson:"actor"`
	CreatedAt     time.Time `json:"created_at" bson:"created_at"`
	Status        string    `json:"status" bson:"status"`
}

type ConfigVersionInfo struct {
	Version   int64     `json:"version" bson:"version"`
	Active    bool      `json:"active" bson:"active"`
	Source    string    `json:"source" bson:"source"`
	CreatedAt time.Time `json:"created_at" bson:"created_at"`
}

type AdmissionEvent struct {
	ID              string          `json:"id" bson:"id"`
	RequestUID      string          `json:"request_uid" bson:"request_uid"`
	Time            time.Time       `json:"time" bson:"time"`
	Cluster         string          `json:"cluster" bson:"cluster"`
	Operation       string          `json:"operation" bson:"operation"`
	APIVersion      string          `json:"api_version" bson:"api_version"`
	APIGroup        string          `json:"api_group" bson:"api_group"`
	Resource        string          `json:"resource" bson:"resource"`
	SubResource     string          `json:"sub_resource" bson:"sub_resource"`
	Kind            string          `json:"kind" bson:"kind"`
	Namespace       string          `json:"namespace" bson:"namespace"`
	Name            string          `json:"name" bson:"name"`
	User            string          `json:"user" bson:"user"`
	Groups          []string        `json:"groups" bson:"groups"`
	ScopeMatched    bool            `json:"scope_matched" bson:"scope_matched"`
	ScopeIDs        []string        `json:"scope_ids" bson:"scope_ids"`
	RuleID          string          `json:"rule_id" bson:"rule_id"`
	RuleName        string          `json:"rule_name" bson:"rule_name"`
	Decision        string          `json:"decision" bson:"decision"`
	Allowed         bool            `json:"allowed" bson:"allowed"`
	Reason          string          `json:"reason" bson:"reason"`
	ChangeClass     string          `json:"change_class" bson:"change_class"`
	ChangeSummary   string          `json:"change_summary" bson:"change_summary"`
	RollbackID      string          `json:"rollback_id,omitempty" bson:"rollback_id,omitempty"`
	PersistStatus   string          `json:"persist_status" bson:"persist_status"`
	NotificationIDs []string        `json:"notification_ids,omitempty" bson:"notification_ids,omitempty"`
	Object          json.RawMessage `json:"object,omitempty" bson:"object,omitempty"`
	OldObject       json.RawMessage `json:"old_object,omitempty" bson:"old_object,omitempty"`
}

type RollbackBackup struct {
	ID               string          `json:"id" bson:"id"`
	RequestUID       string          `json:"request_uid" bson:"request_uid"`
	EventID          string          `json:"event_id" bson:"event_id"`
	CreatedAt        time.Time       `json:"created_at" bson:"created_at"`
	Cluster          string          `json:"cluster" bson:"cluster"`
	Operation        string          `json:"operation" bson:"operation"`
	Mode             string          `json:"mode" bson:"mode"`
	APIGroup         string          `json:"api_group" bson:"api_group"`
	APIVersion       string          `json:"api_version" bson:"api_version"`
	Kind             string          `json:"kind" bson:"kind"`
	Resource         string          `json:"resource" bson:"resource"`
	Namespace        string          `json:"namespace" bson:"namespace"`
	Name             string          `json:"name" bson:"name"`
	SourceObject     json.RawMessage `json:"source_object" bson:"source_object"`
	CreatedObjectUID string          `json:"created_object_uid,omitempty" bson:"created_object_uid,omitempty"`
	DryRunRequired   bool            `json:"dry_run_required" bson:"dry_run_required"`
}

type ServiceAccountInfo struct {
	ID                  string    `json:"id" bson:"id"`
	Cluster             string    `json:"cluster" bson:"cluster"`
	Namespace           string    `json:"namespace" bson:"namespace"`
	Name                string    `json:"name" bson:"name"`
	UserString          string    `json:"user_string" bson:"user_string"`
	Category            string    `json:"category" bson:"category"`
	Confidence          string    `json:"confidence" bson:"confidence"`
	UsedBy              []string  `json:"used_by" bson:"used_by"`
	RoleBindings        []string  `json:"role_bindings" bson:"role_bindings"`
	ClusterRoleBindings []string  `json:"cluster_role_bindings" bson:"cluster_role_bindings"`
	SuggestedActorGroup string    `json:"suggested_actor_group" bson:"suggested_actor_group"`
	ScannedAt           time.Time `json:"scanned_at" bson:"scanned_at"`
}
