package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"k8s.io/klog/v2"
)

const (
	rollbackStorageAuto  = "auto"
	rollbackStorageMongo = "mongo"
	rollbackStorageFile  = "file"

	rollbackActionApply    = "apply"
	rollbackActionDownload = "yaml"

	rollbackStatusPending = "pending"
	rollbackStatusRunning = "running"
	rollbackStatusApplied = "applied"
	rollbackStatusFailed  = "failed"
	rollbackStatusExpired = "expired"

	defaultRollbackStorageLockTTLSeconds = 60
	defaultRollbackSchemaVersion         = 1
)

var (
	ErrRollbackNotFound       = errors.New("rollback record not found")
	ErrRollbackExpired        = errors.New("rollback record expired")
	ErrRollbackAlreadyApplied = errors.New("rollback record already applied")
	ErrRollbackLocked         = errors.New("rollback record is locked by another pod")
)

// RollbackStorageConfig controls the persistence backend.
//
// type:
//   - auto: use MongoDB when rollback.mongo.enabled=true and MongoDB is reachable;
//     otherwise use the PVC/file backend.
//   - mongo: require MongoDB.
//   - file: require PVC/file backend.
//
// data_directory is used by the file backend and also stores Telegram offset / lock
// files for multi-pod callback consumption.
type RollbackStorageConfig struct {
	Type           string `json:"type" yaml:"type"`
	DataDirectory  string `json:"data_directory" yaml:"data_directory"`
	LockTTLSeconds int    `json:"lock_ttl_seconds" yaml:"lock_ttl_seconds"`
	WriteFsync     *bool  `json:"write_fsync" yaml:"write_fsync"`
}

// RollbackConfig replaces the original rollback config and keeps backward
// compatibility with state_directory.
type RollbackConfig struct {
	Enabled               bool                  `json:"enabled" yaml:"enabled"`
	AuthorizedTelegramIDs []string              `json:"authorized_telegram_ids" yaml:"authorized_telegram_ids"`
	RetentionHours        int                   `json:"retention_hours" yaml:"retention_hours"`
	StateDirectory        string                `json:"state_directory" yaml:"state_directory"` // backward compatible alias
	PollIntervalSeconds   int                   `json:"poll_interval_seconds" yaml:"poll_interval_seconds"`
	FieldManager          string                `json:"field_manager" yaml:"field_manager"`
	AllowReapply          bool                  `json:"allow_reapply" yaml:"allow_reapply"`
	Storage               RollbackStorageConfig `json:"storage" yaml:"storage"`
	Mongo                 MongoAuditConfig      `json:"mongo" yaml:"mongo"`
}

type RollbackTelegramMessage struct {
	ChatID    int64  `json:"chat_id" bson:"chat_id"`
	MessageID int    `json:"message_id" bson:"message_id"`
	Kind      string `json:"kind" bson:"kind"`
}

type RollbackHistoryItem struct {
	At        time.Time `json:"at" bson:"at"`
	Action    string    `json:"action" bson:"action"`
	By        string    `json:"by" bson:"by"`
	Status    string    `json:"status" bson:"status"`
	Message   string    `json:"message,omitempty" bson:"message,omitempty"`
	Pod       string    `json:"pod,omitempty" bson:"pod,omitempty"`
	ChatID    int64     `json:"chat_id,omitempty" bson:"chat_id,omitempty"`
	MessageID int       `json:"message_id,omitempty" bson:"message_id,omitempty"`
}

type RollbackRecord struct {
	SchemaVersion int       `json:"schema_version" bson:"schema_version"`
	ID            string    `json:"id" bson:"_id"`
	Timestamp     time.Time `json:"timestamp" bson:"timestamp"`
	UpdatedAt     time.Time `json:"updated_at" bson:"updated_at"`
	ExpiresAt     time.Time `json:"expires_at" bson:"expires_at"`

	ClusterName     string `json:"cluster_name" bson:"cluster_name"`
	RequestUID      string `json:"request_uid" bson:"request_uid"`
	Username        string `json:"username" bson:"username"`
	Operation       string `json:"operation" bson:"operation"`
	Kind            string `json:"kind" bson:"kind"`
	Resource        string `json:"resource" bson:"resource"`
	ResourceGroup   string `json:"resource_group,omitempty" bson:"resource_group,omitempty"`
	ResourceVersion string `json:"resource_version" bson:"resource_version"`
	Name            string `json:"name" bson:"name"`
	Namespace       string `json:"namespace,omitempty" bson:"namespace,omitempty"`
	ResourceDisplay string `json:"resource_display" bson:"resource_display"`

	// MongoDB backend can keep ManifestYAML inline.
	// File backend stores the manifest in ManifestFile and returns it through
	// RollbackStore.LoadRecord / mutation methods.
	ManifestYAML   string `json:"manifest_yaml,omitempty" bson:"manifest_yaml,omitempty"`
	ManifestJSON   string `json:"manifest_json" bson:"manifest_json"`
	ManifestFile   string `json:"manifest_file,omitempty" bson:"manifest_file,omitempty"`
	ManifestSHA256 string `json:"manifest_sha256,omitempty" bson:"manifest_sha256,omitempty"`

	ExecutedAt      time.Time `json:"executed_at,omitempty" bson:"executed_at,omitempty"`
	ExecutedBy      string    `json:"executed_by,omitempty" bson:"executed_by,omitempty"`
	ExecutionStatus string    `json:"execution_status" bson:"execution_status"`
	ExecutionError  string    `json:"execution_error,omitempty" bson:"execution_error,omitempty"`

	RollbackClickCount int       `json:"rollback_click_count" bson:"rollback_click_count"`
	DownloadClickCount int       `json:"download_click_count" bson:"download_click_count"`
	LastClickedAt      time.Time `json:"last_clicked_at,omitempty" bson:"last_clicked_at,omitempty"`
	LastClickedBy      string    `json:"last_clicked_by,omitempty" bson:"last_clicked_by,omitempty"`
	LastAction         string    `json:"last_action,omitempty" bson:"last_action,omitempty"`

	TelegramMessages []RollbackTelegramMessage `json:"telegram_messages,omitempty" bson:"telegram_messages,omitempty"`
	History          []RollbackHistoryItem     `json:"history,omitempty" bson:"history,omitempty"`
}

type RollbackStore interface {
	Type() string
	SaveRecord(record RollbackRecord, manifestYAML string) error
	LoadRecord(id string) (RollbackRecord, string, error)

	MarkRunning(id string, telegramID string, allowReapply bool) (RollbackRecord, string, error)
	MarkApplied(id string, telegramID string) (RollbackRecord, string, error)
	MarkFailed(id string, telegramID string, message string) (RollbackRecord, string, error)
	IncrementDownload(id string, telegramID string) (RollbackRecord, string, error)

	AddTelegramMessage(id string, msg telegramSentMessage, kind string) error
	CleanupExpired(now time.Time) error
}

func applyRollbackDefaults() {
	if !config.Rollback.Enabled {
		return
	}
	if config.Rollback.RetentionHours <= 0 {
		config.Rollback.RetentionHours = defaultRollbackRetentionHours
	}
	if strings.TrimSpace(config.Rollback.Storage.Type) == "" {
		config.Rollback.Storage.Type = rollbackStorageAuto
	}
	if strings.TrimSpace(config.Rollback.Storage.DataDirectory) == "" {
		if strings.TrimSpace(config.Rollback.StateDirectory) != "" {
			config.Rollback.Storage.DataDirectory = config.Rollback.StateDirectory
		} else {
			config.Rollback.Storage.DataDirectory = defaultDeleteConfirmationDirectory
		}
	}
	if strings.TrimSpace(config.Rollback.StateDirectory) == "" {
		config.Rollback.StateDirectory = config.Rollback.Storage.DataDirectory
	}
	if config.Rollback.Storage.LockTTLSeconds <= 0 {
		config.Rollback.Storage.LockTTLSeconds = defaultRollbackStorageLockTTLSeconds
	}
	if config.Rollback.PollIntervalSeconds <= 0 {
		config.Rollback.PollIntervalSeconds = defaultDeleteConfirmationPollSeconds
	}
	if strings.TrimSpace(config.Rollback.FieldManager) == "" {
		config.Rollback.FieldManager = defaultRollbackFieldManager
	}
}

func rollbackWriteFsyncEnabled(cfg RollbackConfig) bool {
	if cfg.Storage.WriteFsync == nil {
		return true
	}
	return *cfg.Storage.WriteFsync
}

func rollbackDataDirectory(cfg RollbackConfig) string {
	if strings.TrimSpace(cfg.Storage.DataDirectory) != "" {
		return strings.TrimSpace(cfg.Storage.DataDirectory)
	}
	if strings.TrimSpace(cfg.StateDirectory) != "" {
		return strings.TrimSpace(cfg.StateDirectory)
	}
	return defaultDeleteConfirmationDirectory
}

func rollbackRuntimeDirectory(cfg RollbackConfig) string {
	return filepath.Join(rollbackDataDirectory(cfg), "rollback")
}

func newRollbackStore(cfg RollbackConfig) (RollbackStore, error) {
	storageType := strings.ToLower(strings.TrimSpace(cfg.Storage.Type))
	if storageType == "" {
		storageType = rollbackStorageAuto
	}

	switch storageType {
	case rollbackStorageMongo:
		store, err := newMongoRollbackStore(cfg)
		if err != nil {
			return nil, err
		}
		return store, nil
	case rollbackStorageFile:
		return newFileRollbackStore(cfg)
	case rollbackStorageAuto:
		if cfg.Mongo.Enabled || config.Audit.Mongo.Enabled {
			store, err := newMongoRollbackStore(cfg)
			if err == nil {
				klog.Infof("Rollback storage backend: mongo")
				return store, nil
			}
			klog.Warningf("Rollback MongoDB backend unavailable, falling back to file backend: %v", err)
		}
		store, err := newFileRollbackStore(cfg)
		if err != nil {
			return nil, err
		}
		klog.Infof("Rollback storage backend: file")
		return store, nil
	default:
		return nil, fmt.Errorf("unsupported rollback.storage.type '%s' (expected auto, mongo, or file)", storageType)
	}
}

func normalizeRollbackRecord(record RollbackRecord) RollbackRecord {
	now := time.Now()
	if record.SchemaVersion <= 0 {
		record.SchemaVersion = defaultRollbackSchemaVersion
	}
	if record.Timestamp.IsZero() {
		record.Timestamp = now
	}
	if record.UpdatedAt.IsZero() {
		record.UpdatedAt = record.Timestamp
	}
	if strings.TrimSpace(record.ExecutionStatus) == "" {
		record.ExecutionStatus = rollbackStatusPending
	}
	return record
}

func rollbackHistory(action string, by string, status string, message string) RollbackHistoryItem {
	host, _ := os.Hostname()
	return RollbackHistoryItem{
		At:      time.Now(),
		Action:  action,
		By:      by,
		Status:  status,
		Message: message,
		Pod:     host,
	}
}
