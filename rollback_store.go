package main

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	rollbackStorageTypeAuto  = "auto"
	rollbackStorageTypeMongo = "mongo"
	rollbackStorageTypeFile  = "file"

	rollbackStatusPending = "pending"
	rollbackStatusRunning = "running"
	rollbackStatusApplied = "applied"
	rollbackStatusFailed  = "failed"
	rollbackStatusExpired = "expired"

	rollbackActionApply    = "apply"
	rollbackActionDownload = "yaml"
	rollbackActionCreated  = "created"
)

var (
	errRollbackAlreadyApplied = errors.New("rollback already applied")
	errRollbackAlreadyRunning = errors.New("rollback already running")
	errRollbackExpired        = errors.New("rollback backup expired")
	errRollbackNotFound       = errors.New("rollback backup not found")
)

type RollbackStorageConfig struct {
	Type           string `json:"type" yaml:"type"`
	DataDirectory  string `json:"data_directory" yaml:"data_directory"`
	LockTTLSeconds int    `json:"lock_ttl_seconds" yaml:"lock_ttl_seconds"`
	WriteFsync     bool   `json:"write_fsync" yaml:"write_fsync"`
	DisableFsync   bool   `json:"disable_fsync" yaml:"disable_fsync"`
}

type RollbackTelegramMessage struct {
	ChatID    int64     `json:"chat_id" bson:"chat_id"`
	MessageID int       `json:"message_id" bson:"message_id"`
	Kind      string    `json:"kind" bson:"kind"`
	SentAt    time.Time `json:"sent_at" bson:"sent_at"`
}

type RollbackHistoryItem struct {
	At      time.Time `json:"at" bson:"at"`
	Action  string    `json:"action" bson:"action"`
	By      string    `json:"by" bson:"by"`
	Status  string    `json:"status" bson:"status"`
	Message string    `json:"message,omitempty" bson:"message,omitempty"`
}

type rollbackLoadedRecord struct {
	Record       RollbackRecord
	ManifestYAML string
}

type RollbackStore interface {
	BackendName() string
	SaveRecord(record RollbackRecord, manifestYAML string) error
	LoadRecord(id string) (RollbackRecord, error)
	LoadRecordWithManifest(id string) (rollbackLoadedRecord, error)
	MarkRunning(id string, telegramID string) (RollbackRecord, error)
	MarkApplied(id string, telegramID string) (RollbackRecord, error)
	MarkFailed(id string, telegramID string, message string) (RollbackRecord, error)
	IncrementDownload(id string, telegramID string) (rollbackLoadedRecord, error)
	AddTelegramMessage(id string, msg RollbackTelegramMessage) error
	CleanupExpired(now time.Time) error
}

func normalizeRollbackStorageType(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", rollbackStorageTypeAuto:
		return rollbackStorageTypeAuto
	case rollbackStorageTypeMongo:
		return rollbackStorageTypeMongo
	case rollbackStorageTypeFile, "pvc", "local":
		return rollbackStorageTypeFile
	default:
		return rollbackStorageTypeAuto
	}
}

func defaultRollbackHistory(action string, by string, status string, message string) RollbackHistoryItem {
	return RollbackHistoryItem{
		At:      time.Now(),
		Action:  action,
		By:      by,
		Status:  status,
		Message: message,
	}
}

func rollbackTerminalStatusLabel(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case rollbackStatusPending, "":
		return "未执行"
	case rollbackStatusRunning:
		return "执行中"
	case rollbackStatusApplied:
		return "已成功"
	case rollbackStatusFailed:
		return "已失败"
	case rollbackStatusExpired:
		return "已过期"
	default:
		return status
	}
}

func rollbackActionLabel(action string) string {
	switch strings.ToLower(strings.TrimSpace(action)) {
	case rollbackActionApply:
		return "执行回滚"
	case rollbackActionDownload:
		return "下载 YAML"
	case rollbackActionCreated:
		return "创建备份"
	default:
		if strings.TrimSpace(action) == "" {
			return "-"
		}
		return action
	}
}

func rollbackStoreErrorForNoMatch(id string, current RollbackRecord, loadErr error) error {
	if loadErr != nil {
		return loadErr
	}
	if current.ID == "" {
		return fmt.Errorf("%w: %s", errRollbackNotFound, id)
	}
	if time.Now().After(current.ExpiresAt) || current.ExecutionStatus == rollbackStatusExpired {
		return errRollbackExpired
	}
	if current.ExecutionStatus == rollbackStatusApplied {
		return errRollbackAlreadyApplied
	}
	if current.ExecutionStatus == rollbackStatusRunning {
		return errRollbackAlreadyRunning
	}
	return fmt.Errorf("rollback record %s cannot be updated from status %q", id, current.ExecutionStatus)
}

func isRollbackRunningStale(record *RollbackRecord, now time.Time, timeout time.Duration) bool {
	if record == nil {
		return false
	}
	if record.ExecutionStatus != rollbackStatusRunning {
		return false
	}
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	if record.LastClickedAt.IsZero() {
		return false
	}
	return now.Sub(record.LastClickedAt) > timeout
}
