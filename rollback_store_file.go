package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"k8s.io/klog/v2"
)

const (
	rollbackFileRecordsDir   = "records"
	rollbackFileManifestsDir = "manifests"
	rollbackFileLocksDir     = "locks"
	rollbackFileEventsDir    = "events"
	rollbackFileTmpDir       = "tmp"
	rollbackFileLockDefault  = 60 * time.Second
)

type fileRollbackStore struct {
	rootDir        string
	lockTTL        time.Duration
	fsyncEnabled   bool
	allowReapply   bool
	runningTimeout time.Duration
}

func newFileRollbackStore(dataDirectory string, lockTTL time.Duration, fsyncEnabled bool, allowReapply bool, runningTimeout time.Duration) (*fileRollbackStore, error) {
	base := strings.TrimSpace(dataDirectory)
	if base == "" {
		base = defaultDeleteConfirmationDirectory
	}
	root := filepath.Join(base, "rollback")
	store := &fileRollbackStore{
		rootDir:        root,
		lockTTL:        lockTTL,
		fsyncEnabled:   fsyncEnabled,
		allowReapply:   allowReapply,
		runningTimeout: runningTimeout,
	}
	if store.lockTTL <= 0 {
		store.lockTTL = rollbackFileLockDefault
	}
	for _, dir := range []string{
		filepath.Join(root, rollbackFileRecordsDir),
		filepath.Join(root, rollbackFileManifestsDir),
		filepath.Join(root, rollbackFileLocksDir),
		filepath.Join(root, rollbackFileEventsDir),
		filepath.Join(root, rollbackFileTmpDir),
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("failed to create rollback file store directory '%s': %w", dir, err)
		}
	}
	return store, nil
}

func (s *fileRollbackStore) BackendName() string { return rollbackStorageTypeFile }

func (s *fileRollbackStore) shard(id string) (string, string) {
	clean := strings.TrimSpace(id)
	if len(clean) < 4 {
		return "00", "00"
	}
	return clean[:2], clean[2:4]
}

func (s *fileRollbackStore) recordPath(id string) string {
	a, b := s.shard(id)
	return filepath.Join(s.rootDir, rollbackFileRecordsDir, a, b, id+".json")
}

func (s *fileRollbackStore) manifestPath(id string) string {
	a, b := s.shard(id)
	return filepath.Join(s.rootDir, rollbackFileManifestsDir, a, b, id+".yaml")
}

func (s *fileRollbackStore) manifestRelativePath(id string) string {
	a, b := s.shard(id)
	return filepath.ToSlash(filepath.Join("rollback", rollbackFileManifestsDir, a, b, id+".yaml"))
}

func (s *fileRollbackStore) lockPath(id string) string {
	a, b := s.shard(id)
	return filepath.Join(s.rootDir, rollbackFileLocksDir, a, b, id+".lock")
}

func (s *fileRollbackStore) SaveRecord(record RollbackRecord, manifestYAML string) error {
	if strings.TrimSpace(record.ID) == "" {
		return fmt.Errorf("rollback record id is empty")
	}
	if strings.TrimSpace(manifestYAML) == "" {
		return fmt.Errorf("rollback manifest is empty")
	}
	unlock, err := s.acquireRecordLock(record.ID, "save")
	if err != nil {
		return err
	}
	defer unlock()

	if _, err := os.Stat(s.recordPath(record.ID)); err == nil {
		return nil
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}

	manifestAbs := s.manifestPath(record.ID)
	manifestSum := sha256.Sum256([]byte(manifestYAML))
	record.ManifestSHA256 = hex.EncodeToString(manifestSum[:])
	record.ManifestFile = s.manifestRelativePath(record.ID)
	record.ManifestYAML = ""
	if strings.TrimSpace(record.ExecutionStatus) == "" {
		record.ExecutionStatus = rollbackStatusPending
	}
	if record.History == nil {
		record.History = []RollbackHistoryItem{}
	}
	record.History = appendRollbackHistory(record.History, defaultRollbackHistory(rollbackActionCreated, "system", record.ExecutionStatus, "rollback backup recorded"))

	if err := atomicWriteFile(manifestAbs, []byte(manifestYAML), 0o600, s.fsyncEnabled); err != nil {
		return err
	}
	if err := s.writeRecord(record); err != nil {
		return err
	}
	s.writeEvent(record.ID, rollbackActionCreated, "system", "created")
	return nil
}

func (s *fileRollbackStore) LoadRecord(id string) (RollbackRecord, error) {
	return s.readRecord(id)
}

func (s *fileRollbackStore) LoadRecordWithManifest(id string) (rollbackLoadedRecord, error) {
	record, err := s.readRecord(id)
	if err != nil {
		return rollbackLoadedRecord{}, err
	}
	manifest := strings.TrimSpace(record.ManifestYAML)
	if manifest == "" {
		payload, err := os.ReadFile(s.manifestPath(id))
		if err != nil {
			return rollbackLoadedRecord{}, err
		}
		manifest = string(payload)
	}
	return rollbackLoadedRecord{Record: record, ManifestYAML: manifest}, nil
}

func (s *fileRollbackStore) MarkRunning(id string, actor RollbackActor) (RollbackRecord, error) {
	return s.mutateRecord(id, func(record *RollbackRecord, now time.Time) error {
		if now.After(record.ExpiresAt) || record.ExecutionStatus == rollbackStatusExpired {
			record.ExecutionStatus = rollbackStatusExpired
			return errRollbackExpired
		}
		if record.ExecutionStatus == rollbackStatusRunning && !isRollbackRunningStale(record, now, s.runningTimeout) {
			return errRollbackAlreadyRunning
		}
		if record.ExecutionStatus == rollbackStatusApplied && !s.allowReapply {
			return errRollbackAlreadyApplied
		}
		record.RollbackClickCount++
		record.LastClickedAt = now
		record.LastClickedBy = actor.ID
		record.LastClickedByUsername = actor.Username
		record.LastClickedByDisplayName = actor.DisplayName
		record.LastAction = rollbackActionApply
		record.ExecutionStatus = rollbackStatusRunning
		record.ExecutionError = ""
		record.History = appendRollbackHistory(record.History, RollbackHistoryItem{At: now, Action: rollbackActionApply, By: actor.Identifier(), Status: rollbackStatusRunning, Message: "rollback apply clicked"})
		return nil
	})
}

func (s *fileRollbackStore) MarkApplied(id string, actor RollbackActor) (RollbackRecord, error) {
	return s.mutateRecord(id, func(record *RollbackRecord, now time.Time) error {
		record.ExecutedAt = now
		record.ExecutedBy = actor.ID
		record.ExecutedByUsername = actor.Username
		record.ExecutedByDisplayName = actor.DisplayName
		record.ExecutionStatus = rollbackStatusApplied
		record.ExecutionError = ""
		record.LastClickedAt = now
		record.LastClickedBy = actor.ID
		record.LastClickedByUsername = actor.Username
		record.LastClickedByDisplayName = actor.DisplayName
		record.LastAction = rollbackActionApply
		record.History = appendRollbackHistory(record.History, RollbackHistoryItem{At: now, Action: rollbackActionApply, By: actor.Identifier(), Status: rollbackStatusApplied, Message: "rollback applied"})
		return nil
	})
}

func (s *fileRollbackStore) MarkFailed(id string, actor RollbackActor, message string) (RollbackRecord, error) {
	return s.mutateRecord(id, func(record *RollbackRecord, now time.Time) error {
		record.ExecutedAt = now
		record.ExecutedBy = actor.ID
		record.ExecutedByUsername = actor.Username
		record.ExecutedByDisplayName = actor.DisplayName
		record.ExecutionStatus = rollbackStatusFailed
		record.ExecutionError = message
		record.LastClickedAt = now
		record.LastClickedBy = actor.ID
		record.LastClickedByUsername = actor.Username
		record.LastClickedByDisplayName = actor.DisplayName
		record.LastAction = rollbackActionApply
		record.History = appendRollbackHistory(record.History, RollbackHistoryItem{At: now, Action: rollbackActionApply, By: actor.Identifier(), Status: rollbackStatusFailed, Message: message})
		return nil
	})
}

func (s *fileRollbackStore) IncrementDownload(id string, actor RollbackActor) (rollbackLoadedRecord, error) {
	record, err := s.mutateRecord(id, func(record *RollbackRecord, now time.Time) error {
		if now.After(record.ExpiresAt) || record.ExecutionStatus == rollbackStatusExpired {
			record.ExecutionStatus = rollbackStatusExpired
			return errRollbackExpired
		}
		record.DownloadClickCount++
		record.LastClickedAt = now
		record.LastClickedBy = actor.ID
		record.LastClickedByUsername = actor.Username
		record.LastClickedByDisplayName = actor.DisplayName
		record.LastAction = rollbackActionDownload
		record.History = appendRollbackHistory(record.History, RollbackHistoryItem{At: now, Action: rollbackActionDownload, By: actor.Identifier(), Status: record.ExecutionStatus, Message: "rollback yaml requested"})
		return nil
	})
	if err != nil {
		return rollbackLoadedRecord{}, err
	}
	manifest := strings.TrimSpace(record.ManifestYAML)
	if manifest == "" {
		payload, readErr := os.ReadFile(s.manifestPath(id))
		if readErr != nil {
			return rollbackLoadedRecord{}, readErr
		}
		manifest = string(payload)
	}
	return rollbackLoadedRecord{Record: record, ManifestYAML: manifest}, nil
}

func (s *fileRollbackStore) AddTelegramMessage(id string, msg RollbackTelegramMessage) error {
	_, err := s.mutateRecord(id, func(record *RollbackRecord, now time.Time) error {
		if msg.SentAt.IsZero() {
			msg.SentAt = now
		}
		record.TelegramMessages = append(record.TelegramMessages, msg)
		return nil
	})
	return err
}

func (s *fileRollbackStore) CleanupExpired(now time.Time) error {
	recordsRoot := filepath.Join(s.rootDir, rollbackFileRecordsDir)
	return filepath.WalkDir(recordsRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil || d == nil || d.IsDir() || !strings.HasSuffix(d.Name(), ".json") {
			return nil
		}
		id := strings.TrimSuffix(d.Name(), ".json")
		record, readErr := s.readRecord(id)
		if readErr != nil {
			klog.Errorf("Failed to read rollback record during cleanup '%s': %v", id, readErr)
			return nil
		}
		if now.Before(record.ExpiresAt) {
			return nil
		}
		_, _ = s.mutateRecord(id, func(record *RollbackRecord, now time.Time) error {
			record.ExecutionStatus = rollbackStatusExpired
			record.History = appendRollbackHistory(record.History, RollbackHistoryItem{At: now, Action: "expire", By: "system", Status: rollbackStatusExpired, Message: "rollback backup expired"})
			return nil
		})
		return nil
	})
}

func (s *fileRollbackStore) mutateRecord(id string, fn func(*RollbackRecord, time.Time) error) (RollbackRecord, error) {
	unlock, err := s.acquireRecordLock(id, "mutate")
	if err != nil {
		return RollbackRecord{}, err
	}
	defer unlock()

	record, err := s.readRecord(id)
	if err != nil {
		return RollbackRecord{}, err
	}
	now := time.Now()
	if err := fn(&record, now); err != nil {
		if writeErr := s.writeRecord(record); writeErr != nil {
			klog.Errorf("Failed to persist rollback record %s after state error %v: %v", id, err, writeErr)
		}
		return record, err
	}
	if err := s.writeRecord(record); err != nil {
		return RollbackRecord{}, err
	}
	s.writeEvent(id, record.LastAction, record.LastClickedBy, record.ExecutionStatus)
	return record, nil
}

func (s *fileRollbackStore) readRecord(id string) (RollbackRecord, error) {
	payload, err := os.ReadFile(s.recordPath(id))
	if err != nil {
		if os.IsNotExist(err) {
			return RollbackRecord{}, errRollbackNotFound
		}
		return RollbackRecord{}, err
	}
	var record RollbackRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		return RollbackRecord{}, err
	}
	if strings.TrimSpace(record.ExecutionStatus) == "" {
		record.ExecutionStatus = rollbackStatusPending
	}
	return record, nil
}

func (s *fileRollbackStore) writeRecord(record RollbackRecord) error {
	payload, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	return atomicWriteFile(s.recordPath(record.ID), payload, 0o600, s.fsyncEnabled)
}

func (s *fileRollbackStore) acquireRecordLock(id string, operation string) (func(), error) {
	path := s.lockPath(id)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(s.lockTTL)
	for {
		lockPayload := map[string]string{
			"pod":        strings.TrimSpace(os.Getenv("POD_NAME")),
			"operation":  operation,
			"created_at": time.Now().Format(time.RFC3339Nano),
		}
		if lockPayload["pod"] == "" {
			lockPayload["pod"] = "unknown-pod"
		}
		encoded, _ := json.Marshal(lockPayload)
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			_, _ = f.Write(encoded)
			_ = f.Close()
			return func() { _ = os.Remove(path) }, nil
		}
		if !os.IsExist(err) {
			return nil, err
		}
		if info, statErr := os.Stat(path); statErr == nil && time.Since(info.ModTime()) > s.lockTTL {
			_ = os.Remove(path)
			continue
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timed out waiting for rollback record lock %s", id)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func atomicWriteFile(path string, data []byte, perm os.FileMode, fsyncEnabled bool) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if fsyncEnabled {
		if err := tmp.Sync(); err != nil {
			_ = tmp.Close()
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, perm); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	if fsyncEnabled {
		dirFD, err := os.Open(dir)
		if err == nil {
			_ = dirFD.Sync()
			_ = dirFD.Close()
		}
	}
	return nil
}

func appendRollbackHistory(history []RollbackHistoryItem, item RollbackHistoryItem) []RollbackHistoryItem {
	if item.At.IsZero() {
		item.At = time.Now()
	}
	history = append(history, item)
	if len(history) > 100 {
		return history[len(history)-100:]
	}
	return history
}

func (s *fileRollbackStore) writeEvent(id string, action string, by string, status string) {
	if strings.TrimSpace(action) == "" {
		return
	}
	day := time.Now().UTC().Format("2006-01-02")
	dir := filepath.Join(s.rootDir, rollbackFileEventsDir, day)
	_ = os.MkdirAll(dir, 0o755)
	name := fmt.Sprintf("%s-%s-%d.json", time.Now().UTC().Format("20060102T150405.000000000Z"), safeFileNamePart(strings.TrimSpace(os.Getenv("POD_NAME"))), os.Getpid())
	payload, _ := json.Marshal(map[string]string{
		"at":        time.Now().UTC().Format(time.RFC3339Nano),
		"record_id": id,
		"action":    action,
		"by":        by,
		"status":    status,
	})
	_ = atomicWriteFile(filepath.Join(dir, name), payload, 0o600, s.fsyncEnabled)
}
