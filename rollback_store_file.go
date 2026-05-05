package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	fileRollbackRecordsDir   = "records"
	fileRollbackManifestsDir = "manifests"
	fileRollbackLocksDir     = "locks"
	fileRollbackEventsDir    = "events"
	fileRollbackTmpDir       = "tmp"
)

type fileRollbackStore struct {
	rootDir      string
	lockTTL      time.Duration
	fsyncEnabled bool
}

type fileLockHandle struct {
	path  string
	token string
}

func newFileRollbackStore(cfg RollbackConfig) (*fileRollbackStore, error) {
	root := rollbackRuntimeDirectory(cfg)
	store := &fileRollbackStore{
		rootDir:      root,
		lockTTL:      time.Duration(cfg.Storage.LockTTLSeconds) * time.Second,
		fsyncEnabled: rollbackWriteFsyncEnabled(cfg),
	}

	for _, dir := range []string{
		fileRollbackRecordsDir,
		fileRollbackManifestsDir,
		fileRollbackLocksDir,
		fileRollbackEventsDir,
		fileRollbackTmpDir,
	} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
			return nil, fmt.Errorf("failed to create rollback file store directory '%s': %w", dir, err)
		}
	}

	return store, nil
}

func (s *fileRollbackStore) Type() string { return rollbackStorageFile }

func (s *fileRollbackStore) SaveRecord(record RollbackRecord, manifestYAML string) error {
	record = normalizeRollbackRecord(record)
	if strings.TrimSpace(record.ID) == "" {
		return fmt.Errorf("rollback record id is empty")
	}
	if strings.TrimSpace(manifestYAML) == "" {
		return fmt.Errorf("rollback manifest yaml is empty")
	}

	lock, err := s.acquireRecordLock(record.ID, "save")
	if err != nil {
		return err
	}
	defer lock.Release()

	if _, err := os.Stat(s.recordPath(record.ID)); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}

	sum := sha256.Sum256([]byte(manifestYAML))
	record.ManifestSHA256 = hex.EncodeToString(sum[:])
	record.ManifestFile = s.relativeManifestPath(record.ID)
	record.ManifestYAML = ""
	record.UpdatedAt = time.Now()
	record.History = append(record.History, rollbackHistory("created", "system", record.ExecutionStatus, "rollback backup recorded"))

	if err := atomicWriteFile(s.manifestPath(record.ID), []byte(manifestYAML), 0o600, s.fsyncEnabled); err != nil {
		return fmt.Errorf("failed to write rollback manifest: %w", err)
	}

	payload, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	if err := atomicWriteFile(s.recordPath(record.ID), payload, 0o600, s.fsyncEnabled); err != nil {
		return fmt.Errorf("failed to write rollback record: %w", err)
	}

	s.writeEvent(record.ID, "created", "system", record.ExecutionStatus, "rollback backup recorded", 0, 0)
	return nil
}

func (s *fileRollbackStore) LoadRecord(id string) (RollbackRecord, string, error) {
	record, manifest, err := s.readRecordAndManifest(id)
	if err != nil {
		return RollbackRecord{}, "", err
	}
	if time.Now().After(record.ExpiresAt) {
		_ = s.markExpired(id)
		return RollbackRecord{}, "", ErrRollbackExpired
	}
	return record, manifest, nil
}

func (s *fileRollbackStore) MarkRunning(id string, telegramID string, allowReapply bool) (RollbackRecord, string, error) {
	return s.updateRecord(id, func(record *RollbackRecord) error {
		if time.Now().After(record.ExpiresAt) {
			record.ExecutionStatus = rollbackStatusExpired
			return ErrRollbackExpired
		}
		if record.ExecutionStatus == rollbackStatusApplied && !allowReapply {
			return ErrRollbackAlreadyApplied
		}
		record.RollbackClickCount++
		record.LastClickedAt = time.Now()
		record.LastClickedBy = telegramID
		record.LastAction = rollbackActionApply
		record.ExecutionStatus = rollbackStatusRunning
		record.ExecutionError = ""
		record.UpdatedAt = time.Now()
		record.History = append(record.History, rollbackHistory("rollback_apply_clicked", telegramID, rollbackStatusRunning, "rollback execution started"))
		return nil
	})
}

func (s *fileRollbackStore) MarkApplied(id string, telegramID string) (RollbackRecord, string, error) {
	return s.updateRecord(id, func(record *RollbackRecord) error {
		now := time.Now()
		record.ExecutionStatus = rollbackStatusApplied
		record.ExecutionError = ""
		record.ExecutedAt = now
		record.ExecutedBy = telegramID
		record.LastClickedAt = now
		record.LastClickedBy = telegramID
		record.LastAction = rollbackActionApply
		record.UpdatedAt = now
		record.History = append(record.History, rollbackHistory("rollback_applied", telegramID, rollbackStatusApplied, "rollback applied successfully"))
		return nil
	})
}

func (s *fileRollbackStore) MarkFailed(id string, telegramID string, message string) (RollbackRecord, string, error) {
	return s.updateRecord(id, func(record *RollbackRecord) error {
		now := time.Now()
		record.ExecutionStatus = rollbackStatusFailed
		record.ExecutionError = message
		record.ExecutedAt = now
		record.ExecutedBy = telegramID
		record.LastClickedAt = now
		record.LastClickedBy = telegramID
		record.LastAction = rollbackActionApply
		record.UpdatedAt = now
		record.History = append(record.History, rollbackHistory("rollback_failed", telegramID, rollbackStatusFailed, message))
		return nil
	})
}

func (s *fileRollbackStore) IncrementDownload(id string, telegramID string) (RollbackRecord, string, error) {
	return s.updateRecord(id, func(record *RollbackRecord) error {
		if time.Now().After(record.ExpiresAt) {
			record.ExecutionStatus = rollbackStatusExpired
			return ErrRollbackExpired
		}
		now := time.Now()
		record.DownloadClickCount++
		record.LastClickedAt = now
		record.LastClickedBy = telegramID
		record.LastAction = rollbackActionDownload
		record.UpdatedAt = now
		record.History = append(record.History, rollbackHistory("yaml_download_clicked", telegramID, record.ExecutionStatus, "rollback yaml download requested"))
		return nil
	})
}

func (s *fileRollbackStore) AddTelegramMessage(id string, msg telegramSentMessage, kind string) error {
	_, _, err := s.updateRecord(id, func(record *RollbackRecord) error {
		record.TelegramMessages = append(record.TelegramMessages, RollbackTelegramMessage{
			ChatID:    msg.ChatID,
			MessageID: msg.MessageID,
			Kind:      kind,
		})
		record.UpdatedAt = time.Now()
		record.History = append(record.History, RollbackHistoryItem{
			At:        time.Now(),
			Action:    "telegram_message_added",
			By:        "system",
			Status:    record.ExecutionStatus,
			ChatID:    msg.ChatID,
			MessageID: msg.MessageID,
		})
		return nil
	})
	return err
}

func (s *fileRollbackStore) CleanupExpired(now time.Time) error {
	root := filepath.Join(s.rootDir, fileRollbackRecordsDir)
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d == nil || d.IsDir() || !strings.HasSuffix(path, ".json") {
			return nil
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return nil
		}

		var record RollbackRecord
		if err := json.Unmarshal(data, &record); err != nil {
			return nil
		}
		if record.ID == "" || now.Before(record.ExpiresAt) {
			return nil
		}

		lock, err := s.acquireRecordLock(record.ID, "gc")
		if err != nil {
			return nil
		}
		defer lock.Release()

		_ = os.Remove(s.manifestPath(record.ID))
		_ = os.Remove(s.recordPath(record.ID))
		return nil
	})
}

func (s *fileRollbackStore) updateRecord(id string, mutate func(*RollbackRecord) error) (RollbackRecord, string, error) {
	lock, err := s.acquireRecordLock(id, "update")
	if err != nil {
		return RollbackRecord{}, "", err
	}
	defer lock.Release()

	record, manifest, err := s.readRecordAndManifest(id)
	if err != nil {
		return RollbackRecord{}, "", err
	}

	if err := mutate(&record); err != nil {
		payload, _ := json.MarshalIndent(record, "", "  ")
		_ = atomicWriteFile(s.recordPath(id), payload, 0o600, s.fsyncEnabled)
		return record, manifest, err
	}

	payload, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return RollbackRecord{}, "", err
	}
	if err := atomicWriteFile(s.recordPath(id), payload, 0o600, s.fsyncEnabled); err != nil {
		return RollbackRecord{}, "", err
	}

	s.writeEvent(id, record.LastAction, record.LastClickedBy, record.ExecutionStatus, record.ExecutionError, 0, 0)
	return record, manifest, nil
}

func (s *fileRollbackStore) readRecordAndManifest(id string) (RollbackRecord, string, error) {
	data, err := os.ReadFile(s.recordPath(id))
	if err != nil {
		if os.IsNotExist(err) {
			return RollbackRecord{}, "", ErrRollbackNotFound
		}
		return RollbackRecord{}, "", err
	}

	var record RollbackRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return RollbackRecord{}, "", err
	}

	manifest := record.ManifestYAML
	if strings.TrimSpace(manifest) == "" {
		manifestPath := s.manifestPath(id)
		if strings.TrimSpace(record.ManifestFile) != "" {
			manifestPath = filepath.Join(s.rootDir, filepath.FromSlash(strings.TrimPrefix(record.ManifestFile, "rollback/")))
			if !strings.HasPrefix(filepath.Clean(manifestPath), filepath.Clean(s.rootDir)) {
				return RollbackRecord{}, "", fmt.Errorf("invalid manifest_file path")
			}
		}
		manifestBytes, err := os.ReadFile(manifestPath)
		if err != nil {
			return RollbackRecord{}, "", err
		}
		manifest = string(manifestBytes)
	}

	return record, manifest, nil
}

func (s *fileRollbackStore) markExpired(id string) error {
	_, _, err := s.updateRecord(id, func(record *RollbackRecord) error {
		record.ExecutionStatus = rollbackStatusExpired
		record.UpdatedAt = time.Now()
		record.History = append(record.History, rollbackHistory("expired", "system", rollbackStatusExpired, "rollback backup expired"))
		return nil
	})
	if errorsIs(err, ErrRollbackExpired) {
		return nil
	}
	return err
}

func (s *fileRollbackStore) recordPath(id string) string {
	return filepath.Join(s.rootDir, fileRollbackRecordsDir, id[0:2], id[2:4], id+".json")
}

func (s *fileRollbackStore) manifestPath(id string) string {
	return filepath.Join(s.rootDir, fileRollbackManifestsDir, id[0:2], id[2:4], id+".yaml")
}

func (s *fileRollbackStore) relativeManifestPath(id string) string {
	return filepath.ToSlash(filepath.Join("rollback", fileRollbackManifestsDir, id[0:2], id[2:4], id+".yaml"))
}

func (s *fileRollbackStore) lockPath(id string) string {
	return filepath.Join(s.rootDir, fileRollbackLocksDir, id[0:2], id[2:4], id+".lock")
}

func (s *fileRollbackStore) acquireRecordLock(id string, operation string) (*fileLockHandle, error) {
	if len(id) < 4 {
		return nil, fmt.Errorf("invalid rollback id '%s'", id)
	}

	lockPath := s.lockPath(id)
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
		return nil, err
	}

	host, _ := os.Hostname()
	token := fmt.Sprintf("%s-%d-%d", host, os.Getpid(), time.Now().UnixNano())
	payload := map[string]interface{}{
		"token":      token,
		"pod":        host,
		"pid":        os.Getpid(),
		"operation":  operation,
		"created_at": time.Now().UTC(),
		"expires_at": time.Now().Add(s.lockTTL).UTC(),
	}
	data, _ := json.Marshal(payload)

	for i := 0; i < 2; i++ {
		fd, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			if _, writeErr := fd.Write(data); writeErr != nil {
				_ = fd.Close()
				_ = os.Remove(lockPath)
				return nil, writeErr
			}
			if s.fsyncEnabled {
				_ = fd.Sync()
			}
			if closeErr := fd.Close(); closeErr != nil {
				_ = os.Remove(lockPath)
				return nil, closeErr
			}
			return &fileLockHandle{path: lockPath, token: token}, nil
		}
		if !os.IsExist(err) {
			return nil, err
		}

		if s.isLockExpired(lockPath) {
			_ = os.Remove(lockPath)
			continue
		}
		return nil, ErrRollbackLocked
	}
	return nil, ErrRollbackLocked
}

func (s *fileRollbackStore) isLockExpired(path string) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return true
	}
	var payload struct {
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return true
	}
	return payload.ExpiresAt.IsZero() || time.Now().After(payload.ExpiresAt)
}

func (h *fileLockHandle) Release() {
	if h == nil || h.path == "" {
		return
	}
	data, err := os.ReadFile(h.path)
	if err != nil {
		return
	}
	var payload struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(data, &payload); err == nil && payload.Token == h.token {
		_ = os.Remove(h.path)
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
		if dirFD, err := os.Open(dir); err == nil {
			_ = dirFD.Sync()
			_ = dirFD.Close()
		}
	}
	return nil
}

func (s *fileRollbackStore) writeEvent(id string, action string, by string, status string, message string, chatID int64, messageID int) {
	date := time.Now().UTC().Format("2006-01-02")
	host, _ := os.Hostname()
	name := fmt.Sprintf("%s-%s-%d.json", time.Now().UTC().Format("20060102T150405.000000000Z"), host, time.Now().UnixNano())
	path := filepath.Join(s.rootDir, fileRollbackEventsDir, date, name)
	event := RollbackHistoryItem{
		At:        time.Now().UTC(),
		Action:    action,
		By:        by,
		Status:    status,
		Message:   message,
		Pod:       host,
		ChatID:    chatID,
		MessageID: messageID,
	}
	payload := map[string]interface{}{
		"record_id": id,
		"event":     event,
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return
	}
	_ = atomicWriteFile(path, data, 0o600, s.fsyncEnabled)
}

func errorsIs(err error, target error) bool {
	if err == nil {
		return false
	}
	if err == target {
		return true
	}
	return strings.Contains(err.Error(), target.Error())
}

func parseInt64(value string) int64 {
	i, _ := strconv.ParseInt(value, 10, 64)
	return i
}
