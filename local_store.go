package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type LocalStore struct{ root string }

type ConfigMeta struct {
	Version   int64     `json:"version"`
	File      string    `json:"file"`
	Hash      string    `json:"hash"`
	Source    string    `json:"source"`
	UpdatedAt time.Time `json:"updated_at"`
}

func NewLocalStore(root string) (*LocalStore, error) {
	if root == "" {
		root = "/var/lib/k8s-delete-interceptor"
	}
	dirs := []string{
		"config/versions", "config/lock", "spool/admission-events/pending", "spool/admission-events/processing", "spool/admission-events/synced", "spool/admission-events/failed",
		"rollback/backups", "rollback/jobs", "rollback/locks", "approvals/pending", "approvals/decided", "tmp",
	}
	for _, d := range dirs {
		if err := os.MkdirAll(filepath.Join(root, d), 0755); err != nil {
			return nil, err
		}
	}
	return &LocalStore{root: root}, nil
}

func (s *LocalStore) Root() string { return s.root }

func (s *LocalStore) LoadLatestConfig() (*RuntimeConfig, error) {
	metaPath := filepath.Join(s.root, "config/current.meta.json")
	b, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, err
	}
	var meta ConfigMeta
	if err := json.Unmarshal(b, &meta); err != nil {
		return nil, err
	}
	cfgPath := filepath.Join(s.root, "config", meta.File)
	cb, err := os.ReadFile(cfgPath)
	if err != nil {
		return nil, err
	}
	if meta.Hash != "" && sha256Hex(cb) != meta.Hash {
		return nil, fmt.Errorf("cached config hash mismatch")
	}
	var cfg RuntimeConfig
	if err := json.Unmarshal(cb, &cfg); err != nil {
		return nil, err
	}
	if err := validateRuntimeConfig(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (s *LocalStore) SaveConfig(cfg *RuntimeConfig, source string) error {
	if err := validateRuntimeConfig(cfg); err != nil {
		return err
	}
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	versionFile := fmt.Sprintf("versions/runtime-config-v%d.json", cfg.Version)
	finalPath := filepath.Join(s.root, "config", versionFile)
	tmpPath := filepath.Join(s.root, "tmp", fmt.Sprintf("runtime-config-v%d-%d.tmp", cfg.Version, time.Now().UnixNano()))
	if err := os.WriteFile(tmpPath, b, 0644); err != nil {
		return err
	}
	if err := fsyncFile(tmpPath); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		if errors.Is(err, fs.ErrExist) {
			_ = os.Remove(tmpPath)
		} else {
			return err
		}
	}
	meta := ConfigMeta{Version: cfg.Version, File: versionFile, Hash: sha256Hex(b), Source: source, UpdatedAt: time.Now().UTC()}
	mb, _ := json.MarshalIndent(meta, "", "  ")
	metaTmp := filepath.Join(s.root, "tmp", fmt.Sprintf("current-meta-%d.tmp", time.Now().UnixNano()))
	if err := os.WriteFile(metaTmp, mb, 0644); err != nil {
		return err
	}
	if err := fsyncFile(metaTmp); err != nil {
		return err
	}
	return os.Rename(metaTmp, filepath.Join(s.root, "config/current.meta.json"))
}

func (s *LocalStore) SpoolEvent(ev *AdmissionEvent) error {
	if ev.ID == "" {
		return errors.New("event id is required")
	}
	b, err := json.MarshalIndent(ev, "", "  ")
	if err != nil {
		return err
	}
	name := safeFileName(ev.ID) + ".json"
	tmp := filepath.Join(s.root, "tmp", name+fmt.Sprintf(".%d.tmp", time.Now().UnixNano()))
	pending := filepath.Join(s.root, "spool/admission-events/pending", name)
	if err := os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	if err := fsyncFile(tmp); err != nil {
		return err
	}
	return os.Rename(tmp, pending)
}

func (s *LocalStore) SaveRollback(rb *RollbackBackup) error {
	if rb.ID == "" {
		return errors.New("rollback id is required")
	}
	b, err := json.MarshalIndent(rb, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(s.root, "rollback/backups", safeFileName(rb.ID)+".json")
	tmp := filepath.Join(s.root, "tmp", safeFileName(rb.ID)+fmt.Sprintf(".%d.rollback.tmp", time.Now().UnixNano()))
	if err := os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	if err := fsyncFile(tmp); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (s *LocalStore) GetRollback(id string) (*RollbackBackup, error) {
	b, err := os.ReadFile(filepath.Join(s.root, "rollback/backups", safeFileName(id)+".json"))
	if err != nil {
		return nil, err
	}
	var rb RollbackBackup
	if err := json.Unmarshal(b, &rb); err != nil {
		return nil, err
	}
	return &rb, nil
}

func (s *LocalStore) ListRecentEvents(limit int) ([]AdmissionEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	paths := []string{}
	for _, d := range []string{"pending", "processing", "failed", "synced"} {
		_ = filepath.WalkDir(filepath.Join(s.root, "spool/admission-events", d), func(path string, de fs.DirEntry, err error) error {
			if err == nil && !de.IsDir() && strings.HasSuffix(path, ".json") {
				paths = append(paths, path)
			}
			return nil
		})
	}
	sort.Slice(paths, func(i, j int) bool {
		a, _ := os.Stat(paths[i])
		b, _ := os.Stat(paths[j])
		return a.ModTime().After(b.ModTime())
	})
	if len(paths) > limit {
		paths = paths[:limit]
	}
	out := make([]AdmissionEvent, 0, len(paths))
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var ev AdmissionEvent
		if json.Unmarshal(b, &ev) == nil {
			out = append(out, ev)
		}
	}
	return out, nil
}

func (s *LocalStore) ListRecentEventsByQuery(q EventQuery) ([]AdmissionEvent, error) {
	paths := []string{}
	for _, d := range []string{"pending", "processing", "failed", "synced"} {
		_ = filepath.WalkDir(filepath.Join(s.root, "spool/admission-events", d), func(path string, de fs.DirEntry, err error) error {
			if err == nil && !de.IsDir() && strings.HasSuffix(path, ".json") {
				paths = append(paths, path)
			}
			return nil
		})
	}
	type eventWithModTime struct {
		event AdmissionEvent
		mtime time.Time
	}
	items := make([]eventWithModTime, 0, len(paths))
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var ev AdmissionEvent
		if json.Unmarshal(b, &ev) != nil || !q.Match(ev) {
			continue
		}
		st, _ := os.Stat(p)
		var mt time.Time
		if st != nil {
			mt = st.ModTime()
		}
		items = append(items, eventWithModTime{event: ev, mtime: mt})
	}
	sort.Slice(items, func(i, j int) bool {
		it, jt := items[i].event.Time, items[j].event.Time
		if it.IsZero() {
			it = items[i].mtime
		}
		if jt.IsZero() {
			jt = items[j].mtime
		}
		return it.After(jt)
	})
	limit := q.NormalizedLimit(100)
	if len(items) > limit {
		items = items[:limit]
	}
	out := make([]AdmissionEvent, 0, len(items))
	for _, it := range items {
		out = append(out, it.event)
	}
	return out, nil
}

func (s *LocalStore) FlushEventsToMongo(ctx context.Context, m *MongoStore, batch int) error {
	if m == nil || !m.Healthy() {
		return errors.New("mongo unavailable")
	}
	if batch <= 0 {
		batch = 100
	}
	pendingDir := filepath.Join(s.root, "spool/admission-events/pending")
	processingDir := filepath.Join(s.root, "spool/admission-events/processing")
	syncedDir := filepath.Join(s.root, "spool/admission-events/synced")
	entries, err := os.ReadDir(pendingDir)
	if err != nil {
		return err
	}
	count := 0
	for _, e := range entries {
		if count >= batch {
			break
		}
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		from := filepath.Join(pendingDir, e.Name())
		proc := filepath.Join(processingDir, fmt.Sprintf("%s__%s", envOr("HOSTNAME", "pod"), e.Name()))
		if err := os.Rename(from, proc); err != nil {
			continue
		}
		b, err := os.ReadFile(proc)
		if err != nil {
			_ = os.Rename(proc, from)
			continue
		}
		var ev AdmissionEvent
		if err := json.Unmarshal(b, &ev); err != nil {
			_ = os.Rename(proc, filepath.Join(s.root, "spool/admission-events/failed", e.Name()))
			continue
		}
		if err := m.SaveEvent(ctx, &ev); err != nil {
			_ = os.Rename(proc, from)
			return err
		}
		_ = os.Rename(proc, filepath.Join(syncedDir, e.Name()))
		count++
	}
	return nil
}

func (s *LocalStore) TryLock(name string, ttl time.Duration) (*FileLock, error) {
	lockDir := filepath.Join(s.root, "config/lock", safeFileName(name)+".lock")
	now := time.Now().UTC()
	err := os.Mkdir(lockDir, 0755)
	if err == nil {
		_ = os.WriteFile(filepath.Join(lockDir, "owner.json"), []byte(fmt.Sprintf(`{"owner":"%s","expires_at":"%s"}`, envOr("HOSTNAME", "pod"), now.Add(ttl).Format(time.RFC3339Nano))), 0644)
		return &FileLock{path: lockDir}, nil
	}
	b, rerr := os.ReadFile(filepath.Join(lockDir, "owner.json"))
	if rerr == nil && strings.Contains(string(b), "expires_at") {
		var x struct {
			ExpiresAt time.Time `json:"expires_at"`
		}
		if json.Unmarshal(b, &x) == nil && time.Now().After(x.ExpiresAt) {
			_ = os.RemoveAll(lockDir)
			return s.TryLock(name, ttl)
		}
	}
	return nil, err
}

type FileLock struct{ path string }

func (l *FileLock) Unlock() {
	if l != nil {
		_ = os.RemoveAll(l.path)
	}
}

func sha256Hex(b []byte) string { h := sha256.Sum256(b); return "sha256:" + hex.EncodeToString(h[:]) }
func safeFileName(s string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_", " ", "_", "\n", "_")
	return replacer.Replace(s)
}

func fsyncFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
