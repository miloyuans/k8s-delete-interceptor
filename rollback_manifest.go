package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

func rollbackManifestRelativePath(id string) string {
	return filepath.ToSlash(filepath.Join("rollback", "manifests", safeFileName(id)+".yaml"))
}

func rollbackBackupYAML(rb *RollbackBackup) (string, error) {
	if rb == nil {
		return "", fmt.Errorf("rollback backup is nil")
	}
	if len(rb.SourceObject) == 0 {
		return "", fmt.Errorf("rollback source object is empty")
	}
	var obj map[string]any
	if err := json.Unmarshal(rb.SourceObject, &obj); err != nil {
		return "", err
	}
	cleanupRollbackObject(obj)
	if isScaleRollbackBackup(rb) {
		normalizeScaleRollbackObject(obj)
	}
	b, err := yaml.Marshal(obj)
	if err != nil {
		return "", err
	}
	y := strings.TrimSpace(string(b)) + "\n"
	if rb.Mode == RollbackDeleteCreatedObject {
		return "# Rollback mode: delete_created_object\n# Use: kubectl delete -f this-file.yaml\n" + y, nil
	}
	if isScaleRollbackBackup(rb) {
		return "# Rollback mode: restore_old_object\n# Target: parent resource subresource /scale\n# Web execute/dry-run uses the Kubernetes scale subresource directly.\n# For manual rollback, prefer: kubectl scale " + strings.TrimSpace(rb.Resource) + " " + strings.TrimSpace(rb.Name) + " -n " + strings.TrimSpace(rb.Namespace) + " --replicas=<spec.replicas from this file>\n" + y, nil
	}
	return "# Rollback mode: restore_old_object\n# Use: kubectl apply --server-side --force-conflicts -f this-file.yaml\n" + y, nil
}

func (s *LocalStore) SaveRollbackManifest(rb *RollbackBackup) error {
	if s == nil || rb == nil || strings.TrimSpace(rb.ID) == "" {
		return nil
	}
	yml, err := rollbackBackupYAML(rb)
	if err != nil {
		return err
	}
	rel := rollbackManifestRelativePath(rb.ID)
	rb.ManifestFile = rel
	rb.ManifestSHA256 = sha256Hex([]byte(yml))
	finalPath := filepath.Join(s.root, "rollback", "manifests", safeFileName(rb.ID)+".yaml")
	if err := os.MkdirAll(filepath.Dir(finalPath), 0755); err != nil {
		return err
	}
	tmpPath := filepath.Join(s.root, "tmp", safeFileName(rb.ID)+fmt.Sprintf(".%d.rollback-yaml.tmp", time.Now().UnixNano()))
	if err := os.WriteFile(tmpPath, []byte(yml), 0600); err != nil {
		return err
	}
	if err := fsyncFile(tmpPath); err != nil {
		return err
	}
	return os.Rename(tmpPath, finalPath)
}
