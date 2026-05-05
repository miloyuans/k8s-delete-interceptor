package main

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
	v1 "k8s.io/api/admission/v1"
	"k8s.io/klog/v2"
)

const (
	defaultRollbackRetentionHours = 24
	defaultRollbackCollection     = "rollback_backups"
	defaultRollbackFieldManager   = "k8s-delete-interceptor-rollback"
	defaultRollbackApplyTimeout   = 30 * time.Second
	rollbackCallbackPrefix        = "rb"
	rollbackOffsetFileName        = ".rollback-telegram-offset"
	rollbackPollLockFileName      = ".rollback-telegram-poll.lock"
	serviceAccountTokenPath       = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	serviceAccountCAPath          = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

type RollbackConfig struct {
	Enabled               bool                  `json:"enabled" yaml:"enabled"`
	AuthorizedTelegramIDs []string              `json:"authorized_telegram_ids" yaml:"authorized_telegram_ids"`
	RetentionHours        int                   `json:"retention_hours" yaml:"retention_hours"`
	StateDirectory        string                `json:"state_directory" yaml:"state_directory"`
	PollIntervalSeconds   int                   `json:"poll_interval_seconds" yaml:"poll_interval_seconds"`
	FieldManager          string                `json:"field_manager" yaml:"field_manager"`
	AllowReapply          bool                  `json:"allow_reapply" yaml:"allow_reapply"`
	RunningTimeoutSeconds int                   `json:"running_timeout_seconds" yaml:"running_timeout_seconds"`
	Storage               RollbackStorageConfig `json:"storage" yaml:"storage"`
	Mongo                 MongoAuditConfig      `json:"mongo" yaml:"mongo"`
}

type RollbackRecord struct {
	ID              string    `json:"id" bson:"_id"`
	Timestamp       time.Time `json:"timestamp" bson:"timestamp"`
	ExpiresAt       time.Time `json:"expires_at" bson:"expires_at"`
	ClusterName     string    `json:"cluster_name" bson:"cluster_name"`
	RequestUID      string    `json:"request_uid" bson:"request_uid"`
	Username        string    `json:"username" bson:"username"`
	Operation       string    `json:"operation" bson:"operation"`
	Kind            string    `json:"kind" bson:"kind"`
	Resource        string    `json:"resource" bson:"resource"`
	ResourceGroup   string    `json:"resource_group,omitempty" bson:"resource_group,omitempty"`
	ResourceVersion string    `json:"resource_version" bson:"resource_version"`
	Name            string    `json:"name" bson:"name"`
	Namespace       string    `json:"namespace,omitempty" bson:"namespace,omitempty"`
	ResourceDisplay string    `json:"resource_display" bson:"resource_display"`

	ManifestYAML   string `json:"manifest_yaml,omitempty" bson:"manifest_yaml,omitempty"`
	ManifestJSON   string `json:"manifest_json" bson:"manifest_json"`
	ManifestFile   string `json:"manifest_file,omitempty" bson:"manifest_file,omitempty"`
	ManifestSHA256 string `json:"manifest_sha256,omitempty" bson:"manifest_sha256,omitempty"`

	ExecutedAt            time.Time `json:"executed_at,omitempty" bson:"executed_at,omitempty"`
	ExecutedBy            string    `json:"executed_by,omitempty" bson:"executed_by,omitempty"`
	ExecutedByUsername    string    `json:"executed_by_username,omitempty" bson:"executed_by_username,omitempty"`
	ExecutedByDisplayName string    `json:"executed_by_display_name,omitempty" bson:"executed_by_display_name,omitempty"`
	ExecutionStatus       string    `json:"execution_status,omitempty" bson:"execution_status,omitempty"`
	ExecutionError        string    `json:"execution_error,omitempty" bson:"execution_error,omitempty"`

	RollbackClickCount       int       `json:"rollback_click_count" bson:"rollback_click_count"`
	DownloadClickCount       int       `json:"download_click_count" bson:"download_click_count"`
	LastClickedAt            time.Time `json:"last_clicked_at,omitempty" bson:"last_clicked_at,omitempty"`
	LastClickedBy            string    `json:"last_clicked_by,omitempty" bson:"last_clicked_by,omitempty"`
	LastClickedByUsername    string    `json:"last_clicked_by_username,omitempty" bson:"last_clicked_by_username,omitempty"`
	LastClickedByDisplayName string    `json:"last_clicked_by_display_name,omitempty" bson:"last_clicked_by_display_name,omitempty"`
	LastAction               string    `json:"last_action,omitempty" bson:"last_action,omitempty"`

	TelegramMessages []RollbackTelegramMessage `json:"telegram_messages,omitempty" bson:"telegram_messages,omitempty"`
	History          []RollbackHistoryItem     `json:"history,omitempty" bson:"history,omitempty"`
}

type rollbackManager struct {
	enabled       bool
	authorizedIDs []string
	retention     time.Duration
	store         RollbackStore
	storageName   string
	fieldManager  string
	statePath     string
	pollLockPath  string
	pollInterval  time.Duration
	allowReapply  bool
	stopCh        chan struct{}
	mu            sync.Mutex

	k8sClient  *http.Client
	k8sBaseURL string
	k8sToken   string
}

var rollbacker *rollbackManager

func applyRollbackDefaults() {
	if !config.Rollback.Enabled {
		return
	}
	if config.Rollback.RetentionHours <= 0 {
		config.Rollback.RetentionHours = defaultRollbackRetentionHours
	}
	if strings.TrimSpace(config.Rollback.StateDirectory) == "" {
		config.Rollback.StateDirectory = defaultDeleteConfirmationDirectory
	}
	if strings.TrimSpace(config.Rollback.Storage.DataDirectory) == "" {
		config.Rollback.Storage.DataDirectory = config.Rollback.StateDirectory
	}
	if config.Rollback.Storage.LockTTLSeconds <= 0 {
		config.Rollback.Storage.LockTTLSeconds = 60
	}
	config.Rollback.Storage.WriteFsync = !config.Rollback.Storage.DisableFsync
	if strings.TrimSpace(config.Rollback.Storage.Type) == "" {
		config.Rollback.Storage.Type = rollbackStorageTypeAuto
	}
	if config.Rollback.PollIntervalSeconds <= 0 {
		config.Rollback.PollIntervalSeconds = defaultDeleteConfirmationPollSeconds
	}
	if strings.TrimSpace(config.Rollback.FieldManager) == "" {
		config.Rollback.FieldManager = defaultRollbackFieldManager
	}
	if config.Rollback.RunningTimeoutSeconds <= 0 {
		config.Rollback.RunningTimeoutSeconds = 300
	}
}

func newRollbackManager(cfg RollbackConfig) (*rollbackManager, error) {
	if !cfg.Enabled {
		return &rollbackManager{enabled: false}, nil
	}

	directory := strings.TrimSpace(cfg.Storage.DataDirectory)
	if directory == "" {
		directory = strings.TrimSpace(cfg.StateDirectory)
	}
	if directory == "" {
		directory = defaultDeleteConfirmationDirectory
	}
	if err := os.MkdirAll(filepath.Join(directory, "rollback"), 0o755); err != nil {
		return nil, fmt.Errorf("failed to create rollback state directory '%s': %w", directory, err)
	}

	retentionHours := cfg.RetentionHours
	if retentionHours <= 0 {
		retentionHours = defaultRollbackRetentionHours
	}
	connectTimeoutSeconds := defaultMongoConnectTimeout
	runningTimeout := time.Duration(cfg.RunningTimeoutSeconds) * time.Second

	storageType := normalizeRollbackStorageType(cfg.Storage.Type)
	var store RollbackStore
	var storeErr error

	if storageType == rollbackStorageTypeMongo || storageType == rollbackStorageTypeAuto {
		if mongoCfg, ok := resolveRollbackMongoConfig(cfg); ok {
			if mongoCfg.ConnectTimeoutSeconds > 0 {
				connectTimeoutSeconds = mongoCfg.ConnectTimeoutSeconds
			}
			timeout := time.Duration(connectTimeoutSeconds) * time.Second
			store, storeErr = newMongoRollbackStore(mongoCfg, timeout, cfg.AllowReapply, runningTimeout)
			if storeErr != nil {
				if storageType == rollbackStorageTypeMongo {
					return nil, fmt.Errorf("rollback mongo storage requested but unavailable: %w", storeErr)
				}
				klog.Warningf("Rollback MongoDB storage unavailable, falling back to PVC/file storage: %v", storeErr)
			} else {
				klog.Infof("Rollback storage backend selected: mongo")
			}
		} else if storageType == rollbackStorageTypeMongo {
			return nil, fmt.Errorf("rollback mongo storage requested but no mongo configuration is available")
		}
	}

	if store == nil {
		lockTTL := time.Duration(cfg.Storage.LockTTLSeconds) * time.Second
		store, storeErr = newFileRollbackStore(directory, lockTTL, cfg.Storage.WriteFsync, cfg.AllowReapply, runningTimeout)
		if storeErr != nil {
			return nil, fmt.Errorf("failed to initialize rollback file storage: %w", storeErr)
		}
		klog.Infof("Rollback storage backend selected: file, directory=%s", filepath.Join(directory, "rollback"))
	}

	manager := &rollbackManager{
		enabled:       true,
		authorizedIDs: normalizeTelegramIDs(cfg.AuthorizedTelegramIDs),
		retention:     time.Duration(retentionHours) * time.Hour,
		store:         store,
		storageName:   store.BackendName(),
		fieldManager:  strings.TrimSpace(cfg.FieldManager),
		statePath:     filepath.Join(directory, "rollback", rollbackOffsetFileName),
		pollLockPath:  filepath.Join(directory, "rollback", rollbackPollLockFileName),
		pollInterval:  time.Duration(cfg.PollIntervalSeconds) * time.Second,
		allowReapply:  cfg.AllowReapply,
		stopCh:        make(chan struct{}),
	}
	if manager.fieldManager == "" {
		manager.fieldManager = defaultRollbackFieldManager
	}
	if manager.pollInterval <= 0 {
		manager.pollInterval = time.Duration(defaultDeleteConfirmationPollSeconds) * time.Second
	}
	if err := manager.store.CleanupExpired(time.Now()); err != nil {
		klog.Errorf("Rollback storage cleanup failed during startup: %v", err)
	}

	return manager, nil
}

func resolveRollbackMongoConfig(cfg RollbackConfig) (MongoAuditConfig, bool) {
	if cfg.Mongo.Enabled {
		mongoCfg := cfg.Mongo
		if strings.TrimSpace(mongoCfg.Collection) == "" {
			mongoCfg.Collection = defaultRollbackCollection
		}
		return mongoCfg, true
	}
	if config.Audit.Mongo.Enabled {
		mongoCfg := config.Audit.Mongo
		mongoCfg.Collection = defaultRollbackCollection
		return mongoCfg, true
	}
	return MongoAuditConfig{}, false
}

func (m *rollbackManager) Start() {
	if m == nil || !m.enabled || m.stopCh == nil {
		return
	}
	go m.run()
}

func (m *rollbackManager) Stop() {
	if m == nil || !m.enabled || m.stopCh == nil {
		return
	}
	close(m.stopCh)
}

func (m *rollbackManager) run() {
	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.pollTelegramCallbacks()
			if err := m.store.CleanupExpired(time.Now()); err != nil {
				klog.Errorf("Rollback cleanup failed: %v", err)
			}
		case <-m.stopCh:
			return
		}
	}
}

func recordRollbackBackupForAdmission(req *v1.AdmissionRequest) string {
	if rollbacker == nil || !rollbacker.enabled {
		return ""
	}

	id, err := rollbacker.RecordAdmission(req)
	if err != nil {
		klog.Errorf("Failed to record rollback backup for request %s: %v", req.UID, err)
		return ""
	}
	return id
}

func (m *rollbackManager) RecordAdmission(req *v1.AdmissionRequest) (string, error) {
	if m == nil || !m.enabled || req == nil {
		return "", nil
	}

	raw := rollbackSourceObject(req)
	if len(raw) == 0 {
		return "", nil
	}

	manifestYAML, manifestJSON, err := buildRollbackManifest(req, raw)
	if err != nil {
		return "", err
	}

	now := time.Now()
	id := rollbackRecordID(req, manifestJSON)
	record := RollbackRecord{
		ID:              id,
		Timestamp:       now,
		ExpiresAt:       now.Add(m.retention),
		ClusterName:     config.ClusterName,
		RequestUID:      string(req.UID),
		Username:        req.UserInfo.Username,
		Operation:       string(req.Operation),
		Kind:            req.Kind.Kind,
		Resource:        req.Resource.Resource,
		ResourceGroup:   req.Resource.Group,
		ResourceVersion: req.Resource.Version,
		Name:            req.Name,
		Namespace:       req.Namespace,
		ResourceDisplay: formatResource(req.Kind.Kind, req.Name, req.Namespace),
		ManifestYAML:    manifestYAML,
		ManifestJSON:    manifestJSON,
		ExecutionStatus: rollbackStatusPending,
	}

	if err := m.store.SaveRecord(record, manifestYAML); err != nil {
		return "", err
	}
	return id, nil
}

func rollbackSourceObject(req *v1.AdmissionRequest) []byte {
	switch req.Operation {
	case v1.Update, v1.Delete:
		return req.OldObject.Raw
	default:
		return nil
	}
}

func rollbackRecordID(req *v1.AdmissionRequest, manifestJSON string) string {
	parts := []string{
		config.ClusterName,
		string(req.UID),
		string(req.Operation),
		req.Resource.Group,
		req.Resource.Version,
		req.Resource.Resource,
		req.Namespace,
		req.Name,
		manifestJSON,
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(sum[:])[:24]
}

func buildRollbackManifest(req *v1.AdmissionRequest, raw []byte) (string, string, error) {
	var obj map[string]interface{}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return "", "", fmt.Errorf("failed to decode old object: %w", err)
	}
	if len(obj) == 0 {
		return "", "", fmt.Errorf("old object is empty")
	}

	ensureRollbackObjectIdentity(req, obj)
	sanitizeRollbackObject(obj)

	jsonPayload, err := json.Marshal(obj)
	if err != nil {
		return "", "", fmt.Errorf("failed to encode sanitized rollback object json: %w", err)
	}

	yamlPayload, err := yaml.Marshal(obj)
	if err != nil {
		return "", "", fmt.Errorf("failed to encode sanitized rollback object yaml: %w", err)
	}

	return string(yamlPayload), string(jsonPayload), nil
}

func ensureRollbackObjectIdentity(req *v1.AdmissionRequest, obj map[string]interface{}) {
	if _, ok := obj["apiVersion"]; !ok {
		if req.Kind.Group == "" {
			obj["apiVersion"] = req.Kind.Version
		} else {
			obj["apiVersion"] = req.Kind.Group + "/" + req.Kind.Version
		}
	}
	if _, ok := obj["kind"]; !ok {
		obj["kind"] = req.Kind.Kind
	}

	metadata, ok := obj["metadata"].(map[string]interface{})
	if !ok {
		metadata = map[string]interface{}{}
		obj["metadata"] = metadata
	}
	if strings.TrimSpace(fmt.Sprint(metadata["name"])) == "" {
		metadata["name"] = req.Name
	}
	if strings.TrimSpace(req.Namespace) != "" {
		metadata["namespace"] = req.Namespace
	}
}

func sanitizeRollbackObject(obj map[string]interface{}) {
	delete(obj, "status")

	metadata, _ := obj["metadata"].(map[string]interface{})
	removeMapKeys(metadata,
		"uid",
		"resourceVersion",
		"generation",
		"creationTimestamp",
		"deletionTimestamp",
		"deletionGracePeriodSeconds",
		"managedFields",
		"selfLink",
	)
	if annotations, ok := metadata["annotations"].(map[string]interface{}); ok {
		delete(annotations, "kubectl.kubernetes.io/last-applied-configuration")
		if len(annotations) == 0 {
			delete(metadata, "annotations")
		}
	}

	kind := strings.ToLower(strings.TrimSpace(fmt.Sprint(obj["kind"])))
	spec, _ := obj["spec"].(map[string]interface{})
	switch kind {
	case "service":
		if clusterIP, ok := spec["clusterIP"].(string); !ok || clusterIP != "None" {
			delete(spec, "clusterIP")
		}
		removeMapKeys(spec, "clusterIPs", "ipFamilies", "ipFamilyPolicy", "healthCheckNodePort")
	case "persistentvolumeclaim":
		delete(spec, "volumeName")
	}
}

func removeMapKeys(values map[string]interface{}, keys ...string) {
	if values == nil {
		return
	}
	for _, key := range keys {
		delete(values, key)
	}
}

func buildRollbackReplyMarkup(rollbackID string) string {
	return buildRollbackReplyMarkupForRecord(rollbackID)
}

func buildRollbackReplyMarkupForRecord(rollbackID string) string {
	rollbackID = strings.TrimSpace(rollbackID)
	if rollbackID == "" {
		return ""
	}

	payload := map[string]interface{}{
		"inline_keyboard": [][]map[string]string{
			{
				{"text": "执行回滚", "callback_data": rollbackCallbackDataWithAction(rollbackActionApply, rollbackID)},
				{"text": "下载 YAML", "callback_data": rollbackCallbackDataWithAction(rollbackActionDownload, rollbackID)},
			},
		},
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		klog.Errorf("Failed to build rollback reply markup: %v", err)
		return ""
	}
	return string(encoded)
}

func rollbackCallbackData(rollbackID string) string {
	return rollbackCallbackDataWithAction(rollbackActionApply, rollbackID)
}

func rollbackCallbackDataWithAction(action string, rollbackID string) string {
	action = strings.TrimSpace(action)
	rollbackID = strings.TrimSpace(rollbackID)
	if action == "" {
		action = rollbackActionApply
	}
	return fmt.Sprintf("%s:%s:%s", rollbackCallbackPrefix, action, rollbackID)
}

func parseRollbackCallbackData(data string) (string, string, bool) {
	parts := strings.Split(data, ":")
	if len(parts) == 2 && parts[0] == rollbackCallbackPrefix && strings.TrimSpace(parts[1]) != "" {
		return rollbackActionApply, strings.TrimSpace(parts[1]), true
	}
	if len(parts) != 3 || parts[0] != rollbackCallbackPrefix {
		return "", "", false
	}
	action := strings.TrimSpace(parts[1])
	rollbackID := strings.TrimSpace(parts[2])
	if rollbackID == "" {
		return "", "", false
	}
	if action != rollbackActionApply && action != rollbackActionDownload {
		return "", "", false
	}
	return action, rollbackID, true
}

func (m *rollbackManager) HandleTelegramCallback(callback telegramCallbackQuery) bool {
	action, rollbackID, ok := parseRollbackCallbackData(callback.Data)
	if !ok {
		return false
	}

	if m == nil || !m.enabled {
		answerTelegramCallback(callback.ID, "Rollback is not enabled.")
		return true
	}

	actor := rollbackActorFromTelegramUser(callback.From)
	telegramID := actor.ID
	if !m.isAuthorized(telegramID) {
		answerTelegramCallback(callback.ID, "You are not allowed to execute rollback.")
		return true
	}

	switch action {
	case rollbackActionDownload:
		m.handleRollbackYAMLDownload(callback, rollbackID, actor)
	case rollbackActionApply:
		m.handleRollbackApply(callback, rollbackID, actor)
	}
	return true
}

func (m *rollbackManager) handleRollbackYAMLDownload(callback telegramCallbackQuery, rollbackID string, actor RollbackActor) {
	loaded, err := m.store.IncrementDownload(rollbackID, actor)
	if err != nil {
		m.answerRollbackStoreError(callback.ID, err)
		return
	}
	if strings.TrimSpace(loaded.ManifestYAML) == "" {
		answerTelegramCallback(callback.ID, "Rollback YAML is empty.")
		return
	}

	if callback.Message != nil {
		if err := editTelegramMessageTextWithMarkup(callback.Message.Chat.ID, callback.Message.MessageID, buildRollbackStatusMessage(loaded.Record), buildRollbackReplyMarkupForRecord(rollbackID)); err != nil {
			klog.Errorf("Failed to update rollback message after YAML download click: %v", err)
		}
	}

	if err := sendRollbackYAMLDocumentToUser(config.Telegram.BotToken, callback.From.ID, loaded.Record, loaded.ManifestYAML); err != nil {
		klog.Errorf("Failed to send rollback YAML %s to Telegram user %s: %v", rollbackID, actor.Identifier(), err)
		answerTelegramCallback(callback.ID, "私聊发送失败，请先私聊机器人发送 /start。")
		if callback.Message != nil {
			sendStartBotReminderToGroup(callback.Message.Chat.ID, callback.From)
		}
		return
	}
	answerTelegramCallback(callback.ID, "YAML 已发送到你的私聊。")
}

func (m *rollbackManager) handleRollbackApply(callback telegramCallbackQuery, rollbackID string, actor RollbackActor) {
	loaded, err := m.store.LoadRecordWithManifest(rollbackID)
	if err != nil {
		m.answerRollbackStoreError(callback.ID, err)
		return
	}
	if strings.TrimSpace(loaded.ManifestYAML) == "" {
		answerTelegramCallback(callback.ID, "Rollback manifest is empty.")
		return
	}

	runningRecord, err := m.store.MarkRunning(rollbackID, actor)
	if err != nil {
		m.answerRollbackStoreError(callback.ID, err)
		if callback.Message != nil {
			if current, loadErr := m.store.LoadRecord(rollbackID); loadErr == nil {
				_ = editTelegramMessageTextWithMarkup(callback.Message.Chat.ID, callback.Message.MessageID, buildRollbackStatusMessage(current), buildRollbackReplyMarkupForRecord(rollbackID))
			}
		}
		return
	}
	if callback.Message != nil {
		if err := editTelegramMessageTextWithMarkup(callback.Message.Chat.ID, callback.Message.MessageID, buildRollbackStatusMessage(runningRecord), buildRollbackReplyMarkupForRecord(rollbackID)); err != nil {
			klog.Errorf("Failed to update rollback message to running: %v", err)
		}
	}

	applyRecord := loaded.Record
	applyRecord.ManifestYAML = loaded.ManifestYAML
	if err := m.applyRecord(applyRecord, loaded.ManifestYAML); err != nil {
		klog.Errorf("Failed to apply rollback record %s: %v", rollbackID, err)
		failedRecord, updateErr := m.store.MarkFailed(rollbackID, actor, err.Error())
		if updateErr != nil {
			klog.Errorf("Failed to persist rollback failed state %s: %v", rollbackID, updateErr)
			failedRecord = runningRecord
			failedRecord.ExecutionStatus = rollbackStatusFailed
			failedRecord.ExecutionError = err.Error()
		}
		if callback.Message != nil {
			_ = editTelegramMessageTextWithMarkup(callback.Message.Chat.ID, callback.Message.MessageID, buildRollbackStatusMessage(failedRecord), buildRollbackReplyMarkupForRecord(rollbackID))
		}
		answerTelegramCallback(callback.ID, "Rollback failed. Check service logs.")
		return
	}

	appliedRecord, err := m.store.MarkApplied(rollbackID, actor)
	if err != nil {
		klog.Errorf("Failed to persist rollback applied state %s: %v", rollbackID, err)
		appliedRecord = runningRecord
		appliedRecord.ExecutionStatus = rollbackStatusApplied
		appliedRecord.ExecutedAt = time.Now()
		appliedRecord.ExecutedBy = actor.ID
		appliedRecord.ExecutedByUsername = actor.Username
		appliedRecord.ExecutedByDisplayName = actor.DisplayName
	}
	if callback.Message != nil {
		_ = editTelegramMessageTextWithMarkup(callback.Message.Chat.ID, callback.Message.MessageID, buildRollbackStatusMessage(appliedRecord), buildRollbackReplyMarkupForRecord(rollbackID))
	}
	answerTelegramCallback(callback.ID, "Rollback applied.")
}

func (m *rollbackManager) answerRollbackStoreError(callbackID string, err error) {
	switch err {
	case errRollbackExpired:
		answerTelegramCallback(callbackID, "Rollback backup has expired.")
	case errRollbackAlreadyApplied:
		answerTelegramCallback(callbackID, "该备份已经回滚成功，默认不允许重复执行。")
	case errRollbackAlreadyRunning:
		answerTelegramCallback(callbackID, "该备份正在执行回滚，请不要重复点击。")
	case errRollbackNotFound:
		answerTelegramCallback(callbackID, "Rollback backup does not exist or has expired.")
	default:
		if strings.Contains(err.Error(), errRollbackNotFound.Error()) {
			answerTelegramCallback(callbackID, "Rollback backup does not exist or has expired.")
			return
		}
		klog.Errorf("Rollback operation failed: %v", err)
		answerTelegramCallback(callbackID, "Rollback operation failed. Check service logs.")
	}
}

func (m *rollbackManager) isAuthorized(telegramID string) bool {
	if len(m.authorizedIDs) == 0 {
		return false
	}
	return stringSliceContains(m.authorizedIDs, telegramID)
}

func (m *rollbackManager) applyRecord(record RollbackRecord, manifestYAML string) error {
	if strings.TrimSpace(manifestYAML) == "" {
		manifestYAML = record.ManifestYAML
	}
	if strings.TrimSpace(manifestYAML) == "" {
		return fmt.Errorf("rollback manifest is empty")
	}
	if err := m.ensureKubernetesClient(); err != nil {
		return err
	}

	apiURL, err := m.rollbackApplyURL(record)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPatch, apiURL, bytes.NewBufferString(manifestYAML))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+m.k8sToken)
	req.Header.Set("Content-Type", "application/apply-patch+yaml")
	req.Header.Set("Accept", "application/json")

	resp, err := m.k8sClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("kubernetes apply status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return nil
}

func (m *rollbackManager) ensureKubernetesClient() error {
	if m.k8sClient != nil && m.k8sBaseURL != "" && m.k8sToken != "" {
		return nil
	}

	host := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST"))
	port := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_PORT"))
	if port == "" {
		port = "443"
	}
	if host == "" {
		return fmt.Errorf("KUBERNETES_SERVICE_HOST is not set")
	}

	tokenBytes, err := os.ReadFile(serviceAccountTokenPath)
	if err != nil {
		return fmt.Errorf("failed to read service account token: %w", err)
	}
	token := strings.TrimSpace(string(tokenBytes))
	if token == "" {
		return fmt.Errorf("service account token is empty")
	}

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if caBytes, err := os.ReadFile(serviceAccountCAPath); err == nil && len(caBytes) > 0 {
		pool := x509.NewCertPool()
		if pool.AppendCertsFromPEM(caBytes) {
			tlsConfig.RootCAs = pool
		}
	}

	m.k8sClient = &http.Client{
		Timeout: defaultRollbackApplyTimeout,
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}
	m.k8sBaseURL = fmt.Sprintf("https://%s:%s", host, port)
	m.k8sToken = token
	return nil
}

func (m *rollbackManager) rollbackApplyURL(record RollbackRecord) (string, error) {
	if strings.TrimSpace(record.Resource) == "" || strings.TrimSpace(record.Name) == "" {
		return "", fmt.Errorf("rollback record missing resource or name")
	}

	version := strings.TrimSpace(record.ResourceVersion)
	if version == "" {
		version = "v1"
	}

	var path string
	if strings.TrimSpace(record.ResourceGroup) == "" {
		path = "/api/" + url.PathEscape(version)
	} else {
		path = "/apis/" + url.PathEscape(record.ResourceGroup) + "/" + url.PathEscape(version)
	}
	if strings.TrimSpace(record.Namespace) != "" {
		path += "/namespaces/" + url.PathEscape(record.Namespace)
	}
	path += "/" + url.PathEscape(record.Resource) + "/" + url.PathEscape(record.Name)

	values := url.Values{}
	values.Set("fieldManager", m.fieldManager)
	values.Set("force", "true")

	return strings.TrimRight(m.k8sBaseURL, "/") + path + "?" + values.Encode(), nil
}

func (m *rollbackManager) pollTelegramCallbacks() {
	if strings.TrimSpace(config.Telegram.BotToken) == "" {
		return
	}

	locked, err := m.acquirePollLock()
	if err != nil {
		klog.Errorf("Failed to acquire rollback Telegram poll lock: %v", err)
		return
	}
	if !locked {
		return
	}
	defer os.Remove(m.pollLockPath)

	offset, err := m.telegramOffset()
	if err != nil {
		klog.Errorf("Failed to read rollback Telegram update offset: %v", err)
		return
	}

	updates, err := fetchTelegramUpdates(offset)
	if err != nil {
		klog.Errorf("Failed to fetch rollback Telegram updates: %v", err)
		return
	}
	if len(updates) == 0 {
		return
	}

	maxUpdateID := offset - 1
	for _, update := range updates {
		if update.UpdateID > maxUpdateID {
			maxUpdateID = update.UpdateID
		}
		if update.CallbackQuery == nil {
			continue
		}
		m.HandleTelegramCallback(*update.CallbackQuery)
	}

	if err := m.setTelegramOffset(maxUpdateID + 1); err != nil {
		klog.Errorf("Failed to update rollback Telegram offset: %v", err)
	}
}

func (m *rollbackManager) acquirePollLock() (bool, error) {
	err := acquireFileLock(m.pollLockPath, 0, 10*time.Second)
	if err == nil {
		return true, nil
	}
	if os.IsExist(err) {
		return false, nil
	}
	return false, err
}

func (m *rollbackManager) telegramOffset() (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := os.ReadFile(m.statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	offset, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, err
	}
	return offset, nil
}

func (m *rollbackManager) setTelegramOffset(offset int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return atomicWriteFile(m.statePath, []byte(strconv.Itoa(offset)), 0o600, true)
}

func rollbackActorFromTelegramUser(user telegramUser) RollbackActor {
	displayName := strings.TrimSpace(strings.Join([]string{user.FirstName, user.LastName}, " "))
	if displayName == "" {
		displayName = strings.TrimSpace(user.Username)
	}
	return RollbackActor{
		ID:          strconv.FormatInt(user.ID, 10),
		Username:    strings.TrimSpace(user.Username),
		DisplayName: displayName,
	}
}

func rollbackActorMarkdown(id string, username string, displayName string) string {
	id = strings.TrimSpace(id)
	username = strings.TrimSpace(username)
	displayName = strings.TrimSpace(displayName)
	if id == "" {
		if username != "" {
			return "@" + escapeMarkdownV2(username)
		}
		if displayName != "" {
			return escapeMarkdownV2(displayName)
		}
		return "`-`"
	}

	label := displayName
	if username != "" {
		label = "@" + username
	}
	if strings.TrimSpace(label) == "" {
		label = "用户 " + id
	}
	return fmt.Sprintf("[%s](tg://user?id=%s)", escapeMarkdownV2(label), escapeMarkdownV2(id))
}

func sendRollbackYAMLDocumentToUser(botToken string, userID int64, record RollbackRecord, manifestYAML string) error {
	fileName := fmt.Sprintf("rollback-%s-%s-%s.yaml", strings.ToLower(record.Kind), safeFileNamePart(record.Name), record.ID)
	return sendTelegramDocument(botToken, strconv.FormatInt(userID, 10), fileName, manifestYAML)
}

func sendStartBotReminderToGroup(chatID int64, user telegramUser) {
	if strings.TrimSpace(config.Telegram.BotToken) == "" {
		return
	}
	displayName := strings.TrimSpace(strings.Join([]string{user.FirstName, user.LastName}, " "))
	if displayName == "" {
		displayName = strings.TrimSpace(user.Username)
	}
	if displayName == "" {
		displayName = strconv.FormatInt(user.ID, 10)
	}
	mention := fmt.Sprintf(`<a href="tg://user?id=%d">%s</a>`, user.ID, html.EscapeString(displayName))
	text := fmt.Sprintf(`%s YAML 文件私聊发送失败。请先私聊机器人并发送 /start，然后再点击“下载 YAML”。`, mention)

	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", config.Telegram.BotToken)
	values := url.Values{}
	values.Add("chat_id", strconv.FormatInt(chatID, 10))
	values.Add("text", text)
	values.Add("parse_mode", "HTML")
	resp, err := httpClient.PostForm(apiURL, values)
	if err != nil {
		klog.Errorf("Failed to send Telegram private chat reminder: %v", err)
		return
	}
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
}

func buildRollbackStatusMessage(record RollbackRecord) string {
	statusLabel := rollbackTerminalStatusLabel(record.ExecutionStatus)
	lastAction := rollbackActionLabel(record.LastAction)
	lastBy := rollbackActorMarkdown(record.LastClickedBy, record.LastClickedByUsername, record.LastClickedByDisplayName)
	lastAt := "-"
	if !record.LastClickedAt.IsZero() {
		lastAt = record.LastClickedAt.Format("2006-01-02 15:04:05 MST")
	}
	executedAt := "-"
	if !record.ExecutedAt.IsZero() {
		executedAt = record.ExecutedAt.Format("2006-01-02 15:04:05 MST")
	}
	executedBy := rollbackActorMarkdown(record.ExecutedBy, record.ExecutedByUsername, record.ExecutedByDisplayName)
	errorLine := ""
	if strings.TrimSpace(record.ExecutionError) != "" {
		errorLine = fmt.Sprintf("\n*错误*: `%s`", escapeMarkdownV2(limitTelegramText(record.ExecutionError, 500)))
	}
	return fmt.Sprintf(
		"🔁 *K8s 回滚状态*\n\n"+
			"*集群*: `%s`\n"+
			"*资源*: `%s`\n"+
			"*状态*: `%s`\n"+
			"*执行回滚点击次数*: `%d`\n"+
			"*下载 YAML 点击次数*: `%d`\n"+
			"*最后操作*: `%s`\n"+
			"*最后点击人*: %s\n"+
			"*最后点击时间*: `%s`\n"+
			"*执行人*: %s\n"+
			"*执行时间*: `%s`\n"+
			"*回滚ID*: `%s`%s",
		escapeMarkdownV2(record.ClusterName),
		escapeMarkdownV2(record.ResourceDisplay),
		escapeMarkdownV2(statusLabel),
		record.RollbackClickCount,
		record.DownloadClickCount,
		escapeMarkdownV2(lastAction),
		lastBy,
		escapeMarkdownV2(lastAt),
		executedBy,
		escapeMarkdownV2(executedAt),
		escapeMarkdownV2(record.ID),
		errorLine,
	)
}

func editTelegramMessageTextWithMarkup(chatID int64, messageID int, text string, replyMarkup string) error {
	if strings.TrimSpace(config.Telegram.BotToken) == "" {
		return nil
	}
	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/editMessageText", config.Telegram.BotToken)
	values := url.Values{}
	values.Add("chat_id", strconv.FormatInt(chatID, 10))
	values.Add("message_id", strconv.Itoa(messageID))
	values.Add("text", text)
	values.Add("parse_mode", "MarkdownV2")
	if strings.TrimSpace(replyMarkup) != "" {
		values.Add("reply_markup", replyMarkup)
	}
	resp, err := httpClient.PostForm(apiURL, values)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("editMessageText status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func limitTelegramText(value string, maxRunes int) string {
	runes := []rune(value)
	if len(runes) <= maxRunes {
		return value
	}
	return string(runes[:maxRunes]) + "...(truncated)"
}
