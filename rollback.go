package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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
	Enabled                bool             `json:"enabled" yaml:"enabled"`
	AuthorizedTelegramIDs  []string         `json:"authorized_telegram_ids" yaml:"authorized_telegram_ids"`
	RetentionHours         int              `json:"retention_hours" yaml:"retention_hours"`
	StateDirectory         string           `json:"state_directory" yaml:"state_directory"`
	PollIntervalSeconds    int              `json:"poll_interval_seconds" yaml:"poll_interval_seconds"`
	FieldManager           string           `json:"field_manager" yaml:"field_manager"`
	Mongo                  MongoAuditConfig `json:"mongo" yaml:"mongo"`
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
	ManifestYAML    string    `json:"manifest_yaml" bson:"manifest_yaml"`
	ManifestJSON    string    `json:"manifest_json" bson:"manifest_json"`
	ExecutedAt      time.Time `json:"executed_at,omitempty" bson:"executed_at,omitempty"`
	ExecutedBy      string    `json:"executed_by,omitempty" bson:"executed_by,omitempty"`
	ExecutionStatus string    `json:"execution_status,omitempty" bson:"execution_status,omitempty"`
	ExecutionError  string    `json:"execution_error,omitempty" bson:"execution_error,omitempty"`
}

type rollbackManager struct {
	enabled       bool
	authorizedIDs []string
	retention     time.Duration
	collection    *mongo.Collection
	mongoTimeout  time.Duration
	fieldManager  string
	statePath     string
	pollLockPath  string
	pollInterval  time.Duration
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
	if config.Rollback.PollIntervalSeconds <= 0 {
		config.Rollback.PollIntervalSeconds = defaultDeleteConfirmationPollSeconds
	}
	if strings.TrimSpace(config.Rollback.FieldManager) == "" {
		config.Rollback.FieldManager = defaultRollbackFieldManager
	}
}

func newRollbackManager(cfg RollbackConfig) (*rollbackManager, error) {
	if !cfg.Enabled {
		return &rollbackManager{enabled: false}, nil
	}

	mongoCfg, ok := resolveRollbackMongoConfig(cfg)
	if !ok {
		klog.Warning("Rollback is enabled, but no rollback MongoDB configuration is available. Rollback backups and buttons are disabled.")
		return &rollbackManager{enabled: false}, nil
	}

	directory := strings.TrimSpace(cfg.StateDirectory)
	if directory == "" {
		directory = defaultDeleteConfirmationDirectory
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create rollback state directory '%s': %w", directory, err)
	}

	retentionHours := cfg.RetentionHours
	if retentionHours <= 0 {
		retentionHours = defaultRollbackRetentionHours
	}
	connectTimeoutSeconds := mongoCfg.ConnectTimeoutSeconds
	if connectTimeoutSeconds <= 0 {
		connectTimeoutSeconds = defaultMongoConnectTimeout
	}
	timeout := time.Duration(connectTimeoutSeconds) * time.Second

	coll, err := initRollbackMongoCollection(mongoCfg, timeout)
	if err != nil {
		klog.Errorf("Rollback backups disabled: %v", err)
		return &rollbackManager{enabled: false}, nil
	}

	manager := &rollbackManager{
		enabled:       true,
		authorizedIDs: normalizeTelegramIDs(cfg.AuthorizedTelegramIDs),
		retention:     time.Duration(retentionHours) * time.Hour,
		collection:    coll,
		mongoTimeout:  timeout,
		fieldManager:  strings.TrimSpace(cfg.FieldManager),
		statePath:     filepath.Join(directory, rollbackOffsetFileName),
		pollLockPath:  filepath.Join(directory, rollbackPollLockFileName),
		pollInterval:  time.Duration(cfg.PollIntervalSeconds) * time.Second,
		stopCh:        make(chan struct{}),
	}
	if manager.fieldManager == "" {
		manager.fieldManager = defaultRollbackFieldManager
	}
	if manager.pollInterval <= 0 {
		manager.pollInterval = time.Duration(defaultDeleteConfirmationPollSeconds) * time.Second
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

func initRollbackMongoCollection(cfg MongoAuditConfig, timeout time.Duration) (*mongo.Collection, error) {
	uri := strings.TrimSpace(cfg.URI)
	if uri == "" {
		uri = strings.TrimSpace(os.Getenv("MONGODB_URI"))
	}
	if uri == "" {
		return nil, fmt.Errorf("rollback mongo enabled but no uri configured")
	}

	database := strings.TrimSpace(cfg.Database)
	if database == "" {
		database = defaultMongoDatabase
	}

	collection := strings.TrimSpace(cfg.Collection)
	if collection == "" {
		collection = defaultRollbackCollection
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to rollback mongodb: %w", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("failed to ping rollback mongodb: %w", err)
	}

	coll := client.Database(database).Collection(collection)
	ttlIndex := mongo.IndexModel{
		Keys: bson.D{{Key: "expires_at", Value: 1}},
		Options: options.Index().
			SetName("rollback_expires_at_ttl").
			SetExpireAfterSeconds(0),
	}
	if _, err := coll.Indexes().CreateOne(ctx, ttlIndex); err != nil {
		return nil, fmt.Errorf("failed to create rollback ttl index: %w", err)
	}

	requestIndex := mongo.IndexModel{
		Keys:    bson.D{{Key: "request_uid", Value: 1}},
		Options: options.Index().SetName("rollback_request_uid"),
	}
	if _, err := coll.Indexes().CreateOne(ctx, requestIndex); err != nil {
		klog.Errorf("Failed to create rollback request_uid index: %v", err)
	}

	return coll, nil
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
	}

	ctx, cancel := context.WithTimeout(context.Background(), m.mongoTimeout)
	defer cancel()

	_, err = m.collection.UpdateByID(ctx, id, bson.M{
		"$setOnInsert": record,
	}, options.Update().SetUpsert(true))
	if err != nil {
		return "", fmt.Errorf("failed to persist rollback record: %w", err)
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
	rollbackID = strings.TrimSpace(rollbackID)
	if rollbackID == "" {
		return ""
	}

	payload := map[string]interface{}{
		"inline_keyboard": [][]map[string]string{
			{
				{"text": "Rollback to this version", "callback_data": rollbackCallbackData(rollbackID)},
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
	return fmt.Sprintf("%s:%s", rollbackCallbackPrefix, rollbackID)
}

func parseRollbackCallbackData(data string) (string, bool) {
	parts := strings.SplitN(data, ":", 2)
	if len(parts) != 2 || parts[0] != rollbackCallbackPrefix || strings.TrimSpace(parts[1]) == "" {
		return "", false
	}
	return parts[1], true
}

func (m *rollbackManager) HandleTelegramCallback(callback telegramCallbackQuery) bool {
	rollbackID, ok := parseRollbackCallbackData(callback.Data)
	if !ok {
		return false
	}

	if m == nil || !m.enabled {
		answerTelegramCallback(callback.ID, "Rollback is not enabled.")
		return true
	}

	telegramID := strconv.FormatInt(callback.From.ID, 10)
	if !m.isAuthorized(telegramID) {
		answerTelegramCallback(callback.ID, "You are not allowed to execute rollback.")
		return true
	}

	record, err := m.loadRecord(rollbackID)
	if err != nil {
		klog.Errorf("Failed to load rollback record %s: %v", rollbackID, err)
		answerTelegramCallback(callback.ID, "Rollback backup does not exist or has expired.")
		return true
	}
	if time.Now().After(record.ExpiresAt) {
		answerTelegramCallback(callback.ID, "Rollback backup has expired.")
		return true
	}

	if err := m.applyRecord(record); err != nil {
		klog.Errorf("Failed to apply rollback record %s: %v", rollbackID, err)
		m.recordExecution(rollbackID, telegramID, "failed", err.Error())
		answerTelegramCallback(callback.ID, "Rollback failed. Check service logs.")
		return true
	}

	m.recordExecution(rollbackID, telegramID, "applied", "")
	answerTelegramCallback(callback.ID, "Rollback applied.")
	return true
}

func (m *rollbackManager) isAuthorized(telegramID string) bool {
	if len(m.authorizedIDs) == 0 {
		return false
	}
	return stringSliceContains(m.authorizedIDs, telegramID)
}

func (m *rollbackManager) loadRecord(id string) (RollbackRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), m.mongoTimeout)
	defer cancel()

	var record RollbackRecord
	err := m.collection.FindOne(ctx, bson.M{
		"_id":        id,
		"expires_at": bson.M{"$gt": time.Now()},
	}).Decode(&record)
	return record, err
}

func (m *rollbackManager) recordExecution(id string, telegramID string, status string, message string) {
	ctx, cancel := context.WithTimeout(context.Background(), m.mongoTimeout)
	defer cancel()

	update := bson.M{
		"$set": bson.M{
			"executed_at":      time.Now(),
			"executed_by":      telegramID,
			"execution_status": status,
			"execution_error":  message,
		},
	}
	if _, err := m.collection.UpdateByID(ctx, id, update); err != nil {
		klog.Errorf("Failed to record rollback execution for %s: %v", id, err)
	}
}

func (m *rollbackManager) applyRecord(record RollbackRecord) error {
	if strings.TrimSpace(record.ManifestYAML) == "" {
		return fmt.Errorf("rollback manifest is empty")
	}
	if err := m.ensureKubernetesClient(); err != nil {
		return err
	}

	apiURL, err := m.rollbackApplyURL(record)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPatch, apiURL, bytes.NewBufferString(record.ManifestYAML))
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

	tempPath := m.statePath + ".tmp"
	if err := os.WriteFile(tempPath, []byte(strconv.Itoa(offset)), 0o600); err != nil {
		return err
	}
	return os.Rename(tempPath, m.statePath)
}
