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

type rollbackManager struct {
	enabled       bool
	authorizedIDs []string
	store         RollbackStore
	fieldManager  string
	allowReapply  bool

	statePath    string
	pollLockPath string
	pollInterval time.Duration
	stopCh       chan struct{}
	mu           sync.Mutex

	k8sClient  *http.Client
	k8sBaseURL string
	k8sToken   string
}

var rollbacker *rollbackManager

func newRollbackManager(cfg RollbackConfig) (*rollbackManager, error) {
	if !cfg.Enabled {
		return &rollbackManager{enabled: false}, nil
	}

	store, err := newRollbackStore(cfg)
	if err != nil {
		return &rollbackManager{enabled: false}, fmt.Errorf("failed to initialize rollback storage: %w", err)
	}

	runtimeDir := rollbackRuntimeDirectory(cfg)
	if err := os.MkdirAll(runtimeDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create rollback runtime directory '%s': %w", runtimeDir, err)
	}

	pollInterval := time.Duration(cfg.PollIntervalSeconds) * time.Second
	if pollInterval <= 0 {
		pollInterval = time.Duration(defaultDeleteConfirmationPollSeconds) * time.Second
	}

	manager := &rollbackManager{
		enabled:       true,
		authorizedIDs: normalizeTelegramIDs(cfg.AuthorizedTelegramIDs),
		store:         store,
		fieldManager:  strings.TrimSpace(cfg.FieldManager),
		allowReapply:  cfg.AllowReapply,
		statePath:     filepath.Join(runtimeDir, rollbackOffsetFileName),
		pollLockPath:  filepath.Join(runtimeDir, rollbackPollLockFileName),
		pollInterval:  pollInterval,
		stopCh:        make(chan struct{}),
	}
	if manager.fieldManager == "" {
		manager.fieldManager = defaultRollbackFieldManager
	}

	klog.Infof("Rollback enabled. storage=%s data_directory=%s allow_reapply=%v", store.Type(), rollbackDataDirectory(cfg), manager.allowReapply)
	return manager, nil
}

func (m *rollbackManager) Start() {
	if m == nil || !m.enabled || m.stopCh == nil {
		return
	}

	// When delete confirmation is enabled it already polls Telegram and delegates
	// non-delete-confirmation callback data to rollbacker.HandleTelegramCallback.
	// Running a second rollback poller with a different offset can consume the same
	// bot updates out of order in multi-pod mode.
	if deleteConfirmer != nil && deleteConfirmer.enabled {
		klog.Info("Rollback standalone Telegram poller disabled because delete confirmation poller will delegate rollback callbacks.")
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
		SchemaVersion:   defaultRollbackSchemaVersion,
		ID:              id,
		Timestamp:       now,
		UpdatedAt:       now,
		ExpiresAt:       now.Add(time.Duration(config.Rollback.RetentionHours) * time.Hour),
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
	rollbackID = strings.TrimSpace(rollbackID)
	if rollbackID == "" {
		return ""
	}

	payload := map[string]interface{}{
		"inline_keyboard": [][]map[string]string{
			{
				{"text": "执行回滚", "callback_data": rollbackCallbackData(rollbackActionApply, rollbackID)},
				{"text": "下载 YAML", "callback_data": rollbackCallbackData(rollbackActionDownload, rollbackID)},
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

func buildRollbackReplyMarkupForRecord(record RollbackRecord) string {
	buttons := []map[string]string{}
	if record.ExecutionStatus != rollbackStatusApplied || config.Rollback.AllowReapply {
		buttons = append(buttons, map[string]string{
			"text":          "执行回滚",
			"callback_data": rollbackCallbackData(rollbackActionApply, record.ID),
		})
	}
	buttons = append(buttons, map[string]string{
		"text":          "下载 YAML",
		"callback_data": rollbackCallbackData(rollbackActionDownload, record.ID),
	})

	payload := map[string]interface{}{"inline_keyboard": [][]map[string]string{buttons}}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	return string(encoded)
}

func rollbackCallbackData(action string, rollbackID string) string {
	return fmt.Sprintf("%s:%s:%s", rollbackCallbackPrefix, action, rollbackID)
}

func parseRollbackCallbackData(data string) (string, string, bool) {
	parts := strings.Split(data, ":")
	if len(parts) == 2 && parts[0] == rollbackCallbackPrefix {
		// Backward compatibility for old callback_data: rb:<id>
		return rollbackActionApply, parts[1], strings.TrimSpace(parts[1]) != ""
	}
	if len(parts) != 3 || parts[0] != rollbackCallbackPrefix {
		return "", "", false
	}
	action := strings.TrimSpace(parts[1])
	id := strings.TrimSpace(parts[2])
	if id == "" {
		return "", "", false
	}
	if action != rollbackActionApply && action != rollbackActionDownload {
		return "", "", false
	}
	return action, id, true
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

	telegramID := strconv.FormatInt(callback.From.ID, 10)
	if !m.isAuthorized(telegramID) {
		answerTelegramCallback(callback.ID, "You are not allowed to execute rollback.")
		return true
	}

	switch action {
	case rollbackActionDownload:
		m.handleRollbackYAMLDownload(callback, rollbackID)
	case rollbackActionApply:
		m.handleRollbackApply(callback, rollbackID)
	default:
		answerTelegramCallback(callback.ID, "Unsupported rollback action.")
	}

	return true
}

func (m *rollbackManager) handleRollbackYAMLDownload(callback telegramCallbackQuery, rollbackID string) {
	telegramID := strconv.FormatInt(callback.From.ID, 10)

	record, manifestYAML, err := m.store.IncrementDownload(rollbackID, telegramID)
	if err != nil {
		answerTelegramCallback(callback.ID, rollbackErrorMessage(err))
		return
	}
	if strings.TrimSpace(manifestYAML) == "" {
		answerTelegramCallback(callback.ID, "Rollback YAML is empty.")
		return
	}

	if err := sendRollbackYAMLDocumentToUser(config.Telegram.BotToken, callback.From.ID, record, manifestYAML); err != nil {
		klog.Errorf("Failed to send rollback YAML to user %s: %v", telegramID, err)
		answerTelegramCallback(callback.ID, "私聊发送失败，请先私聊机器人发送 /start。")

		if callback.Message != nil {
			sendStartBotReminderToGroup(callback.Message.Chat.ID, callback.From)
			_ = editTelegramMessageText(callback.Message.Chat.ID, callback.Message.MessageID, buildRollbackStatusMessage(record, "YAML 私聊失败", telegramID, err.Error()))
			_ = editTelegramMessageReplyMarkupJSON(callback.Message.Chat.ID, callback.Message.MessageID, buildRollbackReplyMarkupForRecord(record))
		}
		return
	}

	answerTelegramCallback(callback.ID, "YAML 已发送到你的私聊。")
	if callback.Message != nil {
		_ = editTelegramMessageText(callback.Message.Chat.ID, callback.Message.MessageID, buildRollbackStatusMessage(record, "YAML 已私聊发送", telegramID, ""))
		_ = editTelegramMessageReplyMarkupJSON(callback.Message.Chat.ID, callback.Message.MessageID, buildRollbackReplyMarkupForRecord(record))
	}
}

func (m *rollbackManager) handleRollbackApply(callback telegramCallbackQuery, rollbackID string) {
	telegramID := strconv.FormatInt(callback.From.ID, 10)

	record, manifestYAML, err := m.store.MarkRunning(rollbackID, telegramID, m.allowReapply)
	if err != nil {
		answerTelegramCallback(callback.ID, rollbackErrorMessage(err))
		return
	}

	if callback.Message != nil {
		_ = editTelegramMessageText(callback.Message.Chat.ID, callback.Message.MessageID, buildRollbackStatusMessage(record, "执行中", telegramID, ""))
		_ = editTelegramMessageReplyMarkupJSON(callback.Message.Chat.ID, callback.Message.MessageID, buildRollbackReplyMarkupForRecord(record))
	}

	if err := m.applyRecord(record, manifestYAML); err != nil {
		klog.Errorf("Failed to apply rollback record %s: %v", rollbackID, err)
		failedRecord, _, markErr := m.store.MarkFailed(rollbackID, telegramID, err.Error())
		if markErr != nil {
			klog.Errorf("Failed to record rollback failure for %s: %v", rollbackID, markErr)
			failedRecord = record
			failedRecord.ExecutionStatus = rollbackStatusFailed
			failedRecord.ExecutionError = err.Error()
		}

		if callback.Message != nil {
			_ = editTelegramMessageText(callback.Message.Chat.ID, callback.Message.MessageID, buildRollbackStatusMessage(failedRecord, "已失败", telegramID, err.Error()))
			_ = editTelegramMessageReplyMarkupJSON(callback.Message.Chat.ID, callback.Message.MessageID, buildRollbackReplyMarkupForRecord(failedRecord))
		}

		answerTelegramCallback(callback.ID, "Rollback failed. Check service logs.")
		return
	}

	appliedRecord, _, err := m.store.MarkApplied(rollbackID, telegramID)
	if err != nil {
		klog.Errorf("Failed to record rollback success for %s: %v", rollbackID, err)
		appliedRecord = record
		appliedRecord.ExecutionStatus = rollbackStatusApplied
	}

	if callback.Message != nil {
		_ = editTelegramMessageText(callback.Message.Chat.ID, callback.Message.MessageID, buildRollbackStatusMessage(appliedRecord, "已成功", telegramID, ""))
		_ = editTelegramMessageReplyMarkupJSON(callback.Message.Chat.ID, callback.Message.MessageID, buildRollbackReplyMarkupForRecord(appliedRecord))
	}

	answerTelegramCallback(callback.ID, "Rollback applied.")
}

func rollbackErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	if err == ErrRollbackNotFound {
		return "Rollback backup does not exist or has expired."
	}
	if err == ErrRollbackExpired {
		return "Rollback backup has expired."
	}
	if err == ErrRollbackAlreadyApplied {
		return "该备份已经回滚成功，默认禁止重复执行。"
	}
	if err == ErrRollbackLocked {
		return "该回滚记录正在被其他 Pod 处理，请稍后重试。"
	}
	return "Rollback operation failed. Check service logs."
}

func (m *rollbackManager) isAuthorized(telegramID string) bool {
	if len(m.authorizedIDs) == 0 {
		return false
	}
	return stringSliceContains(m.authorizedIDs, telegramID)
}

func (m *rollbackManager) applyRecord(record RollbackRecord, manifestYAML string) error {
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

func sendRollbackYAMLDocumentToUser(botToken string, userID int64, record RollbackRecord, manifestYAML string) error {
	fileName := fmt.Sprintf(
		"rollback-%s-%s-%s.yaml",
		strings.ToLower(record.Kind),
		safeFileNamePart(record.Name),
		record.ID,
	)
	return sendTelegramDocument(botToken, strconv.FormatInt(userID, 10), fileName, manifestYAML)
}

func sendStartBotReminderToGroup(chatID int64, user telegramUser) {
	if strings.TrimSpace(config.Telegram.BotToken) == "" {
		return
	}

	displayName := strings.TrimSpace(strings.Join([]string{user.FirstName, user.LastName}, " "))
	if displayName == "" {
		displayName = user.Username
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
	resp.Body.Close()
}

func buildRollbackStatusMessage(record RollbackRecord, displayStatus string, actor string, errorMessage string) string {
	if displayStatus == "" {
		displayStatus = displayRollbackStatus(record.ExecutionStatus)
	}
	lines := []string{
		"♻️ *K8s 回滚状态*",
		"",
		fmt.Sprintf("*集群*: `%s`", escapeMarkdownV2(record.ClusterName)),
		fmt.Sprintf("*资源*: `%s`", escapeMarkdownV2(record.ResourceDisplay)),
		fmt.Sprintf("*状态*: `%s`", escapeMarkdownV2(displayStatus)),
		fmt.Sprintf("*回滚点击次数*: `%d`", record.RollbackClickCount),
		fmt.Sprintf("*YAML 下载次数*: `%d`", record.DownloadClickCount),
	}
	if actor != "" {
		lines = append(lines, fmt.Sprintf("*最后点击用户ID*: `%s`", escapeMarkdownV2(actor)))
	}
	if !record.LastClickedAt.IsZero() {
		lines = append(lines, fmt.Sprintf("*最后点击时间*: `%s`", escapeMarkdownV2(record.LastClickedAt.Format("2006-01-02 15:04:05 MST"))))
	}
	if !record.ExecutedAt.IsZero() {
		lines = append(lines, fmt.Sprintf("*执行时间*: `%s`", escapeMarkdownV2(record.ExecutedAt.Format("2006-01-02 15:04:05 MST"))))
	}
	if record.ExecutedBy != "" {
		lines = append(lines, fmt.Sprintf("*执行用户ID*: `%s`", escapeMarkdownV2(record.ExecutedBy)))
	}
	if errorMessage == "" {
		errorMessage = record.ExecutionError
	}
	if strings.TrimSpace(errorMessage) != "" {
		lines = append(lines, fmt.Sprintf("*错误*: `%s`", escapeMarkdownV2(limitRollbackMessage(errorMessage))))
	}
	lines = append(lines, "", fmt.Sprintf("*RollbackID*: `%s`", escapeMarkdownV2(record.ID)))
	return strings.Join(lines, "\n")
}

func displayRollbackStatus(status string) string {
	switch status {
	case rollbackStatusPending:
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

func limitRollbackMessage(value string) string {
	runes := []rune(strings.TrimSpace(value))
	if len(runes) <= 500 {
		return string(runes)
	}
	return string(runes[:500]) + "...(truncated)"
}

func editTelegramMessageReplyMarkupJSON(chatID int64, messageID int, replyMarkup string) error {
	if strings.TrimSpace(config.Telegram.BotToken) == "" {
		return nil
	}
	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/editMessageReplyMarkup", config.Telegram.BotToken)
	values := url.Values{}
	values.Add("chat_id", strconv.FormatInt(chatID, 10))
	values.Add("message_id", strconv.Itoa(messageID))
	values.Add("reply_markup", replyMarkup)

	resp, err := httpClient.PostForm(apiURL, values)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("telegram editMessageReplyMarkup status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}
