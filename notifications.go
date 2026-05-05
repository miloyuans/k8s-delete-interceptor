package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

const (
	defaultNotificationDedupeWindowSeconds = 300
	defaultNotificationRetryBatch          = 50
	notificationChannelDefault             = "default"
	notificationChannelAudit               = "audit"
	notificationChannelLifecycle           = "lifecycle"
	notificationStateFileName              = ".notification-state.json"
)

type NotificationControlConfig struct {
	DedupeWindowSeconds  int  `json:"dedupe_window_seconds" yaml:"dedupe_window_seconds"`
	RetryFailedOnStartup bool `json:"retry_failed_on_startup" yaml:"retry_failed_on_startup"`
	MaxRetryBatch        int  `json:"max_retry_batch" yaml:"max_retry_batch"`
}

type notificationSignatureState struct {
	LastDispatchedAt  time.Time `json:"last_dispatched_at"`
	SuppressedCount   int       `json:"suppressed_count"`
	FirstSuppressedAt time.Time `json:"first_suppressed_at,omitempty"`
	LastSuppressedAt  time.Time `json:"last_suppressed_at,omitempty"`
}

type pendingNotification struct {
	Signature     string              `json:"signature"`
	Channel       string              `json:"channel"`
	Context       NotificationContext `json:"context"`
	FirstFailedAt time.Time           `json:"first_failed_at"`
	LastFailedAt  time.Time           `json:"last_failed_at"`
	Attempts      int                 `json:"attempts"`
}

type notificationState struct {
	Signatures map[string]notificationSignatureState `json:"signatures"`
	Pending    map[string]pendingNotification        `json:"pending"`
}

type notificationManager struct {
	statePath      string
	dedupeWindow   time.Duration
	retryOnStartup bool
	maxRetryBatch  int
	mu             sync.Mutex
	state          notificationState
}

var notifier *notificationManager

func applyNotificationDefaults() {
	if config.Notifications.DedupeWindowSeconds <= 0 {
		config.Notifications.DedupeWindowSeconds = defaultNotificationDedupeWindowSeconds
	}
	if config.Notifications.MaxRetryBatch <= 0 {
		config.Notifications.MaxRetryBatch = defaultNotificationRetryBatch
	}
	if !config.Notifications.RetryFailedOnStartup {
		config.Notifications.RetryFailedOnStartup = true
	}
}

func newNotificationManager(cfg NotificationControlConfig) (*notificationManager, error) {
	directory := lifecycleStateDirectory()
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create notification directory '%s': %w", directory, err)
	}

	manager := &notificationManager{
		statePath:      filepath.Join(directory, notificationStateFileName),
		dedupeWindow:   time.Duration(cfg.DedupeWindowSeconds) * time.Second,
		retryOnStartup: cfg.RetryFailedOnStartup,
		maxRetryBatch:  cfg.MaxRetryBatch,
		state: notificationState{
			Signatures: map[string]notificationSignatureState{},
			Pending:    map[string]pendingNotification{},
		},
	}

	if err := manager.loadState(); err != nil {
		klog.Errorf("Failed to load notification state, continuing with empty state: %v", err)
	}

	if manager.retryOnStartup {
		go manager.retryPendingNotifications()
	}

	return manager, nil
}

func (m *notificationManager) loadState() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := os.ReadFile(m.statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to read notification state file '%s': %w", m.statePath, err)
	}

	var state notificationState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("failed to unmarshal notification state file '%s': %w", m.statePath, err)
	}

	if state.Signatures == nil {
		state.Signatures = map[string]notificationSignatureState{}
	}
	if state.Pending == nil {
		state.Pending = map[string]pendingNotification{}
	}

	m.state = state
	return nil
}

func (m *notificationManager) saveStateLocked() error {
	payload, err := json.Marshal(m.state)
	if err != nil {
		return err
	}

	tempPath := m.statePath + ".tmp"
	if err := os.WriteFile(tempPath, payload, 0o600); err != nil {
		return err
	}

	return os.Rename(tempPath, m.statePath)
}

func buildNotificationSignature(channel string, ctx NotificationContext) string {
	parts := []string{
		strings.ToLower(strings.TrimSpace(channel)),
		strings.ToLower(strings.TrimSpace(ctx.OperationType)),
		strings.ToLower(strings.TrimSpace(ctx.Action)),
		strings.ToLower(strings.TrimSpace(ctx.Kind)),
		strings.ToLower(strings.TrimSpace(ctx.Namespace)),
		strings.ToLower(strings.TrimSpace(ctx.Name)),
		strings.ToLower(strings.TrimSpace(ctx.User)),
		strings.ToLower(strings.TrimSpace(ctx.Cluster)),
	}
	if strings.TrimSpace(ctx.RollbackID) != "" {
		parts = append(parts, strings.ToLower(strings.TrimSpace(ctx.RollbackID)))
	}

	return strings.Join(parts, "|")
}

func appendSuppressionSummary(reason string, suppressedCount int, firstSuppressedAt time.Time, lastSuppressedAt time.Time) string {
	if suppressedCount <= 0 {
		return reason
	}

	summary := fmt.Sprintf("降噪摘要: 最近重复抑制 %d 次，时间范围 %s - %s。", suppressedCount, firstSuppressedAt.Format("2006-01-02 15:04:05 MST"), lastSuppressedAt.Format("2006-01-02 15:04:05 MST"))
	if strings.TrimSpace(reason) == "" {
		return summary
	}

	return reason + "\n" + summary
}

func (m *notificationManager) prepareNotification(channel string, ctx NotificationContext) (bool, NotificationContext, string, error) {
	signature := buildNotificationSignature(channel, ctx)
	now := time.Now()

	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.state.Signatures[signature]
	if !state.LastDispatchedAt.IsZero() && now.Sub(state.LastDispatchedAt) < m.dedupeWindow {
		if state.SuppressedCount == 0 {
			state.FirstSuppressedAt = now
		}
		state.SuppressedCount++
		state.LastSuppressedAt = now
		m.state.Signatures[signature] = state
		if pending, ok := m.state.Pending[signature]; ok {
			pending.Context = ctx
			pending.LastFailedAt = now
			m.state.Pending[signature] = pending
		}
		if err := m.saveStateLocked(); err != nil {
			return false, ctx, signature, err
		}
		return false, ctx, signature, nil
	}

	ctx.Reason = appendSuppressionSummary(ctx.Reason, state.SuppressedCount, state.FirstSuppressedAt, state.LastSuppressedAt)
	state.LastDispatchedAt = now
	m.state.Signatures[signature] = state
	if err := m.saveStateLocked(); err != nil {
		return false, ctx, signature, err
	}

	return true, ctx, signature, nil
}

func (m *notificationManager) markDelivered(signature string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.state.Signatures[signature]
	state.SuppressedCount = 0
	state.FirstSuppressedAt = time.Time{}
	state.LastSuppressedAt = time.Time{}
	m.state.Signatures[signature] = state
	delete(m.state.Pending, signature)
	if err := m.saveStateLocked(); err != nil {
		klog.Errorf("Failed to persist notification delivery state: %v", err)
	}
}

func (m *notificationManager) recordFailed(signature string, channel string, ctx NotificationContext) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pending := m.state.Pending[signature]
	if pending.Signature == "" {
		pending = pendingNotification{
			Signature:     signature,
			Channel:       channel,
			Context:       ctx,
			FirstFailedAt: time.Now(),
		}
	}

	pending.Channel = channel
	pending.Context = ctx
	pending.LastFailedAt = time.Now()
	pending.Attempts++
	m.state.Pending[signature] = pending
	if err := m.saveStateLocked(); err != nil {
		klog.Errorf("Failed to persist pending notification state: %v", err)
	}
}

func (m *notificationManager) snapshotPendingNotifications() []pendingNotification {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]pendingNotification, 0, len(m.state.Pending))
	count := 0
	for _, pending := range m.state.Pending {
		result = append(result, pending)
		count++
		if m.maxRetryBatch > 0 && count >= m.maxRetryBatch {
			break
		}
	}

	return result
}

func resolveTelegramConfigForChannel(channel string) TelegramConfig {
	switch channel {
	case notificationChannelAudit:
		return resolveAuditTelegramConfig()
	case notificationChannelLifecycle:
		return resolveLifecycleTelegramConfig()
	default:
		return config.Telegram
	}
}

func (m *notificationManager) retryPendingNotifications() {
	for _, pending := range m.snapshotPendingNotifications() {
		cfg := resolveTelegramConfigForChannel(pending.Channel)
		if !isTelegramConfigConfigured(cfg) {
			continue
		}

		if err := deliverTelegramNotification(cfg, pending.Context); err != nil {
			klog.Errorf("Failed to retry pending notification '%s': %v", pending.Signature, err)
			m.recordFailed(pending.Signature, pending.Channel, pending.Context)
			continue
		}

		klog.Infof("Retried pending notification '%s' successfully", pending.Signature)
		m.markDelivered(pending.Signature)
	}
}

func (m *notificationManager) dispatch(channel string, telegramCfg TelegramConfig, ctx NotificationContext) {
	if !isTelegramConfigConfigured(telegramCfg) {
		klog.Warning("Telegram config (token or chat_ids) missing or incomplete, skipping notification")
		return
	}

	shouldSend, preparedCtx, signature, err := m.prepareNotification(channel, ctx)
	if err != nil {
		klog.Errorf("Failed to prepare notification state: %v", err)
	}
	if err == nil && !shouldSend {
		klog.Infof("Notification suppressed by dedupe window. Signature=%s", signature)
		return
	}

	if signature == "" {
		signature = buildNotificationSignature(channel, preparedCtx)
	}

	if err := deliverTelegramNotification(telegramCfg, preparedCtx); err != nil {
		klog.Errorf("Telegram notification delivery failed. Signature=%s, Error=%v", signature, err)
		m.recordFailed(signature, channel, preparedCtx)
		return
	}

	m.markDelivered(signature)
}

func (m *notificationManager) Dispatch(channel string, telegramCfg TelegramConfig, ctx NotificationContext, async bool) {
	if async {
		go m.dispatch(channel, telegramCfg, ctx)
		return
	}

	m.dispatch(channel, telegramCfg, ctx)
}
