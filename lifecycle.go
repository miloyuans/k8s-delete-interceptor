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
	lifecycleStateRunning        = "running"
	lifecycleStateStopped        = "stopped"
	lifecycleStateUnexpectedStop = "unexpected_stop"
	lifecycleEventStarted        = "started"
	lifecycleEventStopped        = "stopped"
	lifecycleEventUnexpectedStop = "unexpected_stop"
	lifecycleOperationType       = "LIFECYCLE"
	lifecycleOperationLabel      = "生命周期"
	lifecycleComponentKind       = "Service"
	lifecycleComponentName       = "delete-interceptor"
	lifecycleStateFileName       = ".lifecycle-state.json"
)

type LifecycleConfig struct {
	Enabled              bool               `json:"enabled" yaml:"enabled"`
	NotifyStartup        bool               `json:"notify_startup" yaml:"notify_startup"`
	NotifyShutdown       bool               `json:"notify_shutdown" yaml:"notify_shutdown"`
	DetectUncleanShutdown bool              `json:"detect_unclean_shutdown" yaml:"detect_unclean_shutdown"`
	Telegram             AuditTelegramConfig `json:"telegram" yaml:"telegram"`
}

type lifecycleState struct {
	Status     string    `json:"status"`
	UpdatedAt  time.Time `json:"updated_at"`
	Instance   string    `json:"instance"`
	Reason     string    `json:"reason,omitempty"`
}

type lifecycleManager struct {
	enabled   bool
	statePath string
	instance  string
	mu        sync.Mutex
	stopOnce  sync.Once
}

var serviceLifecycle *lifecycleManager

func applyLifecycleDefaults() {
	if !config.Lifecycle.Enabled {
		return
	}

	if !config.Lifecycle.NotifyStartup && !config.Lifecycle.NotifyShutdown && !config.Lifecycle.DetectUncleanShutdown {
		config.Lifecycle.NotifyStartup = true
		config.Lifecycle.NotifyShutdown = true
		config.Lifecycle.DetectUncleanShutdown = true
	}
}

func lifecycleStateDirectory() string {
	directory := strings.TrimSpace(config.Audit.Directory)
	if directory == "" {
		return defaultAuditDirectory
	}
	return directory
}

func resolveLifecycleTelegramConfig() TelegramConfig {
	lifecycleCfg := config.Lifecycle.Telegram
	customCfg := TelegramConfig{
		BotToken:             lifecycleCfg.BotToken,
		ChatIDs:              lifecycleCfg.ChatIDs,
		NotificationTemplate: lifecycleCfg.NotificationTemplate,
	}

	if lifecycleCfg.UseGlobal || !isTelegramConfigConfigured(customCfg) {
		return config.Telegram
	}

	return customCfg
}

func displayLifecycleEventLabel(event string) string {
	switch event {
	case lifecycleEventStarted:
		return "启动"
	case lifecycleEventStopped:
		return "停止"
	case lifecycleEventUnexpectedStop:
		return "异常停止"
	default:
		return event
	}
}

func displayLifecycleTitle(event string) string {
	switch event {
	case lifecycleEventStarted:
		return "K8s Webhook 服务启动通知"
	case lifecycleEventStopped:
		return "K8s Webhook 服务停止通知"
	case lifecycleEventUnexpectedStop:
		return "K8s Webhook 服务异常停止通知"
	default:
		return "K8s Webhook 服务生命周期通知"
	}
}

func displayLifecycleTitleIcon(event string) string {
	switch event {
	case lifecycleEventStarted:
		return "🟢"
	case lifecycleEventStopped:
		return "🔴"
	case lifecycleEventUnexpectedStop:
		return "⚠️"
	default:
		return "ℹ️"
	}
}

func lifecycleEventLabel(event string) string {
	switch event {
	case lifecycleEventStarted:
		return "启动"
	case lifecycleEventStopped:
		return "停止"
	case lifecycleEventUnexpectedStop:
		return "异常停止"
	default:
		return event
	}
}

func lifecycleTitle(event string) string {
	switch event {
	case lifecycleEventStarted:
		return "K8s 删除拦截服务启动通知"
	case lifecycleEventStopped:
		return "K8s 删除拦截服务停止通知"
	case lifecycleEventUnexpectedStop:
		return "K8s 删除拦截服务异常停止通知"
	default:
		return "K8s 删除拦截服务生命周期通知"
	}
}

func resolveLifecycleInstance() string {
	instance := strings.TrimSpace(os.Getenv("POD_NAME"))
	if instance != "" {
		return instance
	}

	hostname, err := os.Hostname()
	if err == nil && strings.TrimSpace(hostname) != "" {
		return strings.TrimSpace(hostname)
	}

	return "unknown-instance"
}

func newLifecycleManager(cfg LifecycleConfig) (*lifecycleManager, error) {
	if !cfg.Enabled {
		return &lifecycleManager{enabled: false}, nil
	}

	directory := lifecycleStateDirectory()
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create lifecycle directory '%s': %w", directory, err)
	}

	return &lifecycleManager{
		enabled:   true,
		statePath: filepath.Join(directory, lifecycleStateFileName),
		instance:  resolveLifecycleInstance(),
	}, nil
}

func (m *lifecycleManager) buildNotificationContext(event string, reason string) NotificationContext {
	return NotificationContext{
		Title:          displayLifecycleTitle(event),
		TitleIcon:      displayLifecycleTitleIcon(event),
		Action:         event,
		ActionIcon:     displayNotificationActionIcon(event),
		ActionLabel:    displayLifecycleEventLabel(event),
		User:           "system",
		Operation:      fmt.Sprintf("%s %s", lifecycleOperationType, m.instance),
		OperationType:  lifecycleOperationType,
		OperationLabel: displayNotificationOperationLabel(lifecycleOperationType),
		Cluster:        config.ClusterName,
		Reason:         reason,
		Timestamp:      time.Now().Format("2006-01-02 15:04:05 MST"),
		Kind:           lifecycleComponentKind,
		Name:           lifecycleComponentName,
		Namespace:      formatNamespace(webhookNs),
		Resource:       formatResource(lifecycleComponentKind, lifecycleComponentName, webhookNs),
		ResourceType:   lifecycleComponentKind,
		ResourceName:   lifecycleComponentName,
		ChangeDetails:  reason,
		RequestUID:     "",
	}
}

func (m *lifecycleManager) readState() (*lifecycleState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := os.ReadFile(m.statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var state lifecycleState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}

	return &state, nil
}

func (m *lifecycleManager) writeState(status string, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state := lifecycleState{
		Status:    status,
		UpdatedAt: time.Now(),
		Instance:  m.instance,
		Reason:    reason,
	}

	payload, err := json.Marshal(state)
	if err != nil {
		return err
	}

	tempPath := m.statePath + ".tmp"
	if err := os.WriteFile(tempPath, payload, 0o600); err != nil {
		return err
	}

	return os.Rename(tempPath, m.statePath)
}

func (m *lifecycleManager) sendSync(event string, reason string) {
	sendLifecycleTelegramNotificationSync(m.buildNotificationContext(event, reason))
}

func (m *lifecycleManager) HandleStartup() {
	if m == nil || !m.enabled {
		return
	}

	if config.Lifecycle.DetectUncleanShutdown {
		previousState, err := m.readState()
		if err != nil {
			klog.Errorf("Failed to read lifecycle state file '%s': %v", m.statePath, err)
		} else if previousState != nil && previousState.Status == lifecycleStateRunning {
			reason := fmt.Sprintf("实例 '%s' 检测到上一次未正常关闭。上一状态时间: %s。", m.instance, previousState.UpdatedAt.Format("2006-01-02 15:04:05 MST"))
			m.sendSync(lifecycleEventUnexpectedStop, reason)
			emitLifecycleAuditRecord(lifecycleEventUnexpectedStop, reason, true, reason)
			if err := m.writeState(lifecycleStateUnexpectedStop, reason); err != nil {
				klog.Errorf("Failed to update lifecycle state after unexpected stop detection: %v", err)
			}
		}
	}

	if err := m.writeState(lifecycleStateRunning, "service started"); err != nil {
		klog.Errorf("Failed to write lifecycle running state: %v", err)
	}

	if config.Lifecycle.NotifyStartup {
		reason := fmt.Sprintf("实例 '%s' 已启动，Webhook 服务开始提供准入能力。", m.instance)
		m.sendSync(lifecycleEventStarted, reason)
		emitLifecycleAuditRecord(lifecycleEventStarted, reason, true, reason)
	}
	if false && !config.Lifecycle.NotifyStartup {
		emitLifecycleAuditRecord(lifecycleEventStarted, fmt.Sprintf("å®žä¾‹ '%s' å·²å¯åŠ¨ï¼ŒWebhook æœåŠ¡å¼€å§‹æä¾›å‡†å…¥èƒ½åŠ›ã€‚", m.instance), false, "")
	}
	if !config.Lifecycle.NotifyStartup {
		emitLifecycleAuditRecord(lifecycleEventStarted, fmt.Sprintf("Instance '%s' started and webhook service is ready.", m.instance), false, "")
	}
}

func (m *lifecycleManager) HandleGracefulShutdown(reason string) {
	if m == nil || !m.enabled {
		return
	}

	m.stopOnce.Do(func() {
		if strings.TrimSpace(reason) == "" {
			reason = fmt.Sprintf("实例 '%s' 正在优雅停止。", m.instance)
		}

		if config.Lifecycle.NotifyShutdown {
			m.sendSync(lifecycleEventStopped, reason)
			emitLifecycleAuditRecord(lifecycleEventStopped, reason, true, reason)
		} else {
			emitLifecycleAuditRecord(lifecycleEventStopped, reason, false, "")
		}

		if err := m.writeState(lifecycleStateStopped, reason); err != nil {
			klog.Errorf("Failed to write lifecycle stopped state: %v", err)
		}
	})
}

func (m *lifecycleManager) HandleUnexpectedTermination(reason string) {
	if m == nil || !m.enabled {
		return
	}

	m.stopOnce.Do(func() {
		if strings.TrimSpace(reason) == "" {
			reason = fmt.Sprintf("实例 '%s' 检测到异常退出。", m.instance)
		}

		m.sendSync(lifecycleEventUnexpectedStop, reason)
		emitLifecycleAuditRecord(lifecycleEventUnexpectedStop, reason, true, reason)

		if err := m.writeState(lifecycleStateUnexpectedStop, reason); err != nil {
			klog.Errorf("Failed to write lifecycle unexpected stop state: %v", err)
		}
	})
}
