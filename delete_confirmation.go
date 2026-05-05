package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/admission/v1"
	"k8s.io/klog/v2"
)

const (
	defaultDeleteConfirmationDirectory       = "/var/lib/k8s-delete-interceptor"
	defaultDeleteConfirmationTTLSeconds      = 300
	defaultDeleteConfirmationConsumeSeconds  = 60
	defaultDeleteConfirmationAggregateSecond = 2
	defaultDeleteConfirmationPollSeconds     = 2
	defaultDeleteConfirmationMaxItems        = 20
	deleteConfirmationStateFileName          = "delete-confirmation-state.json"
	deleteConfirmationLockFileName           = "delete-confirmation-state.lock"
	deleteConfirmationPollLockFileName       = "delete-confirmation-poll.lock"
	deleteConfirmationStatusPending          = "pending"
	deleteConfirmationStatusSent             = "sent"
	deleteConfirmationStatusApproved         = "approved"
	deleteConfirmationStatusRejected         = "rejected"
	deleteConfirmationStatusConsumed         = "consumed"
	deleteConfirmationStatusExpired          = "expired"
	deleteConfirmationCallbackPrefix         = "dc"
)

type DeleteConfirmationConfig struct {
	Enabled                bool                     `json:"enabled" yaml:"enabled"`
	StateDirectory         string                   `json:"state_directory" yaml:"state_directory"`
	ChatIDs                []string                 `json:"chat_ids" yaml:"chat_ids"`
	TTLSeconds             int                      `json:"ttl_seconds" yaml:"ttl_seconds"`
	ConsumeWindowSeconds   int                      `json:"consume_window_seconds" yaml:"consume_window_seconds"`
	AggregateWindowSeconds int                      `json:"aggregate_window_seconds" yaml:"aggregate_window_seconds"`
	PollIntervalSeconds    int                      `json:"poll_interval_seconds" yaml:"poll_interval_seconds"`
	MaxItemsPerMessage     int                      `json:"max_items_per_message" yaml:"max_items_per_message"`
	Rules                  []DeleteConfirmationRule `json:"rules" yaml:"rules"`
}

type DeleteConfirmationRule struct {
	Users       []string `json:"users" yaml:"users"`
	TelegramIDs []string `json:"telegram_ids" yaml:"telegram_ids"`
}

type deleteConfirmationResourceKey struct {
	Cluster   string `json:"cluster"`
	User      string `json:"user"`
	Operation string `json:"operation"`
	Group     string `json:"group"`
	Resource  string `json:"resource"`
	Kind      string `json:"kind"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	ObjectUID string `json:"object_uid,omitempty"`
}

type deleteConfirmationEntry struct {
	Key              deleteConfirmationResourceKey `json:"key"`
	KeyID            string                        `json:"key_id"`
	GroupID          string                        `json:"group_id"`
	Status           string                        `json:"status"`
	CreatedAt        time.Time                     `json:"created_at"`
	ExpiresAt        time.Time                     `json:"expires_at"`
	ApprovedAt       time.Time                     `json:"approved_at,omitempty"`
	ConsumedAt       time.Time                     `json:"consumed_at,omitempty"`
	ApprovedBy       string                        `json:"approved_by,omitempty"`
	RequestedBy      string                        `json:"requested_by"`
	RequestedByShort string                        `json:"requested_by_short"`
	Kind             string                        `json:"kind"`
	Name             string                        `json:"name"`
	Namespace        string                        `json:"namespace"`
	ResourceDisplay  string                        `json:"resource_display"`
	MatchedPattern   string                        `json:"matched_pattern"`
	RollbackID       string                        `json:"rollback_id,omitempty"`
}

type deleteConfirmationGroup struct {
	ID               string                `json:"id"`
	Status           string                `json:"status"`
	CreatedAt        time.Time             `json:"created_at"`
	SendAfter        time.Time             `json:"send_after"`
	ExpiresAt        time.Time             `json:"expires_at"`
	ApprovedAt       time.Time             `json:"approved_at,omitempty"`
	RespondedAt      time.Time             `json:"responded_at,omitempty"`
	RespondedBy      string                `json:"responded_by,omitempty"`
	RequestedBy      string                `json:"requested_by"`
	RequestedByShort string                `json:"requested_by_short"`
	Kind             string                `json:"kind"`
	Namespace        string                `json:"namespace"`
	MatchedPattern   string                `json:"matched_pattern"`
	TelegramIDs      []string              `json:"telegram_ids"`
	EntryIDs         []string              `json:"entry_ids"`
	MessageText      string                `json:"message_text,omitempty"`
	SentMessages     []telegramSentMessage `json:"sent_messages,omitempty"`
}

type telegramSentMessage struct {
	ChatID    int64 `json:"chat_id"`
	MessageID int   `json:"message_id"`
}

type deleteConfirmationState struct {
	Entries        map[string]deleteConfirmationEntry `json:"entries"`
	Groups         map[string]deleteConfirmationGroup `json:"groups"`
	TelegramOffset int                                `json:"telegram_offset"`
}

type deleteConfirmationManager struct {
	enabled         bool
	directory       string
	statePath       string
	lockPath        string
	pollLockPath    string
	ttl             time.Duration
	consumeWindow   time.Duration
	aggregateWindow time.Duration
	pollInterval    time.Duration
	maxItems        int
	mu              sync.Mutex
	stopCh          chan struct{}
}

type telegramUpdateResponse struct {
	OK     bool             `json:"ok"`
	Result []telegramUpdate `json:"result"`
}

type telegramSendMessageResponse struct {
	OK     bool `json:"ok"`
	Result struct {
		MessageID int `json:"message_id"`
		Chat      struct {
			ID int64 `json:"id"`
		} `json:"chat"`
	} `json:"result"`
}

type telegramUpdate struct {
	UpdateID      int                    `json:"update_id"`
	CallbackQuery *telegramCallbackQuery `json:"callback_query,omitempty"`
}

type telegramCallbackQuery struct {
	ID      string       `json:"id"`
	From    telegramUser `json:"from"`
	Data    string       `json:"data"`
	Message *struct {
		Chat struct {
			ID int64 `json:"id"`
		} `json:"chat"`
		MessageID int `json:"message_id"`
	} `json:"message,omitempty"`
}

type telegramUser struct {
	ID        int64  `json:"id"`
	Username  string `json:"username"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}

var deleteConfirmer *deleteConfirmationManager

func applyDeleteConfirmationDefaults() {
	if !config.DeleteConfirmation.Enabled {
		return
	}
	if strings.TrimSpace(config.DeleteConfirmation.StateDirectory) == "" {
		config.DeleteConfirmation.StateDirectory = defaultDeleteConfirmationDirectory
	}
	if config.DeleteConfirmation.TTLSeconds <= 0 {
		config.DeleteConfirmation.TTLSeconds = defaultDeleteConfirmationTTLSeconds
	}
	if config.DeleteConfirmation.ConsumeWindowSeconds <= 0 {
		config.DeleteConfirmation.ConsumeWindowSeconds = defaultDeleteConfirmationConsumeSeconds
	}
	if config.DeleteConfirmation.AggregateWindowSeconds <= 0 {
		config.DeleteConfirmation.AggregateWindowSeconds = defaultDeleteConfirmationAggregateSecond
	}
	if config.DeleteConfirmation.PollIntervalSeconds <= 0 {
		config.DeleteConfirmation.PollIntervalSeconds = defaultDeleteConfirmationPollSeconds
	}
	if config.DeleteConfirmation.MaxItemsPerMessage <= 0 {
		config.DeleteConfirmation.MaxItemsPerMessage = defaultDeleteConfirmationMaxItems
	}
}

func newDeleteConfirmationManager(cfg DeleteConfirmationConfig) (*deleteConfirmationManager, error) {
	if !cfg.Enabled {
		return &deleteConfirmationManager{enabled: false}, nil
	}
	if strings.TrimSpace(config.Telegram.BotToken) == "" || len(resolveDeleteConfirmationChatIDs()) == 0 {
		return &deleteConfirmationManager{enabled: false}, fmt.Errorf("delete confirmation requires global telegram.bot_token and at least one delete_confirmation.chat_ids or global telegram.chat_ids")
	}
	if len(cfg.Rules) == 0 {
		return &deleteConfirmationManager{enabled: false}, fmt.Errorf("delete confirmation enabled but no rules configured")
	}

	directory := strings.TrimSpace(cfg.StateDirectory)
	if directory == "" {
		directory = defaultDeleteConfirmationDirectory
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create delete confirmation directory '%s': %w", directory, err)
	}

	manager := &deleteConfirmationManager{
		enabled:         true,
		directory:       directory,
		statePath:       filepath.Join(directory, deleteConfirmationStateFileName),
		lockPath:        filepath.Join(directory, deleteConfirmationLockFileName),
		pollLockPath:    filepath.Join(directory, deleteConfirmationPollLockFileName),
		ttl:             time.Duration(cfg.TTLSeconds) * time.Second,
		consumeWindow:   time.Duration(cfg.ConsumeWindowSeconds) * time.Second,
		aggregateWindow: time.Duration(cfg.AggregateWindowSeconds) * time.Second,
		pollInterval:    time.Duration(cfg.PollIntervalSeconds) * time.Second,
		maxItems:        cfg.MaxItemsPerMessage,
		stopCh:          make(chan struct{}),
	}

	if err := manager.withStateLock(func(state *deleteConfirmationState) error {
		manager.cleanupExpiredLocked(state, time.Now())
		return nil
	}); err != nil {
		return nil, err
	}

	go manager.run()
	return manager, nil
}

func (m *deleteConfirmationManager) Stop() {
	if m == nil || !m.enabled {
		return
	}
	close(m.stopCh)
}

func (m *deleteConfirmationManager) run() {
	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.sendDueApprovalMessages()
			m.pollTelegramCallbacks()
		case <-m.stopCh:
			return
		}
	}
}

func (m *deleteConfirmationManager) ConsumeApproval(req *v1.AdmissionRequest) (bool, string) {
	if m == nil || !m.enabled || req == nil {
		return false, ""
	}

	key := buildDeleteConfirmationResourceKey(req)
	keyID := deleteConfirmationKeyID(key)
	now := time.Now()
	var reason string
	consumed := false

	err := m.withStateLock(func(state *deleteConfirmationState) error {
		m.cleanupExpiredLocked(state, now)
		entry, ok := state.Entries[keyID]
		if !ok || entry.Status != deleteConfirmationStatusApproved {
			return nil
		}
		if now.After(entry.ExpiresAt) {
			entry.Status = deleteConfirmationStatusExpired
			state.Entries[keyID] = entry
			return nil
		}
		if !entry.ApprovedAt.IsZero() && m.consumeWindow > 0 && now.After(entry.ApprovedAt.Add(m.consumeWindow)) {
			entry.Status = deleteConfirmationStatusExpired
			state.Entries[keyID] = entry
			return nil
		}

		entry.Status = deleteConfirmationStatusConsumed
		entry.ConsumedAt = now
		state.Entries[keyID] = entry
		consumed = true
		reason = fmt.Sprintf("Delete allowed by one-time Telegram approval for %s.", entry.ResourceDisplay)
		return nil
	})
	if err != nil {
		klog.Errorf("Failed to consume delete confirmation approval: %v", err)
		return false, ""
	}

	return consumed, reason
}

func (m *deleteConfirmationManager) RequestApproval(req *v1.AdmissionRequest, matchedPattern string, rollbackID string) (bool, string, string) {
	if m == nil || !m.enabled || req == nil {
		return false, "", ""
	}

	telegramIDs, userPattern, userMatcher := matchDeleteConfirmationRule(req.UserInfo.Username)
	if len(telegramIDs) == 0 {
		return false, "", ""
	}

	key := buildDeleteConfirmationResourceKey(req)
	keyID := deleteConfirmationKeyID(key)
	now := time.Now()
	entry := deleteConfirmationEntry{
		Key:              key,
		KeyID:            keyID,
		Status:           deleteConfirmationStatusPending,
		CreatedAt:        now,
		ExpiresAt:        now.Add(m.ttl),
		RequestedBy:      req.UserInfo.Username,
		RequestedByShort: formatNotificationUser(req.UserInfo.Username),
		Kind:             req.Kind.Kind,
		Name:             req.Name,
		Namespace:        formatNamespace(req.Namespace),
		ResourceDisplay:  formatResource(req.Kind.Kind, req.Name, req.Namespace),
		MatchedPattern:   matchedPattern,
		RollbackID:       rollbackID,
	}
	groupID := deleteConfirmationGroupID(req, matchedPattern, userPattern, now.Truncate(m.aggregateWindow))
	entry.GroupID = groupID

	err := m.withStateLock(func(state *deleteConfirmationState) error {
		m.cleanupExpiredLocked(state, now)
		if existing, ok := state.Entries[keyID]; ok {
			switch existing.Status {
			case deleteConfirmationStatusPending, deleteConfirmationStatusSent:
				return nil
			case deleteConfirmationStatusApproved:
				return nil
			}
		}

		group := state.Groups[groupID]
		if group.ID == "" {
			group = deleteConfirmationGroup{
				ID:               groupID,
				Status:           deleteConfirmationStatusPending,
				CreatedAt:        now,
				SendAfter:        now.Add(m.aggregateWindow),
				ExpiresAt:        now.Add(m.ttl),
				RequestedBy:      req.UserInfo.Username,
				RequestedByShort: formatNotificationUser(req.UserInfo.Username),
				Kind:             req.Kind.Kind,
				Namespace:        formatNamespace(req.Namespace),
				MatchedPattern:   matchedPattern,
				TelegramIDs:      normalizeTelegramIDs(telegramIDs),
				EntryIDs:         []string{},
			}
		}
		if !stringSliceContains(group.EntryIDs, keyID) {
			group.EntryIDs = append(group.EntryIDs, keyID)
			sort.Strings(group.EntryIDs)
		}

		state.Entries[keyID] = entry
		state.Groups[groupID] = group
		return nil
	})
	if err != nil {
		klog.Errorf("Failed to create delete confirmation request: %v", err)
		return false, "", ""
	}

	reason := fmt.Sprintf("Delete requires Telegram approval. Confirm deletion for %s, then retry the same delete command.", formatResource(req.Kind.Kind, req.Name, req.Namespace))
	policy := fmt.Sprintf("delete_confirmation_pending:%s:%s:%s", userPattern, userMatcher, matchedPattern)
	return true, reason, policy
}

func matchDeleteConfirmationRule(user string) ([]string, string, string) {
	for _, rule := range config.DeleteConfirmation.Rules {
		if len(rule.TelegramIDs) == 0 {
			continue
		}
		for _, pattern := range rule.Users {
			matched, matcher, err := matchPattern(pattern, user)
			if err != nil {
				klog.Errorf("Invalid delete confirmation user pattern '%s': %v", pattern, err)
				continue
			}
			if matched {
				return rule.TelegramIDs, pattern, matcher
			}
		}
	}
	return nil, "", ""
}

func (m *deleteConfirmationManager) sendDueApprovalMessages() {
	now := time.Now()
	groups := make([]deleteConfirmationGroup, 0)

	if err := m.withStateLock(func(state *deleteConfirmationState) error {
		m.cleanupExpiredLocked(state, now)
		for _, group := range state.Groups {
			if group.Status == deleteConfirmationStatusPending && !now.Before(group.SendAfter) {
				group.Status = deleteConfirmationStatusSent
				state.Groups[group.ID] = group
				for _, entryID := range group.EntryIDs {
					entry := state.Entries[entryID]
					if entry.Status == deleteConfirmationStatusPending {
						entry.Status = deleteConfirmationStatusSent
						state.Entries[entryID] = entry
					}
				}
				groups = append(groups, group)
			}
		}
		return nil
	}); err != nil {
		klog.Errorf("Failed to collect due delete confirmation messages: %v", err)
		return
	}

	for _, group := range groups {
		if err := m.sendApprovalMessage(group); err != nil {
			klog.Errorf("Failed to send delete confirmation message for group %s: %v", group.ID, err)
			_ = m.withStateLock(func(state *deleteConfirmationState) error {
				current := state.Groups[group.ID]
				if current.ID != "" && current.Status == deleteConfirmationStatusSent {
					current.Status = deleteConfirmationStatusPending
					current.SendAfter = time.Now().Add(m.pollInterval)
					state.Groups[group.ID] = current
				}
				return nil
			})
		}
	}
}

func (m *deleteConfirmationManager) sendApprovalMessage(group deleteConfirmationGroup) error {
	entries := make([]deleteConfirmationEntry, 0, len(group.EntryIDs))
	if err := m.withStateLock(func(state *deleteConfirmationState) error {
		for _, entryID := range group.EntryIDs {
			entry, ok := state.Entries[entryID]
			if ok && (entry.Status == deleteConfirmationStatusSent || entry.Status == deleteConfirmationStatusPending) {
				entries = append(entries, entry)
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}

	message := m.buildApprovalMessage(group, entries)
	replyMarkup := map[string]interface{}{
		"inline_keyboard": m.buildApprovalKeyboard(group, entries),
	}
	replyPayload, err := json.Marshal(replyMarkup)
	if err != nil {
		return err
	}

	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", config.Telegram.BotToken)
	sentMessages := make([]telegramSentMessage, 0)
	for _, chatID := range resolveDeleteConfirmationChatIDs() {
		values := url.Values{}
		values.Add("chat_id", chatID)
		values.Add("text", message)
		values.Add("parse_mode", "MarkdownV2")
		values.Add("reply_markup", string(replyPayload))

		resp, err := httpClient.PostForm(apiURL, values)
		if err != nil {
			return err
		}
		body, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("telegram sendMessage status %d: %s", resp.StatusCode, string(body))
		}
		var sendResult telegramSendMessageResponse
		if err := json.Unmarshal(body, &sendResult); err != nil {
			return err
		}
		if sendResult.OK {
			sentMessages = append(sentMessages, telegramSentMessage{
				ChatID:    sendResult.Result.Chat.ID,
				MessageID: sendResult.Result.MessageID,
			})
		}
	}

	if len(sentMessages) > 0 {
		if err := m.withStateLock(func(state *deleteConfirmationState) error {
			current := state.Groups[group.ID]
			if current.ID != "" {
				current.MessageText = message
				current.SentMessages = append(current.SentMessages, sentMessages...)
				state.Groups[group.ID] = current
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (m *deleteConfirmationManager) buildApprovalKeyboard(group deleteConfirmationGroup, entries []deleteConfirmationEntry) [][]map[string]string {
	keyboard := [][]map[string]string{
		{
			{"text": "确认删除", "callback_data": deleteConfirmationCallbackData("approve", group.ID)},
			{"text": "拒绝", "callback_data": deleteConfirmationCallbackData("reject", group.ID)},
		},
	}

	limit := m.maxItems
	if limit <= 0 {
		limit = defaultDeleteConfirmationMaxItems
	}
	for i, entry := range entries {
		if i >= limit {
			break
		}
		if strings.TrimSpace(entry.RollbackID) == "" {
			continue
		}
		keyboard = append(keyboard, []map[string]string{
			{"text": "回滚 " + limitButtonText(entry.Name), "callback_data": rollbackCallbackDataWithAction(rollbackActionApply, entry.RollbackID)},
			{"text": "下载 YAML", "callback_data": rollbackCallbackDataWithAction(rollbackActionDownload, entry.RollbackID)},
		})
	}

	return keyboard
}

func limitButtonText(value string) string {
	runes := []rune(strings.TrimSpace(value))
	if len(runes) <= 28 {
		return string(runes)
	}
	return string(runes[:25]) + "..."
}

func (m *deleteConfirmationManager) buildApprovalMessage(group deleteConfirmationGroup, entries []deleteConfirmationEntry) string {
	lines := []string{
		"⚠️ *K8s 删除审批*",
		"",
		fmt.Sprintf("*用户*: `%s`", escapeMarkdownV2(group.RequestedByShort)),
		fmt.Sprintf("*资源类型*: `%s`", escapeMarkdownV2(group.Kind)),
		fmt.Sprintf("*命名空间*: `%s`", escapeMarkdownV2(group.Namespace)),
		fmt.Sprintf("*数量*: `%d`", len(entries)),
		"",
		"*资源列表*:",
	}
	lines = append(lines[:2], append([]string{fmt.Sprintf("*集群*: `%s`", escapeMarkdownV2(config.ClusterName))}, lines[2:]...)...)

	limit := m.maxItems
	if limit <= 0 {
		limit = defaultDeleteConfirmationMaxItems
	}
	for i, entry := range entries {
		if i >= limit {
			lines = append(lines, fmt.Sprintf("还有 %d 个资源未展示", len(entries)-limit))
			break
		}
		lines = append(lines, fmt.Sprintf("• `%s`", escapeMarkdownV2(entry.Name)))
	}

	lines = append(lines, "")
	lines = append(lines, fmt.Sprintf("确认后 %d 秒内重试相同删除命令，授权只生效一次。", int(m.consumeWindow.Seconds())))
	return strings.Join(lines, "\n")
}

func (m *deleteConfirmationManager) pollTelegramCallbacks() {
	locked, err := m.acquirePollLock()
	if err != nil {
		klog.Errorf("Failed to acquire Telegram poll lock: %v", err)
		return
	}
	if !locked {
		return
	}
	defer os.Remove(m.pollLockPath)

	offset, err := m.telegramOffset()
	if err != nil {
		klog.Errorf("Failed to read Telegram update offset: %v", err)
		return
	}

	updates, err := fetchTelegramUpdates(offset)
	if err != nil {
		klog.Errorf("Failed to fetch Telegram updates: %v", err)
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
		if m.handleTelegramCallback(*update.CallbackQuery) {
			continue
		}
		if rollbacker != nil {
			rollbacker.HandleTelegramCallback(*update.CallbackQuery)
		}
	}

	if err := m.setTelegramOffset(maxUpdateID + 1); err != nil {
		klog.Errorf("Failed to update Telegram offset: %v", err)
	}
}

func fetchTelegramUpdates(offset int) ([]telegramUpdate, error) {
	if strings.TrimSpace(config.Telegram.BotToken) == "" {
		return nil, nil
	}
	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/getUpdates", config.Telegram.BotToken)
	values := url.Values{}
	if offset > 0 {
		values.Add("offset", strconv.Itoa(offset))
	}
	values.Add("timeout", "0")
	values.Add("allowed_updates", `["callback_query"]`)

	resp, err := httpClient.PostForm(apiURL, values)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("telegram getUpdates status %d: %s", resp.StatusCode, string(body))
	}

	var result telegramUpdateResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	if !result.OK {
		return nil, fmt.Errorf("telegram getUpdates returned ok=false")
	}

	return result.Result, nil
}

func (m *deleteConfirmationManager) handleTelegramCallback(callback telegramCallbackQuery) bool {
	action, groupID, ok := parseDeleteConfirmationCallbackData(callback.Data)
	if !ok {
		return false
	}

	telegramID := strconv.FormatInt(callback.From.ID, 10)
	var answer string
	var approved bool
	var rejected bool
	var messageText string
	var sentMessages []telegramSentMessage

	err := m.withStateLock(func(state *deleteConfirmationState) error {
		m.cleanupExpiredLocked(state, time.Now())
		group := state.Groups[groupID]
		if group.ID == "" {
			answer = "审批请求不存在或已过期。"
			return nil
		}
		if !stringSliceContains(group.TelegramIDs, telegramID) {
			answer = "你没有这个删除审批权限。"
			return nil
		}
		if group.Status == deleteConfirmationStatusApproved {
			answer = "该删除审批已经确认。"
			return nil
		}
		if group.Status == deleteConfirmationStatusRejected {
			answer = "该删除审批已经拒绝。"
			return nil
		}

		now := time.Now()
		if action == "approve" {
			group.Status = deleteConfirmationStatusApproved
			group.ApprovedAt = now
			group.RespondedAt = now
			group.RespondedBy = telegramID
			for _, entryID := range group.EntryIDs {
				entry := state.Entries[entryID]
				if entry.Status == deleteConfirmationStatusSent || entry.Status == deleteConfirmationStatusPending {
					entry.Status = deleteConfirmationStatusApproved
					entry.ApprovedAt = now
					entry.ApprovedBy = telegramID
					if m.consumeWindow > 0 {
						entry.ExpiresAt = now.Add(m.consumeWindow)
					}
					state.Entries[entryID] = entry
				}
			}
			approved = true
			answer = "已确认，请在有效期内重试删除。"
		} else {
			group.Status = deleteConfirmationStatusRejected
			group.RespondedAt = now
			group.RespondedBy = telegramID
			for _, entryID := range group.EntryIDs {
				entry := state.Entries[entryID]
				if entry.Status == deleteConfirmationStatusSent || entry.Status == deleteConfirmationStatusPending {
					entry.Status = deleteConfirmationStatusRejected
					state.Entries[entryID] = entry
				}
			}
			rejected = true
			answer = "已拒绝，删除继续被拦截。"
		}
		state.Groups[groupID] = group
		messageText = group.MessageText
		sentMessages = append([]telegramSentMessage(nil), group.SentMessages...)
		return nil
	})
	if err != nil {
		klog.Errorf("Failed to process delete confirmation callback: %v", err)
		answer = "处理审批失败，请查看服务日志。"
	}

	answerTelegramCallback(callback.ID, answer)
	if callback.Message != nil && (approved || rejected) {
		status := "已确认"
		if rejected {
			status = "已拒绝"
		}
		m.syncApprovalMessagesAfterCallback(callback.Message.Chat.ID, callback.Message.MessageID, status, messageText, sentMessages)
	}
	return true
}

func (m *deleteConfirmationManager) telegramOffset() (int, error) {
	offset := 0
	err := m.withStateLock(func(state *deleteConfirmationState) error {
		offset = state.TelegramOffset
		return nil
	})
	return offset, err
}

func (m *deleteConfirmationManager) syncApprovalMessagesAfterCallback(clickedChatID int64, clickedMessageID int, status string, messageText string, sentMessages []telegramSentMessage) {
	for _, sent := range sentMessages {
		if sent.ChatID == clickedChatID && sent.MessageID == clickedMessageID {
			if err := editTelegramMessageText(sent.ChatID, sent.MessageID, appendApprovalStatus(messageText, status)); err != nil {
				klog.Errorf("Failed to update clicked delete confirmation message: %v", err)
				_ = editTelegramMessageReplyMarkup(sent.ChatID, sent.MessageID, status)
			}
			continue
		}

		if err := deleteTelegramMessage(sent.ChatID, sent.MessageID); err != nil {
			klog.Errorf("Failed to delete stale delete confirmation message in chat %d message %d: %v", sent.ChatID, sent.MessageID, err)
		}
	}
}

func appendApprovalStatus(messageText string, status string) string {
	if strings.TrimSpace(messageText) == "" {
		return fmt.Sprintf("*状态*: `%s`", escapeMarkdownV2(status))
	}

	return messageText + "\n\n" + fmt.Sprintf("*状态*: `%s`", escapeMarkdownV2(status))
}

func (m *deleteConfirmationManager) setTelegramOffset(offset int) error {
	return m.withStateLock(func(state *deleteConfirmationState) error {
		if offset > state.TelegramOffset {
			state.TelegramOffset = offset
		}
		return nil
	})
}

func answerTelegramCallback(callbackID string, text string) {
	if strings.TrimSpace(callbackID) == "" || strings.TrimSpace(config.Telegram.BotToken) == "" {
		return
	}
	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/answerCallbackQuery", config.Telegram.BotToken)
	values := url.Values{}
	values.Add("callback_query_id", callbackID)
	values.Add("text", text)
	values.Add("show_alert", "true")
	resp, err := httpClient.PostForm(apiURL, values)
	if err != nil {
		klog.Errorf("Failed to answer Telegram callback: %v", err)
		return
	}
	_, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
}

func editTelegramMessageReplyMarkup(chatID int64, messageID int, status string) error {
	if strings.TrimSpace(config.Telegram.BotToken) == "" {
		return nil
	}
	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/editMessageReplyMarkup", config.Telegram.BotToken)
	values := url.Values{}
	values.Add("chat_id", strconv.FormatInt(chatID, 10))
	values.Add("message_id", strconv.Itoa(messageID))
	values.Add("reply_markup", `{"inline_keyboard":[]}`)
	resp, err := httpClient.PostForm(apiURL, values)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("editMessageReplyMarkup %s status %d: %s", status, resp.StatusCode, string(body))
	}
	return nil
}

func editTelegramMessageText(chatID int64, messageID int, text string) error {
	if strings.TrimSpace(config.Telegram.BotToken) == "" {
		return nil
	}
	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/editMessageText", config.Telegram.BotToken)
	values := url.Values{}
	values.Add("chat_id", strconv.FormatInt(chatID, 10))
	values.Add("message_id", strconv.Itoa(messageID))
	values.Add("text", text)
	values.Add("parse_mode", "MarkdownV2")
	values.Add("reply_markup", `{"inline_keyboard":[]}`)
	resp, err := httpClient.PostForm(apiURL, values)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("editMessageText status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func deleteTelegramMessage(chatID int64, messageID int) error {
	if strings.TrimSpace(config.Telegram.BotToken) == "" {
		return nil
	}
	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/deleteMessage", config.Telegram.BotToken)
	values := url.Values{}
	values.Add("chat_id", strconv.FormatInt(chatID, 10))
	values.Add("message_id", strconv.Itoa(messageID))
	resp, err := httpClient.PostForm(apiURL, values)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("deleteMessage status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func (m *deleteConfirmationManager) withStateLock(fn func(*deleteConfirmationState) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := acquireFileLock(m.lockPath, 1500*time.Millisecond, 10*time.Second); err != nil {
		return err
	}
	defer os.Remove(m.lockPath)

	state, err := m.loadState()
	if err != nil {
		return err
	}
	if err := fn(state); err != nil {
		return err
	}
	return m.saveState(state)
}

func (m *deleteConfirmationManager) acquirePollLock() (bool, error) {
	err := acquireFileLock(m.pollLockPath, 0, 10*time.Second)
	if err == nil {
		return true, nil
	}
	if os.IsExist(err) {
		return false, nil
	}
	return false, err
}

func acquireFileLock(path string, wait time.Duration, staleAfter time.Duration) error {
	deadline := time.Now().Add(wait)
	for {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			_, _ = f.WriteString(time.Now().Format(time.RFC3339Nano))
			_ = f.Close()
			return nil
		}
		if !os.IsExist(err) {
			return err
		}

		if info, statErr := os.Stat(path); statErr == nil && staleAfter > 0 && time.Since(info.ModTime()) > staleAfter {
			_ = os.Remove(path)
			continue
		}
		if wait <= 0 {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for delete confirmation state lock")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (m *deleteConfirmationManager) loadState() (*deleteConfirmationState, error) {
	data, err := os.ReadFile(m.statePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		return newDeleteConfirmationState(), nil
	}
	var state deleteConfirmationState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	if state.Entries == nil {
		state.Entries = map[string]deleteConfirmationEntry{}
	}
	if state.Groups == nil {
		state.Groups = map[string]deleteConfirmationGroup{}
	}
	return &state, nil
}

func newDeleteConfirmationState() *deleteConfirmationState {
	return &deleteConfirmationState{
		Entries: map[string]deleteConfirmationEntry{},
		Groups:  map[string]deleteConfirmationGroup{},
	}
}

func (m *deleteConfirmationManager) saveState(state *deleteConfirmationState) error {
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

func (m *deleteConfirmationManager) cleanupExpiredLocked(state *deleteConfirmationState, now time.Time) {
	for id, entry := range state.Entries {
		if (entry.Status == deleteConfirmationStatusPending || entry.Status == deleteConfirmationStatusSent || entry.Status == deleteConfirmationStatusApproved) && now.After(entry.ExpiresAt) {
			entry.Status = deleteConfirmationStatusExpired
			state.Entries[id] = entry
			continue
		}
		if (entry.Status == deleteConfirmationStatusConsumed || entry.Status == deleteConfirmationStatusRejected || entry.Status == deleteConfirmationStatusExpired) && now.After(entry.CreatedAt.Add(m.ttl)) {
			delete(state.Entries, id)
		}
	}
	for id, group := range state.Groups {
		if (group.Status == deleteConfirmationStatusPending || group.Status == deleteConfirmationStatusSent || group.Status == deleteConfirmationStatusApproved) && now.After(group.ExpiresAt) {
			group.Status = deleteConfirmationStatusExpired
			state.Groups[id] = group
			continue
		}
		if (group.Status == deleteConfirmationStatusRejected || group.Status == deleteConfirmationStatusExpired || group.Status == deleteConfirmationStatusApproved) && now.After(group.CreatedAt.Add(m.ttl)) {
			delete(state.Groups, id)
		}
	}
}

func buildDeleteConfirmationResourceKey(req *v1.AdmissionRequest) deleteConfirmationResourceKey {
	return deleteConfirmationResourceKey{
		Cluster:   config.ClusterName,
		User:      req.UserInfo.Username,
		Operation: string(req.Operation),
		Group:     req.Resource.Group,
		Resource:  req.Resource.Resource,
		Kind:      req.Kind.Kind,
		Namespace: req.Namespace,
		Name:      req.Name,
		ObjectUID: admissionObjectUID(req),
	}
}

func admissionObjectUID(req *v1.AdmissionRequest) string {
	if req == nil || len(req.OldObject.Raw) == 0 {
		return ""
	}
	var object struct {
		Metadata struct {
			UID string `json:"uid"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(req.OldObject.Raw, &object); err != nil {
		return ""
	}
	return object.Metadata.UID
}

func deleteConfirmationKeyID(key deleteConfirmationResourceKey) string {
	payload, _ := json.Marshal(key)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func deleteConfirmationGroupID(req *v1.AdmissionRequest, matchedPattern string, userPattern string, bucket time.Time) string {
	parts := []string{
		config.ClusterName,
		req.UserInfo.Username,
		req.Kind.Kind,
		req.Namespace,
		matchedPattern,
		userPattern,
		bucket.Format(time.RFC3339),
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(sum[:])[:24]
}

func deleteConfirmationCallbackData(action string, groupID string) string {
	return fmt.Sprintf("%s:%s:%s", deleteConfirmationCallbackPrefix, action, groupID)
}

func parseDeleteConfirmationCallbackData(data string) (string, string, bool) {
	parts := strings.Split(data, ":")
	if len(parts) != 3 || parts[0] != deleteConfirmationCallbackPrefix {
		return "", "", false
	}
	if parts[1] != "approve" && parts[1] != "reject" {
		return "", "", false
	}
	return parts[1], parts[2], true
}

func normalizeTelegramIDs(ids []string) []string {
	result := make([]string, 0, len(ids))
	seen := map[string]struct{}{}
	for _, id := range ids {
		trimmed := strings.TrimSpace(id)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	sort.Strings(result)
	return result
}

func resolveDeleteConfirmationChatIDs() []string {
	if len(config.DeleteConfirmation.ChatIDs) > 0 {
		return normalizeTelegramIDs(config.DeleteConfirmation.ChatIDs)
	}
	return normalizeTelegramIDs(config.Telegram.ChatIDs)
}

func stringSliceContains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
