package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"text/template"
	"time"
)

type telegramSendMessageRequest struct {
	ChatID      string `json:"chat_id"`
	Text        string `json:"text"`
	ParseMode   string `json:"parse_mode,omitempty"`
	ReplyMarkup any    `json:"reply_markup,omitempty"`
}

func (a *App) notifyEvent(ctx context.Context, cfg *RuntimeConfig, ev *AdmissionEvent, pd PolicyDecision) {
	if cfg == nil || ev == nil || pd.Rule == nil {
		return
	}
	if !cfg.Telegram.Enabled {
		log.Printf("telegram notify skipped: global disabled rule=%s event=%s", pd.Rule.ID, ev.ID)
		return
	}
	bind := pd.Rule.Notify
	if !bind.Enabled {
		log.Printf("telegram notify skipped: rule notify disabled rule=%s event=%s decision=%s", pd.Rule.ID, ev.ID, pd.Decision)
		return
	}
	tpl := findTemplate(cfg, bind.TemplateID)
	if tpl == nil || !tpl.Enabled {
		return
	}
	targets := resolveTelegramTargets(cfg, bind)
	log.Printf("telegram notify resolving: rule=%s event=%s template=%s bots=%v chats=%v users=%v targets=%d", pd.Rule.ID, ev.ID, bind.TemplateID, bind.TelegramBotIDs, bind.TelegramChatIDs, bind.TelegramUserIDs, len(targets))
	if len(targets) == 0 {
		log.Printf("telegram notify skipped: no enabled target rule=%s template=%s", pd.Rule.ID, tpl.ID)
		return
	}
	msg, err := renderTemplate(cfg, ev, pd, *tpl)
	if err != nil {
		log.Printf("telegram render template failed: rule=%s template=%s err=%v", pd.Rule.ID, tpl.ID, err)
		return
	}
	for _, target := range targets {
		bot := findTelegramBot(cfg, target.BotID)
		if bot == nil || !bot.Enabled {
			log.Printf("telegram notify skipped: bot not found or disabled bot_id=%s rule=%s", target.BotID, pd.Rule.ID)
			continue
		}
		token := bot.Token
		if token == "" && bot.TokenEnv != "" {
			token = os.Getenv(bot.TokenEnv)
		}
		if token == "" {
			log.Printf("telegram notify skipped: empty token bot_id=%s token_env=%s", bot.ID, bot.TokenEnv)
			continue
		}
		if err := sendTelegram(ctx, token, target.ChatID, msg, tpl.ParseMode, eventKeyboard(ev)); err != nil {
			log.Printf("telegram notify failed: bot_id=%s chat_id=%s rule=%s err=%v", bot.ID, target.ChatID, pd.Rule.ID, err)
		} else {
			log.Printf("telegram notify sent: bot_id=%s chat_id=%s rule=%s event=%s", bot.ID, target.ChatID, pd.Rule.ID, ev.ID)
		}
	}
}

type telegramTarget struct {
	BotID  string
	ChatID string
	Name   string
}

func findTemplate(cfg *RuntimeConfig, id string) *NotificationTemplate {
	for i := range cfg.NotificationTemplates {
		if cfg.NotificationTemplates[i].ID == id {
			return &cfg.NotificationTemplates[i]
		}
	}
	if len(cfg.NotificationTemplates) > 0 {
		return &cfg.NotificationTemplates[0]
	}
	return nil
}
func findTelegramBot(cfg *RuntimeConfig, id string) *TelegramBot {
	for i := range cfg.Telegram.Bots {
		if cfg.Telegram.Bots[i].ID == id {
			return &cfg.Telegram.Bots[i]
		}
	}
	return nil
}
func findTelegramUser(cfg *RuntimeConfig, id string) *TelegramUser {
	for i := range cfg.Telegram.Users {
		if cfg.Telegram.Users[i].ID == id || cfg.Telegram.Users[i].TelegramID == id {
			return &cfg.Telegram.Users[i]
		}
	}
	return nil
}

func resolveTelegramTargets(cfg *RuntimeConfig, bind NotificationBinding) []telegramTarget {
	botIDs := map[string]bool{}
	chatIDs := map[string]bool{}
	userIDs := map[string]bool{}
	for _, id := range bind.TelegramBotIDs {
		if strings.TrimSpace(id) != "" {
			botIDs[id] = true
		}
	}
	for _, id := range bind.TelegramChatIDs {
		if strings.TrimSpace(id) != "" {
			chatIDs[id] = true
		}
	}
	for _, id := range bind.TelegramUserIDs {
		if strings.TrimSpace(id) != "" {
			userIDs[id] = true
		}
	}
	allBots := len(botIDs) == 0
	allChats := len(chatIDs) == 0
	out := []telegramTarget{}
	seen := map[string]bool{}
	add := func(t telegramTarget) {
		if t.BotID == "" || t.ChatID == "" {
			return
		}
		key := t.BotID + "|" + t.ChatID
		if seen[key] {
			return
		}
		seen[key] = true
		out = append(out, t)
	}
	for _, c := range cfg.Telegram.Chats {
		if !c.Enabled {
			continue
		}
		if !allBots && !botIDs[c.BotID] {
			continue
		}
		if !allChats && !chatIDs[c.ID] {
			continue
		}
		add(telegramTarget{BotID: c.BotID, ChatID: c.ChatID, Name: c.Name})
	}
	if len(userIDs) > 0 {
		for _, u := range cfg.Telegram.Users {
			if !u.Enabled || u.TelegramID == "" {
				continue
			}
			if !userIDs[u.ID] && !userIDs[u.TelegramID] {
				continue
			}
			for _, b := range cfg.Telegram.Bots {
				if !b.Enabled {
					continue
				}
				if !allBots && !botIDs[b.ID] {
					continue
				}
				add(telegramTarget{BotID: b.ID, ChatID: u.TelegramID, Name: u.DisplayName})
			}
		}
	}
	return out
}

func renderTemplate(cfg *RuntimeConfig, ev *AdmissionEvent, pd PolicyDecision, tpl NotificationTemplate) (string, error) {
	web := strings.TrimRight(os.Getenv("WEB_BASE_URL"), "/")
	eventURL := ""
	if web != "" {
		eventURL = web + "/?event=" + ev.ID
	}
	actorDisplay := ev.User
	approvers := []string{}
	if pd.Rule != nil {
		for _, id := range pd.Rule.Approval.ApproverTelegramUsers {
			if u := findTelegramUser(cfg, id); u != nil {
				approvers = append(approvers, telegramMention(*u, tpl.ParseMode))
			}
		}
	}
	data := map[string]string{
		"cluster": escapeForMode(ev.Cluster, tpl.ParseMode), "operation": escapeForMode(ev.Operation, tpl.ParseMode), "kind": escapeForMode(ev.Kind, tpl.ParseMode),
		"namespace": escapeForMode(ev.Namespace, tpl.ParseMode), "name": escapeForMode(ev.Name, tpl.ParseMode), "resource": escapeForMode(ev.Resource, tpl.ParseMode),
		"user": escapeForMode(ev.User, tpl.ParseMode), "actor_display": escapeForMode(actorDisplay, tpl.ParseMode), "rule_name": escapeForMode(ev.RuleName, tpl.ParseMode),
		"reason": escapeForMode(ev.Reason, tpl.ParseMode), "change_class": escapeForMode(ev.ChangeClass, tpl.ParseMode), "change_summary": escapeForMode(ev.ChangeSummary, tpl.ParseMode),
		"request_uid": escapeForMode(ev.RequestUID, tpl.ParseMode), "event_id": escapeForMode(ev.ID, tpl.ParseMode), "rollback_id": escapeForMode(ev.RollbackID, tpl.ParseMode),
		"event_url": escapeForMode(eventURL, tpl.ParseMode), "time": escapeForMode(ev.Time.Format(time.RFC3339), tpl.ParseMode), "approvers_mentions": strings.Join(approvers, " "),
	}
	t, err := template.New(tpl.ID).Option("missingkey=zero").Parse(tpl.Body)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func telegramMention(u TelegramUser, mode string) string {
	label := u.DisplayName
	if label == "" {
		label = u.Alias
	}
	if label == "" {
		label = u.Username
	}
	if label == "" {
		label = u.TelegramID
	}
	if u.MentionEnabled && u.Username != "" {
		return "@" + escapeForMode(strings.TrimPrefix(u.Username, "@"), mode)
	}
	return escapeForMode(label, mode)
}

func escapeForMode(s, mode string) string {
	if strings.EqualFold(mode, "MarkdownV2") {
		replacer := strings.NewReplacer("_", "\\_", "*", "\\*", "[", "\\[", "]", "\\]", "(", "\\(", ")", "\\)", "~", "\\~", "`", "\\`", ">", "\\>", "#", "\\#", "+", "\\+", "-", "\\-", "=", "\\=", "|", "\\|", "{", "\\{", "}", "\\}", ".", "\\.", "!", "\\!")
		return replacer.Replace(s)
	}
	return s
}

func sendTelegram(ctx context.Context, token, chatID, text, parseMode string, markup any) error {
	token = normalizeTelegramToken(token)
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	body := telegramSendMessageRequest{ChatID: chatID, Text: text, ParseMode: parseMode, ReplyMarkup: markup}
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", token), bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode/100 != 2 {
		detail := strings.TrimSpace(string(bodyBytes))
		if resp.StatusCode == http.StatusNotFound {
			return fmt.Errorf("telegram API 404 Not Found: Bot Token 无效、粘贴了错误 token，或 token_env 读取到的环境变量不正确；Telegram 返回：%s", detail)
		}
		return fmt.Errorf("telegram status %s: %s", resp.Status, detail)
	}
	return nil
}

func normalizeTelegramToken(token string) string {
	token = strings.TrimSpace(token)
	token = strings.Trim(token, "\"'")
	if strings.HasPrefix(token, "https://api.telegram.org/bot") {
		token = strings.TrimPrefix(token, "https://api.telegram.org/bot")
		if idx := strings.Index(token, "/"); idx >= 0 {
			token = token[:idx]
		}
	}
	if strings.HasPrefix(strings.ToLower(token), "bot") {
		token = token[3:]
	}
	return strings.TrimSpace(token)
}

func tokenFingerprint(token string) string {
	token = normalizeTelegramToken(token)
	if token == "" {
		return "empty"
	}
	parts := strings.SplitN(token, ":", 2)
	if len(parts) == 2 {
		suffix := parts[1]
		if len(suffix) > 4 {
			suffix = suffix[len(suffix)-4:]
		}
		return fmt.Sprintf("id=%s len=%d suffix=***%s", parts[0], len(token), suffix)
	}
	return fmt.Sprintf("len=%d no-colon", len(token))
}

func validateTelegramBotToken(ctx context.Context, token string) (string, error) {
	token = normalizeTelegramToken(token)
	if token == "" {
		return "", fmt.Errorf("telegram token is empty")
	}
	ctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("https://api.telegram.org/bot%s/getMe", token), nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode/100 != 2 {
		detail := strings.TrimSpace(string(bodyBytes))
		if resp.StatusCode == http.StatusNotFound {
			return "", fmt.Errorf("telegram getMe 404 Not Found: Bot Token 无效或 token_env 指向了错误环境变量；Telegram 返回：%s", detail)
		}
		return "", fmt.Errorf("telegram getMe status %s: %s", resp.Status, detail)
	}
	var out struct {
		OK     bool `json:"ok"`
		Result struct {
			ID       int64  `json:"id"`
			Username string `json:"username"`
		} `json:"result"`
		Description string `json:"description"`
	}
	if err := json.Unmarshal(bodyBytes, &out); err != nil {
		return "", err
	}
	if !out.OK {
		return "", fmt.Errorf("telegram getMe failed: %s", out.Description)
	}
	return out.Result.Username, nil
}

func eventKeyboard(ev *AdmissionEvent) any {
	web := strings.TrimRight(os.Getenv("WEB_BASE_URL"), "/")
	if web == "" {
		return nil
	}
	buttons := [][]map[string]string{{{"text": "打开 Web 详情", "url": web + "/?event=" + ev.ID}}}
	if ev.RollbackID != "" {
		buttons = append(buttons, []map[string]string{{"text": "查看回滚", "url": web + "/?rollback=" + ev.RollbackID}})
	}
	return map[string]any{"inline_keyboard": buttons}
}

func (a *App) handleTelegramWebhook(w http.ResponseWriter, r *http.Request) {
	// 当前版本将 Telegram 作为通知与跳转入口，审批/回滚统一落到 Web Console。
	// 保留此入口，后续可把 callback_data 接入 approval 状态机。
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}
