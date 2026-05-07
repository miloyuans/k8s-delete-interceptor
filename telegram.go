package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"
)

type telegramSendMessageRequest struct {
	ChatID      string `json:"chat_id"`
	Text        string `json:"text"`
	ParseMode   string `json:"parse_mode,omitempty"`
	ReplyMarkup any    `json:"reply_markup,omitempty"`
}

type telegramEditMessageRequest struct {
	ChatID      string `json:"chat_id"`
	MessageID   int64  `json:"message_id"`
	Text        string `json:"text"`
	ParseMode   string `json:"parse_mode,omitempty"`
	ReplyMarkup any    `json:"reply_markup,omitempty"`
}

type telegramSendResult struct {
	MessageID int64
}

type telegramTokenCandidate struct {
	Token  string
	Source string
}

func (a *App) getTelegramConfig(ctx context.Context) (*TelegramConfig, error) {
	if a.mongo == nil || !a.mongo.Healthy() {
		return nil, fmt.Errorf("telegram config store unavailable: mongo is not healthy")
	}
	cfg, err := a.mongo.GetTelegramConfig(ctx)
	if err != nil {
		return nil, err
	}
	return normalizeTelegramConfig(*cfg), nil
}

func (a *App) saveTelegramConfig(ctx context.Context, cfg TelegramConfig, actor string) (*TelegramConfig, error) {
	if a.mongo == nil || !a.mongo.Healthy() {
		return nil, fmt.Errorf("telegram config store unavailable: mongo is not healthy")
	}
	cfg = *normalizeTelegramConfig(cfg)
	return a.mongo.SaveTelegramConfig(ctx, cfg, actor)
}

func normalizeTelegramConfig(cfg TelegramConfig) *TelegramConfig {
	cfg.ID = "default"
	seenBots := map[string]bool{}
	bots := []TelegramBot{}
	for _, b := range cfg.Bots {
		b.ID = strings.TrimSpace(b.ID)
		b.Name = strings.TrimSpace(b.Name)
		b.Token = strings.TrimSpace(b.Token)
		b.TokenEnv = strings.TrimSpace(b.TokenEnv)
		b.Tokens = dedupeSort(compactStrings(b.Tokens))
		b.TokenEnvs = dedupeSort(compactStrings(b.TokenEnvs))
		if b.ID == "" {
			b.ID = autoID("bot", b.Name)
		}
		if b.ID == "" || seenBots[b.ID] {
			continue
		}
		if b.Name == "" {
			b.Name = b.ID
		}
		seenBots[b.ID] = true
		bots = append(bots, b)
	}
	cfg.Bots = bots
	seenChats := map[string]bool{}
	chats := []TelegramChat{}
	for _, c := range cfg.Chats {
		c.ID = strings.TrimSpace(c.ID)
		c.Name = strings.TrimSpace(c.Name)
		c.BotID = strings.TrimSpace(c.BotID)
		c.ChatID = strings.TrimSpace(c.ChatID)
		if c.ID == "" {
			c.ID = autoID("chat", c.Name+c.ChatID)
		}
		if c.ID == "" || seenChats[c.ID] {
			continue
		}
		if c.Name == "" {
			c.Name = c.ID
		}
		seenChats[c.ID] = true
		chats = append(chats, c)
	}
	cfg.Chats = chats
	seenUsers := map[string]bool{}
	users := []TelegramUser{}
	for _, u := range cfg.Users {
		u.ID = strings.TrimSpace(u.ID)
		u.TelegramID = strings.TrimSpace(u.TelegramID)
		u.Username = strings.TrimSpace(strings.TrimPrefix(u.Username, "@"))
		u.DisplayName = strings.TrimSpace(u.DisplayName)
		u.Alias = strings.TrimSpace(u.Alias)
		u.Roles = dedupeSort(compactStrings(u.Roles))
		if u.ID == "" {
			u.ID = autoID("tg_user", u.DisplayName+u.TelegramID+u.Username)
		}
		if u.ID == "" || seenUsers[u.ID] {
			continue
		}
		if u.DisplayName == "" {
			u.DisplayName = u.ID
		}
		seenUsers[u.ID] = true
		users = append(users, u)
	}
	cfg.Users = users
	return &cfg
}

func compactStrings(xs []string) []string {
	out := []string{}
	for _, x := range xs {
		x = strings.TrimSpace(x)
		if x != "" {
			out = append(out, x)
		}
	}
	return out
}

func (a *App) notifyEvent(ctx context.Context, cfg *RuntimeConfig, ev *AdmissionEvent, pd PolicyDecision) {
	if cfg == nil || ev == nil || pd.Rule == nil {
		return
	}
	bind := pd.Rule.Notify
	if !bind.Enabled {
		log.Printf("telegram notify skipped: rule notify disabled rule=%s event=%s decision=%s", pd.Rule.ID, ev.ID, pd.Decision)
		return
	}
	tg, err := a.getTelegramConfig(ctx)
	if err != nil {
		log.Printf("telegram notify skipped: cannot read db telegram config rule=%s event=%s err=%v", pd.Rule.ID, ev.ID, err)
		return
	}
	if !tg.Enabled {
		log.Printf("telegram notify skipped: db telegram global disabled rule=%s event=%s", pd.Rule.ID, ev.ID)
		return
	}
	tpl := findTemplate(cfg, bind.TemplateID)
	if tpl == nil || !tpl.Enabled {
		log.Printf("telegram notify skipped: template disabled or missing rule=%s template=%s", pd.Rule.ID, bind.TemplateID)
		return
	}
	targets := resolveTelegramTargets(tg, bind)
	log.Printf("telegram notify resolving: rule=%s event=%s template=%s bots=%v chats=%v users=%v targets=%d", pd.Rule.ID, ev.ID, bind.TemplateID, bind.TelegramBotIDs, bind.TelegramChatIDs, bind.TelegramUserIDs, len(targets))
	if len(targets) == 0 {
		log.Printf("telegram notify skipped: no enabled target rule=%s template=%s", pd.Rule.ID, tpl.ID)
		return
	}
	msg, err := renderTemplate(cfg, tg, ev, pd, *tpl)
	if err != nil {
		log.Printf("telegram render template failed: rule=%s template=%s err=%v", pd.Rule.ID, tpl.ID, err)
		return
	}
	queued := 0
	for _, target := range targets {
		n := &TelegramNotificationEvent{Kind: NotifyKindAdmissionEvent, EventID: ev.ID, RuleID: pd.Rule.ID, RuleName: pd.Rule.Name, BotID: target.BotID, ChatID: target.ChatID, TargetName: target.Name, Text: msg, ParseMode: tpl.ParseMode, Status: NotifyStatusPending, MaxAttempts: telegramMaxAttempts(), CreatedAt: time.Now().UTC(), NextAttemptAt: time.Now().UTC()}
		n.ID = notificationID(n.Kind, ev.ID, target.BotID, target.ChatID)
		if kb := eventKeyboardFor(ev, n.ID); kb != nil {
			if m, ok := kb.(map[string]any); ok {
				n.ReplyMarkup = m
			}
		}
		if err := a.enqueueTelegramNotification(ctx, n); err != nil {
			log.Printf("telegram notify enqueue failed: rule=%s event=%s bot_id=%s chat_id=%s err=%v", pd.Rule.ID, ev.ID, target.BotID, target.ChatID, err)
			continue
		}
		queued++
	}
	log.Printf("telegram notify queued: rule=%s event=%s targets=%d", pd.Rule.ID, ev.ID, queued)
}

func notificationID(kind, eventID, botID, chatID string) string {
	s := strings.Join([]string{kind, eventID, botID, chatID}, "|")
	h := sha1.Sum([]byte(s))
	return "ntf_" + hex.EncodeToString(h[:10])
}

func (a *App) enqueueTelegramNotification(ctx context.Context, ev *TelegramNotificationEvent) error {
	if ev == nil {
		return nil
	}
	if a.mongo == nil || !a.mongo.Healthy() {
		return fmt.Errorf("telegram notification queue unavailable: mongo is not healthy")
	}
	return a.mongo.EnqueueTelegramNotification(ctx, ev)
}

func (a *App) telegramNotificationLoop(ctx context.Context) {
	owner := fmt.Sprintf("%s-%d", envOr("HOSTNAME", "pod"), time.Now().UnixNano())
	idle := envDuration("TELEGRAM_NOTIFY_IDLE_POLL", 2*time.Second)
	lease := telegramLease()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if a.mongo == nil || !a.mongo.Healthy() {
			waitContext(ctx, idle)
			continue
		}
		pending, err := a.mongo.CountRunnableTelegramNotifications(ctx, time.Now().UTC())
		if err != nil {
			log.Printf("telegram dispatcher probe failed owner=%s err=%v", owner, err)
			waitContext(ctx, idle)
			continue
		}
		if pending == 0 {
			waitContext(ctx, idle)
			continue
		}
		jitter := time.Duration(rng.Int63n(int64(telegramProbeJitter())))
		waitContext(ctx, jitter)
		ok, err := a.mongo.AcquireTelegramDispatchLock(ctx, owner, lease)
		if err != nil {
			log.Printf("telegram dispatcher lock failed owner=%s err=%v", owner, err)
			waitContext(ctx, idle)
			continue
		}
		if !ok {
			waitContext(ctx, idle)
			continue
		}
		log.Printf("telegram dispatcher acquired owner=%s pending=%d", owner, pending)
		a.runTelegramDispatchSession(ctx, owner)
		_ = a.mongo.ReleaseTelegramDispatchLock(context.Background(), owner)
		log.Printf("telegram dispatcher released owner=%s", owner)
	}
}

func (a *App) runTelegramDispatchSession(ctx context.Context, owner string) {
	lease := telegramLease()
	tg, err := a.getTelegramConfig(ctx)
	if err != nil {
		log.Printf("telegram dispatcher cannot load db telegram config owner=%s err=%v", owner, err)
		return
	}
	if !tg.Enabled {
		log.Printf("telegram dispatcher stopped: telegram disabled owner=%s", owner)
		return
	}
	workers := telegramWorkerCount(tg)
	if workers < 1 {
		log.Printf("telegram dispatcher stopped: no enabled bot token owner=%s", owner)
		return
	}
	maxWorkers := envInt("TELEGRAM_NOTIFY_MAX_WORKERS", 8)
	if maxWorkers < 1 {
		maxWorkers = 1
	}
	if maxWorkers > 32 {
		maxWorkers = 32
	}
	if workers > maxWorkers {
		workers = maxWorkers
	}
	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			a.telegramNotificationWorker(sessionCtx, fmt.Sprintf("%s-w%d", owner, idx))
		}(i)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	renew := time.NewTicker(maxDuration(lease/3, 5*time.Second))
	defer renew.Stop()
	for {
		select {
		case <-ctx.Done():
			cancel()
			<-done
			return
		case <-done:
			return
		case <-renew.C:
			ok, err := a.mongo.RenewTelegramDispatchLock(ctx, owner, lease)
			if err != nil || !ok {
				log.Printf("telegram dispatcher lost lock owner=%s ok=%v err=%v", owner, ok, err)
				cancel()
				<-done
				return
			}
		}
	}
}

func telegramWorkerCount(tg *TelegramConfig) int {
	if tg == nil || !tg.Enabled {
		return 0
	}
	n := 0
	for _, b := range tg.Bots {
		if !b.Enabled {
			continue
		}
		c := len(telegramTokenCandidates(b))
		if c == 0 {
			continue
		}
		n += c
	}
	if n < 1 {
		return 0
	}
	return n
}

func (a *App) telegramNotificationWorker(ctx context.Context, worker string) {
	minInterval := telegramMinInterval()
	idle := envDuration("TELEGRAM_NOTIFY_EMPTY_WAIT", 700*time.Millisecond)
	empty := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if a.mongo == nil || !a.mongo.Healthy() {
			return
		}
		n, err := a.mongo.ClaimTelegramNotification(ctx, worker, telegramLease())
		if err != nil {
			log.Printf("telegram notification claim failed worker=%s err=%v", worker, err)
			waitContext(ctx, idle)
			continue
		}
		if n == nil {
			empty++
			if empty >= 3 {
				return
			}
			waitContext(ctx, idle)
			continue
		}
		empty = 0
		if err := a.sendTelegramNotificationNow(ctx, n); err != nil {
			delay := telegramRetryDelay(err, n.Attempts)
			terminal := n.Attempts >= n.MaxAttempts
			if terminal {
				log.Printf("telegram notification failed permanently: id=%s kind=%s event=%s attempts=%d err=%v", n.ID, n.Kind, n.EventID, n.Attempts, err)
			} else {
				log.Printf("telegram notification failed, will retry: id=%s kind=%s event=%s attempts=%d delay=%s err=%v", n.ID, n.Kind, n.EventID, n.Attempts, delay, err)
			}
			_ = a.mongo.FailTelegramNotification(ctx, n, err.Error(), time.Now().UTC().Add(delay), terminal)
		} else {
			log.Printf("telegram notification consumed: id=%s kind=%s event=%s bot_id=%s chat_id=%s worker=%s", n.ID, n.Kind, n.EventID, n.BotID, n.ChatID, worker)
		}
		waitContext(ctx, minInterval)
	}
}

func (a *App) sendTelegramNotificationNow(ctx context.Context, n *TelegramNotificationEvent) error {
	if n == nil {
		return fmt.Errorf("notification event unavailable")
	}
	tg, err := a.getTelegramConfig(ctx)
	if err != nil {
		return fmt.Errorf("telegram db config unavailable: %w", err)
	}
	if !tg.Enabled {
		return fmt.Errorf("telegram global setting is disabled")
	}
	bot := findTelegramBot(tg, n.BotID)
	if bot == nil || !bot.Enabled {
		return fmt.Errorf("telegram bot not found or disabled: %s", n.BotID)
	}
	token, source := telegramTokenForBotKey(*bot, n.ID+"|"+n.ClaimedBy)
	if token == "" {
		return fmt.Errorf("telegram token is empty for bot_id=%s token_env=%s", bot.ID, bot.TokenEnv)
	}
	text := n.Text
	markup := n.ReplyMarkup
	parseMode := n.ParseMode
	if n.Kind == NotifyKindConfigChange && n.ChangeID != "" {
		if cr, err := a.getConfigChange(ctx, n.ChangeID); err == nil && cr != nil {
			text = configChangeTelegramText(cr)
			web := strings.TrimRight(os.Getenv("WEB_BASE_URL"), "/")
			if web != "" {
				buttonText := "打开 Web 审批"
				if cr.Status != ChangePending {
					buttonText = "打开 Web 查看"
				}
				markup = map[string]any{"inline_keyboard": [][]map[string]string{{{"text": buttonText, "url": web + "/?change=" + cr.ID}}}}
			}
		}
	}
	log.Printf("telegram notification sending: id=%s kind=%s event=%s bot_id=%s chat_id=%s source=%s", n.ID, n.Kind, n.EventID, bot.ID, n.ChatID, source)
	res, err := sendTelegramWithResult(ctx, token, n.ChatID, text, parseMode, markup)
	if err != nil {
		return err
	}
	if a.mongo != nil && a.mongo.Healthy() {
		_ = a.mongo.CompleteTelegramNotification(ctx, n.ID, res.MessageID)
	}
	if n.Kind == NotifyKindConfigChange && n.ChangeID != "" && res.MessageID > 0 {
		_ = a.addConfigChangeTelegramRef(ctx, n.ChangeID, TelegramMessageRef{BotID: bot.ID, ChatID: n.ChatID, MessageID: res.MessageID})
	}
	return nil
}

func envDuration(k string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return def
	}
	return d
}

func telegramMaxAttempts() int {
	n := envInt("TELEGRAM_NOTIFY_MAX_ATTEMPTS", 10)
	if n < 1 {
		return 1
	}
	if n > 50 {
		return 50
	}
	return n
}

func telegramMinInterval() time.Duration {
	v := strings.TrimSpace(os.Getenv("TELEGRAM_NOTIFY_MIN_INTERVAL"))
	if v == "" {
		return 1200 * time.Millisecond
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 100*time.Millisecond {
		return 1200 * time.Millisecond
	}
	return d
}

func telegramProbeJitter() time.Duration {
	v := strings.TrimSpace(os.Getenv("TELEGRAM_NOTIFY_PROBE_JITTER"))
	if v == "" {
		return 1500 * time.Millisecond
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 100*time.Millisecond {
		return 1500 * time.Millisecond
	}
	return d
}

func telegramLease() time.Duration {
	v := strings.TrimSpace(os.Getenv("TELEGRAM_NOTIFY_LEASE"))
	if v == "" {
		return 2 * time.Minute
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 30*time.Second {
		return 2 * time.Minute
	}
	return d
}

func telegramRetryDelay(err error, attempts int) time.Duration {
	msg := err.Error()
	if i := strings.Index(msg, "retry_after"); i >= 0 {
		tail := msg[i:]
		for _, sep := range []string{":", " ", ",", "}"} {
			tail = strings.ReplaceAll(tail, sep, " ")
		}
		parts := strings.Fields(tail)
		for _, p := range parts {
			if n, e := strconv.Atoi(p); e == nil && n > 0 {
				return time.Duration(n+1) * time.Second
			}
		}
	}
	if attempts < 1 {
		attempts = 1
	}
	sec := 5 * (1 << minInt(attempts-1, 5))
	return time.Duration(sec) * time.Second
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func waitContext(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	select {
	case <-ctx.Done():
	case <-time.After(d):
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

func findTelegramBot(tg *TelegramConfig, id string) *TelegramBot {
	if tg == nil {
		return nil
	}
	for i := range tg.Bots {
		if tg.Bots[i].ID == id {
			return &tg.Bots[i]
		}
	}
	return nil
}

func findTelegramUser(tg *TelegramConfig, id string) *TelegramUser {
	if tg == nil {
		return nil
	}
	for i := range tg.Users {
		if tg.Users[i].ID == id || tg.Users[i].TelegramID == id {
			return &tg.Users[i]
		}
	}
	return nil
}

func resolveTelegramTargets(tg *TelegramConfig, bind NotificationBinding) []telegramTarget {
	if tg == nil || !tg.Enabled {
		return nil
	}
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
	for _, c := range tg.Chats {
		if !c.Enabled {
			continue
		}
		if !allBots && !botIDs[c.BotID] {
			continue
		}
		if !allChats && !chatIDs[c.ID] {
			continue
		}
		bot := findTelegramBot(tg, c.BotID)
		if bot == nil || !bot.Enabled || len(telegramTokenCandidates(*bot)) == 0 {
			continue
		}
		add(telegramTarget{BotID: c.BotID, ChatID: c.ChatID, Name: c.Name})
	}
	if len(userIDs) > 0 {
		for _, u := range tg.Users {
			if !u.Enabled || u.TelegramID == "" {
				continue
			}
			if !userIDs[u.ID] && !userIDs[u.TelegramID] {
				continue
			}
			for _, b := range tg.Bots {
				if !b.Enabled || len(telegramTokenCandidates(b)) == 0 {
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

func renderTemplate(cfg *RuntimeConfig, tg *TelegramConfig, ev *AdmissionEvent, pd PolicyDecision, tpl NotificationTemplate) (string, error) {
	web := strings.TrimRight(os.Getenv("WEB_BASE_URL"), "/")
	eventURL := ""
	if web != "" {
		eventURL = webURL(web, "/events", map[string]string{"event": ev.ID})
	}
	actorDisplay := ev.User
	approvers := []string{}
	if pd.Rule != nil {
		for _, id := range pd.Rule.Approval.ApproverTelegramUsers {
			if u := findTelegramUser(tg, id); u != nil {
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

func telegramTokenCandidates(bot TelegramBot) []telegramTokenCandidate {
	out := []telegramTokenCandidate{}
	seen := map[string]bool{}
	add := func(token, source string) {
		token = normalizeTelegramToken(token)
		if token == "" || token == "********" {
			return
		}
		key := source + "|" + token
		if seen[key] {
			return
		}
		seen[key] = true
		out = append(out, telegramTokenCandidate{Token: token, Source: source})
	}
	if bot.Token != "" && bot.Token != "********" {
		add(bot.Token, "inline_token")
	}
	for i, t := range bot.Tokens {
		add(t, fmt.Sprintf("inline_token[%d]", i))
	}
	if bot.TokenEnv != "" {
		add(os.Getenv(bot.TokenEnv), "env:"+bot.TokenEnv)
	}
	for _, env := range bot.TokenEnvs {
		env = strings.TrimSpace(env)
		if env == "" {
			continue
		}
		add(os.Getenv(env), "env:"+env)
	}
	return out
}

func telegramTokenForBot(bot TelegramBot) (string, string) {
	return telegramTokenForBotKey(bot, "")
}

func telegramTokenForBotKey(bot TelegramBot, key string) (string, string) {
	cands := telegramTokenCandidates(bot)
	if len(cands) == 0 {
		return "", "none"
	}
	idx := stableIndex(key, len(cands))
	return cands[idx].Token, cands[idx].Source
}

func stableIndex(key string, n int) int {
	if n <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(n))
}

func sendTelegram(ctx context.Context, token, chatID, text, parseMode string, markup any) error {
	_, err := sendTelegramWithResult(ctx, token, chatID, text, parseMode, markup)
	return err
}

func sendTelegramWithResult(ctx context.Context, token, chatID, text, parseMode string, markup any) (telegramSendResult, error) {
	token = normalizeTelegramToken(token)
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	body := telegramSendMessageRequest{ChatID: chatID, Text: text, ParseMode: parseMode, ReplyMarkup: markup}
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", token), bytes.NewReader(b))
	if err != nil {
		return telegramSendResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return telegramSendResult{}, err
	}
	defer resp.Body.Close()
	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
	if resp.StatusCode/100 != 2 {
		detail := strings.TrimSpace(string(bodyBytes))
		if resp.StatusCode == http.StatusNotFound {
			return telegramSendResult{}, fmt.Errorf("telegram API 404 Not Found: Bot Token 无效、粘贴了错误 token，或 token_env 读取到的环境变量不正确；Telegram 返回：%s", detail)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			return telegramSendResult{}, fmt.Errorf("telegram API 429 Too Many Requests: 发送过于频繁或重复目标过多，请等待 Telegram 限流恢复；Telegram 返回：%s", detail)
		}
		return telegramSendResult{}, fmt.Errorf("telegram status %s: %s", resp.Status, detail)
	}
	var out struct {
		OK     bool `json:"ok"`
		Result struct {
			MessageID int64 `json:"message_id"`
		} `json:"result"`
		Description string `json:"description"`
	}
	if err := json.Unmarshal(bodyBytes, &out); err != nil {
		return telegramSendResult{}, err
	}
	if !out.OK {
		return telegramSendResult{}, fmt.Errorf("telegram send failed: %s", out.Description)
	}
	return telegramSendResult{MessageID: out.Result.MessageID}, nil
}

func editTelegramMessage(ctx context.Context, token, chatID string, messageID int64, text, parseMode string, markup any) error {
	token = normalizeTelegramToken(token)
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	body := telegramEditMessageRequest{ChatID: chatID, MessageID: messageID, Text: text, ParseMode: parseMode, ReplyMarkup: markup}
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("https://api.telegram.org/bot%s/editMessageText", token), bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
	if resp.StatusCode/100 != 2 {
		detail := strings.TrimSpace(string(bodyBytes))
		return fmt.Errorf("telegram editMessageText status %s: %s", resp.Status, detail)
	}
	return nil
}

func normalizeTelegramToken(token string) string {
	token = strings.TrimSpace(token)
	token = strings.Trim(token, "\"'")
	if strings.Contains(token, "=") && strings.HasPrefix(strings.ToUpper(strings.TrimSpace(token)), "TELEGRAM") {
		parts := strings.SplitN(token, "=", 2)
		token = strings.TrimSpace(parts[1])
		token = strings.Trim(token, "\"'")
	}
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

func webURL(base, path string, params map[string]string) string {
	base = strings.TrimRight(base, "/")
	if base == "" {
		return ""
	}
	if path == "" {
		path = "/"
	}
	v := url.Values{}
	for k, val := range params {
		if strings.TrimSpace(val) != "" {
			v.Set(k, val)
		}
	}
	if enc := v.Encode(); enc != "" {
		return base + path + "?" + enc
	}
	return base + path
}

func eventKeyboardFor(ev *AdmissionEvent, notificationID string) any {
	web := strings.TrimRight(os.Getenv("WEB_BASE_URL"), "/")
	if web == "" {
		return nil
	}
	buttons := [][]map[string]string{{{"text": "打开 Web 详情", "url": webURL(web, "/events", map[string]string{"event": ev.ID, "ntf": notificationID})}}}
	if ev.RollbackID != "" {
		buttons = append(buttons, []map[string]string{{"text": "查看回滚", "url": webURL(web, "/events", map[string]string{"event": ev.ID, "rollback": ev.RollbackID, "ntf": notificationID})}})
	}
	return map[string]any{"inline_keyboard": buttons}
}

func (a *App) handleTelegramWebhook(w http.ResponseWriter, r *http.Request) {
	// 当前版本将 Telegram 作为通知与跳转入口，审批/回滚统一落到 Web Console。
	// 保留此入口，后续可把 callback_data 接入 approval 状态机。
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}
