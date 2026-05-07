package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type telegramGetUpdatesResponse struct {
	OK          bool             `json:"ok"`
	Description string           `json:"description"`
	Result      []telegramUpdate `json:"result"`
}

func (a *App) telegramCallbackPollingLoop(ctx context.Context) {
	waitContext(ctx, 3*time.Second)
	owner := fmt.Sprintf("%s-cb-%d", envOr("HOSTNAME", "pod"), time.Now().UnixNano())
	idle := envDuration("TELEGRAM_CALLBACK_IDLE_POLL", 10*time.Second)
	lease := envDuration("TELEGRAM_CALLBACK_LOCK_LEASE", 90*time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		cfg := a.Config()
		if cfg == nil || !cfg.Persistence.TelegramCallbackPolling || a.mongo == nil || !a.mongo.Healthy() {
			waitContext(ctx, idle)
			continue
		}
		active, err := a.mongo.CountActiveTelegramInteractions(ctx, time.Now().UTC())
		if err != nil {
			log.Printf("telegram callback poller active probe failed: %v", err)
			waitContext(ctx, idle)
			continue
		}
		if active == 0 {
			waitContext(ctx, idle)
			continue
		}
		ok, err := a.mongo.AcquireTelegramCallbackLock(ctx, owner, lease)
		if err != nil || !ok {
			if err != nil {
				log.Printf("telegram callback poller lock failed: %v", err)
			}
			waitContext(ctx, idle)
			continue
		}
		log.Printf("telegram callback poller acquired owner=%s active_interactions=%d", owner, active)
		a.runTelegramCallbackPollingSession(ctx, owner, lease)
		_ = a.mongo.ReleaseTelegramCallbackLock(context.Background(), owner)
		log.Printf("telegram callback poller released owner=%s", owner)
	}
}

func (a *App) runTelegramCallbackPollingSession(ctx context.Context, owner string, lease time.Duration) {
	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	renew := time.NewTicker(maxDuration(lease/3, 10*time.Second))
	defer renew.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-renew.C:
			ok, err := a.mongo.RenewTelegramCallbackLock(ctx, owner, lease)
			if err != nil || !ok {
				log.Printf("telegram callback poller lost lock owner=%s ok=%v err=%v", owner, ok, err)
				return
			}
		default:
		}
		active, err := a.mongo.CountActiveTelegramInteractions(ctx, time.Now().UTC())
		if err != nil || active == 0 {
			return
		}
		tg, err := a.getTelegramConfig(ctx)
		if err != nil || !tg.Enabled {
			return
		}
		a.pollTelegramCallbacksOnce(sessionCtx, tg)
	}
}

func (a *App) pollTelegramCallbacksOnce(ctx context.Context, tg *TelegramConfig) {
	if tg == nil || !tg.Enabled {
		return
	}
	var wg sync.WaitGroup
	limit := envInt("TELEGRAM_CALLBACK_MAX_BOTS", 8)
	if limit < 1 {
		limit = 1
	}
	sem := make(chan struct{}, limit)
	for _, bot := range tg.Bots {
		if !bot.Enabled {
			continue
		}
		for _, cand := range telegramTokenCandidates(bot) {
			token := cand.Token
			if token == "" {
				continue
			}
			sem <- struct{}{}
			wg.Add(1)
			go func(botID, token string) {
				defer wg.Done()
				defer func() { <-sem }()
				a.pollTelegramBotCallbacks(ctx, botID, token)
			}(bot.ID, token)
		}
	}
	wg.Wait()
}

func (a *App) pollTelegramBotCallbacks(ctx context.Context, botID, token string) {
	key := botID + "|" + tokenFingerprint(token)
	offset := a.telegramPollingOffset(key)
	updates, next, err := getTelegramUpdates(ctx, token, offset)
	if err != nil {
		msg := err.Error()
		if strings.Contains(strings.ToLower(msg), "conflict") || strings.Contains(strings.ToLower(msg), "webhook") {
			log.Printf("telegram callback polling disabled by Telegram for bot=%s: webhook is active; webhook route /telegram/webhook will handle callbacks", botID)
		} else {
			log.Printf("telegram callback polling failed bot=%s err=%v", botID, err)
		}
		return
	}
	if next > offset {
		a.setTelegramPollingOffset(key, next)
	}
	for _, up := range updates {
		if up.CallbackQuery != nil {
			a.handleTelegramCallback(context.Background(), up.CallbackQuery)
		}
	}
}

func (a *App) telegramPollingOffset(key string) int64 {
	a.telegramOffsetMu.Lock()
	defer a.telegramOffsetMu.Unlock()
	if a.telegramOffsets == nil {
		a.telegramOffsets = map[string]int64{}
	}
	return a.telegramOffsets[key]
}

func (a *App) setTelegramPollingOffset(key string, offset int64) {
	a.telegramOffsetMu.Lock()
	defer a.telegramOffsetMu.Unlock()
	if a.telegramOffsets == nil {
		a.telegramOffsets = map[string]int64{}
	}
	if offset > a.telegramOffsets[key] {
		a.telegramOffsets[key] = offset
	}
}

func getTelegramUpdates(ctx context.Context, token string, offset int64) ([]telegramUpdate, int64, error) {
	if token == "" {
		return nil, offset, fmt.Errorf("telegram token is empty")
	}
	timeoutSeconds := envInt("TELEGRAM_CALLBACK_LONG_POLL_SECONDS", 8)
	if timeoutSeconds < 1 {
		timeoutSeconds = 1
	}
	if timeoutSeconds > 30 {
		timeoutSeconds = 30
	}
	v := url.Values{}
	if offset > 0 {
		v.Set("offset", strconv.FormatInt(offset, 10))
	}
	v.Set("timeout", strconv.Itoa(timeoutSeconds))
	v.Set("allowed_updates", `["callback_query"]`)
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds+5)*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("https://api.telegram.org/bot%s/getUpdates?%s", token, v.Encode()), nil)
	if err != nil {
		return nil, offset, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, offset, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if resp.StatusCode/100 != 2 {
		return nil, offset, fmt.Errorf("telegram getUpdates status %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	var out telegramGetUpdatesResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, offset, err
	}
	if !out.OK {
		return nil, offset, fmt.Errorf("telegram getUpdates failed: %s", out.Description)
	}
	next := offset
	for _, up := range out.Result {
		if up.UpdateID >= next {
			next = up.UpdateID + 1
		}
	}
	return out.Result, next, nil
}

func (a *App) expireTelegramInteractionWindows(ctx context.Context, cfg *RuntimeConfig) error {
	if a == nil || a.mongo == nil || !a.mongo.Healthy() || cfg == nil || !cfg.Persistence.Enabled {
		return nil
	}
	ns, err := a.mongo.ListExpiredTelegramInteractions(ctx, time.Now().UTC(), 100)
	if err != nil {
		return err
	}
	for _, n := range ns {
		a.closeExpiredTelegramNotification(ctx, &n)
	}
	return nil
}

func (a *App) closeExpiredTelegramNotification(ctx context.Context, n *TelegramNotificationEvent) {
	if n == nil || a.mongo == nil || !a.mongo.Healthy() {
		return
	}
	status := "⌛ 交互窗口已过期，后续请登录 Web 事件页处理或下载 YAML。"
	var markup any
	text := n.Text + "\n\n" + status
	parseMode := n.ParseMode
	if n.Kind == NotifyKindAdmissionEvent && n.EventID != "" {
		if ev, err := a.getAdmissionEvent(ctx, n.EventID); err == nil && ev != nil {
			text = eventStatusText(n.Text, ev, status)
			markup = eventKeyboardForStatus(ev, n.ID, status)
		}
	} else if n.Kind == NotifyKindConfigChange && n.ChangeID != "" {
		if cr, err := a.getConfigChange(ctx, n.ChangeID); err == nil && cr != nil {
			text = configChangeTelegramText(cr) + "\n\n" + status
			markup = configChangeWebKeyboard(cr, n.ID)
		}
	}
	if n.MessageID > 0 && n.BotID != "" && n.ChatID != "" {
		if tg, err := a.getTelegramConfig(ctx); err == nil && tg.Enabled {
			if bot := findTelegramBot(tg, n.BotID); bot != nil && bot.Enabled {
				if token, _ := telegramTokenForBotKey(*bot, n.ID+"|expire"); token != "" {
					if err := editTelegramMessage(ctx, token, n.ChatID, n.MessageID, text, parseMode, markup); err != nil {
						log.Printf("telegram interaction expiration edit failed: notification=%s err=%v", n.ID, err)
					}
				}
			}
		}
	}
	_ = a.mongo.CloseTelegramNotificationInteraction(ctx, n.ID, markup, status)
}

func configChangeWebKeyboard(cr *ConfigChangeRequest, notificationID string) any {
	if cr == nil {
		return nil
	}
	web := strings.TrimRight(os.Getenv("WEB_BASE_URL"), "/")
	if web == "" {
		return nil
	}
	return map[string]any{"inline_keyboard": [][]map[string]string{{{"text": "打开 Web 查看", "url": webURL(web, "/changes", map[string]string{"change": cr.ID, "ntf": notificationID})}}}}
}
