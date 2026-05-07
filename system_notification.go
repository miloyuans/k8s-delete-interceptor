package main

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type SystemNotification struct {
	ID        string    `json:"id"`
	Kind      string    `json:"kind"`
	Title     string    `json:"title"`
	Body      string    `json:"body"`
	CreatedAt time.Time `json:"created_at"`
}

func newSystemNotification(kind, title, body string) *SystemNotification {
	now := time.Now().UTC()
	h := sha1.Sum([]byte(strings.Join([]string{kind, title, body, now.Format(time.RFC3339Nano), envOr("HOSTNAME", "pod")}, "|")))
	return &SystemNotification{ID: "sys_" + hex.EncodeToString(h[:10]), Kind: strings.TrimSpace(kind), Title: strings.TrimSpace(title), Body: strings.TrimSpace(body), CreatedAt: now}
}

func (a *App) emitSystemNotification(ctx context.Context, kind, title, body string, immediate bool) {
	if a == nil || a.local == nil {
		return
	}
	n := newSystemNotification(kind, title, body)
	if immediate {
		if err := a.sendSystemNotificationDirect(ctx, n); err == nil {
			log.Printf("system notification sent directly: kind=%s id=%s", kind, n.ID)
			return
		} else {
			log.Printf("system notification direct send failed, queued locally: kind=%s id=%s err=%v", kind, n.ID, err)
		}
	}
	if err := a.local.SaveSystemNotification(n); err != nil {
		log.Printf("system notification local queue failed: kind=%s err=%v", kind, err)
		return
	}
	log.Printf("system notification queued locally: kind=%s id=%s", kind, n.ID)
	_ = a.flushSystemNotifications(ctx)
}

func (a *App) emitStartupNotification(ctx context.Context) {
	cfg := a.Config()
	cluster := "unknown"
	if cfg != nil && cfg.ClusterName != "" {
		cluster = cfg.ClusterName
	}
	body := fmt.Sprintf("✅ *K8s 删除拦截服务启动成功*\n集群: `%s`\n实例: `%s`\n时间: `%s`\n状态: `webhook/web console 已启动`", cluster, envOr("HOSTNAME", "pod"), time.Now().UTC().Format(time.RFC3339))
	a.emitSystemNotification(ctx, "service_start", "服务启动成功", body, true)
}

func (a *App) emitShutdownNotification(ctx context.Context) {
	cfg := a.Config()
	cluster := "unknown"
	if cfg != nil && cfg.ClusterName != "" {
		cluster = cfg.ClusterName
	}
	body := fmt.Sprintf("🛑 *K8s 删除拦截服务正在关闭*\n集群: `%s`\n实例: `%s`\n时间: `%s`\n状态: `收到终止信号，正在优雅关闭`", cluster, envOr("HOSTNAME", "pod"), time.Now().UTC().Format(time.RFC3339))
	a.emitSystemNotification(ctx, "service_shutdown", "服务关闭", body, true)
}

func (a *App) emitMongoStatusNotification(ctx context.Context, recovered bool, detail string) {
	cfg := a.Config()
	cluster := "unknown"
	if cfg != nil && cfg.ClusterName != "" {
		cluster = cfg.ClusterName
	}
	kind := "mongo_down"
	title := "Mongo 数据源异常"
	icon := "⚠️"
	state := "Mongo 数据源不可用，事件将暂存在本地队列"
	if recovered {
		kind = "mongo_recovered"
		title = "Mongo 数据源恢复"
		icon = "✅"
		state = "Mongo 数据源已恢复，本地队列将继续同步并补发通知"
	}
	body := fmt.Sprintf("%s *%s*\n集群: `%s`\n实例: `%s`\n时间: `%s`\n状态: `%s`", icon, title, cluster, envOr("HOSTNAME", "pod"), time.Now().UTC().Format(time.RFC3339), state)
	if strings.TrimSpace(detail) != "" {
		detail = strings.ReplaceAll(truncateForTelegram(detail, 300), "`", "'")
		body += "\n详情: `" + detail + "`"
	}
	a.emitSystemNotification(ctx, kind, title, body, false)
}

func truncateForTelegram(s string, n int) string {
	s = strings.TrimSpace(s)
	if n <= 0 || len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func (a *App) sendSystemNotificationDirect(ctx context.Context, n *SystemNotification) error {
	if n == nil {
		return nil
	}
	tg, err := a.getTelegramConfig(ctx)
	if err != nil {
		return err
	}
	if !tg.Enabled {
		return fmt.Errorf("telegram disabled")
	}
	targets := resolveTelegramTargets(tg, NotificationBinding{Enabled: true})
	if len(targets) == 0 {
		return fmt.Errorf("no telegram targets")
	}
	var lastErr error
	sent := 0
	for _, target := range targets {
		bot := findTelegramBot(tg, target.BotID)
		if bot == nil || !bot.Enabled {
			continue
		}
		token, _ := telegramTokenForBotKey(*bot, n.ID+"|"+target.ChatID+"|system")
		if token == "" {
			continue
		}
		if err := sendTelegram(ctx, token, target.ChatID, n.Body, "Markdown", nil); err != nil {
			lastErr = err
			continue
		}
		sent++
	}
	if sent == 0 {
		if lastErr != nil {
			return lastErr
		}
		return fmt.Errorf("no telegram target sent")
	}
	return nil
}

func (a *App) flushSystemNotifications(ctx context.Context) error {
	if a == nil || a.local == nil || a.mongo == nil || !a.mongo.Healthy() {
		return nil
	}
	tg, err := a.getTelegramConfig(ctx)
	if err != nil || !tg.Enabled {
		return err
	}
	targets := resolveTelegramTargets(tg, NotificationBinding{Enabled: true})
	if len(targets) == 0 {
		return nil
	}
	items, err := a.local.ListPendingSystemNotifications(100)
	if err != nil {
		return err
	}
	for _, item := range items {
		enqueued := 0
		for _, target := range targets {
			n := &TelegramNotificationEvent{Kind: NotifyKindSystemEvent, EventID: item.ID, BotID: target.BotID, ChatID: target.ChatID, TargetName: target.Name, Text: item.Body, ParseMode: "Markdown", Status: NotifyStatusPending, MaxAttempts: telegramMaxAttempts(), CreatedAt: item.CreatedAt, NextAttemptAt: time.Now().UTC()}
			n.ID = notificationID(n.Kind, item.ID, target.BotID, target.ChatID)
			if err := a.mongo.EnqueueTelegramNotification(ctx, n); err != nil {
				log.Printf("system notification enqueue failed: sys=%s target=%s err=%v", item.ID, target.ChatID, err)
				continue
			}
			enqueued++
		}
		if enqueued > 0 {
			_ = a.local.MarkSystemNotificationQueued(item.ID)
			log.Printf("system notification enqueued: sys=%s targets=%d", item.ID, enqueued)
		}
	}
	return nil
}

func (s *LocalStore) SaveSystemNotification(n *SystemNotification) error {
	if s == nil || n == nil || strings.TrimSpace(n.ID) == "" {
		return nil
	}
	if n.CreatedAt.IsZero() {
		n.CreatedAt = time.Now().UTC()
	}
	b, err := json.MarshalIndent(n, "", "  ")
	if err != nil {
		return err
	}
	name := safeFileName(n.ID) + ".json"
	tmp := filepath.Join(s.root, "tmp", name+fmt.Sprintf(".%d.system.tmp", time.Now().UnixNano()))
	pending := filepath.Join(s.root, "spool/system-notifications/pending", name)
	if err := os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	if err := fsyncFile(tmp); err != nil {
		return err
	}
	return os.Rename(tmp, pending)
}

func (s *LocalStore) ListPendingSystemNotifications(limit int) ([]SystemNotification, error) {
	if s == nil {
		return nil, nil
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	dir := filepath.Join(s.root, "spool/system-notifications/pending")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := []SystemNotification{}
	for _, e := range entries {
		if len(out) >= limit {
			break
		}
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		var item SystemNotification
		if json.Unmarshal(b, &item) == nil && item.ID != "" {
			out = append(out, item)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

func (s *LocalStore) MarkSystemNotificationQueued(id string) error {
	if s == nil || strings.TrimSpace(id) == "" {
		return nil
	}
	name := safeFileName(id) + ".json"
	src := filepath.Join(s.root, "spool/system-notifications/pending", name)
	dst := filepath.Join(s.root, "spool/system-notifications/sent", name)
	if err := os.Rename(src, dst); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
