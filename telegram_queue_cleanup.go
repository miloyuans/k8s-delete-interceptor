package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type TelegramQueueCleanupStats struct {
	StaleAdmission int64 `json:"stale_admission"`
	ExpiredQueue   int64 `json:"expired_queue"`
}

func telegramQueueCleanupTTL(cfg *RuntimeConfig) time.Duration {
	if cfg == nil {
		return 24 * time.Hour
	}
	def := activeDataTTL(cfg)
	if def <= 0 {
		def = 24 * time.Hour
	}
	return parseConfigDuration(cfg.Persistence.TelegramQueueCleanupTTL, def)
}

func telegramQueueCleanupInterval(cfg *RuntimeConfig) time.Duration {
	d := retentionInterval(cfg)
	if d > time.Minute {
		d = time.Minute
	}
	if d < 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

func telegramQueueCleanupStatsNonZero(s TelegramQueueCleanupStats) bool {
	return s.StaleAdmission > 0 || s.ExpiredQueue > 0
}

func (a *App) telegramQueueCleanupLoop(ctx context.Context) {
	waitContext(ctx, 5*time.Second)
	for {
		cfg := a.Config()
		if cfg != nil && cfg.Persistence.Enabled && a.mongo != nil && a.mongo.Healthy() {
			stats, err := a.cleanupUselessTelegramQueue(ctx, cfg)
			if err != nil {
				log.Printf("telegram queue cleanup failed: %v", err)
			} else if telegramQueueCleanupStatsNonZero(stats) {
				log.Printf("telegram queue cleanup completed: stale_admission=%d expired_queue=%d", stats.StaleAdmission, stats.ExpiredQueue)
			}
		}
		waitContext(ctx, telegramQueueCleanupInterval(cfg))
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func (a *App) cleanupUselessTelegramQueue(ctx context.Context, cfg *RuntimeConfig) (TelegramQueueCleanupStats, error) {
	stats := TelegramQueueCleanupStats{}
	if a == nil || a.mongo == nil || !a.mongo.Healthy() || cfg == nil || !cfg.Persistence.Enabled {
		return stats, nil
	}
	batch := retentionBatchSize(cfg)
	if batch <= 0 {
		batch = 500
	}
	stale, err := a.mongo.ListRunnableAdmissionNotifications(ctx, batch)
	if err != nil {
		return stats, err
	}
	deleteIDs := make([]string, 0, len(stale))
	for i := range stale {
		if reason := a.uselessAdmissionNotificationReason(ctx, cfg, &stale[i]); reason != "" {
			log.Printf("telegram queue cleanup drop stale admission notification: id=%s event=%s rule=%s reason=%s", stale[i].ID, stale[i].EventID, stale[i].RuleID, reason)
			deleteIDs = append(deleteIDs, stale[i].ID)
		}
	}
	if len(deleteIDs) > 0 {
		n, err := a.mongo.DeleteTelegramNotifications(ctx, deleteIDs, "stale admission notification no longer matches current policy")
		if err != nil {
			return stats, err
		}
		stats.StaleAdmission += n
	}
	if ttl := telegramQueueCleanupTTL(cfg); ttl > 0 {
		n, err := a.mongo.DeleteExpiredUselessTelegramQueue(ctx, time.Now().UTC().Add(-ttl))
		if err != nil {
			return stats, err
		}
		stats.ExpiredQueue += n
	}
	return stats, nil
}

func (a *App) uselessAdmissionNotificationReason(ctx context.Context, cfg *RuntimeConfig, n *TelegramNotificationEvent) string {
	if cfg == nil || n == nil {
		return "missing config or notification"
	}
	if strings.TrimSpace(n.EventID) == "" {
		return "missing event_id"
	}
	ev, err := a.getAdmissionEvent(ctx, n.EventID)
	if err != nil || ev == nil {
		// Event write and notification enqueue are normally in order. Keep very new
		// rows for a short grace period to avoid deleting a notification during a
		// transient store read failure.
		if n.CreatedAt.IsZero() || time.Since(n.CreatedAt) > 5*time.Minute {
			return fmt.Sprintf("admission event unavailable: %v", err)
		}
		return ""
	}
	pd, ok, reason := admissionNotificationPolicyStillApplies(cfg, n, ev)
	if !ok {
		return reason
	}
	if pd.Rule == nil || !pd.Rule.Notify.Enabled {
		return "matched rule does not notify"
	}
	tg, err := a.getTelegramConfig(ctx)
	if err != nil || tg == nil || !tg.Enabled {
		return ""
	}
	if !telegramNotificationTargetStillExists(tg, pd.Rule.Notify, n.BotID, n.ChatID) {
		return "telegram target no longer configured in matched rule"
	}
	return ""
}

func telegramNotificationTargetStillExists(tg *TelegramConfig, bind NotificationBinding, botID, chatID string) bool {
	botID = strings.TrimSpace(botID)
	chatID = strings.TrimSpace(chatID)
	if tg == nil || botID == "" || chatID == "" {
		return false
	}
	for _, target := range resolveTelegramTargets(tg, bind) {
		if strings.TrimSpace(target.BotID) == botID && strings.TrimSpace(target.ChatID) == chatID {
			return true
		}
	}
	return false
}

func (m *MongoStore) ListRunnableAdmissionNotifications(ctx context.Context, limit int) ([]TelegramNotificationEvent, error) {
	if m == nil {
		return nil, errors.New("mongo unavailable")
	}
	if limit <= 0 || limit > 5000 {
		limit = 500
	}
	filter := bson.M{
		"kind":   NotifyKindAdmissionEvent,
		"status": bson.M{"$in": []string{NotifyStatusPending, NotifyStatusSending}},
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	cur, err := m.db.Collection("telegram_notification_events").Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "created_at", Value: 1}}).SetLimit(int64(limit)))
	if err != nil {
		m.healthy.Store(false)
		return nil, err
	}
	defer cur.Close(ctx)
	out := []TelegramNotificationEvent{}
	for cur.Next(ctx) {
		var n TelegramNotificationEvent
		if cur.Decode(&n) == nil {
			out = append(out, n)
		}
	}
	if err := cur.Err(); err != nil {
		m.healthy.Store(false)
		return out, err
	}
	m.healthy.Store(true)
	return out, nil
}

func (m *MongoStore) DeleteTelegramNotifications(ctx context.Context, ids []string, reason string) (int64, error) {
	if m == nil {
		return 0, errors.New("mongo unavailable")
	}
	ids = dedupeSort(compactStrings(ids))
	if len(ids) == 0 {
		return 0, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	res, err := m.db.Collection("telegram_notification_events").DeleteMany(ctx, bson.M{"id": bson.M{"$in": ids}})
	m.healthy.Store(err == nil)
	if err != nil {
		return 0, err
	}
	if strings.TrimSpace(reason) != "" && res.DeletedCount > 0 {
		log.Printf("telegram notification queue deleted %d records: %s", res.DeletedCount, reason)
	}
	return res.DeletedCount, nil
}

func (m *MongoStore) DeleteExpiredUselessTelegramQueue(ctx context.Context, before time.Time) (int64, error) {
	if m == nil {
		return 0, errors.New("mongo unavailable")
	}
	if before.IsZero() {
		return 0, nil
	}
	filter := bson.M{
		"status":     bson.M{"$in": []string{NotifyStatusPending, NotifyStatusSending, NotifyStatusFailed}},
		"created_at": bson.M{"$lt": before},
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	res, err := m.db.Collection("telegram_notification_events").DeleteMany(ctx, filter)
	m.healthy.Store(err == nil || errors.Is(err, mongo.ErrNoDocuments))
	if err != nil {
		return 0, err
	}
	return res.DeletedCount, nil
}
