package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type RetentionRunStats struct {
	Archived map[string]int64 `json:"archived"`
	Purged   map[string]int64 `json:"purged"`
}

func parseConfigDuration(value string, def time.Duration) time.Duration {
	v := strings.TrimSpace(value)
	if v == "" {
		return def
	}
	if d, err := time.ParseDuration(v); err == nil && d > 0 {
		return d
	}
	return def
}

func activeDataTTL(cfg *RuntimeConfig) time.Duration {
	if cfg == nil {
		return 24 * time.Hour
	}
	return parseConfigDuration(cfg.Persistence.ActiveDataTTL, 24*time.Hour)
}

func coldDataTTL(cfg *RuntimeConfig) time.Duration {
	if cfg == nil {
		return 24 * time.Hour
	}
	return parseConfigDuration(cfg.Persistence.ColdDataTTL, 24*time.Hour)
}

func retentionInterval(cfg *RuntimeConfig) time.Duration {
	if cfg == nil {
		return 15 * time.Minute
	}
	d := parseConfigDuration(cfg.Persistence.CleanupInterval, 15*time.Minute)
	if d < time.Minute {
		return time.Minute
	}
	return d
}

func telegramInteractionTTL(cfg *RuntimeConfig) time.Duration {
	if cfg == nil {
		return 12 * time.Hour
	}
	return parseConfigDuration(cfg.Persistence.TelegramInteractionTTL, 12*time.Hour)
}

func duplicateEventWindow(cfg *RuntimeConfig) time.Duration {
	if cfg == nil {
		return 30 * time.Second
	}
	return parseConfigDuration(cfg.Persistence.DuplicateEventWindow, 30*time.Second)
}

func retentionBatchSize(cfg *RuntimeConfig) int {
	if cfg == nil || cfg.Persistence.ArchiveBatchSize <= 0 {
		return 500
	}
	if cfg.Persistence.ArchiveBatchSize > 5000 {
		return 5000
	}
	return cfg.Persistence.ArchiveBatchSize
}

func (a *App) retentionMaintenanceLoop(ctx context.Context) {
	waitContext(ctx, 20*time.Second)
	for {
		cfg := a.Config()
		interval := retentionInterval(cfg)
		if cfg != nil && cfg.Persistence.Enabled && a.mongo != nil && a.mongo.Healthy() {
			if err := a.expireTelegramInteractionWindows(ctx, cfg); err != nil {
				log.Printf("telegram interaction expiration failed: %v", err)
			}
			stats, err := a.mongo.ArchiveAndPurgeOperationalData(ctx, cfg)
			if err != nil {
				log.Printf("data retention failed: %v", retentionErrorWithHint(cfg, err))
			} else if retentionStatsNonZero(stats) {
				log.Printf("data retention completed: archived=%v purged=%v", stats.Archived, stats.Purged)
			}
		}
		waitContext(ctx, interval)
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func retentionErrorWithHint(cfg *RuntimeConfig, err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if !strings.Contains(strings.ToLower(msg), "unauthorized") && !strings.Contains(strings.ToLower(msg), "not authorized") {
		return err
	}
	warmDB := envOr("MONGO_DATABASE", "k8s_delete_interceptor")
	coldDB := warmDB + "_cold"
	if cfg != nil {
		for _, ds := range cfg.DataSources {
			if ds.Active && strings.TrimSpace(ds.Database) != "" {
				warmDB = strings.TrimSpace(ds.Database)
			}
		}
		if strings.TrimSpace(cfg.Persistence.ColdDatabase) != "" {
			coldDB = strings.TrimSpace(cfg.Persistence.ColdDatabase)
		} else {
			coldDB = warmDB + "_cold"
		}
	}
	return fmt.Errorf("%w；冷库归档需要 Mongo 用户同时拥有热库 %s 的 readWrite 和冷库 %s 的 readWrite 权限。Mongo 不会预先创建空库，首次写入冷库时会自动创建；请给当前 MONGO_URI 用户补充冷库权限后重试", err, warmDB, coldDB)
}

func retentionStatsNonZero(s RetentionRunStats) bool {
	for _, n := range s.Archived {
		if n > 0 {
			return true
		}
	}
	for _, n := range s.Purged {
		if n > 0 {
			return true
		}
	}
	return false
}

func (m *MongoStore) coldDatabase(cfg *RuntimeConfig) *mongo.Database {
	name := ""
	if cfg != nil {
		name = strings.TrimSpace(cfg.Persistence.ColdDatabase)
	}
	if name == "" {
		name = m.database + "_cold"
	}
	return m.client.Database(name)
}

func (m *MongoStore) ArchiveAndPurgeOperationalData(ctx context.Context, cfg *RuntimeConfig) (RetentionRunStats, error) {
	stats := RetentionRunStats{Archived: map[string]int64{}, Purged: map[string]int64{}}
	if m == nil || m.db == nil || cfg == nil || !cfg.Persistence.Enabled {
		return stats, nil
	}
	warmTTL := activeDataTTL(cfg)
	coldTTL := coldDataTTL(cfg)
	if warmTTL <= 0 || coldTTL <= 0 {
		return stats, nil
	}
	cold := m.coldDatabase(cfg)
	now := time.Now().UTC()
	warmBefore := now.Add(-warmTTL)
	batch := retentionBatchSize(cfg)
	collections := []struct {
		Name      string
		TimeField string
	}{
		{Name: "admission_events", TimeField: "time"},
		{Name: "rollback_backups", TimeField: "created_at"},
		{Name: "telegram_notification_events", TimeField: "created_at"},
		{Name: "config_change_requests", TimeField: "created_at"},
		{Name: "config_audit_events", TimeField: "created_at"},
		{Name: "admission_approval_grants", TimeField: "approved_at"},
	}
	for _, c := range collections {
		archived, err := m.archiveWarmCollection(ctx, cold, c.Name, c.TimeField, warmBefore, batch, now)
		stats.Archived[c.Name] = archived
		if err != nil {
			return stats, err
		}
		purged, err := m.purgeColdCollection(ctx, cold, c.Name, now.Add(-coldTTL))
		stats.Purged[c.Name] = purged
		if err != nil {
			return stats, err
		}
	}
	m.healthy.Store(true)
	return stats, nil
}

func (m *MongoStore) archiveWarmCollection(ctx context.Context, cold *mongo.Database, name, timeField string, before time.Time, batch int, now time.Time) (int64, error) {
	if m == nil || cold == nil {
		return 0, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	filter := bson.M{timeField: bson.M{"$lt": before}}
	cur, err := m.db.Collection(name).Find(ctx, filter, options.Find().SetSort(bson.D{{Key: timeField, Value: 1}}).SetLimit(int64(batch)))
	if err != nil {
		return 0, err
	}
	defer cur.Close(ctx)
	var moved int64
	for cur.Next(ctx) {
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			return moved, err
		}
		doc["archived_at"] = now
		doc["warm_database"] = m.database
		doc["warm_collection"] = name
		idFilter, err := documentIdentityFilter(doc)
		if err != nil {
			return moved, err
		}
		if _, err := cold.Collection(name).ReplaceOne(ctx, idFilter, doc, options.Replace().SetUpsert(true)); err != nil {
			return moved, err
		}
		if _, err := m.db.Collection(name).DeleteOne(ctx, idFilter); err != nil {
			return moved, err
		}
		moved++
	}
	return moved, cur.Err()
}

func (m *MongoStore) purgeColdCollection(ctx context.Context, cold *mongo.Database, name string, before time.Time) (int64, error) {
	if cold == nil {
		return 0, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	res, err := cold.Collection(name).DeleteMany(ctx, bson.M{"archived_at": bson.M{"$lt": before}})
	if err != nil {
		return 0, err
	}
	return res.DeletedCount, nil
}

func documentIdentityFilter(doc bson.M) (bson.M, error) {
	if v, ok := doc["_id"]; ok {
		return bson.M{"_id": v}, nil
	}
	if v, ok := doc["id"]; ok && fmt.Sprint(v) != "" {
		return bson.M{"id": v}, nil
	}
	return nil, fmt.Errorf("cannot archive document without _id or id")
}
