package main

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoStore struct {
	client   *mongo.Client
	db       *mongo.Database
	database string
	healthy  atomic.Bool
}

func NewMongoStore(ctx context.Context, uri, database string) (*MongoStore, error) {
	if uri == "" {
		return nil, errors.New("empty mongo uri")
	}
	if database == "" {
		database = "k8s_delete_interceptor"
	}
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).SetServerSelectionTimeout(3*time.Second))
	if err != nil {
		return nil, err
	}
	m := &MongoStore{client: client, db: client.Database(database), database: database}
	if err := m.Ping(ctx); err != nil {
		return nil, err
	}
	if err := m.Init(ctx); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *MongoStore) Ping(ctx context.Context) error {
	if m == nil || m.client == nil {
		return errors.New("mongo not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	err := m.client.Ping(ctx, nil)
	m.healthy.Store(err == nil)
	return err
}

func (m *MongoStore) Healthy() bool { return m != nil && m.healthy.Load() }

func (m *MongoStore) Init(ctx context.Context) error {
	if m == nil {
		return nil
	}
	idx := []struct {
		col   string
		model mongo.IndexModel
	}{
		{"config_versions", mongo.IndexModel{Keys: bson.D{{Key: "version", Value: 1}}, Options: options.Index().SetUnique(true)}},
		{"config_versions", mongo.IndexModel{Keys: bson.D{{Key: "active", Value: 1}}, Options: options.Index().SetName("active_unique").SetUnique(true).SetPartialFilterExpression(bson.M{"active": true})}},
		{"config_change_requests", mongo.IndexModel{Keys: bson.D{{Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)}},
		{"config_change_requests", mongo.IndexModel{Keys: bson.D{{Key: "status", Value: 1}, {Key: "created_at", Value: -1}}}},
		{"admission_events", mongo.IndexModel{Keys: bson.D{{Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)}},
		{"admission_events", mongo.IndexModel{Keys: bson.D{{Key: "time", Value: -1}}}},
		{"admission_events", mongo.IndexModel{Keys: bson.D{{Key: "fingerprint", Value: 1}, {Key: "time", Value: -1}}}},
		{"admission_events", mongo.IndexModel{Keys: bson.D{{Key: "cluster", Value: 1}, {Key: "namespace", Value: 1}, {Key: "kind", Value: 1}, {Key: "name", Value: 1}, {Key: "operation", Value: 1}, {Key: "decision", Value: 1}}}},
		{"admission_approval_grants", mongo.IndexModel{Keys: bson.D{{Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)}},
		{"admission_approval_grants", mongo.IndexModel{Keys: bson.D{{Key: "expires_at", Value: 1}, {Key: "consumed", Value: 1}}}},
		{"rollback_backups", mongo.IndexModel{Keys: bson.D{{Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)}},
		{"rollback_backups", mongo.IndexModel{Keys: bson.D{{Key: "request_uid", Value: 1}}}},
		{"service_account_inventory", mongo.IndexModel{Keys: bson.D{{Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)}},
		{"service_account_inventory", mongo.IndexModel{Keys: bson.D{{Key: "namespace", Value: 1}, {Key: "name", Value: 1}}}},
		{"data_sources", mongo.IndexModel{Keys: bson.D{{Key: "active", Value: 1}}, Options: options.Index().SetName("datasource_active_unique").SetUnique(true).SetPartialFilterExpression(bson.M{"active": true, "enabled": true})}},
		{"cluster_metadata_snapshots", mongo.IndexModel{Keys: bson.D{{Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)}},
		{"telegram_config", mongo.IndexModel{Keys: bson.D{{Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)}},
		{"telegram_worker_locks", mongo.IndexModel{Keys: bson.D{{Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)}},
		{"telegram_notification_events", mongo.IndexModel{Keys: bson.D{{Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)}},
		{"telegram_notification_events", mongo.IndexModel{Keys: bson.D{{Key: "status", Value: 1}, {Key: "bot_id", Value: 1}, {Key: "next_attempt_at", Value: 1}, {Key: "created_at", Value: 1}}}},
		{"telegram_notification_events", mongo.IndexModel{Keys: bson.D{{Key: "kind", Value: 1}, {Key: "event_id", Value: 1}}}},
		{"telegram_notification_events", mongo.IndexModel{Keys: bson.D{{Key: "interactive", Value: 1}, {Key: "interaction_expires_at", Value: 1}, {Key: "interaction_closed_at", Value: 1}}}},
	}
	for _, x := range idx {
		_, _ = m.db.Collection(x.col).Indexes().CreateOne(ctx, x.model)
	}
	return nil
}

func (m *MongoStore) EnsureTelegramConfig(ctx context.Context, cfg TelegramConfig) error {
	if m == nil {
		return errors.New("mongo not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var existing TelegramConfig
	err := m.db.Collection("telegram_config").FindOne(ctx, bson.M{"id": "default"}).Decode(&existing)
	if err == nil {
		return nil
	}
	if !errors.Is(err, mongo.ErrNoDocuments) {
		m.healthy.Store(false)
		return err
	}
	if cfg.ID == "" {
		cfg.ID = "default"
	}
	if cfg.UpdatedAt.IsZero() {
		cfg.UpdatedAt = time.Now().UTC()
	}
	if cfg.Version == 0 {
		cfg.Version = 1
	}
	_, err = m.db.Collection("telegram_config").InsertOne(ctx, cfg)
	m.healthy.Store(err == nil)
	return err
}

func (m *MongoStore) GetTelegramConfig(ctx context.Context) (*TelegramConfig, error) {
	if m == nil {
		return nil, errors.New("mongo not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var cfg TelegramConfig
	err := m.db.Collection("telegram_config").FindOne(ctx, bson.M{"id": "default"}).Decode(&cfg)
	m.healthy.Store(err == nil)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (m *MongoStore) SaveTelegramConfig(ctx context.Context, cfg TelegramConfig, actor string) (*TelegramConfig, error) {
	if m == nil {
		return nil, errors.New("mongo not configured")
	}
	now := time.Now().UTC()
	cfg.ID = "default"
	cfg.UpdatedAt = now
	cfg.UpdatedBy = actor
	cfg.Version = 0 // updated through $inc to avoid lost updates and concurrent writes overwriting the counter
	ctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	var out TelegramConfig
	err := m.db.Collection("telegram_config").FindOneAndUpdate(ctx, bson.M{"id": "default"}, bson.M{"$set": cfg, "$inc": bson.M{"version": 1}}, options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&out)
	if errors.Is(err, mongo.ErrNoDocuments) {
		err = m.db.Collection("telegram_config").FindOne(ctx, bson.M{"id": "default"}).Decode(&out)
	}
	m.healthy.Store(err == nil)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (m *MongoStore) CountRunnableTelegramNotifications(ctx context.Context, now time.Time) (int64, error) {
	if m == nil {
		return 0, errors.New("mongo unavailable")
	}
	filter := bson.M{"$or": []bson.M{
		{"status": NotifyStatusPending, "next_attempt_at": bson.M{"$lte": now}},
		{"status": NotifyStatusPending, "next_attempt_at": bson.M{"$exists": false}},
		{"status": NotifyStatusSending, "claimed_at": bson.M{"$lte": now.Add(-telegramLease())}},
	}}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	n, err := m.db.Collection("telegram_notification_events").CountDocuments(ctx, filter)
	m.healthy.Store(err == nil)
	return n, err
}

func (m *MongoStore) acquireWorkerLock(ctx context.Context, lockID, owner string, lease time.Duration) (bool, error) {
	if m == nil {
		return false, errors.New("mongo unavailable")
	}
	if lease <= 0 {
		lease = 2 * time.Minute
	}
	now := time.Now().UTC()
	filter := bson.M{"id": lockID, "$or": []bson.M{{"expires_at": bson.M{"$lte": now}}, {"owner": owner}, {"expires_at": bson.M{"$exists": false}}}}
	update := bson.M{"$set": bson.M{"id": lockID, "owner": owner, "expires_at": now.Add(lease), "updated_at": now}, "$inc": bson.M{"epoch": 1}}
	opts := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var doc struct {
		Owner string `bson:"owner"`
	}
	err := m.db.Collection("telegram_worker_locks").FindOneAndUpdate(ctx, filter, update, opts).Decode(&doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return false, nil
		}
		m.healthy.Store(false)
		return false, err
	}
	m.healthy.Store(true)
	return doc.Owner == owner, nil
}

func (m *MongoStore) renewWorkerLock(ctx context.Context, lockID, owner string, lease time.Duration) (bool, error) {
	if m == nil {
		return false, errors.New("mongo unavailable")
	}
	if lease <= 0 {
		lease = 2 * time.Minute
	}
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	res, err := m.db.Collection("telegram_worker_locks").UpdateOne(ctx, bson.M{"id": lockID, "owner": owner}, bson.M{"$set": bson.M{"expires_at": now.Add(lease), "updated_at": now}})
	m.healthy.Store(err == nil)
	if err != nil {
		return false, err
	}
	return res.ModifiedCount > 0 || res.MatchedCount > 0, nil
}

func (m *MongoStore) releaseWorkerLock(ctx context.Context, lockID, owner string) error {
	if m == nil {
		return errors.New("mongo unavailable")
	}
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err := m.db.Collection("telegram_worker_locks").UpdateOne(ctx, bson.M{"id": lockID, "owner": owner}, bson.M{"$set": bson.M{"expires_at": now, "updated_at": now}})
	m.healthy.Store(err == nil)
	return err
}

func (m *MongoStore) AcquireTelegramDispatchLock(ctx context.Context, owner string, lease time.Duration) (bool, error) {
	return m.acquireWorkerLock(ctx, "telegram_dispatcher", owner, lease)
}

func (m *MongoStore) RenewTelegramDispatchLock(ctx context.Context, owner string, lease time.Duration) (bool, error) {
	return m.renewWorkerLock(ctx, "telegram_dispatcher", owner, lease)
}

func (m *MongoStore) ReleaseTelegramDispatchLock(ctx context.Context, owner string) error {
	return m.releaseWorkerLock(ctx, "telegram_dispatcher", owner)
}

func (m *MongoStore) AcquireTelegramCallbackLock(ctx context.Context, owner string, lease time.Duration) (bool, error) {
	return m.acquireWorkerLock(ctx, "telegram_callback_poller", owner, lease)
}

func (m *MongoStore) RenewTelegramCallbackLock(ctx context.Context, owner string, lease time.Duration) (bool, error) {
	return m.renewWorkerLock(ctx, "telegram_callback_poller", owner, lease)
}

func (m *MongoStore) ReleaseTelegramCallbackLock(ctx context.Context, owner string) error {
	return m.releaseWorkerLock(ctx, "telegram_callback_poller", owner)
}

func (m *MongoStore) EnsureConfig(ctx context.Context, cfg *RuntimeConfig) error {
	if m == nil || cfg == nil {
		return nil
	}
	var existing bson.M
	err := m.db.Collection("config_versions").FindOne(ctx, bson.M{"active": true}).Decode(&existing)
	if err == nil {
		return nil
	}
	if !errors.Is(err, mongo.ErrNoDocuments) {
		return err
	}
	return m.SaveConfig(ctx, cfg, "bootstrap", true)
}

func (m *MongoStore) SaveConfig(ctx context.Context, cfg *RuntimeConfig, source string, active bool) error {
	if m == nil {
		return errors.New("mongo not configured")
	}
	if err := validateRuntimeConfig(cfg); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	if active {
		_, _ = m.db.Collection("config_versions").UpdateMany(ctx, bson.M{"active": true}, bson.M{"$set": bson.M{"active": false}})
	}
	doc := bson.M{"version": cfg.Version, "active": active, "source": source, "created_at": time.Now().UTC(), "config": cfg}
	_, err := m.db.Collection("config_versions").UpdateOne(ctx, bson.M{"version": cfg.Version}, bson.M{"$set": doc}, options.Update().SetUpsert(true))
	m.healthy.Store(err == nil)
	return err
}

func (m *MongoStore) GetActiveConfig(ctx context.Context) (*RuntimeConfig, error) {
	if m == nil {
		return nil, errors.New("mongo not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var doc struct {
		Version int64         `bson:"version"`
		Config  RuntimeConfig `bson:"config"`
	}
	err := m.db.Collection("config_versions").FindOne(ctx, bson.M{"active": true}).Decode(&doc)
	m.healthy.Store(err == nil)
	if err != nil {
		return nil, err
	}
	cfg := doc.Config
	if cfg.Version == 0 {
		cfg.Version = doc.Version
	}
	if err := validateRuntimeConfig(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (m *MongoStore) GetConfigVersion(ctx context.Context, version int64) (*RuntimeConfig, error) {
	if m == nil {
		return nil, errors.New("mongo not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var doc struct {
		Version int64         `bson:"version"`
		Config  RuntimeConfig `bson:"config"`
	}
	err := m.db.Collection("config_versions").FindOne(ctx, bson.M{"version": version}).Decode(&doc)
	if err != nil {
		return nil, err
	}
	cfg := doc.Config
	if cfg.Version == 0 {
		cfg.Version = doc.Version
	}
	return &cfg, validateRuntimeConfig(&cfg)
}

func (m *MongoStore) ListConfigVersions(ctx context.Context, limit int) ([]ConfigVersionInfo, error) {
	if m == nil {
		return nil, errors.New("mongo not configured")
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cur, err := m.db.Collection("config_versions").Find(ctx, bson.M{}, options.Find().SetSort(bson.D{{Key: "version", Value: -1}}).SetLimit(int64(limit)))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	var out []ConfigVersionInfo
	for cur.Next(ctx) {
		var v ConfigVersionInfo
		if cur.Decode(&v) == nil {
			out = append(out, v)
		}
	}
	return out, nil
}

func (m *MongoStore) SaveConfigChange(ctx context.Context, cr *ConfigChangeRequest) error {
	if m == nil || cr == nil {
		return errors.New("mongo not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	_, err := m.db.Collection("config_change_requests").UpdateOne(ctx, bson.M{"id": cr.ID}, bson.M{"$set": cr}, options.Update().SetUpsert(true))
	m.healthy.Store(err == nil)
	return err
}

func (m *MongoStore) GetConfigChange(ctx context.Context, id string) (*ConfigChangeRequest, error) {
	if m == nil {
		return nil, errors.New("mongo not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var cr ConfigChangeRequest
	err := m.db.Collection("config_change_requests").FindOne(ctx, bson.M{"id": id}).Decode(&cr)
	return &cr, err
}

func (m *MongoStore) ClaimConfigChange(ctx context.Context, id, eventID, nextStatus, decidedBy string) (*ConfigChangeRequest, error) {
	if m == nil {
		return nil, errors.New("mongo not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	filter := bson.M{"id": id, "status": ChangePending}
	if strings.TrimSpace(eventID) != "" {
		filter["event_id"] = eventID
	}
	update := bson.M{"$set": bson.M{"status": nextStatus, "decided_by": decidedBy, "decided_at": time.Now().UTC()}}
	var cr ConfigChangeRequest
	err := m.db.Collection("config_change_requests").FindOneAndUpdate(ctx, filter, update, options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&cr)
	m.healthy.Store(err == nil)
	if err != nil {
		return nil, err
	}
	return &cr, nil
}

func (m *MongoStore) AddConfigChangeTelegramRef(ctx context.Context, id string, ref TelegramMessageRef) error {
	if m == nil {
		return errors.New("mongo not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := m.db.Collection("config_change_requests").UpdateOne(ctx, bson.M{"id": id}, bson.M{"$addToSet": bson.M{"notification_messages": ref}})
	m.healthy.Store(err == nil)
	return err
}

func (m *MongoStore) ListConfigChanges(ctx context.Context, status string, limit int) ([]ConfigChangeRequest, error) {
	if m == nil {
		return nil, errors.New("mongo not configured")
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	filter := bson.M{}
	if status != "" {
		filter["status"] = status
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cur, err := m.db.Collection("config_change_requests").Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "created_at", Value: -1}}).SetLimit(int64(limit)))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	var out []ConfigChangeRequest
	for cur.Next(ctx) {
		var cr ConfigChangeRequest
		if cur.Decode(&cr) == nil {
			out = append(out, cr)
		}
	}
	return out, nil
}

func (m *MongoStore) SaveConfigAudit(ctx context.Context, ev *ConfigAuditEvent) error {
	if m == nil || ev == nil {
		return errors.New("mongo not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := m.db.Collection("config_audit_events").UpdateOne(ctx, bson.M{"id": ev.ID}, bson.M{"$set": ev}, options.Update().SetUpsert(true))
	m.healthy.Store(err == nil)
	return err
}

func (m *MongoStore) ListConfigAudits(ctx context.Context, category string, limit int) ([]ConfigAuditEvent, error) {
	if m == nil {
		return nil, errors.New("mongo not configured")
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	filter := bson.M{}
	if strings.TrimSpace(category) != "" {
		filter["category"] = strings.TrimSpace(category)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cur, err := m.db.Collection("config_audit_events").Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "created_at", Value: -1}}).SetLimit(int64(limit)))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	out := []ConfigAuditEvent{}
	for cur.Next(ctx) {
		var ev ConfigAuditEvent
		if cur.Decode(&ev) == nil {
			out = append(out, ev)
		}
	}
	return out, nil
}

func (m *MongoStore) SaveEvent(ctx context.Context, ev *AdmissionEvent) error {
	if m == nil || ev == nil {
		return errors.New("mongo unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := m.db.Collection("admission_events").UpdateOne(ctx, bson.M{"id": ev.ID}, bson.M{"$set": ev}, options.Update().SetUpsert(true))
	m.healthy.Store(err == nil)
	return err
}

func (m *MongoStore) ListEvents(ctx context.Context, limit int) ([]AdmissionEvent, error) {
	return m.ListEventsByQuery(ctx, EventQuery{Limit: limit})
}

func (m *MongoStore) ListEventsByQuery(ctx context.Context, q EventQuery) ([]AdmissionEvent, error) {
	if m == nil {
		return nil, errors.New("mongo unavailable")
	}
	filter := bson.M{}
	if q.ID != "" {
		filter["$or"] = []bson.M{{"id": q.ID}, {"request_uid": q.ID}}
	}
	if !q.Start.IsZero() || !q.End.IsZero() {
		timeFilter := bson.M{}
		if !q.Start.IsZero() {
			timeFilter["$gte"] = q.Start.UTC()
		}
		if !q.End.IsZero() {
			timeFilter["$lt"] = q.End.UTC()
		}
		filter["time"] = timeFilter
	}
	addPatternFilter(filter, "cluster", q.Cluster, true)
	addPatternFilter(filter, "namespace", q.Namespace, true)
	addPatternFilter(filter, "kind", q.Kind, true)
	addPatternFilter(filter, "resource", q.Resource, true)
	addPatternFilter(filter, "operation", q.Operation, true)
	addPatternFilter(filter, "decision", q.Decision, true)
	addPatternFilter(filter, "name", q.Name, false)
	addPatternFilter(filter, "user", q.User, false)
	if q.Allowed != nil {
		filter["allowed"] = *q.Allowed
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	cur, err := m.db.Collection("admission_events").Find(ctx, filter, options.Find().SetLimit(int64(q.NormalizedLimit(100))).SetSort(bson.D{{Key: "time", Value: -1}}))
	if err != nil {
		m.healthy.Store(false)
		return nil, err
	}
	defer cur.Close(ctx)
	var out []AdmissionEvent
	for cur.Next(ctx) {
		var ev AdmissionEvent
		if cur.Decode(&ev) == nil {
			out = append(out, ev)
		}
	}
	m.healthy.Store(true)
	return out, nil
}

func (m *MongoStore) GetEvent(ctx context.Context, id string) (*AdmissionEvent, error) {
	if m == nil {
		return nil, errors.New("mongo unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var ev AdmissionEvent
	err := m.db.Collection("admission_events").FindOne(ctx, bson.M{"id": id}).Decode(&ev)
	m.healthy.Store(err == nil)
	if err != nil {
		return nil, err
	}
	return &ev, nil
}

func (m *MongoStore) EnqueueTelegramNotification(ctx context.Context, ev *TelegramNotificationEvent) error {
	if m == nil || ev == nil {
		return errors.New("mongo unavailable")
	}
	if ev.ID == "" {
		return errors.New("notification id is required")
	}
	now := time.Now().UTC()
	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = now
	}
	if ev.NextAttemptAt.IsZero() {
		ev.NextAttemptAt = now
	}
	if ev.Status == "" {
		ev.Status = NotifyStatusPending
	}
	if ev.MaxAttempts <= 0 {
		ev.MaxAttempts = 10
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := m.db.Collection("telegram_notification_events").UpdateOne(ctx, bson.M{"id": ev.ID}, bson.M{"$setOnInsert": ev}, options.Update().SetUpsert(true))
	m.healthy.Store(err == nil)
	return err
}

func (m *MongoStore) ClaimTelegramNotification(ctx context.Context, worker string, lease time.Duration) (*TelegramNotificationEvent, error) {
	if m == nil {
		return nil, errors.New("mongo unavailable")
	}
	if lease <= 0 {
		lease = 2 * time.Minute
	}
	now := time.Now().UTC()
	stale := now.Add(-lease)
	filter := bson.M{"$or": []bson.M{
		{"status": NotifyStatusPending, "next_attempt_at": bson.M{"$lte": now}},
		{"status": NotifyStatusPending, "next_attempt_at": bson.M{"$exists": false}},
		{"status": NotifyStatusSending, "claimed_at": bson.M{"$lte": stale}},
	}}
	update := bson.M{"$set": bson.M{"status": NotifyStatusSending, "claimed_by": worker, "claimed_at": now}, "$inc": bson.M{"attempts": 1}}
	opts := options.FindOneAndUpdate().SetSort(bson.D{{Key: "created_at", Value: 1}}).SetReturnDocument(options.After)
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var ev TelegramNotificationEvent
	err := m.db.Collection("telegram_notification_events").FindOneAndUpdate(ctx, filter, update, opts).Decode(&ev)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		m.healthy.Store(false)
		return nil, err
	}
	m.healthy.Store(true)
	return &ev, nil
}

func (m *MongoStore) GetTelegramNotification(ctx context.Context, id string) (*TelegramNotificationEvent, error) {
	if m == nil {
		return nil, errors.New("mongo unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var ev TelegramNotificationEvent
	err := m.db.Collection("telegram_notification_events").FindOne(ctx, bson.M{"id": id}).Decode(&ev)
	m.healthy.Store(err == nil)
	if err != nil {
		return nil, err
	}
	return &ev, nil
}

func (m *MongoStore) MarkTelegramNotificationViewed(ctx context.Context, id, viewedBy string) (*TelegramNotificationEvent, error) {
	if m == nil {
		return nil, errors.New("mongo unavailable")
	}
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	update := bson.M{"$set": bson.M{"viewed_at": now, "viewed_by": viewedBy}, "$inc": bson.M{"view_count": 1}}
	var ev TelegramNotificationEvent
	err := m.db.Collection("telegram_notification_events").FindOneAndUpdate(ctx, bson.M{"id": id}, update, options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&ev)
	m.healthy.Store(err == nil)
	if err != nil {
		return nil, err
	}
	return &ev, nil
}

func (m *MongoStore) ListTelegramNotificationsByChange(ctx context.Context, changeID string) ([]TelegramNotificationEvent, error) {
	if m == nil {
		return nil, errors.New("mongo unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cur, err := m.db.Collection("telegram_notification_events").Find(ctx, bson.M{"change_id": changeID, "status": NotifyStatusSent}, options.Find().SetSort(bson.D{{Key: "sent_at", Value: 1}}))
	if err != nil {
		m.healthy.Store(false)
		return nil, err
	}
	defer cur.Close(ctx)
	out := []TelegramNotificationEvent{}
	for cur.Next(ctx) {
		var ev TelegramNotificationEvent
		if cur.Decode(&ev) == nil {
			out = append(out, ev)
		}
	}
	m.healthy.Store(true)
	return out, nil
}

func (m *MongoStore) ListTelegramNotificationsByEvent(ctx context.Context, eventID string) ([]TelegramNotificationEvent, error) {
	if m == nil {
		return nil, errors.New("mongo unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cur, err := m.db.Collection("telegram_notification_events").Find(ctx, bson.M{"event_id": eventID, "status": NotifyStatusSent}, options.Find().SetSort(bson.D{{Key: "sent_at", Value: 1}}))
	if err != nil {
		m.healthy.Store(false)
		return nil, err
	}
	defer cur.Close(ctx)
	out := []TelegramNotificationEvent{}
	for cur.Next(ctx) {
		var ev TelegramNotificationEvent
		if cur.Decode(&ev) == nil {
			out = append(out, ev)
		}
	}
	m.healthy.Store(true)
	return out, nil
}

func (m *MongoStore) CompleteTelegramNotification(ctx context.Context, id string, messageID int64) error {
	if m == nil {
		return errors.New("mongo unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := m.db.Collection("telegram_notification_events").UpdateOne(ctx, bson.M{"id": id}, bson.M{"$set": bson.M{"status": NotifyStatusSent, "sent_at": time.Now().UTC(), "message_id": messageID, "last_error": ""}})
	m.healthy.Store(err == nil)
	return err
}

func (m *MongoStore) FailTelegramNotification(ctx context.Context, ev *TelegramNotificationEvent, errMsg string, next time.Time, terminal bool) error {
	if m == nil || ev == nil {
		return errors.New("mongo unavailable")
	}
	status := NotifyStatusPending
	if terminal {
		status = NotifyStatusFailed
	}
	set := bson.M{"status": status, "last_error": errMsg, "next_attempt_at": next}
	if terminal {
		set["sent_at"] = time.Now().UTC()
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := m.db.Collection("telegram_notification_events").UpdateOne(ctx, bson.M{"id": ev.ID}, bson.M{"$set": set})
	m.healthy.Store(err == nil)
	return err
}

func (m *MongoStore) FindRecentEventByFingerprint(ctx context.Context, fingerprint string, since time.Time) (*AdmissionEvent, error) {
	if m == nil || strings.TrimSpace(fingerprint) == "" {
		return nil, errors.New("mongo unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	filter := bson.M{"fingerprint": fingerprint, "time": bson.M{"$gte": since}}
	var ev AdmissionEvent
	err := m.db.Collection("admission_events").FindOne(ctx, filter, options.FindOne().SetSort(bson.D{{Key: "time", Value: -1}})).Decode(&ev)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			m.healthy.Store(true)
			return nil, nil
		}
		m.healthy.Store(false)
		return nil, err
	}
	m.healthy.Store(true)
	return &ev, nil
}

func (m *MongoStore) CountActiveTelegramInteractions(ctx context.Context, now time.Time) (int64, error) {
	if m == nil {
		return 0, errors.New("mongo unavailable")
	}
	filter := bson.M{"interactive": true, "status": NotifyStatusSent, "interaction_expires_at": bson.M{"$gt": now}, "$or": []bson.M{{"interaction_closed_at": bson.M{"$exists": false}}, {"interaction_closed_at": time.Time{}}}}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	n, err := m.db.Collection("telegram_notification_events").CountDocuments(ctx, filter)
	m.healthy.Store(err == nil)
	return n, err
}

func (m *MongoStore) ListExpiredTelegramInteractions(ctx context.Context, now time.Time, limit int) ([]TelegramNotificationEvent, error) {
	if m == nil {
		return nil, errors.New("mongo unavailable")
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	filter := bson.M{"interactive": true, "status": NotifyStatusSent, "interaction_expires_at": bson.M{"$lte": now}, "$or": []bson.M{{"interaction_closed_at": bson.M{"$exists": false}}, {"interaction_closed_at": time.Time{}}}}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cur, err := m.db.Collection("telegram_notification_events").Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "interaction_expires_at", Value: 1}}).SetLimit(int64(limit)))
	if err != nil {
		m.healthy.Store(false)
		return nil, err
	}
	defer cur.Close(ctx)
	out := []TelegramNotificationEvent{}
	for cur.Next(ctx) {
		var ev TelegramNotificationEvent
		if cur.Decode(&ev) == nil {
			out = append(out, ev)
		}
	}
	m.healthy.Store(cur.Err() == nil)
	return out, cur.Err()
}

func (m *MongoStore) CloseTelegramNotificationInteraction(ctx context.Context, id string, markup any, note string) error {
	if m == nil || strings.TrimSpace(id) == "" {
		return errors.New("mongo unavailable")
	}
	set := bson.M{"interaction_closed_at": time.Now().UTC(), "interactive": false}
	if m, ok := markup.(map[string]any); ok && m != nil {
		set["reply_markup"] = m
	}
	if strings.TrimSpace(note) != "" {
		set["last_error"] = "interaction closed: " + note
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := m.db.Collection("telegram_notification_events").UpdateOne(ctx, bson.M{"id": id}, bson.M{"$set": set})
	m.healthy.Store(err == nil)
	return err
}

func addPatternFilter(filter bson.M, key, value string, exactDefault bool) {
	value = strings.TrimSpace(value)
	if value == "" || value == "*" || strings.EqualFold(value, "all") {
		return
	}
	if strings.HasPrefix(value, "regex:") {
		filter[key] = bson.M{"$regex": strings.TrimPrefix(value, "regex:"), "$options": "i"}
		return
	}
	if strings.ContainsAny(value, "*?") {
		filter[key] = bson.M{"$regex": strings.TrimPrefix(wildcardRegex(strings.ToLower(value)).String(), "^"), "$options": "i"}
		return
	}
	if exactDefault {
		filter[key] = value
		return
	}
	filter[key] = bson.M{"$regex": regexp.QuoteMeta(value), "$options": "i"}
}

func (m *MongoStore) SaveRollback(ctx context.Context, rb *RollbackBackup) error {
	if m == nil || rb == nil {
		return errors.New("mongo unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := m.db.Collection("rollback_backups").UpdateOne(ctx, bson.M{"id": rb.ID}, bson.M{"$setOnInsert": rb}, options.Update().SetUpsert(true))
	m.healthy.Store(err == nil)
	return err
}

func (m *MongoStore) GetRollback(ctx context.Context, id string) (*RollbackBackup, error) {
	if m == nil {
		return nil, errors.New("mongo unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var rb RollbackBackup
	err := m.db.Collection("rollback_backups").FindOne(ctx, bson.M{"id": id}).Decode(&rb)
	m.healthy.Store(err == nil)
	return &rb, err
}

func (m *MongoStore) SaveServiceAccounts(ctx context.Context, items []ServiceAccountInfo) error {
	if m == nil {
		return errors.New("mongo unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for _, it := range items {
		_, err := m.db.Collection("service_account_inventory").UpdateOne(ctx, bson.M{"id": it.ID}, bson.M{"$set": it}, options.Update().SetUpsert(true))
		if err != nil {
			m.healthy.Store(false)
			return err
		}
	}
	m.healthy.Store(true)
	return nil
}

func (m *MongoStore) ListServiceAccounts(ctx context.Context, limit int) ([]ServiceAccountInfo, error) {
	return m.ListServiceAccountsByNamespace(ctx, "", limit)
}

func (m *MongoStore) ListServiceAccountsByNamespace(ctx context.Context, namespace string, limit int) ([]ServiceAccountInfo, error) {
	if m == nil {
		return nil, errors.New("mongo unavailable")
	}
	if limit <= 0 || limit > 5000 {
		limit = 1000
	}
	filter := bson.M{}
	if namespace != "" && namespace != "all" && namespace != "*" {
		filter["namespace"] = namespace
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cur, err := m.db.Collection("service_account_inventory").Find(ctx, filter, options.Find().SetLimit(int64(limit)).SetSort(bson.D{{Key: "namespace", Value: 1}, {Key: "name", Value: 1}}))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	var out []ServiceAccountInfo
	for cur.Next(ctx) {
		var it ServiceAccountInfo
		if cur.Decode(&it) == nil {
			out = append(out, it)
		}
	}
	return out, nil
}

func (m *MongoStore) SaveClusterMetadata(ctx context.Context, md *ClusterMetadata) error {
	if m == nil || md == nil {
		return errors.New("mongo unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	doc := bson.M{"id": "current", "generated_at": md.GeneratedAt, "metadata": md}
	_, err := m.db.Collection("cluster_metadata_snapshots").UpdateOne(ctx, bson.M{"id": "current"}, bson.M{"$set": doc}, options.Update().SetUpsert(true))
	m.healthy.Store(err == nil)
	return err
}

func (m *MongoStore) LoadClusterMetadata(ctx context.Context) (*ClusterMetadata, error) {
	if m == nil {
		return nil, errors.New("mongo unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var doc struct {
		Metadata ClusterMetadata `bson:"metadata"`
	}
	if err := m.db.Collection("cluster_metadata_snapshots").FindOne(ctx, bson.M{"id": "current"}).Decode(&doc); err != nil {
		return nil, err
	}
	return &doc.Metadata, nil
}

func (m *MongoStore) Disconnect(ctx context.Context) {
	if m != nil && m.client != nil {
		_ = m.client.Disconnect(ctx)
	}
}

func resolveMongoURI(cfg *RuntimeConfig) (uri, database string) {
	for _, ds := range cfg.DataSources {
		if !ds.Enabled || !ds.Active {
			continue
		}
		uri = ds.URI
		if uri == "" && ds.URIEnv != "" {
			uri = envOr(ds.URIEnv, "")
		}
		database = ds.Database
		if database == "" {
			database = envOr("MONGO_DATABASE", "k8s_delete_interceptor")
		}
		return
	}
	return envOr("MONGO_URI", ""), envOr("MONGO_DATABASE", "k8s_delete_interceptor")
}

func (m *MongoStore) Test(ctx context.Context) string {
	if m == nil {
		return "not_configured"
	}
	if err := m.Ping(ctx); err != nil {
		return fmt.Sprintf("unavailable: %v", err)
	}
	return "healthy"
}
