package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoStore struct {
	client  *mongo.Client
	db      *mongo.Database
	healthy atomic.Bool
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
	m := &MongoStore{client: client, db: client.Database(database)}
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
		{"admission_events", mongo.IndexModel{Keys: bson.D{{Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)}},
		{"admission_events", mongo.IndexModel{Keys: bson.D{{Key: "time", Value: -1}}}},
		{"admission_events", mongo.IndexModel{Keys: bson.D{{Key: "cluster", Value: 1}, {Key: "namespace", Value: 1}, {Key: "kind", Value: 1}, {Key: "name", Value: 1}, {Key: "operation", Value: 1}, {Key: "decision", Value: 1}}}},
		{"rollback_backups", mongo.IndexModel{Keys: bson.D{{Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)}},
		{"rollback_backups", mongo.IndexModel{Keys: bson.D{{Key: "request_uid", Value: 1}}}},
		{"service_account_inventory", mongo.IndexModel{Keys: bson.D{{Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)}},
		{"data_sources", mongo.IndexModel{Keys: bson.D{{Key: "active", Value: 1}}, Options: options.Index().SetName("datasource_active_unique").SetUnique(true).SetPartialFilterExpression(bson.M{"active": true, "enabled": true})}},
	}
	for _, x := range idx {
		_, _ = m.db.Collection(x.col).Indexes().CreateOne(ctx, x.model)
	}
	return nil
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

func (m *MongoStore) SaveEvent(ctx context.Context, ev *AdmissionEvent) error {
	if m == nil || ev == nil {
		return errors.New("mongo unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := m.db.Collection("admission_events").UpdateOne(ctx, bson.M{"id": ev.ID}, bson.M{"$setOnInsert": ev}, options.Update().SetUpsert(true))
	m.healthy.Store(err == nil)
	return err
}

func (m *MongoStore) ListEvents(ctx context.Context, limit int) ([]AdmissionEvent, error) {
	if m == nil {
		return nil, errors.New("mongo unavailable")
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cur, err := m.db.Collection("admission_events").Find(ctx, bson.M{}, options.Find().SetLimit(int64(limit)).SetSort(bson.D{{Key: "time", Value: -1}}))
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
	if m == nil {
		return nil, errors.New("mongo unavailable")
	}
	if limit <= 0 || limit > 5000 {
		limit = 1000
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cur, err := m.db.Collection("service_account_inventory").Find(ctx, bson.M{}, options.Find().SetLimit(int64(limit)).SetSort(bson.D{{Key: "namespace", Value: 1}, {Key: "name", Value: 1}}))
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
