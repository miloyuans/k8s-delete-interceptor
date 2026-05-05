package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type mongoRollbackStore struct {
	collection *mongo.Collection
	timeout    time.Duration
	retention  time.Duration
}

func newMongoRollbackStore(cfg RollbackConfig) (*mongoRollbackStore, error) {
	mongoCfg, ok := resolveRollbackMongoConfig(cfg)
	if !ok {
		return nil, fmt.Errorf("mongo backend requested but no rollback mongo configuration is available")
	}

	retentionHours := cfg.RetentionHours
	if retentionHours <= 0 {
		retentionHours = defaultRollbackRetentionHours
	}
	connectTimeoutSeconds := mongoCfg.ConnectTimeoutSeconds
	if connectTimeoutSeconds <= 0 {
		connectTimeoutSeconds = defaultMongoConnectTimeout
	}
	timeout := time.Duration(connectTimeoutSeconds) * time.Second

	coll, err := initRollbackMongoCollection(mongoCfg, timeout)
	if err != nil {
		return nil, err
	}

	return &mongoRollbackStore{
		collection: coll,
		timeout:    timeout,
		retention:  time.Duration(retentionHours) * time.Hour,
	}, nil
}

func (s *mongoRollbackStore) Type() string { return rollbackStorageMongo }

func resolveRollbackMongoConfig(cfg RollbackConfig) (MongoAuditConfig, bool) {
	if cfg.Mongo.Enabled {
		mongoCfg := cfg.Mongo
		if strings.TrimSpace(mongoCfg.Collection) == "" {
			mongoCfg.Collection = defaultRollbackCollection
		}
		return mongoCfg, true
	}
	if config.Audit.Mongo.Enabled {
		mongoCfg := config.Audit.Mongo
		mongoCfg.Collection = defaultRollbackCollection
		return mongoCfg, true
	}
	return MongoAuditConfig{}, false
}

func initRollbackMongoCollection(cfg MongoAuditConfig, timeout time.Duration) (*mongo.Collection, error) {
	uri := strings.TrimSpace(cfg.URI)
	if uri == "" {
		uri = strings.TrimSpace(os.Getenv("MONGODB_URI"))
	}
	if uri == "" {
		return nil, fmt.Errorf("rollback mongo enabled but no uri configured")
	}

	database := strings.TrimSpace(cfg.Database)
	if database == "" {
		database = defaultMongoDatabase
	}

	collection := strings.TrimSpace(cfg.Collection)
	if collection == "" {
		collection = defaultRollbackCollection
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to rollback mongodb: %w", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("failed to ping rollback mongodb: %w", err)
	}

	coll := client.Database(database).Collection(collection)
	ttlIndex := mongo.IndexModel{
		Keys: bson.D{{Key: "expires_at", Value: 1}},
		Options: options.Index().
			SetName("rollback_expires_at_ttl").
			SetExpireAfterSeconds(0),
	}
	if _, err := coll.Indexes().CreateOne(ctx, ttlIndex); err != nil {
		return nil, fmt.Errorf("failed to create rollback ttl index: %w", err)
	}

	requestIndex := mongo.IndexModel{
		Keys:    bson.D{{Key: "request_uid", Value: 1}},
		Options: options.Index().SetName("rollback_request_uid"),
	}
	_, _ = coll.Indexes().CreateOne(ctx, requestIndex)

	return coll, nil
}

func (s *mongoRollbackStore) SaveRecord(record RollbackRecord, manifestYAML string) error {
	record = normalizeRollbackRecord(record)
	record.ManifestYAML = manifestYAML
	if strings.TrimSpace(record.ExecutionStatus) == "" {
		record.ExecutionStatus = rollbackStatusPending
	}
	record.History = append(record.History, rollbackHistory("created", "system", record.ExecutionStatus, "rollback backup recorded"))

	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	_, err := s.collection.UpdateByID(ctx, record.ID, bson.M{
		"$setOnInsert": record,
	}, options.Update().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("failed to persist rollback record to mongodb: %w", err)
	}
	return nil
}

func (s *mongoRollbackStore) LoadRecord(id string) (RollbackRecord, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	var record RollbackRecord
	err := s.collection.FindOne(ctx, bson.M{
		"_id":        id,
		"expires_at": bson.M{"$gt": time.Now()},
	}).Decode(&record)
	if err == mongo.ErrNoDocuments {
		return RollbackRecord{}, "", ErrRollbackNotFound
	}
	if err != nil {
		return RollbackRecord{}, "", err
	}
	return record, record.ManifestYAML, nil
}

func (s *mongoRollbackStore) MarkRunning(id string, telegramID string, allowReapply bool) (RollbackRecord, string, error) {
	filter := bson.M{
		"_id":        id,
		"expires_at": bson.M{"$gt": time.Now()},
	}
	if !allowReapply {
		filter["execution_status"] = bson.M{"$ne": rollbackStatusApplied}
	}
	update := bson.M{
		"$inc": bson.M{"rollback_click_count": 1},
		"$set": bson.M{
			"execution_status": rollbackStatusRunning,
			"execution_error":  "",
			"last_clicked_at":  time.Now(),
			"last_clicked_by":  telegramID,
			"last_action":      rollbackActionApply,
			"updated_at":       time.Now(),
		},
		"$push": bson.M{"history": rollbackHistory("rollback_apply_clicked", telegramID, rollbackStatusRunning, "rollback execution started")},
	}
	return s.findOneAndUpdate(filter, update)
}

func (s *mongoRollbackStore) MarkApplied(id string, telegramID string) (RollbackRecord, string, error) {
	now := time.Now()
	update := bson.M{
		"$set": bson.M{
			"execution_status": rollbackStatusApplied,
			"execution_error":  "",
			"executed_at":      now,
			"executed_by":      telegramID,
			"last_clicked_at":  now,
			"last_clicked_by":  telegramID,
			"last_action":      rollbackActionApply,
			"updated_at":       now,
		},
		"$push": bson.M{"history": rollbackHistory("rollback_applied", telegramID, rollbackStatusApplied, "rollback applied successfully")},
	}
	return s.findOneAndUpdate(bson.M{"_id": id}, update)
}

func (s *mongoRollbackStore) MarkFailed(id string, telegramID string, message string) (RollbackRecord, string, error) {
	now := time.Now()
	update := bson.M{
		"$set": bson.M{
			"execution_status": rollbackStatusFailed,
			"execution_error":  message,
			"executed_at":      now,
			"executed_by":      telegramID,
			"last_clicked_at":  now,
			"last_clicked_by":  telegramID,
			"last_action":      rollbackActionApply,
			"updated_at":       now,
		},
		"$push": bson.M{"history": rollbackHistory("rollback_failed", telegramID, rollbackStatusFailed, message)},
	}
	return s.findOneAndUpdate(bson.M{"_id": id}, update)
}

func (s *mongoRollbackStore) IncrementDownload(id string, telegramID string) (RollbackRecord, string, error) {
	now := time.Now()
	update := bson.M{
		"$inc": bson.M{"download_click_count": 1},
		"$set": bson.M{
			"last_clicked_at": now,
			"last_clicked_by": telegramID,
			"last_action":     rollbackActionDownload,
			"updated_at":      now,
		},
		"$push": bson.M{"history": rollbackHistory("yaml_download_clicked", telegramID, "", "rollback yaml download requested")},
	}
	return s.findOneAndUpdate(bson.M{
		"_id":        id,
		"expires_at": bson.M{"$gt": time.Now()},
	}, update)
}

func (s *mongoRollbackStore) AddTelegramMessage(id string, msg telegramSentMessage, kind string) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	_, err := s.collection.UpdateByID(ctx, id, bson.M{
		"$push": bson.M{
			"telegram_messages": RollbackTelegramMessage{ChatID: msg.ChatID, MessageID: msg.MessageID, Kind: kind},
			"history": RollbackHistoryItem{
				At:        time.Now(),
				Action:    "telegram_message_added",
				By:        "system",
				ChatID:    msg.ChatID,
				MessageID: msg.MessageID,
			},
		},
		"$set": bson.M{"updated_at": time.Now()},
	})
	return err
}

func (s *mongoRollbackStore) CleanupExpired(now time.Time) error {
	return nil // MongoDB TTL index handles cleanup.
}

func (s *mongoRollbackStore) findOneAndUpdate(filter interface{}, update interface{}) (RollbackRecord, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	var record RollbackRecord
	err := s.collection.FindOneAndUpdate(
		ctx,
		filter,
		update,
		options.FindOneAndUpdate().SetReturnDocument(options.After),
	).Decode(&record)
	if err == mongo.ErrNoDocuments {
		return RollbackRecord{}, "", ErrRollbackNotFound
	}
	if err != nil {
		return RollbackRecord{}, "", err
	}
	return record, record.ManifestYAML, nil
}
