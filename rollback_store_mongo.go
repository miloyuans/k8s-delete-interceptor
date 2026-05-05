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
	"k8s.io/klog/v2"
)

type mongoRollbackStore struct {
	collection     *mongo.Collection
	timeout        time.Duration
	allowReapply   bool
	runningTimeout time.Duration
}

func newMongoRollbackStore(cfg MongoAuditConfig, timeout time.Duration, allowReapply bool, runningTimeout time.Duration) (*mongoRollbackStore, error) {
	coll, err := initRollbackMongoCollection(cfg, timeout)
	if err != nil {
		return nil, err
	}
	return &mongoRollbackStore{collection: coll, timeout: timeout, allowReapply: allowReapply, runningTimeout: runningTimeout}, nil
}

func (s *mongoRollbackStore) BackendName() string { return rollbackStorageTypeMongo }

func (s *mongoRollbackStore) SaveRecord(record RollbackRecord, manifestYAML string) error {
	if strings.TrimSpace(record.ID) == "" {
		return fmt.Errorf("rollback record id is empty")
	}
	if strings.TrimSpace(manifestYAML) == "" {
		return fmt.Errorf("rollback manifest is empty")
	}
	if strings.TrimSpace(record.ExecutionStatus) == "" {
		record.ExecutionStatus = rollbackStatusPending
	}
	record.ManifestYAML = manifestYAML
	if record.History == nil {
		record.History = []RollbackHistoryItem{}
	}
	record.History = appendRollbackHistory(record.History, defaultRollbackHistory(rollbackActionCreated, "system", record.ExecutionStatus, "rollback backup recorded"))

	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	_, err := s.collection.UpdateByID(ctx, record.ID, bson.M{"$setOnInsert": record}, options.Update().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("failed to persist rollback record: %w", err)
	}
	return nil
}

func (s *mongoRollbackStore) LoadRecord(id string) (RollbackRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	var record RollbackRecord
	err := s.collection.FindOne(ctx, bson.M{"_id": id}).Decode(&record)
	if err == mongo.ErrNoDocuments {
		return RollbackRecord{}, errRollbackNotFound
	}
	if err != nil {
		return RollbackRecord{}, err
	}
	if strings.TrimSpace(record.ExecutionStatus) == "" {
		record.ExecutionStatus = rollbackStatusPending
	}
	return record, nil
}

func (s *mongoRollbackStore) LoadRecordWithManifest(id string) (rollbackLoadedRecord, error) {
	record, err := s.LoadRecord(id)
	if err != nil {
		return rollbackLoadedRecord{}, err
	}
	return rollbackLoadedRecord{Record: record, ManifestYAML: record.ManifestYAML}, nil
}

func (s *mongoRollbackStore) MarkRunning(id string, telegramID string) (RollbackRecord, error) {
	now := time.Now()
	filter := bson.M{
		"_id":        id,
		"expires_at": bson.M{"$gt": now},
	}
	cutoff := now.Add(-s.runningTimeout)
	if s.runningTimeout <= 0 {
		cutoff = now.Add(-5 * time.Minute)
	}
	if s.allowReapply {
		filter["$or"] = []bson.M{
			{"execution_status": bson.M{"$ne": rollbackStatusRunning}},
			{"execution_status": rollbackStatusRunning, "last_clicked_at": bson.M{"$lt": cutoff}},
		}
	} else {
		filter["$or"] = []bson.M{
			{"execution_status": bson.M{"$nin": []string{rollbackStatusRunning, rollbackStatusApplied, rollbackStatusExpired}}},
			{"execution_status": rollbackStatusRunning, "last_clicked_at": bson.M{"$lt": cutoff}},
		}
	}
	update := bson.M{
		"$inc": bson.M{"rollback_click_count": 1},
		"$set": bson.M{
			"last_clicked_at":  now,
			"last_clicked_by":  telegramID,
			"last_action":      rollbackActionApply,
			"execution_status": rollbackStatusRunning,
			"execution_error":  "",
		},
		"$push": bson.M{"history": defaultRollbackHistory(rollbackActionApply, telegramID, rollbackStatusRunning, "rollback apply clicked")},
	}
	return s.findOneAndUpdate(id, filter, update)
}

func (s *mongoRollbackStore) MarkApplied(id string, telegramID string) (RollbackRecord, error) {
	now := time.Now()
	update := bson.M{
		"$set": bson.M{
			"executed_at":      now,
			"executed_by":      telegramID,
			"execution_status": rollbackStatusApplied,
			"execution_error":  "",
			"last_clicked_at":  now,
			"last_clicked_by":  telegramID,
			"last_action":      rollbackActionApply,
		},
		"$push": bson.M{"history": defaultRollbackHistory(rollbackActionApply, telegramID, rollbackStatusApplied, "rollback applied")},
	}
	return s.findOneAndUpdate(id, bson.M{"_id": id}, update)
}

func (s *mongoRollbackStore) MarkFailed(id string, telegramID string, message string) (RollbackRecord, error) {
	now := time.Now()
	update := bson.M{
		"$set": bson.M{
			"executed_at":      now,
			"executed_by":      telegramID,
			"execution_status": rollbackStatusFailed,
			"execution_error":  message,
			"last_clicked_at":  now,
			"last_clicked_by":  telegramID,
			"last_action":      rollbackActionApply,
		},
		"$push": bson.M{"history": defaultRollbackHistory(rollbackActionApply, telegramID, rollbackStatusFailed, message)},
	}
	return s.findOneAndUpdate(id, bson.M{"_id": id}, update)
}

func (s *mongoRollbackStore) IncrementDownload(id string, telegramID string) (rollbackLoadedRecord, error) {
	now := time.Now()
	filter := bson.M{"_id": id, "expires_at": bson.M{"$gt": now}}
	update := bson.M{
		"$inc": bson.M{"download_click_count": 1},
		"$set": bson.M{
			"last_clicked_at": now,
			"last_clicked_by": telegramID,
			"last_action":     rollbackActionDownload,
		},
		"$push": bson.M{"history": defaultRollbackHistory(rollbackActionDownload, telegramID, "", "rollback yaml requested")},
	}
	record, err := s.findOneAndUpdate(id, filter, update)
	if err != nil {
		return rollbackLoadedRecord{}, err
	}
	return rollbackLoadedRecord{Record: record, ManifestYAML: record.ManifestYAML}, nil
}

func (s *mongoRollbackStore) AddTelegramMessage(id string, msg RollbackTelegramMessage) error {
	if msg.SentAt.IsZero() {
		msg.SentAt = time.Now()
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	_, err := s.collection.UpdateByID(ctx, id, bson.M{"$push": bson.M{"telegram_messages": msg}})
	return err
}

func (s *mongoRollbackStore) CleanupExpired(now time.Time) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	_, err := s.collection.UpdateMany(ctx, bson.M{
		"expires_at":       bson.M{"$lte": now},
		"execution_status": bson.M{"$ne": rollbackStatusExpired},
	}, bson.M{"$set": bson.M{"execution_status": rollbackStatusExpired}})
	return err
}

func (s *mongoRollbackStore) findOneAndUpdate(id string, filter bson.M, update bson.M) (RollbackRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
	var record RollbackRecord
	err := s.collection.FindOneAndUpdate(ctx, filter, update, opts).Decode(&record)
	if err == nil {
		if strings.TrimSpace(record.ExecutionStatus) == "" {
			record.ExecutionStatus = rollbackStatusPending
		}
		return record, nil
	}
	if err != mongo.ErrNoDocuments {
		return RollbackRecord{}, err
	}
	current, loadErr := s.LoadRecord(id)
	mapped := rollbackStoreErrorForNoMatch(id, current, loadErr)
	if mapped != nil && mapped != errRollbackNotFound {
		klog.V(4).Infof("Rollback update for %s rejected: %v", id, mapped)
	}
	return current, mapped
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
	if _, err := coll.Indexes().CreateOne(ctx, requestIndex); err != nil {
		klog.Errorf("Failed to create rollback request_uid index: %v", err)
	}

	return coll, nil
}
