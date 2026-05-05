package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/admission/v1"
	"k8s.io/klog/v2"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	defaultAuditDirectory       = "/var/log/k8s-delete-interceptor"
	defaultFileRetentionDays    = 7
	defaultMongoRetentionDays   = 30
	defaultMongoDatabase        = "k8s_delete_interceptor"
	defaultMongoCollection      = "admission_audit"
	defaultMongoConnectTimeout  = 5
	defaultAuditQueueSize       = 1024
	auditDecisionAllowed        = "allowed"
	auditDecisionBlocked        = "blocked"
	auditPolicyCreateAudit      = "create_audit"
	auditPolicyUpdateAudit      = "update_audit"
	auditPolicyDeleteAudit      = "delete_audit"
	auditPolicyGlobalWhitelist  = "global_whitelist"
	auditPolicyInterceptorOff   = "interceptor_disabled"
	auditPolicySelfPreservation = "self_preservation"
	auditSourceWebhook          = "admission-webhook"
	auditEventTypeCreate        = "create"
	auditEventTypeUpdate        = "update"
	auditEventTypeDelete        = "delete"
	auditEventTypeLifecycle     = "lifecycle"
)

var defaultAuditNotifyResources = []string{
	"deployment",
	"statefulset",
	"daemonset",
	"pod",
	"pvc",
	"pv",
	"service",
	"svc",
	"configmap",
	"secret",
	"ingress",
	"namespace",
	"serviceaccount",
	"sa",
	"role",
	"clusterrole",
	"rolebinding",
	"clusterrolebinding",
	"storageclass",
}

type AuditConfig struct {
	Enabled                        bool                 `json:"enabled" yaml:"enabled"`
	Directory                      string               `json:"directory" yaml:"directory"`
	FileRetentionDays              int                  `json:"file_retention_days" yaml:"file_retention_days"`
	ChangeDetailAuditOnlyResources []string             `json:"change_detail_audit_only_resources" yaml:"change_detail_audit_only_resources"`
	Create                         AuditOperationConfig `json:"create" yaml:"create"`
	Update                         AuditOperationConfig `json:"update" yaml:"update"`
	Telegram                       AuditTelegramConfig  `json:"telegram" yaml:"telegram"`
	Mongo                          MongoAuditConfig     `json:"mongo" yaml:"mongo"`
}

type AuditOperationConfig struct {
	Enabled         bool     `json:"enabled" yaml:"enabled"`
	NotifyUsers     []string `json:"notify_users" yaml:"notify_users"`
	NotifyResources []string `json:"notify_resources" yaml:"notify_resources"`
}

type MongoAuditConfig struct {
	Enabled               bool   `json:"enabled" yaml:"enabled"`
	URI                   string `json:"uri" yaml:"uri"`
	Database              string `json:"database" yaml:"database"`
	Collection            string `json:"collection" yaml:"collection"`
	RetentionDays         int    `json:"retention_days" yaml:"retention_days"`
	ConnectTimeoutSeconds int    `json:"connect_timeout_seconds" yaml:"connect_timeout_seconds"`
}

type AuditRecord struct {
	Timestamp          time.Time       `json:"timestamp" bson:"timestamp"`
	EventType          string          `json:"event_type" bson:"event_type"`
	ClusterName        string          `json:"cluster_name" bson:"cluster_name"`
	RequestUID         string          `json:"request_uid" bson:"request_uid"`
	Source             string          `json:"source" bson:"source"`
	Username           string          `json:"username" bson:"username"`
	UserGroups         []string        `json:"user_groups,omitempty" bson:"user_groups,omitempty"`
	IsServiceAccount   bool            `json:"is_service_account" bson:"is_service_account"`
	Operation          string          `json:"operation" bson:"operation"`
	Kind               string          `json:"kind" bson:"kind"`
	Resource           string          `json:"resource" bson:"resource"`
	ResourceGroup      string          `json:"resource_group,omitempty" bson:"resource_group,omitempty"`
	ResourceVersion    string          `json:"resource_version,omitempty" bson:"resource_version,omitempty"`
	SubResource        string          `json:"sub_resource,omitempty" bson:"sub_resource,omitempty"`
	Name               string          `json:"name,omitempty" bson:"name,omitempty"`
	Namespace          string          `json:"namespace,omitempty" bson:"namespace,omitempty"`
	ResourceDisplay    string          `json:"resource_display" bson:"resource_display"`
	Decision           string          `json:"decision" bson:"decision"`
	DecisionLabel      string          `json:"decision_label" bson:"decision_label"`
	Reason             string          `json:"reason" bson:"reason"`
	MatchedPolicy      string          `json:"matched_policy,omitempty" bson:"matched_policy,omitempty"`
	Notified           bool            `json:"notified" bson:"notified"`
	NotificationReason string          `json:"notification_reason,omitempty" bson:"notification_reason,omitempty"`
	ChangeDetails      string          `json:"change_details,omitempty" bson:"change_details,omitempty"`
	DryRun             bool            `json:"dry_run" bson:"dry_run"`
	Object             json.RawMessage `json:"object,omitempty" bson:"object,omitempty"`
	OldObject          json.RawMessage `json:"old_object,omitempty" bson:"old_object,omitempty"`
}

type auditManager struct {
	enabled         bool
	directory       string
	fileRetention   int
	queue           chan AuditRecord
	lastCleanupDate string
	cleanupMu       sync.Mutex
	mongoCollection *mongo.Collection
	mongoEnabled    bool
	mongoTimeout    time.Duration
}

var admissionAuditor *auditManager

func newAuditManager(cfg AuditConfig) (*auditManager, error) {
	if !cfg.Enabled {
		return &auditManager{enabled: false}, nil
	}

	directory := strings.TrimSpace(cfg.Directory)
	if directory == "" {
		directory = defaultAuditDirectory
	}

	fileRetentionDays := cfg.FileRetentionDays
	if fileRetentionDays <= 0 {
		fileRetentionDays = defaultFileRetentionDays
	}

	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create audit directory '%s': %w", directory, err)
	}

	manager := &auditManager{
		enabled:       true,
		directory:     directory,
		fileRetention: fileRetentionDays,
		queue:         make(chan AuditRecord, defaultAuditQueueSize),
	}

	if err := manager.initMongo(cfg.Mongo); err != nil {
		klog.Errorf("MongoDB audit sink disabled: %v", err)
	}

	go manager.run()
	return manager, nil
}

func (a *auditManager) initMongo(cfg MongoAuditConfig) error {
	if !cfg.Enabled {
		return nil
	}

	uri := strings.TrimSpace(cfg.URI)
	if uri == "" {
		uri = strings.TrimSpace(os.Getenv("MONGODB_URI"))
	}
	if uri == "" {
		return fmt.Errorf("mongo.enabled=true but no uri configured")
	}

	database := strings.TrimSpace(cfg.Database)
	if database == "" {
		database = defaultMongoDatabase
	}

	collection := strings.TrimSpace(cfg.Collection)
	if collection == "" {
		collection = defaultMongoCollection
	}

	retentionDays := cfg.RetentionDays
	if retentionDays <= 0 {
		retentionDays = defaultMongoRetentionDays
	}

	connectTimeoutSeconds := cfg.ConnectTimeoutSeconds
	if connectTimeoutSeconds <= 0 {
		connectTimeoutSeconds = defaultMongoConnectTimeout
	}

	timeout := time.Duration(connectTimeoutSeconds) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return fmt.Errorf("failed to connect to mongodb: %w", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("failed to ping mongodb: %w", err)
	}

	coll := client.Database(database).Collection(collection)
	expireAfterSeconds := int32(retentionDays * 24 * 60 * 60)
	index := mongo.IndexModel{
		Keys: bson.D{{Key: "timestamp", Value: 1}},
		Options: options.Index().
			SetName("audit_timestamp_ttl").
			SetExpireAfterSeconds(expireAfterSeconds),
	}

	if _, err := coll.Indexes().CreateOne(ctx, index); err != nil {
		return fmt.Errorf("failed to create mongodb ttl index: %w", err)
	}

	a.mongoCollection = coll
	a.mongoEnabled = true
	a.mongoTimeout = timeout
	return nil
}

func (a *auditManager) run() {
	for record := range a.queue {
		if err := a.writeFile(record); err != nil {
			klog.Errorf("Failed to write audit record to file: %v", err)
		}

		if a.mongoEnabled {
			if err := a.writeMongo(record); err != nil {
				klog.Errorf("Failed to write audit record to MongoDB: %v", err)
			}
		}
	}
}

func (a *auditManager) enqueue(record AuditRecord) {
	if a == nil || !a.enabled {
		return
	}

	select {
	case a.queue <- record:
	default:
		klog.Errorf("Audit queue is full. Dropping record for request %s", record.RequestUID)
	}
}

func (a *auditManager) writeFile(record AuditRecord) error {
	if err := a.cleanupOldFiles(record.Timestamp); err != nil {
		return err
	}

	filePath := filepath.Join(a.directory, fmt.Sprintf("audit-%s-%s.jsonl", record.Timestamp.Format("2006-01-02"), auditRecordFileKey(record)))
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal audit record: %w", err)
	}

	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("failed to open audit file '%s': %w", filePath, err)
	}
	defer f.Close()

	if _, err := f.Write(append(payload, '\n')); err != nil {
		return fmt.Errorf("failed to append audit file '%s': %w", filePath, err)
	}

	return nil
}

func (a *auditManager) cleanupOldFiles(now time.Time) error {
	if a.fileRetention <= 0 {
		return nil
	}

	currentDate := now.Format("2006-01-02")

	a.cleanupMu.Lock()
	defer a.cleanupMu.Unlock()

	if a.lastCleanupDate == currentDate {
		return nil
	}

	entries, err := os.ReadDir(a.directory)
	if err != nil {
		return fmt.Errorf("failed to read audit directory '%s': %w", a.directory, err)
	}

	cutoff := now.AddDate(0, 0, -(a.fileRetention - 1))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasPrefix(name, "audit-") || !strings.HasSuffix(name, ".jsonl") {
			continue
		}

		middle := strings.TrimSuffix(strings.TrimPrefix(name, "audit-"), ".jsonl")
		if len(middle) < len("2006-01-02") {
			klog.Warningf("Skipping audit retention cleanup for unexpected file name '%s': date segment missing", name)
			continue
		}

		datePart := middle[:len("2006-01-02")]
		fileDate, err := time.Parse("2006-01-02", datePart)
		if err != nil {
			klog.Warningf("Skipping audit retention cleanup for unexpected file name '%s': %v", name, err)
			continue
		}

		if fileDate.Before(cutoff) {
			if err := os.Remove(filepath.Join(a.directory, name)); err != nil {
				klog.Errorf("Failed to delete expired audit file '%s': %v", name, err)
			}
		}
	}

	a.lastCleanupDate = currentDate
	return nil
}

func (a *auditManager) writeMongo(record AuditRecord) error {
	ctx, cancel := context.WithTimeout(context.Background(), a.mongoTimeout)
	defer cancel()

	_, err := a.mongoCollection.InsertOne(ctx, record)
	if err != nil {
		return fmt.Errorf("insert mongodb audit record failed: %w", err)
	}

	return nil
}

func isServiceAccountUser(user string) bool {
	return strings.HasPrefix(user, "system:serviceaccount:")
}

func isAuditOperationEnabled(operation v1.Operation) bool {
	if !config.Audit.Enabled {
		return false
	}

	switch operation {
	case v1.Create:
		return config.Audit.Create.Enabled
	case v1.Update:
		return config.Audit.Update.Enabled
	case v1.Delete:
		return true
	default:
		return false
	}
}

func getAuditOperationConfig(operation v1.Operation) AuditOperationConfig {
	switch operation {
	case v1.Create:
		return config.Audit.Create
	case v1.Update:
		return config.Audit.Update
	default:
		return AuditOperationConfig{}
	}
}

func uniqueLowerValues(values ...string) []string {
	seen := make(map[string]struct{})
	result := make([]string, 0, len(values))

	for _, value := range values {
		normalized := strings.ToLower(strings.TrimSpace(value))
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		result = append(result, normalized)
	}

	return result
}

func auditResourceCandidates(kind string, resource string) []string {
	candidates := uniqueLowerValues(kind, resource)

	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "deployment":
		candidates = append(candidates, "deploy")
	case "statefulset":
		candidates = append(candidates, "sts")
	case "daemonset":
		candidates = append(candidates, "ds")
	case "persistentvolumeclaim":
		candidates = append(candidates, "pvc")
	case "persistentvolume":
		candidates = append(candidates, "pv")
	case "service":
		candidates = append(candidates, "svc")
	case "configmap":
		candidates = append(candidates, "cm")
	case "ingress":
		candidates = append(candidates, "ing")
	case "namespace":
		candidates = append(candidates, "ns")
	case "serviceaccount":
		candidates = append(candidates, "sa")
	case "rolebinding":
		candidates = append(candidates, "rb")
	case "clusterrolebinding":
		candidates = append(candidates, "crb")
	}

	return uniqueLowerValues(candidates...)
}

func resolveAuditNotifyResources(operation v1.Operation) []string {
	patterns := getAuditOperationConfig(operation).NotifyResources
	if len(patterns) == 0 {
		return defaultAuditNotifyResources
	}
	return patterns
}

func matchAuditNotifyUser(operation v1.Operation, user string) (bool, string, string) {
	for _, pattern := range getAuditOperationConfig(operation).NotifyUsers {
		matched, matcher, err := matchPattern(pattern, user)
		if err != nil {
			klog.Errorf("Invalid audit notify pattern '%s' for operation '%s': %v. This pattern will be skipped.", pattern, operation, err)
			continue
		}

		if matched {
			return true, pattern, matcher
		}
	}

	return false, "", ""
}

func matchAuditNotifyResource(operation v1.Operation, kind string, resource string) (bool, string, string, string) {
	patterns := resolveAuditNotifyResources(operation)
	candidates := auditResourceCandidates(kind, resource)

	for _, pattern := range patterns {
		for _, candidate := range candidates {
			matched, matcher, err := matchPattern(pattern, candidate)
			if err != nil {
				klog.Errorf("Invalid audit notify resource pattern '%s' for operation '%s': %v. This pattern will be skipped.", pattern, operation, err)
				break
			}

			if matched {
				return true, pattern, matcher, candidate
			}
		}
	}

	return false, "", "", ""
}

func shouldNotifyMutationAudit(req *v1.AdmissionRequest) (bool, string, string, string, string, string) {
	if !config.Audit.Enabled || !isServiceAccountUser(req.UserInfo.Username) {
		return false, "", "", "", "", ""
	}

	userMatched, userPattern, userMatcher := matchAuditNotifyUser(req.Operation, req.UserInfo.Username)
	if !userMatched {
		return false, "", "", "", "", ""
	}

	resourceMatched, resourcePattern, resourceMatcher, resourceCandidate := matchAuditNotifyResource(req.Operation, req.Kind.Kind, req.Resource.Resource)
	if !resourceMatched {
		return false, userPattern, userMatcher, "", "", ""
	}

	return true, userPattern, userMatcher, resourcePattern, resourceMatcher, resourceCandidate
}

func cloneRawMessage(raw []byte) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}

	cloned := make([]byte, len(raw))
	copy(cloned, raw)
	return json.RawMessage(cloned)
}

func auditEventTypeForOperation(operation v1.Operation) string {
	switch operation {
	case v1.Create:
		return auditEventTypeCreate
	case v1.Update:
		return auditEventTypeUpdate
	case v1.Delete:
		return auditEventTypeDelete
	default:
		return strings.ToLower(string(operation))
	}
}

func auditRecordFileKey(record AuditRecord) string {
	key := strings.TrimSpace(strings.ToLower(record.EventType))
	if key != "" {
		return key
	}
	if strings.TrimSpace(record.Operation) != "" {
		return strings.ToLower(record.Operation)
	}
	return "general"
}

func buildAuditRecord(req *v1.AdmissionRequest, decision string, reason string, matchedPolicy string, notified bool, notificationReason string) AuditRecord {
	return buildAuditRecordWithChangeDetails(req, decision, reason, matchedPolicy, notified, notificationReason, "")
}

func buildAuditRecordWithChangeDetails(req *v1.AdmissionRequest, decision string, reason string, matchedPolicy string, notified bool, notificationReason string, changeDetails string) AuditRecord {
	dryRun := false
	if req.DryRun != nil {
		dryRun = *req.DryRun
	}

	return AuditRecord{
		Timestamp:          time.Now(),
		EventType:          auditEventTypeForOperation(req.Operation),
		ClusterName:        config.ClusterName,
		RequestUID:         string(req.UID),
		Source:             auditSourceWebhook,
		Username:           req.UserInfo.Username,
		UserGroups:         req.UserInfo.Groups,
		IsServiceAccount:   isServiceAccountUser(req.UserInfo.Username),
		Operation:          string(req.Operation),
		Kind:               req.Kind.Kind,
		Resource:           req.Resource.Resource,
		ResourceGroup:      req.Resource.Group,
		ResourceVersion:    req.Resource.Version,
		SubResource:        req.SubResource,
		Name:               req.Name,
		Namespace:          req.Namespace,
		ResourceDisplay:    formatResource(req.Kind.Kind, req.Name, req.Namespace),
		Decision:           decision,
		DecisionLabel:      notificationActionLabel(decision),
		Reason:             reason,
		MatchedPolicy:      matchedPolicy,
		Notified:           notified,
		NotificationReason: notificationReason,
		ChangeDetails:      changeDetails,
		DryRun:             dryRun,
		Object:             cloneRawMessage(req.Object.Raw),
		OldObject:          cloneRawMessage(req.OldObject.Raw),
	}
}

func buildLifecycleAuditRecord(event string, reason string, notified bool, notificationReason string) AuditRecord {
	decision := auditDecisionAllowed
	decisionLabel := notificationActionLabel(decision)
	if event == lifecycleEventUnexpectedStop {
		decision = auditDecisionBlocked
		decisionLabel = notificationActionLabel(decision)
	}

	return AuditRecord{
		Timestamp:          time.Now(),
		EventType:          auditEventTypeLifecycle,
		ClusterName:        config.ClusterName,
		RequestUID:         "",
		Source:             "service-lifecycle",
		Username:           "system",
		UserGroups:         nil,
		IsServiceAccount:   false,
		Operation:          lifecycleOperationType,
		Kind:               lifecycleComponentKind,
		Resource:           strings.ToLower(lifecycleComponentKind),
		ResourceGroup:      "",
		ResourceVersion:    "",
		SubResource:        "",
		Name:               lifecycleComponentName,
		Namespace:          webhookNs,
		ResourceDisplay:    formatResource(lifecycleComponentKind, lifecycleComponentName, webhookNs),
		Decision:           decision,
		DecisionLabel:      decisionLabel,
		Reason:             reason,
		MatchedPolicy:      event,
		Notified:           notified,
		NotificationReason: notificationReason,
		DryRun:             false,
		Object:             nil,
		OldObject:          nil,
	}
}

func emitAuditRecord(req *v1.AdmissionRequest, decision string, reason string, matchedPolicy string, notified bool, notificationReason string) {
	emitAuditRecordWithChangeDetails(req, decision, reason, matchedPolicy, notified, notificationReason, "")
}

func emitAuditRecordWithChangeDetails(req *v1.AdmissionRequest, decision string, reason string, matchedPolicy string, notified bool, notificationReason string, changeDetails string) {
	if admissionAuditor == nil || !isAuditOperationEnabled(req.Operation) {
		return
	}

	admissionAuditor.enqueue(buildAuditRecordWithChangeDetails(req, decision, reason, matchedPolicy, notified, notificationReason, changeDetails))
}

func emitLifecycleAuditRecord(event string, reason string, notified bool, notificationReason string) {
	if admissionAuditor == nil || !admissionAuditor.enabled {
		return
	}

	admissionAuditor.enqueue(buildLifecycleAuditRecord(event, reason, notified, notificationReason))
}

func handleCreateOrUpdateAudit(req *v1.AdmissionRequest) {
	if matched, pattern, matcher := matchGlobalWhitelist(req.UserInfo.Username); matched {
		resourceDesc := formatResource(req.Kind.Kind, req.Name, req.Namespace)
		reason := fmt.Sprintf("%s request bypassed all controls for %s: user '%s' matched global whitelist pattern '%s' (%s). Audit recorded only.", strings.ToUpper(string(req.Operation)), resourceDesc, req.UserInfo.Username, pattern, matcher)
		emitAuditRecord(req, auditDecisionAllowed, reason, fmt.Sprintf("%s:%s", auditPolicyGlobalWhitelist, pattern), false, "")
		return
	}

	if !isAuditOperationEnabled(req.Operation) {
		return
	}

	operationLabel := strings.ToUpper(string(req.Operation))
	resourceDesc := formatResource(req.Kind.Kind, req.Name, req.Namespace)
	reason := fmt.Sprintf("%s request allowed and audit recorded for %s.", operationLabel, resourceDesc)
	matchedPolicy := auditPolicyCreateAudit
	if req.Operation == v1.Update {
		matchedPolicy = auditPolicyUpdateAudit
	}

	notified := false
	notificationReason := ""

	if shouldNotify, userPattern, userMatcher, resourcePattern, resourceMatcher, resourceCandidate := shouldNotifyMutationAudit(req); shouldNotify {
		notificationReason = fmt.Sprintf("服务账号命中审计通知规则，重要资源变更已放行并记录。用户规则: '%s' (%s)，资源规则: '%s' (%s, candidate '%s')。", userPattern, userMatcher, resourcePattern, resourceMatcher, resourceCandidate)
		sendAuditTelegramNotification(buildSmartNotificationContext(
			req.UID,
			req.UserInfo.Username,
			req.Kind.Kind,
			req.Name,
			req.Namespace,
			string(req.Operation),
			formatOperation(string(req.Operation), req.Kind.Kind, req.Name, req.Namespace),
			auditDecisionAllowed,
			notificationReason,
		))
		notified = true
		matchedPolicy = fmt.Sprintf("%s_notify:%s|%s", strings.ToLower(string(req.Operation)), userPattern, resourcePattern)
	}

	emitAuditRecord(req, auditDecisionAllowed, reason, matchedPolicy, notified, notificationReason)
}
