package main

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const defaultAdmissionApprovalTTLSeconds = 300

type AdmissionApprovalGrant struct {
	ID                 string    `json:"id" bson:"id"`
	EventID            string    `json:"event_id" bson:"event_id"`
	RuleID             string    `json:"rule_id" bson:"rule_id"`
	RuleName           string    `json:"rule_name,omitempty" bson:"rule_name,omitempty"`
	Cluster            string    `json:"cluster" bson:"cluster"`
	Operation          string    `json:"operation" bson:"operation"`
	APIGroup           string    `json:"api_group" bson:"api_group"`
	Resource           string    `json:"resource" bson:"resource"`
	Kind               string    `json:"kind" bson:"kind"`
	Namespace          string    `json:"namespace" bson:"namespace"`
	Name               string    `json:"name" bson:"name"`
	User               string    `json:"user" bson:"user"`
	ApprovedBy         string    `json:"approved_by" bson:"approved_by"`
	ApprovedByID       string    `json:"approved_by_id,omitempty" bson:"approved_by_id,omitempty"`
	ApprovedAt         time.Time `json:"approved_at" bson:"approved_at"`
	ExpiresAt          time.Time `json:"expires_at" bson:"expires_at"`
	Consumed           bool      `json:"consumed" bson:"consumed"`
	ConsumedAt         time.Time `json:"consumed_at,omitempty" bson:"consumed_at,omitempty"`
	ConsumedRequestUID string    `json:"consumed_request_uid,omitempty" bson:"consumed_request_uid,omitempty"`
}

func admissionApprovalKey(cluster, operation, apiGroup, resource, namespace, name, user, ruleID string) string {
	parts := []string{
		strings.TrimSpace(cluster),
		strings.ToUpper(strings.TrimSpace(operation)),
		strings.TrimSpace(apiGroup),
		strings.ToLower(strings.TrimSpace(resource)),
		strings.TrimSpace(namespace),
		strings.TrimSpace(name),
		strings.TrimSpace(user),
		strings.TrimSpace(ruleID),
	}
	h := sha1.Sum([]byte(strings.Join(parts, "|")))
	return "ap_" + hex.EncodeToString(h[:])
}

func admissionApprovalKeyForContext(cfg *RuntimeConfig, ac AdmissionContext, pd PolicyDecision) string {
	cluster := ""
	if cfg != nil {
		cluster = cfg.ClusterName
	}
	ruleID := ""
	if pd.Rule != nil {
		ruleID = pd.Rule.ID
	}
	return admissionApprovalKey(cluster, ac.Operation, ac.APIGroup, ac.Resource, ac.Namespace, ac.Name, ac.User, ruleID)
}

func admissionApprovalKeyForEvent(ev *AdmissionEvent) string {
	if ev == nil {
		return ""
	}
	return admissionApprovalKey(ev.Cluster, ev.Operation, ev.APIGroup, ev.Resource, ev.Namespace, ev.Name, ev.User, ev.RuleID)
}

func approvalTTLSecondsForRule(rule *PolicyRule) int {
	if rule != nil && rule.Approval.TTLSeconds > 0 {
		return rule.Approval.TTLSeconds
	}
	return defaultAdmissionApprovalTTLSeconds
}

func findRuleByID(cfg *RuntimeConfig, id string) *PolicyRule {
	if cfg == nil || strings.TrimSpace(id) == "" {
		return nil
	}
	for i := range cfg.Rules {
		if cfg.Rules[i].ID == id {
			return &cfg.Rules[i]
		}
	}
	return nil
}

func (a *App) consumeAdmissionApproval(ctx context.Context, cfg *RuntimeConfig, ac AdmissionContext, pd PolicyDecision) (*AdmissionApprovalGrant, error) {
	key := admissionApprovalKeyForContext(cfg, ac, pd)
	if a.mongo != nil && a.mongo.Healthy() {
		if grant, err := a.mongo.ConsumeAdmissionApprovalGrant(ctx, key, ac.RequestUID); err == nil && grant != nil {
			return grant, nil
		}
	}
	if a.local == nil {
		return nil, nil
	}
	grant, err := a.local.ConsumeAdmissionApprovalGrant(key, ac.RequestUID)
	if err != nil {
		return nil, err
	}
	return grant, nil
}

func (a *App) createAdmissionApprovalGrant(ctx context.Context, ev *AdmissionEvent, approvedBy string, approvedByID string) (*AdmissionApprovalGrant, error) {
	if ev == nil {
		return nil, errors.New("event is nil")
	}
	rule := findRuleByID(a.Config(), ev.RuleID)
	ttl := approvalTTLSecondsForRule(rule)
	now := time.Now().UTC()
	grant := &AdmissionApprovalGrant{
		ID:           admissionApprovalKeyForEvent(ev),
		EventID:      ev.ID,
		RuleID:       ev.RuleID,
		RuleName:     ev.RuleName,
		Cluster:      ev.Cluster,
		Operation:    ev.Operation,
		APIGroup:     ev.APIGroup,
		Resource:     ev.Resource,
		Kind:         ev.Kind,
		Namespace:    ev.Namespace,
		Name:         ev.Name,
		User:         ev.User,
		ApprovedBy:   approvedBy,
		ApprovedByID: approvedByID,
		ApprovedAt:   now,
		ExpiresAt:    now.Add(time.Duration(ttl) * time.Second),
	}
	if grant.ID == "" || grant.ID == "ap_da39a3ee5e6b4b0d3255bfef95601890afd80709" {
		return nil, errors.New("approval grant key is empty")
	}
	saved := false
	if a.local != nil {
		if err := a.local.SaveAdmissionApprovalGrant(grant); err != nil {
			return nil, err
		}
		saved = true
	}
	if a.mongo != nil && a.mongo.Healthy() {
		if err := a.mongo.SaveAdmissionApprovalGrant(ctx, grant); err != nil && !saved {
			return nil, err
		}
	}
	return grant, nil
}

func (s *LocalStore) SaveAdmissionApprovalGrant(grant *AdmissionApprovalGrant) error {
	if s == nil || grant == nil || strings.TrimSpace(grant.ID) == "" {
		return errors.New("approval grant is invalid")
	}
	if grant.ApprovedAt.IsZero() {
		grant.ApprovedAt = time.Now().UTC()
	}
	if grant.ExpiresAt.IsZero() {
		grant.ExpiresAt = grant.ApprovedAt.Add(defaultAdmissionApprovalTTLSeconds * time.Second)
	}
	b, err := json.MarshalIndent(grant, "", "  ")
	if err != nil {
		return err
	}
	name := safeFileName(grant.ID) + ".json"
	tmp := filepath.Join(s.root, "tmp", name+fmt.Sprintf(".%d.approval.tmp", time.Now().UnixNano()))
	pending := filepath.Join(s.root, "approvals/pending", name)
	if err := os.WriteFile(tmp, b, 0600); err != nil {
		return err
	}
	if err := fsyncFile(tmp); err != nil {
		return err
	}
	return os.Rename(tmp, pending)
}

func (s *LocalStore) ConsumeAdmissionApprovalGrant(id, requestUID string) (*AdmissionApprovalGrant, error) {
	if s == nil || strings.TrimSpace(id) == "" {
		return nil, nil
	}
	name := safeFileName(id) + ".json"
	pending := filepath.Join(s.root, "approvals/pending", name)
	b, err := os.ReadFile(pending)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var grant AdmissionApprovalGrant
	if err := json.Unmarshal(b, &grant); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	if grant.Consumed || (!grant.ExpiresAt.IsZero() && now.After(grant.ExpiresAt)) {
		_ = os.Remove(pending)
		return nil, nil
	}
	grant.Consumed = true
	grant.ConsumedAt = now
	grant.ConsumedRequestUID = requestUID
	payload, _ := json.MarshalIndent(&grant, "", "  ")
	decided := filepath.Join(s.root, "approvals/decided", name)
	tmp := filepath.Join(s.root, "tmp", name+fmt.Sprintf(".%d.consumed.tmp", time.Now().UnixNano()))
	if err := os.WriteFile(tmp, payload, 0600); err != nil {
		return nil, err
	}
	if err := fsyncFile(tmp); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, decided); err != nil {
		return nil, err
	}
	_ = os.Remove(pending)
	return &grant, nil
}

func (m *MongoStore) SaveAdmissionApprovalGrant(ctx context.Context, grant *AdmissionApprovalGrant) error {
	if m == nil || grant == nil {
		return errors.New("mongo unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := m.db.Collection("admission_approval_grants").UpdateOne(ctx, bson.M{"id": grant.ID}, bson.M{"$setOnInsert": grant}, options.Update().SetUpsert(true))
	m.healthy.Store(err == nil)
	return err
}

func (m *MongoStore) ConsumeAdmissionApprovalGrant(ctx context.Context, id, requestUID string) (*AdmissionApprovalGrant, error) {
	if m == nil || strings.TrimSpace(id) == "" {
		return nil, errors.New("mongo unavailable")
	}
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	filter := bson.M{"id": id, "consumed": bson.M{"$ne": true}, "expires_at": bson.M{"$gt": now}}
	update := bson.M{"$set": bson.M{"consumed": true, "consumed_at": now, "consumed_request_uid": requestUID}}
	var grant AdmissionApprovalGrant
	err := m.db.Collection("admission_approval_grants").FindOneAndUpdate(ctx, filter, update, options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&grant)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		m.healthy.Store(false)
		return nil, err
	}
	m.healthy.Store(true)
	return &grant, nil
}

func telegramCallbackUserID(cb *telegramCallbackQuery) string {
	if cb == nil || cb.From.ID == 0 {
		return ""
	}
	return strconv.FormatInt(cb.From.ID, 10)
}

func telegramCallbackUsername(cb *telegramCallbackQuery) string {
	if cb == nil {
		return ""
	}
	return strings.TrimPrefix(strings.TrimSpace(cb.From.Username), "@")
}

func telegramUserMatchesCallback(u TelegramUser, cb *telegramCallbackQuery) bool {
	if !u.Enabled || cb == nil {
		return false
	}
	telegramID := telegramCallbackUserID(cb)
	username := strings.ToLower(telegramCallbackUsername(cb))
	if telegramID != "" && strings.TrimSpace(u.TelegramID) == telegramID {
		return true
	}
	if username != "" && strings.ToLower(strings.TrimPrefix(strings.TrimSpace(u.Username), "@")) == username {
		return true
	}
	if telegramID != "" && strings.TrimSpace(u.ID) == telegramID {
		return true
	}
	if username != "" && strings.ToLower(strings.TrimPrefix(strings.TrimSpace(u.ID), "@")) == username {
		return true
	}
	return false
}

func findTelegramActorUser(tg *TelegramConfig, cb *telegramCallbackQuery) *TelegramUser {
	if tg == nil || cb == nil {
		return nil
	}
	for i := range tg.Users {
		if telegramUserMatchesCallback(tg.Users[i], cb) {
			return &tg.Users[i]
		}
	}
	return nil
}

func telegramUserHasAnyRole(u *TelegramUser, roles ...string) bool {
	if u == nil || !u.Enabled {
		return false
	}
	wanted := map[string]bool{}
	for _, r := range roles {
		wanted[strings.ToLower(strings.TrimSpace(r))] = true
	}
	for _, r := range u.Roles {
		x := strings.ToLower(strings.TrimSpace(r))
		if x == "*" || wanted[x] {
			return true
		}
	}
	return false
}

func telegramUserListed(u *TelegramUser, allow []string, cb *telegramCallbackQuery) bool {
	if len(allow) == 0 {
		return false
	}
	telegramID := telegramCallbackUserID(cb)
	username := strings.ToLower(telegramCallbackUsername(cb))
	for _, raw := range allow {
		x := strings.TrimSpace(raw)
		if x == "" {
			continue
		}
		if u != nil && (x == u.ID || x == u.TelegramID || strings.EqualFold(strings.TrimPrefix(x, "@"), strings.TrimPrefix(u.Username, "@"))) {
			return true
		}
		if telegramID != "" && x == telegramID {
			return true
		}
		if username != "" && strings.EqualFold(strings.TrimPrefix(x, "@"), username) {
			return true
		}
	}
	return false
}

func telegramActionLegacyAllowed(tg *TelegramConfig) bool {
	if strings.EqualFold(os.Getenv("TELEGRAM_CALLBACK_REQUIRE_CONFIGURED_USER"), "true") {
		return false
	}
	if tg == nil {
		return false
	}
	for _, u := range tg.Users {
		if u.Enabled {
			return false
		}
	}
	return true
}

func telegramCanApproveConfigChange(tg *TelegramConfig, cb *telegramCallbackQuery) bool {
	if telegramActionLegacyAllowed(tg) {
		return true
	}
	u := findTelegramActorUser(tg, cb)
	return telegramUserHasAnyRole(u, "superadmin", "telegram_approver", "config_approver", "rule_manager")
}

func telegramCanApproveAdmissionEvent(tg *TelegramConfig, cb *telegramCallbackQuery, rule *PolicyRule) bool {
	u := findTelegramActorUser(tg, cb)
	if rule != nil && telegramUserListed(u, rule.Approval.ApproverTelegramUsers, cb) {
		return true
	}
	if telegramUserHasAnyRole(u, "superadmin", "telegram_approver", "config_approver", "operator") {
		return true
	}
	return telegramActionLegacyAllowed(tg)
}

func telegramCanExecuteRollback(tg *TelegramConfig, cb *telegramCallbackQuery) bool {
	if telegramActionLegacyAllowed(tg) {
		return true
	}
	u := findTelegramActorUser(tg, cb)
	return telegramUserHasAnyRole(u, "superadmin", "operator", "rollback_operator")
}
