package main

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (a *App) handleValidate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 20<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var review admissionv1.AdmissionReview
	if err := json.Unmarshal(body, &review); err != nil {
		http.Error(w, "invalid AdmissionReview: "+err.Error(), http.StatusBadRequest)
		return
	}
	if review.Request == nil {
		http.Error(w, "empty AdmissionReview request", http.StatusBadRequest)
		return
	}
	resp := a.admit(r.Context(), review.Request)
	outReview := admissionv1.AdmissionReview{TypeMeta: metav1.TypeMeta{APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview"}, Response: resp}
	outReview.Response.UID = review.Request.UID
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(outReview)
}

func (a *App) admit(ctx context.Context, req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	cfg := a.Config()
	if cfg == nil {
		return &admissionv1.AdmissionResponse{Allowed: false, Result: &metav1.Status{Message: "runtime config unavailable"}}
	}
	objMap := map[string]any{}
	oldMap := map[string]any{}
	if len(req.Object.Raw) > 0 {
		_ = json.Unmarshal(req.Object.Raw, &objMap)
	}
	if len(req.OldObject.Raw) > 0 {
		_ = json.Unmarshal(req.OldObject.Raw, &oldMap)
	}
	ac := AdmissionContext{
		Operation: string(req.Operation), APIGroup: req.Resource.Group, APIVersion: req.Resource.Version, Resource: req.Resource.Resource, SubResource: req.SubResource,
		Kind: req.Kind.Kind, Namespace: req.Namespace, Name: req.Name, User: req.UserInfo.Username, Groups: req.UserInfo.Groups,
		Object: objMap, OldObject: oldMap, ObjectRaw: req.Object.Raw, OldObjectRaw: req.OldObject.Raw, RequestUID: string(req.UID),
	}
	pd := decide(cfg, ac)
	log.Printf("admission policy decided: uid=%s op=%s group=%q resource=%s kind=%s ns=%s name=%s user=%s decision=%s allowed=%v rule=%s scopes=%v reason=%s", req.UID, ac.Operation, ac.APIGroup, ac.Resource, ac.Kind, ac.Namespace, ac.Name, ac.User, pd.Decision, pd.Allowed, ruleIDForLog(pd), pd.ScopeIDs, pd.Reason)
	approvalConsumed := false
	if pd.Decision == DecisionRequireApproval {
		if grant, err := a.consumeAdmissionApproval(ctx, cfg, ac, pd); err == nil && grant != nil {
			approvalConsumed = true
			pd.Decision = DecisionAllowNotify
			pd.Allowed = true
			pd.Reason = fmt.Sprintf("已审批放行，审批人: %s，原事件: %s", grant.ApprovedBy, grant.EventID)
		} else if err != nil {
			log.Printf("admission approval lookup failed: uid=%s key=%s err=%v", req.UID, admissionApprovalKeyForContext(cfg, ac, pd), err)
		}
	}
	ev := a.buildEvent(cfg, req, ac, pd)
	if approvalConsumed {
		ev.Reason = pd.Reason
	}
	ev.Fingerprint = admissionEventFingerprint(cfg, ac, pd)
	if dup, err := a.recentDuplicateEvent(ctx, cfg, ev.Fingerprint); err == nil && dup != nil {
		ev.Duplicate = true
		ev.DuplicateOf = dup.ID
		log.Printf("admission duplicate suppressed: uid=%s event=%s duplicate_of=%s fingerprint=%s", req.UID, ev.ID, dup.ID, ev.Fingerprint)
		return admissionResponseForDecision(pd)
	} else if err != nil {
		log.Printf("admission duplicate lookup failed: uid=%s fingerprint=%s err=%v", req.UID, ev.Fingerprint, err)
	}
	if shouldCreateRollback(cfg, pd, ac) {
		if rb, err := a.createRollbackBackup(ctx, cfg, ev, ac, pd); err == nil && rb != nil {
			ev.RollbackID = rb.ID
		}
	}
	a.recordEvent(ev)
	if shouldNotify(pd) {
		go a.notifyEvent(context.Background(), cfg, ev, pd)
	}
	return admissionResponseForDecision(pd)
}

func admissionResponseForDecision(pd PolicyDecision) *admissionv1.AdmissionResponse {
	if pd.Decision == DecisionRequireApproval {
		return &admissionv1.AdmissionResponse{Allowed: false, Result: &metav1.Status{Reason: metav1.StatusReasonForbidden, Message: "请求需要审批，已被拦截。请在 Web Console 或 Telegram 审批后重新执行。"}}
	}
	if !pd.Allowed {
		return &admissionv1.AdmissionResponse{Allowed: false, Result: &metav1.Status{Reason: metav1.StatusReasonForbidden, Message: pd.Reason}}
	}
	return &admissionv1.AdmissionResponse{Allowed: true}
}

func ruleIDForLog(pd PolicyDecision) string {
	if pd.Rule == nil {
		return ""
	}
	return pd.Rule.ID
}

func (a *App) buildEvent(cfg *RuntimeConfig, req *admissionv1.AdmissionRequest, ac AdmissionContext, pd PolicyDecision) *AdmissionEvent {
	id := eventID(cfg.ClusterName, string(req.UID), ac.Operation, ac.APIGroup, ac.Resource, ac.Namespace, ac.Name)
	ev := &AdmissionEvent{ID: id, RequestUID: string(req.UID), Time: time.Now().UTC(), Cluster: cfg.ClusterName, Operation: ac.Operation, APIVersion: ac.APIVersion, APIGroup: ac.APIGroup, Resource: ac.Resource, SubResource: ac.SubResource, Kind: ac.Kind, Namespace: ac.Namespace, Name: ac.Name, User: ac.User, Groups: ac.Groups, ScopeMatched: pd.ScopeMatched, ScopeIDs: pd.ScopeIDs, Decision: pd.Decision, Allowed: pd.Allowed && pd.Decision != DecisionRequireApproval, Reason: pd.Reason, ChangeClass: pd.ChangeClass, ChangeSummary: pd.ChangeSummary, PersistStatus: "unknown", Object: ac.ObjectRaw, OldObject: ac.OldObjectRaw}
	if pd.Rule != nil {
		ev.RuleID = pd.Rule.ID
		ev.RuleName = pd.Rule.Name
	}
	return ev
}

func eventID(cluster, uid, op, group, res, ns, name string) string {
	if strings.TrimSpace(uid) == "" {
		uid = fmt.Sprintf("no_uid_%d", time.Now().UnixNano())
	}
	s := strings.Join([]string{cluster, uid, op, group, res, ns, name}, "|")
	h := sha1.Sum([]byte(s))
	return fmt.Sprintf("%s_%s_%s_%s_%s", cluster, uid, strings.ToLower(op), res, hex.EncodeToString(h[:8]))
}

func shouldNotify(pd PolicyDecision) bool {
	if pd.Rule == nil {
		return false
	}
	if pd.Rule.Notify.Enabled {
		return true
	}
	return pd.Decision == DecisionBlock || pd.Decision == DecisionAllowNotify || pd.Decision == DecisionRequireApproval
}

func shouldCreateRollback(cfg *RuntimeConfig, pd PolicyDecision, ac AdmissionContext) bool {
	if cfg == nil || !cfg.Rollback.Enabled || pd.Rule == nil || !pd.Rule.Rollback.Enabled {
		return false
	}
	switch strings.ToUpper(ac.Operation) {
	case "UPDATE", "DELETE":
		return len(ac.OldObjectRaw) > 0
	case "CREATE":
		return cfg.Rollback.CreateDeleteRollback && len(ac.ObjectRaw) > 0
	default:
		return false
	}
}

func (a *App) recordEvent(ev *AdmissionEvent) {
	if ev == nil {
		return
	}
	if ev.PersistStatus == "" || ev.PersistStatus == "unknown" {
		ev.PersistStatus = "received"
	}
	if err := a.local.SpoolEvent(ev); err != nil {
		log.Printf("admission event local spool failed: id=%s uid=%s err=%v", ev.ID, ev.RequestUID, err)
	} else {
		log.Printf("admission event spooled: id=%s uid=%s resource=%s/%s/%s op=%s", ev.ID, ev.RequestUID, ev.Kind, ev.Namespace, ev.Name, ev.Operation)
	}
	if a.mongo != nil && a.mongo.Healthy() {
		ctx, cancel := context.WithTimeout(context.Background(), eventMongoWriteTimeout())
		defer cancel()
		ev.PersistStatus = "mongo_synced"
		if err := a.mongo.SaveEvent(ctx, ev); err != nil {
			ev.PersistStatus = "local_spooled"
			log.Printf("admission event mongo save failed, kept in local spool: id=%s uid=%s err=%v", ev.ID, ev.RequestUID, err)
		} else {
			log.Printf("admission event saved to mongo: id=%s uid=%s", ev.ID, ev.RequestUID)
		}
	} else {
		log.Printf("admission event kept in local spool because mongo is unavailable: id=%s uid=%s", ev.ID, ev.RequestUID)
	}
}

func eventMongoWriteTimeout() time.Duration {
	v := strings.TrimSpace(os.Getenv("EVENT_MONGO_WRITE_TIMEOUT"))
	if v == "" {
		return 2 * time.Second
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 2 * time.Second
	}
	if d > 10*time.Second {
		return 10 * time.Second
	}
	return d
}
