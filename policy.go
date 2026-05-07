package main

import (
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"
)

type AdmissionContext struct {
	Operation    string
	APIGroup     string
	APIVersion   string
	Resource     string
	SubResource  string
	Kind         string
	Namespace    string
	Name         string
	ResourceUID  string
	User         string
	Groups       []string
	Object       map[string]any
	OldObject    map[string]any
	ObjectRaw    json.RawMessage
	OldObjectRaw json.RawMessage
	RequestUID   string
}

type PolicyDecision struct {
	Decision      string
	Allowed       bool
	Reason        string
	Rule          *PolicyRule
	ScopeMatched  bool
	ScopeIDs      []string
	ChangeClass   string
	ChangeSummary string
}

func decide(cfg *RuntimeConfig, ac AdmissionContext) PolicyDecision {
	if cfg == nil || !cfg.Enabled {
		return PolicyDecision{Decision: DecisionAllowSilent, Allowed: true, Reason: "interceptor disabled"}
	}
	changeClass, summary := classifyChange(ac)
	if isInternalMongoDelete(cfg, ac) {
		return PolicyDecision{Decision: DecisionBlock, Allowed: false, Reason: "内置 MongoDB 系统资源禁止直接删除", ScopeMatched: true, ScopeIDs: []string{"system_internal_mongodb"}, ChangeClass: changeClass, ChangeSummary: summary}
	}
	if isInterceptorServiceAccountDelete(ac) {
		return PolicyDecision{Decision: DecisionBlock, Allowed: false, Reason: "审计服务账号禁止代执行集群删除操作", ScopeMatched: true, ScopeIDs: []string{"system_self"}, ChangeClass: changeClass, ChangeSummary: summary}
	}
	if isWebhookSelfMaintenance(cfg, ac) {
		return PolicyDecision{Decision: DecisionAuditOnly, Allowed: true, Reason: "webhook 自身维护资源，仅审计", ScopeMatched: true, ScopeIDs: []string{"system_self"}, ChangeClass: changeClass, ChangeSummary: summary}
	}
	scopes := matchingScopes(cfg, ac)
	scopeIDs := make([]string, 0, len(scopes))
	for _, s := range scopes {
		scopeIDs = append(scopeIDs, s.ID)
	}
	rules := append([]PolicyRule(nil), cfg.Rules...)
	sort.SliceStable(rules, func(i, j int) bool { return rules[i].Priority < rules[j].Priority })
	for i := range rules {
		r := &rules[i]
		if !r.Enabled {
			continue
		}
		if !matchAnyFold(r.Operations, ac.Operation) {
			continue
		}
		ruleScopeIDs := matchingScopeIDsForRule(cfg, *r, ac)
		if len(r.ScopeIDs) > 0 && len(ruleScopeIDs) == 0 {
			continue
		}
		if len(r.ActorGroupIDs) > 0 {
			if !matchActorGroups(cfg, r.ActorGroupIDs, ac) {
				continue
			}
		} else if ruleNeedsExplicitActorGroup(*r) {
			logPolicyActorGate(ac, *r)
			continue
		}
		if len(r.ChangeClasses) > 0 && !matchAnyFold(r.ChangeClasses, changeClass) {
			continue
		}
		if !passesControllerSafeRule(r.ControllerSafe, ac) {
			continue
		}
		dec := r.Decision
		if dec == "" {
			dec = DecisionAuditOnly
		}
		reason := r.Reason
		if reason == "" {
			reason = "matched rule " + r.Name
		}
		return PolicyDecision{Decision: dec, Allowed: decisionAllowed(dec), Reason: reason, Rule: r, ScopeMatched: len(ruleScopeIDs) > 0 || len(r.ScopeIDs) == 0, ScopeIDs: ruleScopeIDs, ChangeClass: changeClass, ChangeSummary: summary}
	}
	dec := defaultDecisionFor(cfg, ac.Operation)
	reason := "资源未命中策略范围，默认只审计"
	scopeMatched := false
	if len(scopeIDs) > 0 {
		reason = "资源在策略范围内，但未命中具体规则，默认只审计"
		scopeMatched = true
	}
	return PolicyDecision{Decision: dec, Allowed: decisionAllowed(dec), Reason: reason, ScopeMatched: scopeMatched, ScopeIDs: scopeIDs, ChangeClass: changeClass, ChangeSummary: summary}
}

func isInterceptorServiceAccountDelete(ac AdmissionContext) bool {
	if !strings.EqualFold(strings.TrimSpace(ac.Operation), "DELETE") {
		return false
	}
	return strings.TrimSpace(ac.User) != "" && strings.TrimSpace(ac.User) == interceptorServiceAccountUser()
}

func defaultDecisionFor(cfg *RuntimeConfig, op string) string {
	switch strings.ToUpper(op) {
	case "CREATE":
		if cfg.Defaults.Unmatched.Create != "" {
			return cfg.Defaults.Unmatched.Create
		}
	case "UPDATE":
		if cfg.Defaults.Unmatched.Update != "" {
			return cfg.Defaults.Unmatched.Update
		}
	case "DELETE":
		if cfg.Defaults.Unmatched.Delete != "" {
			return cfg.Defaults.Unmatched.Delete
		}
	}
	return DecisionAuditOnly
}

func decisionAllowed(d string) bool {
	switch d {
	case DecisionBlock, DecisionRequireApproval:
		return false
	default:
		return true
	}
}

func matchingScopes(cfg *RuntimeConfig, ac AdmissionContext) []ResourceScope {
	out := []ResourceScope{}
	refs := referencedScopeIDs(cfg)
	for _, s := range cfg.ResourceScopes {
		if !refs[s.ID] || !resourceScopeMatches(s, ac) {
			continue
		}
		out = append(out, s)
	}
	return out
}

func referencedScopeIDs(cfg *RuntimeConfig) map[string]bool {
	refs := map[string]bool{}
	if cfg == nil {
		return refs
	}
	for _, r := range cfg.Rules {
		if !r.Enabled {
			continue
		}
		for _, id := range r.ScopeIDs {
			id = strings.TrimSpace(id)
			if id != "" {
				refs[id] = true
			}
		}
	}
	return refs
}

func matchingScopeIDsForRule(cfg *RuntimeConfig, r PolicyRule, ac AdmissionContext) []string {
	if cfg == nil || len(r.ScopeIDs) == 0 {
		return nil
	}
	want := map[string]bool{}
	for _, id := range r.ScopeIDs {
		id = strings.TrimSpace(id)
		if id != "" {
			want[id] = true
		}
	}
	out := []string{}
	for _, s := range cfg.ResourceScopes {
		if !want[s.ID] || !resourceScopeMatches(s, ac) {
			continue
		}
		out = append(out, s.ID)
	}
	return out
}

func resourceScopeMatches(s ResourceScope, ac AdmissionContext) bool {
	if !s.Enabled {
		return false
	}
	if !matchPatterns(s.APIGroups, ac.APIGroup) {
		return false
	}
	if !matchPatterns(s.Resources, ac.Resource) {
		return false
	}
	if !matchPatterns(s.Kinds, ac.Kind) {
		return false
	}
	if !matchPatterns(s.Namespaces, namespaceOrCluster(ac.Namespace)) {
		return false
	}
	if !matchPatterns(s.Names, nameOrWildcard(ac.Name)) {
		return false
	}
	return true
}

func matchActorGroups(cfg *RuntimeConfig, ids []string, ac AdmissionContext) bool {
	for _, id := range ids {
		for _, a := range cfg.ActorGroups {
			if a.ID != id || !a.Enabled {
				continue
			}
			if len(a.Users) > 0 && matchPatterns(a.Users, ac.User) {
				return true
			}
			if len(a.ServiceAccounts) > 0 && matchPatterns(a.ServiceAccounts, ac.User) {
				return true
			}
			if len(a.Groups) > 0 {
				for _, g := range ac.Groups {
					if matchPatterns(a.Groups, g) {
						return true
					}
				}
			}
		}
	}
	return false
}

func ruleNeedsExplicitActorGroup(r PolicyRule) bool {
	if strings.EqualFold(r.Decision, DecisionAllowNotify) || strings.EqualFold(r.Decision, DecisionRequireApproval) {
		return true
	}
	if r.Approval.Enabled || r.Rollback.Enabled {
		return true
	}
	if r.Notify.Enabled && !strings.EqualFold(r.Decision, DecisionBlock) {
		return true
	}
	return false
}

func logPolicyActorGate(ac AdmissionContext, r PolicyRule) {
	// 通知、审批和回滚必须绑定 ActorGroup，避免全资源 UPDATE 规则误把系统控制器、Lease 续约等噪声事件推送到 Telegram。
	_ = ac
	_ = r
}

func passesControllerSafeRule(rule ControllerSafeRule, ac AdmissionContext) bool {
	if !rule.RequireOwnerReference && !rule.RequireControllerOwnerReference && !rule.RequireNodeUserMatchesPodNode {
		return true
	}
	if strings.EqualFold(ac.Kind, "Pod") && rule.RequireNodeUserMatchesPodNode && strings.HasPrefix(ac.User, "system:node:") {
		nodeName, _ := getNestedString(ac.OldObject, "spec", "nodeName")
		if nodeName == "" {
			nodeName, _ = getNestedString(ac.Object, "spec", "nodeName")
		}
		if nodeName != "" && !strings.EqualFold(strings.TrimPrefix(ac.User, "system:node:"), nodeName) {
			return false
		}
	}
	owners, ok := getNestedSlice(ac.OldObject, "metadata", "ownerReferences")
	if !ok {
		owners, ok = getNestedSlice(ac.Object, "metadata", "ownerReferences")
	}
	if rule.RequireOwnerReference && !ok {
		return false
	}
	if !ok {
		return true
	}
	for _, x := range owners {
		m, _ := x.(map[string]any)
		if m == nil {
			continue
		}
		kind, _ := m["kind"].(string)
		controller, _ := m["controller"].(bool)
		if rule.RequireControllerOwnerReference && !controller {
			continue
		}
		if len(rule.AllowedOwnerKinds) == 0 || matchAnyFold(rule.AllowedOwnerKinds, kind) {
			return true
		}
	}
	return false
}

func isInternalMongoDelete(cfg *RuntimeConfig, ac AdmissionContext) bool {
	if !cfg.SystemProtection.Enabled || !strings.EqualFold(ac.Operation, "DELETE") {
		return false
	}
	labelKey := cfg.SystemProtection.SystemResourceLabel
	val := cfg.SystemProtection.InternalMongoResourceValue
	if labelKey == "" || val == "" {
		return false
	}
	labels := map[string]any{}
	if m, ok := getNestedMap(ac.OldObject, "metadata", "labels"); ok {
		labels = m
	}
	if len(labels) == 0 {
		if m, ok := getNestedMap(ac.Object, "metadata", "labels"); ok {
			labels = m
		}
	}
	if fmt.Sprint(labels[labelKey]) == val {
		return true
	}
	return strings.Contains(ac.Name, "delete-interceptor-mongodb")
}

func isWebhookSelfMaintenance(cfg *RuntimeConfig, ac AdmissionContext) bool {
	if !cfg.SystemProtection.AllowWebhookSelfMaintenance {
		return false
	}
	if ac.Namespace != envOr("POD_NAMESPACE", "webhook-system") {
		return false
	}
	if strings.Contains(ac.Name, "delete-interceptor-mongodb") {
		return false
	}
	if strings.HasPrefix(ac.Name, "delete-interceptor") {
		return true
	}
	labels := map[string]any{}
	if m, ok := getNestedMap(ac.OldObject, "metadata", "labels"); ok {
		labels = m
	}
	if len(labels) == 0 {
		if m, ok := getNestedMap(ac.Object, "metadata", "labels"); ok {
			labels = m
		}
	}
	if fmt.Sprint(labels["app.kubernetes.io/name"]) == "k8s-delete-interceptor" {
		return true
	}
	return false
}

func matchPatterns(patterns []string, value string) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, p := range patterns {
		p = strings.TrimSpace(p)
		// Kubernetes core API group is represented by an empty string.
		// Do not skip empty patterns, otherwise ConfigMap/Secret/Service/Namespace
		// scopes never match AdmissionRequest.Resource.Group == "".
		if p == "" {
			if value == "" {
				return true
			}
			continue
		}
		if p == "core" || p == "<core>" {
			if value == "" {
				return true
			}
			continue
		}
		if p == "*" {
			return true
		}
		if strings.HasPrefix(p, "regex:") {
			if regexp.MustCompile(strings.TrimPrefix(p, "regex:")).MatchString(value) {
				return true
			}
			continue
		}
		if strings.ContainsAny(p, "*?[]") {
			if ok, _ := path.Match(strings.ToLower(p), strings.ToLower(value)); ok {
				return true
			}
			continue
		}
		if strings.EqualFold(p, value) {
			return true
		}
	}
	return false
}

func matchAnyFold(xs []string, v string) bool { return matchPatterns(xs, v) }

func intersects(a, b []string) bool {
	m := map[string]bool{}
	for _, x := range a {
		m[x] = true
	}
	for _, y := range b {
		if m[y] {
			return true
		}
	}
	return false
}
func namespaceOrCluster(ns string) string {
	if ns == "" {
		return "*"
	}
	return ns
}
func nameOrWildcard(n string) string {
	if n == "" {
		return "*"
	}
	return n
}

func getNestedString(m map[string]any, keys ...string) (string, bool) {
	v, ok := getNested(m, keys...)
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}
func getNestedMap(m map[string]any, keys ...string) (map[string]any, bool) {
	v, ok := getNested(m, keys...)
	if !ok {
		return nil, false
	}
	mm, ok := v.(map[string]any)
	return mm, ok
}
func getNestedSlice(m map[string]any, keys ...string) ([]any, bool) {
	v, ok := getNested(m, keys...)
	if !ok {
		return nil, false
	}
	sx, ok := v.([]any)
	return sx, ok
}
func getNested(m map[string]any, keys ...string) (any, bool) {
	var cur any = m
	for _, k := range keys {
		mm, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = mm[k]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}
