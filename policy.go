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
	if isWebhookSelfMaintenance(cfg, ac) {
		return PolicyDecision{Decision: DecisionAuditOnly, Allowed: true, Reason: "webhook 自身维护资源，仅审计", ScopeMatched: true, ScopeIDs: []string{"system_self"}, ChangeClass: changeClass, ChangeSummary: summary}
	}
	scopes := matchingScopes(cfg, ac)
	scopeIDs := make([]string, 0, len(scopes))
	for _, s := range scopes {
		scopeIDs = append(scopeIDs, s.ID)
	}
	if len(scopes) == 0 {
		dec := defaultDecisionFor(cfg, ac.Operation)
		return PolicyDecision{Decision: dec, Allowed: decisionAllowed(dec), Reason: "资源未命中策略范围，默认只审计", ScopeMatched: false, ChangeClass: changeClass, ChangeSummary: summary}
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
		if len(r.ScopeIDs) > 0 && !intersects(r.ScopeIDs, scopeIDs) {
			continue
		}
		if len(r.ActorGroupIDs) > 0 && !matchActorGroups(cfg, r.ActorGroupIDs, ac) {
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
		return PolicyDecision{Decision: dec, Allowed: decisionAllowed(dec), Reason: reason, Rule: r, ScopeMatched: true, ScopeIDs: scopeIDs, ChangeClass: changeClass, ChangeSummary: summary}
	}
	dec := defaultDecisionFor(cfg, ac.Operation)
	return PolicyDecision{Decision: dec, Allowed: decisionAllowed(dec), Reason: "资源在策略范围内，但未命中具体规则，默认只审计", ScopeMatched: true, ScopeIDs: scopeIDs, ChangeClass: changeClass, ChangeSummary: summary}
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
	for _, s := range cfg.ResourceScopes {
		if !s.Enabled {
			continue
		}
		if !matchPatterns(s.APIGroups, ac.APIGroup) {
			continue
		}
		if !matchPatterns(s.Resources, ac.Resource) {
			continue
		}
		if !matchPatterns(s.Kinds, ac.Kind) {
			continue
		}
		if !matchPatterns(s.Namespaces, namespaceOrCluster(ac.Namespace)) {
			continue
		}
		if !matchPatterns(s.Names, nameOrWildcard(ac.Name)) {
			continue
		}
		out = append(out, s)
	}
	return out
}

func matchActorGroups(cfg *RuntimeConfig, ids []string, ac AdmissionContext) bool {
	for _, id := range ids {
		for _, a := range cfg.ActorGroups {
			if a.ID != id || !a.Enabled {
				continue
			}
			if matchPatterns(a.Users, ac.User) || matchPatterns(a.ServiceAccounts, ac.User) {
				return true
			}
			for _, g := range ac.Groups {
				if matchPatterns(a.Groups, g) {
					return true
				}
			}
		}
	}
	return false
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
