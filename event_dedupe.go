package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"
)

func admissionEventFingerprint(cfg *RuntimeConfig, ac AdmissionContext, pd PolicyDecision) string {
	cluster := ""
	if cfg != nil {
		cluster = cfg.ClusterName
	}
	ruleID := ""
	if pd.Rule != nil {
		ruleID = pd.Rule.ID
	}
	payload := map[string]any{
		"cluster":      cluster,
		"operation":    strings.ToUpper(strings.TrimSpace(ac.Operation)),
		"api_group":    strings.TrimSpace(ac.APIGroup),
		"resource":     strings.ToLower(strings.TrimSpace(ac.Resource)),
		"sub_resource": strings.TrimSpace(ac.SubResource),
		"kind":         strings.TrimSpace(ac.Kind),
		"namespace":    strings.TrimSpace(ac.Namespace),
		"name":         strings.TrimSpace(ac.Name),
		"resource_uid": strings.TrimSpace(ac.ResourceUID),
		"user":         strings.TrimSpace(ac.User),
		"rule_id":      ruleID,
		"decision":     pd.Decision,
		"change_class": pd.ChangeClass,
		"old_object":   normalizeObject(ac.OldObject),
		"object":       normalizeObject(ac.Object),
	}
	b, _ := json.Marshal(payload)
	h := sha256.Sum256(b)
	return "fp_" + hex.EncodeToString(h[:16])
}

func (a *App) recentDuplicateEvent(ctx context.Context, cfg *RuntimeConfig, fingerprint string) (*AdmissionEvent, error) {
	if a == nil || a.mongo == nil || !a.mongo.Healthy() || strings.TrimSpace(fingerprint) == "" {
		return nil, nil
	}
	window := duplicateEventWindow(cfg)
	if window <= 0 {
		return nil, nil
	}
	return a.mongo.FindRecentEventByFingerprint(ctx, fingerprint, time.Now().UTC().Add(-window))
}
