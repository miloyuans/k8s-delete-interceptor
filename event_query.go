package main

import (
	"strings"
	"time"
)

type EventQuery struct {
	Limit     int
	Start     time.Time
	End       time.Time
	Cluster   string
	Namespace string
	Kind      string
	Resource  string
	Name      string
	User      string
	Operation string
	Decision  string
	Allowed   *bool
}

func (q EventQuery) NormalizedLimit(def int) int {
	if def <= 0 {
		def = 100
	}
	if q.Limit <= 0 {
		return def
	}
	if q.Limit > 2000 {
		return 2000
	}
	return q.Limit
}

func (q EventQuery) Match(ev AdmissionEvent) bool {
	if !q.Start.IsZero() && ev.Time.Before(q.Start) {
		return false
	}
	if !q.End.IsZero() && !ev.Time.Before(q.End) {
		return false
	}
	if q.Cluster != "" && !matchExact(ev.Cluster, q.Cluster) {
		return false
	}
	if q.Namespace != "" && !matchExact(ev.Namespace, q.Namespace) {
		return false
	}
	if q.Kind != "" && !matchExact(ev.Kind, q.Kind) {
		return false
	}
	if q.Resource != "" && !matchExact(ev.Resource, q.Resource) {
		return false
	}
	if q.Operation != "" && !matchExact(ev.Operation, q.Operation) {
		return false
	}
	if q.Decision != "" && !matchExact(ev.Decision, q.Decision) {
		return false
	}
	if q.Name != "" && !matchContains(ev.Name, q.Name) {
		return false
	}
	if q.User != "" && !matchContains(ev.User, q.User) {
		return false
	}
	if q.Allowed != nil && ev.Allowed != *q.Allowed {
		return false
	}
	return true
}

func matchExact(v, want string) bool {
	return strings.EqualFold(strings.TrimSpace(v), strings.TrimSpace(want))
}

func matchContains(v, want string) bool {
	return strings.Contains(strings.ToLower(strings.TrimSpace(v)), strings.ToLower(strings.TrimSpace(want)))
}
