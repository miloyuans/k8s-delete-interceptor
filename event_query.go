package main

import (
	"regexp"
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
	if q.Limit > 5000 {
		return 5000
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
	if q.Cluster != "" && !matchPattern(ev.Cluster, q.Cluster, true) {
		return false
	}
	if q.Namespace != "" && !matchPattern(ev.Namespace, q.Namespace, true) {
		return false
	}
	if q.Kind != "" && !matchPattern(ev.Kind, q.Kind, true) {
		return false
	}
	if q.Resource != "" && !matchPattern(ev.Resource, q.Resource, true) {
		return false
	}
	if q.Operation != "" && !matchPattern(ev.Operation, q.Operation, true) {
		return false
	}
	if q.Decision != "" && !matchPattern(ev.Decision, q.Decision, true) {
		return false
	}
	if q.Name != "" && !matchPattern(ev.Name, q.Name, false) {
		return false
	}
	if q.User != "" && !matchPattern(ev.User, q.User, false) {
		return false
	}
	if q.Allowed != nil && ev.Allowed != *q.Allowed {
		return false
	}
	return true
}

func matchPattern(value, pattern string, exactDefault bool) bool {
	v := strings.ToLower(strings.TrimSpace(value))
	p := strings.ToLower(strings.TrimSpace(pattern))
	if p == "" || p == "*" || p == "all" {
		return true
	}
	if strings.HasPrefix(p, "regex:") {
		re, err := regexp.Compile(strings.TrimPrefix(p, "regex:"))
		return err == nil && re.MatchString(value)
	}
	if strings.ContainsAny(p, "*?") {
		return wildcardRegex(p).MatchString(v)
	}
	if exactDefault {
		return v == p
	}
	return strings.Contains(v, p)
}

func wildcardRegex(pattern string) *regexp.Regexp {
	var b strings.Builder
	b.WriteString("^")
	for _, r := range pattern {
		switch r {
		case '*':
			b.WriteString(".*")
		case '?':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteString("$")
	re, err := regexp.Compile(b.String())
	if err != nil {
		return regexp.MustCompile("a^")
	}
	return re
}
