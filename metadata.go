package main

import (
	"context"
	"log"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type ResourceOption struct {
	APIGroup   string `json:"api_group" bson:"api_group"`
	Version    string `json:"version" bson:"version"`
	Resource   string `json:"resource" bson:"resource"`
	Kind       string `json:"kind" bson:"kind"`
	Namespaced bool   `json:"namespaced" bson:"namespaced"`
}

type ClusterMetadata struct {
	GeneratedAt      time.Time            `json:"generated_at" bson:"generated_at"`
	NextRefreshAfter time.Time            `json:"next_refresh_after" bson:"next_refresh_after"`
	RefreshInterval  string               `json:"refresh_interval" bson:"refresh_interval"`
	Namespaces       []string             `json:"namespaces" bson:"namespaces"`
	Resources        []ResourceOption     `json:"resources" bson:"resources"`
	Kinds            []string             `json:"kinds" bson:"kinds"`
	Users            []string             `json:"users" bson:"users"`
	ServiceAccounts  []ServiceAccountInfo `json:"service_accounts" bson:"service_accounts"`
	ActorGroups      []ActorGroup         `json:"actor_groups" bson:"actor_groups"`
	ResourceScopes   []ResourceScope      `json:"resource_scopes" bson:"resource_scopes"`
	DataSources      []DataSource         `json:"data_sources" bson:"data_sources"`
	WebRoles         []WebRole            `json:"web_roles" bson:"web_roles"`
	Permissions      []string             `json:"permissions" bson:"permissions"`
	KubeAvailable    bool                 `json:"kube_available" bson:"kube_available"`
	MetadataFallback bool                 `json:"metadata_fallback" bson:"metadata_fallback"`
	Source           string               `json:"source" bson:"source"`
}

func metadataRefreshInterval() time.Duration {
	raw := strings.TrimSpace(envOr("METADATA_REFRESH_INTERVAL", "10m"))
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		d = 10 * time.Minute
	}
	if d < 2*time.Minute {
		d = 2 * time.Minute
	}
	return d
}

func metadataRequestTimeout() time.Duration {
	raw := strings.TrimSpace(envOr("METADATA_REFRESH_TIMEOUT", "20s"))
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 20 * time.Second
	}
	if d > 60*time.Second {
		return 60 * time.Second
	}
	return d
}

func (a *App) BuildMetadata(ctx context.Context) ClusterMetadata {
	if md := a.cachedMetadata(); md != nil {
		return *md
	}
	if err := a.loadPersistedMetadata(ctx); err == nil {
		if md := a.cachedMetadata(); md != nil {
			return *md
		}
	}
	md := a.baseMetadata()
	md.MetadataFallback = true
	md.Source = "config-only"
	a.storeMetadata(md)
	if strings.EqualFold(strings.TrimSpace(envOr("METADATA_AUTO_REFRESH_ON_MISS", "false")), "true") {
		go func() {
			_, _ = a.RefreshMetadata(context.Background(), false)
		}()
	}
	return md
}

func (a *App) RefreshMetadata(ctx context.Context, force bool) (ClusterMetadata, error) {
	if !force {
		if md := a.cachedMetadata(); md != nil && time.Now().Before(md.NextRefreshAfter) {
			return *md, nil
		}
	}
	if !a.metadataRefresh.CompareAndSwap(false, true) {
		if md := a.cachedMetadata(); md != nil {
			return *md, nil
		}
		return a.baseMetadata(), nil
	}
	defer a.metadataRefresh.Store(false)

	ctx, cancel := context.WithTimeout(ctx, metadataRequestTimeout())
	defer cancel()
	md := a.collectMetadata(ctx)
	md.Source = "kubernetes-cache"
	if md.MetadataFallback {
		md.Source = "partial-cache"
	}
	a.storeMetadata(md)
	if err := a.persistMetadata(ctx, md); err != nil {
		log.Printf("metadata persistence failed: %v", err)
	}
	return md, nil
}

func (a *App) cachedMetadata() *ClusterMetadata {
	v := a.metadataValue.Load()
	if v == nil {
		return nil
	}
	if md, ok := v.(*ClusterMetadata); ok && md != nil {
		return md
	}
	return nil
}

func (a *App) storeMetadata(md ClusterMetadata) {
	interval := metadataRefreshInterval()
	if md.GeneratedAt.IsZero() {
		md.GeneratedAt = time.Now().UTC()
	}
	if md.RefreshInterval == "" {
		md.RefreshInterval = interval.String()
	}
	if md.NextRefreshAfter.IsZero() {
		md.NextRefreshAfter = md.GeneratedAt.Add(interval)
	}
	a.metadataValue.Store(&md)
}

func (a *App) loadPersistedMetadata(ctx context.Context) error {
	if a.mongo != nil && a.mongo.Healthy() {
		if md, err := a.mongo.LoadClusterMetadata(ctx); err == nil && md != nil {
			a.storeMetadata(*md)
			return nil
		}
	}
	if a.local != nil {
		if md, err := a.local.LoadClusterMetadata(); err == nil && md != nil {
			a.storeMetadata(*md)
			return nil
		}
	}
	return context.Canceled
}

func (a *App) persistMetadata(ctx context.Context, md ClusterMetadata) error {
	if a.local != nil {
		_ = a.local.SaveClusterMetadata(&md)
	}
	if a.mongo != nil && a.mongo.Healthy() {
		return a.mongo.SaveClusterMetadata(ctx, &md)
	}
	return nil
}

func (a *App) metadataRefreshLoop(ctx context.Context) {
	initialDelay := 20 * time.Second
	if raw := strings.TrimSpace(envOr("METADATA_INITIAL_DELAY", "20s")); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d >= 0 {
			initialDelay = d
		}
	}
	timer := time.NewTimer(initialDelay)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			_, _ = a.RefreshMetadata(ctx, false)
			timer.Reset(metadataRefreshInterval())
		}
	}
}

func (a *App) baseMetadata() ClusterMetadata {
	cfg := a.Config()
	md := ClusterMetadata{GeneratedAt: time.Now().UTC(), KubeAvailable: a.kubeClient != nil}
	if cfg != nil {
		md.ActorGroups = cfg.ActorGroups
		md.ResourceScopes = cfg.ResourceScopes
		md.DataSources = cfg.DataSources
		md.WebRoles = cfg.WebRoles
	}
	md.Permissions = allWebPermissions()
	return md
}

func allWebPermissions() []string {
	out := []string{PermDashboardRead, PermEventsRead, PermSARead, PermSAScan, PermSAMount, PermConfigRead, PermConfigWrite, PermConfigApprove, PermConfigRestore, PermRulesWrite, PermDatasourceWrite, PermTelegramWrite, PermSettingsWrite, PermUsersWrite, PermRolesWrite, PermRollbackExecute}
	sort.Strings(out)
	return out
}

func (a *App) collectMetadata(ctx context.Context) ClusterMetadata {
	md := a.baseMetadata()
	md.Namespaces = a.listNamespaces(ctx)
	md.Resources = a.listAPIResources()
	md.ServiceAccounts = a.scanServiceAccountsQuiet(ctx)
	userSet := map[string]bool{}
	for _, sa := range md.ServiceAccounts {
		if sa.UserString != "" {
			userSet[sa.UserString] = true
		}
	}
	if events, err := a.recentEventsForMetadata(ctx); err == nil {
		for _, ev := range events {
			if ev.User != "" {
				userSet[ev.User] = true
			}
			if ev.Namespace != "" {
				md.Namespaces = appendUnique(md.Namespaces, ev.Namespace)
			}
			if ev.Kind != "" {
				md.Kinds = appendUnique(md.Kinds, ev.Kind)
			}
			if ev.Resource != "" {
				md.Resources = appendResource(md.Resources, ResourceOption{APIGroup: ev.APIGroup, Resource: ev.Resource, Kind: ev.Kind, Namespaced: ev.Namespace != ""})
			}
		}
	}
	for _, r := range md.Resources {
		if r.Kind != "" {
			md.Kinds = appendUnique(md.Kinds, r.Kind)
		}
	}
	for u := range userSet {
		md.Users = append(md.Users, u)
	}
	md.Namespaces = dedupeSort(md.Namespaces)
	md.Kinds = dedupeSort(md.Kinds)
	md.Users = dedupeSort(md.Users)
	sort.Slice(md.Resources, func(i, j int) bool {
		if md.Resources[i].APIGroup == md.Resources[j].APIGroup {
			return md.Resources[i].Resource < md.Resources[j].Resource
		}
		return md.Resources[i].APIGroup < md.Resources[j].APIGroup
	})
	md.MetadataFallback = len(md.Namespaces) == 0 || len(md.Resources) == 0
	return md
}

func (a *App) scanServiceAccountsQuiet(ctx context.Context) []ServiceAccountInfo {
	if a.kubeClient == nil {
		return nil
	}
	items, err := a.ScanServiceAccounts(ctx)
	if err != nil {
		log.Printf("service account metadata refresh failed: %v", err)
		return nil
	}
	return items
}

func (a *App) listNamespaces(ctx context.Context) []string {
	if a.kubeClient == nil {
		return nil
	}
	lst, err := a.kubeClient.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Printf("namespace metadata refresh failed: %v", err)
		return nil
	}
	out := make([]string, 0, len(lst.Items))
	for _, ns := range lst.Items {
		out = append(out, ns.Name)
	}
	return out
}

func (a *App) listAPIResources() []ResourceOption {
	if a.discoveryClient == nil {
		return nil
	}
	lists, err := a.discoveryClient.ServerPreferredResources()
	if err != nil && len(lists) == 0 {
		log.Printf("api resource metadata refresh failed: %v", err)
		return nil
	}
	out := []ResourceOption{}
	for _, list := range lists {
		gv, _ := schema.ParseGroupVersion(list.GroupVersion)
		for _, r := range list.APIResources {
			if strings.Contains(r.Name, "/") || len(r.Verbs) == 0 {
				continue
			}
			if !hasAnyVerb(r.Verbs, []string{"create", "update", "delete"}) {
				continue
			}
			out = appendResource(out, ResourceOption{APIGroup: gv.Group, Version: gv.Version, Resource: r.Name, Kind: r.Kind, Namespaced: r.Namespaced})
		}
	}
	return out
}

func hasAnyVerb(verbs []string, want []string) bool {
	for _, v := range verbs {
		for _, w := range want {
			if v == w {
				return true
			}
		}
	}
	return false
}

func appendResource(xs []ResourceOption, x ResourceOption) []ResourceOption {
	key := x.APIGroup + "/" + x.Resource + "/" + x.Kind
	for _, y := range xs {
		if y.APIGroup+"/"+y.Resource+"/"+y.Kind == key {
			return xs
		}
	}
	return append(xs, x)
}

func (a *App) cachedServiceAccounts(ctx context.Context, namespace string, limit int) []ServiceAccountInfo {
	var items []ServiceAccountInfo
	if md := a.cachedMetadata(); md != nil && len(md.ServiceAccounts) > 0 {
		items = md.ServiceAccounts
	} else if a.mongo != nil && a.mongo.Healthy() {
		items, _ = a.mongo.ListServiceAccountsByNamespace(ctx, namespace, limit)
	}
	if len(items) == 0 && a.local != nil {
		if md, err := a.local.LoadClusterMetadata(); err == nil && md != nil {
			items = md.ServiceAccounts
		}
	}
	if namespace == "" || namespace == "all" || namespace == "*" {
		return limitServiceAccounts(items, limit)
	}
	out := []ServiceAccountInfo{}
	for _, it := range items {
		if it.Namespace == namespace {
			out = append(out, it)
		}
	}
	return limitServiceAccounts(out, limit)
}

func limitServiceAccounts(items []ServiceAccountInfo, limit int) []ServiceAccountInfo {
	if limit <= 0 || limit > 5000 {
		limit = 1000
	}
	if len(items) > limit {
		return items[:limit]
	}
	return items
}

func (a *App) recentEventsForMetadata(ctx context.Context) ([]AdmissionEvent, error) {
	q := EventQuery{Limit: 500}
	if a.mongo != nil && a.mongo.Healthy() {
		if ev, err := a.mongo.ListEventsByQuery(ctx, q); err == nil {
			return ev, nil
		}
	}
	return a.local.ListRecentEventsByQuery(q)
}

func dedupeSort(xs []string) []string {
	set := map[string]bool{}
	out := []string{}
	for _, x := range xs {
		x = strings.TrimSpace(x)
		if x == "" || set[x] {
			continue
		}
		set[x] = true
		out = append(out, x)
	}
	sort.Strings(out)
	return out
}
