package main

import (
	"context"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type ResourceOption struct {
	APIGroup   string `json:"api_group"`
	Version    string `json:"version"`
	Resource   string `json:"resource"`
	Kind       string `json:"kind"`
	Namespaced bool   `json:"namespaced"`
}

type ClusterMetadata struct {
	GeneratedAt      time.Time            `json:"generated_at"`
	Namespaces       []string             `json:"namespaces"`
	Resources        []ResourceOption     `json:"resources"`
	Kinds            []string             `json:"kinds"`
	Users            []string             `json:"users"`
	ServiceAccounts  []ServiceAccountInfo `json:"service_accounts"`
	ActorGroups      []ActorGroup         `json:"actor_groups"`
	ResourceScopes   []ResourceScope      `json:"resource_scopes"`
	DataSources      []DataSource         `json:"data_sources"`
	WebRoles         []WebRole            `json:"web_roles"`
	Permissions      []string             `json:"permissions"`
	KubeAvailable    bool                 `json:"kube_available"`
	MetadataFallback bool                 `json:"metadata_fallback"`
}

func (a *App) BuildMetadata(ctx context.Context) ClusterMetadata {
	cfg := a.Config()
	md := ClusterMetadata{GeneratedAt: time.Now().UTC()}
	if cfg != nil {
		md.ActorGroups = cfg.ActorGroups
		md.ResourceScopes = cfg.ResourceScopes
		md.DataSources = cfg.DataSources
		md.WebRoles = cfg.WebRoles
	}
	for _, p := range []string{PermDashboardRead, PermEventsRead, PermSARead, PermSAScan, PermSAMount, PermConfigRead, PermConfigWrite, PermConfigApprove, PermConfigRestore, PermRulesWrite, PermDatasourceWrite, PermSettingsWrite, PermUsersWrite, PermRolesWrite, PermRollbackExecute} {
		md.Permissions = append(md.Permissions, p)
	}
	md.Namespaces = a.listNamespaces(ctx)
	md.Resources = a.listAPIResources()
	md.ServiceAccounts = a.cachedServiceAccounts(ctx, "", 2000)
	userSet := map[string]bool{}
	for _, sa := range md.ServiceAccounts {
		userSet[sa.UserString] = true
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
	sort.Strings(md.Namespaces)
	sort.Strings(md.Kinds)
	sort.Strings(md.Users)
	sort.Slice(md.Resources, func(i, j int) bool {
		if md.Resources[i].APIGroup == md.Resources[j].APIGroup {
			return md.Resources[i].Resource < md.Resources[j].Resource
		}
		return md.Resources[i].APIGroup < md.Resources[j].APIGroup
	})
	md.KubeAvailable = a.kubeClient != nil
	md.MetadataFallback = len(md.Namespaces) == 0 || len(md.Resources) == 0
	return md
}

func (a *App) listNamespaces(ctx context.Context) []string {
	if a.kubeClient == nil {
		return nil
	}
	lst, err := a.kubeClient.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
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
	if a.mongo != nil && a.mongo.Healthy() {
		if items, err := a.mongo.ListServiceAccountsByNamespace(ctx, namespace, limit); err == nil && len(items) > 0 {
			return items
		}
	}
	items, err := a.ScanServiceAccounts(ctx)
	if err != nil {
		return nil
	}
	if namespace == "" || namespace == "all" || namespace == "*" {
		return items
	}
	out := []ServiceAccountInfo{}
	for _, it := range items {
		if it.Namespace == namespace {
			out = append(out, it)
		}
	}
	return out
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
