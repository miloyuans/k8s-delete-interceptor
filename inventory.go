package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (a *App) ScanServiceAccounts(ctx context.Context) ([]ServiceAccountInfo, error) {
	if a.kubeClient == nil {
		return nil, fmt.Errorf("kubernetes client unavailable")
	}
	cfg := a.Config()
	cluster := "default-cluster"
	if cfg != nil {
		cluster = cfg.ClusterName
	}
	saList, err := a.kubeClient.CoreV1().ServiceAccounts("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	pods, _ := a.kubeClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	rbs, _ := a.kubeClient.RbacV1().RoleBindings("").List(ctx, metav1.ListOptions{})
	crbs, _ := a.kubeClient.RbacV1().ClusterRoleBindings().List(ctx, metav1.ListOptions{})
	usedBy := map[string][]string{}
	for _, p := range pods.Items {
		name := p.Spec.ServiceAccountName
		if name == "" {
			name = "default"
		}
		key := p.Namespace + "/" + name
		owner := "Pod/" + p.Namespace + "/" + p.Name
		if len(p.OwnerReferences) > 0 {
			owner = fmt.Sprintf("%s/%s/%s", p.OwnerReferences[0].Kind, p.Namespace, p.OwnerReferences[0].Name)
		}
		usedBy[key] = appendUnique(usedBy[key], owner)
	}
	rbMap := map[string][]string{}
	for _, rb := range rbs.Items {
		for _, s := range rb.Subjects {
			if s.Kind == "ServiceAccount" {
				ns := s.Namespace
				if ns == "" {
					ns = rb.Namespace
				}
				key := ns + "/" + s.Name
				rbMap[key] = appendUnique(rbMap[key], fmt.Sprintf("%s/%s -> %s/%s", rb.Namespace, rb.Name, rb.RoleRef.Kind, rb.RoleRef.Name))
			}
		}
	}
	crbMap := map[string][]string{}
	for _, rb := range crbs.Items {
		for _, s := range rb.Subjects {
			if s.Kind == "ServiceAccount" {
				key := s.Namespace + "/" + s.Name
				crbMap[key] = appendUnique(crbMap[key], fmt.Sprintf("%s -> %s/%s", rb.Name, rb.RoleRef.Kind, rb.RoleRef.Name))
			}
		}
	}
	out := make([]ServiceAccountInfo, 0, len(saList.Items))
	now := time.Now().UTC()
	for _, sa := range saList.Items {
		key := sa.Namespace + "/" + sa.Name
		cat, conf := classifyServiceAccount(sa.Namespace, sa.Name, usedBy[key], rbMap[key], crbMap[key])
		out = append(out, ServiceAccountInfo{ID: cluster + ":" + key, Cluster: cluster, Namespace: sa.Namespace, Name: sa.Name, UserString: "system:serviceaccount:" + sa.Namespace + ":" + sa.Name, Category: cat, Confidence: conf, UsedBy: sortStrings(usedBy[key]), RoleBindings: sortStrings(rbMap[key]), ClusterRoleBindings: sortStrings(crbMap[key]), SuggestedActorGroup: suggestActorGroup(cat), ScannedAt: now})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace == out[j].Namespace {
			return out[i].Name < out[j].Name
		}
		return out[i].Namespace < out[j].Namespace
	})
	if a.mongo != nil && a.mongo.Healthy() {
		_ = a.mongo.SaveServiceAccounts(ctx, out)
	}
	return out, nil
}

func classifyServiceAccount(ns, name string, used, rb, crb []string) (string, string) {
	if name == "default" {
		return "builtin_default", "high"
	}
	if ns == "kube-system" || ns == "kube-public" || ns == "kube-node-lease" {
		if strings.Contains(name, "controller") || strings.Contains(name, "scheduler") || strings.Contains(name, "autoscaler") {
			return "kubernetes_system_or_addon", "medium"
		}
		return "kubernetes_system", "medium"
	}
	if strings.Contains(ns, "argocd") || strings.Contains(ns, "flux") || strings.Contains(name, "controller") || strings.Contains(name, "operator") {
		return "managed_addon_or_controller", "medium"
	}
	if len(used) > 0 {
		return "application", "medium"
	}
	if len(rb) > 0 || len(crb) > 0 {
		return "rbac_bound_unused", "low"
	}
	return "unknown", "low"
}

func suggestActorGroup(cat string) string {
	switch cat {
	case "kubernetes_system", "kubernetes_system_or_addon", "managed_addon_or_controller":
		return "cluster_controllers"
	case "application", "rbac_bound_unused":
		return "automation_serviceaccounts"
	default:
		return ""
	}
}

func appendUnique(xs []string, x string) []string {
	for _, y := range xs {
		if y == x {
			return xs
		}
	}
	return append(xs, x)
}
func sortStrings(xs []string) []string { sort.Strings(xs); return xs }
