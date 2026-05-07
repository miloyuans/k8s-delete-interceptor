package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

func loadBootstrapConfig(path string) (*RuntimeConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return defaultRuntimeConfig(), nil
	}
	cfg := &RuntimeConfig{}
	if strings.HasSuffix(path, ".json") {
		if err := json.Unmarshal(b, cfg); err != nil {
			return nil, err
		}
	} else {
		if err := yaml.Unmarshal(b, cfg); err != nil {
			return nil, err
		}
	}
	applyRuntimeDefaults(cfg)
	return cfg, validateRuntimeConfig(cfg)
}

func applyRuntimeDefaults(c *RuntimeConfig) {
	if c.ClusterName == "" {
		c.ClusterName = envOr("CLUSTER_NAME", "default-cluster")
	}
	if !c.Enabled {
		c.Enabled = true
	}
	if c.Version == 0 {
		c.Version = 1
	}
	if c.Storage.RootDir == "" {
		c.Storage.RootDir = envOr("STATE_DIR", "/var/lib/k8s-delete-interceptor")
	}
	if c.Defaults.Unmatched.Create == "" {
		c.Defaults.Unmatched.Create = DecisionAuditOnly
	}
	if c.Defaults.Unmatched.Update == "" {
		c.Defaults.Unmatched.Update = DecisionAuditOnly
	}
	if c.Defaults.Unmatched.Delete == "" {
		c.Defaults.Unmatched.Delete = DecisionAuditOnly
	}
	if !c.Audit.Enabled {
		c.Audit.Enabled = true
	}
	if !c.Audit.PreferMongo {
		c.Audit.PreferMongo = true
	}
	if c.SystemProtection.SystemResourceLabel == "" {
		c.SystemProtection.SystemResourceLabel = "k8s-delete-interceptor.io/system-resource"
	}
	if c.SystemProtection.InternalMongoResourceValue == "" {
		c.SystemProtection.InternalMongoResourceValue = "internal-mongodb"
	}
	if !c.SystemProtection.Enabled {
		c.SystemProtection.Enabled = true
	}
	if len(c.ResourceScopes) == 0 {
		c.ResourceScopes = defaultScopes()
	}
	if len(c.ActorGroups) == 0 {
		c.ActorGroups = defaultActors()
	}
	if len(c.NotificationTemplates) == 0 {
		c.NotificationTemplates = defaultTemplates()
	}
	if len(c.Rules) == 0 {
		c.Rules = defaultRules()
	}
	if len(c.DataSources) == 0 {
		c.DataSources = []DataSource{{
			ID: "internal_mongo_default", Name: "内置 MongoDB", Type: "internal_mongodb", Enabled: true, Active: true,
			URIEnv: "MONGO_URI", Database: envOr("MONGO_DATABASE", "k8s_delete_interceptor"), Namespace: envOr("POD_NAMESPACE", "webhook-system"), Service: "delete-interceptor-mongodb", ReplicaSet: "rs0",
		}}
	}
	if c.Web.SiteName == "" {
		c.Web = defaultWebSettings()
	}
	if c.Web.DefaultTimezone == "" {
		c.Web.DefaultTimezone = envOr("WEB_DEFAULT_TIMEZONE", "Asia/Shanghai")
	}
	if c.Web.SiteIcon == "" {
		c.Web.SiteIcon = "⎈"
	}
	if len(c.WebRoles) == 0 {
		c.WebRoles = defaultWebRoles()
	}
	if len(c.WebUsers) == 0 {
		c.WebUsers = defaultWebUsers()
	}
	ensureBuiltInConfigObjects(c)
}

func ensureBuiltInConfigObjects(c *RuntimeConfig) {
	if c == nil {
		return
	}
	ensureScope := func(scope ResourceScope) {
		for i := range c.ResourceScopes {
			if c.ResourceScopes[i].ID == scope.ID {
				return
			}
		}
		c.ResourceScopes = append(c.ResourceScopes, scope)
	}
	ensureTemplate := func(tpl NotificationTemplate) {
		for i := range c.NotificationTemplates {
			if c.NotificationTemplates[i].ID == tpl.ID {
				return
			}
		}
		c.NotificationTemplates = append(c.NotificationTemplates, tpl)
	}
	ensureScope(ResourceScope{ID: "internal_mongo_assets", Name: "内置 Mongo 系统资产", Enabled: true, APIGroups: []string{"*"}, Resources: []string{"*"}, Kinds: []string{"*"}, Namespaces: []string{"*"}, Names: []string{"delete-interceptor-mongodb*", "*mongodb*"}})
	ensureTemplate(NotificationTemplate{ID: "tpl_create_notify", Name: "重要创建通知", Channel: "telegram", ParseMode: "Markdown", Enabled: true, Body: "🆕 *K8s 创建操作审计通知*\n动作: ✅ 放行\n集群: `{{.cluster}}`\n资源类型: `{{.kind}}`\n资源名称: `{{.name}}`\n命名空间: `{{.namespace}}`\n用户: {{.actor_display}}\n操作: 创建\n原因: {{.reason}}\n请求ID: `{{.request_uid}}`\n{{.time}}"})
	// Migrate legacy built-in templates that embedded fixed Web event URLs.
	// Admission event notifications should rely on Telegram callback buttons instead;
	// only config-change approval messages carry an optional Web review link.
	if strings.ToLower(envOr("TEMPLATE_AUTO_MIGRATE_WEB_LINKS", "true")) != "false" {
		builtins := map[string]NotificationTemplate{}
		for _, tpl := range defaultNotificationTemplates() {
			builtins[tpl.ID] = tpl
		}
		for i := range c.NotificationTemplates {
			if tpl, ok := builtins[c.NotificationTemplates[i].ID]; ok && strings.Contains(c.NotificationTemplates[i].Body, "event_url") {
				c.NotificationTemplates[i].Name = tpl.Name
				c.NotificationTemplates[i].Channel = tpl.Channel
				c.NotificationTemplates[i].ParseMode = tpl.ParseMode
				c.NotificationTemplates[i].Body = tpl.Body
			}
		}
	}
	for i := range c.ResourceScopes {
		if scopeNeedsCoreAPIGroup(c.ResourceScopes[i]) && !stringSliceContains(c.ResourceScopes[i].APIGroups, "") && !stringSliceContains(c.ResourceScopes[i].APIGroups, "*") {
			c.ResourceScopes[i].APIGroups = append([]string{""}, c.ResourceScopes[i].APIGroups...)
		}
	}
	for i := range c.Rules {
		if c.Rules[i].ID == "internal_mongo_delete_protection" {
			// Older generated configs used broad core scopes here, which would block
			// all normal deletes once core API group matching is fixed. Keep this rule
			// only for Mongo assets; label-based hard protection is still enforced in code.
			c.Rules[i].ScopeIDs = []string{"internal_mongo_assets"}
			c.Rules[i].Reason = "内置 MongoDB 属于系统核心数据源，禁止直接删除"
		}
	}
}

func scopeNeedsCoreAPIGroup(s ResourceScope) bool {
	coreResources := map[string]bool{"configmaps": true, "secrets": true, "services": true, "namespaces": true, "persistentvolumeclaims": true, "pods": true}
	coreKinds := map[string]bool{"ConfigMap": true, "Secret": true, "Service": true, "Namespace": true, "PersistentVolumeClaim": true, "Pod": true}
	for _, r := range s.Resources {
		if coreResources[strings.ToLower(strings.TrimSpace(r))] {
			return true
		}
	}
	for _, k := range s.Kinds {
		if coreKinds[strings.TrimSpace(k)] {
			return true
		}
	}
	return false
}

func stringSliceContains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

func validateRuntimeConfig(c *RuntimeConfig) error {
	if c == nil {
		return errors.New("nil runtime config")
	}
	applyRuntimeDefaults(c)
	active := 0
	ids := map[string]string{}
	for _, ds := range c.DataSources {
		if ds.Enabled && ds.Active {
			active++
		}
	}
	if active > 1 {
		return fmt.Errorf("only one data source may be active, got %d", active)
	}
	for _, s := range c.ResourceScopes {
		if s.ID == "" {
			return errors.New("resource scope id is required")
		}
		if old := ids["scope:"+s.ID]; old != "" {
			return fmt.Errorf("duplicate scope id %s (%s)", s.ID, old)
		}
		ids["scope:"+s.ID] = s.Name
	}
	for _, a := range c.ActorGroups {
		if a.ID == "" {
			return errors.New("actor group id is required")
		}
		if old := ids["actor:"+a.ID]; old != "" {
			return fmt.Errorf("duplicate actor group id %s (%s)", a.ID, old)
		}
		ids["actor:"+a.ID] = a.Name
	}
	for _, t := range c.NotificationTemplates {
		if t.ID == "" {
			return errors.New("template id is required")
		}
		if old := ids["template:"+t.ID]; old != "" {
			return fmt.Errorf("duplicate template id %s (%s)", t.ID, old)
		}
		ids["template:"+t.ID] = t.Name
	}
	roleIDs := map[string]bool{}
	for _, r := range c.WebRoles {
		if r.ID == "" {
			return errors.New("web role id is required")
		}
		if roleIDs[r.ID] {
			return fmt.Errorf("duplicate web role id %s", r.ID)
		}
		roleIDs[r.ID] = true
	}
	userIDs := map[string]bool{}
	for _, u := range c.WebUsers {
		if strings.TrimSpace(u.Username) == "" {
			return errors.New("web username is required")
		}
		lu := strings.ToLower(strings.TrimSpace(u.Username))
		if userIDs[lu] {
			return fmt.Errorf("duplicate web username %s", u.Username)
		}
		userIDs[lu] = true
		for _, role := range u.Roles {
			if !roleIDs[role] {
				return fmt.Errorf("web user %s references missing role %s", u.Username, role)
			}
		}
	}
	for _, r := range c.Rules {
		if r.ID == "" {
			return errors.New("rule id is required")
		}
		if r.Decision == "" {
			return fmt.Errorf("rule %s decision is required", r.ID)
		}
		for _, sid := range r.ScopeIDs {
			if ids["scope:"+sid] == "" {
				return fmt.Errorf("rule %s references missing scope %s", r.ID, sid)
			}
		}
		for _, aid := range r.ActorGroupIDs {
			if ids["actor:"+aid] == "" {
				return fmt.Errorf("rule %s references missing actor group %s", r.ID, aid)
			}
		}
		if r.Notify.Enabled && r.Notify.TemplateID != "" && ids["template:"+r.Notify.TemplateID] == "" {
			return fmt.Errorf("rule %s references missing template %s", r.ID, r.Notify.TemplateID)
		}
	}
	return nil
}

func defaultRuntimeConfig() *RuntimeConfig {
	c := &RuntimeConfig{
		Version:          1,
		ClusterName:      envOr("CLUSTER_NAME", "default-cluster"),
		Enabled:          true,
		Storage:          SharedStorageConfig{RootDir: envOr("STATE_DIR", "/var/lib/k8s-delete-interceptor"), CleanupSyncedAfter: "7d", WarnUsagePercent: 75, CriticalUsagePercent: 90, CompressSyncedEvents: true, UseSharedApprovalFallback: true},
		Defaults:         DefaultPolicy{Unmatched: DecisionByOperation{Create: DecisionAuditOnly, Update: DecisionAuditOnly, Delete: DecisionAuditOnly}},
		Audit:            AuditConfig{Enabled: true, PreferMongo: true, RecordNoiseEvents: true},
		Rollback:         RollbackConfig{Enabled: true, CreateDeleteRollback: false, RequireDryRun: true},
		SystemProtection: SystemProtectionConfig{Enabled: true, SystemResourceLabel: "k8s-delete-interceptor.io/system-resource", InternalMongoResourceValue: "internal-mongodb", ProtectedNamespaces: []string{"webhook-system"}, AllowWebhookSelfMaintenance: true},
		Web:              defaultWebSettings(),
		WebRoles:         defaultWebRoles(),
		WebUsers:         defaultWebUsers(),
		DataSources:      []DataSource{{ID: "internal_mongo_default", Name: "内置 MongoDB", Type: "internal_mongodb", Enabled: true, Active: true, URIEnv: "MONGO_URI", Database: envOr("MONGO_DATABASE", "k8s_delete_interceptor"), Namespace: envOr("POD_NAMESPACE", "webhook-system"), Service: "delete-interceptor-mongodb", ReplicaSet: "rs0"}},
		ResourceScopes:   defaultScopes(), ActorGroups: defaultActors(), NotificationTemplates: defaultTemplates(), Rules: defaultRules(),
	}
	return c
}

func defaultScopes() []ResourceScope {
	return []ResourceScope{
		{ID: "internal_mongo_assets", Name: "内置 Mongo 系统资产", Enabled: true, APIGroups: []string{"*"}, Resources: []string{"*"}, Kinds: []string{"*"}, Namespaces: []string{"*"}, Names: []string{"delete-interceptor-mongodb*", "*mongodb*"}},
		{ID: "workload_core", Name: "核心工作负载", Enabled: true, APIGroups: []string{"apps"}, Resources: []string{"deployments", "statefulsets", "daemonsets"}, Kinds: []string{"Deployment", "StatefulSet", "DaemonSet"}, Namespaces: []string{"*"}, Names: []string{"*"}},
		{ID: "service_network_core", Name: "服务与入口", Enabled: true, APIGroups: []string{"", "networking.k8s.io"}, Resources: []string{"services", "ingresses"}, Kinds: []string{"Service", "Ingress"}, Namespaces: []string{"*"}, Names: []string{"*"}},
		{ID: "config_secret_core", Name: "配置与密钥", Enabled: true, APIGroups: []string{""}, Resources: []string{"configmaps", "secrets", "persistentvolumeclaims"}, Kinds: []string{"ConfigMap", "Secret", "PersistentVolumeClaim"}, Namespaces: []string{"*"}, Names: []string{"*"}},
		{ID: "cluster_core", Name: "集群关键资源", Enabled: true, APIGroups: []string{"", "storage.k8s.io"}, Resources: []string{"namespaces", "storageclasses"}, Kinds: []string{"Namespace", "StorageClass"}, Namespaces: []string{"*"}, Names: []string{"*"}},
		{ID: "pod_lifecycle", Name: "Pod 生命周期", Enabled: true, APIGroups: []string{""}, Resources: []string{"pods"}, Kinds: []string{"Pod"}, Namespaces: []string{"*"}, Names: []string{"*"}},
	}
}

func defaultActors() []ActorGroup {
	return []ActorGroup{
		{ID: "human_admins", Name: "人工管理员", Enabled: true, Users: []string{"system:admin", "kubernetes-admin", "regex:.*@.*"}},
		{ID: "cluster_controllers", Name: "集群控制器", Enabled: true, Users: []string{"system:kube-controller-manager", "regex:^system:node:.+"}, ServiceAccounts: []string{"regex:^system:serviceaccount:kube-system:.*controller.*", "regex:^system:serviceaccount:kube-system:cluster-autoscaler$", "regex:^system:serviceaccount:karpenter:.*$"}},
		{ID: "automation_serviceaccounts", Name: "自动化服务账号", Enabled: true, ServiceAccounts: []string{"regex:^system:serviceaccount:argocd:.*", "regex:^system:serviceaccount:flux-system:.*", "regex:^system:serviceaccount:prod:.*"}},
	}
}

func defaultNotificationTemplates() []NotificationTemplate {
	return defaultTemplates()
}

func defaultTemplates() []NotificationTemplate {
	return []NotificationTemplate{
		{ID: "tpl_delete_approval", Name: "删除审批通知", Channel: "telegram", ParseMode: "Markdown", Enabled: true, Body: "🚨 *K8s 删除操作审批通知*\n动作: ⏳ 待审批\n集群: `{{.cluster}}`\n资源类型: `{{.kind}}`\n资源名称: `{{.name}}`\n命名空间: `{{.namespace}}`\n用户: {{.actor_display}}\n操作: 删除\n规则: `{{.rule_name}}`\n原因: {{.reason}}\n审批人: {{.approvers_mentions}}\n请求ID: `{{.request_uid}}`\n{{.time}}"},
		{ID: "tpl_update_notify", Name: "重要更新通知", Channel: "telegram", ParseMode: "Markdown", Enabled: true, Body: "📝 *K8s 更新操作审计通知*\n动作: ✅ 放行\n集群: `{{.cluster}}`\n资源类型: `{{.kind}}`\n资源名称: `{{.name}}`\n命名空间: `{{.namespace}}`\n用户: {{.actor_display}}\n操作: 更新\n变更详情: {{.change_summary}}\n请求ID: `{{.request_uid}}`\n{{.time}}"},
		{ID: "tpl_create_notify", Name: "重要创建通知", Channel: "telegram", ParseMode: "Markdown", Enabled: true, Body: "🆕 *K8s 创建操作审计通知*\n动作: ✅ 放行\n集群: `{{.cluster}}`\n资源类型: `{{.kind}}`\n资源名称: `{{.name}}`\n命名空间: `{{.namespace}}`\n用户: {{.actor_display}}\n操作: 创建\n原因: {{.reason}}\n请求ID: `{{.request_uid}}`\n{{.time}}"},
		{ID: "tpl_block", Name: "拦截通知", Channel: "telegram", ParseMode: "Markdown", Enabled: true, Body: "⛔ *K8s 操作拦截通知*\n动作: ⛔ 拦截\n集群: `{{.cluster}}`\n资源类型: `{{.kind}}`\n资源名称: `{{.name}}`\n命名空间: `{{.namespace}}`\n用户: {{.actor_display}}\n操作: {{.operation_cn}}\n原因: {{.reason}}\n请求ID: `{{.request_uid}}`\n{{.time}}"},
	}
}

func defaultRules() []PolicyRule {
	return []PolicyRule{
		{ID: "internal_mongo_delete_protection", Name: "内置 Mongo 硬保护", Enabled: true, Priority: 1, ScopeIDs: []string{"internal_mongo_assets"}, Operations: []string{"DELETE"}, Decision: DecisionBlock, Reason: "内置 MongoDB 属于系统核心数据源，禁止直接删除", Notify: NotificationBinding{Enabled: true, TemplateID: "tpl_block"}},
		{ID: "pod_controller_lifecycle_audit", Name: "控制器 Pod 生命周期只审计", Enabled: true, Priority: 20, ScopeIDs: []string{"pod_lifecycle"}, Operations: []string{"DELETE"}, ActorGroupIDs: []string{"cluster_controllers"}, Decision: DecisionAuditOnly, Reason: "控制器或节点维护 Pod 生命周期，仅审计", ControllerSafe: ControllerSafeRule{RequireOwnerReference: true, RequireControllerOwnerReference: true, AllowedOwnerKinds: []string{"ReplicaSet", "DaemonSet", "StatefulSet", "Job"}, RequireNodeUserMatchesPodNode: true}},
		{ID: "workload_restart_silent", Name: "工作负载重启只审计", Enabled: true, Priority: 30, ScopeIDs: []string{"workload_core"}, Operations: []string{"UPDATE"}, ChangeClasses: []string{"workload_restart", "no_effective_change", "managed_fields_only", "status_only", "metadata_only"}, Decision: DecisionAuditOnly, Reason: "低风险或无有效变化更新，仅审计"},
		{ID: "core_delete_approval", Name: "核心资源删除审批", Enabled: true, Priority: 50, ScopeIDs: []string{"workload_core", "service_network_core", "config_secret_core", "cluster_core"}, Operations: []string{"DELETE"}, Decision: DecisionRequireApproval, Reason: "核心资源删除需要审批", Notify: NotificationBinding{Enabled: true, TemplateID: "tpl_delete_approval"}, Approval: ApprovalBinding{Enabled: true, Mode: "both", TTLSeconds: 300, FailWhenStoreDown: true}, Rollback: RollbackBinding{Enabled: true, Mode: RollbackRestoreOldObject, ShowInWeb: true, ShowInTelegram: true}},
		{ID: "important_update_notify", Name: "重要资源有效更新通知", Enabled: true, Priority: 60, ScopeIDs: []string{"workload_core", "service_network_core", "config_secret_core"}, Operations: []string{"UPDATE"}, ChangeClasses: []string{"image_changed", "env_changed", "volume_changed", "scale_only", "service_selector_changed", "service_port_changed", "ingress_backend_changed", "configmap_data_changed", "secret_data_changed", "spec_changed"}, Decision: DecisionAllowNotify, Reason: "重要资源发生有效更新，已记录回滚和 YAML", Notify: NotificationBinding{Enabled: true, TemplateID: "tpl_update_notify"}, Rollback: RollbackBinding{Enabled: true, Mode: RollbackRestoreOldObject, ShowInWeb: true, ShowInTelegram: false}},
		{ID: "important_create_notify", Name: "重要资源创建记录", Enabled: true, Priority: 70, ScopeIDs: []string{"workload_core", "service_network_core", "config_secret_core"}, Operations: []string{"CREATE"}, Decision: DecisionAllowNotify, Reason: "重要资源创建已放行并记录", Notify: NotificationBinding{Enabled: true, TemplateID: "tpl_create_notify"}, Rollback: RollbackBinding{Enabled: true, Mode: RollbackDeleteCreatedObject, ShowInWeb: true, ShowInTelegram: false}},
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
