package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

func (a *App) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/validate", a.handleValidate)
	mux.HandleFunc("/telegram/webhook", a.handleTelegramWebhook)
	mux.HandleFunc("/", a.handleIndex)
	mux.HandleFunc("/api/health", a.handleHealth)
	mux.HandleFunc("/api/auth/login", a.handleLogin)
	mux.HandleFunc("/api/me", a.auth(a.handleMe))
	mux.HandleFunc("/api/settings", a.auth(a.handleSettings))
	mux.HandleFunc("/api/metadata", a.require(PermDashboardRead, a.handleMetadata))
	mux.HandleFunc("/api/events", a.require(PermEventsRead, a.handleEvents))
	mux.HandleFunc("/api/serviceaccounts", a.require(PermSARead, a.handleServiceAccounts))
	mux.HandleFunc("/api/serviceaccounts/scan", a.require(PermSAScan, a.handleServiceAccountScan))
	mux.HandleFunc("/api/serviceaccounts/mount", a.require(PermSAMount, a.handleServiceAccountMount))
	mux.HandleFunc("/api/config", a.require(PermConfigRead, a.handleConfig))
	mux.HandleFunc("/api/config/publish", a.require(PermConfigWrite, a.handlePublishConfig))
	mux.HandleFunc("/api/config/export", a.require(PermConfigRead, a.handleConfigExport))
	mux.HandleFunc("/api/config/versions", a.require(PermConfigRead, a.handleConfigVersions))
	mux.HandleFunc("/api/config/restore", a.require(PermConfigRestore, a.handleConfigRestore))
	mux.HandleFunc("/api/config/changes", a.require(PermConfigRead, a.handleConfigChanges))
	mux.HandleFunc("/api/config/audits", a.require(PermConfigRead, a.handleConfigAudits))
	mux.HandleFunc("/api/config/changes/", a.handleConfigChangeAction)
	mux.HandleFunc("/api/users", a.require(PermUsersWrite, a.handleUsers))
	mux.HandleFunc("/api/users/", a.require(PermUsersWrite, a.handleUsers))
	mux.HandleFunc("/api/roles", a.auth(a.handleRoles))
	mux.HandleFunc("/api/roles/", a.auth(a.handleRoles))
	mux.HandleFunc("/api/datasources", a.auth(a.handleDatasources))
	mux.HandleFunc("/api/datasources/", a.auth(a.handleDatasources))
	mux.HandleFunc("/api/datasources/test", a.require(PermDatasourceWrite, a.handleDatasourceTest))
	mux.HandleFunc("/api/telegram", a.auth(a.handleTelegram))
	mux.HandleFunc("/api/telegram/bots", a.auth(a.handleTelegramBots))
	mux.HandleFunc("/api/telegram/bots/", a.auth(a.handleTelegramBots))
	mux.HandleFunc("/api/telegram/chats", a.auth(a.handleTelegramChats))
	mux.HandleFunc("/api/telegram/chats/", a.auth(a.handleTelegramChats))
	mux.HandleFunc("/api/telegram/users", a.auth(a.handleTelegramUsers))
	mux.HandleFunc("/api/telegram/users/", a.auth(a.handleTelegramUsers))
	mux.HandleFunc("/api/telegram/test", a.require(PermTelegramWrite, a.handleTelegramTest))
	mux.HandleFunc("/api/rules", a.auth(a.handleRules))
	mux.HandleFunc("/api/rules/", a.auth(a.handleRules))
	mux.HandleFunc("/api/rollback/", a.require(PermRollbackExecute, a.handleRollback))
}

func (a *App) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(indexHTML))
}

func (a *App) handleHealth(w http.ResponseWriter, r *http.Request) {
	cfg := a.Config()
	version := int64(0)
	cluster := ""
	settings := defaultWebSettings()
	if cfg != nil {
		version = cfg.Version
		cluster = cfg.ClusterName
		settings = cfg.Web
	}
	mongoStatus := "not_configured"
	if a.mongo != nil {
		mongoStatus = a.mongo.Test(r.Context())
	}
	writeJSON(w, map[string]any{"ok": true, "cluster": cluster, "config_version": version, "mongo": mongoStatus, "state_dir": a.local.Root(), "auth_required": a.authRequired(), "settings": settings})
}

func (a *App) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Token    string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if body.Token != "" && a.adminToken != "" && body.Token == a.adminToken {
		writeJSON(w, map[string]any{"ok": true, "token": body.Token, "user": AuthUser{Username: "token-admin", DisplayName: "WEB_ADMIN_TOKEN", Roles: []string{"superadmin"}, Permissions: []string{PermAll}, SuperAdmin: true, TokenMode: "admin_token"}})
		return
	}
	cfg := a.Config()
	if cfg == nil {
		http.Error(w, "runtime config unavailable", 500)
		return
	}
	for _, u := range cfg.WebUsers {
		if strings.EqualFold(u.Username, strings.TrimSpace(body.Username)) && u.Enabled && verifyPassword(u, body.Password) {
			tok, err := a.issueToken(u)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			writeJSON(w, map[string]any{"ok": true, "token": tok, "user": a.authUserFromConfigUser(u)})
			return
		}
	}
	http.Error(w, "invalid username/password", http.StatusUnauthorized)
}

func (a *App) handleMe(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{"ok": true, "authenticated": true, "auth_required": a.authRequired(), "user": userFromContext(r.Context())})
}

func (a *App) handleSettings(w http.ResponseWriter, r *http.Request) {
	cfg := a.Config()
	if cfg == nil {
		http.Error(w, "runtime config unavailable", 500)
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, cfg.Web)
	case http.MethodPut, http.MethodPost:
		if !userFromContext(r.Context()).Can(PermSettingsWrite) {
			http.Error(w, "forbidden: need permission "+PermSettingsWrite, 403)
			return
		}
		var s WebSettings
		if err := json.NewDecoder(r.Body).Decode(&s); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		next := cloneConfig(cfg)
		next.Web = s
		cr, applied, err := a.proposeConfigChange(r.Context(), *next, "settings", "更新站点名称、图标、默认时区或主题", userFromContext(r.Context()), true)
		writeChangeResponse(w, cr, applied, err)
	default:
		http.Error(w, "method not allowed", 405)
	}
}

func (a *App) handleMetadata(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("refresh") == "1" || strings.EqualFold(r.URL.Query().Get("refresh"), "true") {
		if !userFromContext(r.Context()).Can(PermSAScan) {
			http.Error(w, "forbidden: need permission "+PermSAScan, 403)
			return
		}
		md, err := a.RefreshMetadata(r.Context(), true)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		writeJSON(w, md)
		return
	}
	writeJSON(w, a.BuildMetadata(r.Context()))
}

func (a *App) handleEvents(w http.ResponseWriter, r *http.Request) {
	q := parseEventQuery(r)
	if a.mongo != nil && a.mongo.Healthy() {
		if events, err := a.mongo.ListEventsByQuery(r.Context(), q); err == nil {
			writeJSON(w, events)
			return
		}
	}
	events, err := a.local.ListRecentEventsByQuery(q)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, events)
}

func (a *App) handleServiceAccounts(w http.ResponseWriter, r *http.Request) {
	ns := strings.TrimSpace(r.URL.Query().Get("namespace"))
	items := a.cachedServiceAccounts(r.Context(), ns, parseLimit(r, 1000))
	writeJSON(w, items)
}

func (a *App) handleServiceAccountScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	md, err := a.RefreshMetadata(r.Context(), true)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, map[string]any{"ok": true, "count": len(md.ServiceAccounts), "items": md.ServiceAccounts, "metadata": md})
}

func (a *App) handleServiceAccountMount(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	var body struct {
		Namespace    string `json:"namespace"`
		Name         string `json:"name"`
		UserString   string `json:"user_string"`
		ActorGroupID string `json:"actor_group_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if body.UserString == "" {
		body.UserString = "system:serviceaccount:" + body.Namespace + ":" + body.Name
	}
	cfg := a.Config()
	next := cloneConfig(cfg)
	found := false
	for i := range next.ActorGroups {
		if next.ActorGroups[i].ID == body.ActorGroupID {
			next.ActorGroups[i].ServiceAccounts = appendUnique(next.ActorGroups[i].ServiceAccounts, body.UserString)
			found = true
		}
	}
	if !found {
		http.Error(w, "actor_group_id not found", 400)
		return
	}
	cr, applied, err := a.proposeConfigChange(r.Context(), *next, "sa_mount", "挂载 ServiceAccount 到安全策略 ActorGroup", userFromContext(r.Context()), false)
	writeChangeResponse(w, cr, applied, err)
}

func (a *App) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", 405)
		return
	}
	writeJSON(w, a.Config())
}

func (a *App) handlePublishConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	var cfg RuntimeConfig
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	cr, applied, err := a.proposeConfigChange(r.Context(), cfg, "raw_config", "通过 JSON 编辑器提交完整运行配置", userFromContext(r.Context()), false)
	writeChangeResponse(w, cr, applied, err)
}

func (a *App) handleConfigExport(w http.ResponseWriter, r *http.Request) {
	cfg := a.Config()
	if cfg == nil {
		http.Error(w, "runtime config unavailable", 500)
		return
	}
	format := strings.ToLower(r.URL.Query().Get("format"))
	if format == "json" {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=runtime-config-v%d.json", cfg.Version))
		_ = json.NewEncoder(w).Encode(cfg)
		return
	}
	b, err := yaml.Marshal(cfg)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Content-Type", "application/yaml")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=runtime-config-v%d.yaml", cfg.Version))
	_, _ = w.Write(b)
}

func (a *App) handleConfigVersions(w http.ResponseWriter, r *http.Request) {
	limit := parseLimit(r, 100)
	if a.mongo != nil && a.mongo.Healthy() {
		if xs, err := a.mongo.ListConfigVersions(r.Context(), limit); err == nil {
			writeJSON(w, xs)
			return
		}
	}
	xs, err := a.local.ListConfigVersions(limit)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, xs)
}

func (a *App) handleConfigRestore(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	var body struct {
		Version int64 `json:"version"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	cfg, err := a.getConfigVersion(r.Context(), body.Version)
	if err != nil {
		http.Error(w, err.Error(), 404)
		return
	}
	cr, applied, err := a.proposeConfigChange(r.Context(), *cfg, "restore", fmt.Sprintf("恢复到历史配置版本 v%d", body.Version), userFromContext(r.Context()), false)
	writeChangeResponse(w, cr, applied, err)
}

func (a *App) getConfigVersion(ctx context.Context, version int64) (*RuntimeConfig, error) {
	if a.mongo != nil && a.mongo.Healthy() {
		if cfg, err := a.mongo.GetConfigVersion(ctx, version); err == nil {
			return cfg, nil
		}
	}
	return a.local.GetConfigVersion(version)
}

func (a *App) handleConfigChanges(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", 405)
		return
	}
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	xs, err := a.listConfigChanges(r.Context(), status, parseLimit(r, 100))
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, xs)
}

func (a *App) handleConfigAudits(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", 405)
		return
	}
	category := strings.TrimSpace(r.URL.Query().Get("category"))
	xs, err := a.listConfigAudits(r.Context(), category, parseLimit(r, 100))
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, xs)
}

func (a *App) handleConfigChangeAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	a.require(PermConfigApprove, func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, "/api/config/changes/")
		parts := strings.Split(strings.Trim(p, "/"), "/")
		if len(parts) != 2 {
			http.Error(w, "expected /api/config/changes/{id}/approve|reject", 400)
			return
		}
		var body struct {
			Note    string `json:"note"`
			EventID string `json:"event_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.EventID == "" {
			http.Error(w, "event_id is required to avoid concurrent approval conflicts", http.StatusConflict)
			return
		}
		currentChange, err := a.getConfigChange(r.Context(), parts[0])
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		expectedEventID := currentChange.EventID
		if expectedEventID == "" {
			expectedEventID = currentChange.ID
		}
		if body.EventID != expectedEventID {
			http.Error(w, "event_id conflict: change has been refreshed or modified", http.StatusConflict)
			return
		}
		var cr *ConfigChangeRequest
		var actionErr error
		switch parts[1] {
		case "approve":
			cr, actionErr = a.approveConfigChange(r.Context(), parts[0], body.Note, userFromContext(r.Context()))
		case "reject":
			cr, actionErr = a.rejectConfigChange(r.Context(), parts[0], body.Note, userFromContext(r.Context()))
		default:
			http.Error(w, "unknown action", 400)
			return
		}
		if actionErr != nil {
			code := 500
			if strings.Contains(actionErr.Error(), "conflict") {
				code = http.StatusConflict
			}
			http.Error(w, actionErr.Error(), code)
			return
		}
		sanitizeRuntimeConfigForResponse(&cr.Config)
		writeJSON(w, map[string]any{"ok": true, "change": cr})
	})(w, r)
}

func (a *App) handleUsers(w http.ResponseWriter, r *http.Request) {
	cfg := a.Config()
	if cfg == nil {
		http.Error(w, "runtime config unavailable", 500)
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, sanitizeUsers(cfg.WebUsers))
	case http.MethodPost, http.MethodPut:
		var body struct {
			Username    string   `json:"username"`
			DisplayName string   `json:"display_name"`
			Password    string   `json:"password"`
			Roles       []string `json:"roles"`
			Enabled     bool     `json:"enabled"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		username := body.Username
		if username == "" {
			username = strings.Trim(pathID("/api/users", r.URL.Path), "/")
		}
		if username == "" {
			http.Error(w, "username is required", 400)
			return
		}
		next := cloneConfig(cfg)
		now := time.Now().UTC()
		updated := false
		for i := range next.WebUsers {
			if strings.EqualFold(next.WebUsers[i].Username, username) {
				next.WebUsers[i].DisplayName = body.DisplayName
				next.WebUsers[i].Roles = body.Roles
				next.WebUsers[i].Enabled = body.Enabled
				next.WebUsers[i].UpdatedAt = now
				if body.Password != "" {
					next.WebUsers[i].PasswordHash = hashPassword(username, body.Password)
				}
				updated = true
			}
		}
		if !updated {
			if body.Password == "" {
				http.Error(w, "password is required for new user", 400)
				return
			}
			next.WebUsers = append(next.WebUsers, WebUser{Username: username, DisplayName: body.DisplayName, PasswordHash: hashPassword(username, body.Password), Roles: body.Roles, Enabled: body.Enabled, CreatedAt: now, UpdatedAt: now})
		}
		cr, applied, err := a.proposeConfigChange(r.Context(), *next, "users", "创建或更新 Web 用户", userFromContext(r.Context()), true)
		writeChangeResponse(w, cr, applied, err)
	case http.MethodDelete:
		username := strings.Trim(pathID("/api/users", r.URL.Path), "/")
		if username == "" {
			http.Error(w, "username is required", 400)
			return
		}
		next := cloneConfig(cfg)
		filtered := []WebUser{}
		for _, u := range next.WebUsers {
			if !strings.EqualFold(u.Username, username) {
				filtered = append(filtered, u)
			}
		}
		next.WebUsers = filtered
		cr, applied, err := a.proposeConfigChange(r.Context(), *next, "users", "删除 Web 用户", userFromContext(r.Context()), true)
		writeChangeResponse(w, cr, applied, err)
	default:
		http.Error(w, "method not allowed", 405)
	}
}

func (a *App) handleRoles(w http.ResponseWriter, r *http.Request) {
	u := userFromContext(r.Context())
	if r.Method == http.MethodGet {
		if !u.Can(PermConfigRead) && !u.Can(PermRolesWrite) {
			http.Error(w, "forbidden", 403)
			return
		}
		writeJSON(w, a.Config().WebRoles)
		return
	}
	if !u.Can(PermRolesWrite) {
		http.Error(w, "forbidden: need permission "+PermRolesWrite, 403)
		return
	}
	cfg := a.Config()
	switch r.Method {
	case http.MethodPost, http.MethodPut:
		var role WebRole
		if err := json.NewDecoder(r.Body).Decode(&role); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if role.ID == "" {
			role.ID = strings.Trim(pathID("/api/roles", r.URL.Path), "/")
		}
		next := cloneConfig(cfg)
		updated := false
		for i := range next.WebRoles {
			if next.WebRoles[i].ID == role.ID {
				if next.WebRoles[i].Builtin && !u.SuperAdmin {
					http.Error(w, "builtin role requires superadmin", 403)
					return
				}
				next.WebRoles[i] = role
				updated = true
			}
		}
		if !updated {
			next.WebRoles = append(next.WebRoles, role)
		}
		cr, applied, err := a.proposeConfigChange(r.Context(), *next, "roles", "创建或更新 Web 角色权限", u, true)
		writeChangeResponse(w, cr, applied, err)
	case http.MethodDelete:
		id := strings.Trim(pathID("/api/roles", r.URL.Path), "/")
		next := cloneConfig(cfg)
		filtered := []WebRole{}
		for _, role := range next.WebRoles {
			if role.ID != id {
				filtered = append(filtered, role)
			}
		}
		next.WebRoles = filtered
		cr, applied, err := a.proposeConfigChange(r.Context(), *next, "roles", "删除 Web 角色", u, true)
		writeChangeResponse(w, cr, applied, err)
	default:
		http.Error(w, "method not allowed", 405)
	}
}

func (a *App) handleDatasources(w http.ResponseWriter, r *http.Request) {
	u := userFromContext(r.Context())
	cfg := a.Config()
	if cfg == nil {
		http.Error(w, "runtime config unavailable", 500)
		return
	}
	id := strings.Trim(pathID("/api/datasources", r.URL.Path), "/")
	if r.Method == http.MethodGet {
		if !u.Can(PermConfigRead) {
			http.Error(w, "forbidden", 403)
			return
		}
		if id != "" {
			for _, ds := range cfg.DataSources {
				if ds.ID == id {
					writeJSON(w, ds)
					return
				}
			}
			http.NotFound(w, r)
			return
		}
		writeJSON(w, cfg.DataSources)
		return
	}
	if !u.Can(PermDatasourceWrite) {
		http.Error(w, "forbidden: need permission "+PermDatasourceWrite, 403)
		return
	}
	next := cloneConfig(cfg)
	switch r.Method {
	case http.MethodPost, http.MethodPut:
		var raw json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		trim := strings.TrimSpace(string(raw))
		if strings.HasPrefix(trim, "[") {
			var items []DataSource
			if err := json.Unmarshal(raw, &items); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			next.DataSources = normalizeDataSources(items)
		} else {
			var ds DataSource
			if err := json.Unmarshal(raw, &ds); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			if ds.ID == "" {
				ds.ID = id
			}
			if ds.ID == "" {
				http.Error(w, "datasource id is required", 400)
				return
			}
			if ds.Name == "" {
				ds.Name = ds.ID
			}
			if ds.Type == "" {
				ds.Type = "external_mongodb"
			}
			if ds.Database == "" {
				ds.Database = "k8s_delete_interceptor"
			}
			next.DataSources = upsertDataSource(next.DataSources, ds)
			next.DataSources = normalizeDataSources(next.DataSources)
		}
		cr, applied, err := a.proposeConfigChange(r.Context(), *next, "datasources", "更新数据源配置", u, true)
		writeChangeResponse(w, cr, applied, err)
	case http.MethodDelete:
		if id == "" {
			http.Error(w, "datasource id is required", 400)
			return
		}
		items := []DataSource{}
		for _, ds := range next.DataSources {
			if ds.ID != id {
				items = append(items, ds)
			}
		}
		next.DataSources = normalizeDataSources(items)
		cr, applied, err := a.proposeConfigChange(r.Context(), *next, "datasources", "删除数据源配置", u, true)
		writeChangeResponse(w, cr, applied, err)
	default:
		http.Error(w, "method not allowed", 405)
	}
}

func upsertDataSource(items []DataSource, ds DataSource) []DataSource {
	for i := range items {
		if items[i].ID == ds.ID {
			items[i] = ds
			return items
		}
	}
	return append(items, ds)
}

func normalizeDataSources(items []DataSource) []DataSource {
	activeSet := false
	for i := range items {
		items[i].ID = strings.TrimSpace(items[i].ID)
		items[i].Name = strings.TrimSpace(items[i].Name)
		items[i].Type = strings.TrimSpace(items[i].Type)
		if items[i].Type == "" {
			items[i].Type = "external_mongodb"
		}
		if items[i].Database == "" {
			items[i].Database = "k8s_delete_interceptor"
		}
		if items[i].Enabled && items[i].Active {
			if activeSet {
				items[i].Active = false
			} else {
				activeSet = true
			}
		}
	}
	return items
}

func (a *App) handleTelegram(w http.ResponseWriter, r *http.Request) {
	u := userFromContext(r.Context())
	cfg := a.Config()
	if cfg == nil {
		http.Error(w, "runtime config unavailable", 500)
		return
	}
	if r.Method == http.MethodGet {
		if !u.Can(PermConfigRead) {
			http.Error(w, "forbidden", 403)
			return
		}
		writeJSON(w, sanitizeTelegram(cfg.Telegram))
		return
	}
	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	if !u.Can(PermTelegramWrite) {
		http.Error(w, "forbidden: need permission "+PermTelegramWrite, 403)
		return
	}
	var tg TelegramConfig
	if err := json.NewDecoder(r.Body).Decode(&tg); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	preserveTelegramSecrets(&tg, cfg.Telegram)
	next := cloneConfig(cfg)
	next.Telegram = tg
	cr, applied, err := a.proposeConfigChange(r.Context(), *next, "telegram", "更新 Telegram 通知与审批人配置", u, true)
	writeChangeResponse(w, cr, applied, err)
}

func (a *App) handleTelegramTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	cfg := a.Config()
	if cfg == nil {
		http.Error(w, "runtime config unavailable", 500)
		return
	}
	var body struct {
		BotID   string `json:"bot_id"`
		ChatID  string `json:"chat_id"`
		Message string `json:"message"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	bot := findTelegramBot(cfg, body.BotID)
	if bot == nil || !bot.Enabled {
		http.Error(w, "telegram bot not found or disabled", 400)
		return
	}
	chatID := strings.TrimSpace(body.ChatID)
	if chatID == "" {
		for _, c := range cfg.Telegram.Chats {
			if c.Enabled && c.BotID == body.BotID {
				chatID = c.ChatID
				break
			}
		}
	}
	if chatID == "" {
		http.Error(w, "chat_id is required", 400)
		return
	}
	token := bot.Token
	if token == "" && bot.TokenEnv != "" {
		token = envOr(bot.TokenEnv, "")
	}
	if token == "" {
		http.Error(w, "telegram token is empty", 400)
		return
	}
	msg := body.Message
	if msg == "" {
		msg = "K8s Delete Interceptor Telegram test message"
	}
	if err := sendTelegram(r.Context(), token, chatID, msg, "", nil); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

func sanitizeTelegram(tg TelegramConfig) TelegramConfig {
	out := tg
	for i := range out.Bots {
		if out.Bots[i].Token != "" {
			out.Bots[i].Token = "********"
		}
	}
	return out
}

func preserveTelegramSecrets(next *TelegramConfig, cur TelegramConfig) {
	if next == nil {
		return
	}
	byID := map[string]TelegramBot{}
	for _, b := range cur.Bots {
		byID[b.ID] = b
	}
	for i := range next.Bots {
		if next.Bots[i].Token == "" || next.Bots[i].Token == "********" {
			if old, ok := byID[next.Bots[i].ID]; ok {
				next.Bots[i].Token = old.Token
			}
		}
	}
}

func (a *App) handleTelegramBots(w http.ResponseWriter, r *http.Request) {
	a.handleTelegramResource(w, r, "bots")
}

func (a *App) handleTelegramChats(w http.ResponseWriter, r *http.Request) {
	a.handleTelegramResource(w, r, "chats")
}

func (a *App) handleTelegramUsers(w http.ResponseWriter, r *http.Request) {
	a.handleTelegramResource(w, r, "users")
}

func (a *App) handleTelegramResource(w http.ResponseWriter, r *http.Request, kind string) {
	u := userFromContext(r.Context())
	cfg := a.Config()
	if cfg == nil {
		http.Error(w, "runtime config unavailable", 500)
		return
	}
	if r.Method == http.MethodGet {
		if !u.Can(PermConfigRead) {
			http.Error(w, "forbidden", 403)
			return
		}
		safe := sanitizeTelegram(cfg.Telegram)
		switch kind {
		case "bots":
			writeJSON(w, safe.Bots)
		case "chats":
			writeJSON(w, safe.Chats)
		case "users":
			writeJSON(w, safe.Users)
		}
		return
	}
	if !u.Can(PermTelegramWrite) {
		http.Error(w, "forbidden: need permission "+PermTelegramWrite, 403)
		return
	}
	id := telegramPathID(kind, r.URL.Path)
	next := cloneConfig(cfg)
	switch r.Method {
	case http.MethodPost, http.MethodPut:
		switch kind {
		case "bots":
			var item TelegramBot
			if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			if item.ID == "" {
				item.ID = id
			}
			if item.ID == "" {
				http.Error(w, "bot id is required", 400)
				return
			}
			if item.Name == "" {
				item.Name = item.ID
			}
			if r.Method == http.MethodPost && item.Token == "" && item.TokenEnv == "" {
				item.TokenEnv = "TELEGRAM_BOT_TOKEN"
			}
			next.Telegram.Bots = upsertTelegramBot(next.Telegram.Bots, item)
		case "chats":
			var item TelegramChat
			if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			if item.ID == "" {
				item.ID = id
			}
			if item.ID == "" {
				http.Error(w, "chat id is required", 400)
				return
			}
			if item.Name == "" {
				item.Name = item.ID
			}
			next.Telegram.Chats = upsertTelegramChat(next.Telegram.Chats, item)
		case "users":
			var item TelegramUser
			if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			if item.ID == "" {
				item.ID = id
			}
			if item.ID == "" {
				http.Error(w, "telegram user id is required", 400)
				return
			}
			if item.DisplayName == "" {
				item.DisplayName = item.ID
			}
			next.Telegram.Users = upsertTelegramUser(next.Telegram.Users, item)
		}
		preserveTelegramSecrets(&next.Telegram, cfg.Telegram)
		cr, applied, err := a.proposeConfigChange(r.Context(), *next, "telegram", "更新 Telegram "+kind+" 资源", u, true)
		writeChangeResponse(w, cr, applied, err)
	case http.MethodDelete:
		if id == "" {
			http.Error(w, "id is required", 400)
			return
		}
		switch kind {
		case "bots":
			next.Telegram.Bots = deleteTelegramBot(next.Telegram.Bots, id)
		case "chats":
			next.Telegram.Chats = deleteTelegramChat(next.Telegram.Chats, id)
		case "users":
			next.Telegram.Users = deleteTelegramUser(next.Telegram.Users, id)
		}
		cr, applied, err := a.proposeConfigChange(r.Context(), *next, "telegram", "删除 Telegram "+kind+" 资源", u, true)
		writeChangeResponse(w, cr, applied, err)
	default:
		http.Error(w, "method not allowed", 405)
	}
}

func telegramPathID(kind, p string) string {
	return strings.Trim(pathID("/api/telegram/"+kind, p), "/")
}

func upsertTelegramBot(items []TelegramBot, item TelegramBot) []TelegramBot {
	for i := range items {
		if items[i].ID == item.ID {
			items[i] = item
			return items
		}
	}
	return append(items, item)
}
func upsertTelegramChat(items []TelegramChat, item TelegramChat) []TelegramChat {
	for i := range items {
		if items[i].ID == item.ID {
			items[i] = item
			return items
		}
	}
	return append(items, item)
}
func upsertTelegramUser(items []TelegramUser, item TelegramUser) []TelegramUser {
	for i := range items {
		if items[i].ID == item.ID {
			items[i] = item
			return items
		}
	}
	return append(items, item)
}
func deleteTelegramBot(items []TelegramBot, id string) []TelegramBot {
	out := []TelegramBot{}
	for _, item := range items {
		if item.ID != id {
			out = append(out, item)
		}
	}
	return out
}
func deleteTelegramChat(items []TelegramChat, id string) []TelegramChat {
	out := []TelegramChat{}
	for _, item := range items {
		if item.ID != id {
			out = append(out, item)
		}
	}
	return out
}
func deleteTelegramUser(items []TelegramUser, id string) []TelegramUser {
	out := []TelegramUser{}
	for _, item := range items {
		if item.ID != id {
			out = append(out, item)
		}
	}
	return out
}

func (a *App) handleRules(w http.ResponseWriter, r *http.Request) {
	u := userFromContext(r.Context())
	cfg := a.Config()
	if r.Method == http.MethodGet {
		if !u.Can(PermConfigRead) {
			http.Error(w, "forbidden", 403)
			return
		}
		writeJSON(w, cfg.Rules)
		return
	}
	if !u.Can(PermRulesWrite) {
		http.Error(w, "forbidden: need permission "+PermRulesWrite, 403)
		return
	}
	switch r.Method {
	case http.MethodPost, http.MethodPut:
		var req RuleEditRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if req.ID == "" {
			req.ID = strings.Trim(pathID("/api/rules", r.URL.Path), "/")
		}
		next := cloneConfig(cfg)
		if err := upsertRuleFromRequest(next, req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		cr, applied, err := a.proposeConfigChange(r.Context(), *next, "rules", "通过表单创建或更新规则策略", u, false)
		writeChangeResponse(w, cr, applied, err)
	case http.MethodDelete:
		id := strings.Trim(pathID("/api/rules", r.URL.Path), "/")
		next := cloneConfig(cfg)
		filtered := []PolicyRule{}
		for _, rule := range next.Rules {
			if rule.ID != id {
				filtered = append(filtered, rule)
			}
		}
		next.Rules = filtered
		cr, applied, err := a.proposeConfigChange(r.Context(), *next, "rules", "删除规则策略", u, false)
		writeChangeResponse(w, cr, applied, err)
	default:
		http.Error(w, "method not allowed", 405)
	}
}

type RuleEditRequest struct {
	ID             string   `json:"id"`
	Name           string   `json:"name"`
	Enabled        bool     `json:"enabled"`
	Priority       int      `json:"priority"`
	Operations     []string `json:"operations"`
	APIGroup       string   `json:"api_group"`
	Resource       string   `json:"resource"`
	Kind           string   `json:"kind"`
	Namespaces     []string `json:"namespaces"`
	Names          []string `json:"names"`
	ActorGroupIDs  []string `json:"actor_group_ids"`
	ChangeClasses  []string `json:"change_classes"`
	Decision       string   `json:"decision"`
	Reason         string   `json:"reason"`
	Approval       bool     `json:"approval"`
	Rollback       bool     `json:"rollback"`
	NotifyTemplate string   `json:"notify_template"`
}

func upsertRuleFromRequest(cfg *RuntimeConfig, req RuleEditRequest) error {
	if strings.TrimSpace(req.ID) == "" {
		return fmt.Errorf("rule id is required")
	}
	if req.Name == "" {
		req.Name = req.ID
	}
	if req.Priority == 0 {
		req.Priority = 100
	}
	if len(req.Operations) == 0 {
		req.Operations = []string{"DELETE"}
	}
	if req.Decision == "" {
		req.Decision = DecisionRequireApproval
	}
	if len(req.Namespaces) == 0 {
		req.Namespaces = []string{"*"}
	}
	if len(req.Names) == 0 {
		req.Names = []string{"*"}
	}
	scopeID := "web_scope_" + req.ID
	scope := ResourceScope{ID: scopeID, Name: req.Name + " 资源范围", Enabled: true, APIGroups: []string{req.APIGroup}, Resources: []string{req.Resource}, Kinds: []string{req.Kind}, Namespaces: req.Namespaces, Names: req.Names}
	upsertScope(cfg, scope)
	rule := PolicyRule{ID: req.ID, Name: req.Name, Enabled: req.Enabled, Priority: req.Priority, ScopeIDs: []string{scopeID}, Operations: upperStrings(req.Operations), ActorGroupIDs: req.ActorGroupIDs, ChangeClasses: req.ChangeClasses, Decision: req.Decision, Reason: req.Reason}
	if req.NotifyTemplate != "" {
		rule.Notify = NotificationBinding{Enabled: true, TemplateID: req.NotifyTemplate}
	}
	if req.Approval || req.Decision == DecisionRequireApproval {
		rule.Approval = ApprovalBinding{Enabled: true, Mode: "both", TTLSeconds: 300, FailWhenStoreDown: true}
	}
	if req.Rollback {
		rule.Rollback = RollbackBinding{Enabled: true, Mode: RollbackRestoreOldObject, ShowInTelegram: true, ShowInWeb: true}
	}
	updated := false
	for i := range cfg.Rules {
		if cfg.Rules[i].ID == rule.ID {
			cfg.Rules[i] = rule
			updated = true
		}
	}
	if !updated {
		cfg.Rules = append(cfg.Rules, rule)
	}
	sort.Slice(cfg.Rules, func(i, j int) bool { return cfg.Rules[i].Priority < cfg.Rules[j].Priority })
	return validateRuntimeConfig(cfg)
}

func upsertScope(cfg *RuntimeConfig, scope ResourceScope) {
	for i := range cfg.ResourceScopes {
		if cfg.ResourceScopes[i].ID == scope.ID {
			cfg.ResourceScopes[i] = scope
			return
		}
	}
	cfg.ResourceScopes = append(cfg.ResourceScopes, scope)
}

func upperStrings(xs []string) []string {
	out := make([]string, 0, len(xs))
	for _, x := range xs {
		out = append(out, strings.ToUpper(strings.TrimSpace(x)))
	}
	return out
}

func (a *App) handleRollback(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, "/api/rollback/")
	parts := strings.Split(strings.Trim(p, "/"), "/")
	if len(parts) < 2 {
		http.Error(w, "expected /api/rollback/{id}/dryrun|execute", 400)
		return
	}
	id, action := parts[0], parts[1]
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	switch action {
	case "dryrun":
		msg, err := a.executeRollback(ctx, id, true)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		writeJSON(w, map[string]any{"ok": true, "dry_run": true, "message": msg})
	case "execute":
		msg, err := a.executeRollback(ctx, id, false)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		writeJSON(w, map[string]any{"ok": true, "dry_run": false, "message": msg})
	default:
		http.Error(w, "unknown rollback action", 400)
	}
}

func (a *App) handleDatasourceTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	var body struct {
		URI      string `json:"uri"`
		Database string `json:"database"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	m, err := NewMongoStore(ctx, body.URI, body.Database)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	m.Disconnect(ctx)
	writeJSON(w, map[string]any{"ok": true})
}

func parseEventQuery(r *http.Request) EventQuery {
	qv := r.URL.Query()
	q := EventQuery{Limit: parseLimit(r, 200), Cluster: strings.TrimSpace(qv.Get("cluster")), Namespace: strings.TrimSpace(qv.Get("namespace")), Kind: strings.TrimSpace(qv.Get("kind")), Resource: strings.TrimSpace(qv.Get("resource")), Name: strings.TrimSpace(qv.Get("name")), User: strings.TrimSpace(qv.Get("user")), Operation: strings.ToUpper(strings.TrimSpace(qv.Get("operation"))), Decision: strings.TrimSpace(qv.Get("decision"))}
	if v := strings.TrimSpace(qv.Get("allowed")); v != "" {
		b := v == "true" || v == "1" || strings.EqualFold(v, "yes")
		q.Allowed = &b
	}
	tz := strings.TrimSpace(qv.Get("tz"))
	q.Start = parseWebTime(qv.Get("start"), tz, false)
	q.End = parseWebTime(qv.Get("end"), tz, true)
	if !q.Start.IsZero() && !q.End.IsZero() && q.End.Before(q.Start) {
		q.Start, q.End = q.End, q.Start
	}
	return q
}

func parseWebTime(v, tz string, endOfDate bool) time.Time {
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Time{}
	}
	loc := time.UTC
	if tz != "" {
		if l, err := time.LoadLocation(tz); err == nil {
			loc = l
		}
	}
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return t.UTC()
	}
	layouts := []string{"2006-01-02T15:04:05", "2006-01-02T15:04"}
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, v, loc); err == nil {
			return t.UTC()
		}
	}
	if t, err := time.ParseInLocation("2006-01-02", v, loc); err == nil {
		if endOfDate {
			return t.AddDate(0, 0, 1).UTC()
		}
		return t.UTC()
	}
	return time.Time{}
}

func cloneConfig(cfg *RuntimeConfig) *RuntimeConfig {
	if cfg == nil {
		return defaultRuntimeConfig()
	}
	b, _ := json.Marshal(cfg)
	var out RuntimeConfig
	_ = json.Unmarshal(b, &out)
	return &out
}

func sanitizeUsers(users []WebUser) []WebUser {
	out := make([]WebUser, len(users))
	copy(out, users)
	for i := range out {
		out[i].PasswordHash = ""
	}
	return out
}

func sanitizeRuntimeConfigForResponse(cfg *RuntimeConfig) {
	if cfg == nil {
		return
	}
	cfg.WebUsers = sanitizeUsers(cfg.WebUsers)
	cfg.Telegram = sanitizeTelegram(cfg.Telegram)
}

func writeChangeResponse(w http.ResponseWriter, cr *ConfigChangeRequest, applied bool, err error) {
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	if cr != nil {
		sanitizeRuntimeConfigForResponse(&cr.Config)
	}
	writeJSON(w, map[string]any{"ok": true, "applied": applied, "change": cr})
}

func parseLimit(r *http.Request, def int) int {
	n, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if n <= 0 {
		return def
	}
	return n
}

func pathID(prefix, p string) string { return strings.TrimPrefix(strings.TrimPrefix(p, prefix), "/") }
