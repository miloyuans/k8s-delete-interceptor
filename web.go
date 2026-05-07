package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
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
	mux.HandleFunc("/api/events/", a.require(PermEventsRead, a.handleEventYAML))
	mux.HandleFunc("/api/events", a.require(PermEventsRead, a.handleEvents))
	mux.HandleFunc("/api/serviceaccounts", a.require(PermSARead, a.handleServiceAccounts))
	mux.HandleFunc("/api/serviceaccounts/scan", a.require(PermSAScan, a.handleServiceAccountScan))
	mux.HandleFunc("/api/serviceaccounts/mount", a.require(PermSAMount, a.handleServiceAccountMount))
	mux.HandleFunc("/api/actorgroups", a.auth(a.handleActorGroups))
	mux.HandleFunc("/api/actorgroups/", a.auth(a.handleActorGroups))
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
	mux.HandleFunc("/api/telegram/notifications/", a.auth(a.handleTelegramNotificationView))
	mux.HandleFunc("/api/telegram", a.auth(a.handleTelegram))
	mux.HandleFunc("/api/telegram/bots", a.auth(a.handleTelegramBots))
	mux.HandleFunc("/api/telegram/bots/", a.auth(a.handleTelegramBots))
	mux.HandleFunc("/api/telegram/chats", a.auth(a.handleTelegramChats))
	mux.HandleFunc("/api/telegram/chats/", a.auth(a.handleTelegramChats))
	mux.HandleFunc("/api/telegram/users", a.auth(a.handleTelegramUsers))
	mux.HandleFunc("/api/telegram/users/", a.auth(a.handleTelegramUsers))
	mux.HandleFunc("/api/telegram/test", a.require(PermTelegramWrite, a.handleTelegramTest))
	mux.HandleFunc("/api/templates", a.auth(a.handleNotificationTemplates))
	mux.HandleFunc("/api/templates/", a.auth(a.handleNotificationTemplates))
	mux.HandleFunc("/api/rules/preview", a.auth(a.handleRulePreview))
	mux.HandleFunc("/api/rules/parse", a.auth(a.handleRuleParse))
	mux.HandleFunc("/api/rules", a.auth(a.handleRules))
	mux.HandleFunc("/api/rules/", a.auth(a.handleRules))
	mux.HandleFunc("/api/rollback/", a.require(PermRollbackExecute, a.handleRollback))
}

func (a *App) handleIndex(w http.ResponseWriter, r *http.Request) {
	if !isWebRoute(r.URL.Path) {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(indexHTML))
}

func isWebRoute(path string) bool {
	if path == "/" {
		return true
	}
	if strings.HasPrefix(path, "/api/") || path == "/validate" || strings.HasPrefix(path, "/telegram/") {
		return false
	}
	switch strings.Trim(path, "/") {
	case "dashboard", "events", "actorgroups", "serviceaccounts", "rules", "datasources", "telegram", "templates", "changes", "users", "settings", "export":
		return true
	default:
		return false
	}
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
		writeJSON(w, map[string]any{"web": cfg.Web, "persistence": cfg.Persistence})
	case http.MethodPut, http.MethodPost:
		if !userFromContext(r.Context()).Can(PermSettingsWrite) {
			http.Error(w, "forbidden: need permission "+PermSettingsWrite, 403)
			return
		}
		var raw map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		webSettings := cfg.Web
		if b, ok := raw["web"]; ok {
			if err := json.Unmarshal(b, &webSettings); err != nil {
				http.Error(w, "invalid web settings: "+err.Error(), 400)
				return
			}
		} else {
			_ = json.Unmarshal(mustJSON(raw), &webSettings)
		}
		persistence := cfg.Persistence
		if b, ok := raw["persistence"]; ok {
			if err := json.Unmarshal(b, &persistence); err != nil {
				http.Error(w, "invalid persistence settings: "+err.Error(), 400)
				return
			}
		}
		next := cloneConfig(cfg)
		next.Web = webSettings
		next.Persistence = normalizePersistenceSettings(persistence)
		cr, applied, err := a.proposeConfigChange(r.Context(), *next, "settings", "更新站点设置和数据持久化生命周期", userFromContext(r.Context()), true)
		writeChangeResponse(w, cr, applied, err)
	default:
		http.Error(w, "method not allowed", 405)
	}
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
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
	start := time.Now()
	q := parseEventQuery(r)
	merged := []AdmissionEvent{}
	seen := map[string]bool{}
	mongoOK := false
	if a.mongo != nil && a.mongo.Healthy() {
		if events, err := a.mongo.ListEventsByQuery(r.Context(), q); err == nil {
			mongoOK = true
			for _, ev := range events {
				if ev.ID != "" && !seen[ev.ID] {
					seen[ev.ID] = true
					merged = append(merged, ev)
				}
			}
		} else {
			log.Printf("events mongo query failed, falling back to local spool: err=%v", err)
		}
	}
	includeLocal := shouldIncludeLocalEvents(r, mongoOK, q, len(merged))
	if includeLocal {
		if localEvents, err := a.local.ListRecentEventsByQuery(q); err == nil {
			for _, ev := range localEvents {
				if ev.ID != "" && !seen[ev.ID] {
					seen[ev.ID] = true
					merged = append(merged, ev)
				}
			}
		} else if len(merged) == 0 {
			http.Error(w, err.Error(), 500)
			return
		} else {
			log.Printf("events local spool query skipped after error: err=%v", err)
		}
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Time.After(merged[j].Time) })
	limit := q.NormalizedLimit(200)
	if len(merged) > limit {
		merged = merged[:limit]
	}
	log.Printf("events query completed: mongo_ok=%v include_local=%v count=%d limit=%d elapsed=%s id=%s ns=%s resource=%s user=%s", mongoOK, includeLocal, len(merged), limit, time.Since(start), q.ID, q.Namespace, q.Resource, q.User)
	writeJSON(w, merged)
}

func shouldIncludeLocalEvents(r *http.Request, mongoOK bool, q EventQuery, mongoCount int) bool {
	v := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("include_local")))
	if v == "1" || v == "true" || v == "yes" {
		return true
	}
	if v == "0" || v == "false" || v == "no" {
		return false
	}
	mode := strings.ToLower(strings.TrimSpace(os.Getenv("EVENT_QUERY_INCLUDE_LOCAL")))
	if mode == "always" || mode == "true" || mode == "1" {
		return true
	}
	if mode == "never" || mode == "false" || mode == "0" {
		return false
	}
	// Default: avoid walking large local spool directories on every history query.
	// Use local spool only when Mongo is unavailable, direct event lookup misses, or Mongo returned no rows.
	if !mongoOK {
		return true
	}
	if q.ID != "" && mongoCount == 0 {
		return true
	}
	return false
}

func (a *App) handleEventYAML(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", 405)
		return
	}
	p := strings.TrimPrefix(r.URL.Path, "/api/events/")
	parts := strings.Split(strings.Trim(p, "/"), "/")
	if len(parts) != 2 || parts[1] != "yaml" {
		http.NotFound(w, r)
		return
	}
	ev, err := a.getAdmissionEvent(r.Context(), parts[0])
	if err != nil {
		http.Error(w, err.Error(), 404)
		return
	}
	raw := ev.Object
	if r.URL.Query().Get("old") == "1" || strings.EqualFold(r.URL.Query().Get("object"), "old") {
		raw = ev.OldObject
	}
	if len(raw) == 0 {
		raw = ev.OldObject
	}
	if len(raw) == 0 {
		http.Error(w, "event object is empty", 404)
		return
	}
	var obj any
	if err := json.Unmarshal(raw, &obj); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	b, err := yaml.Marshal(obj)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	if r.URL.Query().Get("download") == "1" {
		name := safeFileName(strings.Join([]string{ev.Kind, ev.Namespace, ev.Name, ev.ID}, "-")) + ".yaml"
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", name))
	}
	_, _ = w.Write(b)
}

func (a *App) getAdmissionEvent(ctx context.Context, id string) (*AdmissionEvent, error) {
	if a.mongo != nil && a.mongo.Healthy() {
		if ev, err := a.mongo.GetEvent(ctx, id); err == nil {
			return ev, nil
		}
	}
	return a.local.GetEvent(id)
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
	out := cloneConfig(cfg)
	if tg, err := a.getTelegramConfig(r.Context()); err == nil && tg != nil {
		out.Telegram = *tg
	}
	sanitizeRuntimeConfigForResponse(out)
	format := strings.ToLower(r.URL.Query().Get("format"))
	if format == "json" {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=runtime-config-v%d.json", out.Version))
		_ = json.NewEncoder(w).Encode(out)
		return
	}
	b, err := yaml.Marshal(out)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Content-Type", "application/yaml")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=runtime-config-v%d.yaml", out.Version))
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
			cr, actionErr = a.approveConfigChange(r.Context(), parts[0], body.EventID, body.Note, userFromContext(r.Context()))
		case "reject":
			cr, actionErr = a.rejectConfigChange(r.Context(), parts[0], body.EventID, body.Note, userFromContext(r.Context()))
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

func (a *App) handleTelegramNotificationView(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	u := userFromContext(r.Context())
	if !u.Can(PermEventsRead) && !u.Can(PermConfigRead) {
		http.Error(w, "forbidden", 403)
		return
	}
	p := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/telegram/notifications/"), "/")
	parts := strings.Split(p, "/")
	if len(parts) != 2 || parts[1] != "view" || strings.TrimSpace(parts[0]) == "" {
		http.NotFound(w, r)
		return
	}
	if a.mongo == nil || !a.mongo.Healthy() {
		http.Error(w, "telegram notification store unavailable", 503)
		return
	}
	n, err := a.mongo.MarkTelegramNotificationViewed(r.Context(), parts[0], u.Username)
	if err != nil {
		http.Error(w, err.Error(), 404)
		return
	}
	if n.Kind == NotifyKindConfigChange && n.ChangeID != "" {
		go a.updateConfigChangeTelegramViewed(context.Background(), n.ChangeID, u.Username)
	}
	writeJSON(w, map[string]any{"ok": true, "notification": n})
}

func (a *App) handleTelegram(w http.ResponseWriter, r *http.Request) {
	u := userFromContext(r.Context())
	if r.Method == http.MethodGet {
		if !u.Can(PermConfigRead) {
			http.Error(w, "forbidden", 403)
			return
		}
		tg, err := a.getTelegramConfig(r.Context())
		if err != nil {
			http.Error(w, "telegram config store unavailable: "+err.Error(), 503)
			return
		}
		writeJSON(w, sanitizeTelegram(*tg))
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
	cur, err := a.getTelegramConfig(r.Context())
	if err != nil {
		http.Error(w, "telegram config store unavailable: "+err.Error(), 503)
		return
	}
	var tg TelegramConfig
	if err := json.NewDecoder(r.Body).Decode(&tg); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	preserveTelegramSecrets(&tg, *cur)
	saved, err := a.saveTelegramConfig(r.Context(), tg, u.Username)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	_ = a.saveConfigAudit(r.Context(), &ConfigAuditEvent{Kind: "telegram", Category: configChangeCategory("telegram"), Summary: "更新 Telegram 全局配置", Actor: u.Username, CreatedAt: time.Now().UTC(), Status: ChangeApproved})
	writeJSON(w, map[string]any{"applied": true, "message": "Telegram 配置已写入数据库并立即生效", "telegram": sanitizeTelegram(*saved)})
}

func (a *App) handleTelegramTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	tg, err := a.getTelegramConfig(r.Context())
	if err != nil {
		log.Printf("telegram test blocked: db config unavailable err=%v", err)
		http.Error(w, "telegram config store unavailable: "+err.Error(), 503)
		return
	}
	if !tg.Enabled {
		log.Printf("telegram test blocked: telegram global disabled")
		http.Error(w, "telegram global setting is disabled", 400)
		return
	}
	var body struct {
		BotID          string `json:"bot_id"`
		ChatID         string `json:"chat_id"`
		ChatResourceID string `json:"chat_resource_id"`
		TelegramUserID string `json:"telegram_user_id"`
		Message        string `json:"message"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	botID := strings.TrimSpace(body.BotID)
	chatID := strings.TrimSpace(body.ChatID)
	if body.ChatResourceID != "" {
		for _, c := range tg.Chats {
			if c.ID == body.ChatResourceID {
				chatID = c.ChatID
				if botID == "" {
					botID = c.BotID
				}
				break
			}
		}
	}
	if body.TelegramUserID != "" {
		for _, u := range tg.Users {
			if u.ID == body.TelegramUserID || u.TelegramID == body.TelegramUserID {
				if !u.Enabled {
					http.Error(w, "telegram user is disabled", 400)
					return
				}
				chatID = u.TelegramID
				break
			}
		}
	}
	if botID == "" {
		for _, b := range tg.Bots {
			if b.Enabled && len(telegramTokenCandidates(b)) > 0 {
				botID = b.ID
				break
			}
		}
	}
	bot := findTelegramBot(tg, botID)
	if bot == nil || !bot.Enabled {
		log.Printf("telegram test failed: bot not found or disabled bot_id=%s", botID)
		http.Error(w, "telegram bot not found or disabled", 400)
		return
	}
	if chatID == "" {
		for _, c := range tg.Chats {
			if c.Enabled && c.BotID == botID {
				chatID = c.ChatID
				break
			}
		}
	}
	token, tokenSource := telegramTokenForBotKey(*bot, "web-test|"+chatID)
	if token == "" {
		log.Printf("telegram test failed: empty token bot_id=%s token_env=%s token_envs=%v", bot.ID, bot.TokenEnv, bot.TokenEnvs)
		http.Error(w, "telegram token is empty; 请填写 Bot Token 或确认 token_env 环境变量已挂载到容器", 400)
		return
	}
	log.Printf("telegram test validating token: bot_id=%s source=%s fingerprint=%s", bot.ID, tokenSource, tokenFingerprint(token))
	botUsername, err := validateTelegramBotToken(r.Context(), token)
	if err != nil {
		log.Printf("telegram test token validation failed: bot_id=%s source=%s err=%v", bot.ID, tokenSource, err)
		http.Error(w, err.Error(), 400)
		return
	}
	if chatID == "" {
		log.Printf("telegram test token valid without chat: bot_id=%s bot_username=%s source=%s", bot.ID, botUsername, tokenSource)
		writeJSON(w, map[string]any{"ok": true, "validate_only": true, "bot_id": bot.ID, "bot_username": botUsername, "telegram_enabled": tg.Enabled, "token_source": tokenSource})
		return
	}
	msg := body.Message
	if msg == "" {
		msg = "K8s Delete Interceptor Telegram test message"
	}
	log.Printf("telegram test sending: bot_id=%s bot_username=%s chat_id=%s source=%s", bot.ID, botUsername, chatID, tokenSource)
	if err := sendTelegram(r.Context(), token, chatID, msg, "Markdown", nil); err != nil {
		log.Printf("telegram test failed: bot_id=%s bot_username=%s chat_id=%s err=%v", bot.ID, botUsername, chatID, err)
		http.Error(w, err.Error(), 500)
		return
	}
	log.Printf("telegram test sent: bot_id=%s bot_username=%s chat_id=%s", bot.ID, botUsername, chatID)
	writeJSON(w, map[string]any{"ok": true, "bot_id": bot.ID, "bot_username": botUsername, "chat_id": chatID, "telegram_enabled": tg.Enabled, "token_source": tokenSource})
}

func sanitizeTelegram(tg TelegramConfig) TelegramConfig {
	out := tg
	for i := range out.Bots {
		if out.Bots[i].Token != "" {
			out.Bots[i].Token = "********"
		}
		for j := range out.Bots[i].Tokens {
			if out.Bots[i].Tokens[j] != "" {
				out.Bots[i].Tokens[j] = "********"
			}
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
		if old, ok := byID[next.Bots[i].ID]; ok {
			if next.Bots[i].Token == "" || next.Bots[i].Token == "********" {
				next.Bots[i].Token = old.Token
			}
			if len(next.Bots[i].Tokens) == 0 || allMasked(next.Bots[i].Tokens) {
				next.Bots[i].Tokens = old.Tokens
			}
		}
	}
}

func allMasked(xs []string) bool {
	if len(xs) == 0 {
		return false
	}
	for _, x := range xs {
		if strings.TrimSpace(x) != "" && strings.TrimSpace(x) != "********" {
			return false
		}
	}
	return true
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
	cur, err := a.getTelegramConfig(r.Context())
	if err != nil {
		http.Error(w, "telegram config store unavailable: "+err.Error(), 503)
		return
	}
	if r.Method == http.MethodGet {
		if !u.Can(PermConfigRead) {
			http.Error(w, "forbidden", 403)
			return
		}
		safe := sanitizeTelegram(*cur)
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
	next := *cur
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
			if r.Method == http.MethodPost && item.Token == "" && item.TokenEnv == "" && len(item.Tokens) == 0 && len(item.TokenEnvs) == 0 {
				item.TokenEnv = "TELEGRAM_BOT_TOKEN"
			}
			next.Bots = upsertTelegramBot(next.Bots, item)
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
			next.Chats = upsertTelegramChat(next.Chats, item)
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
			next.Users = upsertTelegramUser(next.Users, item)
		}
		preserveTelegramSecrets(&next, *cur)
		saved, err := a.saveTelegramConfig(r.Context(), next, u.Username)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		_ = a.saveConfigAudit(r.Context(), &ConfigAuditEvent{Kind: "telegram", Category: configChangeCategory("telegram"), Summary: "更新 Telegram " + kind + " 资源", Actor: u.Username, CreatedAt: time.Now().UTC(), Status: ChangeApproved})
		writeJSON(w, map[string]any{"applied": true, "message": "Telegram 资源已写入数据库并立即生效", "telegram": sanitizeTelegram(*saved)})
	case http.MethodDelete:
		if id == "" {
			http.Error(w, "id is required", 400)
			return
		}
		switch kind {
		case "bots":
			next.Bots = deleteTelegramBot(next.Bots, id)
		case "chats":
			next.Chats = deleteTelegramChat(next.Chats, id)
		case "users":
			next.Users = deleteTelegramUser(next.Users, id)
		}
		saved, err := a.saveTelegramConfig(r.Context(), next, u.Username)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		_ = a.saveConfigAudit(r.Context(), &ConfigAuditEvent{Kind: "telegram", Category: configChangeCategory("telegram"), Summary: "删除 Telegram " + kind + " 资源", Actor: u.Username, CreatedAt: time.Now().UTC(), Status: ChangeApproved})
		writeJSON(w, map[string]any{"applied": true, "message": "Telegram 资源已从数据库删除", "telegram": sanitizeTelegram(*saved)})
	default:
		http.Error(w, "method not allowed", 405)
	}
}

func telegramPathID(kind, p string) string {
	return strings.Trim(pathID("/api/telegram/"+kind, p), "/")
}

func upsertTelegramBot(items []TelegramBot, item TelegramBot) []TelegramBot {
	cfg := normalizeTelegramConfig(TelegramConfig{Enabled: true, Bots: []TelegramBot{item}})
	if len(cfg.Bots) == 0 {
		return items
	}
	item = cfg.Bots[0]
	for i := range items {
		if items[i].ID == item.ID {
			items[i] = item
			return items
		}
	}
	return append(items, item)
}
func upsertTelegramChat(items []TelegramChat, item TelegramChat) []TelegramChat {
	cfg := normalizeTelegramConfig(TelegramConfig{Enabled: true, Chats: []TelegramChat{item}})
	if len(cfg.Chats) == 0 {
		return items
	}
	item = cfg.Chats[0]
	for i := range items {
		if items[i].ID == item.ID {
			items[i] = item
			return items
		}
	}
	return append(items, item)
}
func upsertTelegramUser(items []TelegramUser, item TelegramUser) []TelegramUser {
	cfg := normalizeTelegramConfig(TelegramConfig{Enabled: true, Users: []TelegramUser{item}})
	if len(cfg.Users) == 0 {
		return items
	}
	item = cfg.Users[0]
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

func (a *App) handleActorGroups(w http.ResponseWriter, r *http.Request) {
	u := userFromContext(r.Context())
	cfg := a.Config()
	if cfg == nil {
		http.Error(w, "runtime config unavailable", 500)
		return
	}
	id := strings.Trim(pathID("/api/actorgroups", r.URL.Path), "/")
	if r.Method == http.MethodGet {
		if !u.Can(PermConfigRead) {
			http.Error(w, "forbidden", 403)
			return
		}
		if id != "" {
			for _, g := range cfg.ActorGroups {
				if g.ID == id {
					writeJSON(w, g)
					return
				}
			}
			http.NotFound(w, r)
			return
		}
		writeJSON(w, cfg.ActorGroups)
		return
	}
	if !u.Can(PermActorGroupsWrite) && !u.Can(PermRulesWrite) {
		http.Error(w, "forbidden: need permission "+PermActorGroupsWrite, 403)
		return
	}
	next := cloneConfig(cfg)
	switch r.Method {
	case http.MethodPost, http.MethodPut:
		var item ActorGroup
		if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if item.ID == "" {
			item.ID = id
		}
		item = normalizeActorGroup(item)
		if item.ID == "" {
			http.Error(w, "actor group name is required", 400)
			return
		}
		next.ActorGroups = upsertActorGroup(next.ActorGroups, item)
		cr, applied, err := a.proposeConfigChange(r.Context(), *next, "actor_groups", "创建或更新 ActorGroup 逻辑组", u, false)
		writeChangeResponse(w, cr, applied, err)
	case http.MethodDelete:
		if id == "" {
			http.Error(w, "actor group id is required", 400)
			return
		}
		next.ActorGroups = deleteActorGroup(next.ActorGroups, id)
		for i := range next.Rules {
			next.Rules[i].ActorGroupIDs = removeString(next.Rules[i].ActorGroupIDs, id)
		}
		cr, applied, err := a.proposeConfigChange(r.Context(), *next, "actor_groups", "删除 ActorGroup 逻辑组", u, false)
		writeChangeResponse(w, cr, applied, err)
	default:
		http.Error(w, "method not allowed", 405)
	}
}

func normalizeActorGroup(g ActorGroup) ActorGroup {
	g.ID = strings.TrimSpace(g.ID)
	g.Name = strings.TrimSpace(g.Name)
	if g.ID == "" {
		g.ID = autoID("actor", g.Name)
	}
	if g.Name == "" {
		g.Name = g.ID
	}
	g.Users = dedupeSort(g.Users)
	g.Groups = dedupeSort(g.Groups)
	g.ServiceAccounts = dedupeSort(g.ServiceAccounts)
	return g
}

func upsertActorGroup(items []ActorGroup, item ActorGroup) []ActorGroup {
	for i := range items {
		if items[i].ID == item.ID {
			items[i] = item
			return items
		}
	}
	return append(items, item)
}

func deleteActorGroup(items []ActorGroup, id string) []ActorGroup {
	out := []ActorGroup{}
	for _, item := range items {
		if item.ID != id {
			out = append(out, item)
		}
	}
	return out
}

func removeString(xs []string, v string) []string {
	out := []string{}
	for _, x := range xs {
		if x != v {
			out = append(out, x)
		}
	}
	return out
}

func (a *App) handleNotificationTemplates(w http.ResponseWriter, r *http.Request) {
	u := userFromContext(r.Context())
	cfg := a.Config()
	if cfg == nil {
		http.Error(w, "runtime config unavailable", 500)
		return
	}
	id := strings.Trim(pathID("/api/templates", r.URL.Path), "/")
	if r.Method == http.MethodGet {
		if !u.Can(PermConfigRead) {
			http.Error(w, "forbidden", 403)
			return
		}
		if id != "" {
			for _, t := range cfg.NotificationTemplates {
				if t.ID == id {
					writeJSON(w, t)
					return
				}
			}
			http.NotFound(w, r)
			return
		}
		writeJSON(w, cfg.NotificationTemplates)
		return
	}
	if !u.Can(PermTemplatesWrite) && !u.Can(PermTelegramWrite) {
		http.Error(w, "forbidden: need permission "+PermTemplatesWrite, 403)
		return
	}
	next := cloneConfig(cfg)
	switch r.Method {
	case http.MethodPost, http.MethodPut:
		var item NotificationTemplate
		if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if item.ID == "" {
			item.ID = id
		}
		item = normalizeNotificationTemplate(item)
		if item.ID == "" {
			http.Error(w, "template name or id is required", 400)
			return
		}
		next.NotificationTemplates = upsertNotificationTemplate(next.NotificationTemplates, item)
		cr, applied, err := a.proposeConfigChange(r.Context(), *next, "templates", "更新通知模板配置", u, true)
		writeChangeResponse(w, cr, applied, err)
	case http.MethodDelete:
		if id == "" {
			http.Error(w, "template id is required", 400)
			return
		}
		next.NotificationTemplates = deleteNotificationTemplate(next.NotificationTemplates, id)
		cr, applied, err := a.proposeConfigChange(r.Context(), *next, "templates", "删除通知模板配置", u, true)
		writeChangeResponse(w, cr, applied, err)
	default:
		http.Error(w, "method not allowed", 405)
	}
}

func normalizeNotificationTemplate(t NotificationTemplate) NotificationTemplate {
	t.ID = strings.TrimSpace(t.ID)
	t.Name = strings.TrimSpace(t.Name)
	if t.ID == "" {
		t.ID = autoID("tpl", t.Name)
	}
	if t.Name == "" {
		t.Name = t.ID
	}
	if t.Channel == "" {
		t.Channel = "telegram"
	}
	if t.ParseMode == "" {
		t.ParseMode = "Markdown"
	}
	if t.Body == "" {
		t.Body = "*{{.cluster}}* {{.operation}} {{.kind}}/{{.namespace}}/{{.name}}\n用户：{{.actor_display}}\n原因：{{.reason}}"
	}
	return t
}

func upsertNotificationTemplate(items []NotificationTemplate, item NotificationTemplate) []NotificationTemplate {
	for i := range items {
		if items[i].ID == item.ID {
			items[i] = item
			return items
		}
	}
	return append(items, item)
}

func deleteNotificationTemplate(items []NotificationTemplate, id string) []NotificationTemplate {
	out := []NotificationTemplate{}
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
	if cfg == nil {
		http.Error(w, "runtime config unavailable", 500)
		return
	}
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
		cr, applied, err := a.proposeConfigChange(r.Context(), *next, "rules", "通过可视化表单或 YAML 提交规则策略", u, false)
		writeChangeResponse(w, cr, applied, err)
	case http.MethodDelete:
		id := strings.Trim(pathID("/api/rules", r.URL.Path), "/")
		next := cloneConfig(cfg)
		filtered := []PolicyRule{}
		removedScopeIDs := []string{}
		for _, rule := range next.Rules {
			if rule.ID != id {
				filtered = append(filtered, rule)
				continue
			}
			removedScopeIDs = append(removedScopeIDs, rule.ScopeIDs...)
		}
		next.Rules = filtered
		next.ResourceScopes = removeUnusedAutoScopes(next.ResourceScopes, next.Rules, removedScopeIDs)
		cr, applied, err := a.proposeConfigChange(r.Context(), *next, "rules", "删除规则策略", u, false)
		writeChangeResponse(w, cr, applied, err)
	default:
		http.Error(w, "method not allowed", 405)
	}
}

func (a *App) handleRulePreview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	u := userFromContext(r.Context())
	if !u.Can(PermConfigRead) {
		http.Error(w, "forbidden", 403)
		return
	}
	var req RuleEditRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	norm, scope, rule := buildRuleFromRequest(req)
	doc := RuleYAMLDocument{Rule: rule, ResourceScope: scope}
	b, err := yaml.Marshal(doc)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, map[string]any{"request": norm, "rule": rule, "resource_scope": scope, "yaml": string(b)})
}

func (a *App) handleRuleParse(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	u := userFromContext(r.Context())
	if !u.Can(PermConfigRead) {
		http.Error(w, "forbidden", 403)
		return
	}
	var body struct {
		YAML string `json:"yaml"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	req, err := parseRuleYAML(body.YAML)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	norm, scope, rule := buildRuleFromRequest(req)
	b, _ := yaml.Marshal(RuleYAMLDocument{Rule: rule, ResourceScope: scope})
	writeJSON(w, map[string]any{"request": norm, "rule": rule, "resource_scope": scope, "yaml": string(b)})
}

type RuleEditRequest struct {
	ID                    string   `json:"id" yaml:"id"`
	Name                  string   `json:"name" yaml:"name"`
	Enabled               bool     `json:"enabled" yaml:"enabled"`
	Priority              int      `json:"priority" yaml:"priority"`
	Operations            []string `json:"operations" yaml:"operations"`
	APIGroup              string   `json:"api_group" yaml:"api_group"`
	APIGroups             []string `json:"api_groups" yaml:"api_groups"`
	Resource              string   `json:"resource" yaml:"resource"`
	Resources             []string `json:"resources" yaml:"resources"`
	Kind                  string   `json:"kind" yaml:"kind"`
	Kinds                 []string `json:"kinds" yaml:"kinds"`
	Namespaces            []string `json:"namespaces" yaml:"namespaces"`
	Names                 []string `json:"names" yaml:"names"`
	ActorGroupIDs         []string `json:"actor_group_ids" yaml:"actor_group_ids"`
	ChangeClasses         []string `json:"change_classes" yaml:"change_classes"`
	Decision              string   `json:"decision" yaml:"decision"`
	Reason                string   `json:"reason" yaml:"reason"`
	Approval              bool     `json:"approval" yaml:"approval"`
	Rollback              bool     `json:"rollback" yaml:"rollback"`
	NotifyTemplate        string   `json:"notify_template" yaml:"notify_template"`
	TelegramBotIDs        []string `json:"telegram_bot_ids" yaml:"telegram_bot_ids"`
	TelegramChatIDs       []string `json:"telegram_chat_ids" yaml:"telegram_chat_ids"`
	TelegramUserIDs       []string `json:"telegram_user_ids" yaml:"telegram_user_ids"`
	ApproverTelegramUsers []string `json:"approver_telegram_users" yaml:"approver_telegram_users"`
}

type RuleYAMLDocument struct {
	Rule          PolicyRule    `json:"rule" yaml:"rule"`
	ResourceScope ResourceScope `json:"resource_scope" yaml:"resource_scope"`
}

func upsertRuleFromRequest(cfg *RuntimeConfig, req RuleEditRequest) error {
	_, scope, rule := buildRuleFromRequest(req)
	if strings.TrimSpace(rule.ID) == "" {
		return fmt.Errorf("rule name is required")
	}
	upsertScope(cfg, scope)
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

func buildRuleFromRequest(req RuleEditRequest) (RuleEditRequest, ResourceScope, PolicyRule) {
	req = normalizeRuleRequest(req)
	scopeID := "web_scope_" + req.ID
	scope := ResourceScope{ID: scopeID, Name: req.Name + " 资源范围", Enabled: true, APIGroups: req.APIGroups, Resources: req.Resources, Kinds: req.Kinds, Namespaces: req.Namespaces, Names: req.Names}
	rule := PolicyRule{ID: req.ID, Name: req.Name, Enabled: req.Enabled, Priority: req.Priority, ScopeIDs: []string{scopeID}, Operations: upperStrings(req.Operations), ActorGroupIDs: req.ActorGroupIDs, ChangeClasses: req.ChangeClasses, Decision: req.Decision, Reason: req.Reason}
	if req.NotifyTemplate != "" || len(req.TelegramBotIDs) > 0 || len(req.TelegramChatIDs) > 0 || len(req.TelegramUserIDs) > 0 || req.Decision == DecisionAllowNotify || req.Decision == DecisionRequireApproval {
		rule.Notify = NotificationBinding{Enabled: true, TemplateID: req.NotifyTemplate, TelegramBotIDs: req.TelegramBotIDs, TelegramChatIDs: req.TelegramChatIDs, TelegramUserIDs: req.TelegramUserIDs}
	}
	if req.Approval || req.Decision == DecisionRequireApproval {
		rule.Approval = ApprovalBinding{Enabled: true, Mode: "both", TTLSeconds: 300, ApproverTelegramUsers: req.ApproverTelegramUsers, FailWhenStoreDown: true}
	}
	if req.Rollback {
		rule.Rollback = RollbackBinding{Enabled: true, Mode: RollbackRestoreOldObject, ShowInTelegram: true, ShowInWeb: true}
	}
	return req, scope, rule
}

func normalizeRuleRequest(req RuleEditRequest) RuleEditRequest {
	req.ID = strings.TrimSpace(req.ID)
	req.Name = strings.TrimSpace(req.Name)
	if req.ID == "" {
		req.ID = autoID("rule", req.Name)
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
	if !req.Enabled {
		// zero value from old clients should still create enabled rules unless the ID already exists.
		req.Enabled = true
	}
	if len(req.APIGroups) == 0 && req.APIGroup != "" {
		req.APIGroups = []string{req.APIGroup}
	}
	if len(req.Resources) == 0 && req.Resource != "" {
		req.Resources = []string{req.Resource}
	}
	if len(req.Kinds) == 0 && req.Kind != "" {
		req.Kinds = []string{req.Kind}
	}
	req.APIGroups = dedupeTrimAPIGroup(req.APIGroups)
	req.Resources = dedupeTrim(req.Resources)
	req.Kinds = dedupeTrim(req.Kinds)
	req.Namespaces = dedupeTrim(req.Namespaces)
	req.Names = dedupeTrim(req.Names)
	req.ActorGroupIDs = dedupeTrim(req.ActorGroupIDs)
	req.ChangeClasses = dedupeTrim(req.ChangeClasses)
	req.TelegramBotIDs = dedupeTrim(req.TelegramBotIDs)
	req.TelegramChatIDs = dedupeTrim(req.TelegramChatIDs)
	req.TelegramUserIDs = dedupeTrim(req.TelegramUserIDs)
	req.ApproverTelegramUsers = dedupeTrim(req.ApproverTelegramUsers)
	if len(req.APIGroups) == 0 {
		req.APIGroups = []string{"*"}
	}
	if len(req.Resources) == 0 {
		req.Resources = []string{"*"}
	}
	if len(req.Kinds) == 0 {
		req.Kinds = []string{"*"}
	}
	if len(req.Namespaces) == 0 {
		req.Namespaces = []string{"*"}
	}
	if len(req.Names) == 0 {
		req.Names = []string{"*"}
	}
	return req
}

func parseRuleYAML(raw string) (RuleEditRequest, error) {
	var doc RuleYAMLDocument
	if err := yaml.Unmarshal([]byte(raw), &doc); err == nil && doc.Rule.ID != "" {
		return requestFromRuleAndScope(doc.Rule, doc.ResourceScope), nil
	}
	var req RuleEditRequest
	if err := yaml.Unmarshal([]byte(raw), &req); err == nil && (req.ID != "" || req.Name != "") {
		return req, nil
	}
	var rule PolicyRule
	if err := yaml.Unmarshal([]byte(raw), &rule); err != nil {
		return RuleEditRequest{}, err
	}
	if rule.ID == "" && rule.Name == "" {
		return RuleEditRequest{}, fmt.Errorf("yaml must contain rule or rule-like fields")
	}
	return requestFromRuleAndScope(rule, ResourceScope{}), nil
}

func requestFromRuleAndScope(rule PolicyRule, scope ResourceScope) RuleEditRequest {
	req := RuleEditRequest{ID: rule.ID, Name: rule.Name, Enabled: rule.Enabled, Priority: rule.Priority, Operations: rule.Operations, ActorGroupIDs: rule.ActorGroupIDs, ChangeClasses: rule.ChangeClasses, Decision: rule.Decision, Reason: rule.Reason, Approval: rule.Approval.Enabled, Rollback: rule.Rollback.Enabled, NotifyTemplate: rule.Notify.TemplateID, TelegramBotIDs: rule.Notify.TelegramBotIDs, TelegramChatIDs: rule.Notify.TelegramChatIDs, TelegramUserIDs: rule.Notify.TelegramUserIDs, ApproverTelegramUsers: rule.Approval.ApproverTelegramUsers}
	if len(scope.APIGroups) > 0 {
		req.APIGroups = scope.APIGroups
	}
	if len(scope.Resources) > 0 {
		req.Resources = scope.Resources
	}
	if len(scope.Kinds) > 0 {
		req.Kinds = scope.Kinds
	}
	if len(scope.Namespaces) > 0 {
		req.Namespaces = scope.Namespaces
	}
	if len(scope.Names) > 0 {
		req.Names = scope.Names
	}
	return req
}

func removeUnusedAutoScopes(scopes []ResourceScope, rules []PolicyRule, ids []string) []ResourceScope {
	remove := map[string]bool{}
	for _, id := range ids {
		if strings.HasPrefix(id, "web_scope_") {
			remove[id] = true
		}
	}
	for _, r := range rules {
		for _, id := range r.ScopeIDs {
			delete(remove, id)
		}
	}
	out := []ResourceScope{}
	for _, s := range scopes {
		if !remove[s.ID] {
			out = append(out, s)
		}
	}
	return out
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

func autoID(prefix, name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	var b strings.Builder
	lastDash := false
	for _, r := range name {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if r > 127 {
			if !lastDash {
				b.WriteByte('-')
				lastDash = true
			}
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	id := strings.Trim(b.String(), "-")
	if id == "" {
		id = fmt.Sprintf("%d", time.Now().Unix())
	}
	return prefix + "_" + id
}

func dedupeTrim(xs []string) []string {
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
	return out
}

func dedupeTrimAPIGroup(xs []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, x := range xs {
		x = strings.TrimSpace(x)
		if x == "core" || x == "<core>" || strings.EqualFold(x, "core/v1") {
			x = ""
		}
		if seen[x] {
			continue
		}
		seen[x] = true
		out = append(out, x)
	}
	return out
}

func keepEmptyCoreGroup(xs []string) []string {
	if len(xs) == 0 {
		return xs
	}
	seenCore := false
	out := []string{}
	for _, x := range xs {
		if x == "core" || x == "<core>" {
			x = ""
		}
		if x == "" {
			if seenCore {
				continue
			}
			seenCore = true
		}
		out = append(out, x)
	}
	return out
}

func upperStrings(xs []string) []string {
	out := make([]string, 0, len(xs))
	for _, x := range xs {
		v := strings.ToUpper(strings.TrimSpace(x))
		if v != "" {
			out = append(out, v)
		}
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
	q := EventQuery{ID: strings.TrimSpace(firstNonEmpty(qv.Get("id"), qv.Get("event_id"), qv.Get("event"))), Limit: parseLimit(r, 200), Cluster: strings.TrimSpace(qv.Get("cluster")), Namespace: strings.TrimSpace(qv.Get("namespace")), Kind: strings.TrimSpace(qv.Get("kind")), Resource: strings.TrimSpace(qv.Get("resource")), Name: strings.TrimSpace(qv.Get("name")), User: strings.TrimSpace(qv.Get("user")), Operation: strings.ToUpper(strings.TrimSpace(qv.Get("operation"))), Decision: strings.TrimSpace(qv.Get("decision"))}
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

func firstNonEmpty(xs ...string) string {
	for _, x := range xs {
		if strings.TrimSpace(x) != "" {
			return x
		}
	}
	return ""
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
