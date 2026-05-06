package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

type contextKey string

const userContextKey contextKey = "webUser"

const (
	PermAll              = "*"
	PermDashboardRead    = "dashboard:read"
	PermEventsRead       = "events:read"
	PermSARead           = "serviceaccounts:read"
	PermSAScan           = "serviceaccounts:scan"
	PermSAMount          = "serviceaccounts:mount"
	PermActorGroupsWrite = "actorgroups:write"
	PermConfigRead       = "config:read"
	PermConfigWrite      = "config:write"
	PermConfigApprove    = "config:approve"
	PermConfigRestore    = "config:restore"
	PermRulesWrite       = "rules:write"
	PermDatasourceWrite  = "datasources:write"
	PermTelegramWrite    = "telegram:write"
	PermSettingsWrite    = "settings:write"
	PermUsersWrite       = "users:write"
	PermRolesWrite       = "roles:write"
	PermRollbackExecute  = "rollback:execute"
)

type AuthUser struct {
	Username    string   `json:"username"`
	DisplayName string   `json:"display_name"`
	Roles       []string `json:"roles"`
	Permissions []string `json:"permissions"`
	SuperAdmin  bool     `json:"super_admin"`
	TokenMode   string   `json:"token_mode"`
}

type authTokenPayload struct {
	Username string   `json:"u"`
	Roles    []string `json:"r"`
	Exp      int64    `json:"exp"`
}

func defaultWebSettings() WebSettings {
	return WebSettings{SiteName: envOr("WEB_SITE_NAME", "K8s Delete Interceptor"), SiteSubtitle: envOr("WEB_SITE_SUBTITLE", "Admission Guard Console"), SiteIcon: envOr("WEB_SITE_ICON", "⎈"), DefaultTimezone: envOr("WEB_DEFAULT_TIMEZONE", "Asia/Shanghai"), Theme: "cyber"}
}

func defaultWebRoles() []WebRole {
	return []WebRole{
		{ID: "superadmin", Name: "超管", Description: "拥有所有 Web、规则、配置、用户、数据源和恢复权限", Permissions: []string{PermAll}, Builtin: true},
		{ID: "viewer", Name: "只读用户", Description: "只能查看首页、事件、SA、配置和数据源", Permissions: []string{PermDashboardRead, PermEventsRead, PermSARead, PermConfigRead}, Builtin: true},
		{ID: "auditor", Name: "审计员", Description: "查看历史事件、SA 资产和配置版本", Permissions: []string{PermDashboardRead, PermEventsRead, PermSARead, PermConfigRead}, Builtin: true},
		{ID: "operator", Name: "运维操作员", Description: "查看审计、扫描 SA、执行授权回滚", Permissions: []string{PermDashboardRead, PermEventsRead, PermSARead, PermSAScan, PermConfigRead, PermRollbackExecute}, Builtin: true},
		{ID: "rule_manager", Name: "规则管理员", Description: "可提交规则、SA 挂载、数据源和配置变更申请", Permissions: []string{PermDashboardRead, PermEventsRead, PermSARead, PermSAScan, PermSAMount, PermActorGroupsWrite, PermConfigRead, PermConfigWrite, PermRulesWrite, PermDatasourceWrite}, Builtin: true},
	}
}

func defaultWebUsers() []WebUser {
	user := envOr("WEB_ADMIN_USERNAME", "admin")
	pass := os.Getenv("WEB_ADMIN_PASSWORD")
	if pass == "" {
		return nil
	}
	now := time.Now().UTC()
	return []WebUser{{Username: user, DisplayName: "默认超管", PasswordHash: hashPassword(user, pass), Roles: []string{"superadmin"}, Enabled: true, CreatedAt: now, UpdatedAt: now}}
}

func hashPassword(username, password string) string {
	seed := envOr("WEB_PASSWORD_SALT", envOr("WEB_SESSION_SECRET", envOr("WEB_ADMIN_TOKEN", "k8s-delete-interceptor")))
	h := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(username)) + ":" + password + ":" + seed))
	return "sha256:" + hex.EncodeToString(h[:])
}

func verifyPassword(u WebUser, password string) bool {
	if u.PasswordHash == "" {
		return false
	}
	return hmac.Equal([]byte(u.PasswordHash), []byte(hashPassword(u.Username, password)))
}

func (a *App) authSecret() string {
	if v := os.Getenv("WEB_SESSION_SECRET"); v != "" {
		return v
	}
	if a.adminToken != "" {
		return a.adminToken
	}
	return "k8s-delete-interceptor-dev-secret"
}

func (a *App) issueToken(u WebUser) (string, error) {
	ttl := 12 * time.Hour
	if raw := os.Getenv("WEB_SESSION_TTL_HOURS"); raw != "" {
		if d, err := time.ParseDuration(raw + "h"); err == nil && d > 0 {
			ttl = d
		}
	}
	payload := authTokenPayload{Username: u.Username, Roles: u.Roles, Exp: time.Now().Add(ttl).Unix()}
	b, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	body := base64.RawURLEncoding.EncodeToString(b)
	mac := hmac.New(sha256.New, []byte(a.authSecret()))
	_, _ = mac.Write([]byte(body))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return body + "." + sig, nil
}

func (a *App) parseToken(token string) (*authTokenPayload, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return nil, errors.New("bad token")
	}
	mac := hmac.New(sha256.New, []byte(a.authSecret()))
	_, _ = mac.Write([]byte(parts[0]))
	want := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(want), []byte(parts[1])) {
		return nil, errors.New("bad signature")
	}
	b, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, err
	}
	var p authTokenPayload
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, err
	}
	if p.Exp > 0 && time.Now().Unix() > p.Exp {
		return nil, errors.New("token expired")
	}
	return &p, nil
}

func (a *App) authRequired() bool {
	cfg := a.Config()
	if a.adminToken != "" {
		return true
	}
	if cfg == nil {
		return false
	}
	for _, u := range cfg.WebUsers {
		if u.Enabled && u.PasswordHash != "" {
			return true
		}
	}
	return false
}

func (a *App) authenticate(r *http.Request) (*AuthUser, error) {
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if auth == "" && r.URL.Query().Get("token") != "" {
		auth = "Bearer " + r.URL.Query().Get("token")
	}
	if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
		tok := strings.TrimSpace(auth[7:])
		if a.adminToken != "" && tok == a.adminToken {
			return &AuthUser{Username: "token-admin", DisplayName: "WEB_ADMIN_TOKEN", Roles: []string{"superadmin"}, Permissions: []string{PermAll}, SuperAdmin: true, TokenMode: "admin_token"}, nil
		}
		if p, err := a.parseToken(tok); err == nil {
			cfg := a.Config()
			if cfg != nil {
				for _, u := range cfg.WebUsers {
					if strings.EqualFold(u.Username, p.Username) && u.Enabled {
						return a.authUserFromConfigUser(u), nil
					}
				}
			}
		}
	}
	if !a.authRequired() {
		return &AuthUser{Username: "anonymous-superadmin", DisplayName: "本地免认证超管", Roles: []string{"superadmin"}, Permissions: []string{PermAll}, SuperAdmin: true, TokenMode: "no_auth"}, nil
	}
	return nil, errors.New("unauthorized")
}

func (a *App) authUserFromConfigUser(u WebUser) *AuthUser {
	perms := a.permissionsForRoles(u.Roles)
	return &AuthUser{Username: u.Username, DisplayName: u.DisplayName, Roles: u.Roles, Permissions: perms, SuperAdmin: hasPermission(perms, PermAll), TokenMode: "session"}
}

func (a *App) permissionsForRoles(roleIDs []string) []string {
	cfg := a.Config()
	set := map[string]bool{}
	if cfg == nil {
		return nil
	}
	for _, rid := range roleIDs {
		for _, r := range cfg.WebRoles {
			if r.ID == rid {
				for _, p := range r.Permissions {
					set[p] = true
				}
			}
		}
	}
	out := make([]string, 0, len(set))
	for p := range set {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

func hasPermission(perms []string, need string) bool {
	for _, p := range perms {
		if p == PermAll || p == need {
			return true
		}
	}
	return false
}

func (u *AuthUser) Can(perm string) bool {
	if u == nil {
		return false
	}
	return hasPermission(u.Permissions, perm)
}

func userFromContext(ctx context.Context) *AuthUser {
	if v, ok := ctx.Value(userContextKey).(*AuthUser); ok {
		return v
	}
	return nil
}

func (a *App) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		u, err := a.authenticate(r)
		if err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), userContextKey, u)))
	}
}

func (a *App) require(perm string, next http.HandlerFunc) http.HandlerFunc {
	return a.auth(func(w http.ResponseWriter, r *http.Request) {
		u := userFromContext(r.Context())
		if u == nil || !u.Can(perm) {
			http.Error(w, fmt.Sprintf("forbidden: need permission %s", perm), http.StatusForbidden)
			return
		}
		next(w, r)
	})
}
