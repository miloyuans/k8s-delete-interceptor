package main

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

const (
	ChangePending  = "pending"
	ChangeApproved = "approved"
	ChangeRejected = "rejected"
)

func newChangeID(kind, user string) string {
	s := fmt.Sprintf("%s|%s|%d", kind, user, time.Now().UnixNano())
	h := sha1.Sum([]byte(s))
	return "chg_" + hex.EncodeToString(h[:8])
}

func newConfigEventID(kind, user string) string {
	s := fmt.Sprintf("event|%s|%s|%d", kind, user, time.Now().UnixNano())
	h := sha1.Sum([]byte(s))
	return "cfg_evt_" + hex.EncodeToString(h[:10])
}

func configChangeCategory(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "rules", "sa_mount", "actor_groups", "raw_config", "restore":
		return "business_config"
	case "settings":
		return "site_settings"
	case "users", "roles":
		return "identity_access"
	case "datasources":
		return "data_source"
	case "telegram", "templates":
		return "notification"
	default:
		return "system_config"
	}
}

func configApprovalRequired() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("CONFIG_CHANGE_REQUIRE_APPROVAL")))
	return v == "" || v == "1" || v == "true" || v == "yes"
}

func configApprovalRequiredForKind(kind string) bool {
	if !configApprovalRequired() {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "rules", "sa_mount", "actor_groups", "raw_config", "restore":
		return true
	default:
		return false
	}
}

func (a *App) proposeConfigChange(ctx context.Context, cfg RuntimeConfig, kind, summary string, user *AuthUser, forceApply bool) (*ConfigChangeRequest, bool, error) {
	cur := a.Config()
	base := int64(0)
	if cur != nil {
		base = cur.Version
	}
	cfg.Version = base + 1
	if err := validateRuntimeConfig(&cfg); err != nil {
		return nil, false, err
	}
	createdBy := "unknown"
	if user != nil {
		createdBy = user.Username
	}
	diff := diffConfigSummary(cur, &cfg)
	cr := &ConfigChangeRequest{ID: newChangeID(kind, createdBy), EventID: newConfigEventID(kind, createdBy), Kind: kind, Summary: summary, DiffSummary: diff, Status: ChangePending, BaseVersion: base, TargetVersion: cfg.Version, CreatedBy: createdBy, CreatedAt: time.Now().UTC(), Config: cfg}
	if cur != nil {
		cr.BaseHash = hashJSON(cur)
	}
	cr.TargetHash = hashJSON(&cfg)
	if forceApply || !configApprovalRequiredForKind(kind) {
		if err := a.applyConfig(ctx, &cfg, "web:"+kind); err != nil {
			return nil, false, err
		}
		cr.Status = ChangeApproved
		cr.DecidedBy = createdBy
		cr.DecidedAt = time.Now().UTC()
		_ = a.saveConfigAudit(ctx, auditFromChange(cr, ChangeApproved))
		return cr, true, nil
	}
	if err := a.saveConfigChange(ctx, cr); err != nil {
		return nil, false, err
	}
	go a.notifyConfigChange(context.Background(), cr)
	return cr, false, nil
}

func (a *App) applyConfig(ctx context.Context, cfg *RuntimeConfig, source string) error {
	if err := validateRuntimeConfig(cfg); err != nil {
		return err
	}
	if a.mongo != nil && a.mongo.Healthy() {
		if err := a.mongo.SaveConfig(ctx, cfg, source, true); err != nil {
			return err
		}
	}
	return a.SetConfig(cfg, source)
}

func (a *App) saveConfigChange(ctx context.Context, cr *ConfigChangeRequest) error {
	if a.mongo != nil && a.mongo.Healthy() {
		if err := a.mongo.SaveConfigChange(ctx, cr); err == nil {
			_ = a.local.SaveConfigChange(cr)
			return nil
		}
	}
	return a.local.SaveConfigChange(cr)
}

func auditFromChange(cr *ConfigChangeRequest, status string) *ConfigAuditEvent {
	if cr == nil {
		return nil
	}
	id := cr.EventID
	if id == "" {
		id = newConfigEventID(cr.Kind, cr.CreatedBy)
	}
	return &ConfigAuditEvent{
		ID:            id,
		EventID:       id,
		Kind:          cr.Kind,
		Category:      configChangeCategory(cr.Kind),
		Summary:       cr.Summary,
		DiffSummary:   cr.DiffSummary,
		BaseVersion:   cr.BaseVersion,
		TargetVersion: cr.TargetVersion,
		Actor:         cr.CreatedBy,
		CreatedAt:     cr.CreatedAt,
		Status:        status,
	}
}

func (a *App) saveConfigAudit(ctx context.Context, ev *ConfigAuditEvent) error {
	if ev == nil {
		return nil
	}
	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = time.Now().UTC()
	}
	if ev.ID == "" {
		ev.ID = newConfigEventID(ev.Kind, ev.Actor)
	}
	if ev.EventID == "" {
		ev.EventID = ev.ID
	}
	if a.mongo != nil && a.mongo.Healthy() {
		if err := a.mongo.SaveConfigAudit(ctx, ev); err == nil {
			_ = a.local.SaveConfigAudit(ev)
			return nil
		}
	}
	return a.local.SaveConfigAudit(ev)
}

func (a *App) listConfigAudits(ctx context.Context, category string, limit int) ([]ConfigAuditEvent, error) {
	if a.mongo != nil && a.mongo.Healthy() {
		if xs, err := a.mongo.ListConfigAudits(ctx, category, limit); err == nil {
			return xs, nil
		}
	}
	return a.local.ListConfigAudits(category, limit)
}

func (a *App) getConfigChange(ctx context.Context, id string) (*ConfigChangeRequest, error) {
	if a.mongo != nil && a.mongo.Healthy() {
		if cr, err := a.mongo.GetConfigChange(ctx, id); err == nil {
			return cr, nil
		}
	}
	return a.local.GetConfigChange(id)
}

func (a *App) listConfigChanges(ctx context.Context, status string, limit int) ([]ConfigChangeRequest, error) {
	if a.mongo != nil && a.mongo.Healthy() {
		if xs, err := a.mongo.ListConfigChanges(ctx, status, limit); err == nil {
			return sanitizeChanges(xs), nil
		}
	}
	xs, err := a.local.ListConfigChanges(status, limit)
	return sanitizeChanges(xs), err
}

func sanitizeChanges(xs []ConfigChangeRequest) []ConfigChangeRequest {
	for i := range xs {
		sanitizeRuntimeConfigForResponse(&xs[i].Config)
	}
	return xs
}

func (a *App) approveConfigChange(ctx context.Context, id, note string, user *AuthUser) (*ConfigChangeRequest, error) {
	cr, err := a.getConfigChange(ctx, id)
	if err != nil {
		return nil, err
	}
	if cr.Status != ChangePending {
		return nil, fmt.Errorf("change %s is %s", id, cr.Status)
	}
	cur := a.Config()
	if cur != nil && cur.Version != cr.BaseVersion {
		return nil, fmt.Errorf("config version conflict: current v%d, change %s is based on v%d; please recreate this change", cur.Version, id, cr.BaseVersion)
	}
	if err := a.applyConfig(ctx, &cr.Config, "approved:"+cr.Kind); err != nil {
		return nil, err
	}
	cr.Status = ChangeApproved
	if user != nil {
		cr.DecidedBy = user.Username
	}
	cr.DecidedAt = time.Now().UTC()
	cr.DecisionNote = note
	if err := a.saveConfigChange(ctx, cr); err != nil {
		return cr, err
	}
	_ = a.saveConfigAudit(ctx, auditFromChange(cr, ChangeApproved))
	return cr, nil
}

func (a *App) rejectConfigChange(ctx context.Context, id, note string, user *AuthUser) (*ConfigChangeRequest, error) {
	cr, err := a.getConfigChange(ctx, id)
	if err != nil {
		return nil, err
	}
	if cr.Status != ChangePending {
		return nil, fmt.Errorf("change %s is %s", id, cr.Status)
	}
	cr.Status = ChangeRejected
	if user != nil {
		cr.DecidedBy = user.Username
	}
	cr.DecidedAt = time.Now().UTC()
	cr.DecisionNote = note
	if err := a.saveConfigChange(ctx, cr); err != nil {
		return cr, err
	}
	_ = a.saveConfigAudit(ctx, auditFromChange(cr, ChangeRejected))
	return cr, nil
}

func diffConfigSummary(old, next *RuntimeConfig) []string {
	if next == nil {
		return nil
	}
	out := []string{}
	if old == nil {
		return []string{"初始化完整配置"}
	}
	if old.Web.SiteName != next.Web.SiteName || old.Web.SiteIcon != next.Web.SiteIcon {
		out = append(out, "站点名称或图标变更")
	}
	if len(old.DataSources) != len(next.DataSources) || hashJSON(old.DataSources) != hashJSON(next.DataSources) {
		out = append(out, "数据源配置变更")
	}
	if len(old.Rules) != len(next.Rules) || hashJSON(old.Rules) != hashJSON(next.Rules) {
		out = append(out, "规则策略变更")
	}
	if len(old.ActorGroups) != len(next.ActorGroups) || hashJSON(old.ActorGroups) != hashJSON(next.ActorGroups) {
		out = append(out, "Actor/ServiceAccount 授权组变更")
	}
	if len(old.WebUsers) != len(next.WebUsers) || hashJSON(old.WebUsers) != hashJSON(next.WebUsers) {
		out = append(out, "Web 用户变更")
	}
	if len(old.WebRoles) != len(next.WebRoles) || hashJSON(old.WebRoles) != hashJSON(next.WebRoles) {
		out = append(out, "Web 角色权限变更")
	}
	if len(out) == 0 {
		out = append(out, "配置内容变更")
	}
	sort.Strings(out)
	return out
}

func hashJSON(v any) string {
	b, _ := json.Marshal(v)
	h := sha1.Sum(b)
	return hex.EncodeToString(h[:])
}

func (a *App) notifyConfigChange(ctx context.Context, cr *ConfigChangeRequest) {
	cfg := a.Config()
	if cfg == nil || !cfg.Telegram.Enabled || cr == nil {
		return
	}
	text := fmt.Sprintf("⚙️ 配置变更待审批\n事件ID: %s\n类型: %s\n申请人: %s\n版本: v%d -> v%d\n摘要: %s\n差异: %s", cr.EventID, cr.Kind, cr.CreatedBy, cr.BaseVersion, cr.TargetVersion, cr.Summary, strings.Join(cr.DiffSummary, ", "))
	web := strings.TrimRight(os.Getenv("WEB_BASE_URL"), "/")
	markup := any(nil)
	if web != "" {
		markup = map[string]any{"inline_keyboard": [][]map[string]string{{{"text": "打开 Web 审批", "url": web + "/?change=" + cr.ID}}}}
	}
	for _, chat := range cfg.Telegram.Chats {
		if !chat.Enabled {
			continue
		}
		bot := findTelegramBot(cfg, chat.BotID)
		if bot == nil || !bot.Enabled {
			continue
		}
		token := bot.Token
		if token == "" && bot.TokenEnv != "" {
			token = os.Getenv(bot.TokenEnv)
		}
		if token == "" {
			continue
		}
		_ = sendTelegram(ctx, token, chat.ChatID, text, "", markup)
	}
}
