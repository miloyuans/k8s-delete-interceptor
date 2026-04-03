package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/gobwas/glob"
	v1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"gopkg.in/yaml.v3"
)

var (
	tlsCert       = flag.String("tlscert", "/etc/certs/tls.crt", "TLS certificate file")
	tlsKey        = flag.String("tlskey", "/etc/certs/tls.key", "TLS key file")
	configFile    = flag.String("config", "/etc/config/protected.yaml", "Path to protected config file")
	webhookNs     = "webhook-system"
	webhookDeploy = "delete-interceptor"
	webhookSvc    = "delete-interceptor-svc"
)

var httpClient = &http.Client{
	Timeout: 10 * time.Second,
}

const (
	userActionAllow   = "allow"
	userActionObserve = "observe"
	userActionDeny    = "deny"
	defaultNotificationTemplate = "" +
		"*{{title}}*\n" +
		"------------------------------\n" +
		"*Cluster:* `{{cluster}}`\n" +
		"*Action:* `{{action_label}}`\n" +
		"*Resource:* `{{resource}}`\n" +
		"*User:* `{{user}}`\n" +
		"*Operation:* `{{operation}}`\n" +
		"*Reason:* {{reason}}\n" +
		"*Time:* `{{time}}`\n" +
		"*Request UID:* `{{request_uid}}`"
)

type TelegramConfig struct {
	BotToken           string   `json:"bot_token" yaml:"bot_token"`
	ChatIDs            []string `json:"chat_ids" yaml:"chat_ids"`
	NotificationTemplate string   `json:"notification_template" yaml:"notification_template"` // 新增模板字段
}

type AuditTelegramConfig struct {
	UseGlobal            bool     `json:"use_global" yaml:"use_global"`
	BotToken             string   `json:"bot_token" yaml:"bot_token"`
	ChatIDs              []string `json:"chat_ids" yaml:"chat_ids"`
	NotificationTemplate string   `json:"notification_template" yaml:"notification_template"`
}

type ProtectedRule struct {
	Kind  string   `json:"kind" yaml:"kind"`
	Names []string `json:"names" yaml:"names"`
}

type UserPolicyRule struct {
	Action string   `json:"action" yaml:"action"`
	Users  []string `json:"users" yaml:"users"`
}

type NotificationContext struct {
	Title          string
	Action         string
	ActionLabel    string
	User           string
	Operation      string
	OperationType  string
	OperationLabel string
	Cluster        string
	Reason         string
	Timestamp      string
	Kind           string
	Name           string
	Namespace      string
	Resource       string
	RequestUID     string
}

type Config struct {
	Enabled      bool             `json:"enabled" yaml:"enabled"`
	ClusterName  string           `json:"cluster_name" yaml:"cluster_name"`
	Telegram     TelegramConfig   `json:"telegram" yaml:"telegram"`
	Protected    []ProtectedRule  `json:"protected" yaml:"protected"`
	UserPolicies []UserPolicyRule `json:"user_policies" yaml:"user_policies"`
	Audit        AuditConfig      `json:"audit" yaml:"audit"`
}

var config Config

func loadConfig(file string) error {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return fmt.Errorf("failed to read config file '%s': %w", file, err)
	}
	if err := yaml.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("failed to unmarshal config from '%s': %w", file, err)
	}
	return nil
}

func isValidUserAction(action string) bool {
	switch action {
	case userActionAllow, userActionObserve, userActionDeny:
		return true
	default:
		return false
	}
}

func matchPattern(pattern string, candidate string) (bool, string, error) {
	trimmedPattern := strings.TrimSpace(pattern)
	if trimmedPattern == "" {
		return false, "", fmt.Errorf("pattern is empty")
	}

	if strings.HasPrefix(strings.ToLower(trimmedPattern), "regex:") {
		expr := strings.TrimSpace(trimmedPattern[len("regex:"):])
		if expr == "" {
			return false, "", fmt.Errorf("regex expression is empty")
		}

		re, err := regexp.Compile(expr)
		if err != nil {
			return false, "", fmt.Errorf("invalid regex '%s': %w", expr, err)
		}
		return re.MatchString(candidate), "regex", nil
	}

	g, err := glob.Compile(trimmedPattern)
	if err != nil {
		return false, "", fmt.Errorf("invalid glob '%s': %w", trimmedPattern, err)
	}

	return g.Match(candidate), "glob", nil
}

func matchUserPolicy(user string) (bool, string, string, string) {
	for _, rule := range config.UserPolicies {
		action := strings.ToLower(strings.TrimSpace(rule.Action))
		if !isValidUserAction(action) {
			klog.Errorf("Invalid user policy action '%s'. Supported actions: %s, %s, %s. Rule will be skipped.", rule.Action, userActionAllow, userActionObserve, userActionDeny)
			continue
		}

		for _, pattern := range rule.Users {
			matched, matcher, err := matchPattern(pattern, user)
			if err != nil {
				klog.Errorf("Invalid user policy pattern '%s' for action '%s': %v. This pattern will be skipped.", pattern, action, err)
				continue
			}

			if matched {
				return true, action, pattern, matcher
			}
		}
	}

	return false, "", "", ""
}

func formatResource(kind string, name string, namespace string) string {
	if namespace == "" {
		return fmt.Sprintf("%s '%s'", kind, name)
	}

	return fmt.Sprintf("%s '%s' in namespace '%s'", kind, name, namespace)
}

func formatOperation(operation string, kind string, name string, namespace string) string {
	if namespace == "" {
		return fmt.Sprintf("%s %s %s (Cluster Scoped)", strings.ToUpper(operation), kind, name)
	}

	return fmt.Sprintf("%s %s %s (NS: %s)", strings.ToUpper(operation), kind, name, namespace)
}

func formatNamespace(namespace string) string {
	if namespace == "" {
		return "cluster-scoped"
	}

	return namespace
}

func formatNotificationActionLabel(action string) string {
	switch action {
	case "blocked":
		return "拦截"
	case "allowed":
		return "放行"
	case "observed":
		return "放行"
	default:
		return action
	}
}

func buildNotificationTitle(actionLabel string) string {
	if actionLabel == "" {
		return "K8s 删除操作通知"
	}

	return fmt.Sprintf("K8s 删除操作%s通知", actionLabel)
}

func buildNotificationContext(reqUID types.UID, user string, kind string, name string, namespace string, operation string, action string, reason string) NotificationContext {
	actionLabel := formatNotificationActionLabel(action)

	return NotificationContext{
		Title:      buildNotificationTitle(actionLabel),
		Action:     action,
		ActionLabel: actionLabel,
		User:       user,
		Operation:  operation,
		Cluster:    config.ClusterName,
		Reason:     reason,
		Timestamp:  time.Now().Format("2006-01-02 15:04:05 MST"),
		Kind:       kind,
		Name:       name,
		Namespace:  formatNamespace(namespace),
		Resource:   formatResource(kind, name, namespace),
		RequestUID: string(reqUID),
	}
}

func notificationActionLabel(action string) string {
	switch action {
	case "blocked":
		return "拦截"
	case "allowed", "observed":
		return "放行"
	default:
		return action
	}
}

func notificationOperationLabel(operation string) string {
	switch strings.ToUpper(strings.TrimSpace(operation)) {
	case "CREATE":
		return "创建"
	case "UPDATE":
		return "更新"
	case "DELETE":
		return "删除"
	default:
		return strings.ToUpper(strings.TrimSpace(operation))
	}
}

func notificationTitle(operation string, action string) string {
	opLabel := notificationOperationLabel(operation)
	if opLabel == "" {
		opLabel = "资源"
	}

	titleAction := "通知"
	switch strings.ToUpper(strings.TrimSpace(operation)) {
	case "CREATE", "UPDATE":
		if action == "blocked" {
			titleAction = "拦截"
		} else {
			titleAction = "审计"
		}
	case "DELETE":
		switch action {
		case "blocked":
			titleAction = "拦截"
		case "allowed", "observed":
			titleAction = "放行"
		}
	default:
		if action == "blocked" {
			titleAction = "拦截"
		}
	}

	return fmt.Sprintf("K8s %s操作%s通知", opLabel, titleAction)
}

func buildSmartNotificationContext(reqUID types.UID, user string, kind string, name string, namespace string, operationType string, operation string, action string, reason string) NotificationContext {
	return NotificationContext{
		Title:          notificationTitle(operationType, action),
		Action:         action,
		ActionLabel:    notificationActionLabel(action),
		User:           user,
		Operation:      operation,
		OperationType:  strings.ToUpper(strings.TrimSpace(operationType)),
		OperationLabel: notificationOperationLabel(operationType),
		Cluster:        config.ClusterName,
		Reason:         reason,
		Timestamp:      time.Now().Format("2006-01-02 15:04:05 MST"),
		Kind:           kind,
		Name:           name,
		Namespace:      formatNamespace(namespace),
		Resource:       formatResource(kind, name, namespace),
		RequestUID:     string(reqUID),
	}
}

func renderNotificationTemplate(template string, ctx NotificationContext) string {
	if template == "" {
		template = defaultNotificationTemplate
	}

	values := map[string]string{
		"title":        escapeMarkdownV2(ctx.Title),
		"action":       escapeMarkdownV2(ctx.Action),
		"action_label": escapeMarkdownV2(ctx.ActionLabel),
		"user":         escapeMarkdownV2(ctx.User),
		"operation":    escapeMarkdownV2(ctx.Operation),
		"operation_type":  escapeMarkdownV2(ctx.OperationType),
		"operation_label": escapeMarkdownV2(ctx.OperationLabel),
		"cluster":      escapeMarkdownV2(ctx.Cluster),
		"reason":       escapeMarkdownV2(ctx.Reason),
		"time":         escapeMarkdownV2(ctx.Timestamp),
		"kind":         escapeMarkdownV2(ctx.Kind),
		"name":         escapeMarkdownV2(ctx.Name),
		"namespace":    escapeMarkdownV2(ctx.Namespace),
		"resource":     escapeMarkdownV2(ctx.Resource),
		"request_uid":  escapeMarkdownV2(ctx.RequestUID),
	}

	if strings.Contains(template, "{{") {
		replacerArgs := []string{
			"{{title}}", values["title"],
			"{{action}}", values["action"],
			"{{action_label}}", values["action_label"],
			"{{user}}", values["user"],
			"{{operation}}", values["operation"],
			"{{operation_type}}", values["operation_type"],
			"{{operation_label}}", values["operation_label"],
			"{{cluster}}", values["cluster"],
			"{{reason}}", values["reason"],
			"{{time}}", values["time"],
			"{{kind}}", values["kind"],
			"{{name}}", values["name"],
			"{{namespace}}", values["namespace"],
			"{{resource}}", values["resource"],
			"{{request_uid}}", values["request_uid"],
		}
		return strings.NewReplacer(replacerArgs...).Replace(template)
	}

	return fmt.Sprintf(
		template,
		values["user"],
		values["operation"],
		values["cluster"],
		values["reason"],
		values["time"],
	)
}

func isTelegramConfigConfigured(cfg TelegramConfig) bool {
	return strings.TrimSpace(cfg.BotToken) != "" && len(cfg.ChatIDs) > 0
}

func resolveAuditTelegramConfig() TelegramConfig {
	auditCfg := config.Audit.Telegram
	customCfg := TelegramConfig{
		BotToken:             auditCfg.BotToken,
		ChatIDs:              auditCfg.ChatIDs,
		NotificationTemplate: auditCfg.NotificationTemplate,
	}

	if auditCfg.UseGlobal || !isTelegramConfigConfigured(customCfg) {
		return config.Telegram
	}

	return customCfg
}

// escapeMarkdownV2 escapes special characters for Telegram MarkdownV2 parse_mode
// See https://core.telegram.org/bots/api#markdownv2-style
func escapeMarkdownV2(text string) string {
	// 定义需要转义的字符
	// _ * [ ] ( ) ~ ` > # + - = | { } . !
	// 注意，这里只需要转义那些可能出现在用户数据中，与模板自身Markdown语法冲突的字符。
	// 如果用户在模板中使用了，例如 `_` 表示斜体，那么它不应该被转义。
	// 但如果 `denyReason` 中包含 `_`，则需要转义以避免破坏模板格式。
	replacer := strings.NewReplacer(
		"_", "\\_",
		"*", "\\*",
		"[", "\\[",
		"]", "\\]",
		"(", "\\(",
		")", "\\)",
		"~", "\\~",
		"`", "\\`",
		">", "\\>",
		"#", "\\#",
		"+", "\\+",
		"-", "\\-",
		"=", "\\=",
		"|", "\\|",
		"{", "\\{",
		"}", "\\}",
		".", "\\.",
		"!", "\\!",
	)
	return replacer.Replace(text)
}

func sendTelegramNotification(ctx NotificationContext) {
	sendTelegramNotificationWithConfig(config.Telegram, ctx)
}

func sendAuditTelegramNotification(ctx NotificationContext) {
	sendTelegramNotificationWithConfig(resolveAuditTelegramConfig(), ctx)
}

func sendTelegramNotificationWithConfig(telegramCfg TelegramConfig, ctx NotificationContext) {
	if !isTelegramConfigConfigured(telegramCfg) {
		klog.Warning("Telegram config (token or chat_ids) missing or incomplete, skipping notification")
		return
	}

	user := ctx.User
	operation := ctx.Operation
	clusterName := ctx.Cluster
	eventTitle := ctx.Title
	reason := ctx.Reason
	timestamp := ctx.Timestamp
	if timestamp == "" {
		timestamp = time.Now().Format("2006-01-02 15:04:05 MST")
	}


	// 转义所有可能包含特殊 Markdown 字符的变量，确保它们不会破坏模板的 Markdown 格式
	escapedUser := escapeMarkdownV2(user)
	escapedOperation := escapeMarkdownV2(operation)
	escapedClusterName := escapeMarkdownV2(clusterName) // clusterName 可能也包含特殊字符
	escapedEventTitle := escapeMarkdownV2(eventTitle)
	escapedReason := escapeMarkdownV2(reason)
	escapedTimestamp := escapeMarkdownV2(timestamp)
	_ = escapedUser
	_ = escapedOperation
	_ = escapedClusterName
	_ = escapedReason
	_ = escapedTimestamp

	// 使用配置的模板，如果模板为空，则使用默认模板
	template := telegramCfg.NotificationTemplate
	if template == "" {
		template = "⚠️ *Kubernetes Deletion Blocked*\n" +
			"--------------------------------\n" +
			"👤 *User:* `%s`\n" +
			"🔨 *Operation:* `%s`\n" +
			"☸️ *Cluster:* `%s`\n" +
			"🚫 *Reason:* %s\n" +
			"🕒 *Time:* %s"
		if escapedEventTitle == "" {
			escapedEventTitle = "Kubernetes Deletion Blocked"
		}
		template = strings.Replace(template, "Kubernetes Deletion Blocked", escapedEventTitle, 1)
		klog.V(4).Info("Using default Telegram notification template.")
	} else {
		klog.V(4).Infof("Using custom Telegram notification template: %s", template)
	}

	// 将转义后的变量填充到模板中
	message := renderNotificationTemplate(telegramCfg.NotificationTemplate, ctx)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				klog.Errorf("[PANIC RECOVERED] Telegram notification goroutine crashed: %v", r)
			}
		}()

		apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", telegramCfg.BotToken)

		for _, chatID := range telegramCfg.ChatIDs {
			values := url.Values{}
			values.Add("chat_id", chatID)
			values.Add("text", message)
			values.Add("parse_mode", "MarkdownV2") // 明确指定 MarkdownV2

			resp, err := httpClient.PostForm(apiURL, values)
			if err != nil {
				klog.Errorf("Failed to send Telegram notification to chatID %s (URL: %s): %v", chatID, apiURL, err)
				continue
			}

			body, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				klog.Errorf("Telegram API error for chatID %s. Status: %d, Response: %s", chatID, resp.StatusCode, string(body))
			} else {
				klog.Infof("Telegram notification successfully sent to chatID %s", chatID)
			}
		}
	}()
}

func validate(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		klog.Errorf("Failed to read request body: %v", err)
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}

	var review v1.AdmissionReview
	if err := json.Unmarshal(body, &review); err != nil {
		klog.Errorf("Failed to unmarshal admission review: %v", err)
		http.Error(w, "Failed to unmarshal", http.StatusBadRequest)
		return
	}

	if review.Request == nil {
		klog.Error("AdmissionReview request is nil")
		http.Error(w, "Invalid request: review.Request is nil", http.StatusBadRequest)
		return
	}

	reqUID := review.Request.UID
	user := review.Request.UserInfo.Username
	kind := review.Request.Kind.Kind
	name := review.Request.Name
	namespace := review.Request.Namespace
	operation := string(review.Request.Operation)
	resourceDesc := formatResource(kind, name, namespace)
	opDesc := formatOperation(operation, kind, name, namespace)

	klog.V(4).Infof("[Request %s] Received: User=%s, Op=%s, Resource=%s/%s, NS=%s", reqUID, user, operation, kind, name, namespace)

	if review.Request.Operation == v1.Create || review.Request.Operation == v1.Update {
		handleCreateOrUpdateAudit(review.Request)
		sendResponse(w, reqUID, true, "")
		return
	}

	if !config.Enabled {
		klog.Infof("[Request %s] Allowed: Global interceptor is DISABLED", reqUID)
		emitAuditRecord(review.Request, auditDecisionAllowed, "Deletion allowed because the interceptor is globally disabled.", auditPolicyInterceptorOff, false, "")
		sendResponse(w, reqUID, true, "Interceptor is globally disabled.")
		return
	}

	if review.Request.Operation != v1.Delete {
		klog.V(4).Infof("[Request %s] Allowed: Operation is %s (not DELETE)", reqUID, operation)
		sendResponse(w, reqUID, true, fmt.Sprintf("Operation %s is not subject to deletion interception.", operation))
		return
	}

	if (kind == "Deployment" && namespace == webhookNs && name == webhookDeploy) ||
		(kind == "Service" && namespace == webhookNs && name == webhookSvc) ||
		(kind == "Namespace" && name == webhookNs) {
		klog.Infof("[Request %s] Allowed: Self-preservation rule matched for %s '%s' in namespace '%s'", reqUID, kind, name, namespace)
		emitAuditRecord(review.Request, auditDecisionAllowed, "Deletion allowed because self-preservation rule matched.", auditPolicySelfPreservation, false, "")
		sendResponse(w, reqUID, true, "Allowing deletion of self-webhook components.")
		return
	}

	if matched, action, pattern, matcher := matchUserPolicy(user); matched {
		switch action {
		case userActionAllow:
			reason := fmt.Sprintf("Delete request allowed: user '%s' matched user policy pattern '%s' (%s).", user, pattern, matcher)
			klog.Infof("[Request %s] Allowed: %s", reqUID, reason)
			emitAuditRecord(review.Request, auditDecisionAllowed, reason, fmt.Sprintf("delete_user_policy_allow:%s", pattern), false, "")
			sendResponse(w, reqUID, true, "")
			return
		case userActionObserve:
			reason := fmt.Sprintf("Observed delete request for %s: user '%s' matched user policy pattern '%s' (%s). Operation allowed.", resourceDesc, user, pattern, matcher)
			klog.Infof("[Request %s] Observed: %s", reqUID, reason)
			sendTelegramNotification(buildSmartNotificationContext(reqUID, user, kind, name, namespace, operation, opDesc, "observed", reason))
			emitAuditRecord(review.Request, auditDecisionAllowed, reason, fmt.Sprintf("delete_user_policy_observe:%s", pattern), true, reason)
			sendResponse(w, reqUID, true, "")
			return
		case userActionDeny:
			denyReason := fmt.Sprintf("User policy blocked delete for %s: user '%s' matched user policy pattern '%s' (%s).", resourceDesc, user, pattern, matcher)
			klog.Warningf("[Request %s] DENIED: %s", reqUID, denyReason)
			sendTelegramNotification(buildSmartNotificationContext(reqUID, user, kind, name, namespace, operation, opDesc, "blocked", denyReason))
			emitAuditRecord(review.Request, auditDecisionBlocked, denyReason, fmt.Sprintf("delete_user_policy_deny:%s", pattern), true, denyReason)
			sendResponse(w, reqUID, false, denyReason)
			return
		}
	}

	allowed := true
	var denyReason string

	for _, rule := range config.Protected {
		if strings.EqualFold(rule.Kind, kind) {
			target := name
			for _, pattern := range rule.Names {
				g, err := glob.Compile(pattern)
				if err != nil {
					klog.Errorf("Invalid glob pattern in config: '%s' for kind '%s', error: %v. This rule will be skipped.", pattern, kind, err)
					continue
				}

				if g.Match(target) {
					allowed = false
					denyReason = fmt.Sprintf("Protected resource: Cannot delete %s '%s' (matched pattern: '%s').", kind, target, pattern)

					klog.Warningf("[Request %s] DENIED: %s. User: %s, Resource: %s/%s, NS: %s", reqUID, denyReason, user, kind, name, namespace)

					sendTelegramNotification(buildSmartNotificationContext(reqUID, user, kind, name, namespace, operation, opDesc, "blocked", denyReason))
					emitAuditRecord(review.Request, auditDecisionBlocked, denyReason, fmt.Sprintf("protected_rule:%s", pattern), true, denyReason)

					goto respond
				}
			}
		}
	}

respond:
	if allowed {
		klog.Infof("[Request %s] Allowed: No protected rules matched for %s '%s' in namespace '%s'", reqUID, kind, name, namespace)
		emitAuditRecord(review.Request, auditDecisionAllowed, "Deletion allowed because no protected rule matched.", auditPolicyDeleteAudit, false, "")
	}
	sendResponse(w, reqUID, allowed, denyReason)
}

func sendResponse(w http.ResponseWriter, uid types.UID, allowed bool, message string) {
	resp := v1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview"},
		Response: &v1.AdmissionResponse{
			UID:     uid,
			Allowed: allowed,
		},
	}
	if !allowed {
		resp.Response.Result = &metav1.Status{
			Message: message,
			Code:    http.StatusForbidden,
		}
	}

	jsonResp, err := json.Marshal(resp)
	if err != nil {
		klog.Errorf("Failed to marshal AdmissionReview response: %v", err)
		http.Error(w, "Failed to marshal response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonResp)
}

// healthz 处理函数，简单返回 200 OK
func healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "ok")
}

func main() {
	klog.InitFlags(nil)
	flag.Parse()
	flag.Set("logtostderr", "true")

	if err := loadConfig(*configFile); err != nil {
		klog.Fatalf("Failed to load config from %s: %v", *configFile, err)
	}

	auditor, err := newAuditManager(config.Audit)
	if err != nil {
		klog.Fatalf("Failed to initialize audit manager: %v", err)
	}
	admissionAuditor = auditor

	klog.Info("=========================================================")
	klog.Info("Starting Kubernetes Delete Interceptor Admission Webhook")
	klog.Infof("Configuration File: %s", *configFile)
	klog.Infof("Interceptor Enabled: %v", config.Enabled)
	if config.Enabled {
		klog.Infof("Cluster Name for Notifications: %s", config.ClusterName)
		if config.Telegram.BotToken != "" {
			maskedToken := config.Telegram.BotToken
			if len(maskedToken) > 5 {
				maskedToken = maskedToken[:5] + "..."
			}
			klog.Infof("Telegram Notifications configured (Token prefix: %s, Chat IDs: %d)", maskedToken, len(config.Telegram.ChatIDs))
			if len(config.Telegram.ChatIDs) == 0 {
				klog.Warning("Telegram Bot Token is provided, but no Chat IDs are configured. Notifications will NOT be sent.")
			}
			if config.Telegram.NotificationTemplate != "" {
				klog.Infof("Using custom notification template.")
			} else {
				klog.Infof("Using default notification template.")
			}
		} else {
			klog.Warning("Telegram Bot Token is empty in config file. Telegram notifications will NOT be sent.")
		}
		klog.Infof("Loaded %d protected rules for resource kinds: %v", len(config.Protected), func() []string {
			kinds := make([]string, len(config.Protected))
			for i, r := range config.Protected {
				kinds[i] = r.Kind
			}
			return kinds
		}())
		if len(config.UserPolicies) > 0 {
			klog.Infof("Loaded %d user policy rules", len(config.UserPolicies))
		} else {
			klog.Infof("No user policy rules configured.")
		}
	} else {
		klog.Warning("Global interceptor is DISABLED. All deletion requests will be allowed.")
	}
	if config.Audit.Enabled {
		klog.Infof("Audit enabled. Directory: %s, File retention: %d days, Create audit: %v, Update audit: %v, Mongo enabled: %v, Audit telegram uses global: %v", func() string {
			if strings.TrimSpace(config.Audit.Directory) == "" {
				return defaultAuditDirectory
			}
			return config.Audit.Directory
		}(), func() int {
			if config.Audit.FileRetentionDays <= 0 {
				return defaultFileRetentionDays
			}
			return config.Audit.FileRetentionDays
		}(), config.Audit.Create.Enabled, config.Audit.Update.Enabled, config.Audit.Mongo.Enabled, config.Audit.Telegram.UseGlobal || !isTelegramConfigConfigured(TelegramConfig{
			BotToken:             config.Audit.Telegram.BotToken,
			ChatIDs:              config.Audit.Telegram.ChatIDs,
			NotificationTemplate: config.Audit.Telegram.NotificationTemplate,
		}))
	} else {
		klog.Infof("Audit disabled.")
	}
	klog.Info("=========================================================")

	http.HandleFunc("/validate", validate)
	http.HandleFunc("/healthz", healthz)

	cert, err := tls.LoadX509KeyPair(*tlsCert, *tlsKey)
	if err != nil {
		klog.Fatalf("Failed to load TLS certificate and key from '%s' and '%s': %v", *tlsCert, *tlsKey, err)
	}

	server := &http.Server{
		Addr:      ":8443",
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}},
	}

	klog.Info("Webhook server listening on :8443 (HTTPS)...")
	if err := server.ListenAndServeTLS("", ""); err != nil {
		klog.Fatalf("Failed to start webhook server: %v", err)
	}
}
