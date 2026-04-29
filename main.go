package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
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
)

var httpClient = &http.Client{
	Timeout: 10 * time.Second,
}

const (
	userActionAllow   = "allow"
	userActionObserve = "observe"
	userActionDeny    = "deny"
	defaultNotificationTemplate = "" +
		"*动作*: {{action_icon}} `{{action_label}}`\n" +
		"*集群*: `{{cluster}}`\n" +
		"*资源*: `{{resource}}`\n" +
		"*用户*: `{{user}}`\n" +
		"*操作*: `{{operation_label}}`\n" +
		"*原因*: {{reason}}\n" +
		"*请求ID*: `{{request_uid}}`\n\n" +
		"{{title_icon}} *{{title}}*   `{{time}}`"
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
	TitleIcon      string
	Action         string
	ActionIcon     string
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
	ResourceType   string
	ResourceName   string
	ChangeDetails string
	AttachmentName string
	AttachmentContent string
	RequestUID     string
}

type Config struct {
	Enabled      bool             `json:"enabled" yaml:"enabled"`
	ClusterName  string           `json:"cluster_name" yaml:"cluster_name"`
	Telegram     TelegramConfig   `json:"telegram" yaml:"telegram"`
	Protected    []ProtectedRule  `json:"protected" yaml:"protected"`
	UserPolicies []UserPolicyRule `json:"user_policies" yaml:"user_policies"`
	Audit        AuditConfig      `json:"audit" yaml:"audit"`
	Lifecycle    LifecycleConfig  `json:"lifecycle" yaml:"lifecycle"`
	Notifications NotificationControlConfig `json:"notifications" yaml:"notifications"`
	DeleteConfirmation DeleteConfirmationConfig `json:"delete_confirmation" yaml:"delete_confirmation"`
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

func formatNotificationUser(user string) string {
	parts := strings.Split(user, ":")
	if len(parts) == 4 && parts[0] == "system" && parts[1] == "serviceaccount" {
		return parts[3]
	}

	return user
}

func isSelfManagedAdmissionResource(kind string, name string) bool {
	return strings.EqualFold(kind, "ValidatingWebhookConfiguration") && name == "delete-interceptor.k8s.io"
}

func shouldBypassForSelfPreservation(req *v1.AdmissionRequest) (bool, string) {
	if req == nil {
		return false, ""
	}

	if req.Namespace == webhookNs {
		return true, fmt.Sprintf("Allowing request because namespace '%s' is reserved for the webhook's own runtime resources.", webhookNs)
	}

	if strings.EqualFold(req.Kind.Kind, "Namespace") && req.Name == webhookNs {
		return true, fmt.Sprintf("Allowing request because namespace '%s' is reserved for the webhook itself.", webhookNs)
	}

	if isSelfManagedAdmissionResource(req.Kind.Kind, req.Name) {
		return true, fmt.Sprintf("Allowing request because admission resource '%s' is owned by the webhook itself.", req.Name)
	}

	return false, ""
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
	actionLabel := displayNotificationActionLabel(action)

	return NotificationContext{
		Title:          displayNotificationTitle(operation, action),
		TitleIcon:      displayNotificationTitleIcon(operation, action),
		Action:         action,
		ActionIcon:     displayNotificationActionIcon(action),
		ActionLabel:    actionLabel,
		User:           formatNotificationUser(user),
		Operation:      operation,
		OperationType:  strings.ToUpper(strings.TrimSpace(operation)),
		OperationLabel: displayNotificationOperationLabel(operation),
		Cluster:        config.ClusterName,
		Reason:         reason,
		Timestamp:      time.Now().Format("2006-01-02 15:04:05 MST"),
		Kind:           kind,
		Name:           name,
		Namespace:      formatNamespace(namespace),
		Resource:       formatResource(kind, name, namespace),
		ResourceType:   kind,
		ResourceName:   name,
		ChangeDetails: reason,
		RequestUID:     string(reqUID),
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

func displayNotificationActionLabel(action string) string {
	switch strings.ToLower(strings.TrimSpace(action)) {
	case "blocked":
		return "拦截"
	case "allowed":
		return "放行"
	case "observed":
		return "观察放行"
	case lifecycleEventStarted:
		return "启动"
	case lifecycleEventStopped:
		return "停止"
	case lifecycleEventUnexpectedStop:
		return "异常停止"
	default:
		return action
	}
}

func displayNotificationActionIcon(action string) string {
	switch strings.ToLower(strings.TrimSpace(action)) {
	case "blocked":
		return "⛔"
	case "allowed":
		return "✅"
	case "observed":
		return "👀"
	case lifecycleEventStarted:
		return "🟢"
	case lifecycleEventStopped:
		return "🔴"
	case lifecycleEventUnexpectedStop:
		return "⚠️"
	default:
		return "ℹ️"
	}
}

func displayNotificationOperationLabel(operation string) string {
	switch strings.ToUpper(strings.TrimSpace(operation)) {
	case "CREATE":
		return "创建"
	case "UPDATE":
		return "更新"
	case "DELETE":
		return "删除"
	case lifecycleOperationType:
		return "生命周期"
	default:
		return strings.ToUpper(strings.TrimSpace(operation))
	}
}

func displayNotificationTitle(operation string, action string) string {
	switch strings.ToUpper(strings.TrimSpace(operation)) {
	case "CREATE":
		if strings.EqualFold(strings.TrimSpace(action), "blocked") {
			return "K8s 创建操作拦截通知"
		}
		return "K8s 创建操作审计通知"
	case "UPDATE":
		if strings.EqualFold(strings.TrimSpace(action), "blocked") {
			return "K8s 更新操作拦截通知"
		}
		return "K8s 更新操作审计通知"
	case "DELETE":
		switch strings.ToLower(strings.TrimSpace(action)) {
		case "blocked":
			return "K8s 删除操作拦截通知"
		case "allowed", "observed":
			return "K8s 删除操作放行通知"
		default:
			return "K8s 删除操作通知"
		}
	default:
		if strings.EqualFold(strings.TrimSpace(action), "blocked") {
			return "K8s 资源操作拦截通知"
		}
		return "K8s 资源操作通知"
	}
}

func displayNotificationTitleIcon(operation string, action string) string {
	switch strings.ToUpper(strings.TrimSpace(operation)) {
	case "CREATE":
		if strings.EqualFold(strings.TrimSpace(action), "blocked") {
			return "⛔"
		}
		return "🆕"
	case "UPDATE":
		if strings.EqualFold(strings.TrimSpace(action), "blocked") {
			return "⛔"
		}
		return "✏️"
	case "DELETE":
		switch strings.ToLower(strings.TrimSpace(action)) {
		case "blocked":
			return "⛔"
		case "allowed", "observed":
			return "✅"
		default:
			return "🗑️"
		}
	default:
		if strings.EqualFold(strings.TrimSpace(action), "blocked") {
			return "⛔"
		}
		return "ℹ️"
	}
}

func buildSmartNotificationContext(reqUID types.UID, user string, kind string, name string, namespace string, operationType string, operation string, action string, reason string) NotificationContext {
	return NotificationContext{
		Title:          displayNotificationTitle(operationType, action),
		TitleIcon:      displayNotificationTitleIcon(operationType, action),
		Action:         action,
		ActionIcon:     displayNotificationActionIcon(action),
		ActionLabel:    displayNotificationActionLabel(action),
		User:           formatNotificationUser(user),
		Operation:      operation,
		OperationType:  strings.ToUpper(strings.TrimSpace(operationType)),
		OperationLabel: displayNotificationOperationLabel(operationType),
		Cluster:        config.ClusterName,
		Reason:         reason,
		Timestamp:      time.Now().Format("2006-01-02 15:04:05 MST"),
		Kind:           kind,
		Name:           name,
		Namespace:      formatNamespace(namespace),
		Resource:       formatResource(kind, name, namespace),
		ResourceType:   kind,
		ResourceName:   name,
		ChangeDetails: reason,
		RequestUID:     string(reqUID),
	}
}

func renderNotificationTemplate(template string, ctx NotificationContext) string {
	if template == "" {
		template = defaultNotificationTemplate
	}

	values := map[string]string{
		"title":        escapeMarkdownV2(ctx.Title),
		"title_icon":   escapeMarkdownV2(ctx.TitleIcon),
		"action":       escapeMarkdownV2(ctx.Action),
		"action_icon":  escapeMarkdownV2(ctx.ActionIcon),
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
		"resource_type": escapeMarkdownV2(ctx.ResourceType),
		"resource_name": escapeMarkdownV2(ctx.ResourceName),
		"change_details": escapeMarkdownV2(ctx.ChangeDetails),
		"request_uid":  escapeMarkdownV2(ctx.RequestUID),
	}

	if strings.Contains(template, "{{") {
		replacerArgs := []string{
			"{{title}}", values["title"],
			"{{title_icon}}", values["title_icon"],
			"{{action}}", values["action"],
			"{{action_icon}}", values["action_icon"],
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
			"{{resource_type}}", values["resource_type"],
			"{{resource_name}}", values["resource_name"],
			"{{change_details}}", values["change_details"],
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
	sendTelegramNotificationWithConfigMode(notificationChannelDefault, config.Telegram, ctx, true)
}

func sendAuditTelegramNotification(ctx NotificationContext) {
	sendTelegramNotificationWithConfigMode(notificationChannelAudit, resolveAuditTelegramConfig(), ctx, true)
}

func sendTelegramNotificationWithConfigSync(telegramCfg TelegramConfig, ctx NotificationContext) {
	sendTelegramNotificationWithConfigMode(notificationChannelDefault, telegramCfg, ctx, false)
}

func sendTelegramNotificationWithConfig(telegramCfg TelegramConfig, ctx NotificationContext) {
	sendTelegramNotificationWithConfigMode(notificationChannelDefault, telegramCfg, ctx, true)
}

func sendLifecycleTelegramNotificationSync(ctx NotificationContext) {
	sendTelegramNotificationWithConfigMode(notificationChannelLifecycle, resolveLifecycleTelegramConfig(), ctx, false)
}

func sendTelegramNotificationWithConfigMode(channel string, telegramCfg TelegramConfig, ctx NotificationContext, async bool) {
	if notifier != nil {
		notifier.Dispatch(channel, telegramCfg, ctx, async)
		return
	}

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

	send := func() {
		defer func() {
			if r := recover(); r != nil {
				klog.Errorf("[PANIC RECOVERED] Telegram notification sender crashed: %v", r)
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
				if ctx.AttachmentContent != "" {
					if err := sendTelegramDocument(telegramCfg.BotToken, chatID, ctx.AttachmentName, ctx.AttachmentContent); err != nil {
						klog.Errorf("Failed to send Telegram attachment to chatID %s: %v", chatID, err)
					}
				}
			}
		}
	}

	if async {
		go send()
		return
	}

	send()
}

func deliverTelegramNotification(telegramCfg TelegramConfig, ctx NotificationContext) error {
	if !isTelegramConfigConfigured(telegramCfg) {
		return fmt.Errorf("telegram config (token or chat_ids) missing or incomplete")
	}

	message := renderNotificationTemplate(telegramCfg.NotificationTemplate, ctx)

	defer func() {
		if r := recover(); r != nil {
			klog.Errorf("[PANIC RECOVERED] Telegram notification sender crashed: %v", r)
		}
	}()

	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", telegramCfg.BotToken)
	successCount := 0
	failures := make([]string, 0)

	for _, chatID := range telegramCfg.ChatIDs {
		values := url.Values{}
		values.Add("chat_id", chatID)
		values.Add("text", message)
		values.Add("parse_mode", "MarkdownV2")

		resp, err := httpClient.PostForm(apiURL, values)
		if err != nil {
			klog.Errorf("Failed to send Telegram notification to chatID %s (URL: %s): %v", chatID, apiURL, err)
			failures = append(failures, fmt.Sprintf("chatID %s post failed: %v", chatID, err))
			continue
		}

		body, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			klog.Errorf("Telegram API error for chatID %s. Status: %d, Response: %s", chatID, resp.StatusCode, string(body))
			failures = append(failures, fmt.Sprintf("chatID %s api status %d", chatID, resp.StatusCode))
			continue
		}

		successCount++
		klog.Infof("Telegram notification successfully sent to chatID %s", chatID)
		if ctx.AttachmentContent != "" {
			if err := sendTelegramDocument(telegramCfg.BotToken, chatID, ctx.AttachmentName, ctx.AttachmentContent); err != nil {
				klog.Errorf("Failed to send Telegram attachment to chatID %s: %v", chatID, err)
				failures = append(failures, fmt.Sprintf("chatID %s document failed: %v", chatID, err))
				continue
			}
		}
	}

	if (successCount == 0 || ctx.AttachmentContent != "") && len(failures) > 0 {
		return fmt.Errorf(strings.Join(failures, "; "))
	}

	return nil
}

func sendTelegramDocument(botToken string, chatID string, fileName string, content string) error {
	if strings.TrimSpace(fileName) == "" {
		fileName = "k8s-change-details.txt"
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	if err := writer.WriteField("chat_id", chatID); err != nil {
		return err
	}

	part, err := writer.CreateFormFile("document", fileName)
	if err != nil {
		return err
	}
	if _, err := part.Write([]byte(content)); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}

	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/sendDocument", botToken)
	req, err := http.NewRequest(http.MethodPost, apiURL, &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("telegram document api status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
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
		handleCreateOrUpdateAuditV2(review.Request)
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

	if bypass, bypassReason := shouldBypassForSelfPreservation(review.Request); bypass {
		klog.Infof("[Request %s] Allowed: Self-preservation rule matched for %s '%s' in namespace '%s'", reqUID, kind, name, namespace)
		emitAuditRecord(review.Request, auditDecisionAllowed, bypassReason, auditPolicySelfPreservation, false, "")
		sendResponse(w, reqUID, true, bypassReason)
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
			reason := fmt.Sprintf("触发删除观察策略，操作已放行。资源: %s。", resourceDesc)
			klog.Infof("[Request %s] Observed: %s", reqUID, reason)
			sendTelegramNotification(buildSmartNotificationContext(reqUID, user, kind, name, namespace, operation, opDesc, "observed", reason))
			emitAuditRecord(review.Request, auditDecisionAllowed, fmt.Sprintf("Observed delete request for %s: user '%s' matched user policy pattern '%s' (%s). Operation allowed.", resourceDesc, user, pattern, matcher), fmt.Sprintf("delete_user_policy_observe:%s", pattern), true, reason)
			sendResponse(w, reqUID, true, "")
			return
		case userActionDeny:
			denyReason := fmt.Sprintf("User policy blocked delete for %s: user '%s' matched user policy pattern '%s' (%s).", resourceDesc, user, pattern, matcher)
			notificationReason := fmt.Sprintf("触发用户删除策略拦截。资源: %s。", resourceDesc)
			klog.Warningf("[Request %s] DENIED: %s", reqUID, denyReason)
			sendTelegramNotification(buildSmartNotificationContext(reqUID, user, kind, name, namespace, operation, opDesc, "blocked", notificationReason))
			emitAuditRecord(review.Request, auditDecisionBlocked, denyReason, fmt.Sprintf("delete_user_policy_deny:%s", pattern), true, notificationReason)
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
					notificationReason := fmt.Sprintf("触发重要资源删除拦截：%s。", resourceDesc)

					klog.Warningf("[Request %s] DENIED: %s. User: %s, Resource: %s/%s, NS: %s", reqUID, denyReason, user, kind, name, namespace)

					if approved, approvalReason := deleteConfirmer.ConsumeApproval(review.Request); approved {
						klog.Infof("[Request %s] Allowed by Telegram delete confirmation: %s", reqUID, approvalReason)
						emitAuditRecord(review.Request, auditDecisionAllowed, approvalReason, fmt.Sprintf("delete_confirmation_approved:%s", pattern), false, "")
						sendResponse(w, reqUID, true, "")
						return
					}

					if pending, pendingReason, pendingPolicy := deleteConfirmer.RequestApproval(review.Request, pattern); pending {
						klog.Infof("[Request %s] Delete requires Telegram confirmation: %s", reqUID, pendingReason)
						emitAuditRecord(review.Request, auditDecisionBlocked, pendingReason, pendingPolicy, true, pendingReason)
						sendResponse(w, reqUID, false, pendingReason)
						return
					}

					sendTelegramNotification(buildSmartNotificationContext(reqUID, user, kind, name, namespace, operation, opDesc, "blocked", notificationReason))
					emitAuditRecord(review.Request, auditDecisionBlocked, denyReason, fmt.Sprintf("protected_rule:%s", pattern), true, notificationReason)

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
	applyNotificationDefaults()
	applyLifecycleDefaults()
	applyDeleteConfirmationDefaults()

	auditor, err := newAuditManager(config.Audit)
	if err != nil {
		klog.Fatalf("Failed to initialize audit manager: %v", err)
	}
	admissionAuditor = auditor

	lifecycleMgr, err := newLifecycleManager(config.Lifecycle)
	if err != nil {
		klog.Fatalf("Failed to initialize lifecycle manager: %v", err)
	}
	serviceLifecycle = lifecycleMgr

	notificationMgr, err := newNotificationManager(config.Notifications)
	if err != nil {
		klog.Fatalf("Failed to initialize notification manager: %v", err)
	}
	notifier = notificationMgr

	deleteConfirmationMgr, err := newDeleteConfirmationManager(config.DeleteConfirmation)
	if err != nil {
		klog.Fatalf("Failed to initialize delete confirmation manager: %v", err)
	}
	deleteConfirmer = deleteConfirmationMgr
	defer deleteConfirmer.Stop()
	defer func() {
		if r := recover(); r != nil {
			if serviceLifecycle != nil {
				serviceLifecycle.HandleUnexpectedTermination(fmt.Sprintf("Webhook main process panicked: %v", r))
			}
			panic(r)
		}
	}()

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
	if config.Lifecycle.Enabled {
		klog.Infof("Lifecycle notifications enabled. Startup: %v, Shutdown: %v, Detect unclean shutdown: %v, Lifecycle telegram uses global: %v", config.Lifecycle.NotifyStartup, config.Lifecycle.NotifyShutdown, config.Lifecycle.DetectUncleanShutdown, config.Lifecycle.Telegram.UseGlobal || !isTelegramConfigConfigured(TelegramConfig{
			BotToken:             config.Lifecycle.Telegram.BotToken,
			ChatIDs:              config.Lifecycle.Telegram.ChatIDs,
			NotificationTemplate: config.Lifecycle.Telegram.NotificationTemplate,
		}))
	} else {
		klog.Infof("Lifecycle notifications disabled.")
	}
	if config.DeleteConfirmation.Enabled {
		klog.Infof("Delete confirmation enabled. State directory: %s, TTL: %ds, Consume window: %ds, Aggregate window: %ds, Rules: %d", config.DeleteConfirmation.StateDirectory, config.DeleteConfirmation.TTLSeconds, config.DeleteConfirmation.ConsumeWindowSeconds, config.DeleteConfirmation.AggregateWindowSeconds, len(config.DeleteConfirmation.Rules))
	} else {
		klog.Infof("Delete confirmation disabled.")
	}
	klog.Infof("Notification control enabled. Dedupe window: %ds, Retry failed on startup: %v, Max retry batch: %d", config.Notifications.DedupeWindowSeconds, config.Notifications.RetryFailedOnStartup, config.Notifications.MaxRetryBatch)
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

	listener, err := tls.Listen("tcp", server.Addr, server.TLSConfig)
	if err != nil {
		klog.Fatalf("Failed to bind webhook server on %s: %v", server.Addr, err)
	}

	klog.Info("Webhook server listening on :8443 (HTTPS)...")
	serverErrCh := make(chan error, 1)
	go func() {
		if err := server.Serve(listener); err != nil {
			serverErrCh <- err
		}
	}()

	if serviceLifecycle != nil {
		serviceLifecycle.HandleStartup()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigCh)

	select {
	case sig := <-sigCh:
		shutdownReason := fmt.Sprintf("实例收到退出信号 '%s'，即将停止服务。", sig.String())
		if serviceLifecycle != nil {
			serviceLifecycle.HandleGracefulShutdown(shutdownReason)
		}

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			klog.Errorf("Failed to gracefully shut down webhook server: %v", err)
		}
	case err := <-serverErrCh:
		if errors.Is(err, http.ErrServerClosed) {
			return
		}
		if serviceLifecycle != nil {
			serviceLifecycle.HandleUnexpectedTermination(fmt.Sprintf("Webhook server exited unexpectedly: %v", err))
		}
		klog.Fatalf("Failed to start webhook server: %v", err)
	}
}
