package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
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

type TelegramConfig struct {
	BotToken           string   `json:"bot_token" yaml:"bot_token"`
	ChatIDs            []string `json:"chat_ids" yaml:"chat_ids"`
	NotificationTemplate string   `json:"notification_template" yaml:"notification_template"` // 新增模板字段
}

type ProtectedRule struct {
	Kind  string   `json:"kind" yaml:"kind"`
	Names []string `json:"names" yaml:"names"`
}

type Config struct {
	Enabled     bool            `json:"enabled" yaml:"enabled"`
	ClusterName string          `json:"cluster_name" yaml:"cluster_name"`
	Telegram    TelegramConfig  `json:"telegram" yaml:"telegram"`
	Protected   []ProtectedRule `json:"protected" yaml:"protected"`
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

func sendTelegramNotification(user string, operation string, clusterName string, denyReason string) {
	if config.Telegram.BotToken == "" || len(config.Telegram.ChatIDs) == 0 {
		klog.Warning("Telegram config (token or chat_ids) missing or incomplete, skipping notification")
		return
	}

	timestamp := time.Now().Format("2006-01-02 15:04:05 MST")

	// 转义所有可能包含特殊 Markdown 字符的变量，确保它们不会破坏模板的 Markdown 格式
	escapedUser := escapeMarkdownV2(user)
	escapedOperation := escapeMarkdownV2(operation)
	escapedClusterName := escapeMarkdownV2(clusterName) // clusterName 可能也包含特殊字符
	escapedDenyReason := escapeMarkdownV2(denyReason)
	escapedTimestamp := escapeMarkdownV2(timestamp)

	// 使用配置的模板，如果模板为空，则使用默认模板
	template := config.Telegram.NotificationTemplate
	if template == "" {
		template = "⚠️ *Kubernetes Deletion Blocked*\n" +
			"--------------------------------\n" +
			"👤 *User:* `%s`\n" +
			"🔨 *Operation:* `%s`\n" +
			"☸️ *Cluster:* `%s`\n" +
			"🚫 *Reason:* %s\n" +
			"🕒 *Time:* %s"
		klog.V(4).Info("Using default Telegram notification template.")
	} else {
		klog.V(4).Infof("Using custom Telegram notification template: %s", template)
	}

	// 将转义后的变量填充到模板中
	message := fmt.Sprintf(
		template,
		escapedUser, escapedOperation, escapedClusterName, escapedDenyReason, escapedTimestamp,
	)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				klog.Errorf("[PANIC RECOVERED] Telegram notification goroutine crashed: %v", r)
			}
		}()

		apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", config.Telegram.BotToken)

		for _, chatID := range config.Telegram.ChatIDs {
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

	klog.V(4).Infof("[Request %s] Received: User=%s, Op=%s, Resource=%s/%s, NS=%s", reqUID, user, operation, kind, name, namespace)

	if !config.Enabled {
		klog.Infof("[Request %s] Allowed: Global interceptor is DISABLED", reqUID)
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
		sendResponse(w, reqUID, true, "Allowing deletion of self-webhook components.")
		return
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

					opDesc := fmt.Sprintf("DELETE %s %s (NS: %s)", kind, target, namespace)
					sendTelegramNotification(user, opDesc, config.ClusterName, denyReason)

					goto respond
				}
			}
		}
	}

respond:
	if allowed {
		klog.Infof("[Request %s] Allowed: No protected rules matched for %s '%s' in namespace '%s'", reqUID, kind, name, namespace)
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
	} else {
		klog.Warning("Global interceptor is DISABLED. All deletion requests will be allowed, and no notifications will be sent.")
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
