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
	"time" // 确保导入了 time 包

	"github.com/gobwas/glob"
	v1 "k8s.io/api/admission/v1" // 明确指定 v1
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2" // 确保使用 klog/v2
)

var (
	tlsCert       = flag.String("tlscert", "/etc/certs/tls.crt", "TLS certificate file")
	tlsKey        = flag.String("tlskey", "/etc/certs/tls.key", "TLS key file")
	configFile    = flag.String("config", "/etc/config/protected.yaml", "Path to protected config file")
	webhookNs     = "webhook-system"       // webhook 所在的 ns，避免拦截自己
	webhookDeploy = "delete-interceptor"   // webhook 的 Deployment 名称
	webhookSvc    = "delete-interceptor-svc" // webhook 的 Service 名称
)

// 定义带超时的 HTTP Client，防止通知请求卡死
var httpClient = &http.Client{
	Timeout: 10 * time.Second,
}

type TelegramConfig struct {
	BotToken string   `json:"bot_token"`
	ChatIDs  []string `json:"chat_ids"`
}

type ProtectedRule struct {
	Kind  string   `json:"kind"`
	Names []string `json:"names"`
}

type Config struct {
	Enabled     bool            `json:"enabled"`      // 全局开关
	ClusterName string          `json:"cluster_name"` // 集群名称
	Telegram    TelegramConfig  `json:"telegram"`     // Telegram 配置
	Protected   []ProtectedRule `json:"protected"`
}

var config Config

func loadConfig(file string) error {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return fmt.Errorf("failed to read config file '%s': %w", file, err)
	}
	if err := json.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("failed to unmarshal config from '%s': %w", file, err)
	}
	return nil
}

// 异步发送 Telegram 通知 (包含 Panic 恢复和错误处理)
func sendTelegramNotification(user string, operation string, clusterName string, denyReason string) {
	// 基础检查
	if config.Telegram.BotToken == "" || len(config.Telegram.ChatIDs) == 0 {
		klog.Warning("Telegram config (token or chat_ids) missing or incomplete, skipping notification")
		return
	}

	// 构造消息内容
	timestamp := time.Now().Format("2006-01-02 15:04:05 MST") // 添加时区信息
	message := fmt.Sprintf(
		"⚠️ *Kubernetes Deletion Blocked*\n"+
			"--------------------------------\n"+
			"👤 *User:* `%s`\n"+
			"🔨 *Operation:* `%s`\n"+
			"☸️ *Cluster:* `%s`\n"+
			"🚫 *Reason:* %s\n"+
			"🕒 *Time:* %s",
		user, operation, clusterName, denyReason, timestamp,
	)

	// 启动异步 Goroutine
	go func() {
		// Panic Recover：防止通知逻辑崩溃导致整个程序退出
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
			values.Add("parse_mode", "Markdown") // 支持 Markdown 格式

			// 使用带超时的 Client 发送请求
			resp, err := httpClient.PostForm(apiURL, values)
			if err != nil {
				// 网络错误，连接超时等
				klog.Errorf("Failed to send Telegram notification to chatID %s (URL: %s): %v", chatID, apiURL, err)
				continue
			}

			// 读取响应体以便调试
			body, _ := ioutil.ReadAll(resp.Body) // 忽略读取错误，反正只用于日志
			resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				// API 返回错误（如 Token 无效，ChatID 错误）
				klog.Errorf("Telegram API error for chatID %s. Status: %d, Response: %s", chatID, resp.StatusCode, string(body))
			} else {
				// 发送成功
				klog.Infof("Telegram notification successfully sent to chatID %s", chatID)
			}
		}
	}()
}

func validate(w http.ResponseWriter, r *http.Request) {
	// 1. 读取 Body
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		klog.Errorf("Failed to read request body: %v", err)
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}

	// 2. 解析 Review 对象
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

	// 3. 检查全局开关
	if !config.Enabled {
		klog.Infof("[Request %s] Allowed: Global interceptor is DISABLED", reqUID)
		sendResponse(w, reqUID, true, "Interceptor is globally disabled.")
		return
	}

	// 4. 只拦截删除操作
	if review.Request.Operation != v1.OperationDelete {
		klog.V(4).Infof("[Request %s] Allowed: Operation is %s (not DELETE)", reqUID, operation)
		sendResponse(w, reqUID, true, fmt.Sprintf("Operation %s is not subject to deletion interception.", operation))
		return
	}

	// 5. 排除自身 (Self-Exclusion) - 防止无法删除 Webhook 自身导致死锁
	if (kind == "Deployment" && namespace == webhookNs && name == webhookDeploy) ||
		(kind == "Service" && namespace == webhookNs && name == webhookSvc) ||
		(kind == "Namespace" && name == webhookNs) {
		klog.Infof("[Request %s] Allowed: Self-preservation rule matched for %s '%s' in namespace '%s'", reqUID, kind, name, namespace)
		sendResponse(w, reqUID, true, "Allowing deletion of self-webhook components.")
		return
	}

	// 6. 规则匹配逻辑
	allowed := true
	var denyReason string

	for _, rule := range config.Protected {
		// 匹配 Kind
		if strings.EqualFold(rule.Kind, kind) {
			target := name
			// Namespace 资源没有 namespace 字段，name 就是目标
			// 对于 Cluster-scoped resources (e.g. ClusterRole, CustomResourceDefinition) 也没有 namespace
			// current target is already 'name' which is correct for both namespaced and cluster-scoped resources
			// The original `if strings.EqualFold(kind, "Namespace") { target = name }` is redundant here
			// but harmless, 'name' is already the correct target.

			// 匹配 Names (支持 Glob)
			for _, pattern := range rule.Names {
				g, err := glob.Compile(pattern)
				if err != nil {
					klog.Errorf("Invalid glob pattern in config: '%s' for kind '%s', error: %v. This rule will be skipped.", pattern, kind, err)
					continue // 跳过此无效模式，继续检查下一个规则
				}

				if g.Match(target) {
					// --- 命中规则，拒绝删除 ---
					allowed = false
					denyReason = fmt.Sprintf("Protected resource: Cannot delete %s '%s' (matched pattern: '%s').", kind, target, pattern)

					klog.Warningf("[Request %s] DENIED: %s. User: %s, Resource: %s/%s, NS: %s", reqUID, denyReason, user, kind, name, namespace)

					// 7. 触发异步通知
					// 构造操作描述
					opDesc := fmt.Sprintf("DELETE %s %s (NS: %s)", kind, target, namespace)
					sendTelegramNotification(user, opDesc, config.ClusterName, denyReason)

					goto respond // 找到匹配规则后，直接跳转到响应，不再检查其他规则
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
			Code:    http.StatusForbidden, // 403 Forbidden
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

func main() {
	// 初始化 klog flags
	klog.InitFlags(nil)
	flag.Parse()

	// 强制将日志输出到 stderr 方便容器收集
	flag.Set("logtostderr", "true")

	// 加载配置
	if err := loadConfig(*configFile); err != nil {
		klog.Fatalf("Failed to load config from %s: %v", *configFile, err)
	}

	// 打印启动日志
	klog.Info("=========================================================")
	klog.Info("Starting Kubernetes Delete Interceptor Admission Webhook")
	klog.Infof("Configuration File: %s", *configFile)
	klog.Infof("Interceptor Enabled: %v", config.Enabled)
	if config.Enabled {
		klog.Infof("Cluster Name for Notifications: %s", config.ClusterName)
		if config.Telegram.BotToken != "" {
			maskedToken := config.Telegram.BotToken
			if len(maskedToken) > 5 { // 至少显示前5位，防止过短的token被完全隐藏
				maskedToken = maskedToken[:5] + "..."
			}
			klog.Infof("Telegram Notifications configured (Token prefix: %s, Chat IDs: %d)", maskedToken, len(config.Telegram.ChatIDs))
			if len(config.Telegram.ChatIDs) == 0 {
				klog.Warning("Telegram Bot Token is provided, but no Chat IDs are configured. Notifications will NOT be sent.")
			}
		} else {
			klog.Warning("Telegram Bot Token is empty. Telegram notifications will NOT be sent.")
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

	// 加载证书
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
