package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	v1 "k8s.io/api/admission/v1"
	"k8s.io/klog/v2"
)

const (
	inlineChangeDetailsLimit = 1800
	diffValueLimit           = 300
)

var defaultChangeDetailAuditOnlyResources = []string{
	"configmap",
	"configmaps",
	"cm",
	"secret",
	"secrets",
}

type AdmissionChangeDetails struct {
	NotificationText  string
	AuditDetails      string
	AttachmentName    string
	AttachmentContent string
	AuditOnly         bool
	ChangedFieldCount int
}

func buildAdmissionChangeDetails(req *v1.AdmissionRequest) AdmissionChangeDetails {
	if req == nil {
		message := "未获取到请求详情。"
		return AdmissionChangeDetails{NotificationText: message, AuditDetails: message}
	}

	resourceDesc := formatResource(req.Kind.Kind, req.Name, req.Namespace)
	switch req.Operation {
	case v1.Create:
		message := fmt.Sprintf("触发重要资源创建审计：%s。", resourceDesc)
		return AdmissionChangeDetails{NotificationText: message, AuditDetails: message}
	case v1.Update:
		diffText := buildAdmissionObjectDiff(req.OldObject.Raw, req.Object.Raw)
		if strings.TrimSpace(diffText) == "" {
			message := fmt.Sprintf("触发重要资源更新审计：%s，未检测到有效字段差异。", resourceDesc)
			return AdmissionChangeDetails{NotificationText: message, AuditDetails: message}
		}

		full := fmt.Sprintf("触发重要资源更新审计：%s。\n%s", resourceDesc, diffText)
		changedFieldCount := countDiffLines(diffText)
		if isChangeDetailAuditOnlyResource(req.Kind.Kind, req.Resource.Resource) {
			message := fmt.Sprintf("触发重要资源更新审计：%s。该资源属于配置/密钥类，变更详情已记录到审计存储，不在通知中展示。变更字段数: %d。", resourceDesc, changedFieldCount)
			return AdmissionChangeDetails{
				NotificationText:  message,
				AuditDetails:      full,
				AuditOnly:         true,
				ChangedFieldCount: changedFieldCount,
			}
		}

		if len([]rune(full)) <= inlineChangeDetailsLimit {
			return AdmissionChangeDetails{NotificationText: full, AuditDetails: full, ChangedFieldCount: changedFieldCount}
		}

		fileName := fmt.Sprintf(
			"k8s-update-diff-%s-%s-%s.txt",
			strings.ToLower(req.Kind.Kind),
			safeFileNamePart(req.Name),
			time.Now().Format("20060102-150405"),
		)
		message := fmt.Sprintf("触发重要资源更新审计：%s。变更详情较多，已作为附件发送。变更字段数: %d。", resourceDesc, changedFieldCount)
		return AdmissionChangeDetails{NotificationText: message, AuditDetails: full, AttachmentName: fileName, AttachmentContent: full, ChangedFieldCount: changedFieldCount}
	case v1.Delete:
		message := fmt.Sprintf("触发重要资源删除拦截：%s。", resourceDesc)
		return AdmissionChangeDetails{NotificationText: message, AuditDetails: message}
	default:
		message := fmt.Sprintf("触发重要资源%s操作审计：%s。", strings.ToUpper(string(req.Operation)), resourceDesc)
		return AdmissionChangeDetails{NotificationText: message, AuditDetails: message}
	}
}

func countDiffLines(diffText string) int {
	count := 0
	for _, line := range strings.Split(diffText, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "+ ") || strings.HasPrefix(trimmed, "- ") || strings.HasPrefix(trimmed, "~ ") {
			count++
		}
	}
	return count
}

func resolveChangeDetailAuditOnlyResources() []string {
	if len(config.Audit.ChangeDetailAuditOnlyResources) == 0 {
		return defaultChangeDetailAuditOnlyResources
	}
	return config.Audit.ChangeDetailAuditOnlyResources
}

func isChangeDetailAuditOnlyResource(kind string, resource string) bool {
	patterns := resolveChangeDetailAuditOnlyResources()
	candidates := auditResourceCandidates(kind, resource)
	for _, pattern := range patterns {
		for _, candidate := range candidates {
			matched, _, err := matchPattern(pattern, candidate)
			if err != nil {
				klog.Errorf("Invalid change detail audit-only resource pattern '%s': %v. This pattern will be skipped.", pattern, err)
				break
			}
			if matched {
				return true
			}
		}
	}
	return false
}

func buildAdmissionObjectDiff(oldRaw []byte, newRaw []byte) string {
	oldFields := map[string]string{}
	newFields := map[string]string{}

	var oldObj interface{}
	var newObj interface{}
	if len(oldRaw) > 0 && json.Unmarshal(oldRaw, &oldObj) == nil {
		flattenJSON("", oldObj, oldFields)
	}
	if len(newRaw) > 0 && json.Unmarshal(newRaw, &newObj) == nil {
		flattenJSON("", newObj, newFields)
	}

	keysMap := map[string]struct{}{}
	for key := range oldFields {
		keysMap[key] = struct{}{}
	}
	for key := range newFields {
		keysMap[key] = struct{}{}
	}

	keys := make([]string, 0, len(keysMap))
	for key := range keysMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	lines := make([]string, 0)
	for _, key := range keys {
		oldValue, oldOK := oldFields[key]
		newValue, newOK := newFields[key]
		if oldOK && newOK && oldValue == newValue {
			continue
		}

		switch {
		case !oldOK && newOK:
			lines = append(lines, fmt.Sprintf("+ %s: %s", key, limitDiffValue(newValue)))
		case oldOK && !newOK:
			lines = append(lines, fmt.Sprintf("- %s: %s", key, limitDiffValue(oldValue)))
		default:
			lines = append(lines, fmt.Sprintf("~ %s: %s -> %s", key, limitDiffValue(oldValue), limitDiffValue(newValue)))
		}
	}

	return strings.Join(lines, "\n")
}

func flattenJSON(prefix string, value interface{}, out map[string]string) {
	if shouldSkipDiffPath(prefix) {
		return
	}

	switch typed := value.(type) {
	case map[string]interface{}:
		if len(typed) == 0 && prefix != "" {
			out[prefix] = "{}"
			return
		}
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			childPath := key
			if prefix != "" {
				childPath = prefix + "." + key
			}
			flattenJSON(childPath, typed[key], out)
		}
	case []interface{}:
		if flattenNamedJSONArray(prefix, typed, out) {
			return
		}
		out[prefix] = compactJSONValue(typed)
	default:
		out[prefix] = compactJSONValue(typed)
	}
}

func flattenNamedJSONArray(prefix string, values []interface{}, out map[string]string) bool {
	if len(values) == 0 || strings.TrimSpace(prefix) == "" {
		return false
	}

	seen := map[string]struct{}{}
	named := make([]struct {
		name string
		obj  map[string]interface{}
	}, 0, len(values))

	for _, value := range values {
		obj, ok := value.(map[string]interface{})
		if !ok {
			return false
		}
		name := strings.TrimSpace(fmt.Sprint(obj["name"]))
		if name == "" {
			return false
		}
		if _, exists := seen[name]; exists {
			return false
		}
		seen[name] = struct{}{}
		named = append(named, struct {
			name string
			obj  map[string]interface{}
		}{name: name, obj: obj})
	}

	sort.Slice(named, func(i, j int) bool { return named[i].name < named[j].name })
	for _, item := range named {
		childPath := fmt.Sprintf("%s[name=%s]", prefix, item.name)
		flattenJSON(childPath, item.obj, out)
	}
	return true
}

func shouldSkipDiffPath(path string) bool {
	switch path {
	case "metadata.managedFields",
		"metadata.resourceVersion",
		"metadata.generation",
		"metadata.creationTimestamp",
		"metadata.uid",
		"metadata.annotations.kubectl.kubernetes.io/last-applied-configuration":
		return true
	default:
		return false
	}
}

func compactJSONValue(value interface{}) string {
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%v", value)
	}
	return string(payload)
}

func limitDiffValue(value string) string {
	if len([]rune(value)) <= diffValueLimit {
		return value
	}

	runes := []rune(value)
	return string(runes[:diffValueLimit]) + "...(truncated)"
}

func safeFileNamePart(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "cluster-scoped"
	}

	replacer := strings.NewReplacer(
		"/", "-",
		"\\", "-",
		":", "-",
		"*", "-",
		"?", "-",
		"\"", "-",
		"<", "-",
		">", "-",
		"|", "-",
		" ", "-",
	)
	return replacer.Replace(value)
}

func handleCreateOrUpdateAuditV2(req *v1.AdmissionRequest, rollbackID string) {
	if matched, pattern, matcher := matchGlobalWhitelist(req.UserInfo.Username); matched {
		resourceDesc := formatResource(req.Kind.Kind, req.Name, req.Namespace)
		reason := fmt.Sprintf("%s request bypassed all controls for %s: user '%s' matched global whitelist pattern '%s' (%s). Audit recorded only.", strings.ToUpper(string(req.Operation)), resourceDesc, req.UserInfo.Username, pattern, matcher)
		emitAuditRecord(req, auditDecisionAllowed, reason, fmt.Sprintf("%s:%s", auditPolicyGlobalWhitelist, pattern), false, "")
		return
	}

	if !isAuditOperationEnabled(req.Operation) {
		return
	}

	operationLabel := strings.ToUpper(string(req.Operation))
	resourceDesc := formatResource(req.Kind.Kind, req.Name, req.Namespace)
	reason := fmt.Sprintf("%s request allowed and audit recorded for %s.", operationLabel, resourceDesc)
	matchedPolicy := auditPolicyCreateAudit
	if req.Operation == v1.Update {
		matchedPolicy = auditPolicyUpdateAudit
	}

	notified := false
	notificationReason := ""
	changeDetails := buildAdmissionChangeDetails(req)

	if shouldNotify, userPattern, userMatcher, resourcePattern, resourceMatcher, resourceCandidate := shouldNotifyMutationAudit(req); shouldNotify {
		notificationReason = changeDetails.NotificationText
		ctx := buildSmartNotificationContext(
			req.UID,
			req.UserInfo.Username,
			req.Kind.Kind,
			req.Name,
			req.Namespace,
			string(req.Operation),
			formatOperation(string(req.Operation), req.Kind.Kind, req.Name, req.Namespace),
			auditDecisionAllowed,
			notificationReason,
		)
		ctx.ChangeDetails = changeDetails.NotificationText
		ctx.AttachmentName = changeDetails.AttachmentName
		ctx.AttachmentContent = changeDetails.AttachmentContent
		ctx.RollbackID = rollbackID
		sendAuditTelegramNotification(ctx)
		notified = true
		matchedPolicy = fmt.Sprintf("%s_notify:%s|%s", strings.ToLower(string(req.Operation)), userPattern, resourcePattern)
		if changeDetails.AuditOnly {
			matchedPolicy += ":change_detail_audit_only"
		}
		klog.V(4).Infof("Mutation audit notification matched user pattern '%s' (%s), resource pattern '%s' (%s, candidate '%s')", userPattern, userMatcher, resourcePattern, resourceMatcher, resourceCandidate)
	}

	emitAuditRecordWithChangeDetails(req, auditDecisionAllowed, reason, matchedPolicy, notified, notificationReason, changeDetails.AuditDetails)
}
