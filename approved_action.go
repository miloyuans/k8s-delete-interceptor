package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

type approvedActionResult struct {
	Supported bool
	Executed  bool
	Message   string
}

// executeApprovedAdmissionAction intentionally does not mutate Kubernetes resources.
//
// A validating admission webhook cannot safely resume a DELETE request after it has
// returned a denial to the API server. Replaying the DELETE from this service would
// use the interceptor ServiceAccount and would make the audit program a cluster
// operator, which is both unsafe and misleading. The supported flow is therefore:
// Telegram/Web approval creates a short lived, single-use grant for the original
// Kubernetes user; the original user or automation retries the same command and the
// admission path consumes that grant, preserves the original event ID, and records
// the final real cluster operation.
func (a *App) executeApprovedAdmissionAction(ctx context.Context, ev *AdmissionEvent, actor string) (approvedActionResult, error) {
	if ev == nil {
		return approvedActionResult{}, errors.New("event is nil")
	}
	_ = ctx
	_ = actor
	switch strings.ToUpper(strings.TrimSpace(ev.Operation)) {
	case "DELETE":
		return approvedActionResult{Supported: true, Executed: false, Message: "已创建一次性审批授权，请原用户在有效期内重新执行删除命令"}, nil
	default:
		return approvedActionResult{Supported: false, Executed: false, Message: "该操作类型无法在审批回调中安全复放，已保留一次性重试授权"}, nil
	}
}

func approvalGrantTTLText(grant *AdmissionApprovalGrant) string {
	if grant == nil || grant.ExpiresAt.IsZero() {
		return ""
	}
	remain := time.Until(grant.ExpiresAt)
	if remain <= 0 {
		return grant.ExpiresAt.Format(time.RFC3339)
	}
	if remain > time.Hour {
		return fmt.Sprintf("约 %.1f 小时，截止 %s", remain.Hours(), grant.ExpiresAt.Format(time.RFC3339))
	}
	return fmt.Sprintf("约 %.0f 分钟，截止 %s", remain.Minutes(), grant.ExpiresAt.Format(time.RFC3339))
}

func (a *App) saveAdmissionEventState(ctx context.Context, ev *AdmissionEvent) {
	if ev == nil {
		return
	}
	if ev.PersistStatus == "" {
		ev.PersistStatus = "updated"
	}
	if a.local != nil {
		if err := a.local.SpoolEvent(ev); err != nil {
			fmt.Printf("admission event local state save failed: id=%s err=%v\n", ev.ID, err)
		}
	}
	if a.mongo != nil && a.mongo.Healthy() {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_ = a.mongo.SaveEvent(cctx, ev)
	}
}
