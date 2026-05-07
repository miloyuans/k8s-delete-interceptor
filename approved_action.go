package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/restmapper"
)

type approvedActionResult struct {
	Supported bool
	Executed  bool
	Message   string
}

func (a *App) executeApprovedAdmissionAction(ctx context.Context, ev *AdmissionEvent, actor string) (approvedActionResult, error) {
	if ev == nil {
		return approvedActionResult{}, errors.New("event is nil")
	}
	switch strings.ToUpper(strings.TrimSpace(ev.Operation)) {
	case "DELETE":
		return a.executeApprovedDelete(ctx, ev, actor)
	default:
		return approvedActionResult{Supported: false, Executed: false, Message: "该操作类型无法在审批回调中安全复放，已保留一次性重试授权"}, nil
	}
}

func (a *App) createSystemExecutionApprovalGrant(ctx context.Context, ev *AdmissionEvent, actor string) error {
	if ev == nil {
		return errors.New("event is nil")
	}
	now := time.Now().UTC()
	serviceUser := interceptorServiceAccountUser()
	grant := &AdmissionApprovalGrant{
		ID:           admissionApprovalKey(ev.Cluster, ev.Operation, ev.APIGroup, ev.Resource, ev.Namespace, ev.Name, serviceUser, ev.RuleID),
		EventID:      ev.ID,
		RuleID:       ev.RuleID,
		RuleName:     ev.RuleName,
		Cluster:      ev.Cluster,
		Operation:    ev.Operation,
		APIGroup:     ev.APIGroup,
		Resource:     ev.Resource,
		Kind:         ev.Kind,
		Namespace:    ev.Namespace,
		Name:         ev.Name,
		User:         serviceUser,
		ApprovedBy:   actor,
		ApprovedByID: admissionApprovalSystemExecutorID,
		ApprovedAt:   now,
		ExpiresAt:    now.Add(2 * time.Minute),
	}
	if a.local != nil {
		if err := a.local.SaveAdmissionApprovalGrant(grant); err != nil {
			return err
		}
	}
	if a.mongo != nil && a.mongo.Healthy() {
		if err := a.mongo.SaveAdmissionApprovalGrant(ctx, grant); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) executeApprovedDelete(ctx context.Context, ev *AdmissionEvent, actor string) (approvedActionResult, error) {
	if a.dynamicClient == nil || a.discoveryClient == nil {
		return approvedActionResult{Supported: true}, errors.New("kubernetes dynamic client unavailable")
	}
	if strings.TrimSpace(ev.Name) == "" {
		return approvedActionResult{Supported: true}, errors.New("event resource name is empty")
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(a.discoveryClient))
	mapping, err := eventRESTMapping(mapper, ev)
	if err != nil {
		return approvedActionResult{Supported: true}, err
	}
	ri := resourceInterface(a, mapping, ev.Namespace)
	if err := a.createSystemExecutionApprovalGrant(ctx, ev, actor); err != nil {
		return approvedActionResult{Supported: true}, fmt.Errorf("create system execution approval grant: %w", err)
	}
	opts := metav1.DeleteOptions{}
	if uid := admissionEventObjectUID(ev); uid != "" {
		u := types.UID(uid)
		opts.Preconditions = &metav1.Preconditions{UID: &u}
	}
	if err := ri.Delete(ctx, ev.Name, opts); err != nil {
		if apierrors.IsNotFound(err) {
			return approvedActionResult{Supported: true, Executed: true, Message: "资源已不存在，视为删除完成"}, nil
		}
		return approvedActionResult{Supported: true}, err
	}
	msg := fmt.Sprintf("已由审批人 %s 触发删除执行", actor)
	return approvedActionResult{Supported: true, Executed: true, Message: msg}, nil
}

func eventRESTMapping(mapper *restmapper.DeferredDiscoveryRESTMapper, ev *AdmissionEvent) (*meta.RESTMapping, error) {
	if mapper == nil || ev == nil {
		return nil, errors.New("rest mapper unavailable")
	}
	version := strings.TrimSpace(ev.APIVersion)
	if version == "" {
		version = "v1"
	}
	group := strings.TrimSpace(ev.APIGroup)
	kind := strings.TrimSpace(ev.Kind)
	if kind != "" {
		return mapper.RESTMapping(schema.GroupVersion{Group: group, Version: version}.WithKind(kind).GroupKind(), version)
	}
	resource := strings.TrimSpace(ev.Resource)
	if resource == "" {
		return nil, errors.New("event kind/resource is empty")
	}
	return mapper.RESTMapping(schema.GroupKind{Group: group, Kind: resource}, version)
}

func admissionEventObjectUID(ev *AdmissionEvent) string {
	if ev == nil {
		return ""
	}
	for _, raw := range [][]byte{ev.OldObject, ev.Object} {
		if len(raw) == 0 {
			continue
		}
		var obj map[string]any
		if json.Unmarshal(raw, &obj) != nil {
			continue
		}
		if uid, _ := getNestedString(obj, "metadata", "uid"); strings.TrimSpace(uid) != "" {
			return strings.TrimSpace(uid)
		}
	}
	return ""
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
			// Local spooling is a safety net only. Mongo remains the primary state for the Web UI.
			fmt.Printf("admission event local state save failed: id=%s err=%v\n", ev.ID, err)
		}
	}
	if a.mongo != nil && a.mongo.Healthy() {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_ = a.mongo.SaveEvent(cctx, ev)
	}
}
