package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/restmapper"
)

func (a *App) createRollbackBackup(ctx context.Context, cfg *RuntimeConfig, ev *AdmissionEvent, ac AdmissionContext, pd PolicyDecision) (*RollbackBackup, error) {
	mode := RollbackRestoreOldObject
	source := ac.OldObjectRaw
	createdUID := ""
	if strings.EqualFold(ac.Operation, "CREATE") {
		mode = RollbackDeleteCreatedObject
		source = ac.ObjectRaw
		createdUID, _ = getNestedString(ac.Object, "metadata", "uid")
	}
	if pd.Rule != nil && pd.Rule.Rollback.Mode != "" {
		mode = pd.Rule.Rollback.Mode
	}
	if len(source) == 0 {
		return nil, nil
	}
	rb := &RollbackBackup{ID: "rb_" + ev.ID, RequestUID: ev.RequestUID, EventID: ev.ID, CreatedAt: time.Now().UTC(), Cluster: cfg.ClusterName, Operation: ac.Operation, Mode: mode, APIGroup: ac.APIGroup, APIVersion: ac.APIVersion, Kind: ac.Kind, Resource: ac.Resource, SubResource: ac.SubResource, Namespace: ac.Namespace, Name: ac.Name, SourceObject: source, CreatedObjectUID: createdUID, DryRunRequired: cfg.Rollback.RequireDryRun}
	_ = a.local.SaveRollback(rb)
	if a.mongo != nil && a.mongo.Healthy() {
		_ = a.mongo.SaveRollback(ctx, rb)
	}
	return rb, nil
}

func (a *App) getRollback(ctx context.Context, id string) (*RollbackBackup, error) {
	if a.mongo != nil && a.mongo.Healthy() {
		if rb, err := a.mongo.GetRollback(ctx, id); err == nil {
			return rb, nil
		}
	}
	return a.local.GetRollback(id)
}

func (a *App) executeRollback(ctx context.Context, id string, dryRun bool) (string, error) {
	if a.dynamicClient == nil || a.discoveryClient == nil {
		return "", errors.New("kubernetes dynamic client unavailable")
	}
	rb, err := a.getRollback(ctx, id)
	if err != nil {
		return "", err
	}
	ri, patchSubresource, err := a.rollbackResourceInterface(rb)
	if err != nil {
		return "", err
	}
	dry := []string{}
	if dryRun {
		dry = []string{metav1.DryRunAll}
	}
	if rb.Mode == RollbackDeleteCreatedObject {
		opts := metav1.DeleteOptions{DryRun: dry}
		if rb.CreatedObjectUID != "" {
			uid := types.UID(rb.CreatedObjectUID)
			opts.Preconditions = &metav1.Preconditions{UID: &uid}
		}
		if err := ri.Delete(ctx, rb.Name, opts); err != nil {
			return "", err
		}
		if dryRun {
			return "dry-run delete ok", nil
		}
		return "delete-created-object rollback executed", nil
	}
	var obj map[string]any
	if err := json.Unmarshal(rb.SourceObject, &obj); err != nil {
		return "", err
	}
	cleanupRollbackObject(obj)
	if isScaleRollbackBackup(rb) {
		normalizeScaleRollbackObject(obj)
	}
	ub, _ := json.Marshal(obj)
	patchType := types.ApplyPatchType
	patchOptions := metav1.PatchOptions{FieldManager: "k8s-delete-interceptor-rollback", Force: boolPtr(true), DryRun: dry}
	if strings.EqualFold(patchSubresource, "scale") {
		if b, err := scaleRollbackMergePatch(obj); err == nil {
			ub = b
			patchType = types.MergePatchType
			patchOptions = metav1.PatchOptions{DryRun: dry}
		} else {
			return "", err
		}
	}
	patchSubresources := []string{}
	if patchSubresource != "" {
		patchSubresources = append(patchSubresources, patchSubresource)
	}
	_, err = ri.Patch(ctx, rb.Name, patchType, ub, patchOptions, patchSubresources...)
	if err != nil {
		return "", err
	}
	if dryRun {
		return "dry-run apply ok", nil
	}
	return "restore-old-object rollback executed", nil
}

func (a *App) rollbackResourceInterface(rb *RollbackBackup) (dynamicResourceInterface, string, error) {
	if rb == nil {
		return nil, "", errors.New("rollback backup is nil")
	}
	subresource := strings.TrimSpace(rb.SubResource)
	if subresource == "" && isScaleRollbackBackup(rb) {
		subresource = "scale"
	}
	if subresource != "" {
		if strings.TrimSpace(rb.Resource) == "" || strings.TrimSpace(rb.APIVersion) == "" {
			return nil, "", fmt.Errorf("rollback subresource %q requires parent resource and api_version", subresource)
		}
		gvr := schema.GroupVersionResource{Group: rb.APIGroup, Version: rb.APIVersion, Resource: rb.Resource}
		if rb.Namespace != "" {
			return a.dynamicClient.Resource(gvr).Namespace(rb.Namespace), subresource, nil
		}
		return a.dynamicClient.Resource(gvr), subresource, nil
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(a.discoveryClient))
	gv := schema.GroupVersion{Group: rb.APIGroup, Version: rb.APIVersion}
	mapping, err := mapper.RESTMapping(gv.WithKind(rb.Kind).GroupKind(), rb.APIVersion)
	if err != nil {
		return nil, "", err
	}
	return resourceInterface(a, mapping, rb.Namespace), "", nil
}

func isScaleRollbackBackup(rb *RollbackBackup) bool {
	if rb == nil {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(rb.SubResource), "scale") {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(rb.Kind), "Scale") && strings.TrimSpace(rb.Resource) != "" {
		return true
	}
	var obj map[string]any
	if len(rb.SourceObject) > 0 && json.Unmarshal(rb.SourceObject, &obj) == nil {
		kind, _ := obj["kind"].(string)
		return strings.EqualFold(strings.TrimSpace(kind), "Scale") && strings.TrimSpace(rb.Resource) != ""
	}
	return false
}

func scaleRollbackMergePatch(obj map[string]any) ([]byte, error) {
	replicas, ok := scaleReplicas(obj)
	if !ok {
		return nil, errors.New("scale rollback source object is missing spec.replicas")
	}
	return json.Marshal(map[string]any{"spec": map[string]any{"replicas": replicas}})
}

func scaleReplicas(obj map[string]any) (int64, bool) {
	v, ok := getNested(obj, "spec", "replicas")
	if !ok {
		return 0, false
	}
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case float64:
		return int64(x), true
	case json.Number:
		n, err := x.Int64()
		return n, err == nil
	default:
		return 0, false
	}
}

func normalizeScaleRollbackObject(obj map[string]any) {
	if obj == nil {
		return
	}
	obj["apiVersion"] = "autoscaling/v1"
	obj["kind"] = "Scale"
}

func resourceInterface(a *App, mapping *meta.RESTMapping, namespace string) dynamicResourceInterface {
	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		return a.dynamicClient.Resource(mapping.Resource).Namespace(namespace)
	}
	return a.dynamicClient.Resource(mapping.Resource)
}

type dynamicResourceInterface interface {
	Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (*unstructured.Unstructured, error)
	Delete(ctx context.Context, name string, opts metav1.DeleteOptions, subresources ...string) error
}

func cleanupRollbackObject(obj map[string]any) {
	delete(obj, "status")
	meta, _ := obj["metadata"].(map[string]any)
	if meta != nil {
		for _, k := range []string{"managedFields", "resourceVersion", "uid", "selfLink", "creationTimestamp", "generation", "deletionTimestamp", "deletionGracePeriodSeconds"} {
			delete(meta, k)
		}
	}
}

func boolPtr(b bool) *bool { return &b }

func (a *App) rollbackSummary(id string) string {
	rb, err := a.getRollback(context.Background(), id)
	if err != nil {
		return fmt.Sprintf("rollback %s not found: %v", id, err)
	}
	return fmt.Sprintf("%s %s/%s/%s mode=%s", rb.Operation, rb.Kind, rb.Namespace, rb.Name, rb.Mode)
}
