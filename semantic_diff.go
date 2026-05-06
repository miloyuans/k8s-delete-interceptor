package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

func classifyChange(ac AdmissionContext) (string, string) {
	switch strings.ToUpper(ac.Operation) {
	case "CREATE":
		return "resource_created", "资源创建请求"
	case "DELETE":
		return "resource_deleted", "资源删除请求"
	case "UPDATE":
		return classifyUpdate(ac)
	default:
		return strings.ToLower(ac.Operation), ac.Operation
	}
}

func classifyUpdate(ac AdmissionContext) (string, string) {
	oldN := normalizeObject(ac.OldObject)
	newN := normalizeObject(ac.Object)
	if deepEqualJSON(oldN, newN) {
		return "no_effective_change", "标准化后无有效变化"
	}
	oldNoRestart := removeRestartAnnotation(normalizeObject(ac.OldObject))
	newNoRestart := removeRestartAnnotation(normalizeObject(ac.Object))
	if deepEqualJSON(oldNoRestart, newNoRestart) {
		return "workload_restart", "仅修改 kubectl.kubernetes.io/restartedAt，属于工作负载重启"
	}
	if statusOnly(ac.OldObject, ac.Object) {
		return "status_only", "仅 status 变化"
	}
	if metadataOnly(ac.OldObject, ac.Object) {
		return "metadata_only", "仅 metadata 变化"
	}
	if scaleOnly(ac.OldObject, ac.Object) {
		return "scale_only", "副本数变化"
	}
	if strings.EqualFold(ac.Kind, "Service") {
		if !deepEqualPath(ac.OldObject, ac.Object, "spec", "selector") {
			return "service_selector_changed", "Service selector 变化"
		}
		if !deepEqualPath(ac.OldObject, ac.Object, "spec", "ports") {
			return "service_port_changed", "Service ports 变化"
		}
	}
	if strings.EqualFold(ac.Kind, "Ingress") {
		if !deepEqualPath(ac.OldObject, ac.Object, "spec", "rules") || !deepEqualPath(ac.OldObject, ac.Object, "spec", "defaultBackend") || !deepEqualPath(ac.OldObject, ac.Object, "spec", "tls") {
			return "ingress_backend_changed", "Ingress backend/rules/tls 变化"
		}
	}
	if strings.EqualFold(ac.Kind, "ConfigMap") {
		if !deepEqualPath(ac.OldObject, ac.Object, "data") || !deepEqualPath(ac.OldObject, ac.Object, "binaryData") {
			return "configmap_data_changed", "ConfigMap data 变化"
		}
	}
	if strings.EqualFold(ac.Kind, "Secret") {
		if !deepEqualPath(ac.OldObject, ac.Object, "data") || !deepEqualPath(ac.OldObject, ac.Object, "stringData") {
			return "secret_data_changed", "Secret data 变化，详细 diff 仅进入审计"
		}
	}
	if workloadImageChanged(ac.OldObject, ac.Object) {
		return "image_changed", "容器镜像变化"
	}
	if workloadEnvChanged(ac.OldObject, ac.Object) {
		return "env_changed", "容器环境变量变化"
	}
	if workloadVolumeChanged(ac.OldObject, ac.Object) {
		return "volume_changed", "volume/volumeMounts 变化"
	}
	if !deepEqualPath(oldN, newN, "spec") {
		return "spec_changed", summarizeChangedTopPaths(oldN, newN)
	}
	return "object_changed", summarizeChangedTopPaths(oldN, newN)
}

func normalizeObject(in map[string]any) map[string]any {
	b, _ := json.Marshal(in)
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	delete(out, "status")
	meta, _ := out["metadata"].(map[string]any)
	if meta != nil {
		for _, k := range []string{"managedFields", "resourceVersion", "uid", "selfLink", "creationTimestamp", "generation"} {
			delete(meta, k)
		}
		ann, _ := meta["annotations"].(map[string]any)
		if ann != nil {
			for _, k := range []string{"deployment.kubernetes.io/revision", "kubectl.kubernetes.io/last-applied-configuration"} {
				delete(ann, k)
			}
		}
	}
	return out
}

func removeRestartAnnotation(in map[string]any) map[string]any {
	paths := [][]string{{"spec", "template", "metadata", "annotations"}, {"metadata", "annotations"}}
	for _, p := range paths {
		m, ok := getNestedMap(in, p...)
		if !ok {
			continue
		}
		for k := range m {
			if strings.Contains(strings.ToLower(k), "restartedat") || k == "kubectl.kubernetes.io/restartedAt" {
				delete(m, k)
			}
		}
	}
	return in
}

func deepEqualJSON(a, b any) bool { return reflect.DeepEqual(canonical(a), canonical(b)) }
func deepEqualPath(a, b map[string]any, keys ...string) bool {
	av, _ := getNested(a, keys...)
	bv, _ := getNested(b, keys...)
	return deepEqualJSON(av, bv)
}

func canonical(v any) any {
	switch x := v.(type) {
	case map[string]any:
		m := map[string]any{}
		for k, v2 := range x {
			m[k] = canonical(v2)
		}
		return m
	case []any:
		arr := make([]any, len(x))
		for i := range x {
			arr[i] = canonical(x[i])
		}
		return arr
	default:
		return x
	}
}

func statusOnly(oldObj, newObj map[string]any) bool {
	a := cloneMap(oldObj)
	b := cloneMap(newObj)
	delete(a, "status")
	delete(b, "status")
	return deepEqualJSON(normalizeObject(a), normalizeObject(b))
}
func metadataOnly(oldObj, newObj map[string]any) bool {
	a := cloneMap(oldObj)
	b := cloneMap(newObj)
	delete(a, "metadata")
	delete(b, "metadata")
	return deepEqualJSON(a, b)
}
func scaleOnly(oldObj, newObj map[string]any) bool {
	a := normalizeObject(oldObj)
	b := normalizeObject(newObj)
	as, aok := getNestedMap(a, "spec")
	bs, bok := getNestedMap(b, "spec")
	if !aok || !bok {
		return false
	}
	delete(as, "replicas")
	delete(bs, "replicas")
	return deepEqualJSON(a, b)
}

func workloadImageChanged(oldObj, newObj map[string]any) bool {
	return !deepEqualJSON(extractContainerField(oldObj, "image"), extractContainerField(newObj, "image"))
}
func workloadEnvChanged(oldObj, newObj map[string]any) bool {
	return !deepEqualJSON(extractContainerField(oldObj, "env"), extractContainerField(newObj, "env"))
}
func workloadVolumeChanged(oldObj, newObj map[string]any) bool {
	return !deepEqualPath(oldObj, newObj, "spec", "template", "spec", "volumes") || !deepEqualJSON(extractContainerField(oldObj, "volumeMounts"), extractContainerField(newObj, "volumeMounts"))
}

func extractContainerField(obj map[string]any, field string) any {
	out := map[string]any{}
	for _, p := range [][]string{{"spec", "template", "spec", "containers"}, {"spec", "template", "spec", "initContainers"}, {"spec", "containers"}, {"spec", "initContainers"}} {
		arr, ok := getNestedSlice(obj, p...)
		if !ok {
			continue
		}
		for _, item := range arr {
			m, _ := item.(map[string]any)
			if m == nil {
				continue
			}
			name := fmt.Sprint(m["name"])
			out[strings.Join(p, ".")+"."+name] = m[field]
		}
	}
	return out
}

func cloneMap(in map[string]any) map[string]any {
	b, _ := json.Marshal(in)
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	return out
}

func summarizeChangedTopPaths(a, b map[string]any) string {
	paths := []string{}
	collectTopDiff("", a, b, &paths, 12)
	if len(paths) == 0 {
		return "对象内容变化"
	}
	sort.Strings(paths)
	return "变化字段: " + strings.Join(paths, ", ")
}

func collectTopDiff(prefix string, a, b any, out *[]string, limit int) {
	if len(*out) >= limit {
		return
	}
	am, aok := a.(map[string]any)
	bm, bok := b.(map[string]any)
	if aok && bok {
		keys := map[string]bool{}
		for k := range am {
			keys[k] = true
		}
		for k := range bm {
			keys[k] = true
		}
		ks := []string{}
		for k := range keys {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		for _, k := range ks {
			collectTopDiff(joinPath(prefix, k), am[k], bm[k], out, limit)
		}
		return
	}
	if !deepEqualJSON(a, b) {
		if prefix == "" {
			prefix = "root"
		}
		*out = append(*out, prefix)
	}
}
func joinPath(a, b string) string {
	if a == "" {
		return b
	}
	return a + "." + b
}

func compactJSON(raw json.RawMessage) string {
	var buf bytes.Buffer
	_ = json.Compact(&buf, raw)
	return buf.String()
}
