package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

func (a *App) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/validate", a.handleValidate)
	mux.HandleFunc("/telegram/webhook", a.handleTelegramWebhook)
	mux.HandleFunc("/", a.handleIndex)
	mux.HandleFunc("/api/health", a.handleHealth)
	mux.HandleFunc("/api/config", a.auth(a.handleConfig))
	mux.HandleFunc("/api/config/publish", a.auth(a.handlePublishConfig))
	mux.HandleFunc("/api/events", a.auth(a.handleEvents))
	mux.HandleFunc("/api/serviceaccounts", a.auth(a.handleServiceAccounts))
	mux.HandleFunc("/api/serviceaccounts/scan", a.auth(a.handleServiceAccountScan))
	mux.HandleFunc("/api/rollback/", a.auth(a.handleRollback))
	mux.HandleFunc("/api/datasources/test", a.auth(a.handleDatasourceTest))
}

func (a *App) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(indexHTML))
}

func (a *App) handleHealth(w http.ResponseWriter, r *http.Request) {
	cfg := a.Config()
	version := int64(0)
	cluster := ""
	if cfg != nil {
		version = cfg.Version
		cluster = cfg.ClusterName
	}
	mongoStatus := "not_configured"
	if a.mongo != nil {
		mongoStatus = a.mongo.Test(r.Context())
	}
	writeJSON(w, map[string]any{"ok": true, "cluster": cluster, "config_version": version, "mongo": mongoStatus, "state_dir": a.local.Root()})
}

func (a *App) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", 405)
		return
	}
	writeJSON(w, a.Config())
}

func (a *App) handlePublishConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	var cfg RuntimeConfig
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	cur := a.Config()
	if cur != nil && cfg.Version <= cur.Version {
		cfg.Version = cur.Version + 1
	}
	if err := validateRuntimeConfig(&cfg); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if a.mongo != nil && a.mongo.Healthy() {
		if err := a.mongo.SaveConfig(r.Context(), &cfg, "web", true); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
	}
	if err := a.SetConfig(&cfg, "web"); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, map[string]any{"ok": true, "version": cfg.Version})
}

func (a *App) handleEvents(w http.ResponseWriter, r *http.Request) {
	limit := parseLimit(r, 100)
	if a.mongo != nil && a.mongo.Healthy() {
		if events, err := a.mongo.ListEvents(r.Context(), limit); err == nil {
			writeJSON(w, events)
			return
		}
	}
	events, err := a.local.ListRecentEvents(limit)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, events)
}

func (a *App) handleServiceAccounts(w http.ResponseWriter, r *http.Request) {
	if a.mongo != nil && a.mongo.Healthy() {
		if items, err := a.mongo.ListServiceAccounts(r.Context(), parseLimit(r, 1000)); err == nil && len(items) > 0 {
			writeJSON(w, items)
			return
		}
	}
	items, err := a.ScanServiceAccounts(r.Context())
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, items)
}

func (a *App) handleServiceAccountScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	items, err := a.ScanServiceAccounts(r.Context())
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, map[string]any{"ok": true, "count": len(items), "items": items})
}

func (a *App) handleRollback(w http.ResponseWriter, r *http.Request) {
	// /api/rollback/{id}/dryrun or /api/rollback/{id}/execute
	p := strings.TrimPrefix(r.URL.Path, "/api/rollback/")
	parts := strings.Split(strings.Trim(p, "/"), "/")
	if len(parts) < 2 {
		http.Error(w, "expected /api/rollback/{id}/dryrun|execute", 400)
		return
	}
	id, action := parts[0], parts[1]
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	switch action {
	case "dryrun":
		msg, err := a.executeRollback(ctx, id, true)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		writeJSON(w, map[string]any{"ok": true, "dry_run": true, "message": msg})
	case "execute":
		msg, err := a.executeRollback(ctx, id, false)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		writeJSON(w, map[string]any{"ok": true, "dry_run": false, "message": msg})
	default:
		http.Error(w, "unknown rollback action", 400)
	}
}

func (a *App) handleDatasourceTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	var body struct {
		URI      string `json:"uri"`
		Database string `json:"database"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	m, err := NewMongoStore(ctx, body.URI, body.Database)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	m.Disconnect(ctx)
	writeJSON(w, map[string]any{"ok": true})
}

const indexHTML = `<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>K8s Interceptor v2</title><style>
body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,"Noto Sans SC",sans-serif;margin:0;background:#f6f7fb;color:#111827}.wrap{max-width:1280px;margin:0 auto;padding:24px}header{display:flex;align-items:center;justify-content:space-between;margin-bottom:20px}.card{background:#fff;border:1px solid #e5e7eb;border-radius:16px;padding:18px;margin:14px 0;box-shadow:0 8px 30px rgba(15,23,42,.06)}button{border:0;border-radius:10px;padding:9px 14px;background:#111827;color:#fff;cursor:pointer}button.secondary{background:#e5e7eb;color:#111827}input,textarea{width:100%;box-sizing:border-box;border:1px solid #d1d5db;border-radius:10px;padding:10px;font:inherit}table{width:100%;border-collapse:collapse}th,td{padding:10px;border-bottom:1px solid #e5e7eb;text-align:left;font-size:13px}.tabs button{margin-right:8px}.muted{color:#6b7280}.pill{display:inline-block;padding:3px 8px;border-radius:999px;background:#eef2ff;font-size:12px}.danger{background:#fee2e2}.ok{background:#dcfce7}.grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}pre{white-space:pre-wrap;word-break:break-all;background:#0f172a;color:#e5e7eb;border-radius:12px;padding:12px;max-height:420px;overflow:auto}</style></head><body><div class="wrap"><header><div><h1>K8s Delete Interceptor v2</h1><div class="muted">事件查询、SA 扫描、动态规则、模板、回滚入口</div></div><button onclick="loadAll()">刷新</button></header><div id="health" class="card">加载中...</div><div class="tabs"><button onclick="show('events')">事件</button><button onclick="show('sa')">ServiceAccount</button><button onclick="show('config')">配置</button></div><section id="events" class="card"></section><section id="sa" class="card" style="display:none"></section><section id="config" class="card" style="display:none"></section></div><script>
let token=localStorage.getItem('token')||''; if(!token){token=prompt('WEB_ADMIN_TOKEN，可留空')||''; localStorage.setItem('token',token)}
function h(){return token?{Authorization:'Bearer '+token}:{}}
function esc(x){return (x===undefined||x===null)?'':String(x).replace(/[&<>]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;'}[c]})}
function show(id){for(const x of ['events','sa','config'])document.getElementById(x).style.display=x==id?'block':'none'}
async function j(url,opt={}){opt.headers=Object.assign({'Content-Type':'application/json'},h(),opt.headers||{});const r=await fetch(url,opt);if(!r.ok)throw new Error(await r.text());return r.json()}
async function loadAll(){try{const health=await j('/api/health');document.getElementById('health').innerHTML='<b>状态</b> '+esc(JSON.stringify(health));loadEvents();loadSA();loadConfig()}catch(e){document.getElementById('health').innerText=e}}
async function loadEvents(){const ev=await j('/api/events?limit=100');let html='<h2>最近事件</h2><table><tr><th>时间</th><th>资源</th><th>用户</th><th>操作</th><th>决策</th><th>变更</th><th>回滚</th></tr>';for(const x of ev){html+='<tr><td>'+esc(x.time)+'</td><td>'+esc(x.kind)+'/'+esc(x.namespace||'-')+'/'+esc(x.name)+'</td><td>'+esc(x.user)+'</td><td>'+esc(x.operation)+'</td><td><span class="pill '+(x.allowed?'ok':'danger')+'">'+esc(x.decision)+'</span></td><td>'+esc(x.change_class)+'<br><span class="muted">'+esc(x.change_summary)+'</span></td><td>'+(x.rollback_id?'<button class="secondary" onclick="dryrun(\''+esc(x.rollback_id)+'\')">dry-run</button> <button onclick="execRb(\''+esc(x.rollback_id)+'\')">执行</button>':'')+'</td></tr>'}document.getElementById('events').innerHTML=html+'</table>'}
async function loadSA(){const items=await j('/api/serviceaccounts');let html='<h2>ServiceAccount 资产</h2><p><button onclick="scanSA()">重新扫描</button></p><table><tr><th>Namespace</th><th>Name</th><th>User</th><th>分类</th><th>建议组</th><th>使用</th></tr>';for(const x of items){html+='<tr><td>'+esc(x.namespace)+'</td><td>'+esc(x.name)+'</td><td>'+esc(x.user_string)+'</td><td>'+esc(x.category)+'/'+esc(x.confidence)+'</td><td>'+esc(x.suggested_actor_group||'')+'</td><td>'+esc((x.used_by||[]).slice(0,4).join('\n')).replace(/\n/g,'<br>')+'</td></tr>'}document.getElementById('sa').innerHTML=html+'</table>'}
async function scanSA(){await j('/api/serviceaccounts/scan',{method:'POST'});loadSA()}
async function loadConfig(){const cfg=await j('/api/config');document.getElementById('config').innerHTML='<h2>运行配置</h2><p class="muted">修改 JSON 后发布会生成新版本并热加载。</p><textarea id="cfg" rows="28"></textarea><p><button onclick="publishCfg()">发布配置</button></p>';document.getElementById('cfg').value=JSON.stringify(cfg,null,2)}
async function publishCfg(){const cfg=JSON.parse(document.getElementById('cfg').value);const r=await j('/api/config/publish',{method:'POST',body:JSON.stringify(cfg)});alert('发布成功 version '+r.version);loadAll()}
async function dryrun(id){alert(JSON.stringify(await j('/api/rollback/'+id+'/dryrun',{method:'POST'}),null,2))}
async function execRb(id){if(confirm('确认执行回滚 '+id+' ?')) alert(JSON.stringify(await j('/api/rollback/'+id+'/execute',{method:'POST'}),null,2))}
loadAll();</script></body></html>`
