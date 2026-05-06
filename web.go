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
	mux.HandleFunc("/api/me", a.auth(a.handleMe))
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
	writeJSON(w, map[string]any{"ok": true, "cluster": cluster, "config_version": version, "mongo": mongoStatus, "state_dir": a.local.Root(), "auth_required": a.adminToken != ""})
}

func (a *App) handleMe(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{"ok": true, "authenticated": true, "auth_required": a.adminToken != "", "role": "admin"})
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
	q := parseEventQuery(r)
	if a.mongo != nil && a.mongo.Healthy() {
		if events, err := a.mongo.ListEventsByQuery(r.Context(), q); err == nil {
			writeJSON(w, events)
			return
		}
	}
	events, err := a.local.ListRecentEventsByQuery(q)
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

func parseEventQuery(r *http.Request) EventQuery {
	qv := r.URL.Query()
	q := EventQuery{
		Limit:     parseLimit(r, 200),
		Cluster:   strings.TrimSpace(qv.Get("cluster")),
		Namespace: strings.TrimSpace(qv.Get("namespace")),
		Kind:      strings.TrimSpace(qv.Get("kind")),
		Resource:  strings.TrimSpace(qv.Get("resource")),
		Name:      strings.TrimSpace(qv.Get("name")),
		User:      strings.TrimSpace(qv.Get("user")),
		Operation: strings.ToUpper(strings.TrimSpace(qv.Get("operation"))),
		Decision:  strings.TrimSpace(qv.Get("decision")),
	}
	if v := strings.TrimSpace(qv.Get("allowed")); v != "" {
		b := v == "true" || v == "1" || strings.EqualFold(v, "yes")
		q.Allowed = &b
	}
	tz := strings.TrimSpace(qv.Get("tz"))
	q.Start = parseWebTime(qv.Get("start"), tz, false)
	q.End = parseWebTime(qv.Get("end"), tz, true)
	if !q.Start.IsZero() && !q.End.IsZero() && q.End.Before(q.Start) {
		q.Start, q.End = q.End, q.Start
	}
	return q
}

func parseWebTime(v, tz string, endOfDate bool) time.Time {
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Time{}
	}
	loc := time.UTC
	if tz != "" {
		if l, err := time.LoadLocation(tz); err == nil {
			loc = l
		}
	}
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return t.UTC()
	}
	layouts := []string{"2006-01-02T15:04:05", "2006-01-02T15:04"}
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, v, loc); err == nil {
			return t.UTC()
		}
	}
	if t, err := time.ParseInLocation("2006-01-02", v, loc); err == nil {
		if endOfDate {
			return t.AddDate(0, 0, 1).UTC()
		}
		return t.UTC()
	}
	return time.Time{}
}

const indexHTML = `<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>K8s Delete Interceptor Console</title><style>
:root{--bg:#07111f;--panel:#0c1729;--panel2:#111e33;--line:rgba(148,163,184,.18);--text:#e5eefb;--muted:#94a3b8;--brand:#38bdf8;--brand2:#a78bfa;--ok:#22c55e;--bad:#ef4444;--warn:#f59e0b;--shadow:0 18px 60px rgba(0,0,0,.28)}*{box-sizing:border-box}body{margin:0;font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans SC",sans-serif;background:radial-gradient(circle at top left,rgba(56,189,248,.20),transparent 32%),radial-gradient(circle at 80% 10%,rgba(167,139,250,.18),transparent 28%),linear-gradient(145deg,#050b15,#081321 55%,#050914);color:var(--text);min-height:100vh}.app{display:grid;grid-template-columns:260px minmax(0,1fr);min-height:100vh}.sidebar{position:sticky;top:0;height:100vh;padding:24px 18px;border-right:1px solid var(--line);background:rgba(3,7,18,.72);backdrop-filter:blur(18px)}.brand{display:flex;gap:12px;align-items:center;margin-bottom:26px}.logo{width:42px;height:42px;border-radius:14px;background:linear-gradient(135deg,var(--brand),var(--brand2));box-shadow:0 0 30px rgba(56,189,248,.35)}.brand h1{font-size:16px;line-height:1.1;margin:0}.brand p{margin:4px 0 0;color:var(--muted);font-size:12px}.nav{display:flex;flex-direction:column;gap:8px}.nav button{display:flex;align-items:center;gap:10px;width:100%;border:1px solid transparent;background:transparent;color:var(--muted);padding:11px 12px;border-radius:14px;text-align:left;cursor:pointer;font:inherit}.nav button:hover,.nav button.active{color:var(--text);border-color:rgba(56,189,248,.25);background:linear-gradient(90deg,rgba(56,189,248,.14),rgba(167,139,250,.08))}.sideFoot{position:absolute;left:18px;right:18px;bottom:20px;color:var(--muted);font-size:12px;border:1px solid var(--line);border-radius:16px;padding:12px;background:rgba(15,23,42,.55)}main{min-width:0;padding:20px 24px 38px}.topbar{position:sticky;top:0;z-index:5;display:flex;justify-content:space-between;align-items:center;gap:16px;margin:-20px -24px 22px;padding:16px 24px;border-bottom:1px solid var(--line);background:rgba(8,19,33,.78);backdrop-filter:blur(18px)}.title h2{margin:0;font-size:20px}.title p{margin:4px 0 0;color:var(--muted);font-size:13px}.actions{display:flex;align-items:center;gap:10px;flex-wrap:wrap}.select,input,textarea{border:1px solid var(--line);border-radius:12px;background:rgba(15,23,42,.84);color:var(--text);padding:9px 11px;font:inherit}.select{min-width:170px}textarea{width:100%;min-height:540px;font-family:ui-monospace,SFMono-Regular,Menlo,monospace}.btn{border:0;border-radius:12px;padding:10px 14px;background:linear-gradient(135deg,#0ea5e9,#8b5cf6);color:white;cursor:pointer;font-weight:650;box-shadow:0 10px 30px rgba(14,165,233,.20)}.btn.secondary{background:rgba(148,163,184,.14);box-shadow:none;color:var(--text);border:1px solid var(--line)}.btn.danger{background:rgba(239,68,68,.16);box-shadow:none;border:1px solid rgba(239,68,68,.28)}.grid{display:grid;gap:16px}.cards{grid-template-columns:repeat(4,minmax(0,1fr))}.two{grid-template-columns:1.1fr .9fr}.card{background:linear-gradient(180deg,rgba(17,30,51,.82),rgba(12,23,41,.82));border:1px solid var(--line);border-radius:20px;padding:18px;box-shadow:var(--shadow)}.card h3{margin:0 0 12px;font-size:15px}.metric{font-size:28px;font-weight:800;letter-spacing:-.04em}.muted{color:var(--muted)}.pill{display:inline-flex;align-items:center;gap:5px;border:1px solid var(--line);border-radius:999px;padding:4px 9px;font-size:12px;color:var(--muted);background:rgba(148,163,184,.08)}.pill.ok{color:#86efac;border-color:rgba(34,197,94,.32);background:rgba(34,197,94,.12)}.pill.bad{color:#fca5a5;border-color:rgba(239,68,68,.32);background:rgba(239,68,68,.12)}.pill.warn{color:#fcd34d;border-color:rgba(245,158,11,.32);background:rgba(245,158,11,.12)}.toolbar{display:grid;grid-template-columns:repeat(6,minmax(120px,1fr));gap:10px;margin-bottom:14px}.toolbar label{display:flex;flex-direction:column;gap:6px;color:var(--muted);font-size:12px}.tableWrap{overflow:auto;border:1px solid var(--line);border-radius:16px}table{width:100%;border-collapse:collapse;min-width:920px}th,td{padding:12px 10px;border-bottom:1px solid var(--line);text-align:left;font-size:13px;vertical-align:top}th{color:#bcd3ee;font-weight:700;background:rgba(15,23,42,.72);position:sticky;top:0}tr:hover td{background:rgba(56,189,248,.05)}pre{white-space:pre-wrap;word-break:break-all;background:#020617;border:1px solid var(--line);border-radius:16px;padding:14px;max-height:520px;overflow:auto;color:#c4d7ee}.section{display:none}.section.active{display:block}.notice{border:1px solid rgba(56,189,248,.22);background:rgba(56,189,248,.08);border-radius:16px;padding:12px 14px;color:#bae6fd}.rightPanel{position:sticky;top:86px}.mini{font-size:12px}.kvs{display:grid;grid-template-columns:130px 1fr;gap:8px;color:var(--muted);font-size:13px}.kvs b{color:var(--text)}@media(max-width:960px){.app{grid-template-columns:1fr}.sidebar{position:relative;height:auto}.sideFoot{position:static;margin-top:18px}.cards,.two,.toolbar{grid-template-columns:1fr}.topbar{position:relative;top:auto}.rightPanel{position:static}}
</style></head><body><div class="app"><aside class="sidebar"><div class="brand"><div class="logo"></div><div><h1>K8s Delete Interceptor</h1><p>Admission Guard Console</p></div></div><nav class="nav"><button id="nav-dashboard" class="active" onclick="show('dashboard')">▣ 控制首页</button><button id="nav-events" onclick="show('events')">◎ 历史事件</button><button id="nav-sa" onclick="show('sa')">◌ ServiceAccount</button><button id="nav-config" onclick="show('config')">⚙ 运行配置</button></nav><div class="sideFoot"><div>数据源状态</div><div id="sideStatus" style="margin-top:6px">加载中...</div></div></aside><main><div class="topbar"><div class="title"><h2 id="pageTitle">控制首页</h2><p id="pageSub">配置、审计历史、回滚入口集中展示在右侧工作区</p></div><div class="actions"><select id="tz" class="select" onchange="setTZ(this.value)"></select><span id="authState" class="pill warn">未确认</span><button class="btn secondary" onclick="confirmAuth()">用户确认</button><button class="btn secondary" onclick="login()">登录</button><button class="btn danger" onclick="logout()">登出</button><button class="btn" onclick="loadAll()">刷新</button></div></div><section id="dashboard" class="section active"><div class="grid cards"><div class="card"><h3>集群</h3><div id="mCluster" class="metric">-</div><div class="muted mini">当前 RuntimeConfig 集群名</div></div><div class="card"><h3>配置版本</h3><div id="mVersion" class="metric">-</div><div class="muted mini">热加载版本</div></div><div class="card"><h3>事件数量</h3><div id="mEvents" class="metric">-</div><div class="muted mini">当前筛选结果</div></div><div class="card"><h3>拦截 / 审批</h3><div id="mRisk" class="metric">-</div><div class="muted mini">block + require_approval</div></div></div><div class="grid two" style="margin-top:16px"><div class="card"><h3>最近高风险事件</h3><div id="riskEvents" class="tableWrap"></div></div><div class="card rightPanel"><h3>运行状态</h3><div id="health" class="kvs">加载中...</div><div style="margin-top:14px" class="notice">建议把历史事件查询作为主要审计入口：左侧导航固定，右侧根据所选时间范围、时区、操作类型和决策状态即时刷新。</div></div></div></section><section id="events" class="section"><div class="card"><h3>历史事件查询</h3><div class="toolbar"><label>开始时间<input id="startAt" type="datetime-local"></label><label>结束时间<input id="endAt" type="datetime-local"></label><label>Namespace<input id="fNamespace" placeholder="prod"></label><label>资源名<input id="fName" placeholder="deployment"></label><label>用户<input id="fUser" placeholder="system:serviceaccount"></label><label>操作<select id="fOperation" class="select"><option value="">全部</option><option>CREATE</option><option>UPDATE</option><option>DELETE</option></select></label><label>决策<select id="fDecision" class="select"><option value="">全部</option><option value="audit_only">audit_only</option><option value="allow_silent">allow_silent</option><option value="allow_notify">allow_notify</option><option value="require_approval">require_approval</option><option value="block">block</option></select></label><label>允许状态<select id="fAllowed" class="select"><option value="">全部</option><option value="true">Allowed</option><option value="false">Denied</option></select></label><label>Kind<input id="fKind" placeholder="Deployment"></label><label>Limit<input id="fLimit" type="number" min="1" max="2000" value="200"></label><label>&nbsp;<button class="btn" onclick="loadEvents()">查询</button></label><label>&nbsp;<button class="btn secondary" onclick="quickRange(24)">近24小时</button></label></div><div class="actions" style="margin-bottom:12px"><button class="btn secondary" onclick="quickRange(1)">近1小时</button><button class="btn secondary" onclick="quickRange(168)">近7天</button><button class="btn secondary" onclick="clearFilters()">清空条件</button></div><div id="eventsTable" class="tableWrap"></div></div></section><section id="sa" class="section"><div class="card"><h3>ServiceAccount 资产</h3><p class="muted">用于识别控制器、自动化账号和人类管理员账号，辅助规则配置。</p><p><button class="btn" onclick="scanSA()">重新扫描</button></p><div id="saTable" class="tableWrap"></div></div></section><section id="config" class="section"><div class="grid two"><div class="card"><h3>配置概览</h3><div id="cfgSummary" class="kvs"></div><h3 style="margin-top:18px">配置编辑</h3><p class="muted">修改 JSON 后发布会生成新版本并热加载。建议后续拆分成数据源、资源范围、Actor 组、规则、模板等表单页。</p><p><button class="btn" onclick="publishCfg()">发布配置</button></p></div><div class="card"><textarea id="cfg" spellcheck="false"></textarea></div></div></section></main></div><script>
const tzOptions=[['Asia/Shanghai','中国上海 / 北京时间'],['Asia/Dubai','迪拜 / 阿联酋'],['Asia/Singapore','新加坡'],['Asia/Tokyo','东京'],['UTC','UTC'],['Europe/Berlin','柏林'],['America/New_York','纽约']];
let token=localStorage.getItem('kdi_token')||localStorage.getItem('token')||'';let eventsCache=[];let current='dashboard';
function initTZ(){const sel=document.getElementById('tz');const saved=localStorage.getItem('kdi_tz')||Intl.DateTimeFormat().resolvedOptions().timeZone||'UTC';sel.innerHTML=tzOptions.map(x=>'<option value="'+esc(x[0])+'">'+esc(x[1])+'</option>').join('');if(!tzOptions.find(x=>x[0]===saved)){sel.innerHTML+='<option value="'+esc(saved)+'">'+esc(saved)+'</option>'}sel.value=saved}
function setTZ(v){localStorage.setItem('kdi_tz',v);renderEvents(eventsCache);loadEvents()}
function tz(){return document.getElementById('tz').value||'UTC'}
function h(){return token?{Authorization:'Bearer '+token}:{} }
function esc(x){return (x===undefined||x===null)?'':String(x).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]})}
async function j(url,opt={}){opt.headers=Object.assign({'Content-Type':'application/json'},h(),opt.headers||{});const r=await fetch(url,opt);if(!r.ok)throw new Error(await r.text());return r.json()}
function show(id){current=id;for(const x of ['dashboard','events','sa','config']){document.getElementById(x).classList.toggle('active',x===id);document.getElementById('nav-'+x).classList.toggle('active',x===id)}const titles={dashboard:['控制首页','配置、审计历史、回滚入口集中展示在右侧工作区'],events:['历史事件','按时间范围、时区、资源、用户、操作和决策查询 Admission 事件'],sa:['ServiceAccount','扫描和展示集群账号资产'],config:['运行配置','查看并发布 RuntimeConfig']};document.getElementById('pageTitle').innerText=titles[id][0];document.getElementById('pageSub').innerText=titles[id][1]}
async function confirmAuth(){try{const me=await j('/api/me');document.getElementById('authState').className='pill ok';document.getElementById('authState').innerText=me.auth_required?'管理员已认证':'无需 Token';return true}catch(e){document.getElementById('authState').className='pill bad';document.getElementById('authState').innerText='认证失败';return false}}
function login(){const t=prompt('请输入 WEB_ADMIN_TOKEN');if(t!==null){token=t.trim();localStorage.setItem('kdi_token',token);localStorage.setItem('token',token);confirmAuth();loadAll()}}
function logout(){token='';localStorage.removeItem('kdi_token');localStorage.removeItem('token');document.getElementById('authState').className='pill warn';document.getElementById('authState').innerText='已登出';}
function fmtTime(t){if(!t)return'-';try{return new Intl.DateTimeFormat('zh-CN',{timeZone:tz(),year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false}).format(new Date(t))}catch(e){return t}}
function localDT(d){const pad=n=>String(n).padStart(2,'0');return d.getFullYear()+'-'+pad(d.getMonth()+1)+'-'+pad(d.getDate())+'T'+pad(d.getHours())+':'+pad(d.getMinutes())}
function quickRange(hours){const end=new Date();const start=new Date(end.getTime()-hours*3600*1000);document.getElementById('startAt').value=localDT(start);document.getElementById('endAt').value=localDT(end);loadEvents()}
function clearFilters(){for(const id of ['startAt','endAt','fNamespace','fName','fUser','fKind','fDecision','fAllowed','fOperation'])document.getElementById(id).value='';document.getElementById('fLimit').value=200;loadEvents()}
function eventQuery(){const p=new URLSearchParams();for(const [id,key] of [['startAt','start'],['endAt','end'],['fNamespace','namespace'],['fName','name'],['fUser','user'],['fKind','kind'],['fOperation','operation'],['fDecision','decision'],['fAllowed','allowed'],['fLimit','limit']]){const v=document.getElementById(id).value;if(v)p.set(key,v)}p.set('tz',tz());return p.toString()}
async function loadAll(){await loadHealth();await confirmAuth();await Promise.allSettled([loadEvents(),loadSA(),loadConfig()])}
async function loadHealth(){try{const x=await j('/api/health');document.getElementById('mCluster').innerText=x.cluster||'-';document.getElementById('mVersion').innerText=x.config_version||0;document.getElementById('sideStatus').innerHTML='<span class="pill '+(String(x.mongo).includes('healthy')?'ok':'warn')+'">'+esc(x.mongo)+'</span>';document.getElementById('health').innerHTML='<b>Cluster</b><span>'+esc(x.cluster||'-')+'</span><b>Config Version</b><span>'+esc(x.config_version)+'</span><b>Mongo</b><span>'+esc(x.mongo)+'</span><b>State Dir</b><span>'+esc(x.state_dir)+'</span><b>Auth Required</b><span>'+esc(x.auth_required)+'</span>'}catch(e){document.getElementById('health').innerText=e.message}}
async function loadEvents(){try{const ev=await j('/api/events?'+eventQuery());eventsCache=ev||[];renderEvents(eventsCache);updateMetrics()}catch(e){document.getElementById('eventsTable').innerHTML='<div class="notice">'+esc(e.message)+'</div>'}}
function updateMetrics(){document.getElementById('mEvents').innerText=eventsCache.length;const risk=eventsCache.filter(x=>x.decision==='block'||x.decision==='require_approval').length;document.getElementById('mRisk').innerText=risk;const top=eventsCache.filter(x=>x.decision==='block'||x.decision==='require_approval').slice(0,8);document.getElementById('riskEvents').innerHTML=smallEventTable(top)}
function smallEventTable(ev){if(!ev.length)return'<div style="padding:14px" class="muted">当前筛选范围内暂无高风险事件</div>';let html='<table><tr><th>时间</th><th>资源</th><th>决策</th></tr>';for(const x of ev){html+='<tr><td>'+esc(fmtTime(x.time))+'</td><td>'+esc(x.kind)+'/'+esc(x.namespace||'-')+'/'+esc(x.name)+'</td><td>'+pill(x)+'</td></tr>'}return html+'</table>'}
function pill(x){const c=x.allowed?'ok':'bad';return '<span class="pill '+c+'">'+esc(x.decision||'-')+'</span>'}
function renderEvents(ev){let html='<table><tr><th>时间 '+esc(tz())+'</th><th>资源</th><th>用户</th><th>操作</th><th>决策</th><th>变更</th><th>回滚</th></tr>';for(const x of ev){const rb=x.rollback_id?'<button class="btn secondary" onclick="dryrun(\''+esc(x.rollback_id)+'\')">dry-run</button> <button class="btn" onclick="execRb(\''+esc(x.rollback_id)+'\')">执行</button>':'';html+='<tr><td>'+esc(fmtTime(x.time))+'</td><td><b>'+esc(x.kind)+'</b><br><span class="muted">'+esc(x.namespace||'-')+'/'+esc(x.name)+'</span></td><td>'+esc(x.user)+'</td><td>'+esc(x.operation)+'</td><td>'+pill(x)+'<br><span class="muted">'+esc(x.reason||'')+'</span></td><td>'+esc(x.change_class||'-')+'<br><span class="muted">'+esc(x.change_summary||'')+'</span></td><td>'+rb+'</td></tr>'}if(!ev.length)html+='<tr><td colspan="7" class="muted">没有匹配事件，请调整时间范围或筛选条件。</td></tr>';document.getElementById('eventsTable').innerHTML=html+'</table>'}
async function loadSA(){try{const items=await j('/api/serviceaccounts');let html='<table><tr><th>Namespace</th><th>Name</th><th>User</th><th>分类</th><th>建议组</th><th>使用</th></tr>';for(const x of items){html+='<tr><td>'+esc(x.namespace)+'</td><td>'+esc(x.name)+'</td><td>'+esc(x.user_string)+'</td><td><span class="pill">'+esc(x.category)+' / '+esc(x.confidence)+'</span></td><td>'+esc(x.suggested_actor_group||'')+'</td><td>'+esc((x.used_by||[]).slice(0,5).join('\n')).replace(/\n/g,'<br>')+'</td></tr>'}document.getElementById('saTable').innerHTML=html+'</table>'}catch(e){document.getElementById('saTable').innerHTML='<div class="notice">'+esc(e.message)+'</div>'}}
async function scanSA(){await j('/api/serviceaccounts/scan',{method:'POST'});loadSA()}
async function loadConfig(){try{const cfg=await j('/api/config');document.getElementById('cfg').value=JSON.stringify(cfg,null,2);document.getElementById('cfgSummary').innerHTML='<b>Cluster</b><span>'+esc(cfg.cluster_name)+'</span><b>Version</b><span>'+esc(cfg.version)+'</span><b>Rules</b><span>'+esc((cfg.rules||[]).length)+'</span><b>Scopes</b><span>'+esc((cfg.resource_scopes||[]).length)+'</span><b>Actor Groups</b><span>'+esc((cfg.actor_groups||[]).length)+'</span><b>Templates</b><span>'+esc((cfg.notification_templates||[]).length)+'</span>'}catch(e){document.getElementById('cfgSummary').innerHTML='<span class="pill bad">'+esc(e.message)+'</span>'}}
async function publishCfg(){try{const cfg=JSON.parse(document.getElementById('cfg').value);const r=await j('/api/config/publish',{method:'POST',body:JSON.stringify(cfg)});alert('发布成功 version '+r.version);loadAll()}catch(e){alert(e.message)}}
async function dryrun(id){alert(JSON.stringify(await j('/api/rollback/'+id+'/dryrun',{method:'POST'}),null,2))}
async function execRb(id){if(confirm('确认执行回滚 '+id+' ?')) alert(JSON.stringify(await j('/api/rollback/'+id+'/execute',{method:'POST'}),null,2))}
initTZ();quickRange(24);loadAll();</script></body></html>`
