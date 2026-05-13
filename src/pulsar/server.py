"""FastAPI application — REST API + embedded web UI."""

import os
from typing import Optional
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from .db import Database
from .executor import JobExecutor
from .scheduler import JobScheduler


# ── request models ──────────────────────────────────────────────────────

class AddJobRequest(BaseModel):
    name: str
    script_path: str
    cron_expression: str
    args: str = ""


# ── app factory ─────────────────────────────────────────────────────────

def create_app(db: Database, executor: JobExecutor, scheduler: JobScheduler) -> FastAPI:
    app = FastAPI(title="PyRunner", docs_url="/docs")
    start_time = datetime.now(timezone.utc)

    # ── Web UI ──────────────────────────────────────────────────────────

    @app.get("/", response_class=HTMLResponse)
    async def ui():
        return _HTML

    # ── Status ──────────────────────────────────────────────────────────

    @app.get("/api/status")
    async def status():
        now = datetime.now(timezone.utc)
        return {
            "status": "running",
            "pid": os.getpid(),
            "active_jobs": executor.active_count,
            "uptime_seconds": int((now - start_time).total_seconds()),
            "started_at": start_time.isoformat(),
        }

    # ── Jobs ────────────────────────────────────────────────────────────

    @app.get("/api/jobs")
    async def list_jobs():
        jobs = db.get_jobs()
        next_runs = scheduler.get_next_runs()
        out = []
        for j in jobs:
            last_runs = db.get_runs(j.id, limit=1)
            last = None
            if last_runs:
                lr = last_runs[0]
                last = {"id": lr.id, "status": lr.status,
                        "started_at": lr.started_at, "finished_at": lr.finished_at}
            out.append({
                "id": j.id, "name": j.name, "script_path": j.script_path,
                "cron_expression": j.cron_expression, "args": j.args,
                "enabled": j.enabled, "created_at": j.created_at,
                "next_run": next_runs.get(j.id), "last_run": last,
            })
        return out

    @app.post("/api/jobs")
    async def add_job(req: AddJobRequest):
        try:
            job = db.add_job(req.name, req.script_path, req.cron_expression, req.args)
            if job.enabled:
                scheduler.schedule(job)
            return {"id": job.id, "name": job.name}
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    @app.delete("/api/jobs/{job_id}")
    async def remove_job(job_id: str):
        scheduler.unschedule(job_id)
        db.remove_job(job_id)
        return {"ok": True}

    @app.post("/api/jobs/{job_id}/trigger")
    async def trigger_job(job_id: str):
        job = db.get_job(job_id)
        if not job:
            raise HTTPException(404, "Job not found")
        run_id = executor.execute(job_id, triggered_by="manual")
        return {"run_id": run_id}

    @app.post("/api/jobs/{job_id}/toggle")
    async def toggle_job(job_id: str):
        new_state = db.toggle_job(job_id)
        if new_state is None:
            raise HTTPException(404, "Job not found")
        job = db.get_job(job_id)
        if job and new_state:
            scheduler.schedule(job)
        else:
            scheduler.unschedule(job_id)
        return {"enabled": new_state}

    # ── Runs ────────────────────────────────────────────────────────────

    @app.get("/api/runs")
    async def list_runs(job_id: Optional[str] = None, limit: int = 50):
        runs = db.get_runs(job_id, limit)
        names = {j.id: j.name for j in db.get_jobs()}
        return [{
            "id": r.id, "job_id": r.job_id, "job_name": names.get(r.job_id, "?"),
            "status": r.status, "triggered_by": r.triggered_by,
            "started_at": r.started_at, "finished_at": r.finished_at,
            "exit_code": r.exit_code, "pid": r.pid,
            "stdout": r.stdout, "stderr": r.stderr,
        } for r in runs]

    @app.post("/api/runs/{run_id}/cancel")
    async def cancel_run(run_id: str):
        if executor.cancel(run_id):
            return {"ok": True}
        raise HTTPException(404, "Run not found or already finished")

    # ── Reload ──────────────────────────────────────────────────────────

    @app.post("/api/reload")
    async def reload():
        scheduler.reload(db)
        return {"ok": True}

    return app


# ═══════════════════════════════════════════════════════════════════════
# Embedded HTML UI
# ═══════════════════════════════════════════════════════════════════════

_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>PyRunner</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#f0f2f5;color:#1a1a2e;font-size:14px}
a{color:#2563eb;text-decoration:none}

/* header */
.hdr{background:#111827;color:#fff;padding:14px 24px;display:flex;align-items:center;justify-content:space-between}
.hdr h1{font-size:18px;font-weight:700;letter-spacing:-.3px}
.hdr h1 span{color:#60a5fa}
.hdr .st{display:flex;align-items:center;gap:8px;font-size:12px;color:#9ca3af}
.dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}

/* layout */
.wrap{max-width:1260px;margin:0 auto;padding:20px}
.card{background:#fff;border-radius:10px;box-shadow:0 1px 3px rgba(0,0,0,.06);margin-bottom:18px;overflow:hidden}
.card-hdr{padding:14px 18px;border-bottom:1px solid #e5e7eb;display:flex;justify-content:space-between;align-items:center}
.card-hdr h2{font-size:15px;font-weight:600}

/* table */
table{width:100%;border-collapse:collapse;font-size:13px}
th{text-align:left;padding:9px 14px;color:#6b7280;font-weight:500;font-size:11px;text-transform:uppercase;letter-spacing:.4px;border-bottom:1px solid #e5e7eb;background:#fafafa}
td{padding:10px 14px;border-bottom:1px solid #f3f4f6;vertical-align:middle}
tr:hover td{background:#f9fafb}

/* badge */
.b{padding:2px 8px;border-radius:10px;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.3px;white-space:nowrap}
.b-ok{background:#dcfce7;color:#16a34a}
.b-fail{background:#fee2e2;color:#dc2626}
.b-run{background:#dbeafe;color:#2563eb;animation:pulse 2s infinite}
.b-crash{background:#fef3c7;color:#d97706}
.b-cancel{background:#f3f4f6;color:#6b7280}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}

/* buttons */
.btn{padding:5px 11px;border-radius:6px;border:1px solid #d1d5db;background:#fff;cursor:pointer;font-size:11px;font-weight:500;transition:.15s}
.btn:hover{background:#f3f4f6;border-color:#9ca3af}
.bp{background:#2563eb;color:#fff;border-color:#2563eb}
.bp:hover{background:#1d4ed8}
.bd{color:#dc2626;border-color:#fca5a5}
.bd:hover{background:#fef2f2}
.acts{display:flex;gap:4px;flex-wrap:wrap}

/* toggle */
.tgl{position:relative;width:34px;height:18px;-webkit-appearance:none;appearance:none;background:#d1d5db;border-radius:9px;cursor:pointer;transition:.2s;border:none;outline:none;flex-shrink:0}
.tgl:checked{background:#2563eb}
.tgl::after{content:'';position:absolute;width:14px;height:14px;border-radius:50%;background:#fff;top:2px;left:2px;transition:.2s}
.tgl:checked::after{transform:translateX(16px)}

/* modal */
.mo{display:none;position:fixed;inset:0;background:rgba(0,0,0,.45);z-index:100;justify-content:center;align-items:center}
.mo.open{display:flex}
.md{background:#fff;border-radius:12px;padding:24px;width:500px;max-width:92vw;max-height:90vh;overflow-y:auto}
.md h3{margin-bottom:16px;font-size:16px}
.fg{margin-bottom:12px}
.fg label{display:block;font-size:12px;font-weight:600;margin-bottom:3px;color:#374151}
.fg input{width:100%;padding:8px 10px;border:1px solid #d1d5db;border-radius:6px;font-size:13px}
.fg input:focus{outline:none;border-color:#2563eb;box-shadow:0 0 0 3px rgba(37,99,235,.1)}
.fg .hint{font-size:11px;color:#9ca3af;margin-top:2px}
.fa{display:flex;gap:8px;justify-content:flex-end;margin-top:18px}

.mono{font-family:'SF Mono',SFMono-Regular,Consolas,'Liberation Mono',Menlo,monospace;font-size:12px}
.muted{color:#9ca3af}
.empty{text-align:center;padding:28px;color:#9ca3af;font-size:13px}

/* log viewer */
.logbox{background:#1e1e1e;color:#d4d4d4;padding:14px;border-radius:6px;font-family:monospace;font-size:11px;max-height:350px;overflow:auto;white-space:pre-wrap;word-break:break-all;margin-top:6px}
</style>
</head>
<body>

<div class="hdr">
  <h1><span>⚡</span> PyRunner</h1>
  <div class="st"><div class="dot" id="sDot" style="background:#6b7280"></div><span id="sTxt">Connecting…</span></div>
</div>

<div class="wrap">
  <!-- Jobs -->
  <div class="card">
    <div class="card-hdr"><h2>Registered Jobs</h2><button class="btn bp" onclick="openAdd()">+ Add Job</button></div>
    <div id="jWrap">
      <table><thead><tr><th>Name</th><th>Script</th><th>Cron</th><th>Next Run</th><th>Last Run</th><th>On</th><th>Actions</th></tr></thead>
      <tbody id="jTb"></tbody></table>
      <div id="jE" class="empty" style="display:none">No jobs registered. Click <b>+ Add Job</b> to get started.</div>
    </div>
  </div>

  <!-- Runs -->
  <div class="card">
    <div class="card-hdr"><h2>Recent Runs</h2><span class="muted" id="rCnt" style="font-size:12px"></span></div>
    <div id="rWrap">
      <table><thead><tr><th>ID</th><th>Job</th><th>Status</th><th>Trigger</th><th>Started</th><th>Duration</th><th>Actions</th></tr></thead>
      <tbody id="rTb"></tbody></table>
      <div id="rE" class="empty" style="display:none">No runs yet.</div>
    </div>
  </div>
</div>

<!-- Add modal -->
<div class="mo" id="mAdd"><div class="md">
  <h3>Add New Job</h3>
  <div class="fg"><label>Name</label><input id="fN" placeholder="daily-report"><div class="hint">Unique identifier for this job</div></div>
  <div class="fg"><label>Script Path</label><input id="fS" placeholder="/home/user/scripts/report.py"><div class="hint">Absolute or relative path to a Python script</div></div>
  <div class="fg"><label>Cron Expression</label><input id="fC" placeholder="*/5 * * * *"><div class="hint">5-field cron: minute hour day month weekday</div></div>
  <div class="fg"><label>Arguments <span class="muted">(optional)</span></label><input id="fA" placeholder="--verbose --date today"></div>
  <div class="fa"><button class="btn" onclick="closeAdd()">Cancel</button><button class="btn bp" onclick="doAdd()">Add Job</button></div>
</div></div>

<!-- Log modal -->
<div class="mo" id="mLog"><div class="md" style="width:720px">
  <h3 id="lT">Logs</h3>
  <div><label style="font-size:11px;font-weight:700;color:#374151">STDOUT</label><div class="logbox" id="lO">(empty)</div></div>
  <div style="margin-top:10px"><label style="font-size:11px;font-weight:700;color:#dc2626">STDERR</label><div class="logbox" id="lE">(empty)</div></div>
  <div class="fa"><button class="btn" onclick="closeLog()">Close</button></div>
</div></div>

<script>
let R=[];
const $=id=>document.getElementById(id);
const esc=s=>{const d=document.createElement('div');d.textContent=s||'';return d.innerHTML};

function ago(iso){if(!iso)return'-';const s=Math.floor((Date.now()-new Date(iso))/1e3);if(s<0)return'in '+fmt(-s);return fmt(s)+' ago'}
function fmt(s){if(s<60)return s+'s';if(s<3600)return Math.floor(s/60)+'m';if(s<86400)return Math.floor(s/3600)+'h';return Math.floor(s/86400)+'d'}
function dur(a,b){if(!a||!b)return'-';const s=(new Date(b)-new Date(a))/1e3;if(s<60)return s.toFixed(1)+'s';if(s<3600)return Math.floor(s/60)+'m '+Math.floor(s%60)+'s';return Math.floor(s/3600)+'h '+Math.floor((s%3600)/60)+'m'}
function ts(iso){if(!iso)return'-';return new Date(iso).toLocaleString()}
function bg(st){const m={success:'b-ok',failed:'b-fail',running:'b-run',crashed:'b-crash',cancelled:'b-cancel'};return `<span class="b ${m[st]||''}">${st}</span>`}

async function fStatus(){try{const r=await(await fetch('/api/status')).json();$('sDot').style.background='#4ade80';$('sTxt').textContent=`Running · PID ${r.pid} · ${r.active_jobs} active`}catch{$('sDot').style.background='#ef4444';$('sTxt').textContent='Disconnected'}}

async function fJobs(){const jobs=await(await fetch('/api/jobs')).json();const tb=$('jTb'),em=$('jE');if(!jobs.length){tb.innerHTML='';em.style.display='';return}em.style.display='none';
tb.innerHTML=jobs.map(j=>`<tr>
<td><strong>${esc(j.name)}</strong><br><span class="mono muted">${j.id}</span></td>
<td class="mono" style="max-width:220px;overflow:hidden;text-overflow:ellipsis">${esc(j.script_path)}${j.args?' <span class="muted">'+esc(j.args)+'</span>':''}</td>
<td class="mono">${esc(j.cron_expression)}</td>
<td>${j.next_run?ago(j.next_run)+'<br><span class="muted" style="font-size:10px">'+ts(j.next_run)+'</span>':'-'}</td>
<td>${j.last_run?bg(j.last_run.status)+' '+ago(j.last_run.started_at):'-'}</td>
<td><input type="checkbox" class="tgl" ${j.enabled?'checked':''} onchange="doToggle('${j.id}')"></td>
<td class="acts"><button class="btn" onclick="doTrigger('${j.id}')">▶ Run</button><button class="btn bd" onclick="doRemove('${j.id}','${esc(j.name)}')">✕</button></td>
</tr>`).join('')}

async function fRuns(){R=await(await fetch('/api/runs?limit=30')).json();const tb=$('rTb'),em=$('rE');$('rCnt').textContent=R.length+' shown';if(!R.length){tb.innerHTML='';em.style.display='';return}em.style.display='none';
tb.innerHTML=R.map(r=>`<tr>
<td class="mono">${r.id}</td><td>${esc(r.job_name)}</td><td>${bg(r.status)}</td><td>${r.triggered_by}</td>
<td>${ago(r.started_at)}<br><span class="muted" style="font-size:10px">${ts(r.started_at)}</span></td>
<td class="mono">${r.status==='running'?bg('running'):dur(r.started_at,r.finished_at)}</td>
<td class="acts"><button class="btn" onclick="showLog('${r.id}')">📋</button>${r.status==='running'?'<button class="btn bd" onclick="doCancel(\''+r.id+'\')">⏹</button>':''}</td>
</tr>`).join('')}

async function doTrigger(id){await fetch('/api/jobs/'+id+'/trigger',{method:'POST'});refresh()}
async function doToggle(id){await fetch('/api/jobs/'+id+'/toggle',{method:'POST'});refresh()}
async function doRemove(id,n){if(!confirm('Delete job "'+n+'" and all its history?'))return;await fetch('/api/jobs/'+id,{method:'DELETE'});refresh()}
async function doCancel(id){await fetch('/api/runs/'+id+'/cancel',{method:'POST'});refresh()}

function showLog(id){const r=R.find(x=>x.id===id);if(!r)return;$('lT').textContent='Logs — '+r.job_name+' ('+r.id+')';$('lO').textContent=r.stdout||'(empty)';$('lE').textContent=r.stderr||'(empty)';$('mLog').classList.add('open')}

function openAdd(){$('mAdd').classList.add('open');$('fN').focus()}
function closeAdd(){$('mAdd').classList.remove('open')}
function closeLog(){$('mLog').classList.remove('open')}

async function doAdd(){
  const[n,s,c,a]=[$('fN').value.trim(),$('fS').value.trim(),$('fC').value.trim(),$('fA').value.trim()];
  if(!n||!s||!c){alert('Name, script and cron are required.');return}
  const r=await fetch('/api/jobs',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:n,script_path:s,cron_expression:c,args:a})});
  if(!r.ok){const e=await r.json();alert(e.detail||'Error');return}
  closeAdd();['fN','fS','fC','fA'].forEach(i=>$(i).value='');refresh()
}

document.querySelectorAll('.mo').forEach(el=>el.addEventListener('click',e=>{if(e.target===el)el.classList.remove('open')}));

async function refresh(){await Promise.all([fStatus(),fJobs(),fRuns()])}
refresh();setInterval(refresh,5000);
</script>
</body>
</html>"""
