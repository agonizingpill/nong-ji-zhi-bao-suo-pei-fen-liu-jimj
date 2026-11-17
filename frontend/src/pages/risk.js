import{heading,badge}from'../components/common.js';import{esc,pct,localTime}from'../utils/format.js';import{api}from'../utils/api.js';import{PARTS,RISK_RUN_STATES,RISK_RUN_OUTCOMES}from'../constants.js';

function metricsOf(s){const c=s.claims.find(x=>!x.historical&&x.risk!=null&&x.risk_run?.state==='succeeded')||s.claims.find(x=>x.risk!=null);try{return JSON.parse(c?.risk_run?.evidence||c?.assessment_note)}catch{return null}}
function runLine(c){
  const r=c.risk_run;if(!r)return '<p class="muted">尚未登记分流运行。</p>';
  const state=RISK_RUN_STATES[r.state]||r.state;
  const parts=[`第 ${r.generation} 次运行 · ${state}`];
  if(r.attempt)parts.push(`已执行 ${r.attempt} 次`);
  if(r.takeover_count)parts.push(`接管 ${r.takeover_count} 次`);
  if(r.retry_of_run_id)parts.push(`重试原次 #${r.retry_of_run_id}`);
  if(r.superseded_by_run_id)parts.push(`已被 #${r.superseded_by_run_id} 取代`);
  const lease=r.state==='running'?`<br>执行进程 ${esc(r.lease_owner||'未知')} · 心跳 ${r.heartbeat_at?esc(localTime(r.heartbeat_at)):'—'}`:'';
  const times=[r.queued_at&&`入队 ${localTime(r.queued_at)}`,r.started_at&&`开始 ${localTime(r.started_at)}`,r.finished_at&&`结束 ${localTime(r.finished_at)}`].filter(Boolean).map(esc).join(' · ');
  const outcome=r.state==='succeeded'?`<br>本次结果：${esc(RISK_RUN_OUTCOMES[r.outcome]||r.outcome||'—')}${r.outcome==='out_of_warranty'?'（超出质保工时，不能赔付）':''}`:'';
  return `<p class="muted mono" style="font-size:12px">${parts.map(esc).join(' · ')}${lease}${times?'<br>'+times:''}${outcome}</p>`
}
function actions(c,u){
  const r=c.risk_run,viewer=u.role==='viewer';
  const retry=`<button data-retry="${c.id}" ${viewer?'disabled':''}>从失败原次重试</button>`;
  const takeover=`<button class="ghost" data-takeover="${c.id}" ${u.role!=='admin'||!['pending','queued','running'].includes(r?.state)?'disabled':''}>立即接管</button>`;
  if(c.status==='submitted'&&!r)return `<button data-assess="${c.id}" ${viewer?'disabled':''}>计算分流</button>`;
  if(r&&['pending','queued','running'].includes(r.state)&&c.status!=='failed')
    return `<span class="muted">正在核算风险（${esc(RISK_RUN_STATES[r.state]||'排队中')}），完成后自动写回。</span>${takeover}`;
  if(c.status==='failed')return retry;
  if(c.status==='submitted')return `<button data-assess="${c.id}" ${viewer?'disabled':''}>计算分流</button>`;
  if(c.status==='out_of_warranty')return '<span class="muted">过保拦截，不进入拆检。</span>';
  return '<a href="#inspection">进入拆检裁决 →</a>';
}
export function render(s,u){const metrics=metricsOf(s);return heading('03 / RISK TRIAGE','先找出需要多看一眼的索赔','根据机型、部位编码，结合工时、机龄与渗漏记录，计算争议风险。高风险阈值为 50%。')+`<div class="split"><section><div class="plate"><h2>争议风险队列</h2>${s.claims.map(c=>`<article class="sheet"><div style="display:flex;justify-content:space-between"><div><h3>${esc(c.code)} <small class="muted">/ ${esc(c.machine_serial)}</small></h3><span class="muted">${PARTS[c.part]} · ${c.hours_at_failure} h · ${c.age_days} 天 · ${c.leak_flag?'存在渗漏':'无渗漏'}</span></div>${badge(c.status)}</div><div class="bar"><span style="width:${c.risk==null?0:c.risk*100}%"></span></div><div style="display:flex;justify-content:space-between;gap:12px;align-items:center;flex-wrap:wrap"><b>争议概率 ${pct(c.risk)}</b><div class="actions" style="margin:0">${actions(c,u)}</div></div>${runLine(c)}${c.status==='failed'?`<p class="legend">失败原因：${esc(c.risk_run?.error||c.assessment_note||'未知')}</p>`:''}</article>`).join('')}</div></section><aside class="rail"><h2>模型审阅札记</h2><p class="muted">180 条模拟历史已结案索赔。按争议标签分层划分训练 / 留出集，固定随机种子 42。</p><div class="plate ochre"><span class="muted">留出集 ROC AUC</span><div class="metric">${metrics?.roc_auc??'—'}</div><p>准确率 ${metrics?.accuracy??'—'}<br>训练 ${metrics?.training_rows??135} 条 / 留出 ${metrics?.holdout_rows??45} 条</p></div><p class="legend">OneHotEncoder → StandardScaler → LogisticRegression<br><br>分类仅辅助确定拆检强度。高风险需完整证据，模型不会直接作出赔付裁决。</p><p class="legend">分流运行落库后再入队；进程中断会在重启或租约到期后自动接管原次任务。只有风险概率、模型依据与完成状态同一事务写回后，索赔才能进入拆检。</p><button class="ghost" id="refresh-risk">刷新队列</button></aside></div>`}
export function bind(refresh,rerender,store){const s=store||{claims:[]};document.querySelectorAll('[data-assess]').forEach(b=>b.onclick=()=>refresh(()=>api('/claims/'+b.dataset.assess+'/assess','POST'),'分流任务已登记，完成后自动写回'));document.querySelectorAll('[data-retry]').forEach(b=>b.onclick=()=>refresh(()=>api('/claims/'+b.dataset.retry+'/risk-run/retry','POST'),'已从失败原次重新排队'));document.querySelectorAll('[data-takeover]').forEach(b=>b.onclick=()=>refresh(()=>api('/claims/'+b.dataset.takeover+'/risk-run/takeover','POST'),'已接管原次任务并重新排队'));document.querySelector('#refresh-risk').onclick=()=>refresh(async()=>{},'');const inFlight=s.claims.some(c=>['pending','queued','running'].includes(c.risk_run?.state));if(inFlight)window.__riskPoll=setInterval(()=>refresh(async()=>{},''),5000)}
