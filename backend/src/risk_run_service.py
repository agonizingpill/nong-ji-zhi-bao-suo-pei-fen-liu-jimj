"""可恢复的风险分流运行。

设计要点：
- 数据库 risk_runs 是唯一权威。Redis 列表只是提示，消息丢失/重复都不影响正确性。
- 每次分流有独立 run 与 generation；重试生成新 run 并通过 retry_of_run_id 关联原次。
- 执行者接管时领取租约令牌（lease_token）。写回结果必须同时匹配 state='running'
  与租约令牌，因此失联的旧执行晚回会被拒绝，不能覆盖新结果。
- 风险概率、模型依据、运行完成记录与 Claim.status 在同一事务提交；
  索赔一旦被裁决（paid/denied），写回会判为 blocked，不做任何补写。
"""
import json,logging,os,time,uuid
from datetime import datetime,timedelta,timezone
from sqlalchemy import select,update
from sqlalchemy.exc import IntegrityError
from .database import Session
from .models import Audit,Claim
from .risk_run_models import RiskRun,ACTIVE_RUN_STATES

log=logging.getLogger(__name__)
QUEUE=os.getenv('RISK_QUEUE','claim-risk')

class RiskRunConflict(Exception):
    """登记分流违反当前状态时抛出，由路由转换为 409。"""

def lease_seconds():return int(os.getenv('RISK_LEASE_SECONDS','60'))
def heartbeat_seconds():return max(3,int(os.getenv('RISK_HEARTBEAT_SECONDS','8')))
def repush_seconds():return int(os.getenv('RISK_REPUSH_SECONDS','20'))
def utcnow():return datetime.now(timezone.utc).replace(tzinfo=None)
def iso(dt):return None if dt is None else dt.replace(tzinfo=timezone.utc).isoformat()
def short(err,n=800):return (str(err) if err is not None else '')[:n]

def _audit(s,actor,action,detail):s.add(Audit(actor=actor,action=action,detail=str(detail),at=datetime.now(timezone.utc).isoformat()))

def run_view(r):
    return {'id':r.id,'claim_id':r.claim_id,'generation':r.generation,'state':r.state,
            'outcome':r.outcome,'attempt':r.attempt,'takeover_count':r.takeover_count,
            'retry_of_run_id':r.retry_of_run_id,'superseded_by_run_id':r.superseded_by_run_id,
            'lease_owner':r.lease_owner,'heartbeat_at':iso(r.heartbeat_at),
            'lease_expires_at':iso(r.lease_expires_at),'error':r.error,
            'risk':r.risk,'evidence':r.evidence,'created_at':iso(r.created_at),
            'queued_at':iso(r.queued_at),'started_at':iso(r.started_at),'finished_at':iso(r.finished_at)}

# ---------- Redis 提示队列 ----------
_redis=None
def redis_client():
    global _redis
    if _redis is None:
        from redis import Redis
        _redis=Redis.from_url(os.getenv('REDIS_URL','redis://redis:6379/0'),decode_responses=True)
    return _redis
def push_run(run_id):
    """以 run.id 为成员写入 zset：重复补投只更新时间，不会堆积重复消息。"""
    redis_client().zadd(QUEUE,{str(run_id):time.time()})
def _pop_once():
    item=redis_client().zpopmin(QUEUE,1)
    if not item:return None
    first=next(iter(item))
    member=first[0] if isinstance(first,(list,tuple)) else first  # [(member,score)] 或旧式 {member:score}
    try:return int(member)
    except (TypeError,ValueError):
        log.warning('丢弃无法识别的队列消息 %r',member);return None
def pop_run(timeout=2):
    deadline=time.time()+max(0,timeout)
    while True:
        run_id=_pop_once()
        if run_id is not None:return run_id
        if time.time()>=deadline:return None
        time.sleep(0.2)
def pop_run_nowait():return _pop_once()
def dispatch(run_id):
    """提交后投递；失败不改变数据库事实，协调扫描会补投。"""
    try:push_run(run_id);mark_pushed(run_id);return True
    except Exception:
        log.exception('risk run %s 入队失败，等待协调扫描补投',run_id);return False

# ---------- 受理 / 重试 ----------
def active_run(s,claim_id):
    return s.scalar(select(RiskRun).where(RiskRun.active_claim_id==claim_id))

def _block(s,run,reason,actor=None,now=None):
    now=now or utcnow()
    run.state='blocked';run.error=reason;run.finished_at=now
    run.lease_owner=None;run.lease_token=None;run.lease_expires_at=None
    if run.active_claim_id is not None:run.active_claim_id=None
    _audit(s,actor or 'system','risk_run_blocked',f'run={run.id} claim={run.claim_id} {reason}')

def enqueue_claim(s,claim,actor,retry_of=None):
    """在调用方事务内登记一次运行并置 claim 为 queued；返回 (run, created)。"""
    if claim.historical or claim.status not in ('submitted','failed'):
        raise RiskRunConflict('仅未完成或计算失败的现场索赔可登记分流')
    now=utcnow()
    current=active_run(s,claim.id)
    if current:
        if claim.status=='queued' and current.state in ACTIVE_RUN_STATES:
            return current,False
        _block(s,current,'索赔状态在运行期间发生变化，旧运行不再生效',actor,now)
        s.flush()
    last=s.scalar(select(RiskRun).where(RiskRun.claim_id==claim.id).order_by(RiskRun.generation.desc()).limit(1))
    run=RiskRun(claim_id=claim.id,generation=(last.generation if last else 0)+1,state='pending',
                active_claim_id=claim.id,attempt=0,takeover_count=0,created_at=now,
                retry_of_run_id=retry_of.id if retry_of else None)
    s.add(run);s.flush()
    claim.status='queued';claim.risk=None;claim.assessment_note=''
    _audit(s,actor,'risk_run_queued',f'{claim.code}:run={run.id}:gen={run.generation}'+(' 重试原次运行 '+str(retry_of.id) if retry_of else ''))
    return run,True

def latest_run_for_claim(s,claim_id):
    return s.scalar(select(RiskRun).where(RiskRun.claim_id==claim_id).order_by(RiskRun.generation.desc()).limit(1))

# ---------- 执行者领取租约 ----------
def _locked(s,run_id):
    """统一加锁顺序 Claim → RiskRun，避免与受理/裁决事务形成死锁。"""
    probe=s.get(RiskRun,run_id)
    if not probe:return None,None
    claim=s.scalar(select(Claim).where(Claim.id==probe.claim_id).with_for_update())
    run=s.scalar(select(RiskRun).where(RiskRun.id==run_id).with_for_update())
    return run,claim

def claim_run(run_id,worker_id):
    """返回租约令牌；任务已结束、已由别的执行者持有或索赔不再待算时返回 None。"""
    now=utcnow()
    with Session.begin() as s:
        run,claim=_locked(s,run_id)
        if not run or run.state not in ('pending','queued'):return None
        if not claim or claim.historical or claim.status!='queued':
            _block(s,run,'领取时索赔已不在待计算状态，任务不再执行',worker_id,now);return None
        if run.active_claim_id!=claim.id:
            _block(s,run,'该次运行已被更新的运行取代，不再执行',worker_id,now);return None
        token=uuid.uuid4().hex
        run.state='running';run.attempt=(run.attempt or 0)+1
        run.lease_owner=worker_id;run.lease_token=token
        run.heartbeat_at=now;run.lease_expires_at=now+timedelta(seconds=lease_seconds())
        if not run.started_at:run.started_at=now
        return token

def heartbeat(run_id,token):
    now=utcnow()
    with Session.begin() as s:
        res=s.execute(update(RiskRun).where(RiskRun.id==run_id,RiskRun.lease_token==token,
                                            RiskRun.state=='running')
                      .values(heartbeat_at=now,lease_expires_at=now+timedelta(seconds=lease_seconds())))
        return res.rowcount==1

# ---------- 结果写回（幂等 + 栅栏） ----------
def complete(run_id,token,outcome,risk,evidence):
    """仅当令牌仍是当前租约时一次性写回概率、模型依据与索赔状态。"""
    now=utcnow()
    if outcome=='assessed' and risk is None:
        return fail(run_id,token,'风险模型未返回概率，结果不完整')
    with Session.begin() as s:
        run,claim=_locked(s,run_id)
        if not run:return 'missing'
        if run.state!='running' or run.lease_token!=token:return 'stale'
        if not claim or claim.historical or claim.status!='queued':
            _block(s,run,'索赔已完成裁决或状态变化，本次结果不写入',run.lease_owner,now)
            return 'blocked'
        claim.risk=risk
        claim.assessment_note='超出质保工时' if outcome=='out_of_warranty' else evidence
        claim.status=outcome
        actor=run.lease_owner or 'worker'
        run.state='succeeded';run.outcome=outcome;run.risk=risk;run.evidence=evidence
        run.finished_at=now;run.heartbeat_at=now;run.error=None
        run.lease_owner=None;run.lease_token=None;run.lease_expires_at=None
        run.active_claim_id=None
        _audit(s,actor,'risk_assessed',f'{claim.code}:{outcome}:run={run.id}:risk={risk}')
        s.flush()
        return 'ok'

def fail(run_id,token,error):
    now=utcnow()
    with Session.begin() as s:
        run,claim=_locked(s,run_id)
        if not run:return 'missing'
        if run.state!='running' or run.lease_token!=token:return 'stale'
        owner=run.lease_owner;reason=short(error)
        run.error=reason;run.outcome=None;run.finished_at=now
        run.lease_owner=None;run.lease_token=None;run.lease_expires_at=None
        if claim and not claim.historical and claim.status=='queued':
            claim.status='failed';claim.risk=None;claim.assessment_note='风险计算失败：'+reason
            run.state='failed'
            _audit(s,owner or 'worker','risk_assessment_failed',f'{claim.code}:run={run.id}:{reason}')
        else:
            run.state='blocked'
            _audit(s,'system','risk_run_blocked',f'run={run.id} 失败后索赔已不可写回')
        run.active_claim_id=None
        return run.state

# ---------- 恢复与协调 ----------
def recover_expired_leases(now=None,limit=50):
    """把租约到期仍 running 的运行放回队列，供任意执行进程原地接管原次任务。"""
    now=now or utcnow()
    with Session() as s:
        ids=s.scalars(select(RiskRun.id).where(RiskRun.state=='running',RiskRun.lease_expires_at<now)
                      .order_by(RiskRun.id).limit(limit)).all()
    requeued=[]
    for rid in ids:
        with Session.begin() as s:
            run,claim=_locked(s,rid)
            if not run or run.state!='running' or not run.lease_expires_at or run.lease_expires_at>=utcnow():continue
            if not claim or claim.historical or claim.status!='queued':
                _block(s,run,'执行者失联，但索赔已不在待计算状态',None,now);continue
            owner=run.lease_owner or 'unknown'
            run.state='queued';run.lease_owner=None;run.lease_token=None;run.lease_expires_at=None
            run.queued_at=None
            run.takeover_count=(run.takeover_count or 0)+1
            run.error=f'执行进程 {owner} 租约到期，已自动接管原次任务'
            _audit(s,'system','risk_run_recovered',f'run={run.id} claim={run.claim_id} 接管原执行者 {owner}')
            requeued.append(rid)
    return requeued

def takeover_run(run_id,actor='admin'):
    """立即接管原次任务：作废当前租约令牌并重新入队，旧执行者晚回将被拒绝。"""
    now=utcnow()
    with Session.begin() as s:
        run,claim=_locked(s,run_id)
        if not run:return 'missing'
        if not claim or claim.historical or claim.status!='queued':
            _block(s,run,'接管时索赔已不在待计算状态',actor,now);return 'blocked'
        if run.state not in ('pending','queued','running'):return 'inactive'
        old=run.lease_owner
        run.state='queued';run.lease_owner=None;run.lease_token=None
        run.lease_expires_at=None;run.queued_at=None
        run.takeover_count=(run.takeover_count or 0)+1
        run.error=f'管理员 {actor} 接管原次任务'+(f'，原执行者 {old}' if old else '')
        _audit(s,actor,'risk_run_takeover',f'run={run.id} claim={run.claim_id} 原执行者 {old or "-"}')
    try:push_run(run_id);mark_pushed(run_id)
    except Exception:
        log.exception('接管后投递运行 %s 失败，协调扫描会补投',run_id);return 'requeued_unpushed'
    return 'requeued'

def finalize_orphans(limit=100):
    """活动运行存在但索赔已被裁决/删除时结束运行，绝不补写签署后的索赔。

    包括仍标为 running 的运行：签署后的索赔不可能再接受结果，租约是否到期都不影响。
    """
    done=[]
    with Session() as s:
        ids=s.scalars(select(RiskRun.id).where(RiskRun.state.in_(ACTIVE_RUN_STATES),
                                               RiskRun.active_claim_id!=None)
                      .order_by(RiskRun.id).limit(limit)).all()
    for rid in ids:
        with Session.begin() as s:
            run=s.scalar(select(RiskRun).where(RiskRun.id==rid).with_for_update())
            if not run or run.state not in ACTIVE_RUN_STATES:continue
            claim=s.get(Claim,run.claim_id)
            if claim and not claim.historical and claim.status=='queued':continue
            _block(s,run,'索赔已进入签署或历史状态，未完成的分流任务作废')
            done.append(rid)
    return done

def adopt_legacy_queued(limit=50):
    """待算索赔缺少活动运行时补建一次运行。

    覆盖旧版本遗留（只有 claim.status='queued'、没有运行记录）以及运行被作废后
    仍停在 queued 的索赔，确保每份待算索赔都有一条可被恢复扫描推进的运行。
    """
    created=[]
    with Session() as s:
        claims=s.scalars(select(Claim).where(Claim.status=='queued',Claim.historical==False).order_by(Claim.id).limit(limit)).all()
    for claim in claims:
        try:
            with Session.begin() as s:
                if s.scalar(select(Claim.status).where(Claim.id==claim.id))!='queued':continue
                if s.scalar(select(RiskRun.id).where(RiskRun.active_claim_id==claim.id).limit(1)):continue
                last=s.scalar(select(RiskRun).where(RiskRun.claim_id==claim.id).order_by(RiskRun.generation.desc()).limit(1))
                s.add(RiskRun(claim_id=claim.id,generation=(last.generation if last else 0)+1,state='pending',
                              active_claim_id=claim.id,attempt=0,takeover_count=0,created_at=utcnow(),
                              error='待算索赔缺少活动运行，补建原次任务'))
                _audit(s,'system','risk_run_recovered',f'claim={claim.id} 补建运行')
                s.flush();created.append(claim.id)
        except IntegrityError:
            log.info('claim %s 已由其他进程补建运行，跳过',claim.id)
    return created

def pending_push_ids(limit=100):
    cutoff=utcnow()-timedelta(seconds=repush_seconds())
    with Session() as s:
        return s.scalars(select(RiskRun.id).join(Claim,Claim.id==RiskRun.claim_id)
                         .where(RiskRun.state.in_(('pending','queued')),Claim.status=='queued',
                                (RiskRun.queued_at==None)|(RiskRun.queued_at<cutoff))
                         .order_by(RiskRun.id).limit(limit)).all()

def mark_pushed(run_id):
    now=utcnow()
    with Session.begin() as s:
        s.execute(update(RiskRun).where(RiskRun.id==run_id,RiskRun.state.in_(('pending','queued')))
                  .values(queued_at=now,state='queued'))

def reconcile():
    """启动时与运行期间调用：接管失联运行、作废孤儿运行、补投未入队任务。"""
    summary={'recovered':[],'blocked':[],'created':[],'pushed':[],'push_failed':[]}
    try:summary['recovered']=recover_expired_leases()
    except Exception:log.exception('接管失联运行失败')
    try:summary['blocked']=finalize_orphans()
    except Exception:log.exception('清理孤儿运行失败')
    try:summary['created']=adopt_legacy_queued()
    except Exception:log.exception('补建遗留运行失败')
    try:ids=pending_push_ids()
    except Exception:
        log.exception('收集待投递运行失败');ids=[]
    for rid in ids:
        try:push_run(rid);mark_pushed(rid);summary['pushed'].append(rid)
        except Exception:
            log.exception('补投运行 %s 失败',rid);summary['push_failed'].append(rid)
    if any(summary.values()):log.info('risk reconcile %s',json.dumps(summary))
    return summary
