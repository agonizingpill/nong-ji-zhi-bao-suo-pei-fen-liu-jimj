import os,hmac,logging,asyncio,contextlib
from contextlib import asynccontextmanager
from fastapi import FastAPI,Depends,HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy import select,text
from sqlalchemy.exc import IntegrityError
from redis import Redis
from .database import Base,engine,Session
from .models import User,Machine,Claim,Audit
from .security import current,allow,password_hash,token_for
from .schemas import Login,MachineIn,ClaimIn,InspectionIn,DecisionIn
from .claim_service import obj,get,lock_claim,audit,claim_view,submit,inspect,adjudicate
from .risk_run_service import (RiskRunConflict,dispatch,enqueue_claim,latest_run_for_claim,
                               reconcile,run_view,takeover_run)
from .middlewares import RequestTrace,Recovery
from .fulfilment_routes import router as fulfilment_router
from .workshop_routes import router as workshop_router
from scripts.seed_demo import seed
logging.basicConfig(level=logging.INFO)
r=Redis.from_url(os.getenv('REDIS_URL','redis://redis:6379/0'),decode_responses=True)
async def reconcile_loop():
    """后端自身也周期恢复；worker 全部退出时，受理后的任务仍能在重启/接管后完成。"""
    while True:
        try:await asyncio.to_thread(reconcile)
        except Exception:logging.exception('risk reconcile failed')
        await asyncio.sleep(5)

@asynccontextmanager
async def lifespan(app):
    Base.metadata.create_all(engine)
    if os.getenv('SEED_DEMO','true').lower()=='true':
        try:seed()
        except Exception:logging.exception('seed failed')
    # 重启后自动恢复：接管失联运行、作废已签署索赔的活动运行、补投未入队任务
    try:reconcile()
    except Exception:logging.exception('risk reconcile failed on startup')
    task=asyncio.create_task(reconcile_loop())
    try:yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):await task
app=FastAPI(title='耕川 · 质保田间档案',lifespan=lifespan)
app.add_middleware(RequestTrace);app.add_middleware(Recovery)
app.include_router(fulfilment_router)
app.include_router(workshop_router)
@app.exception_handler(IntegrityError)
async def duplicate(request,e):return JSONResponse({'detail':'编号已存在或关联数据不合法'},status_code=409)
@app.exception_handler(RiskRunConflict)
async def risk_conflict(request,e):return JSONResponse({'detail':str(e)},status_code=409)
@app.get('/health')
def health():
    with Session() as s:s.execute(text('SELECT 1'))
    return {'status':'ok','redis':r.ping()}
@app.post('/auth/login')
def login(data:Login):
    with Session() as s:
        u=s.scalar(select(User).where(User.email==data.email))
        if not u or not hmac.compare_digest(u.password,password_hash(data.password)):raise HTTPException(401,'账号或密码不正确')
        return {'token':token_for(u),'role':u.role,'email':u.email}
@app.get('/machines')
def machines(u=Depends(current)):
    with Session() as s:return [obj(x) for x in s.scalars(select(Machine).order_by(Machine.id))]
@app.post('/machines')
def add_machine(data:MachineIn,u=Depends(current)):
    allow(u,'admin','assessor')
    with Session.begin() as s:
        x=Machine(**data.model_dump());s.add(x);s.flush();audit(s,u,'machine_registered',x.serial);return obj(x)
@app.put('/machines/{id}')
def update_machine(id:int,data:MachineIn,u=Depends(current)):
    allow(u,'admin')
    with Session.begin() as s:
        m=get(s,Machine,id)
        if data.hours<m.hours:raise HTTPException(409,'机具累计工时不可倒退')
        for k,v in data.model_dump().items():setattr(m,k,v)
        audit(s,u,'machine_updated',m.serial);return obj(m)
@app.get('/claims')
def claims(u=Depends(current)):
    with Session() as s:return [claim_view(s,c) for c in s.scalars(select(Claim).where(Claim.historical==False).order_by(Claim.id.desc()))]
@app.post('/claims')
def add_claim(data:ClaimIn,u=Depends(current)):
    allow(u,'admin','assessor')
    with Session.begin() as s:return submit(s,data,u)
@app.post('/claims/{id}/assess')
def assess_claim(id:int,u=Depends(current)):
    allow(u,'admin','assessor')
    with Session.begin() as s:
        c=lock_claim(s,id)
        if c.historical or c.status not in ['submitted','failed']:raise HTTPException(409,'当前索赔状态不可分流')
        if c.hours_at_failure>s.get(Machine,c.machine_id).warranty_hours:
            c.status='out_of_warranty';c.assessment_note='超出质保工时';audit(s,u,'warranty_rejected',c.code)
            result=claim_view(s,c);run=None
        else:
            run,created=enqueue_claim(s,c,u['email'])
            result=claim_view(s,c)
    if run is not None:dispatch(run.id)
    return result

@app.get('/claims/{id}/risk-run')
def claim_risk_run(id:int,u=Depends(current)):
    with Session() as s:
        c=get(s,Claim,id);run=latest_run_for_claim(s,c.id)
        if not run:raise HTTPException(404,'该索赔还没有分流运行记录')
        return run_view(run)

@app.post('/claims/{id}/risk-run/retry')
def retry_claim_risk(id:int,u=Depends(current)):
    """从失败/失联的原次任务重试：新建一次运行，原次保留为审计依据。"""
    allow(u,'admin','assessor')
    with Session.begin() as s:
        c=lock_claim(s,id)
        if c.historical:raise HTTPException(409,'历史已结案记录不可重试分流')
        if c.status not in ['submitted','failed']:raise HTTPException(409,'仅未完成或失败的分流可以重试')
        previous=latest_run_for_claim(s,c.id)
        if previous and previous.state=='running':raise HTTPException(409,'原次任务仍由执行进程持有，请等待租约到期后自动接管')
        run,created=enqueue_claim(s,c,u['email'],retry_of=previous)
        if previous and created:previous.superseded_by_run_id=run.id
        result=claim_view(s,c)
    dispatch(run.id)
    return result

@app.post('/claims/{id}/risk-run/takeover')
def takeover_claim_risk(id:int,u=Depends(current)):
    """立即接管失联执行者的原次任务，不等租约自然到期。"""
    allow(u,'admin')
    with Session.begin() as s:
        c=get(s,Claim,id);run=latest_run_for_claim(s,c.id)
        if not run:raise HTTPException(409,'该索赔还没有可接管的分流任务')
    outcome=takeover_run(run.id)
    with Session() as s:return {'result':outcome,**run_view(latest_run_for_claim(s,id))}

@app.post('/risk-runs/reconcile')
def reconcile_runs(u=Depends(current)):
    allow(u,'admin')
    return reconcile()
@app.put('/claims/{id}/inspection')
def save_inspection(id:int,data:InspectionIn,u=Depends(current)):
    allow(u,'admin','assessor')
    with Session.begin() as s:return inspect(s,lock_claim(s,id),data,u)
@app.post('/claims/{id}/decision')
def decide(id:int,data:DecisionIn,u=Depends(current)):
    allow(u,'admin')
    with Session.begin() as s:return adjudicate(s,lock_claim(s,id),data.decision,u)
@app.get('/audit')
def audit_log(u=Depends(current)):
    with Session() as s:return [obj(x) for x in s.scalars(select(Audit).order_by(Audit.id.desc()).limit(40))]
