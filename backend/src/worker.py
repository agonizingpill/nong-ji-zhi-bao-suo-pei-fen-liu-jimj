import json,logging,os,socket,signal,threading,time
from pathlib import Path
from .database import Session
from .models import Claim,Machine
from .risk_model import assess
from .risk_run_models import RiskRun
from .risk_run_service import (QUEUE,claim_run,complete,fail,heartbeat,pop_run,reconcile,
                               heartbeat_seconds,short)

logging.basicConfig(level=logging.INFO)
log=logging.getLogger('risk-worker')
ARTIFACTS=Path(os.getenv('ARTIFACTS_DIR','/artifacts'))
WORKER_ID=f"{socket.gethostname()}:{os.getpid()}"
_stop=threading.Event()

def _write_heartbeat():
    try:
        ARTIFACTS.mkdir(parents=True,exist_ok=True)
        ARTIFACTS.joinpath('worker-heartbeat').write_text(str(time.time()))
    except Exception:log.exception('写入 worker 心跳失败')

def _write_model_artifact(run_id,claim_id,evidence):
    try:
        ARTIFACTS.mkdir(parents=True,exist_ok=True)
        target=ARTIFACTS/f'claim-{claim_id}-model.json'
        temp=target.with_suffix('.json.tmp')
        temp.write_text(evidence)
        temp.replace(target)
        ARTIFACTS.joinpath(f'risk-run-{run_id}.json').write_text(json.dumps({'run':run_id,'claim':claim_id,'evidence':json.loads(evidence)},ensure_ascii=False))
    except Exception:log.exception('写入模型证据文件失败')

class LeaseKeeper(threading.Thread):
    """后台续租；续租失败即表示已被接管，停止续租并让写回被栅栏拒绝。"""
    def __init__(self,run_id,token):
        super().__init__(daemon=True);self.run_id=run_id;self.token=token;self.lost=threading.Event();self._halt=threading.Event()
    def run(self):
        while not self._halt.wait(heartbeat_seconds()):
            try:
                if not heartbeat(self.run_id,self.token):
                    self.lost.set();log.warning('run %s 租约已被接管，停止续租',self.run_id);return
            except Exception:
                log.exception('run %s 续租失败',self.run_id)
    def stop(self):self._halt.set()

def _compute(run_id):
    """读取当前索赔并计算结果；不写任何持久化事实。"""
    with Session() as s:
        run=s.get(RiskRun,run_id)
        if not run:raise ValueError('分流任务不存在')
        c=s.get(Claim,run.claim_id)
        if not c:raise ValueError('索赔不存在')
        m=s.get(Machine,c.machine_id)
        if not m:raise ValueError('关联机具不存在')
        if c.hours_at_failure>m.warranty_hours:
            return c.id,'out_of_warranty',None,json.dumps({'outcome':'out_of_warranty','reason':'超出质保工时','warranty_hours':m.warranty_hours},ensure_ascii=False)
        risk,metrics=assess(s,c)
        return c.id,'assessed',risk,json.dumps(metrics,ensure_ascii=False)

def process(run_id):
    token=claim_run(run_id,WORKER_ID)
    if not token:
        log.info('run %s 无需执行（已结束或由其他执行进程持有）',run_id);return
    keeper=LeaseKeeper(run_id,token);keeper.start()
    try:
        claim_id,outcome,risk,evidence=_compute(run_id)
        if keeper.lost.is_set():
            log.warning('run %s 已被接管，丢弃本次结果',run_id);return
        status=complete(run_id,token,outcome,risk,evidence)
        if status=='ok':
            _write_model_artifact(run_id,claim_id,evidence)
            log.info('run %s 完成：%s risk=%s',run_id,outcome,risk)
        else:
            log.warning('run %s 写回被拒绝（%s），不覆盖新结果',run_id,status)
    except Exception as e:
        log.exception('run %s 计算失败',run_id)
        state=fail(run_id,token,f'{type(e).__name__}: {short(e)}')
        log.info('run %s 标记为 %s',run_id,state)
    finally:
        keeper.stop()

def _signal(signum,frame):
    log.info('收到信号 %s，完成当前任务后退出',signum);_stop.set()

def main():
    signal.signal(signal.SIGTERM,_signal);signal.signal(signal.SIGINT,_signal)
    log.info('分流执行进程启动 %s，队列 %s',WORKER_ID,QUEUE)
    last_reconcile=0.0
    while not _stop.is_set():
        _write_heartbeat()
        if time.time()-last_reconcile>5:
            try:reconcile()
            except Exception:log.exception('恢复扫描失败')
            last_reconcile=time.time()
        try:run_id=pop_run(timeout=1)
        except Exception:
            log.exception('读取队列失败');time.sleep(1);continue
        if run_id is None:continue
        try:process(run_id)
        except Exception:log.exception('处理 run %s 时出现未预期错误',run_id)
    log.info('分流执行进程退出 %s',WORKER_ID)

if __name__=='__main__':main()
