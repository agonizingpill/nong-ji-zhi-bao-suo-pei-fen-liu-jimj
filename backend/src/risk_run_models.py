from datetime import datetime
from sqlalchemy import String,Integer,Float,Text,ForeignKey,DateTime,UniqueConstraint,Index,CheckConstraint
from sqlalchemy.orm import Mapped,mapped_column
from .database import Base

# 一次分流的持久化运行记录。队列消息只携带 RiskRun.id；
# 数据库中的 state / lease_token 才是权威，因此消息丢失、进程崩溃或重复投递都可恢复。
RUN_STATES=('pending','queued','running','succeeded','failed','blocked')
ACTIVE_RUN_STATES=('pending','queued','running')

class RiskRun(Base):
    __tablename__='risk_runs'
    __table_args__=(
        UniqueConstraint('claim_id','generation',name='uq_risk_run_generation'),
        # 同一索赔最多只有一条活动运行；结束后的 active_claim_id 置空（MySQL 允许多个 NULL）
        UniqueConstraint('active_claim_id',name='uq_risk_run_active_claim'),
        CheckConstraint("state IN ('pending','queued','running','succeeded','failed','blocked')"),
        Index('ix_risk_runs_claim_state','claim_id','state'),
        Index('ix_risk_runs_state_lease','state','lease_expires_at'),
    )
    id:Mapped[int]=mapped_column(primary_key=True)
    claim_id:Mapped[int]=mapped_column(ForeignKey('claims.id'))
    generation:Mapped[int]=mapped_column(Integer,default=1)
    state:Mapped[str]=mapped_column(String(20),default='pending')
    active_claim_id:Mapped[int|None]=mapped_column(Integer,nullable=True,default=None)
    # assessed / out_of_warranty，与风险概率、模型证据、索赔完成状态同一事务落定
    outcome:Mapped[str|None]=mapped_column(String(30),nullable=True)
    attempt:Mapped[int]=mapped_column(Integer,default=0)
    takeover_count:Mapped[int]=mapped_column(Integer,default=0)
    retry_of_run_id:Mapped[int|None]=mapped_column(Integer,nullable=True)
    superseded_by_run_id:Mapped[int|None]=mapped_column(Integer,nullable=True)
    lease_owner:Mapped[str|None]=mapped_column(String(160),nullable=True)
    lease_token:Mapped[str|None]=mapped_column(String(64),nullable=True)
    lease_expires_at:Mapped[datetime|None]=mapped_column(DateTime,nullable=True)
    heartbeat_at:Mapped[datetime|None]=mapped_column(DateTime,nullable=True)
    error:Mapped[str|None]=mapped_column(Text,nullable=True)
    risk:Mapped[float|None]=mapped_column(Float,nullable=True)
    evidence:Mapped[str|None]=mapped_column(Text,nullable=True)
    created_at:Mapped[datetime|None]=mapped_column(DateTime,nullable=True)
    queued_at:Mapped[datetime|None]=mapped_column(DateTime,nullable=True)
    started_at:Mapped[datetime|None]=mapped_column(DateTime,nullable=True)
    finished_at:Mapped[datetime|None]=mapped_column(DateTime,nullable=True)
