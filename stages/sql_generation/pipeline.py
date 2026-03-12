"""
SQL generation stage coordinator (intent_divide -> DAG -> per-intent runtime).

This module focuses on engineering/coordination:
- deterministic state management (GlobalState)
- WAIT_USER dialog integration (single global queue)
- auditable stage output
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

from config.app_config import get_app_config
from stages.intent_divide.main import divide_intents_with_audit
from stages.intent_divide.models import IntentDivideStatus
from stages.sql_generation.divide_resume import build_intent_divide_resume_query
from stages.sql_generation.dag import DAGScheduler, SchedulerConfig
from stages.sql_generation.dag.models import GlobalState, NodeStatus
from stages.sql_generation.intent.dialog import create_dialog_ticket, get_active_dialog_ticket, submit_dialog_user_message
from stages.sql_generation.intent.dialog_queue import get_dialog_repository
from stages.sql_generation.intent.models import DialogResolutionType
from stages.sql_generation.worker_runtime import (
    LocalIsolatedThreadWorkerExecutor,
    WorkerRuntimeConfig,
    run_scheduler_until_blocked_or_done,
)
from utils.id_generator import new_request_id
from utils.logger import attach_request_log_file, detach_request_log_file, get_logger

logger = get_logger("sql_generation_pipeline")
_PIPELINE_CFG = get_app_config().stages.sql_generation.pipeline


class StageStatus(str, Enum):
    SUCCESS = "SUCCESS"
    WAIT_USER = "WAIT_USER"
    FAILED = "FAILED"


@dataclass
class SQLStageResult:
    status: StageStatus
    state: GlobalState
    dialog_ticket: Optional[Dict[str, Any]] = None
    error: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status.value,
            "dialog_ticket": self.dialog_ticket,
            "error": self.error,
        }


def _any_wait_user(state: GlobalState) -> bool:
    return any(n.status == NodeStatus.WAIT_USER for n in state.intent_map.values())


def _first_wait_user_intent_id(state: GlobalState) -> Optional[str]:
    for intent_id, node in state.intent_map.items():
        if node.status == NodeStatus.WAIT_USER:
            return intent_id
    return None


def _empty_stage_state(*, context: Dict[str, Any], model_name: str, max_concurrency: int, query: str) -> GlobalState:
    scheduler = DAGScheduler(
        intents=[],
        config=SchedulerConfig(max_concurrency=int(max_concurrency)),
        tool_registry=None,
    )
    scheduler.state.config["context"] = dict(context)
    scheduler.state.config["model_name"] = model_name
    scheduler.state.config["max_concurrency"] = int(max_concurrency)
    scheduler.state.config["divide_query"] = query
    scheduler.state.config["divide_wait_user"] = False
    return scheduler.state


def run_sql_generation_stage(
    *,
    query: str,
    context: Dict[str, Any],
    model_name: Optional[str] = None,
    max_concurrency: Optional[int] = None,
) -> SQLStageResult:
    """
    End-to-end stage run:
    1) intent_divide: query -> intents (with deps)
    2) DAG scheduler: build + run intents
    3) If any intent is WAIT_USER: return dialog ticket for UI

    每次用户请求会生成一个详细日志文件 log/request_{request_id}_{时间}.log，记录意图分解、autolink、SQL 生成等全链路。
    """
    # Fresh checkouts may not have `data/` or `log/` directories.
    from utils.data_paths import DataPaths

    DataPaths.default().ensure_base_dirs()
    request_id = new_request_id()
    log_path = attach_request_log_file(request_id)
    logger.info("请求开始 | request_id=%s | 详细日志: %s", request_id, log_path)
    try:
        return _run_sql_generation_stage_impl(
            query=query,
            context=context,
            model_name=model_name,
            max_concurrency=max_concurrency,
            request_id=request_id,
            request_log_path=log_path,
        )
    finally:
        detach_request_log_file()


def _run_sql_generation_stage_impl(
    *,
    query: str,
    context: Dict[str, Any],
    model_name: Optional[str],
    max_concurrency: Optional[int],
    request_id: str,
    request_log_path: str,
) -> SQLStageResult:
    resolved_model_name = str(model_name or _PIPELINE_CFG.model_name)
    resolved_max_concurrency = int(max_concurrency or _PIPELINE_CFG.max_concurrency)
    database_scope = list(context.get("database_scope") or get_app_config().get_default_database_scope())
    if not database_scope:
        raise ValueError("context.database_scope is required or must be configured in /Users/w1412x/Files/self/final/src/config/json/database.json")
    merged_context = dict(context)
    merged_context.setdefault("request_log_path", request_log_path)
    merged_context.setdefault("database_scope", database_scope)
    merged_context.setdefault("max_rows", _PIPELINE_CFG.max_rows)
    merged_context.setdefault("max_rounds_per_intent", _PIPELINE_CFG.max_rounds_per_intent)
    merged_context.setdefault("model_name", resolved_model_name)
    state = _empty_stage_state(context=merged_context, model_name=resolved_model_name, max_concurrency=resolved_max_concurrency, query=query)

    divide_out, divide_audit = divide_intents_with_audit(
        query=query,
        database_names=list(database_scope),
        model_name=resolved_model_name,
        verbose=bool(merged_context.get("verbose_intent_divide", False)),
    )
    state.audit_log.append({"event": "intent_divide", "audit": divide_audit.to_dict()})
    if divide_out.status == IntentDivideStatus.WAIT_USER:
        ticket_payload = dict(divide_out.dialog_ticket or {})
        created = create_dialog_ticket(
            state=state,
            intent_id="__intent_divide__",
            question_id=str(ticket_payload.get("question_id") or "intent_divide_clarification"),
            phase="INTENT_DIVIDE",
            payload=ticket_payload,
            thread_id=str(ticket_payload.get("thread_id") or ""),
        )
        state.config["divide_wait_user"] = True
        state.config["divide_ticket_id"] = str(created.get("ticket_id") or "")
        return SQLStageResult(status=StageStatus.WAIT_USER, state=state, dialog_ticket=get_active_dialog_ticket(state))

    scheduler = DAGScheduler(
        intents=divide_out.intents,
        config=SchedulerConfig(max_concurrency=resolved_max_concurrency),
        tool_registry=None,
    )
    scheduler.state.audit_log.extend(state.audit_log)
    scheduler.state.config["context"] = dict(merged_context)
    scheduler.state.config["model_name"] = resolved_model_name
    scheduler.state.config["max_concurrency"] = resolved_max_concurrency
    scheduler.state.config["divide_query"] = query
    scheduler.state.config["divide_wait_user"] = False

    logger.info("SQL 生成阶段开始 | 意图数=%s（每个意图依次：Schema/Autolink→关系代数→SQL→验证→执行）", len(divide_out.intents), intent_count=len(divide_out.intents), database_scope=database_scope)
    runtime_config = WorkerRuntimeConfig(
        model_name=resolved_model_name,
        max_rows=int(merged_context.get("max_rows", _PIPELINE_CFG.max_rows)),
        max_rounds=int(merged_context.get("max_rounds_per_intent", _PIPELINE_CFG.max_rounds_per_intent)),
        max_workers=max(1, resolved_max_concurrency),
    )
    with LocalIsolatedThreadWorkerExecutor(scheduler=scheduler, config=runtime_config) as worker_executor:
        run_scheduler_until_blocked_or_done(
            scheduler=scheduler,
            worker_executor=worker_executor,
            max_workers=runtime_config.max_workers,
        )

    if _any_wait_user(scheduler.state):
        ticket = get_active_dialog_ticket(scheduler.state)
        # If queue has no active ticket (should be rare), at least surface which intent is blocked.
        if ticket is None:
            intent_id = _first_wait_user_intent_id(scheduler.state)
            candidate_ticket_id = ""
            if intent_id and intent_id in scheduler.state.intent_map:
                node = scheduler.state.intent_map[intent_id]
                final_payload = node.artifacts.get("final") or {}
                if isinstance(final_payload, dict):
                    ticket_stub = final_payload.get("ticket") or {}
                    if isinstance(ticket_stub, dict):
                        candidate_ticket_id = str(ticket_stub.get("ticket_id") or "")
            if candidate_ticket_id:
                repo = get_dialog_repository(scheduler.state)
                record = repo.get_ticket(candidate_ticket_id)
                if record and not record.resolved:
                    ticket = {
                        "ticket_id": record.ticket_id,
                        "intent_id": record.intent_id,
                        "question_id": record.question_id,
                        "thread_id": record.thread_id,
                        "created_at": record.created_at,
                        "phase": record.phase,
                        "payload": dict(record.payload),
                        "turns": list(record.turns),
                        "resolved": bool(record.resolved),
                        "resolution_type": record.resolution_type.value if record.resolution_type else "",
                    }
            if ticket is None:
                ticket = {"intent_id": intent_id, "note": "intent WAIT_USER but dialog queue is empty"}
        return SQLStageResult(status=StageStatus.WAIT_USER, state=scheduler.state, dialog_ticket=ticket)

    failed = [n for n in scheduler.state.intent_map.values() if n.status == NodeStatus.FAILED]
    if failed:
        return SQLStageResult(status=StageStatus.FAILED, state=scheduler.state, error=str(failed[0].artifacts.get("error") or "intent failed"))

    return SQLStageResult(status=StageStatus.SUCCESS, state=scheduler.state)


def resume_sql_generation_stage_after_user_reply(
    *,
    state: GlobalState,
    ticket_id: str,
    user_message: str,
    message_id: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
    model_name: Optional[str] = None,
) -> SQLStageResult:
    """
    Resume a previously WAIT_USER-blocked stage after receiving user reply.

    Caller should persist `state` externally (e.g., via dag.serialize.state_to_dict).
    """
    resolved_model_name = str(model_name or state.config.get("model_name") or _PIPELINE_CFG.model_name)
    # 1) submit message -> resolved? + hints merged
    if not state.intent_map and bool(state.config.get("divide_wait_user")):
        expected_ticket_id = str(state.config.get("divide_ticket_id") or "")
        if expected_ticket_id and ticket_id != expected_ticket_id:
            return SQLStageResult(status=StageStatus.FAILED, state=state, error="divide-stage ticket_id mismatch")
        repo = get_dialog_repository(state)
        record = repo.get_ticket(ticket_id)
        if record is None:
            return SQLStageResult(status=StageStatus.FAILED, state=state, error="divide-stage unknown ticket_id")
        if record.resolved:
            return SQLStageResult(status=StageStatus.FAILED, state=state, error="divide-stage ticket already resolved")
        ticket = repo.append_turn(ticket_id=ticket_id, user_message=user_message, message_id=message_id)
        repo.mark_resolved(ticket_id, DialogResolutionType.RESOLVED)
        previous_messages = [str(turn.get("user_message") or "") for turn in list(ticket.turns or []) if str(turn.get("user_message") or "").strip()]
        enriched_query = build_intent_divide_resume_query(
            original_query=str(state.config.get("divide_query") or "").strip(),
            question_id=str(ticket.question_id or ""),
            ticket_payload=dict(ticket.payload or {}),
            user_messages=previous_messages,
        )
        merged_context = dict(state.config.get("context") or {})
        if context:
            merged_context.update(context)
        resumed = run_sql_generation_stage(
            query=enriched_query,
            context=merged_context,
            model_name=resolved_model_name,
            max_concurrency=int(state.config.get("max_concurrency", _PIPELINE_CFG.max_concurrency) or _PIPELINE_CFG.max_concurrency),
        )
        resumed.state.config["divide_query"] = str(state.config.get("divide_query") or enriched_query)
        return resumed

    # Validate ticket before mutating state (avoid replying to wrong/resolved tickets).
    repo = get_dialog_repository(state)
    record = repo.get_ticket(ticket_id)
    if record is None:
        return SQLStageResult(status=StageStatus.FAILED, state=state, error="unknown ticket_id")
    if record.resolved:
        return SQLStageResult(status=StageStatus.FAILED, state=state, error="ticket already resolved")
    if record.intent_id not in state.intent_map:
        return SQLStageResult(status=StageStatus.FAILED, state=state, error=f"ticket intent_id not found in state: {record.intent_id}")
    resume_phase = str((record.payload or {}).get("resume_phase") or "").strip()
    if resume_phase and str(record.phase or "").strip() and resume_phase != str(record.phase):
        return SQLStageResult(status=StageStatus.FAILED, state=state, error="ticket phase/resume_phase mismatch")

    dialog_out = submit_dialog_user_message(
        state=state,
        ticket_id=ticket_id,
        user_message=user_message,
        model_name=resolved_model_name,
        message_id=message_id,
    )
    intent_id = str(dialog_out.get("intent_id") or "")
    if not intent_id:
        return SQLStageResult(status=StageStatus.FAILED, state=state, error="dialog resolution missing intent_id")

    if not dialog_out.get("resolved"):
        # Still waiting; return updated ask content
        ticket = get_active_dialog_ticket(state)
        return SQLStageResult(status=StageStatus.WAIT_USER, state=state, dialog_ticket=ticket)

    # 2) unblock intent + continue running
    from stages.sql_generation.dag.scheduler import DAGScheduler

    scheduler = DAGScheduler.from_state(state)
    scheduler.emit_user_reply_received(intent_id, {"ticket_id": ticket_id, "source": "dialog_resume"})
    scheduler.drain_events()

    if context:
        merged_context = dict(scheduler.state.config.get("context") or {})
        merged_context.update(dict(context))
        scheduler.state.config["context"] = merged_context

    runtime_config = WorkerRuntimeConfig(
        model_name=resolved_model_name,
        max_rows=int((scheduler.state.config.get("context") or {}).get("max_rows", _PIPELINE_CFG.max_rows)),
        max_rounds=int((scheduler.state.config.get("context") or {}).get("max_rounds_per_intent", _PIPELINE_CFG.max_rounds_per_intent)),
        max_workers=max(1, int(scheduler.state.config.get("max_concurrency", _PIPELINE_CFG.max_concurrency))),
    )
    with LocalIsolatedThreadWorkerExecutor(scheduler=scheduler, config=runtime_config) as worker_executor:
        run_scheduler_until_blocked_or_done(
            scheduler=scheduler,
            worker_executor=worker_executor,
            max_workers=runtime_config.max_workers,
        )

    if _any_wait_user(scheduler.state):
        return SQLStageResult(status=StageStatus.WAIT_USER, state=scheduler.state, dialog_ticket=get_active_dialog_ticket(scheduler.state))

    failed = [n for n in scheduler.state.intent_map.values() if n.status == NodeStatus.FAILED]
    if failed:
        return SQLStageResult(status=StageStatus.FAILED, state=scheduler.state, error=str(failed[0].artifacts.get("error") or "intent failed"))

    return SQLStageResult(status=StageStatus.SUCCESS, state=scheduler.state)
