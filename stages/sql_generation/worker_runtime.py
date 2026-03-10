"""
Worker runtime for SQL generation DAG execution.

This module isolates how ready work is executed from the scheduler itself, so the
pipeline can swap local threads for processes or remote workers without changing
the scheduler contract.
"""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Any, Dict, List, Protocol

from stages.sql_generation.dag import DAGScheduler, WorkItem, intent_node_to_dict
from stages.sql_generation.dag.models import DialogTicketRecord
from stages.sql_generation.dag.serialize import state_from_dict, state_to_dict
from stages.sql_generation.intent.models import DialogResolutionType
from stages.sql_generation.intent.runtime import run_intent_node
from utils.logger import get_logger

logger = get_logger("sql_generation_worker_runtime")


class WorkerExecutor(Protocol):
    def submit(self, work_item: WorkItem) -> Future: ...


@dataclass(frozen=True)
class WorkerRuntimeConfig:
    model_name: str
    max_rows: int
    max_rounds: int
    max_workers: int = 1


@dataclass(frozen=True)
class WorkerTaskPayload:
    state_data: Dict[str, Any]
    intent_id: str
    model_name: str
    max_rows: int
    max_rounds: int


@dataclass(frozen=True)
class WorkerTaskResult:
    intent_id: str
    ok: Any
    payload: Any
    node_data: Dict[str, Any]
    dialog_tickets: List[Dict[str, Any]]
    active_ticket_id: str = ""


class LocalIsolatedThreadWorkerExecutor:
    """
    Thread-based worker executor with process-like isolation.

    This executor snapshots state per task and merges results back, matching the
    isolated-worker contract while staying in a single process.
    """

    def __init__(self, *, scheduler: DAGScheduler, config: WorkerRuntimeConfig) -> None:
        self.scheduler = scheduler
        self.config = config
        self._executor = ThreadPoolExecutor(
            max_workers=max(1, int(config.max_workers)),
            thread_name_prefix="sqlgen-intent",
        )

    def submit(self, work_item: WorkItem) -> Future:
        payload = WorkerTaskPayload(
            state_data=state_to_dict(self.scheduler.state),
            intent_id=work_item.intent_id,
            model_name=self.config.model_name,
            max_rows=self.config.max_rows,
            max_rounds=self.config.max_rounds,
        )
        return self._executor.submit(_execute_worker_task, payload)

    def shutdown(self) -> None:
        self._executor.shutdown(wait=True, cancel_futures=False)

    def __enter__(self) -> "LocalIsolatedThreadWorkerExecutor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.shutdown()


def _execute_worker_task(payload: WorkerTaskPayload) -> WorkerTaskResult:
    state = state_from_dict(payload.state_data)
    # 在线程 worker 内挂载同一请求日志文件，使 intent_runtime、autolink 等日志写入 request 文件。
    # （如果主线程已 attach，相同文件会被视为已挂载，函数将幂等返回）
    request_log_path = (state.config.get("context") or {}).get("request_log_path")
    if request_log_path:
        from utils.logger import add_request_log_file_in_process
        add_request_log_file_in_process(request_log_path)
    node = state.intent_map.get(payload.intent_id)
    if node is None:
        raise ValueError(f"unknown intent_id: {payload.intent_id}")
    ok, result_payload = run_intent_node(
        node,
        state,
        model_name=payload.model_name,
        max_rows=payload.max_rows,
        max_rounds=payload.max_rounds,
    )
    tickets = []
    for ticket in state.dialog_state.tickets.values():
        tickets.append(
            {
                "ticket_id": ticket.ticket_id,
                "intent_id": ticket.intent_id,
                "question_id": ticket.question_id,
                "phase": ticket.phase,
                "created_at": ticket.created_at,
                "payload": dict(ticket.payload),
                "thread_id": ticket.thread_id,
                "turns": list(ticket.turns),
                "resolved": bool(ticket.resolved),
                "resolution_type": ticket.resolution_type.value if ticket.resolution_type else "",
            }
        )
    active_ticket_id = str(state.dialog_state.active_ticket_id or "")
    return WorkerTaskResult(
        intent_id=payload.intent_id,
        ok=ok,
        payload=result_payload,
        node_data=intent_node_to_dict(node),
        dialog_tickets=tickets,
        active_ticket_id=active_ticket_id,
    )


def _merge_worker_result(scheduler: DAGScheduler, result: WorkerTaskResult) -> None:
    node = scheduler.state.intent_map.get(result.intent_id)
    if node is not None:
        node.artifacts = dict((result.node_data or {}).get("artifacts") or {})

    if result.dialog_tickets:
        for ticket in result.dialog_tickets:
            ticket_id = str(ticket.get("ticket_id") or "")
            if not ticket_id:
                continue
            scheduler.state.dialog_state.tickets[ticket_id] = DialogTicketRecord(
                ticket_id=ticket_id,
                intent_id=str(ticket.get("intent_id") or ""),
                question_id=str(ticket.get("question_id") or ""),
                phase=str(ticket.get("phase") or ""),
                created_at=float(ticket.get("created_at") or 0.0),
                payload=dict(ticket.get("payload") or {}),
                thread_id=str(ticket.get("thread_id") or ""),
                turns=list(ticket.get("turns") or []),
                resolved=bool(ticket.get("resolved")),
                resolution_type=DialogResolutionType(str(ticket.get("resolution_type")))
                if ticket.get("resolution_type")
                else None,
            )
            if ticket_id not in scheduler.state.dialog_state.queue and not scheduler.state.dialog_state.tickets[ticket_id].resolved:
                scheduler.state.dialog_state.queue.append(ticket_id)
        if result.active_ticket_id and not scheduler.state.dialog_state.active_ticket_id:
            scheduler.state.dialog_state.active_ticket_id = result.active_ticket_id


def run_scheduler_until_blocked_or_done(
    *,
    scheduler: DAGScheduler,
    worker_executor: WorkerExecutor,
    max_workers: int,
) -> None:
    inflight: Dict[Future, WorkItem] = {}
    worker_slots = max(1, int(max_workers))

    while True:
        while len(inflight) < worker_slots:
            polled = scheduler.poll_work(limit=worker_slots - len(inflight))
            if not polled:
                break
            for work_item in polled:
                logger.info("调度：开始执行意图 | intent_id=%s", work_item.intent_id)
                inflight[worker_executor.submit(work_item)] = work_item

        if not inflight:
            scheduler.drain_events()
            if scheduler.is_finished() or (not scheduler.state.pending_events and not scheduler.has_ready()):
                break
            continue

        done, _ = wait(inflight.keys(), return_when=FIRST_COMPLETED)
        for future in done:
            work_item = inflight.pop(future)
            try:
                result = future.result()
            except Exception as exc:
                scheduler.submit_work_result(
                    work_item.intent_id,
                    False,
                    f"handler_exception:{type(exc).__name__}:{exc}",
                )
                logger.warning(
                    "Worker 执行失败",
                    intent_id=work_item.intent_id,
                    error=str(exc),
                )
                continue
            if isinstance(result, WorkerTaskResult):
                _merge_worker_result(scheduler, result)
                scheduler.submit_work_result(result.intent_id, result.ok, result.payload)
            else:
                ok, payload = result
                scheduler.submit_work_result(work_item.intent_id, ok, payload)
