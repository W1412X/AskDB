"""
SQL generation DAG 调度数据模型。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from stages.sql_generation.intent.models import DialogResolutionType, IntentExecutionState


class NodeStatus(str, Enum):
    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    WAIT_USER = "wait_user"
    COMPLETED = "completed"
    FAILED = "failed"
    BLOCKED_BY_FAILED_DEP = "blocked_by_failed_dep"


class SchedulerEventType(str, Enum):
    INTENT_READY = "intent_ready"
    INTENT_COMPLETED = "intent_completed"
    INTENT_FAILED = "intent_failed"
    INTENT_WAIT_USER = "intent_wait_user"
    USER_REPLY_RECEIVED = "user_reply_received"
    NODE_BLOCKED = "node_blocked"


@dataclass
class SchedulerEvent:
    event_id: str
    event_type: SchedulerEventType
    intent_id: str
    created_at: float
    payload: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DialogTicketRecord:
    ticket_id: str
    intent_id: str
    question_id: str
    phase: str
    created_at: float
    payload: Dict[str, Any]
    thread_id: str
    turns: List[Dict[str, Any]] = field(default_factory=list)
    resolved: bool = False
    resolution_type: Optional[DialogResolutionType] = None


@dataclass
class DialogState:
    queue: List[str] = field(default_factory=list)
    tickets: Dict[str, DialogTicketRecord] = field(default_factory=dict)
    active_ticket_id: Optional[str] = None


@dataclass
class IntentNode:
    intent_id: str
    description: str
    deps: List[str]
    status: NodeStatus = NodeStatus.PENDING
    artifacts: Dict[str, Any] = field(
        default_factory=lambda: {
            "intent_meta": None,
            "schema": None,
            "ra_plan": None,
            "sql_candidates": [],
            "validations": [],
            "exec_result": None,
            "exec_raw": None,
            "user_hints": {},
            "facts_bundle": None,
            "checkpoint": {
                "intent_id": "",
                "phase": IntentExecutionState.INIT.value,
                "input_snapshot": {},
                "artifacts": {},
                "errors": [],
                "resume_token": "",
                "updated_at": 0.0,
            },
            "guard": {
                "state_fingerprint": "",
                "action_fingerprint": "",
                "no_progress_rounds": 0,
                "repeated_error_classes": {},
                "visited_phase_edges": [],
            },
            "final": None,
            "error": None,
        }
    )


@dataclass
class GlobalState:
    intent_map: Dict[str, IntentNode]
    ready_queue: List[str]
    running_set: Set[str]
    completed_set: Set[str]
    dependency_index: Dict[str, List[str]]
    remaining_deps_count: Dict[str, int]
    tool_registry: Dict[str, Any] = field(default_factory=dict)
    config: Dict[str, Any] = field(default_factory=dict)
    audit_log: List[Dict[str, Any]] = field(default_factory=list)
    dialog_state: DialogState = field(default_factory=DialogState)
    pending_events: List[SchedulerEvent] = field(default_factory=list)
    processed_events: List[SchedulerEvent] = field(default_factory=list)
    next_event_seq: int = 0

    def summary(self) -> Dict[str, Any]:
        status_counts: Dict[str, int] = {}
        failed_intents: List[Dict[str, str]] = []
        wait_user: List[str] = []
        for intent_id, node in (self.intent_map or {}).items():
            st_enum = getattr(node, "status", None)
            st = getattr(st_enum, "value", None) if st_enum is not None else None
            st = str(st) if st is not None else str(st_enum or "")
            status_counts[st] = status_counts.get(st, 0) + 1
            if st_enum == NodeStatus.FAILED:
                err = node.artifacts.get("error") if isinstance(node.artifacts, dict) else None
                failed_intents.append({"intent_id": intent_id, "error": str(err)[:200] if err else ""})
            if st_enum == NodeStatus.WAIT_USER:
                wait_user.append(intent_id)

        pending_tickets = [
            ticket_id
            for ticket_id, ticket in (self.dialog_state.tickets or {}).items()
            if ticket and not ticket.resolved
        ]
        return {
            "intent_count": len(self.intent_map or {}),
            "ready": len(self.ready_queue or []),
            "running": len(self.running_set or set()),
            "completed": len(self.completed_set or set()),
            "status_counts": status_counts,
            "failed_intents": failed_intents[:8],
            "wait_user": wait_user[:8],
            "audit_events": len(self.audit_log or []),
            "active_ticket_id": self.dialog_state.active_ticket_id or "",
            "pending_tickets": pending_tickets[:8],
            "pending_events": len(self.pending_events or []),
            "processed_events": len(self.processed_events or []),
        }
