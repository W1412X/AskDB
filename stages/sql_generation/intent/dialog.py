"""
Dialog APIs backed by GlobalState.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from config.app_config import get_app_config
from config.llm_config import get_llm
from stages.sql_generation.dag.models import GlobalState
from stages.sql_generation.intent.agents.clarifier import run_clarifier
from stages.sql_generation.intent.dialog_queue import get_dialog_repository
from stages.sql_generation.intent.models import DialogResolutionType


def _intent_payload(node: Any) -> Dict[str, Any]:
    meta = node.artifacts.get("intent_meta") or {}
    return {
        "intent_id": node.intent_id,
        "intent_description": node.description,
        "intent_meta": meta if isinstance(meta, dict) else {},
    }


def create_dialog_ticket(
    *,
    state: GlobalState,
    intent_id: str,
    question_id: str,
    phase: str,
    payload: Dict[str, Any],
    thread_id: str = "",
) -> Dict[str, Any]:
    repo = get_dialog_repository(state)
    ticket = repo.create_ticket(
        intent_id=intent_id,
        question_id=question_id,
        phase=phase,
        payload=payload,
        thread_id=thread_id or None,
    )
    return {
        "ticket_id": ticket.ticket_id,
        "thread_id": ticket.thread_id,
        "intent_id": ticket.intent_id,
        "question_id": ticket.question_id,
    }


def get_active_dialog_ticket(state: GlobalState) -> Optional[Dict[str, Any]]:
    repo = get_dialog_repository(state)
    ticket = repo.get_active_ticket()
    if not ticket:
        return None
    return {
        "ticket_id": ticket.ticket_id,
        "intent_id": ticket.intent_id,
        "question_id": ticket.question_id,
        "thread_id": ticket.thread_id,
        "created_at": ticket.created_at,
        "phase": ticket.phase,
        "payload": dict(ticket.payload),
        "turns": list(ticket.turns),
        "resolved": bool(ticket.resolved),
        "resolution_type": ticket.resolution_type.value if ticket.resolution_type else "",
    }


def submit_dialog_user_message(
    *,
    state: GlobalState,
    ticket_id: str,
    user_message: str,
    model_name: Optional[str] = None,
) -> Dict[str, Any]:
    repo = get_dialog_repository(state)
    ticket = repo.append_turn(ticket_id=ticket_id, user_message=user_message)
    node = state.intent_map.get(ticket.intent_id)
    if node is None:
        raise ValueError(f"unknown intent_id in ticket: {ticket.intent_id}")

    current_hints = node.artifacts.get("user_hints") or {}
    if not isinstance(current_hints, dict):
        current_hints = {}

    model = get_llm(str(model_name or get_app_config().stages.sql_generation.intent_runtime.model_name))
    ticket_payload = dict(ticket.payload)
    ticket_payload["turns"] = list(ticket.turns)

    out = run_clarifier(
        model=model,
        intent_payload=_intent_payload(node),
        ticket_payload=ticket_payload,
        current_hints=current_hints,
    )

    max_turns = int(ticket_payload.get("max_turns") or 0) or 3
    turn_count = len(ticket.turns)
    merged = dict(current_hints)
    if isinstance(out.hints, dict):
        merged.update(out.hints)
    node.artifacts["user_hints"] = merged

    checkpoint = node.artifacts.get("checkpoint") or {}
    resume_phase = str(ticket.payload.get("resume_phase") or checkpoint.get("phase") or "BUILDING_SCHEMA")

    if out.resolved:
        repo.mark_resolved(ticket_id, DialogResolutionType.RESOLVED)
        checkpoint["phase"] = resume_phase
        node.artifacts["checkpoint"] = checkpoint
        return {
            "ok": True,
            "resolved": True,
            "resolution_type": DialogResolutionType.RESOLVED.value,
            "intent_id": node.intent_id,
            "hints": merged,
            "summary": out.summary,
            "next_action": {"resume_phase": resume_phase},
        }

    if turn_count >= max_turns:
        repo.mark_resolved(ticket_id, DialogResolutionType.ASSUMPTIVE)
        checkpoint["phase"] = resume_phase
        assumptions = list(node.artifacts.get("assumptions") or [])
        assumptions.append(f"user clarification incomplete for ticket {ticket_id}; resumed with best-effort hints")
        node.artifacts["assumptions"] = assumptions
        node.artifacts["checkpoint"] = checkpoint
        return {
            "ok": True,
            "resolved": True,
            "resolution_type": DialogResolutionType.ASSUMPTIVE.value,
            "intent_id": node.intent_id,
            "hints": merged,
            "summary": f"{out.summary} (max_turns reached; resuming with assumptions)",
            "next_action": {"resume_phase": resume_phase},
        }

    next_ask = out.next_ask if isinstance(out.next_ask, dict) else None
    if next_ask:
        ticket.payload["ask"] = dict(next_ask)
    return {
        "ok": True,
        "resolved": False,
        "resolution_type": "",
        "intent_id": node.intent_id,
        "next_ask": next_ask or (ticket.payload.get("ask") or {}),
        "summary": out.summary,
        "next_action": {"resume_phase": "WAITING_USER"},
    }
