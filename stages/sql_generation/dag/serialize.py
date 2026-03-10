"""
GlobalState serialization utilities.
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict

from stages.sql_generation.dag.models import (
    DialogState,
    DialogTicketRecord,
    GlobalState,
    IntentNode,
    NodeStatus,
    SchedulerEvent,
    SchedulerEventType,
)
from stages.sql_generation.intent.models import DialogResolutionType


def intent_node_to_dict(node: IntentNode) -> Dict[str, Any]:
    return {
        "intent_id": node.intent_id,
        "description": node.description,
        "deps": list(node.deps or []),
        "status": node.status.value,
        "artifacts": dict(node.artifacts or {}),
    }


def intent_node_from_dict(data: Dict[str, Any]) -> IntentNode:
    return IntentNode(
        intent_id=str(data.get("intent_id") or ""),
        description=str(data.get("description") or ""),
        deps=list(data.get("deps") or []),
        status=NodeStatus(str(data.get("status") or NodeStatus.PENDING.value)),
        artifacts=dict(data.get("artifacts") or {}),
    )


def state_to_dict(state: GlobalState) -> Dict[str, Any]:
    data = asdict(state)
    for _intent_id, node in data.get("intent_map", {}).items():
        if isinstance(node, dict) and isinstance(node.get("status"), NodeStatus):
            node["status"] = node["status"].value
    for collection_name in ("pending_events", "processed_events"):
        events = data.get(collection_name) or []
        for event in events:
            event_type = event.get("event_type")
            if event_type is not None and hasattr(event_type, "value"):
                event["event_type"] = event_type.value
    dialog_state = data.get("dialog_state") or {}
    tickets = dialog_state.get("tickets") or {}
    for ticket_id, ticket in tickets.items():
        resolution_type = ticket.get("resolution_type")
        if resolution_type is not None and hasattr(resolution_type, "value"):
            ticket["resolution_type"] = resolution_type.value
    return data


def state_from_dict(data: Dict[str, Any]) -> GlobalState:
    intent_map_raw = data.get("intent_map") or {}
    intent_map: Dict[str, IntentNode] = {}
    for intent_id, node_raw in intent_map_raw.items():
        intent_map[intent_id] = intent_node_from_dict(node_raw)

    dialog_raw = data.get("dialog_state") or {}
    tickets_raw = dialog_raw.get("tickets") or {}
    tickets: Dict[str, DialogTicketRecord] = {}
    for ticket_id, ticket_raw in tickets_raw.items():
        tickets[ticket_id] = DialogTicketRecord(
            ticket_id=str(ticket_raw.get("ticket_id") or ticket_id),
            intent_id=str(ticket_raw.get("intent_id") or ""),
            question_id=str(ticket_raw.get("question_id") or ""),
            phase=str(ticket_raw.get("phase") or ""),
            created_at=float(ticket_raw.get("created_at") or 0.0),
            payload=dict(ticket_raw.get("payload") or {}),
            thread_id=str(ticket_raw.get("thread_id") or ""),
            turns=list(ticket_raw.get("turns") or []),
            resolved=bool(ticket_raw.get("resolved")),
            resolution_type=DialogResolutionType(str(ticket_raw.get("resolution_type")))
            if ticket_raw.get("resolution_type")
            else None,
        )

    def _load_events(items: Any) -> list[SchedulerEvent]:
        loaded: list[SchedulerEvent] = []
        for raw in list(items or []):
            loaded.append(
                SchedulerEvent(
                    event_id=str(raw.get("event_id") or ""),
                    event_type=SchedulerEventType(str(raw.get("event_type") or SchedulerEventType.INTENT_READY.value)),
                    intent_id=str(raw.get("intent_id") or ""),
                    created_at=float(raw.get("created_at") or 0.0),
                    payload=dict(raw.get("payload") or {}),
                )
            )
        return loaded

    return GlobalState(
        intent_map=intent_map,
        ready_queue=list(data.get("ready_queue") or []),
        running_set=set(data.get("running_set") or []),
        completed_set=set(data.get("completed_set") or []),
        dependency_index=dict(data.get("dependency_index") or {}),
        remaining_deps_count=dict(data.get("remaining_deps_count") or {}),
        tool_registry=dict(data.get("tool_registry") or {}),
        config=dict(data.get("config") or {}),
        audit_log=list(data.get("audit_log") or []),
        dialog_state=DialogState(
            queue=list(dialog_raw.get("queue") or []),
            tickets=tickets,
            active_ticket_id=dialog_raw.get("active_ticket_id"),
        ),
        pending_events=_load_events(data.get("pending_events") or []),
        processed_events=_load_events(data.get("processed_events") or []),
        next_event_seq=int(data.get("next_event_seq") or 0),
    )
