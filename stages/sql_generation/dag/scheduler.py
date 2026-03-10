"""
自研 DAG 调度器：构建拓扑、管理 ready/running/completed、处理失败阻塞。
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from config.app_config import get_app_config
from stages.sql_generation.dag.models import GlobalState, IntentNode, NodeStatus, SchedulerEvent, SchedulerEventType
from utils.id_generator import new_event_id

logger = logging.getLogger("sql_generation_dag")


@dataclass(frozen=True)
class SchedulerConfig:
    max_concurrency: int = get_app_config().stages.sql_generation.pipeline.max_concurrency


@dataclass(frozen=True)
class WorkItem:
    intent_id: str
    node: IntentNode
    lease_created_at: float


def _intent_to_payload(intent: Any) -> Tuple[str, str, List[str], dict]:
    """
    支持 dataclass intent 或 dict intent。
    需要字段: intent_id, intent_description, dependency_intent_ids。
    """
    if isinstance(intent, dict):
        intent_id = str(intent.get("intent_id", "")).strip()
        desc = str(intent.get("intent_description", "")).strip()
        deps_raw = intent.get("dependency_intent_ids", [])
    else:
        intent_id = str(getattr(intent, "intent_id", "")).strip()
        desc = str(getattr(intent, "intent_description", "")).strip()
        deps_raw = getattr(intent, "dependency_intent_ids", [])

    if not intent_id:
        raise ValueError("intent_id is required")
    if not isinstance(deps_raw, list):
        raise ValueError(f"dependency_intent_ids must be list, got {type(deps_raw)}")
    deps = [str(x).strip() for x in deps_raw if str(x).strip()]
    return intent_id, desc, deps, {}


def build_global_state(
    intents: Sequence[Any],
    *,
    config: Optional[SchedulerConfig] = None,
    tool_registry: Optional[Dict[str, Any]] = None,
) -> GlobalState:
    cfg = config or SchedulerConfig()
    tool_registry = tool_registry or {}

    intent_map: Dict[str, IntentNode] = {}
    dependency_index: Dict[str, List[str]] = {}
    remaining_deps_count: Dict[str, int] = {}

    # 1) build nodes
    for raw in intents:
        intent_id, desc, deps, meta = _intent_to_payload(raw)
        if intent_id in intent_map:
            raise ValueError(f"duplicate intent_id: {intent_id}")
        intent_map[intent_id] = IntentNode(intent_id=intent_id, description=desc, deps=deps)
        remaining_deps_count[intent_id] = len(deps)

    # 2) validate deps + build reverse edges
    for intent_id, node in intent_map.items():
        for dep in node.deps:
            if dep not in intent_map:
                raise ValueError(f"intent {intent_id} depends on unknown intent_id: {dep}")
            dependency_index.setdefault(dep, []).append(intent_id)

    # 3) cycle check (Kahn)
    _validate_acyclic(intent_map=intent_map, dependency_index=dependency_index, remaining=remaining_deps_count)

    # 4) init ready queue
    ready_queue: List[str] = []

    state = GlobalState(
        intent_map=intent_map,
        ready_queue=ready_queue,
        running_set=set(),
        completed_set=set(),
        dependency_index=dependency_index,
        remaining_deps_count=remaining_deps_count,
        tool_registry=tool_registry,
        config={"max_concurrency": cfg.max_concurrency},
        audit_log=[],
        pending_events=[],
        processed_events=[],
        next_event_seq=0,
    )
    initial_ready_count = 0
    for intent_id, cnt in remaining_deps_count.items():
        if cnt == 0:
            initial_ready_count += 1
            _append_event(
                state,
                SchedulerEventType.INTENT_READY,
                intent_id,
                {"reason": "dependencies_resolved"},
            )
    state.audit_log.append({"event": "build", "node_count": len(intent_map), "ready_count": initial_ready_count})
    return state


def _append_event(
    state: GlobalState,
    event_type: SchedulerEventType,
    intent_id: str,
    payload: Optional[Dict[str, Any]] = None,
) -> SchedulerEvent:
    event = SchedulerEvent(
        event_id=new_event_id(),
        event_type=event_type,
        intent_id=intent_id,
        created_at=time.time(),
        payload=dict(payload or {}),
    )
    state.next_event_seq += 1
    state.pending_events.append(event)
    state.audit_log.append(
        {
            "event": "scheduler_event_emitted",
            "event_type": event.event_type.value,
            "intent_id": intent_id,
            "payload": dict(payload or {}),
        }
    )
    return event


def _validate_acyclic(
    *,
    intent_map: Dict[str, IntentNode],
    dependency_index: Dict[str, List[str]],
    remaining: Dict[str, int],
) -> None:
    remaining_local = dict(remaining)
    queue = [intent_id for intent_id, cnt in remaining_local.items() if cnt == 0]
    visited = 0
    while queue:
        cur = queue.pop(0)
        visited += 1
        for child in dependency_index.get(cur, []):
            remaining_local[child] -= 1
            if remaining_local[child] == 0:
                queue.append(child)
    if visited != len(intent_map):
        raise ValueError("intent dependency graph has cycle")


class DAGScheduler:
    def __init__(
        self,
        intents: Sequence[Any],
        *,
        config: Optional[SchedulerConfig] = None,
        tool_registry: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.state = build_global_state(intents, config=config, tool_registry=tool_registry)

    @classmethod
    def from_state(cls, state: GlobalState) -> "DAGScheduler":
        obj = cls.__new__(cls)
        obj.state = state
        return obj

    def pop_ready(self, limit: Optional[int] = None) -> List[IntentNode]:
        if limit is None:
            max_concurrency = int(self.state.config.get("max_concurrency", 1))
            remain_slots = max_concurrency - len(self.state.running_set)
            limit = max(remain_slots, 0)
        if limit <= 0:
            return []

        popped: List[IntentNode] = []
        while self.state.ready_queue and len(popped) < limit:
            intent_id = self.state.ready_queue.pop(0)
            node = self.state.intent_map[intent_id]
            if node.status != NodeStatus.READY:
                continue
            node.status = NodeStatus.RUNNING
            self.state.running_set.add(intent_id)
            popped.append(node)

        if popped:
            self.state.audit_log.append({"event": "dispatch", "intent_ids": [x.intent_id for x in popped]})
        return popped

    def poll_work(self, limit: Optional[int] = None) -> List[WorkItem]:
        self.drain_events()
        batch = self.pop_ready(limit=limit)
        return [
            WorkItem(
                intent_id=node.intent_id,
                node=node,
                lease_created_at=time.time(),
            )
            for node in batch
        ]

    def submit_work_result(self, intent_id: str, ok: Any, payload: Any) -> None:
        if ok == "WAIT_USER":
            self.emit_wait_user(intent_id, payload if isinstance(payload, dict) else {"message": str(payload)})
        elif ok:
            self.emit_completed(intent_id, result=payload if isinstance(payload, dict) else {})
        else:
            self.emit_failed(intent_id, error=str(payload))
        self.drain_events()

    def emit_ready(self, intent_id: str, payload: Optional[Dict[str, Any]] = None) -> None:
        _append_event(self.state, SchedulerEventType.INTENT_READY, intent_id, payload)

    def emit_completed(self, intent_id: str, result: Optional[Dict[str, Any]] = None) -> None:
        _append_event(self.state, SchedulerEventType.INTENT_COMPLETED, intent_id, {"result": result or {}})

    def emit_failed(self, intent_id: str, error: str) -> None:
        _append_event(self.state, SchedulerEventType.INTENT_FAILED, intent_id, {"error": error})

    def emit_wait_user(self, intent_id: str, payload: Dict[str, Any]) -> None:
        _append_event(self.state, SchedulerEventType.INTENT_WAIT_USER, intent_id, {"payload": payload})

    def emit_user_reply_received(self, intent_id: str, payload: Optional[Dict[str, Any]] = None) -> None:
        _append_event(self.state, SchedulerEventType.USER_REPLY_RECEIVED, intent_id, payload)

    def drain_events(self) -> int:
        processed = 0
        while self.state.pending_events:
            event = self.state.pending_events.pop(0)
            self._apply_event(event)
            self.state.processed_events.append(event)
            processed += 1
        if processed:
            self.state.audit_log.append({"event": "drain_events", "count": processed})
        return processed

    def mark_completed(self, intent_id: str, result: Optional[Dict[str, Any]] = None) -> None:
        node = self._require_node(intent_id)
        if node.status not in (NodeStatus.RUNNING, NodeStatus.READY, NodeStatus.WAIT_USER):
            raise ValueError(f"intent {intent_id} cannot complete from status {node.status}")
        node.status = NodeStatus.COMPLETED
        node.artifacts["final"] = result

        self.state.running_set.discard(intent_id)
        self.state.completed_set.add(intent_id)

        for child_id in self.state.dependency_index.get(intent_id, []):
            child = self.state.intent_map[child_id]
            if child.status in (NodeStatus.COMPLETED, NodeStatus.FAILED, NodeStatus.BLOCKED_BY_FAILED_DEP):
                continue
            self.state.remaining_deps_count[child_id] -= 1
            if self.state.remaining_deps_count[child_id] == 0:
                child.status = NodeStatus.READY
                self.state.ready_queue.append(child_id)

        self.state.audit_log.append({"event": "complete", "intent_id": intent_id})

    def mark_failed(self, intent_id: str, error: str) -> None:
        node = self._require_node(intent_id)
        if node.status not in (NodeStatus.RUNNING, NodeStatus.READY, NodeStatus.WAIT_USER):
            raise ValueError(f"intent {intent_id} cannot fail from status {node.status}")
        node.status = NodeStatus.FAILED
        node.artifacts["error"] = error
        self.state.running_set.discard(intent_id)
        self.state.audit_log.append({"event": "failed", "intent_id": intent_id, "error": error})
        self._block_descendants(intent_id, reason=f"blocked_by_failed_dep:{intent_id}")

    def mark_wait_user(self, intent_id: str, payload: Dict[str, Any]) -> None:
        node = self._require_node(intent_id)
        if node.status not in (NodeStatus.RUNNING, NodeStatus.READY):
            raise ValueError(f"intent {intent_id} cannot wait_user from status {node.status}")
        node.status = NodeStatus.WAIT_USER
        node.artifacts["final"] = payload
        self.state.running_set.discard(intent_id)
        self.state.audit_log.append({"event": "wait_user", "intent_id": intent_id})

    def _apply_event(self, event: SchedulerEvent) -> None:
        node = self._require_node(event.intent_id)
        event_type = event.event_type
        if event_type == SchedulerEventType.INTENT_READY:
            if node.status in (NodeStatus.COMPLETED, NodeStatus.FAILED, NodeStatus.BLOCKED_BY_FAILED_DEP):
                return
            node.status = NodeStatus.READY
            if event.intent_id not in self.state.ready_queue:
                self.state.ready_queue.append(event.intent_id)
            self.state.audit_log.append({"event": "ready", "intent_id": event.intent_id, "payload": event.payload})
            return

        if event_type == SchedulerEventType.INTENT_COMPLETED:
            result = event.payload.get("result") if isinstance(event.payload, dict) else None
            self.mark_completed(event.intent_id, result=result if isinstance(result, dict) else None)
            return

        if event_type == SchedulerEventType.INTENT_FAILED:
            self.mark_failed(event.intent_id, error=str((event.payload or {}).get("error") or "intent failed"))
            return

        if event_type == SchedulerEventType.INTENT_WAIT_USER:
            payload = (event.payload or {}).get("payload")
            self.mark_wait_user(event.intent_id, payload if isinstance(payload, dict) else {})
            return

        if event_type == SchedulerEventType.USER_REPLY_RECEIVED:
            if node.status != NodeStatus.WAIT_USER:
                return
            node.status = NodeStatus.READY
            if event.intent_id not in self.state.ready_queue:
                self.state.ready_queue.append(event.intent_id)
            self.state.audit_log.append({"event": "user_reply_received", "intent_id": event.intent_id, "payload": event.payload})
            return

        if event_type == SchedulerEventType.NODE_BLOCKED:
            reason = str((event.payload or {}).get("reason") or "")
            if node.status in (NodeStatus.COMPLETED, NodeStatus.FAILED, NodeStatus.BLOCKED_BY_FAILED_DEP):
                return
            node.status = NodeStatus.BLOCKED_BY_FAILED_DEP
            node.artifacts["error"] = reason
            if event.intent_id in self.state.ready_queue:
                self.state.ready_queue = [x for x in self.state.ready_queue if x != event.intent_id]
            self.state.running_set.discard(event.intent_id)
            self.state.audit_log.append({"event": "blocked", "intent_id": event.intent_id, "reason": reason})
            return

        raise ValueError(f"unsupported scheduler event type: {event_type.value}")

    def _block_descendants(self, root_intent_id: str, reason: str) -> None:
        queue = list(self.state.dependency_index.get(root_intent_id, []))
        visited = set()
        while queue:
            cur = queue.pop(0)
            if cur in visited:
                continue
            visited.add(cur)
            node = self.state.intent_map[cur]
            if node.status in (NodeStatus.COMPLETED, NodeStatus.FAILED, NodeStatus.BLOCKED_BY_FAILED_DEP):
                continue
            _append_event(self.state, SchedulerEventType.NODE_BLOCKED, cur, {"reason": reason})
            queue.extend(self.state.dependency_index.get(cur, []))

    def has_ready(self) -> bool:
        return any(self.state.intent_map[x].status == NodeStatus.READY for x in self.state.ready_queue)

    def is_finished(self) -> bool:
        for node in self.state.intent_map.values():
            if node.status in (NodeStatus.PENDING, NodeStatus.READY, NodeStatus.RUNNING, NodeStatus.WAIT_USER):
                return False
        return True

    def summary(self) -> Dict[str, int]:
        stats = {x.value: 0 for x in NodeStatus}
        for node in self.state.intent_map.values():
            stats[node.status.value] += 1
        return stats

    def _require_node(self, intent_id: str) -> IntentNode:
        if intent_id not in self.state.intent_map:
            raise ValueError(f"unknown intent_id: {intent_id}")
        return self.state.intent_map[intent_id]
