"""
全局 trace 记录器。
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from utils.id_generator import new_event_id
from stages.sql_generation.autolink.models import AuditEvent, AuditTrace, EventType

DEFAULT_PREVIEW_MAX_LEN = 800


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def result_digest(value: Any) -> str:
    text = safe_json_dumps(value)
    return f"sha256:{hashlib.sha256(text.encode('utf-8')).hexdigest()}"


def result_preview(value: Any, max_len: int = DEFAULT_PREVIEW_MAX_LEN) -> str:
    text = safe_json_dumps(value)
    if len(text) <= max_len:
        return text
    return f"{text[:max_len]}...(truncated)"


class TraceRecorder:
    def __init__(
        self,
        *,
        request_id: str,
        plan_id: str,
        trace_id: str,
    ) -> None:
        self.request_id = request_id
        self.plan_id = plan_id
        self.trace_id = trace_id
        self._events: List[AuditEvent] = []

    def record(
        self,
        event_type: EventType,
        payload: Optional[Dict[str, Any]] = None,
        *,
        step_id: str = "",
    ) -> AuditEvent:
        event = AuditEvent(
            event_id=new_event_id(),
            request_id=self.request_id,
            plan_id=self.plan_id,
            step_id=step_id,
            timestamp=utc_now_iso(),
            event_type=event_type,
            payload=payload or {},
        )
        self._events.append(event)
        return event

    def extend(self, events: List[AuditEvent]) -> None:
        self._events.extend(events)

    @property
    def events(self) -> List[AuditEvent]:
        return list(self._events)

    def to_trace(self) -> AuditTrace:
        return AuditTrace(trace_id=self.trace_id, events=self.events)
