from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from utils.id_generator import new_event_id, new_trace_id

def utc_now_iso() -> str:
    import datetime

    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()


@dataclass
class AuditEvent:
    event_id: str
    timestamp: str
    event_type: str
    payload: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AuditTrace:
    trace_id: str
    events: List[AuditEvent] = field(default_factory=list)


class TraceRecorder:
    def __init__(self, trace_id: Optional[str] = None) -> None:
        self.trace_id = trace_id or new_trace_id()
        self._events: List[AuditEvent] = []

    def record(self, event_type: str, payload: Optional[Dict[str, Any]] = None) -> None:
        self._events.append(
            AuditEvent(
                event_id=new_event_id(),
                timestamp=utc_now_iso(),
                event_type=str(event_type),
                payload=dict(payload or {}),
            )
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "events": [
                {
                    "event_id": e.event_id,
                    "timestamp": e.timestamp,
                    "event_type": e.event_type,
                    "payload": e.payload,
                }
                for e in self._events
            ],
        }
