"""
Simple audit trace for intent_divide.

Design goals:
- lightweight (dict payloads)
- deterministic (no LLM-dependent formatting)
- serializable (JSON-friendly)
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List

from utils.id_generator import new_event_id, new_trace_id


@dataclass
class TraceEvent:
    event_id: str
    ts: float
    type: str
    payload: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DivideAudit:
    trace_id: str = field(default_factory=new_trace_id)
    events: List[TraceEvent] = field(default_factory=list)

    def record(self, event_type: str, payload: Dict[str, Any] | None = None) -> None:
        self.events.append(
            TraceEvent(
                event_id=new_event_id(),
                ts=time.time(),
                type=str(event_type),
                payload=dict(payload or {}),
            )
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "events": [
                {"event_id": e.event_id, "ts": e.ts, "type": e.type, "payload": e.payload} for e in self.events
            ],
        }
