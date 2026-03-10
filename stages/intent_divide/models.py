"""
意图分解数据模型。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List


class IntentDivideStatus(str, Enum):
    SUCCESS = "SUCCESS"
    WAIT_USER = "WAIT_USER"
    FAILED = "FAILED"


@dataclass
class Intent:
    intent_id: str
    intent_description: str
    dependency_intent_ids: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "intent_id": self.intent_id,
            "intent_description": self.intent_description,
            "dependency_intent_ids": list(self.dependency_intent_ids),
        }


@dataclass
class IntentDivideOutput:
    intents: List[Intent] = field(default_factory=list)
    status: IntentDivideStatus = IntentDivideStatus.SUCCESS
    dialog_ticket: Dict[str, Any] | None = None
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "intents": [x.to_dict() for x in self.intents],
            "status": self.status.value,
            "dialog_ticket": dict(self.dialog_ticket or {}) if self.dialog_ticket else None,
            "errors": list(self.errors),
        }
