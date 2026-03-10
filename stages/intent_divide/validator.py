"""
意图输出校验器。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from stages.intent_divide.models import Intent


@dataclass
class ValidationResult:
    is_valid: bool
    error_message: Optional[str] = None


class IntentOutputValidator:
    def __init__(self) -> None:
        pass

    def validate(self, intents: List[Intent]) -> ValidationResult:
        if not intents:
            return ValidationResult(False, "intents is empty")

        seen_ids = set()
        for idx, intent in enumerate(intents, start=1):
            if not intent.intent_id:
                return ValidationResult(False, f"intent[{idx}] missing intent_id")
            if intent.intent_id in seen_ids:
                return ValidationResult(False, f"duplicated intent_id: {intent.intent_id}")
            seen_ids.add(intent.intent_id)

            if not intent.intent_description.strip():
                return ValidationResult(False, f"intent[{idx}] empty intent_description")

            if not isinstance(intent.dependency_intent_ids, list):
                return ValidationResult(False, f"intent[{idx}] dependency_intent_ids must be list")

        all_ids = {x.intent_id for x in intents}
        for intent in intents:
            for dep in intent.dependency_intent_ids:
                if dep not in all_ids:
                    return ValidationResult(False, f"intent {intent.intent_id} depends on unknown intent_id: {dep}")
                if dep == intent.intent_id:
                    return ValidationResult(False, f"intent {intent.intent_id} cannot depend on itself")

        return ValidationResult(True, None)
