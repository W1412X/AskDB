from __future__ import annotations

from typing import Any, Dict

from config.app_config import get_app_config
from stages.sql_generation.intent.llm_utils import invoke_llm_with_format_retry
from stages.sql_generation.intent.middleware import extract_json_object, safe_json_dumps
from stages.sql_generation.intent.models import ClarificationOutput
from stages.sql_generation.intent.prompts import CLARIFICATION_AGENT_PROMPT


def _parse_clarification(raw: str) -> ClarificationOutput:
    obj = extract_json_object(raw)
    return ClarificationOutput.model_validate(obj)


def run_clarifier(
    *,
    model: Any,
    intent_payload: Dict[str, Any],
    ticket_payload: Dict[str, Any],
    current_hints: Dict[str, Any],
    max_retries: int | None = None,
) -> ClarificationOutput:
    payload = {
        "intent": intent_payload,
        "ticket": ticket_payload,
        "current_hints": current_hints,
    }
    return invoke_llm_with_format_retry(
        model,
        CLARIFICATION_AGENT_PROMPT,
        safe_json_dumps(payload),
        _parse_clarification,
        max_retries=int(max_retries or get_app_config().stages.sql_generation.intent_runtime.agent_max_retries),
    )
