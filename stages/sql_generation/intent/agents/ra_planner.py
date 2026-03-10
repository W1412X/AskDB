from __future__ import annotations

from typing import Any, Dict, Optional

from config.app_config import get_app_config
from stages.sql_generation.intent.llm_utils import invoke_llm_with_format_retry
from stages.sql_generation.intent.middleware import extract_json_object, safe_json_dumps
from stages.sql_generation.intent.models import RAPlan
from stages.sql_generation.intent.prompts import RA_PLANNER_PROMPT


def _parse_ra_plan(raw: str) -> RAPlan:
    obj = extract_json_object(raw)
    return RAPlan.model_validate(obj)


def run_ra_planner(
    *,
    model: Any,
    intent_payload: Dict[str, Any],
    dependency_context: Dict[str, Any],
    schema: Dict[str, Any],
    context: Dict[str, Any],
    max_retries: Optional[int] = None,
) -> RAPlan:
    payload = {
        "intent": intent_payload,
        "dependency_context": dependency_context,
        "schema": schema,
        "context": context,
    }
    return invoke_llm_with_format_retry(
        model,
        RA_PLANNER_PROMPT,
        safe_json_dumps(payload),
        _parse_ra_plan,
        max_retries=int(max_retries or get_app_config().stages.sql_generation.intent_runtime.agent_max_retries),
    )
