from __future__ import annotations

from typing import Any, Dict

from config.app_config import get_app_config
from stages.sql_generation.intent.llm_utils import invoke_llm_with_format_retry
from stages.sql_generation.intent.middleware import extract_json_object, safe_json_dumps
from stages.sql_generation.intent.models import SQLRenderOutput
from stages.sql_generation.intent.prompts import SQL_RENDERER_PROMPT


def _parse_sql_render(raw: str) -> SQLRenderOutput:
    obj = extract_json_object(raw)
    return SQLRenderOutput.model_validate(obj)


def run_sql_renderer(
    *,
    model: Any,
    intent_payload: Dict[str, Any],
    ra_plan: Dict[str, Any],
    schema: Dict[str, Any],
    context: Dict[str, Any],
    max_retries: int | None = None,
) -> SQLRenderOutput:
    payload = {
        "intent": intent_payload,
        "ra_plan": ra_plan,
        "schema": schema,
        "context": context,
    }
    return invoke_llm_with_format_retry(
        model,
        SQL_RENDERER_PROMPT,
        safe_json_dumps(payload),
        _parse_sql_render,
        max_retries=int(max_retries or get_app_config().stages.sql_generation.intent_runtime.agent_max_retries),
    )
