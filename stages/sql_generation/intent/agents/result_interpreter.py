from __future__ import annotations

from typing import Any, Dict, List

from config.app_config import get_app_config
from stages.sql_generation.intent.llm_utils import invoke_llm_with_format_retry
from stages.sql_generation.intent.middleware import extract_json_object, safe_json_dumps
from stages.sql_generation.intent.models import Interpretation
from stages.sql_generation.intent.prompts import RESULT_INTERPRETER_PROMPT


def _parse_interpretation(raw: str) -> Interpretation:
    obj = extract_json_object(raw)
    return Interpretation.model_validate(obj)


def run_result_interpreter(
    *,
    model: Any,
    intent_payload: Dict[str, Any],
    sql: str,
    exec_raw: Dict[str, Any],
    assumptions: List[str],
    max_retries: int | None = None,
) -> Interpretation:
    payload = {
        "intent": intent_payload,
        "sql": sql,
        "exec_raw": exec_raw,
        "assumptions": list(assumptions or []),
    }
    return invoke_llm_with_format_retry(
        model,
        RESULT_INTERPRETER_PROMPT,
        safe_json_dumps(payload),
        _parse_interpretation,
        max_retries=int(max_retries or get_app_config().stages.sql_generation.intent_runtime.agent_max_retries),
    )
