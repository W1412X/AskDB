"""
RoundJudge：统一判断 stop / continue / redundant_items。
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from utils.logger import get_logger

from stages.sql_generation.autolink.completeness import check_build_invariants
from stages.sql_generation.autolink.logging_utils import log_step_input, log_step_output, schema_summary
from stages.sql_generation.autolink.middleware import parse_completeness_assessment_output
from stages.sql_generation.autolink.models import CompletenessAssessment, RequestType, Schema
from stages.sql_generation.autolink.prompts import ROUND_JUDGE_PROMPT

logger = get_logger("autolink")


def run_round_judge(
    *,
    mode: RequestType,
    request: str,
    schema: Schema,
    findings: List[Dict[str, Any]],
    recent_tool_results: List[Dict[str, Any]],
    sql_draft_success: bool = False,
    model: Optional[Any] = None,
    memory_context: Optional[Dict[str, Any]] = None,
) -> CompletenessAssessment:
    payload = {
        "request": request,
        "mode": mode.value,
        "schema": schema.model_dump(mode="json"),
        "findings": findings[-10:],
        "recent_tool_results": recent_tool_results[-5:],
        "sql_draft_success": bool(sql_draft_success),
        "memory": memory_context or {},
    }
    log_step_input(
        logger,
        "RoundJudge.invoke",
        payload,
        mode=mode.value,
        schema_summary=schema_summary(schema),
        finding_count=len(findings),
    )

    out = _deterministic_round_judge(mode=mode, request=request, schema=schema)
    if model is not None:
        try:
            from stages.sql_generation.autolink.llm_utils import invoke_llm_with_format_retry

            out = invoke_llm_with_format_retry(
                model,
                ROUND_JUDGE_PROMPT,
                json.dumps(payload, ensure_ascii=False),
                parse_completeness_assessment_output,
            )
        except Exception as exc:
            err_preview = (str(exc).split("\n")[0] or str(exc))[:200]
            logger.warning(
                "round_judge LLM 调用失败，改用确定性判断器",
                err_preview=err_preview,
                error=str(exc),
            )
    else:
        logger.info("round_judge：无模型，使用确定性判断器", mode=mode.value)

    log_step_output(
        logger,
        "RoundJudge.result",
        out.model_dump(mode="json"),
        should_stop=out.should_stop,
        missing_required_count=len(out.missing_required_fields),
        redundant_count=len(out.redundant_items),
    )
    return out


def _deterministic_round_judge(*, mode: RequestType, request: str, schema: Schema) -> CompletenessAssessment:
    if mode == RequestType.BUILD:
        ok, missing = check_build_invariants(schema, request, require_descriptions=False)
        if ok:
            return CompletenessAssessment(
                reason="deterministic judge: build invariants satisfied",
                should_stop=True,
                stop_reason="minimal_complete",
            )
        return CompletenessAssessment(
            reason="deterministic judge: build invariants not satisfied",
            should_stop=False,
            continue_reason="build invariants not satisfied",
            missing_required_fields=missing,
        )

    has_tables = bool(schema.databases)
    if not has_tables:
        return CompletenessAssessment(
            reason="deterministic judge: schema empty",
            should_stop=False,
            continue_reason="schema still empty",
            missing_required_fields=["schema has no tables"],
        )
    return CompletenessAssessment(
        reason="deterministic judge: schema exists",
        should_stop=True,
        stop_reason="minimal_complete",
    )
