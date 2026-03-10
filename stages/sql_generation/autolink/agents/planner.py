"""
SchemaPlanner：统一处理 BUILD / ENRICH / ERROR 三种模式的规划。
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from utils.logger import get_logger

from stages.sql_generation.autolink.completeness import (
    has_any_keys,
    has_any_sample_values,
    has_any_strong_column_type,
    has_any_tables,
    request_needs_data_exploration,
    request_needs_samples,
)
from stages.sql_generation.autolink.logging_utils import log_step_input, log_step_output, schema_summary
from stages.sql_generation.autolink.middleware import parse_requirement_plan_output
from stages.sql_generation.autolink.models import (
    AutolinkContext,
    RequirementPlan,
    RequestType,
    RequirementConstraints,
    Schema,
)
from stages.sql_generation.autolink.prompts import SCHEMA_PLANNER_PROMPT

logger = get_logger("autolink")


def run_schema_planner(
    *,
    mode: RequestType,
    request: str,
    schema: Schema,
    context: AutolinkContext,
    recent_tool_results: Optional[List[Dict[str, Any]]] = None,
    model: Optional[Any] = None,
    round_index: int = 0,
    latest_judge: Optional[Dict[str, Any]] = None,
    memory_context: Optional[Dict[str, Any]] = None,
    step_logs: Optional[List[Dict[str, Any]]] = None,
    error_context: str = "",
) -> RequirementPlan:
    recent_tool_results = recent_tool_results or []
    payload = {
        "mode": mode.value,
        "request": request,
        "schema": schema.model_dump(mode="json"),
        "context": context.model_dump(mode="json"),
        "recent_tool_results": recent_tool_results,
        "latest_judge": latest_judge or {},
        "memory": memory_context or {},
        "step_logs": (step_logs or [])[-8:],
        "round": round_index,
        "error_context": error_context,
    }
    log_step_input(
        logger,
        "SchemaPlanner.invoke",
        payload,
        round_index=round_index,
        mode=mode.value,
        schema_summary=schema_summary(schema),
    )

    out = _deterministic_planner_output(payload)
    if model is not None:
        try:
            from stages.sql_generation.autolink.llm_utils import invoke_llm_with_format_retry

            out = invoke_llm_with_format_retry(
                model,
                SCHEMA_PLANNER_PROMPT,
                json.dumps(payload, ensure_ascii=False),
                parse_requirement_plan_output,
            )
        except Exception as exc:
            err_preview = (str(exc).split("\n")[0] or str(exc))[:200]
            logger.warning(
                "schema_planner LLM 调用失败，改用确定性规划器",
                err_preview=err_preview,
                error=str(exc),
                mode=mode.value,
            )
    else:
        logger.info("schema_planner：无模型，使用确定性规划器", mode=mode.value)

    log_step_output(
        logger,
        "SchemaPlanner.result",
        out.model_dump(mode="json"),
        round_index=round_index,
        mode=mode.value,
        write_count=len(out.schema_write_plan.writes),
        sub_task_count=len(out.sub_tasks),
    )
    return out


def _deterministic_planner_output(payload: Dict[str, Any]) -> RequirementPlan:
    schema = Schema.model_validate(payload.get("schema") or {})
    mode = RequestType(str(payload.get("mode") or RequestType.BUILD.value))
    request = str(payload.get("request") or "")

    findings: List[Dict[str, Any]] = []
    sub_tasks: List[Dict[str, Any]] = []
    focus_flags: List[str] = []

    if not has_any_tables(schema):
        findings.append({"summary": "schema is empty; discovery is required"})
        focus_flags.append("discover_schema")
        sub_tasks.append({
            "tool_agent_name": "SchemaRetrievalAgent",
            "task": {
                "goal": "retrieve_relevant_schema",
                "target_tables": [],
                "target_columns": [],
                "success_criteria": ["return request-relevant schema write plan"],
                "notes": request[:200],
            },
            "expected_output": "schema_write_plan",
        })

    if has_any_tables(schema) and (not has_any_strong_column_type(schema) or not has_any_keys(schema)):
        findings.append({"summary": "schema exists but lacks strong types or keys; metadata hydration is required"})
        focus_flags.append("hydrate_metadata")
        sub_tasks.append({
            "tool_agent_name": "SchemaMetaAgent",
            "task": {
                "goal": "fetch_table_metadata",
                "target_tables": [
                    table_name
                    for db in schema.databases.values()
                    for table_name in list(db.tables.keys())[:3]
                ][:3],
                "target_columns": [],
                "success_criteria": ["fill column types", "fill key metadata when available"],
                "notes": mode.value,
            },
            "expected_output": "schema_write_plan",
        })

    if (request_needs_samples(request) or request_needs_data_exploration(request)) and not has_any_sample_values(schema):
        findings.append({"summary": "request asks for data examples/patterns; exploration evidence is required"})
        focus_flags.append("explore_samples")
        sub_tasks.append({
            "tool_agent_name": "SchemaExplorerAgent",
            "task": {
                "goal": "collect_sample_values",
                "target_tables": [
                    table_name
                    for db in schema.databases.values()
                    for table_name in list(db.tables.keys())[:2]
                ][:2],
                "target_columns": [],
                "success_criteria": ["collect request-relevant sample values"],
                "notes": request[:200],
            },
            "expected_output": "result_mapping",
        })

    if not findings:
        findings.append({"summary": "schema already satisfies deterministic planner requirements"})
        focus_flags.append("assess_completeness")

    primary_goal = {
        RequestType.BUILD: "build_minimal_schema",
        RequestType.ENRICH: "enrich_existing_schema",
        RequestType.ERROR: "repair_schema_error",
    }[mode]
    return RequirementPlan.model_validate(
        {
            "ok": True,
            "summary": "deterministic planner output",
            "requirement_focus": {
                "primary_goal": primary_goal,
                "focus_flags": focus_flags,
                "target_entities": [],
                "constraints": RequirementConstraints().model_dump(mode="json"),
                "reason": "derived from schema completeness and request evidence requirements",
            },
            "field_requirement_profile": {"requirements": [], "summary": "runtime-enforced field requirements"},
            "schema_write_plan": {"writes": [], "summary": "planner does not mutate schema directly"},
            "findings": findings,
            "sub_tasks": sub_tasks,
        }
    )
