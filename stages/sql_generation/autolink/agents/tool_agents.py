"""
Tool Executors：Retrieval/Meta 为单步执行器，Explorer 保留有限 LLM 规划。
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from config.app_config import get_app_config
from utils.logger import get_logger

from stages.sql_generation.autolink.logging_utils import log_step_input, log_step_output, schema_summary
from stages.sql_generation.autolink.llm_utils import invoke_llm_with_format_retry
from stages.sql_generation.autolink.models import (
    ResultMapping,
    ResultMappingOperation,
    Schema,
    SchemaFieldLevel,
    SchemaFieldTarget,
    SchemaWrite,
    SchemaWritePlan,
    SubTaskIntent,
    ToolAgentName,
    ToolAgentOutput,
    ToolAttempt,
    ValueSource,
    ValueSourceType,
    WriteOperation,
    WritePolicy,
    render_subtask_intent,
)
from stages.sql_generation.autolink.prompts import SCHEMA_EXPLORER_AGENT_PROMPT
from stages.sql_generation.autolink.registry import ToolRegistry
from stages.sql_generation.autolink.schema_merge import apply_schema_write_plan
from stages.sql_generation.autolink.tracing import result_digest, result_preview
from utils.id_generator import new_tool_call_id

logger = get_logger("autolink")
_AUTOLINK_CFG = get_app_config().stages.sql_generation.autolink

EXPLORER_ACTION_SUFFIX = """

重要：你的输出会被严格校验（必须是 1 个纯 JSON 对象；禁止多余字段；枚举值必须严格命中允许集合；字段类型必须正确）。
不合规会被打回并要求按错误提示重写。

只输出严格 JSON（字段名与枚举值必须完全一致，否则解析失败）：
{
  "action": "call_tool",
  "reason": "为什么这样做",
  "tool_name": "sql_explore",
  "tool_args": {"query": "SELECT ... LIMIT 100"},
  "result_mapping": {
    "target_database": "库名",
    "target_table": "表名",
    "mappings": [
      {
        "result_column": "查询结果列名",
        "target_column": "schema列名",
        "target_field": "sample_values",
        "operation": "append_unique",
        "reason": ""
      }
    ],
    "summary": ""
  },
  "summary": ""
}

当 action=finish 时，tool_name、tool_args 为空字符串/空对象，result_mapping 为 null。

格式约束（必须遵守，否则解析报错）：
- action 只能是 "call_tool" 或 "finish"（小写、下划线）。
- tool_name 只能是 "sql_explore" 或 "sql_draft" 或 ""。
- 当 action=call_tool 且 tool 为 sql_explore/sql_draft 时，result_mapping 必填；当 action=finish 时 result_mapping 为 null。
- result_mapping.mappings[].operation 只能是 "set"、"append_unique"、"merge" 之一（小写、下划线），禁止大写。
- 不要添加任何未列出的键。

规则：
1) 只能基于当前 request、task、schema 编写 SQL
2) 对 sql_explore/sql_draft 必须显式给出 result_mapping
3) 当前最多再做一步时，优先 finish，而不是继续盲试
"""


class ExplorerDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: str
    reason: str = ""
    tool_name: str = ""
    tool_args: Dict[str, Any] = Field(default_factory=dict)
    result_mapping: Optional[ResultMapping] = None
    summary: str = ""


def run_tool_agent(
    agent_name: ToolAgentName,
    task: SubTaskIntent,
    request: str,
    database_scope: List[str],
    registry: ToolRegistry,
    model: Optional[Any] = None,
    schema: Optional[Schema] = None,
    memory_context: Optional[Dict[str, Any]] = None,
) -> ToolAgentOutput:
    schema = schema or Schema()
    log_step_input(
        logger,
        f"{agent_name.value}.invoke",
        {
            "request": request,
            "task": task.model_dump(mode="json"),
            "database_scope": database_scope,
            "schema": schema.model_dump(mode="json"),
            "memory": memory_context or {},
        },
        agent_name=agent_name.value,
        schema_summary=schema_summary(schema),
    )
    if agent_name == ToolAgentName.SCHEMA_RETRIEVAL:
        out = _run_retrieval_executor(task, request, database_scope, registry)
    elif agent_name == ToolAgentName.SCHEMA_META:
        out = _run_meta_executor(task, request, database_scope, registry, schema)
    else:
        out = _run_explorer_executor(task, request, database_scope, registry, schema, model, memory_context or {})

    log_step_output(
        logger,
        f"{agent_name.value}.result",
        {
            "summary": out.summary,
            "schema_write_plan": out.schema_write_plan.model_dump(mode="json"),
            "result_mapping": out.result_mapping.model_dump(mode="json") if out.result_mapping else None,
            "errors": out.errors,
        },
        agent_name=agent_name.value,
        write_count=len(out.schema_write_plan.writes),
        tool_call_count=len(out.tool_calls),
    )
    return out


def _run_retrieval_executor(
    task: SubTaskIntent,
    request: str,
    database_scope: List[str],
    registry: ToolRegistry,
) -> ToolAgentOutput:
    tool = registry.get_tool("schema_retrieval")
    if tool is None:
        return ToolAgentOutput(ok=False, summary="schema_retrieval unavailable", errors=["schema_retrieval unavailable"])
    description = task.notes.strip() or request
    tool_args = _prepare_tool_args(
        tool_name="schema_retrieval",
        tool=tool,
        tool_args={
            "table": ",".join(task.target_tables[:5]),
            "column": ",".join(task.target_columns[:8]),
            "description": description[:500],
        },
        database_scope=database_scope,
    )
    raw_result, attempt = _invoke_tool("SchemaRetrievalAgent", "schema_retrieval", tool, tool_args)
    plan = _extract_schema_write_plan(raw_result)
    diagnostics = raw_result.get("diagnostics") if isinstance(raw_result, dict) else None
    return ToolAgentOutput(
        ok=attempt.ok,
        summary="retrieval executed",
        observations=[
            {
                "tool_name": "schema_retrieval",
                "ok": attempt.ok,
                "result_preview": attempt.result_preview,
                "diagnostics": diagnostics or {},
            }
        ],
        tool_calls=[attempt],
        errors=[attempt.error] if attempt.error else [],
        schema_write_plan=plan,
    )


def _run_meta_executor(
    task: SubTaskIntent,
    request: str,
    database_scope: List[str],
    registry: ToolRegistry,
    schema: Schema,
) -> ToolAgentOutput:
    tool = registry.get_tool("schema_meta")
    if tool is None:
        return ToolAgentOutput(ok=False, summary="schema_meta unavailable", errors=["schema_meta unavailable"])
    candidates = list(dict.fromkeys(task.target_tables or _schema_tables(schema)))
    if not task.target_tables and candidates:
        # Prefer tables explicitly mentioned in the request to reduce irrelevant metadata calls.
        req = (request or "").lower()
        scored = []
        for tb in candidates:
            name = str(tb or "")
            score = 1 if name and name.lower() in req else 0
            scored.append((score, name))
        scored.sort(key=lambda x: (-x[0], x[1]))
        candidates = [name for _score, name in scored]
    target_tables = candidates[:3]
    raw_result, attempt = _invoke_tool(
        "SchemaMetaAgent",
        "schema_meta",
        tool,
        _prepare_tool_args(
            tool_name="schema_meta",
            tool=tool,
            tool_args={"tables": target_tables, "include_keys": True},
            database_scope=database_scope,
        ),
    )
    combined_plan = _extract_schema_write_plan(raw_result)
    errors: List[str] = []
    if attempt.error:
        errors.append(attempt.error)
    return ToolAgentOutput(
        ok=bool(combined_plan.writes) or attempt.ok,
        summary="metadata executor finished",
        observations=[{"tool_name": "schema_meta", "target_tables": target_tables, "ok": bool(combined_plan.writes)}],
        tool_calls=[attempt],
        errors=errors,
        schema_write_plan=combined_plan,
    )


def _run_explorer_executor(
    task: SubTaskIntent,
    request: str,
    database_scope: List[str],
    registry: ToolRegistry,
    schema: Schema,
    model: Optional[Any],
    memory_context: Dict[str, Any],
) -> ToolAgentOutput:
    if model is None:
        return ToolAgentOutput(ok=False, summary="SchemaExplorerAgent skipped: no model", errors=["explorer requires model"])

    attempts: List[ToolAttempt] = []
    observations: List[Dict[str, Any]] = []
    errors: List[str] = []
    combined_write_plan = SchemaWritePlan()
    latest_mapping: Optional[ResultMapping] = None
    working_schema = schema
    final_summary = render_subtask_intent(task)[:200]

    for step in range(_AUTOLINK_CFG.max_explorer_steps):
        try:
            decision = _decide_explorer_action(
                model=model,
                request=request,
                task=task,
                schema=working_schema,
                database_scope=database_scope,
                attempts=attempts,
                observations=observations,
                errors=errors,
                memory_context=memory_context,
                remaining_steps=_AUTOLINK_CFG.max_explorer_steps - step,
            )
            log_step_output(
                logger,
                "SchemaExplorerAgent.decision",
                decision.model_dump(mode="json"),
                agent_name=ToolAgentName.SCHEMA_EXPLORER.value,
                step_index=step + 1,
            )
        except Exception as exc:
            errors.append(f"explorer planning failed: {exc}")
            break

        if decision.action == "finish":
            final_summary = decision.summary or final_summary
            break

        tool = registry.get_tool(decision.tool_name)
        if tool is None:
            errors.append(f"invalid explorer tool: {decision.tool_name}")
            continue
        tool_args = _prepare_tool_args(decision.tool_name, tool, decision.tool_args, database_scope)
        raw_result, attempt = _invoke_tool("SchemaExplorerAgent", decision.tool_name, tool, tool_args)
        attempts.append(attempt)
        if attempt.error:
            errors.append(attempt.error)
        observations.append({
            "step": step + 1,
            "tool_name": decision.tool_name,
            "reason": decision.reason,
            "ok": attempt.ok,
            "result_preview": attempt.result_preview,
        })
        latest_mapping = decision.result_mapping
        if decision.result_mapping is not None:
            plan = _build_write_plan_from_mapping(raw_result, decision.result_mapping, decision.tool_name)
            if plan.writes:
                combined_write_plan.writes.extend(plan.writes)
                working_schema = apply_schema_write_plan(working_schema, plan)

    return ToolAgentOutput(
        ok=bool(combined_write_plan.writes) or any(attempt.ok for attempt in attempts),
        summary=final_summary,
        observations=observations,
        tool_calls=attempts,
        errors=errors,
        schema_write_plan=combined_write_plan,
        result_mapping=latest_mapping,
    )


def _decide_explorer_action(
    *,
    model: Any,
    request: str,
    task: SubTaskIntent,
    schema: Schema,
    database_scope: List[str],
    attempts: List[ToolAttempt],
    observations: List[Dict[str, Any]],
    errors: List[str],
    memory_context: Dict[str, Any],
    remaining_steps: int,
) -> ExplorerDecision:
    payload = {
        "request": request,
        "task": task.model_dump(mode="json"),
        "database_scope": database_scope,
        "schema": schema.model_dump(mode="json"),
        "attempts": [attempt.model_dump(mode="json") for attempt in attempts],
        "observations": observations[-4:],
        "errors": errors[-4:],
        "memory": memory_context,
        "remaining_steps": remaining_steps,
    }
    return invoke_llm_with_format_retry(
        model,
        SCHEMA_EXPLORER_AGENT_PROMPT + EXPLORER_ACTION_SUFFIX,
        json.dumps(payload, ensure_ascii=False),
        _parse_explorer_decision,
    )


def _parse_explorer_decision(raw: str) -> ExplorerDecision:
    from stages.sql_generation.autolink.middleware import MiddlewareValidationError, _extract_json_from_text

    obj = _extract_json_from_text(raw)
    decision = ExplorerDecision.model_validate(obj)
    if decision.action not in {"call_tool", "finish"}:
        raise MiddlewareValidationError("explorer action must be call_tool or finish")
    if decision.action == "call_tool" and decision.tool_name not in {"sql_explore", "sql_draft"}:
        raise MiddlewareValidationError("explorer tool must be sql_explore or sql_draft")
    if decision.action == "call_tool" and decision.result_mapping is None:
        raise MiddlewareValidationError("result_mapping is required for explorer SQL tools")
    return decision


def _invoke_tool(agent_label: str, tool_name: str, tool: Any, tool_args: Dict[str, Any]) -> tuple[Any, ToolAttempt]:
    start = time.perf_counter()
    call_id = new_tool_call_id()
    log_step_input(
        logger,
        f"{agent_label}.tool_call",
        {"tool_name": tool_name, "tool_args": tool_args},
        agent_name=agent_label,
        tool_call_id=call_id,
    )
    try:
        raw_result = tool.invoke(tool_args)
        ok = _tool_result_ok(tool_name, raw_result)
        err = _tool_result_error(raw_result)
    except Exception as exc:
        raw_result = {"ok": False, "error": str(exc)}
        ok = False
        err = str(exc)
    duration_ms = int((time.perf_counter() - start) * 1000)
    attempt = ToolAttempt(
        tool_call_id=call_id,
        tool_name=tool_name,
        args=tool_args,
        duration_ms=duration_ms,
        ok=ok,
        result_digest=result_digest(raw_result),
        result_preview=result_preview(raw_result),
        error=err,
    )
    log_step_output(
        logger,
        f"{agent_label}.tool_result",
        raw_result,
        agent_name=agent_label,
        tool_call_id=call_id,
        tool_name=tool_name,
        ok=ok,
        duration_ms=duration_ms,
    )
    return raw_result, attempt


def _schema_tables(schema: Schema) -> List[str]:
    return [
        table_name
        for db in schema.databases.values()
        for table_name in db.tables.keys()
    ]


def _build_write_plan_from_mapping(result: Any, mapping: ResultMapping, source_name: str) -> SchemaWritePlan:
    rows: List[Dict[str, Any]] = []
    if isinstance(result, list):
        rows = result
    elif isinstance(result, dict) and isinstance(result.get("result"), list):
        rows = result.get("result") or []
    if not rows:
        return SchemaWritePlan()

    values_by_result_column: Dict[str, List[Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key, value in row.items():
            bucket = values_by_result_column.setdefault(key, [])
            normalized = value if value is None else str(value)
            if normalized not in bucket:
                bucket.append(normalized)

    writes: List[SchemaWrite] = []
    for item in mapping.mappings:
        values = values_by_result_column.get(item.result_column, [])
        if not values:
            continue
        writes.append(
            SchemaWrite(
                target=SchemaFieldTarget(
                    level=SchemaFieldLevel.COLUMN,
                    database=mapping.target_database,
                    table=mapping.target_table,
                    column=item.target_column,
                    field=item.target_field,
                ),
                operation=(
                    WriteOperation.APPEND_UNIQUE
                    if item.operation == ResultMappingOperation.APPEND_UNIQUE
                    else (WriteOperation.MERGE if item.operation == ResultMappingOperation.MERGE else WriteOperation.SET)
                ),
                value=values if item.operation == ResultMappingOperation.APPEND_UNIQUE else (values[0] if len(values) == 1 else values),
                value_source=ValueSource(
                    source_type=ValueSourceType.DB_SAMPLE if source_name == "sql_explore" else ValueSourceType.TOOL,
                    source_name=source_name,
                    confidence=1.0,
                ),
                # Runtime should not silently drop evidence writes in BUILD when schema is empty.
                # We materialize missing table/column skeletons with a placeholder type later if needed.
                write_policy=WritePolicy(allow_overwrite=False, require_target_exists=False),
                reason=item.reason or f"mapped from result column {item.result_column}",
            )
        )
    return SchemaWritePlan(writes=writes, summary=f"write plan from {source_name} result mapping")


def _extract_schema_write_plan(result: Any) -> SchemaWritePlan:
    if not isinstance(result, dict):
        return SchemaWritePlan()
    plan = result.get("schema_write_plan")
    if isinstance(plan, SchemaWritePlan):
        return plan
    if isinstance(plan, dict):
        try:
            return SchemaWritePlan.model_validate(plan)
        except Exception:
            return SchemaWritePlan()
    return SchemaWritePlan()


def _prepare_tool_args(
    tool_name: str,
    tool: Any,
    tool_args: Dict[str, Any],
    database_scope: List[str],
) -> Dict[str, Any]:
    accepted = set((getattr(tool, "args", {}) or {}).keys())
    sanitized = tool_args if not accepted else {k: v for k, v in tool_args.items() if k in accepted}
    if not database_scope:
        return sanitized
    primary_database = str(database_scope[0])
    if tool_name == "schema_retrieval":
        if "databases" in accepted and not sanitized.get("databases"):
            sanitized["databases"] = [str(db) for db in database_scope]
        if "schema_name" in accepted and not sanitized.get("schema_name"):
            sanitized["schema_name"] = primary_database
        return sanitized
    if tool_name == "schema_meta":
        if "schema_name" in accepted and not sanitized.get("schema_name"):
            sanitized["schema_name"] = primary_database
        return sanitized
    if tool_name in {"sql_explore", "sql_draft"}:
        if "database" in accepted and not sanitized.get("database"):
            sanitized["database"] = primary_database
        return sanitized
    return sanitized


def _tool_result_ok(tool_name: str, result: Any) -> bool:
    if isinstance(result, dict) and "ok" in result:
        return bool(result.get("ok"))
    if tool_name == "sql_explore":
        # legacy behavior: sql_explore used to return list directly
        return isinstance(result, list)
    if isinstance(result, dict):
        return bool(result.get("ok"))
    return bool(result)


def _tool_result_error(result: Any) -> str:
    if isinstance(result, dict):
        return str(result.get("error") or "")
    return ""
