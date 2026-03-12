"""
Per-intent SQL generation runtime.
"""

from __future__ import annotations

import hashlib
import json
import time
from typing import Any, Dict, List, Optional, Tuple

from config.app_config import get_app_config
from config.llm_config import get_llm
from stages.sql_generation.dag.deps import build_dependency_payload
from stages.sql_generation.dag.models import GlobalState, IntentNode, NodeStatus
from stages.sql_generation.intent.agents.ra_planner import run_ra_planner
from stages.sql_generation.intent.agents.result_interpreter import run_result_interpreter
from stages.sql_generation.intent.agents.sql_renderer import run_sql_renderer
from stages.sql_generation.intent.agents.sql_validator import validate_candidates
from stages.sql_generation.intent.dialog import create_dialog_ticket
from stages.sql_generation.intent.middleware import safe_json_dumps
from stages.sql_generation.intent.models import (
    ErrorHint,
    Interpretation,
    IntentCheckpoint,
    IntentError,
    IntentExecutionState,
    IntentFactsBundle,
    IntentFinalBundle,
    IntentRunStatus,
    ResultMetric,
    StepResult,
    StepStatus,
    ValidatedConstraint,
)
from stages.sql_generation.intent.intent_templates import build_template_guidance
from stages.sql_generation.intent.tools import autolink_tool
from stages.sql_generation.intent.tracing import TraceRecorder
from stages.sql_generation.autolink.logging_utils import log_step_input, log_step_output
from stages.sql_generation.tools.db import execute_select_with_limit_tool
from utils.logger import get_logger

logger = get_logger("intent_runtime")
_PIPELINE_CFG = get_app_config().stages.sql_generation.pipeline
_RUNTIME_CFG = get_app_config().stages.sql_generation.intent_runtime

_PHASE_NAME_CN = {
    IntentExecutionState.INIT: "初始化",
    IntentExecutionState.BUILDING_SCHEMA: "构建 Schema（Autolink）",
    IntentExecutionState.PLANNING_RA: "关系代数规划",
    IntentExecutionState.RENDERING_SQL: "关系代数转 SQL",
    IntentExecutionState.VALIDATING_SQL: "SQL 验证",
    IntentExecutionState.EXECUTING_SQL: "执行 SQL",
    IntentExecutionState.INTERPRETING_RESULT: "结果解释",
    IntentExecutionState.WAITING_USER: "等待用户",
    IntentExecutionState.COMPLETED: "已完成",
    IntentExecutionState.FAILED: "失败",
    IntentExecutionState.PARTIAL: "部分完成",
}


def _phase_name_cn(phase: IntentExecutionState) -> str:
    return _PHASE_NAME_CN.get(phase, phase.value)


def _schema_tables_summary(schema: Dict[str, Any]) -> str:
    """返回 schema 中表列表的简要描述，如：db1.t1, db1.t2"""
    dbs = schema.get("databases") or {}
    parts = []
    for db_name, db_obj in dbs.items():
        tables = (db_obj if isinstance(db_obj, dict) else {}).get("tables") or {}
        for t_name in tables:
            parts.append(f"{db_name}.{t_name}")
    return ", ".join(parts) if parts else "（无）"


def _ra_plan_summary(ra_plan: Dict[str, Any]) -> str:
    """关系代数计划简要描述"""
    entities = ra_plan.get("entities") or []
    joins = ra_plan.get("joins") or []
    filters = ra_plan.get("filters") or []
    return f"entities={len(entities)} joins={len(joins)} filters={len(filters)}"


def _intent_payload(node: IntentNode) -> Dict[str, Any]:
    meta = node.artifacts.get("intent_meta") or {}
    return {
        "intent_id": node.intent_id,
        "intent_description": node.description,
        "intent_meta": meta if isinstance(meta, dict) else {},
    }


def _require_deps_completed(node: IntentNode, state: GlobalState) -> Optional[str]:
    for dep_id in node.deps or []:
        dep = state.intent_map.get(dep_id)
        if dep is None:
            return f"unknown dependency intent_id: {dep_id}"
        if dep.status != NodeStatus.COMPLETED:
            return f"dependency not ready: {dep_id} status={dep.status.value}"
    return None


def _artifact_jsonable(value: Any) -> Any:
    return json.loads(safe_json_dumps(value))


def _checkpoint(node: IntentNode) -> IntentCheckpoint:
    raw = node.artifacts.get("checkpoint") or {}
    if raw.get("intent_id") in ("", None):
        raw["intent_id"] = node.intent_id
    return IntentCheckpoint.model_validate(raw)


def _save_checkpoint(node: IntentNode, checkpoint: IntentCheckpoint) -> None:
    checkpoint.updated_at = time.time()
    node.artifacts["checkpoint"] = checkpoint.model_dump(mode="json")


def _guard(node: IntentNode) -> Dict[str, Any]:
    raw = node.artifacts.get("guard") or {}
    if not isinstance(raw, dict):
        raw = {}
    raw.setdefault("state_fingerprint", "")
    raw.setdefault("action_fingerprint", "")
    raw.setdefault("no_progress_rounds", 0)
    raw.setdefault("repeated_error_classes", {})
    raw.setdefault("visited_phase_edges", [])
    node.artifacts["guard"] = raw
    return raw


def _state_fingerprint(node: IntentNode, checkpoint: IntentCheckpoint) -> str:
    payload = {
        "phase": checkpoint.phase.value,
        "schema": node.artifacts.get("schema") or {},
        "ra_plan": node.artifacts.get("ra_plan") or {},
        "validations": node.artifacts.get("validations") or {},
        "user_hints": node.artifacts.get("user_hints") or {},
    }
    return hashlib.sha256(safe_json_dumps(payload).encode("utf-8")).hexdigest()


def _apply_step_artifacts(node: IntentNode, step: StepResult) -> None:
    for key, value in (step.artifacts or {}).items():
        node.artifacts[key] = _artifact_jsonable(value)


def _record_wait_user(
    *,
    node: IntentNode,
    state: GlobalState,
    checkpoint: IntentCheckpoint,
    trace: TraceRecorder,
    question_id: str,
    situation: str,
    request: str,
    why_needed: str,
    examples: Optional[List[str]] = None,
) -> Tuple[Any, Any]:
    payload = {
        "intent_id": node.intent_id,
        "question_id": question_id,
        "priority": 1,
        "state_summary": node.description[:120],
        "ask": {
            "situation": situation,
            "request": request,
            "why_needed": why_needed,
            "examples": list(examples or []),
            "constraints": ["请尽量给出表名与关键字段名", "若有口径（如按 tenant 维度唯一），请明确说明"],
        },
        "acceptance_criteria": ["给出至少一个可操作线索（表名/字段名/口径/时间范围）"],
        "max_turns": 3,
        "resume_phase": checkpoint.phase.value,
    }
    ticket = create_dialog_ticket(
        state=state,
        intent_id=node.intent_id,
        question_id=question_id,
        phase=checkpoint.phase.value,
        payload=payload,
    )
    checkpoint.phase = IntentExecutionState.WAITING_USER
    _save_checkpoint(node, checkpoint)
    final = IntentFinalBundle(
        status=IntentRunStatus.WAIT_USER,
        interpretation=None,
        final_sql="",
        final_sql_fingerprint="",
        schema=node.artifacts.get("schema") or {"databases": {}},
        exec_raw=node.artifacts.get("exec_raw") or {},
        facts_bundle=IntentFactsBundle(),
        assumptions=list(node.artifacts.get("assumptions") or []),
        errors=[IntentError(type="WAIT_USER", message="need user clarification", hint=ErrorHint.ASK_USER)],
    )
    node.artifacts["final"] = final.model_dump(mode="json", by_alias=True)
    trace.record("ASK_USER_ENQUEUED", {"ticket_id": ticket.get("ticket_id"), "question_id": question_id})
    return "WAIT_USER", {"ticket": ticket, "reason": question_id, "audit": trace.to_dict()}


def _record_failed(node: IntentNode, message: str, errors: Optional[List[IntentError]] = None) -> None:
    final = IntentFinalBundle(
        status=IntentRunStatus.FAILED,
        interpretation=None,
        final_sql="",
        final_sql_fingerprint="",
        schema=node.artifacts.get("schema") or {"databases": {}},
        exec_raw=node.artifacts.get("exec_raw") or {},
        facts_bundle=IntentFactsBundle(),
        assumptions=list(node.artifacts.get("assumptions") or []),
        errors=list(errors or [IntentError(type="INTENT_FAILED", message=message)]),
    )
    node.artifacts["final"] = final.model_dump(mode="json", by_alias=True)
    node.artifacts["error"] = message


def _build_facts_bundle(node: IntentNode, rows: List[Dict[str, Any]], chosen_sql: str) -> IntentFactsBundle:
    ra_plan = node.artifacts.get("ra_plan") or {}
    entities = list(ra_plan.get("entities") or []) if isinstance(ra_plan, dict) else []
    checks = list(ra_plan.get("checks") or []) if isinstance(ra_plan, dict) else []
    used_tables = [f"{item.get('database', '')}.{item.get('table', '')}".strip(".") for item in entities if isinstance(item, dict)]
    used_columns: List[str] = []
    for item in entities:
        if isinstance(item, dict):
            table_name = str(item.get("table") or "")
            used_columns.extend([f"{table_name}.{col}" for col in list(item.get("columns") or [])])
    constraints = [
        ValidatedConstraint(name=str(check.get("name") or f"check_{idx}"), status="PLANNED", detail=str(check.get("reason") or ""))
        for idx, check in enumerate(checks, start=1)
        if isinstance(check, dict)
    ]
    metrics = [ResultMetric(name="row_count", value=len(rows))]
    return IntentFactsBundle(
        entity_keys=sorted(set(used_columns))[:32],
        validated_constraints=constraints,
        result_metrics=metrics,
        derived_filters=[str(item.get("expr") or "") for item in list(ra_plan.get("filters") or []) if isinstance(item, dict)],
        used_tables=sorted(set(filter(None, used_tables))),
        used_columns=sorted(set(filter(None, used_columns))),
        assumptions=list(node.artifacts.get("assumptions") or []) + [f"sql_fingerprint:{hashlib.sha256(chosen_sql.encode('utf-8')).hexdigest()[:12]}"],
    )


def _update_guard(node: IntentNode, checkpoint: IntentCheckpoint, step: StepResult) -> Optional[str]:
    guard = _guard(node)
    state_fp = _state_fingerprint(node, checkpoint)
    action_fp = hashlib.sha256(
        safe_json_dumps(
            {
                "phase": checkpoint.phase.value,
                "next_phase": step.next_phase.value,
                "delta": step.state_delta,
                "error_class": step.error_class,
                "evidence": step.new_evidence,
            }
        ).encode("utf-8")
    ).hexdigest()
    prev_state_fp = str(guard.get("state_fingerprint") or "")
    if prev_state_fp == state_fp:
        guard["no_progress_rounds"] = int(guard.get("no_progress_rounds") or 0) + 1
    else:
        guard["no_progress_rounds"] = 0
    repeated = dict(guard.get("repeated_error_classes") or {})
    if step.error_class:
        repeated[step.error_class] = int(repeated.get(step.error_class) or 0) + 1
    guard["repeated_error_classes"] = repeated
    edges = list(guard.get("visited_phase_edges") or [])
    edges.append(f"{checkpoint.phase.value}->{step.next_phase.value}")
    guard["visited_phase_edges"] = edges[-20:]
    guard["state_fingerprint"] = state_fp
    guard["action_fingerprint"] = action_fp
    node.artifacts["guard"] = guard

    if int(guard.get("no_progress_rounds") or 0) >= _RUNTIME_CFG.max_no_progress_rounds:
        return "no_progress"
    if step.error_class and int(repeated.get(step.error_class) or 0) > _RUNTIME_CFG.max_repeated_error_class:
        return f"repeated_error:{step.error_class}"
    return None


def _call_autolink(
    *,
    schema: Any,
    database_scope: List[str],
    context: Dict[str, Any],
    user_hints: Dict[str, Any],
    model_name: str,
    request_type: str,
    req_text: str,
) -> Dict[str, Any]:
    return autolink_tool.invoke(
        {
            "request": req_text,
            "request_type": request_type,
            "schema_data": schema,
            "context": {
                "database_scope": database_scope,
                "sql_dialect": context.get("sql_dialect", "MYSQL"),
                "hints": user_hints,
                "model_name": model_name,
                "max_meta_tables": int(context.get("max_meta_tables", get_app_config().stages.sql_generation.autolink.max_meta_tables)),
            },
        }
    )


def _step_build_schema(
    *,
    node: IntentNode,
    checkpoint: IntentCheckpoint,
    database_scope: List[str],
    context: Dict[str, Any],
    model_name: str,
    user_hints: Dict[str, Any],
) -> StepResult:
    out = _call_autolink(
        schema=node.artifacts.get("schema"),
        database_scope=database_scope,
        context=context,
        user_hints=user_hints,
        model_name=model_name,
        request_type="BUILD",
        req_text=f"为该意图构建最小可用 schema（只包含回答意图所需的最少表/列/键）：{node.description}",
    )
    schema = out.get("schema") or {"databases": {}}
    if not bool((schema or {}).get("databases")):
        return StepResult(
            status=StepStatus.WAIT_USER,
            next_phase=IntentExecutionState.BUILDING_SCHEMA,
            artifacts={"schema": schema},
            errors=[IntentError(type="AUTOLINK_EMPTY_SCHEMA", message="autolink returned empty schema", hint=ErrorHint.ASK_USER)],
            error_class="empty_schema",
            state_delta={"schema_tables": 0},
        )
    return StepResult(
        status=StepStatus.ADVANCE,
        next_phase=IntentExecutionState.PLANNING_RA,
        artifacts={"schema": schema, "autolink_trace_id": (out.get("audit") or {}).get("trace_id", "")},
        new_evidence=["schema_built"],
        state_delta={"schema_tables": sum(len(db.get("tables") or {}) for db in (schema.get("databases") or {}).values())},
    )


def _step_plan_ra(
    *,
    node: IntentNode,
    dependency_payload: Dict[str, Any],
    schema: Dict[str, Any],
    context: Dict[str, Any],
    model: Any,
) -> StepResult:
    ra_input = {
        "intent": _intent_payload(node),
        "dependency_context": dependency_payload,
        "schema": schema,
        "context": context,
    }
    log_step_input(logger, "RA_planner", ra_input, intent_id=node.intent_id)
    ra = run_ra_planner(
        model=model,
        intent_payload=_intent_payload(node),
        dependency_context=dependency_payload,
        schema=schema,
        context=context,
    )
    log_step_output(logger, "RA_planner", ra.model_dump(mode="json"), intent_id=node.intent_id)
    if not ra.ok:
        return StepResult(
            status=StepStatus.ADVANCE,
            next_phase=IntentExecutionState.BUILDING_SCHEMA,
            artifacts={"ra_plan": ra.model_dump(mode="json")},
            errors=[IntentError(type="RA_PLAN_FAILED", message=ra.summary, hint=ErrorHint.AUTOLINK_ENRICH)],
            error_class="ra_plan_failed",
            state_delta={"ra_ok": False},
        )
    return StepResult(
        status=StepStatus.ADVANCE,
        next_phase=IntentExecutionState.RENDERING_SQL,
        artifacts={"ra_plan": ra.model_dump(mode="json")},
        new_evidence=["ra_plan_ready"],
        state_delta={"ra_ok": True},
    )


def _step_render_sql(*, node: IntentNode, schema: Dict[str, Any], context: Dict[str, Any], model: Any) -> StepResult:
    ra_plan = node.artifacts.get("ra_plan") or {}
    sql_render_input = {
        "intent": _intent_payload(node),
        "ra_plan": ra_plan,
        "schema": schema,
        "context": context,
    }
    log_step_input(logger, "SQL_renderer", sql_render_input, intent_id=node.intent_id)
    rendered = run_sql_renderer(
        model=model,
        intent_payload=_intent_payload(node),
        ra_plan=ra_plan,
        schema=schema,
        context=context,
    )
    log_step_output(
        logger,
        "SQL_renderer",
        {"ok": rendered.ok, "summary": rendered.summary, "candidates": [c.model_dump(mode="json") for c in rendered.candidates]},
        intent_id=node.intent_id,
    )
    candidates = [c.model_dump(mode="json") for c in rendered.candidates]
    if not rendered.ok or not rendered.candidates:
        return StepResult(
            status=StepStatus.ADVANCE,
            next_phase=IntentExecutionState.BUILDING_SCHEMA,
            artifacts={"sql_candidates": candidates},
            errors=[IntentError(type="SQL_RENDER_FAILED", message=rendered.summary, hint=ErrorHint.AUTOLINK_ENRICH)],
            error_class="sql_render_failed",
            state_delta={"candidate_count": 0},
        )
    return StepResult(
        status=StepStatus.ADVANCE,
        next_phase=IntentExecutionState.VALIDATING_SQL,
        artifacts={"sql_candidates": candidates},
        new_evidence=["sql_candidates_ready"],
        state_delta={"candidate_count": len(candidates)},
    )


def _step_validate_sql(*, node: IntentNode, database: str) -> StepResult:
    candidates_raw = node.artifacts.get("sql_candidates") or []
    from stages.sql_generation.intent.models import SQLCandidate

    candidates = [SQLCandidate.model_validate(item) for item in candidates_raw if isinstance(item, dict)]
    log_step_input(
        logger,
        "SQL_validator",
        {"database": database, "candidate_count": len(candidates), "candidates": candidates_raw},
        intent_id=node.intent_id,
    )
    validate_out = validate_candidates(candidates=candidates, database=database)
    validations = validate_out.model_dump(mode="json")
    log_step_output(
        logger,
        "SQL_validator",
        {"ok": validate_out.ok, "best_candidate_index": validate_out.best_candidate_index, "reports": validations.get("reports", [])},
        intent_id=node.intent_id,
    )
    if not validate_out.ok:
        return StepResult(
            status=StepStatus.ADVANCE,
            next_phase=IntentExecutionState.RENDERING_SQL,
            artifacts={"validations": validations},
            errors=[IntentError(type="SQL_VALIDATE_FAILED", message="all sql candidates failed validation", hint=ErrorHint.RERENDER_SQL)],
            error_class="sql_validate_failed",
            state_delta={"validated": False},
        )
    best = candidates[validate_out.best_candidate_index].model_dump(mode="json")
    return StepResult(
        status=StepStatus.ADVANCE,
        next_phase=IntentExecutionState.EXECUTING_SQL,
        artifacts={"validations": validations, "chosen_sql_candidate": best},
        new_evidence=["sql_validated"],
        state_delta={"validated": True},
    )


def _step_execute_sql(*, node: IntentNode, database: str, context: Dict[str, Any], max_rows: int) -> StepResult:
    chosen = node.artifacts.get("chosen_sql_candidate") or {}
    sql = str(chosen.get("sql") or "").strip()
    if not sql:
        return StepResult(
            status=StepStatus.FAIL,
            next_phase=IntentExecutionState.FAILED,
            errors=[IntentError(type="MISSING_SQL", message="chosen sql candidate missing", hint=ErrorHint.RERENDER_SQL)],
            error_class="missing_sql",
        )
    try:
        rows = execute_select_with_limit_tool.invoke(
            {
                "sql": sql,
                "limit": int(max_rows),
                "timeout_ms": int(context.get("timeout_ms_per_call", _RUNTIME_CFG.timeout_ms_per_call)),
                "database": database,
            }
        )
    except Exception as exc:
        err = str(exc)
        lower = err.lower()
        error_class = "sql_exec_schema_error" if any(x in lower for x in ("unknown column", "unknown table", "doesn't exist", "1146", "1054")) else "sql_exec_failed"
        next_phase = IntentExecutionState.BUILDING_SCHEMA if error_class == "sql_exec_schema_error" else IntentExecutionState.RENDERING_SQL
        hint = ErrorHint.AUTOLINK_ERROR if error_class == "sql_exec_schema_error" else ErrorHint.RERENDER_SQL
        return StepResult(
            status=StepStatus.ADVANCE,
            next_phase=next_phase,
            errors=[IntentError(type="SQL_EXEC_FAILED", message=err[:300], hint=hint)],
            error_class=error_class,
            state_delta={"exec_ok": False},
        )

    columns = list(rows[0].keys()) if rows and isinstance(rows[0], dict) else []
    safe_rows = json.loads(safe_json_dumps(rows[: int(max_rows)])) if rows else []
    exec_raw = {"columns": columns, "rows": safe_rows, "note": ""}
    return StepResult(
        status=StepStatus.ADVANCE,
        next_phase=IntentExecutionState.INTERPRETING_RESULT,
        artifacts={"exec_raw": exec_raw, "exec_result": {"row_count": len(rows)}},
        new_evidence=["sql_executed"],
        state_delta={"row_count": len(rows)},
    )


def _step_interpret_result(*, node: IntentNode, model: Any) -> StepResult:
    chosen = node.artifacts.get("chosen_sql_candidate") or {}
    exec_raw = node.artifacts.get("exec_raw") or {}
    interp_input = {
        "intent": _intent_payload(node),
        "sql": str(chosen.get("sql") or ""),
        "exec_raw": exec_raw,
        "assumptions": list(chosen.get("assumptions") or []),
    }
    log_step_input(logger, "Result_interpreter", interp_input, intent_id=node.intent_id)
    interp = run_result_interpreter(
        model=model,
        intent_payload=_intent_payload(node),
        sql=str(chosen.get("sql") or ""),
        exec_raw=exec_raw,
        assumptions=list(chosen.get("assumptions") or []),
    )
    log_step_output(logger, "Result_interpreter", interp.model_dump(mode="json"), intent_id=node.intent_id)
    return StepResult(
        status=StepStatus.COMPLETE,
        next_phase=IntentExecutionState.COMPLETED,
        artifacts={"interpretation": interp.model_dump(mode="json")},
        new_evidence=["interpretation_ready"],
        state_delta={"interpretation_ok": bool(interp.ok)},
    )


def run_intent_node(
    node: IntentNode,
    state: GlobalState,
    *,
    model_name: Optional[str] = None,
    max_rows: Optional[int] = None,
    max_rounds: Optional[int] = None,
) -> Tuple[Any, Any]:
    trace = TraceRecorder()
    trace.record("INTENT_START", {"intent_id": node.intent_id, "description": node.description[:200]})
    logger.info(
        "意图节点开始 | intent_id=%s | 描述=%s",
        node.intent_id,
        (node.description or "")[:80],
    )

    dep_err = _require_deps_completed(node, state)
    if dep_err:
        trace.record("DEPENDENCY_NOT_READY", {"error": dep_err})
        return False, dep_err

    dependency_payload = build_dependency_payload(node, state)
    node.artifacts["dependency_payload"] = _artifact_jsonable(dependency_payload)

    context = state.config.get("context") or {}
    database_scope = context.get("database_scope") or get_app_config().get_default_database_scope()
    if not database_scope:
        err = "context.database_scope is required"
        _record_failed(node, err, [IntentError(type="MISSING_CONTEXT", message=err, hint=ErrorHint.ASK_USER)])
        return False, err
    primary_db = str(database_scope[0])
    resolved_model_name = str(model_name or context.get("model_name") or _RUNTIME_CFG.model_name)
    resolved_max_rows = int(max_rows or context.get("max_rows") or _PIPELINE_CFG.max_rows)
    resolved_max_rounds = int(max_rounds or context.get("max_rounds_per_intent") or _PIPELINE_CFG.max_rounds_per_intent)
    model = get_llm(resolved_model_name)

    user_hints = node.artifacts.get("user_hints") or {}
    if not isinstance(user_hints, dict):
        user_hints = {}
    agent_context = {
        "database_scope": database_scope,
        "sql_dialect": context.get("sql_dialect", "MYSQL"),
        "hints": user_hints,
        "template_guidance": build_template_guidance(),
    }

    checkpoint = _checkpoint(node)
    if checkpoint.phase in (IntentExecutionState.INIT, IntentExecutionState.WAITING_USER):
        checkpoint.phase = IntentExecutionState.BUILDING_SCHEMA if not node.artifacts.get("schema") else IntentExecutionState.PLANNING_RA
        _save_checkpoint(node, checkpoint)

    max_iterations = max(int(resolved_max_rounds) * 3, _RUNTIME_CFG.max_runtime_iterations)
    for _ in range(max_iterations):
        phase = checkpoint.phase
        trace.record("PHASE_START", {"phase": phase.value})
        logger.info("意图 %s | 阶段：%s", node.intent_id, _phase_name_cn(phase))

        if phase == IntentExecutionState.BUILDING_SCHEMA:
            step = _step_build_schema(
                node=node,
                checkpoint=checkpoint,
                database_scope=database_scope,
                context=context,
                model_name=resolved_model_name,
                user_hints=user_hints,
            )
        elif phase == IntentExecutionState.PLANNING_RA:
            step = _step_plan_ra(
                node=node,
                dependency_payload=dependency_payload,
                schema=node.artifacts.get("schema") or {"databases": {}},
                context=agent_context,
                model=model,
            )
        elif phase == IntentExecutionState.RENDERING_SQL:
            step = _step_render_sql(
                node=node,
                schema=node.artifacts.get("schema") or {"databases": {}},
                context=agent_context,
                model=model,
            )
        elif phase == IntentExecutionState.VALIDATING_SQL:
            step = _step_validate_sql(node=node, database=primary_db)
        elif phase == IntentExecutionState.EXECUTING_SQL:
            step = _step_execute_sql(node=node, database=primary_db, context=context, max_rows=resolved_max_rows)
        elif phase == IntentExecutionState.INTERPRETING_RESULT:
            step = _step_interpret_result(node=node, model=model)
        else:
            step = StepResult(
                status=StepStatus.FAIL,
                next_phase=IntentExecutionState.FAILED,
                errors=[IntentError(type="UNKNOWN_PHASE", message=f"unsupported phase {phase.value}")],
                error_class="unknown_phase",
            )

        _apply_step_artifacts(node, step)
        trace.record(
            "STEP_RESULT",
            {
                "phase": phase.value,
                "status": step.status.value,
                "next_phase": step.next_phase.value,
                "error_class": step.error_class,
                "evidence": list(step.new_evidence or [])[:5],
                "errors": [e.type for e in list(step.errors or [])[:3]],
            },
        )
        checkpoint.artifacts = {
            "schema_present": bool((node.artifacts.get("schema") or {}).get("databases")),
            "ra_present": bool(node.artifacts.get("ra_plan")),
            "sql_candidates": len(node.artifacts.get("sql_candidates") or []),
        }
        checkpoint.errors.extend([e.message for e in step.errors])

        # 阶段性成果：输出 schema / 关系代数 / 生成 SQL 的具体结果
        if phase == IntentExecutionState.BUILDING_SCHEMA and step.next_phase == IntentExecutionState.PLANNING_RA:
            schema = node.artifacts.get("schema") or {}
            tables_summary = _schema_tables_summary(schema)
            table_count = sum(len((db or {}).get("tables") or {}) for db in (schema.get("databases") or {}).values())
            logger.info("意图 %s | Schema 构建完成 | 表数=%s | 表: %s", node.intent_id, table_count, tables_summary)
        if phase == IntentExecutionState.PLANNING_RA and step.next_phase == IntentExecutionState.RENDERING_SQL:
            ra = node.artifacts.get("ra_plan") or {}
            logger.info("意图 %s | 关系代数生成完成 | %s", node.intent_id, _ra_plan_summary(ra))
        if phase == IntentExecutionState.RENDERING_SQL and step.next_phase == IntentExecutionState.VALIDATING_SQL:
            candidates = node.artifacts.get("sql_candidates") or []
            logger.info("意图 %s | SQL 生成完成 | 候选数=%s", node.intent_id, len(candidates))
            if candidates and isinstance(candidates[0], dict):
                first_sql = (candidates[0].get("sql") or "").strip()[:200]
                if first_sql:
                    logger.info("意图 %s | 候选 SQL(1): %s%s", node.intent_id, first_sql, "..." if len((candidates[0].get("sql") or "")) > 200 else "")
        if phase == IntentExecutionState.VALIDATING_SQL and step.next_phase == IntentExecutionState.EXECUTING_SQL:
            chosen = node.artifacts.get("chosen_sql_candidate") or {}
            sql = (chosen.get("sql") or "").strip()
            if sql:
                logger.info("意图 %s | 选中 SQL: %s", node.intent_id, sql[:300] + ("..." if len(sql) > 300 else ""))

        guard_reason = _update_guard(node, checkpoint, step)
        if guard_reason:
            message = f"intent convergence guard triggered: {guard_reason}"
            trace.record("CONVERGENCE_STOP", {"reason": guard_reason})
            _record_failed(node, message, [IntentError(type="CONVERGENCE_STOP", message=message)])
            return False, message

        if step.status == StepStatus.WAIT_USER:
            return _record_wait_user(
                node=node,
                state=state,
                checkpoint=checkpoint,
                trace=trace,
                question_id="Q_SCHEMA",
                situation="系统在当前数据库范围内未能稳定定位完成该意图所需的最小 schema。",
                request="请补充可能相关的表名/字段名、唯一性/关联口径或时间范围。",
                why_needed="缺少这些信息时系统无法稳定构建最小完备 schema。",
                examples=["例如：表=equipment，字段=name，口径=按 factory_id 唯一"],
            )
        if step.status == StepStatus.FAIL:
            message = "; ".join(e.message for e in step.errors) or "intent step failed"
            trace.record("INTENT_DONE", {"status": "FAILED", "error": message})
            _record_failed(node, message, step.errors)
            return False, message
        if step.status == StepStatus.COMPLETE:
            chosen = node.artifacts.get("chosen_sql_candidate") or {}
            rows = list((node.artifacts.get("exec_raw") or {}).get("rows") or [])
            facts_bundle = _build_facts_bundle(node, rows, str(chosen.get("sql") or ""))
            node.artifacts["facts_bundle"] = facts_bundle.model_dump(mode="json")
            final = IntentFinalBundle(
                status=IntentRunStatus.SUCCESS,
                interpretation=Interpretation.model_validate(node.artifacts.get("interpretation"))
                if node.artifacts.get("interpretation")
                else None,
                final_sql=str(chosen.get("sql") or ""),
                final_sql_fingerprint=str(chosen.get("fingerprint") or ""),
                schema=node.artifacts.get("schema") or {"databases": {}},
                exec_raw=node.artifacts.get("exec_raw") or {},
                facts_bundle=facts_bundle,
                assumptions=list(chosen.get("assumptions") or []) + list(node.artifacts.get("assumptions") or []),
                errors=[],
            )
            payload = final.model_dump(mode="json", by_alias=True)
            payload["audit"] = trace.to_dict()
            node.artifacts["final"] = payload
            checkpoint.phase = IntentExecutionState.COMPLETED
            _save_checkpoint(node, checkpoint)
            trace.record("INTENT_DONE", {"status": "SUCCESS"})
            logger.info("意图 %s | 完成 | 状态=SUCCESS", node.intent_id)
            return True, payload

        checkpoint.phase = step.next_phase
        _save_checkpoint(node, checkpoint)

    message = "max_runtime_iterations exceeded"
    _record_failed(node, message, [IntentError(type="ITERATION_LIMIT", message=message)])
    return False, message
