"""
AutoLink 对外入口：统一 Planner -> Tool Executor -> RoundJudge。
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from config.app_config import get_app_config
from utils.id_generator import ensure_id, new_plan_id, new_request_id, new_trace_id
from utils.logger import get_logger

from stages.sql_generation.autolink.agents import run_round_judge, run_schema_planner, run_tool_agent
from stages.sql_generation.autolink.logging_utils import log_step_input, log_step_output, schema_summary
from stages.sql_generation.autolink.middleware import validate_request
from stages.sql_generation.autolink.models import (
    AgentMessage,
    AgentMemory,
    AutolinkPhase,
    AutolinkOutput,
    AutolinkRequest,
    AutolinkState,
    CompletenessAssessment,
    EventType,
    RequirementPlan,
    RequestType,
    RunStatus,
    Schema,
    SubTask,
    SubTaskIntent,
    ToolAgentName,
    render_subtask_intent,
)
from stages.sql_generation.autolink.registry import build_default_registry
from stages.sql_generation.autolink.tools.sql_draft import reset_sql_draft_count
from stages.sql_generation.autolink.tracing import TraceRecorder
from stages.sql_generation.autolink.completeness import check_build_invariants, request_needs_samples
from stages.sql_generation.autolink.agents.semantic_enricher import run_semantic_enricher

logger = get_logger("autolink")
_AUTOLINK_CFG = get_app_config().stages.sql_generation.autolink


def run_autolink(
    request: AutolinkRequest | dict,
    *,
    registry: Optional[Any] = None,
    model: Optional[Any] = None,
    max_rounds: Optional[int] = None,
    max_meta_tables: Optional[int] = None,
) -> AutolinkOutput:
    req = AutolinkRequest.model_validate(request) if isinstance(request, dict) else request
    resolved_max_rounds = int(max_rounds or _AUTOLINK_CFG.max_rounds)
    max_meta = max(1, int(getattr(req.context, "max_meta_tables", None) or max_meta_tables or _AUTOLINK_CFG.max_meta_tables))
    setattr(req.context, "max_meta_tables", max_meta)
    validate_request(req)

    request_id = ensure_id(req.request_id, new_request_id)
    plan_id = new_plan_id()
    trace_id = ensure_id(req.trace_id, new_trace_id)

    logger.info("Autolink 开始 | request_id=%s | mode=%s", request_id, req.request_type.value)
    log_step_input(
        logger,
        "run_autolink.request",
        req.model_dump(mode="json", by_alias=True),
        request_type=req.request_type.value,
    )

    trace = TraceRecorder(request_id=request_id, plan_id=plan_id, trace_id=trace_id)
    trace.record(EventType.REQUEST_RECEIVED, payload={"request": req.request[:200]})

    registry = registry or build_default_registry()
    reset_sql_draft_count()
    state = AutolinkState(
        request_id=request_id,
        plan_id=plan_id,
        trace_id=trace_id,
        request_type=req.request_type,
        request=req.request,
        schema=req.schema_data or Schema(),
        context=req.context,
        max_rounds=resolved_max_rounds,
        model_available=(model is not None),
    )

    try:
        for round_idx in range(resolved_max_rounds):
            if state.stop_reason:
                break
            state.round = round_idx + 1
            logger.info("第 %s 轮 | mode=%s", state.round, state.request_type.value, round=state.round, max_rounds=resolved_max_rounds, mode=state.request_type.value)
            log_step_input(
                logger,
                "runtime.round_state",
                {
                    "round": state.round,
                    "request_type": state.request_type.value,
                    "sql_draft_success": state.sql_draft_success,
                    "latest_judge_result": state.latest_judge_result.model_dump(mode="json"),
                },
                round_index=state.round,
                schema_summary=schema_summary(state.schema_data),
            )
            trace.record(EventType.REQUEST_ROUTED, payload={"round": state.round, "mode": state.request_type.value})
            schema_at_round_start = _schema_fingerprint(state.schema_data)

            state.phase = _determine_phase(state)
            trace.record(EventType.AGENT_INVOKED, payload={"agent": "SchemaPlanner", "phase": state.phase.value})
            planner_out = run_schema_planner(
                mode=state.request_type,
                request=state.request,
                schema=state.schema_data,
                context=state.context,
                recent_tool_results=state.last_tool_results,
                model=model,
                round_index=state.round,
                latest_judge=state.latest_judge_result.model_dump(mode="json"),
                memory_context=_get_agent_memory_payload(state, "SchemaPlanner"),
                step_logs=state.step_logs,
                error_context=_derive_error_context(state),
            )
            log_step_output(
                logger,
                "runtime.planner_output",
                planner_out.model_dump(mode="json"),
                round_index=state.round,
                write_count=len(planner_out.schema_write_plan.writes),
                sub_task_count=len(planner_out.sub_tasks),
            )
            state.schema_data = _apply_schema_write_plan(state.schema_data, planner_out.schema_write_plan)
            state.findings.extend(planner_out.findings or [])
            _append_step_log(
                state,
                {"round": state.round, "agent": "SchemaPlanner", "phase": state.phase.value, "summary": (planner_out.summary or "")[:200], "ok": planner_out.ok},
            )
            _remember_agent_exchange(
                state,
                "SchemaPlanner",
                user_payload={"round": state.round, "request_type": state.request_type.value},
                assistant_payload={
                    "summary": planner_out.summary,
                    "sub_tasks": [render_subtask_intent(task.task) for task in planner_out.sub_tasks[:4]],
                    "requirement_focus": planner_out.requirement_focus.model_dump(mode="json") if planner_out.requirement_focus else {},
                },
            )

            tool_agent_results_this_round: List[Dict[str, Any]] = []
            for sub_task in _phase_sub_tasks(state, planner_out.sub_tasks):
                trace.record(EventType.TOOL_AGENT_INVOKED, payload={"agent": sub_task.tool_agent_name.value})
                tool_out = run_tool_agent(
                    agent_name=sub_task.tool_agent_name,
                    task=sub_task.task,
                    request=state.request,
                    database_scope=state.context.database_scope,
                    registry=registry,
                    model=model,
                    schema=state.schema_data,
                    memory_context=_get_agent_memory_payload(state, sub_task.tool_agent_name.value),
                )
                trace.record(EventType.TOOL_AGENT_FINISHED, payload={"ok": tool_out.ok})
                state.schema_data = _apply_schema_write_plan(state.schema_data, tool_out.schema_write_plan)
                if any(attempt.tool_name == "sql_draft" and attempt.ok for attempt in tool_out.tool_calls):
                    state.sql_draft_success = True
                tool_result = {
                    "tool_agent": sub_task.tool_agent_name.value,
                    "schema_write_plan": tool_out.schema_write_plan.model_dump(mode="json"),
                    "result_mapping": tool_out.result_mapping.model_dump(mode="json") if tool_out.result_mapping else None,
                    "tool_calls": [attempt.model_dump(mode="json") for attempt in tool_out.tool_calls[-4:]],
                    "ok": tool_out.ok,
                    "errors": tool_out.errors,
                }
                tool_agent_results_this_round.append(tool_result)
                _append_step_log(
                    state,
                    {
                        "round": state.round,
                        "tool": sub_task.tool_agent_name.value,
                        "task_preview": render_subtask_intent(sub_task.task)[:120],
                        "ok": tool_out.ok,
                        "errors": (tool_out.errors or [])[:3],
                    },
                )
                _remember_agent_exchange(
                    state,
                    sub_task.tool_agent_name.value,
                    user_payload={"round": state.round, "task": sub_task.task.model_dump(mode="json")},
                    assistant_payload={
                        "summary": tool_out.summary,
                        "errors": (tool_out.errors or [])[:5],
                        "tool_calls": [attempt.model_dump(mode="json") for attempt in tool_out.tool_calls[-2:]],
                    },
                )
                log_step_output(
                    logger,
                    "runtime.tool_applied",
                    tool_result,
                    round_index=state.round,
                    tool_agent_name=sub_task.tool_agent_name.value,
                    schema_summary=schema_summary(state.schema_data),
                )

            state.last_tool_results = tool_agent_results_this_round

            # Weak semantic enrichment (LLM) is optional and can be expensive.
            # For BUILD, prefer local initialize JSON semantics (semantic_summary) and avoid extra LLM calls.
            if model is not None and state.request_type == RequestType.ENRICH:
                for _ in range(2):
                    semantic_plan = run_semantic_enricher(model=model, request=state.request, schema=state.schema_data)
                    if not semantic_plan.writes:
                        break
                    trace.record(EventType.AGENT_INVOKED, payload={"agent": "SemanticEnricher"})
                    state.schema_data = _apply_schema_write_plan(state.schema_data, semantic_plan)
            judge_result = _run_round_judge(state, trace, tool_agent_results_this_round, model)
            judge_result = _assess_autolink_completion(state, judge_result)
            schema_changed = _schema_fingerprint(state.schema_data) != schema_at_round_start
            judge_result.schema_changed = schema_changed
            judge_result.new_evidence_summary = _build_new_evidence_summary(tool_agent_results_this_round)
            state.latest_judge_result = judge_result
            log_step_output(
                logger,
                "runtime.round_judge",
                judge_result.model_dump(mode="json"),
                round_index=state.round,
                schema_summary=schema_summary(state.schema_data),
            )
            _remember_agent_exchange(
                state,
                "RoundJudge",
                user_payload={"round": state.round, "request_type": state.request_type.value},
                assistant_payload=judge_result.model_dump(mode="json"),
            )
            trace.record(
                EventType.ROUND_ASSESSMENT,
                payload={
                    "should_stop": judge_result.should_stop,
                    "stop_reason": judge_result.stop_reason,
                    "continue_reason": judge_result.continue_reason,
                },
            )

            if judge_result.should_stop:
                state.schema_data, pruned_items = _prune_schema_with_redundant_items(
                    state.schema_data,
                    judge_result.redundant_items,
                    trace,
                )
                state.last_pruned_items = pruned_items
                state.latest_judge_result.pruned_items = pruned_items
                state.stop_reason = judge_result.stop_reason or "minimal_complete"
                logger.info("判断器：停止 | reason=%s", state.stop_reason, round=state.round, stop_reason=state.stop_reason)
                break

            convergence_stop = _update_convergence(state, planner_out, tool_agent_results_this_round, schema_changed)
            if convergence_stop:
                state.stop_reason = convergence_stop
                logger.info("收敛停止 | reason=%s", state.stop_reason, round=state.round, stop_reason=state.stop_reason)
                break

        # No final LLM semantic loop for BUILD; keep artifacts minimal and fast.

        status = _resolve_status(state)
        if status == RunStatus.FAILED and not state.schema_data.databases and not state.errors:
            state.errors.append(
                "schema remains empty: likely no relevant tables/columns found within provided database_scope, "
                "or metadata access/search recall is insufficient"
            )
    except Exception as exc:
        logger.exception("run_autolink 执行失败", exception=exc)
        trace.record(EventType.RUN_FAILED, payload={"error": str(exc)})
        status = RunStatus.FAILED
        state.schema_data = state.schema_data or Schema()
        state.errors.append(str(exc))
    finally:
        table_count = sum(len(db.tables) for db in state.schema_data.databases.values())
        logger.info(
            "Autolink 完成 | status=%s | rounds=%s | tables=%s",
            status.value,
            state.round,
            table_count,
            request_id=request_id,
            status=status.value,
            rounds=state.round,
            table_count=table_count,
        )
        log_step_output(
            logger,
            "run_autolink.result",
            {
                "status": status.value,
                "schema": state.schema_data.model_dump(mode="json"),
                "stop_reason": state.stop_reason,
            },
            request_id=request_id,
            schema_summary=schema_summary(state.schema_data),
        )
        trace.record(EventType.RUN_COMPLETED, payload={"status": status.value})
        from stages.sql_generation.autolink.schema_merge import _normalize_schema_tables
        state.schema_data = _normalize_schema_tables(state.schema_data)

    return AutolinkOutput(schema=state.schema, audit=trace.to_trace(), status=status, errors=state.errors)


def _schema_fingerprint(schema: Schema) -> Dict[str, Any]:
    tables = sum(len(db.tables) for db in schema.databases.values())
    cols = sum(len(t.columns) for db in schema.databases.values() for t in db.tables.values())
    filled = 0
    for db in schema.databases.values():
        for t in db.tables.values():
            for c in t.columns.values():
                if (c.type or "").strip() or (c.description or "").strip():
                    filled += 1
    return {"tables": tables, "cols": cols, "filled": filled}


def _schema_has_sample_values(schema: Schema) -> bool:
    for db in schema.databases.values():
        for t in db.tables.values():
            for c in t.columns.values():
                if getattr(c, "sample_values", None):
                    return True
    return False


def _append_step_log(state: AutolinkState, log: Dict[str, Any]) -> None:
    state.step_logs.append(log)
    if len(state.step_logs) > 24:
        state.step_logs = state.step_logs[-24:]


def _get_or_create_agent_memory(state: AutolinkState, agent_key: str) -> AgentMemory:
    mem = state.agent_memories.get(agent_key)
    if mem is None:
        mem = AgentMemory()
        state.agent_memories[agent_key] = mem
    return mem


def _remember_agent_exchange(
    state: AutolinkState,
    agent_key: str,
    user_payload: Dict[str, Any],
    assistant_payload: Dict[str, Any],
) -> None:
    mem = _get_or_create_agent_memory(state, agent_key)
    mem.messages.append(AgentMessage(role="user", content=jsonable(user_payload)))
    mem.messages.append(AgentMessage(role="assistant", content=jsonable(assistant_payload)))
    mem.messages = mem.messages[-4:]
    mem.latest_round_summary = assistant_payload
    mem.working_memory = {
        "request_type": state.request_type.value,
        "round": state.round,
        "stop_reason": state.stop_reason,
        "latest_judge_result": state.latest_judge_result.model_dump(mode="json"),
    }
    mem.latest_schema_snapshot = {}


def _get_agent_memory_payload(state: AutolinkState, agent_key: str) -> Dict[str, Any]:
    mem = _get_or_create_agent_memory(state, agent_key)
    return {
        "recent_messages": [m.model_dump(mode="json") for m in mem.messages[-4:]],
        "working_memory": mem.working_memory,
        "latest_round_summary": mem.latest_round_summary,
    }


def jsonable(value: Any) -> str:
    import json

    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return str(value)


def _prune_schema_with_redundant_items(schema: Schema, redundant: List[str], trace: TraceRecorder) -> tuple[Schema, List[str]]:
    if not redundant:
        return schema, []

    from stages.sql_generation.autolink.schema_merge import prune_schema_by_redundant_items

    before_count = sum(len(t.columns) for db in schema.databases.values() for t in db.tables.values())
    pruned_schema = prune_schema_by_redundant_items(schema, redundant)
    after_count = sum(len(t.columns) for db in pruned_schema.databases.values() for t in db.tables.values())
    if before_count != after_count:
        logger.info("裁剪冗余项", before=before_count, after=after_count, redundant_count=len(redundant))
        trace.record(
            EventType.COMPLETENESS_CHECKED,
            payload={"pruned": True, "redundant_items": redundant[:20]},
        )
    return pruned_schema, redundant


def _update_stale_counter(state: AutolinkState, schema_changed: bool) -> None:
    if schema_changed:
        state.schema_stale_count = 0
        return
    state.schema_stale_count += 1


def _apply_schema_write_plan(base: Schema, plan: Any) -> Schema:
    from stages.sql_generation.autolink.schema_merge import apply_schema_write_plan

    return apply_schema_write_plan(base, plan)


def _build_new_evidence_summary(tool_agent_results: List[Dict[str, Any]]) -> List[str]:
    summaries: List[str] = []
    for r in tool_agent_results:
        plan = r.get("schema_write_plan") or {}
        plan_writes = plan.get("writes", []) if isinstance(plan, dict) else []
        if plan_writes:
            summaries.append("schema_write_plan_added")
        if r.get("result_mapping") and plan_writes:
            summaries.append("result_mapping_added")
        if r.get("ok") is False and r.get("errors"):
            summaries.append(f"tool_error:{str(r['errors'][0])[:80]}")
    return summaries[:5]


def _determine_phase(state: AutolinkState) -> AutolinkPhase:
    from stages.sql_generation.autolink.completeness import (
        has_any_keys,
        has_any_sample_values,
        has_any_strong_column_type,
        has_any_tables,
        request_needs_data_exploration,
    )

    if not has_any_tables(state.schema_data):
        return AutolinkPhase.DISCOVER
    if not has_any_strong_column_type(state.schema_data) or not has_any_keys(state.schema_data):
        return AutolinkPhase.HYDRATE_METADATA
    needs_samples = request_needs_samples(state.request) or request_needs_data_exploration(state.request)
    if needs_samples and not has_any_sample_values(state.schema_data):
        return AutolinkPhase.EXPLORE_DATA
    return AutolinkPhase.ASSESS_COMPLETENESS


def _phase_sub_tasks(state: AutolinkState, planner_sub_tasks: List[SubTask]) -> List[SubTask]:
    if planner_sub_tasks:
        filtered = [
            task for task in planner_sub_tasks
            if (
                (state.phase == AutolinkPhase.DISCOVER and task.tool_agent_name == ToolAgentName.SCHEMA_RETRIEVAL)
                or (state.phase == AutolinkPhase.HYDRATE_METADATA and task.tool_agent_name in {ToolAgentName.SCHEMA_RETRIEVAL, ToolAgentName.SCHEMA_META})
                or (state.phase == AutolinkPhase.EXPLORE_DATA and task.tool_agent_name == ToolAgentName.SCHEMA_EXPLORER)
                or (state.phase == AutolinkPhase.ASSESS_COMPLETENESS)
            )
        ]
        if filtered:
            return filtered[: state.tool_step_limit_per_round]

    if state.phase == AutolinkPhase.DISCOVER:
        return [
            SubTask(
                tool_agent_name=ToolAgentName.SCHEMA_RETRIEVAL,
                task=SubTaskIntent(
                    goal="retrieve_relevant_schema",
                    success_criteria=["return request-relevant schema write plan"],
                    notes=state.request[:200],
                ),
                expected_output="schema_write_plan",
            )
        ]
    if state.phase == AutolinkPhase.HYDRATE_METADATA:
        return [
            SubTask(
                tool_agent_name=ToolAgentName.SCHEMA_META,
                task=SubTaskIntent(
                    goal="fetch_table_metadata",
                    target_tables=_schema_tables(state.schema_data)[:3],
                    success_criteria=["fill key metadata for current request"],
                    notes="deterministic metadata hydration",
                ),
                expected_output="schema_write_plan",
            )
        ]
    from stages.sql_generation.autolink.completeness import (
        request_needs_data_exploration,
    )

    needs_samples = request_needs_samples(state.request) or request_needs_data_exploration(state.request)
    if state.phase == AutolinkPhase.EXPLORE_DATA and needs_samples:
        return [
            SubTask(
                tool_agent_name=ToolAgentName.SCHEMA_EXPLORER,
                task=SubTaskIntent(
                    goal="collect_sample_values",
                    target_tables=_schema_tables(state.schema_data)[:2],
                    success_criteria=["collect request-relevant sample values"],
                    notes=state.request[:200],
                ),
                expected_output="result_mapping",
            )
        ]
    return []


def _resolve_status(state: AutolinkState) -> RunStatus:
    if state.request_type == RequestType.BUILD:
        ok, _missing = check_build_invariants(state.schema_data, state.request)
        if ok and state.stop_reason and state.stop_reason != "schema_unchanged_stale":
            return RunStatus.SUCCESS
        # BUILD can be partially useful even if not complete, but must not be reported as SUCCESS.
        if not state.schema_data.databases:
            return RunStatus.FAILED
        return RunStatus.PARTIAL_SUCCESS

    has_tables = bool(state.schema_data.databases)
    if not has_tables:
        return RunStatus.PARTIAL_SUCCESS
    if state.stop_reason and state.stop_reason != "schema_unchanged_stale":
        return RunStatus.SUCCESS
    return RunStatus.PARTIAL_SUCCESS


def _assess_autolink_completion(state: AutolinkState, judge_result: CompletenessAssessment) -> CompletenessAssessment:
    from stages.sql_generation.autolink.schema_merge import prune_schema_by_redundant_items

    schema_candidate = prune_schema_by_redundant_items(state.schema_data, list(judge_result.redundant_items or []))
    ok, missing = check_build_invariants(
        schema_candidate,
        state.request,
        # Descriptions are optional for BUILD. Column semantic_summary can be loaded from initialize JSON;
        # forcing LLM weak-semantic fill here makes flows longer and is not required for usability.
        require_descriptions=False,
    )
    if ok:
        if judge_result.should_stop:
            return judge_result
        return CompletenessAssessment(
            reason=(judge_result.reason or "") + " [autolink_runtime: deterministic stop]",
            should_stop=True,
            stop_reason=judge_result.stop_reason or "minimal_complete",
            continue_reason="",
            missing_required_fields=[],
            optional_pending_fields=list(judge_result.optional_pending_fields or []) + list(judge_result.missing_required_fields or []),
            redundant_items=list(judge_result.redundant_items or []),
            new_evidence_summary=list(judge_result.new_evidence_summary or []),
            pruned_items=list(judge_result.pruned_items or []),
            schema_changed=judge_result.schema_changed,
        )

    if _should_allow_empty_schema_exit(state):
        message = "no relevant schema found after best-effort discovery"
        state.errors.append(message)
        return CompletenessAssessment(
            reason=(judge_result.reason or "") + " [autolink_runtime: allow empty schema exit]",
            should_stop=True,
            stop_reason="no_relevant_schema",
            continue_reason="",
            missing_required_fields=[],
            optional_pending_fields=list(judge_result.optional_pending_fields or []),
            redundant_items=list(judge_result.redundant_items or []),
            new_evidence_summary=list(judge_result.new_evidence_summary or []),
            pruned_items=list(judge_result.pruned_items or []),
            schema_changed=judge_result.schema_changed,
        )
    return judge_result.model_copy(update={"missing_required_fields": missing, "should_stop": False, "stop_reason": "", "continue_reason": "build invariants not satisfied"})


def _should_allow_empty_schema_exit(state: AutolinkState) -> bool:
    """
    Prevent "lazy empty schema":
    Only allow empty schema stop when:
    - schema still has no tables
    - we have tried at least retrieval + explorer across rounds
    - recent tool results show no writes were produced
    - we have spent at least 2 rounds (avoid immediate give-up)
    """
    if state.request_type != RequestType.BUILD:
        return False
    if state.schema_data.databases:
        return False
    if state.round < 2:
        return False

    # Flatten tool results across all rounds we kept (only last round is stored, but we can use step_logs to infer).
    # Use last_tool_results + step_logs as best-effort signal while keeping code simple.
    last = list(state.last_tool_results or [])
    if not last:
        return False

    attempted_agents = {r.get("tool_agent") for r in last if isinstance(r, dict)}
    if not {"SchemaRetrievalAgent", "SchemaExplorerAgent"}.issubset(attempted_agents):
        return False

    def _has_writes(r: Dict[str, Any]) -> bool:
        plan = r.get("schema_write_plan") or {}
        if isinstance(plan, dict):
            return bool(plan.get("writes"))
        return False

    if any(_has_writes(r) for r in last if isinstance(r, dict)):
        return False

    # If explorer/sql_explore did run but produced no mapping writes, treat as "no evidence" only if it also had no errors.
    explorer = [r for r in last if isinstance(r, dict) and r.get("tool_agent") == "SchemaExplorerAgent"]
    if explorer and any((r.get("errors") or []) for r in explorer):
        return False
    return True


def _update_convergence(
    state: AutolinkState,
    planner_out: Any,
    tool_agent_results: List[Dict[str, Any]],
    schema_changed: bool,
) -> str:
    repeated = dict(state.convergence.get("repeated_error_classes") or {})
    if not schema_changed and not planner_out.schema_write_plan.writes and not any((r.get("schema_write_plan") or {}).get("writes") for r in tool_agent_results):
        state.convergence["no_progress_rounds"] = int(state.convergence.get("no_progress_rounds") or 0) + 1
    else:
        state.convergence["no_progress_rounds"] = 0

    for result in tool_agent_results:
        if result.get("ok") is False and result.get("errors"):
            key = str(result["errors"][0])[:80]
            repeated[key] = int(repeated.get(key) or 0) + 1
    state.convergence["repeated_error_classes"] = repeated
    edges = list(state.convergence.get("visited_phase_edges") or [])
    edges.append(state.phase.value)
    state.convergence["visited_phase_edges"] = edges[-12:]
    if int(state.convergence.get("no_progress_rounds") or 0) >= 3:
        return "autolink_no_progress"
    if any(count > 2 for count in repeated.values()):
        return "autolink_repeated_error"
    return ""


def _run_round_judge(
    state: AutolinkState,
    trace: TraceRecorder,
    tool_agent_results: List[Dict[str, Any]],
    model: Any,
) -> CompletenessAssessment:
    result = run_round_judge(
        mode=state.request_type,
        request=state.request,
        schema=state.schema_data,
        findings=state.findings,
        recent_tool_results=tool_agent_results,
        sql_draft_success=state.sql_draft_success,
        model=model,
        memory_context=_get_agent_memory_payload(state, "RoundJudge"),
    )
    trace.record(
        EventType.ROUND_ASSESSMENT,
        payload={
            "should_stop": result.should_stop,
            "redundant_items": result.redundant_items[:10],
        },
    )
    return result


def _derive_error_context(state: AutolinkState) -> str:
    error_lines: List[str] = []
    for entry in state.step_logs[-8:]:
        for error in entry.get("errors", []) or []:
            if error:
                error_lines.append(str(error))
    return " | ".join(error_lines[:4])


def _schema_tables(schema: Schema) -> List[str]:
    return [
        table_name
        for db in schema.databases.values()
        for table_name in db.tables.keys()
    ]
