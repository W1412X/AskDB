from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, List

from config import get_app_config
from stages.initialize.agent.run import initialize_databases
from stages.initialize.embedding.build_embedding import build_embeddings
from stages.sql_generation.pipeline import (
    SQLStageResult,
    StageStatus,
    resume_sql_generation_stage_after_user_reply,
    run_sql_generation_stage,
)
from utils.data_paths import DataPaths

EXIT_WORDS = {"exit", "quit", ":q"}


def _column_json_paths(database_name: str) -> List[Path]:
    base = DataPaths.default().initialize_agent_database_dir(database_name)
    if not base.exists():
        return []
    return sorted(
        path
        for path in base.rglob("*.json")
        if not path.name.startswith("TABLE_") and not path.name.startswith("DATABASE_")
    )


def _embedding_paths(database_name: str) -> List[Path]:
    base = DataPaths.default().initialize_embedding_database_dir(database_name)
    if not base.exists():
        return []
    return sorted(base.rglob("*.pkl"))


def _needs_initialize_agent(database_name: str) -> bool:
    return len(_column_json_paths(database_name)) == 0


def _needs_initialize_embedding(database_name: str) -> bool:
    column_jsons = _column_json_paths(database_name)
    if not column_jsons:
        return True
    return len(_embedding_paths(database_name)) < len(column_jsons)


def _ensure_initialize_artifacts() -> None:
    cfg = get_app_config()
    target_databases = sorted(set(cfg.get_initialize_databases()) | set(cfg.get_default_database_scope()))
    if not target_databases:
        raise RuntimeError("No databases configured for initialize/query flow.")

    missing_agent = [db for db in target_databases if _needs_initialize_agent(db)]
    if missing_agent:
        print(f"[bootstrap] initialize agent outputs missing for: {', '.join(missing_agent)}")
        initialize_databases(
            database_names=missing_agent,
            model_name=cfg.stages.initialize.agent.model_name,
        )

    missing_embedding = [db for db in target_databases if _needs_initialize_embedding(db)]
    if missing_embedding:
        emb_cfg = cfg.stages.initialize.embedding
        model_path = None
        local_only = False
        if getattr(emb_cfg, "model_path_name", None):
            candidate = DataPaths.model_embedding_path(str(emb_cfg.model_path_name))
            if candidate.exists():
                model_path = str(candidate)
                local_only = True
        print(f"[bootstrap] initialize embeddings missing for: {', '.join(missing_embedding)}")
        build_embeddings(
            database_names=missing_embedding,
            model_name=emb_cfg.model_name,
            model_path=model_path,
            normalize_embeddings=emb_cfg.normalize_embeddings,
            batch_size=emb_cfg.batch_size,
            device=emb_cfg.device or None,
            local_files_only=local_only,
            overwrite=emb_cfg.overwrite,
        )


def _build_stage_context() -> Dict[str, Any]:
    cfg = get_app_config()
    return {
        "database_scope": cfg.get_default_database_scope(),
        "sql_dialect": "MYSQL",
        "max_rows": cfg.stages.sql_generation.pipeline.max_rows,
        "max_rounds_per_intent": cfg.stages.sql_generation.pipeline.max_rounds_per_intent,
        "timeout_ms_per_call": cfg.stages.sql_generation.intent_runtime.timeout_ms_per_call,
        "max_meta_tables": cfg.stages.sql_generation.autolink.max_meta_tables,
    }


def _render_ticket(ticket: Dict[str, Any]) -> str:
    payload = dict(ticket.get("payload") or {})
    ask = dict(payload.get("ask") or {})
    lines: List[str] = []
    title = str(payload.get("question_id") or ticket.get("question_id") or "clarification")
    lines.append(f"[需要补充信息] {title}")

    question = str(ask.get("question") or "").strip()
    if question:
        lines.append(question)
    else:
        for key in ("situation", "request", "why_needed"):
            value = str(ask.get(key) or "").strip()
            if value:
                lines.append(value)

    options = ask.get("options")
    if isinstance(options, list) and options:
        lines.append("可参考：")
        lines.extend(f"- {str(option)}" for option in options if str(option).strip())

    examples = ask.get("examples")
    if isinstance(examples, list) and examples:
        lines.append("示例：")
        lines.extend(f"- {str(example)}" for example in examples if str(example).strip())

    constraints = ask.get("constraints")
    if isinstance(constraints, list) and constraints:
        lines.append("约束：")
        lines.extend(f"- {str(item)}" for item in constraints if str(item).strip())

    acceptance = payload.get("acceptance_criteria")
    if isinstance(acceptance, list) and acceptance:
        lines.append("回答至少应覆盖：")
        lines.extend(f"- {str(item)}" for item in acceptance if str(item).strip())

    return "\n".join(lines)


def _print_success(result: SQLStageResult) -> None:
    state = result.state
    summary = state.summary()
    print("[完成]")
    print(f"state_summary={summary}")
    for intent_id, node in state.intent_map.items():
        final = dict(node.artifacts.get("final") or {})
        interpretation = dict(final.get("interpretation") or {})
        answer = str(interpretation.get("answer") or "").strip()
        if answer:
            print(f"\n[{intent_id}] {node.description}")
            print(answer)
            continue
        errors = final.get("errors") or []
        if isinstance(errors, list) and errors:
            first_error = errors[0]
            if isinstance(first_error, dict):
                print(f"\n[{intent_id}] FAILED: {first_error.get('message') or first_error.get('type')}")


def _run_query(query: str, *, context: Dict[str, Any], model_name: str, max_concurrency: int) -> None:
    result = run_sql_generation_stage(
        query=query,
        context=context,
        model_name=model_name,
        max_concurrency=max_concurrency,
    )
    while result.status == StageStatus.WAIT_USER:
        ticket = dict(result.dialog_ticket or {})
        ticket_id = str(ticket.get("ticket_id") or "")
        if not ticket_id:
            raise RuntimeError(f"WAIT_USER without ticket_id: {ticket}")
        print()
        print(_render_ticket(ticket))
        reply = input("你> ").strip()
        if reply.lower() in EXIT_WORDS:
            raise KeyboardInterrupt
        result = resume_sql_generation_stage_after_user_reply(
            state=result.state,
            ticket_id=ticket_id,
            user_message=reply,
            context=context,
            model_name=model_name,
        )

    if result.status == StageStatus.SUCCESS:
        _print_success(result)
        return

    print("[失败]")
    print(result.error or result.state.summary())


def main(argv: List[str] | None = None) -> int:
    cfg = get_app_config()
    # Ensure fresh checkout has required base dirs (data/, log/).
    DataPaths.default().ensure_base_dirs()
    parser = argparse.ArgumentParser(description="Interactive SQL generation terminal.")
    parser.add_argument("--skip-init", action="store_true", help="Skip initialize/embedding bootstrap check.")
    parser.add_argument("--query", default="", help="Run one query once, then exit.")
    parser.add_argument("--max-concurrency", type=int, default=cfg.stages.sql_generation.pipeline.max_concurrency)
    args = parser.parse_args(argv)

    if not args.skip_init:
        _ensure_initialize_artifacts()

    context = _build_stage_context()
    model_name = cfg.stages.sql_generation.pipeline.model_name

    if args.query.strip():
        _run_query(
            args.query.strip(),
            context=context,
            model_name=model_name,
            max_concurrency=int(args.max_concurrency),
        )
        return 0

    print("SQL generation interactive loop started. 输入 exit/quit 结束。")
    while True:
        try:
            query = input("用户> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 0
        if not query:
            continue
        if query.lower() in EXIT_WORDS:
            return 0
        try:
            _run_query(
                query,
                context=context,
                model_name=model_name,
                max_concurrency=int(args.max_concurrency),
            )
        except KeyboardInterrupt:
            print()
            return 0
        except Exception as exc:
            print(f"[异常] {type(exc).__name__}: {exc}")


if __name__ == "__main__":
    raise SystemExit(main())
