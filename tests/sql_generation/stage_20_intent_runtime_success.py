from __future__ import annotations

import os
import sys

_this_dir = os.path.dirname(os.path.abspath(__file__))
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)

from stage_utils import add_src_to_path, ensure_db_smoke, get_model_name


if __name__ == "__main__":
    add_src_to_path()
    ensure_db_smoke(database="industrial_monitoring")

    from stages.sql_generation.dag.models import GlobalState, IntentNode
    from stages.sql_generation.intent.runtime import run_intent_node

    # A "likely-to-succeed" deterministic intent against the provided DDL:
    # serial_number is UNIQUE, so duplicates should be empty.
    node = IntentNode(
        intent_id="I1",
        description="检查 equipment 表中 serial_number 是否存在重复记录；如有，输出重复 serial_number 与重复次数。",
        deps=[],
    )

    state = GlobalState(
        intent_map={"I1": node},
        ready_queue=["I1"],
        running_set=set(),
        completed_set=set(),
        dependency_index={},
        remaining_deps_count={"I1": 0},
        tool_registry={},
        config={
            "context": {"database_scope": ["industrial_monitoring"], "sql_dialect": "MYSQL", "max_rows": 50, "timeout_ms_per_call": 30000}
        },
        audit_log=[],
    )

    model_name = get_model_name()
    ok, payload = run_intent_node(node, state, model_name=model_name, max_rows=50, max_rounds=4)
    if ok is not True:
        raise AssertionError(f"intent runtime failed: ok={ok} payload={payload}")

    if not isinstance(payload, dict):
        raise AssertionError(f"expected payload dict, got {type(payload).__name__}")

    final_sql = str(payload.get("final_sql") or "")
    if not final_sql.strip():
        raise AssertionError("expected non-empty final_sql")

    exec_raw = payload.get("exec_raw") or {}
    if not isinstance(exec_raw, dict):
        raise AssertionError(f"expected exec_raw dict, got {type(exec_raw).__name__}")

    print(
        "ok: intent runtime succeeded",
        {
            "model_name": model_name,
            "final_sql_preview": final_sql[:200],
            "row_count": len(exec_raw.get("rows") or []),
            "confidence": ((payload.get("interpretation") or {}) if isinstance(payload.get("interpretation"), dict) else {}).get("confidence"),
        },
    )
