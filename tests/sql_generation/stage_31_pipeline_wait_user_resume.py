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

    from stages.sql_generation.pipeline import StageStatus, resume_sql_generation_stage_after_user_reply, run_sql_generation_stage

    model_name = get_model_name()
    context = {"database_scope": ["industrial_monitoring"], "sql_dialect": "MYSQL", "max_rows": 50, "timeout_ms_per_call": 30000}

    # Intentionally under-specified query to trigger clarification in some runs.
    result = run_sql_generation_stage(
        query="检查设备唯一性是否有问题，并给出证据。",
        context=context,
        model_name=model_name,
        max_concurrency=1,
    )

    if result.status == StageStatus.SUCCESS:
        print("ok: pipeline finished without WAIT_USER", {"model_name": model_name})
        raise SystemExit(0)

    if result.status != StageStatus.WAIT_USER:
        raise AssertionError(f"expected WAIT_USER or SUCCESS, got={result.to_dict()}")

    ticket = result.dialog_ticket or {}
    ticket_id = str(ticket.get("ticket_id") or "")
    if not ticket_id:
        raise AssertionError(f"WAIT_USER but missing ticket_id: {ticket}")

    # Provide a concrete hint for the clarifier to unblock.
    user_message = "相关表是 equipment，唯一标识字段是 serial_number（应唯一）；如有重复请输出 serial_number 与重复次数。"

    resumed = resume_sql_generation_stage_after_user_reply(
        state=result.state,
        ticket_id=ticket_id,
        user_message=user_message,
        context=context,
        model_name=model_name,
    )

    if resumed.status != StageStatus.SUCCESS:
        raise AssertionError(f"expected SUCCESS after resume, got={resumed.to_dict()}")

    print("ok: pipeline WAIT_USER -> resume -> SUCCESS", {"model_name": model_name})
