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

    from stages.sql_generation.pipeline import (
        StageStatus,
        resume_sql_generation_stage_after_user_reply,
        run_sql_generation_stage,
    )

    model_name = get_model_name()
    context = {
        "database_scope": ["industrial_monitoring"],
        "sql_dialect": "MYSQL",
        "max_rows": 50,
        "timeout_ms_per_call": 30000,
    }
    result = run_sql_generation_stage(
        query="检查是否唯一",
        context=context,
        model_name=model_name,
        max_concurrency=1,
    )

    out = result.to_dict()
    if out.get("status") == StageStatus.WAIT_USER.value:
        ticket = out.get("dialog_ticket") or {}
        ticket_id = str(ticket.get("ticket_id") or "")
        if not ticket_id:
            raise AssertionError(f"WAIT_USER without ticket_id: {out}")
        result = resume_sql_generation_stage_after_user_reply(
            state=result.state,
            ticket_id=ticket_id,
            user_message="检查 industrial_monitoring.equipment 表的 serial_number 列唯一性，识别重复值并统计重复次数。",
            context=context,
            model_name=model_name,
        )
        out = result.to_dict()

    if out.get("status") != StageStatus.SUCCESS.value:
        raise AssertionError(f"expected SUCCESS after interactive resume, got={out}")

    summary = result.state.summary()  # type: ignore[attr-defined]
    print("ok: pipeline e2e success", {"model_name": model_name, "state_summary": summary})
