from __future__ import annotations

import os
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from stage_utils import add_src_to_path, ensure_db_smoke
from config.llm_config import get_llm

if __name__ == "__main__":
    add_src_to_path()
    ensure_db_smoke(database="industrial_monitoring")

    from stages.sql_generation.autolink import run_autolink

    out = run_autolink(
        {
            "request": "给出查询“每个工厂所有设备的最新维护时间以及维护人名称以及对应的工厂名称”的schema",
            "request_type": "BUILD",
            "schema": {},
            "context": {"database_scope": ["industrial_monitoring"], "sql_dialect": "MYSQL"},
        },
        model=get_llm("qwen3-max"),  # rely on fallback planner + DB tools (deterministic)
        max_rounds=6,
    )

    payload = out.model_dump(mode="json", by_alias=True)
    schema = payload.get("schema") or {}
    print(schema)
    print(
        "ok: autolink BUILD produced schema",
        {
            "status": payload.get("status"),
            "table_count": sum(len((db.get("tables") or {})) for db in (schema.get("databases") or {}).values() if isinstance(db, dict)),
            "errors": payload.get("errors") or [],
        },
    )
