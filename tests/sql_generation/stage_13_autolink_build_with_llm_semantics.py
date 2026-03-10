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

    from config.llm_config import get_llm
    from stages.sql_generation.autolink import run_autolink

    model_name = get_model_name()
    model = get_llm(model_name)

    out = run_autolink(
        {
            "request": (
                "构建最小可用 schema 用于生成 SQL：检查 equipment.serial_number 是否唯一，并给出少量样本值。"
                "在列类型/索引/样本值等证据就绪后，请补齐 serial_number 的 description。"
            ),
            "request_type": "BUILD",
            "schema": {},
            "context": {"database_scope": ["industrial_monitoring"], "sql_dialect": "MYSQL"},
        },
        model=model,
        max_rounds=6,
    )

    payload = out.model_dump(mode="json", by_alias=True)
    schema = payload.get("schema") or {}
    cols = (
        (((schema.get("databases") or {}).get("industrial_monitoring") or {}).get("tables") or {})
        .get("equipment", {})
        .get("columns", {})
    )
    serial = cols.get("serial_number") or {}
    if not str(serial.get("description") or "").strip():
        raise AssertionError(f"expected serial_number semantics to be filled by LLM, got={serial}")

    print(
        "ok: autolink BUILD with LLM semantics",
        {
            "model_name": model_name,
            "status": payload.get("status"),
            "serial_number.type": serial.get("type"),
            "serial_number.index": serial.get("index"),
            "serial_number.description": (serial.get("description") or "")[:80],
            "errors": payload.get("errors") or [],
        },
    )
