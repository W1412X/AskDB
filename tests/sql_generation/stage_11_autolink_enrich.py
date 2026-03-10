from __future__ import annotations

import os
import sys

_this_dir = os.path.dirname(os.path.abspath(__file__))
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)

from stage_utils import add_src_to_path, assert_has_path, ensure_db_smoke


if __name__ == "__main__":
    add_src_to_path()
    ensure_db_smoke(database="industrial_monitoring")

    from stages.sql_generation.autolink import run_autolink

    # Seed a minimal non-empty schema (ENRICH requires non-empty schema).
    seed_schema = {
        "databases": {
            "industrial_monitoring": {
                "tables": {
                    "equipment": {
                        "columns": {"equipment_id": {"type": ""}},
                        "description": "",
                    }
                }
            }
        }
    }

    out = run_autolink(
        {
            "request": "请补全 equipment 表的列类型、列注释与主键/外键等关键信息（只需要最小够用）。",
            "request_type": "ENRICH",
            "schema": seed_schema,
            "context": {"database_scope": ["industrial_monitoring"], "sql_dialect": "MYSQL"},
        },
        model=None,
        max_rounds=4,
    )

    payload = out.model_dump(mode="json", by_alias=True)
    schema = payload.get("schema") or {}
    assert_has_path(schema, "industrial_monitoring", "equipment", "equipment_id")
    assert_has_path(schema, "industrial_monitoring", "equipment", "serial_number")

    equipment = schema["databases"]["industrial_monitoring"]["tables"]["equipment"]
    serial_type = (equipment.get("columns") or {}).get("serial_number", {}).get("type") or ""
    if not str(serial_type).strip():
        raise AssertionError(f"expected equipment.serial_number.type to be filled, got={serial_type!r}")

    serial_index = (equipment.get("columns") or {}).get("serial_number", {}).get("index") or ""
    if "UNIQUE" not in str(serial_index).upper():
        raise AssertionError(f"expected equipment.serial_number.index to contain UNIQUE, got={serial_index!r}")

    print(
        "ok: autolink ENRICH filled metadata",
        {
            "status": payload.get("status"),
            "serial_number.type": serial_type,
            "serial_number.index": serial_index,
            "errors": payload.get("errors") or [],
        },
    )
