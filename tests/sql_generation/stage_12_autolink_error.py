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

    # Simulate a schema that contains a wrong/obsolete column name from an execution error.
    seed_schema = {
        "databases": {
            "industrial_monitoring": {
                "tables": {
                    "sensors": {
                        "columns": {
                            "sensor_id": {"type": "int"},
                            "sensor_name": {"type": "varchar"},  # nonexistent in provided DDL; common error case
                        }
                    }
                }
            }
        }
    }

    out = run_autolink(
        {
            "request": (
                "执行 SQL 报错：Unknown column 'sensor_name' in 'field list'。"
                "请围绕 sensors 表纠错并补齐正确列定义（最小够用）。"
            ),
            "request_type": "ERROR",
            "schema": seed_schema,
            "context": {"database_scope": ["industrial_monitoring"], "sql_dialect": "MYSQL"},
        },
        model=None,
        max_rounds=4,
    )

    payload = out.model_dump(mode="json", by_alias=True)
    schema = payload.get("schema") or {}
    assert_has_path(schema, "industrial_monitoring", "sensors", "sensor_code")
    assert_has_path(schema, "industrial_monitoring", "sensors", "equipment_id")

    sensors = schema["databases"]["industrial_monitoring"]["tables"]["sensors"]
    sensor_code_type = (sensors.get("columns") or {}).get("sensor_code", {}).get("type") or ""
    if not str(sensor_code_type).strip():
        raise AssertionError(f"expected sensors.sensor_code.type to be filled, got={sensor_code_type!r}")

    sensor_code_index = (sensors.get("columns") or {}).get("sensor_code", {}).get("index") or ""
    if "UNIQUE" not in str(sensor_code_index).upper():
        raise AssertionError(f"expected sensors.sensor_code.index to contain UNIQUE, got={sensor_code_index!r}")

    print(
        "ok: autolink ERROR repaired sensors metadata",
        {
            "status": payload.get("status"),
            "sensor_code.type": sensor_code_type,
            "sensor_code.index": sensor_code_index,
            "errors": payload.get("errors") or [],
        },
    )
