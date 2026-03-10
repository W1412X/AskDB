from __future__ import annotations

import os
import sys

src_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, src_dir)

from stages.sql_generation.autolink.schema_merge import schema_write_plan_from_table_metadata


def test_schema_write_plan_from_table_metadata_emits_index_writes() -> None:
    plan = schema_write_plan_from_table_metadata(
        schema_name="industrial_monitoring",
        table_name="equipment",
        columns=[
            {"column_name": "equipment_id", "column_type": "int", "column_comment": ""},
            {"column_name": "serial_number", "column_type": "varchar", "column_comment": ""},
            {"column_name": "factory_id", "column_type": "int", "column_comment": ""},
        ],
        table_comment="",
        primary_key=["equipment_id"],
        foreign_keys=None,
        indexes=[
            {"index_name": "PRIMARY", "non_unique": 0, "column_name": "equipment_id", "seq_in_index": 1},
            {"index_name": "serial_number", "non_unique": 0, "column_name": "serial_number", "seq_in_index": 1},
            {"index_name": "idx_factory", "non_unique": 1, "column_name": "factory_id", "seq_in_index": 1},
        ],
        source="schema_meta",
    )
    index_writes = [w for w in plan.writes if w.target.field == "index"]
    assert index_writes, "expected index writes"
    by_col = {w.target.column: w.value for w in index_writes}
    assert by_col.get("equipment_id") == "PRIMARY"
    assert str(by_col.get("serial_number") or "").startswith("UNIQUE(")
    assert str(by_col.get("factory_id") or "").startswith("INDEX(")


if __name__ == "__main__":
    test_schema_write_plan_from_table_metadata_emits_index_writes()
    print("ok: unit_schema_merge_indexes")

