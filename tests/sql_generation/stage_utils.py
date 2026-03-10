import os
import sys
from typing import Any, Dict, Iterable, List, Optional

from config.app_config import get_app_config

def add_src_to_path() -> str:
    """
    Match tests/intent_divide/divide.py style: make src importable when running as a script.
    Returns the resolved src dir path.
    """
    src_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)
    return src_dir


DEFAULT_DB = get_app_config().get_default_database_name()
REQUIRED_TABLES = [
    "equipment",
    "equipment_types",
    "factories",
    "maintenance_records",
    "sensors",
    "sensor_readings",
]


def get_model_name(default: Optional[str] = None) -> str:
    """
    Model alias used by config.llm_config.get_llm().
    Override with env MODEL_NAME if needed.
    """
    return str(os.getenv("MODEL_NAME") or default or get_app_config().stages.sql_generation.pipeline.model_name)


def assert_has_path(schema_dict: Dict[str, Any], db: str, table: str, column: Optional[str] = None) -> None:
    dbs = schema_dict.get("databases") or {}
    if db not in dbs:
        raise AssertionError(f"missing database in schema: {db!r}")
    tables = (dbs.get(db) or {}).get("tables") or {}
    if table not in tables:
        raise AssertionError(f"missing table in schema: {db}.{table}")
    if column is not None:
        cols = (tables.get(table) or {}).get("columns") or {}
        if column not in cols:
            raise AssertionError(f"missing column in schema: {db}.{table}.{column}")


def ensure_db_smoke(*, database: str = DEFAULT_DB, required_tables: Optional[Iterable[str]] = None) -> None:
    """
    DB smoke check via sql_generation DB tools.
    This is intentionally lightweight and deterministic.
    """
    from stages.sql_generation.tools.db import list_tables_tool

    tables = list_tables_tool.invoke({"schema_name": database})
    table_set = set(str(t) for t in (tables or []))
    required = list(required_tables or REQUIRED_TABLES)
    missing = [t for t in required if t not in table_set]
    if missing:
        raise AssertionError(f"DB schema missing tables in {database}: {missing}. got={sorted(table_set)[:50]}")
