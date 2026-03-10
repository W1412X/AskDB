from __future__ import annotations

import json
import os
from typing import Any, Dict

from pydantic import BaseModel


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_jsonable(v) for v in value]
    if isinstance(value, tuple):
        return [_to_jsonable(v) for v in value]
    return value


def _log_max_chars(default: int = 0) -> int:
    """
    日志预览长度上限（字符数）。
    - 0 或负数：不截断（输出完整 JSON）
    - 正数：截断到该字符数
    """
    raw = os.getenv("SQL_GENERATION_LOG_MAX_CHARS")
    if raw is None:
        return int(default)
    try:
        return int(str(raw).strip())
    except Exception:
        return int(default)


def compact_json(value: Any, max_chars: int = 0) -> str:
    try:
        text = json.dumps(_to_jsonable(value), ensure_ascii=False, separators=(",", ":"), default=str)
    except Exception:
        text = str(value)
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    return text[:max_chars] + "...[已截断]"


def schema_summary(schema: Any) -> Dict[str, Any]:
    payload = _to_jsonable(schema)
    databases = payload.get("databases", {}) if isinstance(payload, dict) else {}
    table_count = 0
    column_count = 0
    sampled_columns = 0
    described_columns = 0
    for db_info in databases.values():
        tables = db_info.get("tables", {}) if isinstance(db_info, dict) else {}
        table_count += len(tables)
        for table_info in tables.values():
            columns = table_info.get("columns", {}) if isinstance(table_info, dict) else {}
            column_count += len(columns)
            for column in columns.values():
                if not isinstance(column, dict):
                    continue
                if column.get("sample_values"):
                    sampled_columns += 1
                if (column.get("description") or "").strip() or (column.get("type") or "").strip():
                    described_columns += 1
    return {
        "database_count": len(databases),
        "table_count": table_count,
        "column_count": column_count,
        "sampled_columns": sampled_columns,
        "typed_or_described_columns": described_columns,
    }


def log_step_input(logger: Any, stage_label: str, payload: Any, **kwargs: Any) -> None:
    """详细日志，仅写文件不输出到控制台。"""
    preview = compact_json(payload, max_chars=_log_max_chars(default=0))
    logger.info(
        f"{stage_label} 输入 | {preview}",
        extra={"detail": True},
        stage_label=stage_label,
        direction="input",
        payload=_to_jsonable(payload),
        **kwargs,
    )


def log_step_output(logger: Any, stage_label: str, payload: Any, **kwargs: Any) -> None:
    """详细日志，仅写文件不输出到控制台。"""
    preview = compact_json(payload, max_chars=_log_max_chars(default=0))
    logger.info(
        f"{stage_label} 输出 | {preview}",
        extra={"detail": True},
        stage_label=stage_label,
        direction="output",
        payload=_to_jsonable(payload),
        **kwargs,
    )
