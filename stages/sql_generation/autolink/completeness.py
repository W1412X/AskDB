"""
Autolink completeness and invariants (runtime-enforced).

Design goal:
- Judge output is advisory (LLM/heuristic); runtime enforces hard invariants so the system
  cannot "stop successfully" without producing a usable schema artifact.
"""

from __future__ import annotations

from typing import List, Tuple

from stages.sql_generation.autolink.models import Schema

PLACEHOLDER_TYPES = {"", "UNKNOWN", "__UNKNOWN__", "N/A", "NA"}


def request_needs_samples(request: str) -> bool:
    req = (request or "").lower()
    return any(token in req for token in ("sample", "示例", "样本", "数据示例", "example data", "examples"))


def request_needs_data_exploration(request: str) -> bool:
    """
    Whether the user explicitly asks about data distribution/format/patterns.
    In these cases, it is reasonable to call sql_explore to collect sample values.
    """
    req = (request or "").lower()
    tokens = (
        "分布",
        "规律",
        "格式",
        "模式",
        "pattern",
        "regex",
        "正则",
        "取值范围",
        "范围",
        "枚举",
        "distinct",
        "unique values",
        "长度",
        "单位",
    )
    return any(t in req for t in tokens)


def has_any_tables(schema: Schema) -> bool:
    return any(db.tables for db in schema.databases.values())


def has_any_strong_column_type(schema: Schema) -> bool:
    for db in schema.databases.values():
        for t in db.tables.values():
            for c in t.columns.values():
                tpe = str(getattr(c, "type", "") or "").strip()
                if tpe and tpe.upper() not in PLACEHOLDER_TYPES:
                    return True
    return False


def has_any_sample_values(schema: Schema) -> bool:
    for db in schema.databases.values():
        for t in db.tables.values():
            for c in t.columns.values():
                if list(getattr(c, "sample_values", None) or []):
                    return True
    return False


def has_any_keys(schema: Schema) -> bool:
    for db in schema.databases.values():
        for t in db.tables.values():
            if list(getattr(t, "primary_key", None) or []):
                return True
            if list(getattr(t, "foreign_keys", None) or []):
                return True
    return False


def _request_mentions(name: str, request_lower: str) -> bool:
    n = (name or "").strip().lower()
    if not n:
        return False
    return n in request_lower


def _missing_required_descriptions(schema: Schema, request: str, *, max_columns: int = 32) -> List[str]:
    """
    Best-effort semantic requirements (description) for BUILD when an LLM is available.

    We avoid "blindly requiring every column" (can be huge). Instead, require:
    - table.description for all included tables
    - column.description for columns that are likely important for SQL correctness:
      * PK/FK columns
      * indexed columns
      * columns with sample_values
      * columns mentioned in the request
    """
    req = (request or "").lower()
    missing: List[str] = []

    # Table descriptions: required for all included tables.
    for db_name, db in schema.databases.items():
        for tb_name, tb in db.tables.items():
            if not str(getattr(tb, "description", "") or "").strip():
                missing.append(f"{db_name}.{tb_name}.description")

    # Column descriptions: require for a bounded set of important columns.
    candidates: List[Tuple[int, str]] = []
    for db_name, db in schema.databases.items():
        for tb_name, tb in db.tables.items():
            pk = set(tb.primary_key or [])
            fk_cols = set()
            for fk in tb.foreign_keys or []:
                col = str((fk or {}).get("column_name") or "").strip()
                if col:
                    fk_cols.add(col)

            for col_name, col in tb.columns.items():
                tpe = str(getattr(col, "type", "") or "").strip()
                if not tpe or tpe.upper() in PLACEHOLDER_TYPES:
                    continue
                if str(getattr(col, "description", "") or "").strip():
                    continue

                score = 0
                if col_name in pk:
                    score += 4
                if col_name in fk_cols:
                    score += 4
                if str(getattr(col, "index", "") or "").strip():
                    score += 3
                if list(getattr(col, "sample_values", None) or []):
                    score += 3
                if _request_mentions(col_name, req) or _request_mentions(tb_name, req):
                    score += 2
                if score <= 0:
                    continue
                candidates.append((score, f"{db_name}.{tb_name}.{col_name}.description"))

    candidates.sort(key=lambda x: (-x[0], x[1]))
    missing.extend([item for _score, item in candidates[:max_columns]])
    return missing


def check_build_invariants(schema: Schema, request: str, *, require_descriptions: bool = False) -> Tuple[bool, List[str]]:
    """
    Return (ok, missing_items).

    Hard invariants for BUILD:
    - Must have at least one table and one column (table skeleton alone is not sufficient).
    - Must have at least one strong (non-placeholder) column type.
    - If request asks for examples/samples, must have at least one sample_values entry.
    - If require_descriptions, table/important-column descriptions must be present (best-effort).
    """
    missing: List[str] = []
    if not has_any_tables(schema):
        missing.append("schema has no tables")
        return False, missing

    has_any_column = any(
        t.columns
        for db in schema.databases.values()
        for t in db.tables.values()
    )
    if not has_any_column:
        missing.append("schema has no columns")

    if not has_any_strong_column_type(schema):
        missing.append("no strong column types present")

    if request_needs_samples(request) and not has_any_sample_values(schema):
        missing.append("request needs sample_values but none present")

    if require_descriptions:
        missing.extend(_missing_required_descriptions(schema, request))

    return len(missing) == 0, missing
