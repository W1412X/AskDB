"""
Minimal DB tool wrappers for sql_generation.

Design goals:
- Strict select-only enforcement
- Small surface area (only what AutoLink + intent runtime needs)
- LangChain tool-compatible interface (.invoke)
"""

from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, List, Optional, Tuple

import sqlparse

try:
    from langchain.tools import tool
except ImportError:

    def tool(name_or_callable=None, description=""):
        def deco(fn):
            fn.name = name_or_callable if isinstance(name_or_callable, str) else fn.__name__
            fn.description = description or (fn.__doc__ or "")
            fn.invoke = lambda kwargs: fn(**kwargs)
            return fn

        return deco(name_or_callable) if callable(name_or_callable) else deco


def _ensure_select_only(sql: str) -> None:
    parsed = sqlparse.parse(sql or "")
    if not parsed:
        raise ValueError("empty sql")
    if len(parsed) != 1:
        raise ValueError("multi-statement sql is not allowed")
    first = parsed[0].token_first(skip_cm=True, skip_ws=True)
    if first is None:
        raise ValueError("invalid sql")
    if str(first).upper() not in {"SELECT", "WITH"}:
        raise ValueError("only SELECT/WITH is allowed")


def _inject_limit(sql: str, limit: int) -> str:
    upper = (sql or "").upper()
    if "LIMIT" in upper:
        return sql
    return (sql or "").rstrip().rstrip(";") + f" LIMIT {int(limit)}"


def _db() -> Any:
    # Backward-compatible alias for the shared global DB tool.
    from utils.database_tool import _db_tool

    if _db_tool is None:
        raise RuntimeError("global database tool is not initialized")
    return _db_tool


def _get_db_tool() -> Any:
    return _db()


def _exec(
    sql: str,
    database: Optional[str] = None,
    params: Optional[Tuple[Any, ...]] = None,
    *,
    readonly: bool = True,
    timeout_ms: int = 30000,
) -> List[Dict[str, Any]]:
    return _get_db_tool().execute_query(
        sql=sql,
        params=params,
        database=database,
        readonly=readonly,
        timeout_ms=timeout_ms,
    )


@tool(name_or_callable="list_databases", description="列出可访问的数据库实例/逻辑库")
def list_databases_tool() -> List[str]:
    return _get_db_tool().list_databases()


@tool(name_or_callable="list_tables", description="列出 schema 下的表（支持 pattern LIKE）")
def list_tables_tool(schema_name: str, pattern: Optional[str] = None) -> List[str]:
    tables = _get_db_tool().list_tables(schema_name)
    if not pattern:
        return tables
    p = pattern.replace("%", ".*").replace("_", ".")
    regex = re.compile(p, re.IGNORECASE)
    return [t for t in tables if regex.search(t)]


@tool(name_or_callable="describe_table", description="获取表的列名、类型、注释等结构信息")
def describe_table_tool(table: str, schema_name: Optional[str] = None) -> List[Dict[str, Any]]:
    if not table:
        return []
    db = schema_name or ""
    tb = table.split(".")[-1]
    sql = """
    SELECT
      COLUMN_NAME as column_name,
      DATA_TYPE as column_type,
      IFNULL(COLUMN_COMMENT,'') as column_comment
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
    ORDER BY ORDINAL_POSITION
    """
    return _exec(sql, database=db or None, params=(db, tb))


@tool(name_or_callable="get_table_comment", description="获取表注释")
def get_table_comment_tool(table: str, schema_name: Optional[str] = None) -> str:
    db = schema_name or ""
    tb = table.split(".")[-1]
    sql = """
    SELECT IFNULL(TABLE_COMMENT,'') as table_comment
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
    LIMIT 1
    """
    rows = _exec(sql, database=db or None, params=(db, tb))
    return str(rows[0].get("table_comment") or "") if rows else ""


@tool(name_or_callable="get_primary_key", description="获取表主键列名列表")
def get_primary_key_tool(table: str, schema_name: Optional[str] = None) -> List[str]:
    db = schema_name or ""
    tb = table.split(".")[-1]
    sql = """
    SELECT COLUMN_NAME as column_name
    FROM information_schema.KEY_COLUMN_USAGE
    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND CONSTRAINT_NAME = 'PRIMARY'
    ORDER BY ORDINAL_POSITION
    """
    rows = _exec(sql, database=db or None, params=(db, tb))
    return [str(r.get("column_name")) for r in rows if r.get("column_name")]


@tool(name_or_callable="get_foreign_keys", description="获取表外键信息")
def get_foreign_keys_tool(table: str, schema_name: Optional[str] = None) -> List[Dict[str, Any]]:
    db = schema_name or ""
    tb = table.split(".")[-1]
    sql = """
    SELECT
      CONSTRAINT_NAME as constraint_name,
      COLUMN_NAME as column_name,
      REFERENCED_TABLE_SCHEMA as referenced_table_schema,
      REFERENCED_TABLE_NAME as referenced_table_name,
      REFERENCED_COLUMN_NAME as referenced_column_name
    FROM information_schema.KEY_COLUMN_USAGE
    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
      AND REFERENCED_TABLE_NAME IS NOT NULL
    ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION
    """
    return _exec(sql, database=db or None, params=(db, tb))


@tool(name_or_callable="get_table_indexes", description="获取表索引信息（包含唯一索引/普通索引）")
def get_table_indexes_tool(table: str, schema_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Returns rows from information_schema.STATISTICS.
    Keys:
    - index_name
    - non_unique (0/1)
    - column_name
    - seq_in_index
    """
    db = schema_name or ""
    tb = table.split(".")[-1]
    sql = """
    SELECT
      INDEX_NAME as index_name,
      NON_UNIQUE as non_unique,
      COLUMN_NAME as column_name,
      SEQ_IN_INDEX as seq_in_index
    FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
    ORDER BY INDEX_NAME, SEQ_IN_INDEX
    """
    return _exec(sql, database=db or None, params=(db, tb))


@tool(name_or_callable="search_columns", description="按关键词搜索列名/列注释")
def search_columns_tool(keyword: str, schema_name: Optional[str] = None, topk: int = 20) -> List[Dict[str, Any]]:
    db = schema_name or ""
    kw = f"%{keyword}%"
    sql = """
    SELECT TABLE_NAME as table_name, COLUMN_NAME as column_name, IFNULL(COLUMN_COMMENT,'') as column_comment, DATA_TYPE as column_type
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = %s
      AND (COLUMN_NAME LIKE %s OR COLUMN_COMMENT LIKE %s)
    LIMIT %s
    """
    return _exec(sql, database=db or None, params=(db, kw, kw, int(topk)))


@tool(name_or_callable="search_tables", description="按关键词搜索表名/表注释")
def search_tables_tool(keyword: str, schema_name: Optional[str] = None, topk: int = 20) -> List[Dict[str, Any]]:
    db = schema_name or ""
    kw = f"%{keyword}%"
    sql = """
    SELECT TABLE_NAME as table_name, IFNULL(TABLE_COMMENT,'') as table_comment
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = %s
      AND (TABLE_NAME LIKE %s OR TABLE_COMMENT LIKE %s)
    LIMIT %s
    """
    return _exec(sql, database=db or None, params=(db, kw, kw, int(topk)))


@tool(name_or_callable="validate_sql_select_only", description="校验 SQL 是否为只读 SELECT/WITH 单语句")
def validate_sql_select_only_tool(sql: str) -> Dict[str, Any]:
    try:
        _ensure_select_only(sql)
        return {"ok": True, "error": ""}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@tool(name_or_callable="parse_sql", description="解析 SQL（轻量），返回类型与粗略结构")
def parse_sql_tool(sql: str) -> Dict[str, Any]:
    try:
        _ensure_select_only(sql)
        parsed = sqlparse.parse(sql)[0]
        tokens = [t.value for t in parsed.flatten() if t.value and t.value.strip()]
        return {"ok": True, "statement_type": "SELECT", "token_count": len(tokens), "error": ""}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@tool(name_or_callable="fingerprint_sql", description="SQL 指纹（sha256，基于规范化空白）")
def fingerprint_sql_tool(sql: str) -> Dict[str, Any]:
    normalized = " ".join((sql or "").split())
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    return {"ok": True, "fingerprint": f"sha256:{digest}"}


@tool(name_or_callable="dry_run", description="dry_run：通过 EXPLAIN 预编译/对象存在性检查（只读）")
def dry_run_tool(sql: str, database: Optional[str] = None) -> Dict[str, Any]:
    try:
        _ensure_select_only(sql)
        db = database or None
        _exec("EXPLAIN " + sql, database=db, readonly=True, timeout_ms=30000)
        return {"ok": True, "error": ""}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@tool(name_or_callable="execute_select_with_limit", description="执行只读 SELECT，并强制 LIMIT")
def execute_select_with_limit_tool(
    sql: str,
    *,
    limit: int = 100,
    timeout_ms: int = 30000,
    database: Optional[str] = None,
) -> List[Dict[str, Any]]:
    _ensure_select_only(sql)
    limit = max(1, min(int(limit), 1000))
    wrapped = _inject_limit(sql, limit)
    return _exec(
        wrapped,
        database=database or None,
        readonly=True,
        timeout_ms=max(1, int(timeout_ms)),
    )
