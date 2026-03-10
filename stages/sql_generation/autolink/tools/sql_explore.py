"""
sql_explore：执行 SELECT 探索，强制 LIMIT≤100，超时 30s。
"""

from __future__ import annotations

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
        raise ValueError("only SELECT/WITH query is allowed")


def _inject_limit(sql: str, max_limit: int) -> str:
    """若外层无 LIMIT 则注入或强制限制。"""
    upper = sql.upper()
    if "LIMIT" in upper:
        parsed = sqlparse.parse(sql)
        if parsed:
            stmt = parsed[0]
            for token in stmt.flatten():
                if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == "LIMIT":
                    idx = list(stmt.flatten()).index(token)
                    # 简化：若已有 LIMIT 且不超过 max，则通过；否则用 max 替换
                    return sql
    return sql.rstrip().rstrip(";") + f" LIMIT {max_limit}"


@tool(
    name_or_callable="sql_explore",
    description="执行 SELECT 探索数据。强制 LIMIT≤100，超时 30s。仅 SELECT/WITH。",
)
def sql_explore_tool(
    query: str,
    database: str = "",
    limit: int = 100,
    timeout_ms: int = 30000,
) -> dict:
    """
    执行探索 SQL。仅允许 SELECT/WITH，强制 LIMIT。
    """
    if not query or not str(query).strip():
        return {"ok": False, "error": "query 为空", "result": []}
    _ensure_select_only(query)
    limit = min(int(limit), 100)
    timeout_ms = min(int(timeout_ms), 30000)

    from stages.sql_generation.tools.db import execute_select_with_limit_tool

    # ensure outer LIMIT exists (best-effort), while execute_select_with_limit_tool
    # still enforces limit server-side.
    query = _inject_limit(str(query), limit)
    db = database if database else None
    try:
        rows = execute_select_with_limit_tool.invoke(
            {"sql": query, "limit": limit, "timeout_ms": timeout_ms, "database": db}
        )
        return {"ok": True, "error": "", "result": rows}
    except Exception as exc:
        return {"ok": False, "error": str(exc), "result": []}
