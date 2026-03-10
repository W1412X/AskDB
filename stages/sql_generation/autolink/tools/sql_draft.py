"""
sql_draft：草稿 SQL 验证 schema 完备性。强制 LIMIT≤10，全流程最多 3 次。
"""

from __future__ import annotations

import sqlparse
import threading
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

# 计数器必须是“每个并发执行上下文”隔离的。
# 当前项目已改为单进程多线程并发 intent，因此这里使用 thread-local 计数，
# 避免不同 intent/请求之间互相影响（例如一个线程 reset 了另一个线程的计数）。
_tls = threading.local()
_SQL_DRAFT_MAX = 3


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


def get_sql_draft_count() -> int:
    return int(getattr(_tls, "sql_draft_count", 0) or 0)


def reset_sql_draft_count() -> None:
    _tls.sql_draft_count = 0


def increment_sql_draft_count() -> int:
    cur = int(getattr(_tls, "sql_draft_count", 0) or 0) + 1
    _tls.sql_draft_count = cur
    return cur


@tool(
    name_or_callable="sql_draft",
    description="草稿 SQL 验证 schema 完备性。强制 LIMIT≤10，全流程最多 3 次。",
)
def sql_draft_tool(
    query: str,
    database: str = "",
    limit: int = 10,
) -> dict:
    """
    先 dry_run，再执行。成功返回结果，失败返回错误。
    """
    cur = int(getattr(_tls, "sql_draft_count", 0) or 0)
    if cur >= _SQL_DRAFT_MAX:
        return {"ok": False, "error": f"sql_draft limit exceeded ({_SQL_DRAFT_MAX})", "result": None}

    if not query or not str(query).strip():
        return {"ok": False, "error": "query 为空", "result": None}

    _ensure_select_only(query)
    limit = min(int(limit), 10)

    try:
        from stages.sql_generation.tools.db import dry_run_tool, execute_select_with_limit_tool

        dry = dry_run_tool.invoke({"sql": query, "database": database or None})
        if not dry.get("ok"):
            return {"ok": False, "error": dry.get("error", "dry_run failed"), "result": None}

        _tls.sql_draft_count = cur + 1
        rows = execute_select_with_limit_tool.invoke(
            {"sql": query, "limit": limit, "database": database or None}
        )
        return {"ok": True, "result": rows, "error": ""}
    except Exception as e:
        return {"ok": False, "error": str(e), "result": None}
