"""
schema_retrieval：语义检索表/列。优先 local，回退 db。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

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

from stages.sql_generation.autolink.schema_merge import schema_write_plan_from_column_records
from stages.sql_generation.autolink.initialize_catalog import (
    hydrate_column_record_from_initialize,
    load_table_overview,
)


def _search_local(text: str, databases: List[str], top_k: int) -> tuple[List[Dict[str, Any]], str]:
    try:
        from stages.initialize.embedding.query import get_columns_by_text

        cols = get_columns_by_text(text=text, databases=databases)
        cols = sorted(cols, key=lambda x: x.get("similarity", 0), reverse=True)[: int(top_k)]
        return cols, ""
    except Exception as exc:
        return [], f"{type(exc).__name__}:{str(exc)[:200]}"


def _extract_keywords(text: str) -> List[str]:
    """
    Best-effort keyword extraction for DB fallback search.
    Avoid passing the whole request sentence to LIKE %...% which has very low recall.
    """
    import re

    raw = (text or "").strip()
    if not raw:
        return []

    stop = {
        "查询", "给出", "需求", "对应", "一些", "数据", "示例", "样本", "要求", "schema", "表", "字段", "列", "以及",
        "需要", "用于", "验证", "唯一性", "设备", "信息", "请", "帮我", "我们", "你的",
    }

    tokens: List[str] = []
    # alnum tokens (ids, abbreviations)
    tokens.extend(re.findall(r"[A-Za-z0-9_\\-]{2,}", raw))
    # chinese chunks (2-6 chars) to improve recall
    tokens.extend(re.findall(r"[\\u4e00-\\u9fff]{2,6}", raw))

    cleaned: List[str] = []
    seen = set()
    for t in tokens:
        t = t.strip().lower()
        if not t or t in stop:
            continue
        if len(t) <= 1:
            continue
        if t not in seen:
            seen.add(t)
            cleaned.append(t)

    # Prefer earlier tokens; cap to keep cost bounded.
    return cleaned[:6]


def _search_db(keyword: str, schema_name: str, topk: int) -> List[Dict[str, Any]]:
    try:
        from stages.sql_generation.tools.db import search_columns_tool, search_tables_tool

        cols = search_columns_tool.invoke(
            {"keyword": keyword, "schema_name": schema_name, "topk": topk}
        )
        tables = search_tables_tool.invoke(
            {"keyword": keyword, "schema_name": schema_name, "topk": min(5, topk // 2)}
        )
        out: List[Dict[str, Any]] = []
        seen = set()
        for c in cols:
            tbl = c.get("table_name", "")
            col = c.get("column_name", "")
            if (tbl, col) not in seen:
                seen.add((tbl, col))
                out.append({
                    "database_name": schema_name,
                    "table_name": tbl,
                    "column_name": col,
                    "column_type": c.get("column_type", ""),
                    "column_comment": c.get("column_comment", ""),
                })
        for t in tables:
            tbl = t.get("table_name", "")
            if tbl and not any(x.get("table_name") == tbl for x in out):
                out.append({
                    "database_name": schema_name,
                    "table_name": tbl,
                    "column_name": "*",
                    "column_type": "",
                    "column_comment": t.get("table_comment", ""),
                })
        return out[:topk]
    except Exception:
        return []


def _analyze_local_results(
    cols: List[Dict[str, Any]],
    *,
    min_similarity: float,
    ambiguity_delta: float,
    top_n: int = 6,
) -> Tuple[bool, List[Dict[str, Any]], Dict[str, Any], str]:
    """
    Keep local embedding retrieval safe and predictable:
    - below threshold => treat as low confidence (do not write)
    - ambiguous same column_name across tables with similar scores => require user clarification (do not write)
    """
    cols = [c for c in cols if isinstance(c, dict)]
    cols = sorted(cols, key=lambda x: float(x.get("similarity", 0) or 0.0), reverse=True)
    diagnostics: Dict[str, Any] = {
        "min_similarity": float(min_similarity),
        "ambiguity_delta": float(ambiguity_delta),
        "top_local": [
            {
                "table": str(c.get("table_name") or ""),
                "column": str(c.get("column_name") or ""),
                "similarity": float(c.get("similarity", 0) or 0.0),
            }
            for c in cols[:5]
        ],
        "low_confidence": False,
        "ambiguous": False,
        "ambiguous_groups": [],
    }
    if not cols:
        return False, [], diagnostics, "no_local_results"

    top1 = float(cols[0].get("similarity", 0) or 0.0)
    if top1 < float(min_similarity):
        diagnostics["low_confidence"] = True
        return False, [], diagnostics, f"low_confidence:top1_similarity={top1:.4f}"

    # Ambiguity: same column_name appears in multiple tables with very close scores.
    sliced = cols[: max(1, min(int(top_n), len(cols)))]
    by_col: Dict[str, List[Dict[str, Any]]] = {}
    for c in sliced:
        col_name = str(c.get("column_name") or "").strip()
        if not col_name:
            continue
        by_col.setdefault(col_name, []).append(c)

    groups = []
    for col_name, items in by_col.items():
        tables = {str(i.get("table_name") or "").strip() for i in items if str(i.get("table_name") or "").strip()}
        if len(tables) < 2:
            continue
        items_sorted = sorted(items, key=lambda x: float(x.get("similarity", 0) or 0.0), reverse=True)
        if len(items_sorted) < 2:
            continue
        s1 = float(items_sorted[0].get("similarity", 0) or 0.0)
        s2 = float(items_sorted[1].get("similarity", 0) or 0.0)
        if s1 >= float(min_similarity) and s2 >= float(min_similarity) and (s1 - s2) <= float(ambiguity_delta):
            groups.append(
                {
                    "column_name": col_name,
                    "choices": [
                        {
                            "table": str(i.get("table_name") or ""),
                            "column": str(i.get("column_name") or ""),
                            "similarity": float(i.get("similarity", 0) or 0.0),
                        }
                        for i in items_sorted[:4]
                    ],
                }
            )

    if groups:
        diagnostics["ambiguous"] = True
        diagnostics["ambiguous_groups"] = groups[:3]
        return False, [], diagnostics, "ambiguous_local_results"

    return True, cols, diagnostics, ""


@tool(
    name_or_callable="schema_retrieval",
    description="语义检索与需求相关的表/列。至少填 table、column、description 之一。",
)
def schema_retrieval_tool(
    table: str = "",
    column: str = "",
    description: str = "",
    schema_name: Optional[str] = None,
    databases: Optional[List[str]] = None,
    top_k: int = 12,
    min_similarity: float = 0.35,
    ambiguity_delta: float = 0.02,
) -> Dict[str, Any]:
    """
    语义检索表/列。优先 local embedding，若无则回退 db search_columns/search_tables。
    """
    text = " ".join(filter(None, [description, column, table]))
    if not text.strip():
        return {"ok": False, "error": "至少填 table、column、description 之一", "columns": []}

    db_list = databases or ([schema_name] if schema_name else [])
    if not db_list:
        return {"ok": False, "error": "database_scope 为空", "columns": []}

    db_list = [str(d) for d in db_list if d]
    schema_name = schema_name or (db_list[0] if db_list else "")

    cols, local_error = _search_local(text=text, databases=db_list, top_k=top_k)
    diagnostics: Dict[str, Any] = {
        "mode": "local" if cols else "db_fallback",
        "local_error": local_error,
        "local_result_count": len(cols),
        "keywords": [],
        "db_result_count": 0,
    }
    if cols:
        ok_local, safe_cols, local_diag, local_err = _analyze_local_results(
            cols,
            min_similarity=float(min_similarity),
            ambiguity_delta=float(ambiguity_delta),
        )
        diagnostics["local_analysis"] = local_diag
        if not ok_local:
            diagnostics["mode"] = "local_rejected"
            diagnostics["local_reject_reason"] = local_err
            return {
                "ok": False,
                "error": "local retrieval is low-confidence or ambiguous; user clarification required",
                "retrieved_columns": [],
                "columns": [],
                "diagnostics": diagnostics,
                "evidence": [],
                "schema_write_plan": {"writes": [], "summary": "local retrieval rejected"},
            }
        cols = safe_cols
    if not cols:
        keywords = _extract_keywords(text)
        if not keywords:
            keywords = [text[:50]]
        diagnostics["keywords"] = keywords
        aggregated: List[Dict[str, Any]] = []
        seen_pairs = set()
        for kw in keywords:
            for item in _search_db(keyword=kw, schema_name=schema_name, topk=max(4, top_k // 2)):
                key = (item.get("table_name", ""), item.get("column_name", ""))
                if key in seen_pairs:
                    continue
                seen_pairs.add(key)
                aggregated.append(item)
                if len(aggregated) >= top_k:
                    break
            if len(aggregated) >= top_k:
                break
        cols = aggregated
        diagnostics["db_result_count"] = len(cols)

    # Best-effort local hydration: use initialize JSON to fill types/samples/semantic_summary,
    # and to expand table-only hits ("*") into real columns when possible.
    hydrated: List[Dict[str, Any]] = []
    remaining = int(top_k)
    for item in cols:
        if not isinstance(item, dict):
            continue
        table_name = str(item.get("table_name") or "").strip()
        column_name = str(item.get("column_name") or "").strip()
        if not table_name:
            continue

        if column_name == "*":
            overview = load_table_overview(schema_name, table_name)
            columns = list((overview or {}).get("columns") or [])
            # Keep expansion bounded; focus on likely-join columns first.
            columns = [str(c).strip() for c in columns if str(c).strip()]
            joinish = [c for c in columns if c.endswith("_id") or c in ("id", "name")]
            rest = [c for c in columns if c not in joinish]
            expanded = (joinish + rest)[: max(1, min(8, remaining))]
            for c in expanded:
                rec = {
                    "database_name": schema_name,
                    "table_name": table_name,
                    "column_name": c,
                }
                rec = hydrate_column_record_from_initialize(rec, schema_name=schema_name)
                hydrated.append(rec)
                remaining -= 1
                if remaining <= 0:
                    break
            if remaining <= 0:
                break
            continue

        item = hydrate_column_record_from_initialize(dict(item), schema_name=schema_name)
        hydrated.append(item)
        remaining -= 1
        if remaining <= 0:
            break

    if hydrated:
        cols = hydrated

    write_plan = schema_write_plan_from_column_records(cols, schema_name=schema_name, source="schema_retrieval")
    return {
        "ok": True,
        "retrieved_columns": cols,
        "columns": cols,
        "diagnostics": diagnostics,
        "evidence": [
            {
                "source": "initialize_json" if item.get("semantic_summary") or item.get("sample_values") or item.get("data_type") else "tool",
                "target": f"{item.get('database_name', '')}.{item.get('table_name', '')}.{item.get('column_name', '')}".strip("."),
                "field": "column",
                "value": {
                    "type": item.get("data_type") or item.get("column_type") or "",
                    "description": item.get("semantic_summary") or item.get("column_comment") or "",
                },
                "confidence": 0.9 if item.get("semantic_summary") else 0.7,
                "observed_at": "",
            }
            for item in cols
            if isinstance(item, dict)
        ],
        "schema_write_plan": write_plan.model_dump(mode="json"),
    }
