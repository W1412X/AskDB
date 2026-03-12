from __future__ import annotations

import re
from typing import Any, Dict, List, Optional


ALLOWED_HINT_KEYS = {
    "known_tables",
    "known_columns",
    "join_keys",
    "uniqueness_dimensions",
    "time_range",
    "keywords",
}


def normalize_hints(hints: Any) -> Dict[str, Any]:
    """
    Keep hints minimal, stable, and safe to merge/consume.
    - Drop unknown keys
    - Coerce list-like fields to list[str] and de-duplicate
    - Coerce time_range to str
    """
    if not isinstance(hints, dict):
        return {}
    out: Dict[str, Any] = {}
    for key in ALLOWED_HINT_KEYS:
        value = hints.get(key)
        if value is None:
            continue
        if key == "time_range":
            s = str(value).strip()
            if s:
                out[key] = s
            continue
        if isinstance(value, str):
            items = [value.strip()]
        elif isinstance(value, list):
            items = [str(x).strip() for x in value if str(x).strip()]
        else:
            # ignore complex/unexpected types (keep system simple & reliable)
            continue
        # de-dup while keeping order
        seen = set()
        deduped: List[str] = []
        for item in items:
            if item and item not in seen:
                seen.add(item)
                deduped.append(item)
        if deduped:
            out[key] = deduped
    return out


def merge_hints(base: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simple, predictable merge:
    - list fields: append + de-dup
    - time_range: overwrite only if incoming non-empty
    """
    base_n = normalize_hints(base)
    inc_n = normalize_hints(incoming)
    merged: Dict[str, Any] = dict(base_n)
    for key, value in inc_n.items():
        if key == "time_range":
            if str(value).strip():
                merged[key] = str(value).strip()
            continue
        if isinstance(value, list):
            prev = merged.get(key)
            prev_list = prev if isinstance(prev, list) else []
            seen = set()
            out: List[str] = []
            for item in list(prev_list) + list(value):
                s = str(item).strip()
                if s and s not in seen:
                    seen.add(s)
                    out.append(s)
            if out:
                merged[key] = out
        else:
            # should not happen after normalize_hints
            pass
    return merged


_COL_REF_RE = re.compile(r"[A-Za-z0-9_]+\.[A-Za-z0-9_]+")


def is_actionable_hints(hints: Any) -> bool:
    """
    Minimal gate to prevent 'resolved' without any usable clues.
    Keep it intentionally small and conservative.
    """
    if not isinstance(hints, dict):
        return False
    known_tables = hints.get("known_tables")
    if isinstance(known_tables, list) and any(str(x).strip() for x in known_tables):
        return True
    known_columns = hints.get("known_columns")
    if isinstance(known_columns, list) and any(str(x).strip() for x in known_columns):
        return True
    keywords = hints.get("keywords")
    if isinstance(keywords, list):
        joined = " ".join(str(x) for x in keywords if str(x).strip())
        if joined and _COL_REF_RE.search(joined):
            return True
    # Fallback: scan string-ish values for "table.column"
    for v in hints.values():
        if isinstance(v, str) and _COL_REF_RE.search(v):
            return True
        if isinstance(v, list):
            for item in v:
                if isinstance(item, str) and _COL_REF_RE.search(item):
                    return True
    return False


def default_next_ask(*, existing_ask: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Fixed, friendly follow-up when resolved is claimed but hints are empty/inactionable.
    Keep it stable (no model dependence).
    """
    existing = existing_ask if isinstance(existing_ask, dict) else {}
    return {
        "situation": existing.get("situation") or "目前仍缺少可执行的表/字段线索，无法继续生成可靠 SQL。",
        "request": "请至少补充 1 个可操作线索：表名（table）或字段名（table.column）。如涉及关联，请给出 join key。",
        "why_needed": existing.get("why_needed") or "没有明确表/字段时，系统只能猜测，容易生成可运行但语义错误的 SQL。",
        "examples": [
            "例如：表=equipment，字段=serial_number",
            "例如：设备名称字段=equipment_types.name，通过 equipment.type_id 关联",
        ],
        "constraints": [
            "优先给出精确字段名 table.column",
            "如有口径（按 tenant/factory 维度等）请说明",
        ],
    }
