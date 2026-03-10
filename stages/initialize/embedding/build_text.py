"""
Build retrieval text for column metadata.

Keep this logic centralized to avoid duplication between scripts.
"""

from __future__ import annotations

from typing import Any, Dict, Union
import json
import re
from pathlib import Path


def build_semantic_description(
    col_meta: Dict[str, Any],
    *,
    include_keywords: bool = True,
    include_samples: bool = True,
    sample_prefer_distinct: bool = True,
    max_sample_values: int = 6,
    include_relations: bool = True,
) -> str:
    """
    从“列描述 json”（ColumnDescription 的 dict）组合生成一段语义描述文本，尽量少噪声
    """
    table = str(col_meta.get("table_name", "")).strip()
    column = str(col_meta.get("column_name", "")).strip()

    semantic = (col_meta.get("semantic_summary") or "").strip() or "无语义描述"
    keywords = col_meta.get("semantic_keywords") or []
    keywords = [str(k).strip() for k in keywords if str(k).strip()]
    keyword_text = "、".join(keywords)

    parts: list[str] = []

    # 主体：table.column + semantic_summary
    if table and column:
        parts.append(f"{table}.{column} {semantic}")
    elif column:
        parts.append(f"{column} {semantic}")
    else:
        parts.append(semantic)
    # 关键词：用于召回（可选）
    if include_keywords and keyword_text:
        parts.append(f"关键词 {keyword_text}")
    # 关联/主键：仅在存在时输出（可选）
    if include_relations:
        rel_parts: list[str] = []
        if col_meta.get("is_primary_key"):
            rel_parts.append("主键")
        fk_ref = col_meta.get("foreign_key_ref")
        if col_meta.get("is_foreign_key") and fk_ref:
            rel_parts.append(f"外键 {fk_ref}")
        if rel_parts:
            parts.append("；".join(rel_parts))
    # 样本信息
    if include_samples:
        samples = col_meta.get("samples") or {}
        if isinstance(samples, dict):
            rand = samples.get("random_samples") or []
            dist = samples.get("distinct_samples") or []

            def _pick_values(items: Any, limit: int) -> list[str]:
                if not isinstance(items, list):
                    return []
                out: list[str] = []
                for it in items:
                    if isinstance(it, dict):
                        v = str(it.get("sample_value", "")).strip()
                    else:
                        v = str(it).strip()
                    if v:
                        # drop obvious placeholders
                        if v in ("***[sensitive]***",):
                            continue
                        out.append(v)
                    if len(out) >= limit:
                        break
                return out
            rand_vals = _pick_values(rand, max_sample_values)
            dist_vals = _pick_values(dist, max_sample_values)
            values = dist_vals if (sample_prefer_distinct and dist_vals) else (rand_vals or dist_vals)
            if values:
                parts.append(f"示例 { '、'.join(values) }")
    # 合并为一行（低噪声、无Markdown）
    text = "；".join([p for p in parts if p])
    text = re.sub(r"\s+", " ", text).strip()
    print(text)
    return text


def build_semantic_description_from_json_file(
    json_path: Union[str, Path],
    **kwargs: Any,
) -> str:
    """从列描述文件（json）读取并生成语义描述。"""
    path = Path(json_path)
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Invalid column description json: {path}")
    return build_semantic_description(data, **kwargs)
