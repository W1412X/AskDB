"""
SemanticEnricher: fill lightweight column semantics after structural evidence is ready.

Design:
- Only writes to existing targets (no schema expansion).
- Only fills empty fields (only_if_empty=true).
- Uses LLM weak semantic source and keeps scope minimal but useful (best-effort descriptions for selected tables/columns).
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from utils.logger import get_logger

from stages.sql_generation.autolink.llm_utils import invoke_llm_with_format_retry
from stages.sql_generation.autolink.middleware import parse_schema_write_plan_output
from stages.sql_generation.autolink.models import Schema, SchemaWritePlan

logger = get_logger("autolink")


SCHEMA_SEMANTIC_ENRICHER_PROMPT = """
你是 SemanticEnricher。你的职责是在 schema 结构信息已经就绪后，补齐少量“弱语义”字段以帮助后续生成正确 SQL。

重要：你的输出会被严格校验（必须是 1 个纯 JSON 对象；禁止多余字段；枚举值必须严格命中允许集合；字段类型必须正确）。
不合规会被打回并要求按错误提示重写。

输入 JSON：
{
  "request": "...",
  "schema_snapshot": {
    "database": "db",
    "table": "t",
    "columns": [
      {"column": "c", "type": "varchar", "index": "UNIQUE(x)", "sample_values": ["..."], "description": ""}
    ]
  }
}

规则：
1) 只允许对输入 snapshot 里的列写入，不要新增表/列。
2) 只允许写入字段：column.description。
3) 只在目标字段为空时写入（only_if_empty=true）。
4) “最小完备”不是盲目最小：description 的目标是**让后续 SQL 生成能满足用户需求**。
   - description 应尽量包含“字段含义 + 与 SQL 相关的口径/约束（例如是否唯一、是否时间、单位/枚举含义等）”
   - 如需要分布信息，请用一句话嵌入 description 中（不要单独建字段）
5) 输出必须是严格 JSON（禁止 markdown/解释文本）。

输出严格 JSON（SchemaWritePlan）：
{
  "writes": [
    {
      "target": {"level":"column","database":"db","table":"t","column":"c","field":"description"},
      "operation": "set",
      "value": "....",
      "value_source": {"source_type":"llm_weak_semantic","source_name":"SemanticEnricher","source_ref":"","confidence":0.6},
      "write_policy": {"only_if_empty": true, "allow_overwrite": false, "require_target_exists": true},
      "reason": "weak semantic description"
    }
  ],
  "summary": "..."
}
"""


def _extract_candidate_tables(
    schema: Schema,
    request: str,
    *,
    max_tables: int = 4,
    max_columns_per_table: int = 18,
) -> List[Tuple[str, str, Dict[str, Any]]]:
    """
    Return list of (db, table, snapshot_dict) for tables that are structurally ready
    but lack descriptions. Scoring prefers request-mentioned tables/columns and those with evidence.
    """
    req = (request or "").lower()
    scored: List[Tuple[int, str, str, Dict[str, Any]]] = []
    for db_name, db_info in schema.databases.items():
        for tb_name, tb_info in db_info.tables.items():
            cols_snapshot: List[Dict[str, Any]] = []
            table_score = 0

            # Table relevance signals
            if tb_name and tb_name.lower() in req:
                table_score += 2

            for col_name, col in tb_info.columns.items():
                col_d = col.model_dump(mode="json") if hasattr(col, "model_dump") else dict(col or {})
                desc = str(col_d.get("description") or "").strip()
                if desc:
                    continue
                tpe = str(col_d.get("type") or "").strip()
                if not tpe:
                    continue
                score = 0
                # Evidence signals
                if str(col_d.get("index") or "").strip():
                    score += 2
                if list(col_d.get("sample_values") or []):
                    score += 2
                # Request mention
                if col_name and col_name.lower() in req:
                    score += 3
                cols_snapshot.append(
                    {
                        "score": score,
                        "column": col_name,
                        "type": str(col_d.get("type") or ""),
                        "index": str(col_d.get("index") or ""),
                        "sample_values": list(col_d.get("sample_values") or [])[:5],
                        "description": str(col_d.get("description") or ""),
                    }
                )

            if not cols_snapshot:
                continue

            # Prefer enriching tables that have any evidence-bearing columns.
            if any(c.get("score", 0) >= 2 for c in cols_snapshot):
                table_score += 2

            cols_snapshot.sort(key=lambda d: (-int(d.get("score", 0)), str(d.get("column") or "")))
            compact_cols = [
                {k: v for k, v in c.items() if k != "score"} for c in cols_snapshot[:max_columns_per_table]
            ]

            scored.append(
                (
                    table_score + sum(int(c.get("score", 0)) for c in cols_snapshot[:6]),
                    str(db_name),
                    str(tb_name),
                    {
                        "database": str(db_name),
                        "table": str(tb_name),
                        "columns": compact_cols,
                    },
                )
            )

    scored.sort(key=lambda x: (-x[0], x[1], x[2]))
    return [(db, tb, snap) for _score, db, tb, snap in scored[:max_tables]]


def run_semantic_enricher(
    *,
    model: Any,
    request: str,
    schema: Schema,
) -> SchemaWritePlan:
    candidates = _extract_candidate_tables(schema, request)
    if not candidates:
        return SchemaWritePlan()

    # Fill one table per call (simpler + more predictable).
    db, tb, snapshot = candidates[0]
    payload = {"request": request, "schema_snapshot": snapshot}
    logger.info(
        "语义丰富器调用",
        table=f"{db}.{tb}",
        candidate_count=len((snapshot or {}).get("columns") or []),
    )
    return invoke_llm_with_format_retry(
        model,
        SCHEMA_SEMANTIC_ENRICHER_PROMPT,
        json.dumps(payload, ensure_ascii=False),
        parse_schema_write_plan_output,
        max_retries=3,
    )
