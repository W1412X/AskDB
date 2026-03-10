"""
schema_meta：获取表结构、DDL、主外键（请求级缓存）。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

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

from stages.sql_generation.autolink.schema_merge import schema_write_plan_from_table_metadata
from stages.sql_generation.autolink.initialize_catalog import (
    index_string,
    load_table_column_metas,
    parse_foreign_key_ref,
    pick_sample_values,
)
from stages.sql_generation.autolink.models import (
    SchemaFieldLevel,
    SchemaFieldTarget,
    SchemaWrite,
    SchemaWritePlan,
    ValueSource,
    ValueSourceType,
    WriteOperation,
    WritePolicy,
)


def _schema_write_plan_from_initialize(schema_name: str, table_name: str) -> SchemaWritePlan:
    """
    Local-first metadata plan using initialize JSON (types, indexes, samples, PK/FK, semantic_summary).
    Does not write table/database description.
    """
    metas = load_table_column_metas(schema_name, table_name)
    if not metas:
        return SchemaWritePlan()

    writes: List[Dict[str, Any]] = []

    pk_cols: List[str] = []
    fk_rows: List[Dict[str, Any]] = []
    for col_name, meta in metas.items():
        if meta.is_primary_key and col_name not in pk_cols:
            pk_cols.append(col_name)
        fk = parse_foreign_key_ref(meta.foreign_key_ref)
        if fk:
            ref_table, ref_col = fk
            fk_rows.append(
                {
                    "constraint_name": f"init_fk_{table_name}_{col_name}",
                    "column_name": col_name,
                    "referenced_table_schema": schema_name,
                    "referenced_table_name": ref_table,
                    "referenced_column_name": ref_col,
                }
            )

        if meta.data_type:
            writes.append(
                SchemaWrite(
                    target=SchemaFieldTarget(
                        level=SchemaFieldLevel.COLUMN,
                        database=schema_name,
                        table=table_name,
                        column=col_name,
                        field="type",
                    ),
                    operation=WriteOperation.SET,
                    value=meta.data_type,
                    value_source=ValueSource(source_type=ValueSourceType.INITIALIZE_JSON, source_name="initialize_json", confidence=0.95),
                    write_policy=WritePolicy(allow_overwrite=True, require_target_exists=False),
                    reason="initialize column type",
                ).model_dump(mode="json")
            )

        idx = index_string(meta)
        if idx:
            writes.append(
                SchemaWrite(
                    target=SchemaFieldTarget(
                        level=SchemaFieldLevel.COLUMN,
                        database=schema_name,
                        table=table_name,
                        column=col_name,
                        field="index",
                    ),
                    operation=WriteOperation.SET,
                    value=idx,
                    value_source=ValueSource(source_type=ValueSourceType.INITIALIZE_JSON, source_name="initialize_json", confidence=0.95),
                    write_policy=WritePolicy(allow_overwrite=True, require_target_exists=False),
                    reason="initialize index",
                ).model_dump(mode="json")
            )

        samples = pick_sample_values(meta)
        if samples:
            writes.append(
                SchemaWrite(
                    target=SchemaFieldTarget(
                        level=SchemaFieldLevel.COLUMN,
                        database=schema_name,
                        table=table_name,
                        column=col_name,
                        field="sample_values",
                    ),
                    operation=WriteOperation.APPEND_UNIQUE,
                    value=samples[:8],
                    value_source=ValueSource(source_type=ValueSourceType.INITIALIZE_JSON, source_name="initialize_json", confidence=0.9),
                    write_policy=WritePolicy(allow_overwrite=False, require_target_exists=False),
                    reason="initialize sample values",
                ).model_dump(mode="json")
            )

        desc = (meta.semantic_summary or meta.comment or "").strip()
        if desc:
            writes.append(
                SchemaWrite(
                    target=SchemaFieldTarget(
                        level=SchemaFieldLevel.COLUMN,
                        database=schema_name,
                        table=table_name,
                        column=col_name,
                        field="description",
                    ),
                    operation=WriteOperation.REPLACE_IF_BETTER,
                    value=desc,
                    value_source=ValueSource(source_type=ValueSourceType.INITIALIZE_JSON, source_name="initialize_json", confidence=0.85),
                    write_policy=WritePolicy(allow_overwrite=False, require_target_exists=False),
                    reason="initialize semantic summary",
                ).model_dump(mode="json")
            )

    if pk_cols:
        writes.append(
            SchemaWrite(
                target=SchemaFieldTarget(
                    level=SchemaFieldLevel.TABLE,
                    database=schema_name,
                    table=table_name,
                    field="primary_key",
                ),
                operation=WriteOperation.SET,
                value=pk_cols,
                value_source=ValueSource(source_type=ValueSourceType.INITIALIZE_JSON, source_name="initialize_json", confidence=0.95),
                write_policy=WritePolicy(allow_overwrite=True, require_target_exists=False),
                reason="initialize primary key",
            ).model_dump(mode="json")
        )

    if fk_rows:
        writes.append(
            SchemaWrite(
                target=SchemaFieldTarget(
                    level=SchemaFieldLevel.TABLE,
                    database=schema_name,
                    table=table_name,
                    field="foreign_keys",
                ),
                operation=WriteOperation.SET,
                value=fk_rows,
                value_source=ValueSource(source_type=ValueSourceType.INITIALIZE_JSON, source_name="initialize_json", confidence=0.95),
                write_policy=WritePolicy(allow_overwrite=True, require_target_exists=False),
                reason="initialize foreign keys",
            ).model_dump(mode="json")
        )

    return SchemaWritePlan.model_validate({"writes": writes, "summary": "write plan from initialize json"})

def _get_describe(schema_name: str, table: str) -> List[Dict[str, Any]]:
    from stages.sql_generation.tools.db import describe_table_tool

    return describe_table_tool.invoke({"table": table, "schema_name": schema_name})


def _get_primary_key(schema_name: str, table: str) -> List[str]:
    from stages.sql_generation.tools.db import get_primary_key_tool

    return get_primary_key_tool.invoke({"table": table, "schema_name": schema_name})


def _get_foreign_keys(schema_name: str, table: str) -> List[Dict[str, Any]]:
    from stages.sql_generation.tools.db import get_foreign_keys_tool

    return get_foreign_keys_tool.invoke({"table": table, "schema_name": schema_name})


def _get_table_comment(schema_name: str, table: str) -> str:
    from stages.sql_generation.tools.db import get_table_comment_tool

    return str(get_table_comment_tool.invoke({"table": table, "schema_name": schema_name}) or "")

def _get_table_indexes(schema_name: str, table: str) -> List[Dict[str, Any]]:
    from stages.sql_generation.tools.db import get_table_indexes_tool

    return get_table_indexes_tool.invoke({"table": table, "schema_name": schema_name})


@tool(
    name_or_callable="schema_meta",
    description="获取表结构、DDL、主外键。schema_name 必填；支持 table 或 tables 批量。",
)
def schema_meta_tool(
    schema_name: str,
    table: str = "",
    tables: Optional[List[str]] = None,
    include_ddl: bool = False,
    include_keys: bool = True,
) -> Dict[str, Any]:
    """
    获取表元数据，返回 SchemaWritePlan 格式。
    """
    if not schema_name or not str(schema_name).strip():
        return {"ok": False, "error": "schema_name 必填", "schema_write_plan": None}

    schema_name = str(schema_name).strip()
    normalized_tables: List[str] = []
    if tables:
        normalized_tables.extend([str(t).strip() for t in tables if str(t).strip()])
    if str(table or "").strip():
        normalized_tables.append(str(table).strip())
    normalized_tables = list(dict.fromkeys(normalized_tables))[:8]
    if not normalized_tables:
        return {"ok": False, "error": "table/tables 至少填一个", "schema_write_plan": None}

    combined_writes: List[Dict[str, Any]] = []
    errors: List[str] = []
    for tb in normalized_tables:
        try:
            # Local initialize json first: fast + avoids DB calls.
            init_plan = _schema_write_plan_from_initialize(schema_name, tb)
            if init_plan.writes:
                combined_writes.extend(init_plan.model_dump(mode="json").get("writes", []))
                # If DDL explicitly requested, we still need DB; otherwise skip DB fallback.
                if not include_ddl and include_keys:
                    continue

            cols = _get_describe(schema_name, tb)
            if not cols:
                errors.append(f"表 {schema_name}.{tb} 不存在或不可访问")
                continue

            primary_key: Optional[List[str]] = None
            foreign_keys: Optional[List[Dict[str, Any]]] = None
            table_comment = ""
            try:
                table_comment = _get_table_comment(schema_name, tb)
            except Exception:
                table_comment = ""
            if include_keys:
                try:
                    primary_key = _get_primary_key(schema_name, tb)
                except Exception:
                    pass
                try:
                    foreign_keys = _get_foreign_keys(schema_name, tb)
                except Exception:
                    pass
                if foreign_keys is not None and not list(foreign_keys or []):
                    # Treat empty as "unknown/no-op" to avoid overwriting existing FK info with [].
                    foreign_keys = None
            indexes: Optional[List[Dict[str, Any]]] = None
            try:
                indexes = _get_table_indexes(schema_name, tb)
            except Exception:
                indexes = None

            write_plan = schema_write_plan_from_table_metadata(
                schema_name=schema_name,
                table_name=tb,
                columns=cols,
                table_comment=table_comment,
                primary_key=primary_key if primary_key else None,
                foreign_keys=foreign_keys,
                indexes=indexes,
                source="schema_meta",
            )
            combined_writes.extend(write_plan.model_dump(mode="json").get("writes", []))
        except Exception as exc:
            errors.append(str(exc))
            continue

    if not combined_writes:
        return {"ok": False, "error": errors[0] if errors else "schema_meta no results", "schema_write_plan": None}
    return {
        "ok": True,
        "error": "",
        "metadata_evidence": [
            {
                "source": "db_metadata",
                "target": f"{schema_name}.{tb}",
                "field": "table_metadata",
                "value": "loaded",
                "confidence": 1.0,
                "observed_at": "",
            }
            for tb in normalized_tables
        ],
        "schema_write_plan": {"writes": combined_writes, "summary": "combined schema_meta write plan"},
    }
