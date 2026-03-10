"""
统一 schema 写入计划应用与最小化剪枝逻辑。
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict

from stages.sql_generation.autolink.models import (
    DatabaseInfo,
    SchemaFieldLevel,
    SchemaWritePlan,
    SchemaWrite,
    ColumnInfo,
    Schema,
    TableInfo,
    ValueSource,
    ValueSourceType,
    WriteOperation,
    WritePolicy,
    SchemaFieldTarget,
)


_SOURCE_PRIORITY = {
    ValueSourceType.INITIALIZE_JSON: 400,
    ValueSourceType.DB_METADATA: 300,
    ValueSourceType.DB_SAMPLE: 200,
    ValueSourceType.TOOL: 180,
    ValueSourceType.LLM_WEAK_SEMANTIC: 100,
    ValueSourceType.RUNTIME_GUARD: 50,
}


def _value_source_to_dict(source: Any) -> Dict[str, Any]:
    if isinstance(source, ValueSource):
        return source.model_dump(mode="json")
    if isinstance(source, dict):
        return dict(source)
    return {}


def _source_rank(source: Any) -> tuple[int, float, str]:
    payload = _value_source_to_dict(source)
    source_type_raw = payload.get("source_type")
    try:
        source_type = ValueSourceType(str(source_type_raw))
    except Exception:
        source_type = ValueSourceType.TOOL
    base = _SOURCE_PRIORITY.get(source_type, 0)
    confidence = float(payload.get("confidence") or 0.0)
    return base, confidence, str(payload.get("source_name") or "")


def _incoming_preferred(current_source: Any, incoming_source: Any) -> bool:
    return _source_rank(incoming_source) > _source_rank(current_source)


def _is_empty_value(value: Any) -> bool:
    return value in (None, "", [], {})


def _clone_column_info(column: ColumnInfo) -> ColumnInfo:
    cloned = ColumnInfo(**column.model_dump(mode="json"))
    cloned.field_provenance = deepcopy(column.field_provenance)
    return cloned


def _clone_table_info(table: TableInfo) -> TableInfo:
    cloned = TableInfo(**table.model_dump(mode="json"))
    cloned.field_provenance = deepcopy(table.field_provenance)
    cloned.columns = {name: _clone_column_info(col) for name, col in table.columns.items()}
    return cloned


def _clone_database_info(db: DatabaseInfo) -> DatabaseInfo:
    cloned = DatabaseInfo(**db.model_dump(mode="json"))
    cloned.field_provenance = deepcopy(db.field_provenance)
    cloned.tables = {name: _clone_table_info(table) for name, table in db.tables.items()}
    return cloned

def _normalize_foreign_keys_list(fks: Any) -> list[Dict[str, Any]]:
    """
    Normalize foreign key rows into a canonical list-of-dicts format.

    Supports both:
    - information_schema style rows (column_name / referenced_table_name / referenced_column_name ...)
    - legacy compact rows ({columns: [...], ref_table: "...", ref_columns: [...]})

    Output keeps dicts (no strict schema) but ensures per-column mapping fields exist when possible.
    """
    if not fks:
        return []
    if not isinstance(fks, list):
        return []

    out: list[Dict[str, Any]] = []
    for row in fks:
        if not isinstance(row, dict):
            continue

        # Canonical-ish already
        if row.get("column_name") and row.get("referenced_table_name") and row.get("referenced_column_name"):
            out.append(dict(row))
            continue

        # Legacy compact form
        cols = row.get("columns")
        ref_table = row.get("ref_table")
        ref_cols = row.get("ref_columns")
        if isinstance(cols, list) and isinstance(ref_cols, list) and ref_table and cols and ref_cols:
            n = min(len(cols), len(ref_cols))
            for i in range(n):
                c = str(cols[i] or "").strip()
                rc = str(ref_cols[i] or "").strip()
                if not c or not rc:
                    continue
                out.append(
                    {
                        "constraint_name": str(row.get("constraint_name") or f"legacy_fk_{ref_table}_{c}"),
                        "column_name": c,
                        "referenced_table_schema": str(row.get("referenced_table_schema") or row.get("ref_schema") or ""),
                        "referenced_table_name": str(ref_table),
                        "referenced_column_name": rc,
                    }
                )
            continue

        # Best-effort passthrough
        out.append(dict(row))
    return out


def _union_foreign_keys(base: list[Dict[str, Any]], delta: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    """
    Union by (column_name, referenced_table_name, referenced_column_name) when present; otherwise keep order.
    """
    seen: set[tuple[str, str, str]] = set()
    out: list[Dict[str, Any]] = []
    for row in (base or []) + (delta or []):
        if not isinstance(row, dict):
            continue
        key = (
            str(row.get("column_name") or "").strip(),
            str(row.get("referenced_table_name") or "").strip(),
            str(row.get("referenced_column_name") or "").strip(),
        )
        if all(key) and key in seen:
            continue
        if all(key):
            seen.add(key)
        out.append(row)
    return out


def _merge_column(base: ColumnInfo, delta: ColumnInfo) -> ColumnInfo:
    """合并列信息，冲突时保留更完整值。"""
    merged = base.model_dump(mode="json")
    delta_d = delta.model_dump(mode="json")
    for k, v in delta_d.items():
        if k == "field_provenance":
            continue
        if v and (not merged.get(k) or (isinstance(v, str) and len(str(v)) > len(str(merged.get(k, ""))))):
            merged[k] = v
    out = ColumnInfo(**merged)
    out.field_provenance = deepcopy(base.field_provenance)
    for field_name, source in delta.field_provenance.items():
        current_source = out.field_provenance.get(field_name)
        if current_source is None or _incoming_preferred(current_source, source):
            out.field_provenance[field_name] = deepcopy(source)
    return out


def _merge_table(base: TableInfo, delta: TableInfo) -> TableInfo:
    """合并表信息。"""
    merged_cols: Dict[str, ColumnInfo] = {name: _clone_column_info(col) for name, col in base.columns.items()}
    for col_name, col_delta in delta.columns.items():
        if col_name in merged_cols:
            merged_cols[col_name] = _merge_column(merged_cols[col_name], col_delta)
        else:
            merged_cols[col_name] = _clone_column_info(col_delta)

    merged = base.model_dump(mode="json")
    merged["columns"] = merged_cols
    if delta.description:
        merged["description"] = delta.description or merged.get("description", "")
    if delta.primary_key is not None:
        merged["primary_key"] = delta.primary_key
    if delta.foreign_keys is not None:
        # Do not wipe existing FK info with empty list; instead normalize and union.
        base_fks = _normalize_foreign_keys_list(getattr(base, "foreign_keys", None))
        delta_fks = _normalize_foreign_keys_list(delta.foreign_keys)
        if delta_fks:
            merged["foreign_keys"] = _union_foreign_keys(base_fks, delta_fks)
        else:
            merged["foreign_keys"] = base_fks or (getattr(base, "foreign_keys", None))
    out = TableInfo(**merged)
    out.columns = merged_cols
    out.field_provenance = deepcopy(base.field_provenance)
    for field_name, source in delta.field_provenance.items():
        current_source = out.field_provenance.get(field_name)
        if current_source is None or _incoming_preferred(current_source, source):
            out.field_provenance[field_name] = deepcopy(source)
    return out


def _normalize_table_key(tb_name: str) -> str:
    """表 key 规范：若含 schema. 前缀则取 table_name。"""
    if "." in tb_name:
        return tb_name.split(".")[-1]
    return tb_name


def _normalize_schema_tables(schema: Schema) -> Schema:
    """规范化 schema 中的表 key，合并 schema.table 与 table 形式的重复表。"""
    new_dbs = {}
    for db_name, db_info in schema.databases.items():
        new_tables = {}
        for tb_name, tb_info in db_info.tables.items():
            key = _normalize_table_key(tb_name)
            if key in new_tables:
                new_tables[key] = _merge_table(new_tables[key], tb_info)
            else:
                # Also normalize FK representation for robustness.
                if getattr(tb_info, "foreign_keys", None):
                    normalized = tb_info.model_dump(mode="json")
                    normalized["foreign_keys"] = _normalize_foreign_keys_list(tb_info.foreign_keys)
                    cloned = TableInfo(**normalized)
                    cloned.field_provenance = deepcopy(tb_info.field_provenance)
                    cloned.columns = {name: _clone_column_info(col) for name, col in tb_info.columns.items()}
                    new_tables[key] = cloned
                else:
                    new_tables[key] = _clone_table_info(tb_info)
        db_clone = DatabaseInfo(description=db_info.description, tables=new_tables)
        db_clone.field_provenance = deepcopy(db_info.field_provenance)
        new_dbs[db_name] = db_clone
    return Schema(databases=new_dbs)


def apply_schema_write_plan(base: Schema, plan: SchemaWritePlan) -> Schema:
    result = _normalize_schema_tables(base)
    for write in plan.writes:
        _apply_schema_write(result, write)
    # Normalize again to:
    # - merge potential duplicated table keys introduced by writes
    # - normalize foreign_keys representation added/overwritten by tools
    result = _normalize_schema_tables(result)
    return result


def _source_type_for_source_name(source: str) -> ValueSourceType:
    source_name = str(source or "").strip().lower()
    if source_name == "initialize_json":
        return ValueSourceType.INITIALIZE_JSON
    if source_name in {"schema_meta", "db_metadata"}:
        return ValueSourceType.DB_METADATA
    if source_name in {"sql_explore", "db_sample"}:
        return ValueSourceType.DB_SAMPLE
    if source_name in {"semantic_enricher", "llm_inference"}:
        return ValueSourceType.LLM_WEAK_SEMANTIC
    return ValueSourceType.TOOL


def schema_write_plan_from_column_records(
    columns: list[Dict[str, Any]],
    schema_name: str,
    *,
    source: str = "schema_retrieval",
) -> SchemaWritePlan:
    writes: list[SchemaWrite] = []
    seen_columns: set[tuple[str, str, str]] = set()
    pk_by_table: Dict[tuple[str, str], List[str]] = {}
    fk_by_table: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for col in columns:
        db_name = str(col.get("database_name") or schema_name or "").strip()
        table_name = str(col.get("table_name") or "").strip()
        column_name = str(col.get("column_name") or "").strip()
        if not db_name or not table_name:
            continue

        if column_name == "*":
            continue

        column_type = str(col.get("column_type") or col.get("data_type") or "").strip()
        semantic_summary = str(col.get("semantic_summary") or "").strip()
        column_comment = str(col.get("column_comment") or "").strip()
        description = semantic_summary or column_comment
        sample_values = list(col.get("sample_values") or [])
        index_val = str(col.get("index") or "").strip()
        column_key = (db_name, table_name, column_name)
        if column_key not in seen_columns:
            seen_columns.add(column_key)
            writes.append(
                SchemaWrite(
                    target=SchemaFieldTarget(
                        level=SchemaFieldLevel.COLUMN,
                        database=db_name,
                        table=table_name,
                        column=column_name,
                        field="type",
                    ),
                    operation=WriteOperation.REPLACE_IF_BETTER,
                    value=column_type,
                    value_source=ValueSource(source_type=_source_type_for_source_name(source), source_name=source, confidence=0.8),
                    write_policy=WritePolicy(allow_overwrite=False, require_target_exists=False),
                    reason="retrieval column skeleton" if not column_type else "retrieval column type",
                )
            )
        if index_val:
            writes.append(
                SchemaWrite(
                    target=SchemaFieldTarget(
                        level=SchemaFieldLevel.COLUMN,
                        database=db_name,
                        table=table_name,
                        column=column_name,
                        field="index",
                    ),
                    operation=WriteOperation.SET,
                    value=index_val,
                    value_source=ValueSource(source_type=_source_type_for_source_name(source), source_name=source, confidence=0.9),
                    write_policy=WritePolicy(allow_overwrite=False, require_target_exists=False),
                    reason="retrieval column index",
                )
            )
        if sample_values:
            writes.append(
                SchemaWrite(
                    target=SchemaFieldTarget(
                        level=SchemaFieldLevel.COLUMN,
                        database=db_name,
                        table=table_name,
                        column=column_name,
                        field="sample_values",
                    ),
                    operation=WriteOperation.APPEND_UNIQUE,
                    value=sample_values[:8],
                    value_source=ValueSource(source_type=_source_type_for_source_name(source), source_name=source, confidence=0.9),
                    write_policy=WritePolicy(allow_overwrite=False, require_target_exists=False),
                    reason="retrieval column sample values",
                )
            )
        if description:
            writes.append(
                SchemaWrite(
                    target=SchemaFieldTarget(
                        level=SchemaFieldLevel.COLUMN,
                        database=db_name,
                        table=table_name,
                        column=column_name,
                        field="description",
                    ),
                    operation=WriteOperation.REPLACE_IF_BETTER,
                    value=description,
                    value_source=ValueSource(source_type=_source_type_for_source_name(source), source_name=source, confidence=0.85),
                    write_policy=WritePolicy(allow_overwrite=False, require_target_exists=False),
                    reason="retrieval column semantic summary" if semantic_summary else "retrieval column comment",
                )
            )

        # Best-effort keys: allow local initialize JSON to populate PK/FK without DB calls.
        if bool(col.get("is_primary_key") or False):
            pk_by_table.setdefault((db_name, table_name), [])
            if column_name not in pk_by_table[(db_name, table_name)]:
                pk_by_table[(db_name, table_name)].append(column_name)
        fk_ref = str(col.get("foreign_key_ref") or "").strip()
        if fk_ref:
            import re

            m = re.match(r"^(?P<table>[A-Za-z0-9_]+)\\((?P<column>[A-Za-z0-9_]+)\\)$", fk_ref)
            if m:
                fk_by_table.setdefault((db_name, table_name), [])
                fk_by_table[(db_name, table_name)].append(
                    {
                        "constraint_name": f"init_fk_{table_name}_{column_name}",
                        "column_name": column_name,
                        "referenced_table_schema": db_name,
                        "referenced_table_name": m.group("table"),
                        "referenced_column_name": m.group("column"),
                    }
                )

    for (db_name, table_name), pk_cols in pk_by_table.items():
        if pk_cols:
            writes.append(
                SchemaWrite(
                    target=SchemaFieldTarget(
                        level=SchemaFieldLevel.TABLE,
                        database=db_name,
                        table=table_name,
                        field="primary_key",
                    ),
                    operation=WriteOperation.SET,
                    value=pk_cols,
                    value_source=ValueSource(source_type=_source_type_for_source_name(source), source_name=source, confidence=0.95),
                    write_policy=WritePolicy(allow_overwrite=True, require_target_exists=False),
                    reason="retrieval primary key (initialize)",
                )
            )

    for (db_name, table_name), fks in fk_by_table.items():
        if fks:
            writes.append(
                SchemaWrite(
                    target=SchemaFieldTarget(
                        level=SchemaFieldLevel.TABLE,
                        database=db_name,
                        table=table_name,
                        field="foreign_keys",
                    ),
                    operation=WriteOperation.SET,
                    value=fks,
                    value_source=ValueSource(source_type=_source_type_for_source_name(source), source_name=source, confidence=0.95),
                    write_policy=WritePolicy(allow_overwrite=True, require_target_exists=False),
                    reason="retrieval foreign keys (initialize)",
                )
            )
    return SchemaWritePlan(writes=writes, summary=f"write plan from {source}")


def schema_write_plan_from_table_metadata(
    schema_name: str,
    table_name: str,
    *,
    columns: list[Dict[str, Any]],
    table_comment: str = "",
    primary_key: list[str] | None = None,
    foreign_keys: list[Dict[str, Any]] | None = None,
    indexes: list[Dict[str, Any]] | None = None,
    source: str = "schema_meta",
) -> SchemaWritePlan:
    writes: list[SchemaWrite] = []
    normalized_table = table_name.split(".")[-1] if "." in table_name else table_name

    if table_comment.strip():
        writes.append(
            SchemaWrite(
                target=SchemaFieldTarget(
                    level=SchemaFieldLevel.TABLE,
                    database=schema_name,
                    table=normalized_table,
                    field="description",
                ),
                operation=WriteOperation.REPLACE_IF_BETTER,
                value=table_comment.strip(),
                value_source=ValueSource(source_type=_source_type_for_source_name(source), source_name=source, confidence=1.0),
                write_policy=WritePolicy(allow_overwrite=False, require_target_exists=False),
                reason="table comment metadata",
            )
        )
    if primary_key is not None:
        writes.append(
            SchemaWrite(
                target=SchemaFieldTarget(
                    level=SchemaFieldLevel.TABLE,
                    database=schema_name,
                    table=normalized_table,
                    field="primary_key",
                ),
                operation=WriteOperation.SET,
                value=primary_key,
                value_source=ValueSource(source_type=_source_type_for_source_name(source), source_name=source, confidence=1.0),
                write_policy=WritePolicy(allow_overwrite=True, require_target_exists=False),
                reason="primary key metadata",
            )
        )
    if foreign_keys is not None:
        writes.append(
            SchemaWrite(
                target=SchemaFieldTarget(
                    level=SchemaFieldLevel.TABLE,
                    database=schema_name,
                    table=normalized_table,
                    field="foreign_keys",
                ),
                operation=WriteOperation.SET,
                value=foreign_keys,
                value_source=ValueSource(source_type=_source_type_for_source_name(source), source_name=source, confidence=1.0),
                write_policy=WritePolicy(allow_overwrite=True, require_target_exists=False),
                reason="foreign key metadata",
            )
        )

    def _index_value_for_column(col_name: str) -> str:
        name = str(col_name or "").strip()
        if not name:
            return ""
        if primary_key and name in set(primary_key):
            return "PRIMARY"
        if not indexes:
            return ""
        # Prefer UNIQUE over plain INDEX.
        unique_names: set[str] = set()
        index_names: set[str] = set()
        for r in indexes:
            if not isinstance(r, dict):
                continue
            if str(r.get("column_name") or "").strip() != name:
                continue
            idx_name = str(r.get("index_name") or "").strip()
            if not idx_name:
                continue
            if idx_name.upper() == "PRIMARY":
                return "PRIMARY"
            non_unique = r.get("non_unique")
            is_unique = False
            if isinstance(non_unique, (int, float)):
                is_unique = int(non_unique) == 0
            else:
                # MySQL can return "0"/"1" as strings depending on drivers.
                is_unique = str(non_unique).strip() == "0"
            if is_unique:
                unique_names.add(idx_name)
            else:
                index_names.add(idx_name)
        if unique_names:
            one = sorted(unique_names)[0]
            return f"UNIQUE({one})"
        if index_names:
            one = sorted(index_names)[0]
            return f"INDEX({one})"
        return ""

    for column_meta in columns:
        column_name = str(column_meta.get("column_name") or "").strip()
        if not column_name:
            continue
        column_type = str(column_meta.get("column_type") or column_meta.get("data_type") or "").strip()
        column_comment = str(column_meta.get("column_comment") or "").strip()
        if column_type:
            writes.append(
                SchemaWrite(
                    target=SchemaFieldTarget(
                        level=SchemaFieldLevel.COLUMN,
                        database=schema_name,
                        table=normalized_table,
                        column=column_name,
                        field="type",
                    ),
                    operation=WriteOperation.SET,
                    value=column_type,
                    value_source=ValueSource(source_type=_source_type_for_source_name(source), source_name=source, confidence=1.0),
                    write_policy=WritePolicy(allow_overwrite=True, require_target_exists=False),
                    reason="describe column type",
                )
            )
        if column_comment:
            writes.append(
                SchemaWrite(
                    target=SchemaFieldTarget(
                        level=SchemaFieldLevel.COLUMN,
                        database=schema_name,
                        table=normalized_table,
                        column=column_name,
                        field="description",
                    ),
                    operation=WriteOperation.REPLACE_IF_BETTER,
                    value=column_comment,
                    value_source=ValueSource(source_type=_source_type_for_source_name(source), source_name=source, confidence=1.0),
                    write_policy=WritePolicy(allow_overwrite=False, require_target_exists=False),
                    reason="column comment metadata",
                )
            )

        idx_val = _index_value_for_column(column_name)
        if idx_val:
            writes.append(
                SchemaWrite(
                    target=SchemaFieldTarget(
                        level=SchemaFieldLevel.COLUMN,
                        database=schema_name,
                        table=normalized_table,
                        column=column_name,
                        field="index",
                    ),
                    operation=WriteOperation.SET,
                    value=idx_val,
                    value_source=ValueSource(source_type=_source_type_for_source_name(source), source_name=source, confidence=1.0),
                    write_policy=WritePolicy(allow_overwrite=True, require_target_exists=False),
                    reason="index metadata",
                )
            )

    return SchemaWritePlan(writes=writes, summary=f"write plan from {source}")


def _apply_schema_write(schema: Schema, write: SchemaWrite) -> None:
    target = write.target
    if target.level == SchemaFieldLevel.DATABASE:
        db_info = _get_or_create_database(schema, target.database, write.write_policy.require_target_exists)
        if db_info is None:
            return
        _apply_field_write(db_info, target.field, write)
        return

    if target.level == SchemaFieldLevel.TABLE:
        table_info = _get_or_create_table(
            schema,
            target.database,
            target.table,
            write.write_policy.require_target_exists,
        )
        if table_info is None:
            return
        _apply_field_write(table_info, target.field, write)
        return

    column_info = _get_or_create_column(
        schema,
        target.database,
        target.table,
        target.column,
        write.write_policy.require_target_exists,
    )
    if column_info is None:
        return
    _apply_field_write(column_info, target.field, write)


def _get_or_create_database(schema: Schema, db_name: str, require_exists: bool) -> DatabaseInfo | None:
    if db_name in schema.databases:
        return schema.databases[db_name]
    if require_exists or not db_name:
        return None
    schema.databases[db_name] = DatabaseInfo()
    return schema.databases[db_name]


def _get_or_create_table(schema: Schema, db_name: str, table_name: str, require_exists: bool) -> TableInfo | None:
    db_info = _get_or_create_database(schema, db_name, require_exists)
    if db_info is None:
        return None
    if table_name in db_info.tables:
        return db_info.tables[table_name]
    if require_exists or not table_name:
        return None
    db_info.tables[table_name] = TableInfo()
    return db_info.tables[table_name]


def _get_or_create_column(
    schema: Schema,
    db_name: str,
    table_name: str,
    column_name: str,
    require_exists: bool,
) -> ColumnInfo | None:
    table_info = _get_or_create_table(schema, db_name, table_name, require_exists)
    if table_info is None:
        return None
    if column_name in table_info.columns:
        return table_info.columns[column_name]
    if require_exists or not column_name:
        return None
    table_info.columns[column_name] = ColumnInfo()
    return table_info.columns[column_name]


def _apply_field_write(entity: Any, field_name: str, write: SchemaWrite) -> None:
    current = getattr(entity, field_name, None)
    if write.write_policy.only_if_empty and current not in (None, "", [], {}):
        return
    provenance = getattr(entity, "field_provenance", None)
    current_source = provenance.get(field_name) if isinstance(provenance, dict) else None
    if not _can_apply_write(current, current_source, write):
        return
    new_value = _resolve_write_value(current, write.operation, write.value, current_source=current_source, incoming_source=write.value_source)
    setattr(entity, field_name, new_value)
    if isinstance(provenance, dict):
        provenance[field_name] = _value_source_to_dict(write.value_source)


def _can_apply_write(current: Any, current_source: Any, write: SchemaWrite) -> bool:
    if _is_empty_value(current):
        return True
    if write.write_policy.allow_overwrite:
        return _incoming_preferred(current_source, write.value_source) or current_source is None
    if write.operation == WriteOperation.APPEND_UNIQUE:
        return True
    if write.operation == WriteOperation.MERGE:
        return True
    if write.operation == WriteOperation.REPLACE_IF_BETTER:
        return _incoming_preferred(current_source, write.value_source) or current_source is None
    return _incoming_preferred(current_source, write.value_source) or current_source is None


def _resolve_write_value(current: Any, operation: WriteOperation, incoming: Any, *, current_source: Any = None, incoming_source: Any = None) -> Any:
    if operation == WriteOperation.SET:
        return incoming
    if operation == WriteOperation.MERGE:
        if isinstance(current, dict) and isinstance(incoming, dict):
            merged = dict(current)
            merged.update(incoming)
            return merged
        if isinstance(current, list) and isinstance(incoming, list):
            if current and incoming and not _incoming_preferred(current_source, incoming_source):
                return list(current)
            return _union_foreign_keys(_normalize_foreign_keys_list(current), _normalize_foreign_keys_list(incoming)) or list(current)
        return incoming
    if operation == WriteOperation.APPEND_UNIQUE:
        base = list(current or [])
        items = incoming if isinstance(incoming, list) else [incoming]
        for item in items:
            if item not in base:
                base.append(item)
        return base
    if operation == WriteOperation.REPLACE_IF_BETTER:
        if current in (None, "", [], {}):
            return incoming
        if _incoming_preferred(current_source, incoming_source):
            return incoming
        if isinstance(current, str) and isinstance(incoming, str) and len(incoming) > len(current):
            return incoming
        return current
    if operation == WriteOperation.REMOVE:
        if isinstance(current, list):
            items = incoming if isinstance(incoming, list) else [incoming]
            return [item for item in current if item not in items]
        if isinstance(current, dict) and isinstance(incoming, list):
            updated = dict(current)
            for key in incoming:
                updated.pop(key, None)
            return updated
        return "" if isinstance(current, str) else None
    return incoming


def _normalize_redundant_items(schema: Schema, items: list[str]) -> set[str]:
    """将 redundant_items 规范化为 db.table 或 db.table.column 格式。"""
    normalized: set[str] = set()
    dbs = list(schema.databases.keys())
    for item in (i.strip() for i in items if i and isinstance(i, str)):
        if not item:
            continue
        parts = [p.strip() for p in item.split(".") if p.strip()]
        if len(parts) == 3:
            normalized.add(f"{parts[0]}.{parts[1]}.{parts[2]}")
        elif len(parts) == 2:
            normalized.add(f"{parts[0]}.{parts[1]}")
        elif len(parts) == 1 and dbs:
            for db in dbs:
                if parts[0] in schema.databases.get(db, DatabaseInfo()).tables:
                    normalized.add(f"{db}.{parts[0]}")
                    break
    return normalized


def prune_schema_by_redundant_items(schema: Schema, redundant_items: list[str]) -> Schema:
    """
    根据 redundant_items 剪枝 schema，实现最小化。
    redundant_items 格式："db.table" 或 "db.table.column"，支持 "table" 单段（自动匹配 db）
    """
    to_remove = _normalize_redundant_items(schema, redundant_items)
    if not to_remove:
        return schema

    result = Schema(databases={})
    for db_name, db_info in schema.databases.items():
        new_tables: Dict[str, TableInfo] = {}
        for tb_name, tb_info in db_info.tables.items():
            full_table = f"{db_name}.{tb_name}"
            if full_table in to_remove:
                continue  # 移除整表
            new_cols: Dict[str, ColumnInfo] = {}
            for col_name, col_info in tb_info.columns.items():
                full_col = f"{db_name}.{tb_name}.{col_name}"
                if full_col in to_remove:
                    continue  # 移除列
                new_cols[col_name] = col_info
            if new_cols:
                new_tables[tb_name] = TableInfo(
                    description=tb_info.description,
                    primary_key=tb_info.primary_key,
                    foreign_keys=tb_info.foreign_keys,
                    columns=new_cols,
                )
        if new_tables:
            result.databases[db_name] = DatabaseInfo(
                description=db_info.description,
                tables=new_tables,
            )
    return result
