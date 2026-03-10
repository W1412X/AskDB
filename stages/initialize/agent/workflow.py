"""\
LangGraph workflow for initialize stage.
"""

from typing import Dict, Any, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import os
import json
import time
import re
from config.app_config import get_app_config
from utils.database_tool import _db_tool
from utils.logger import get_logger
from utils.data_paths import DataPaths
from .column_agent import generate_semantic_summary_and_keywords
from .readme_generator import generate_table_readme, generate_database_readme
from .state import (
    DatabaseState, TableState, ColumnState,
    TaskStatus, StateManager
)
from .models import (
    ColumnDescription,
    IndexInfo,
    StatisticsInfo,
    SamplesInfo,
    SampleData,
    TableContextSamples,
)

logger = get_logger("workflow")
_COLUMN_AGENT_CFG = get_app_config().stages.column_agent

SAMPLE_CONFIG = {
    "random_sample_n": 8,
    "distinct_sample_n": 12,
    "table_context_n": 5,
    "table_context_extra_cols": 5,
    "truncate_length": 200,
    # COUNT(DISTINCT) may be expensive; only do it when row_count is small.
    "max_rows_for_distinct_count": 200_000,
}


def _is_safe_identifier(name: str) -> bool:
    """Basic identifier safety check for backtick-quoted SQL."""
    return bool(name) and re.fullmatch(r"[A-Za-z0-9_]+", name) is not None


def _is_sensitive_column(column_name: str) -> bool:
    keywords = ("password", "secret", "token", "key", "credential")
    s = (column_name or "").lower()
    return any(k in s for k in keywords)


def _truncate_value(value: Any, max_length: int) -> tuple[str, bool, int]:
    if value is None:
        return "", False, 0
    s = str(value)
    if len(s) <= max_length:
        return s, False, len(s)
    return s[:max_length] + "...[truncated]", True, len(s)


def _sample_column_values(database_name: str, table_name: str, column_name: str) -> Dict[str, Any]:
    """
    Collect lightweight samples for a column.
    Returns dict compatible with SamplesInfo model_dump structure.
    """
    if not (_is_safe_identifier(database_name) and _is_safe_identifier(table_name) and _is_safe_identifier(column_name)):
        return {"random_samples": [], "distinct_samples": [], "total_distinct_count": None}

    n_rand = int(SAMPLE_CONFIG["random_sample_n"])
    n_dist = int(SAMPLE_CONFIG["distinct_sample_n"])
    trunc = int(SAMPLE_CONFIG["truncate_length"])

    masked = _is_sensitive_column(column_name)

    # NOTE: avoid ORDER BY RAND() for performance; take first N non-null rows
    sql_rand = (
        f"SELECT `{column_name}` AS v "
        f"FROM `{database_name}`.`{table_name}` "
        f"WHERE `{column_name}` IS NOT NULL "
        f"LIMIT {n_rand}"
    )
    rows = _db_tool.execute_query(sql_rand, database=database_name)
    random_samples: list[Dict[str, Any]] = []
    for r in rows:
        v = r.get("v")
        if masked:
            v = "***[sensitive]***"
        sample_value, truncated, original_length = _truncate_value(v, trunc)
        random_samples.append(
            {"sample_value": sample_value, "original_length": original_length, "truncated": truncated}
        )

    sql_dist = (
        f"SELECT DISTINCT `{column_name}` AS v "
        f"FROM `{database_name}`.`{table_name}` "
        f"WHERE `{column_name}` IS NOT NULL "
        f"LIMIT {n_dist}"
    )
    rows = _db_tool.execute_query(sql_dist, database=database_name)
    distinct_samples: list[Dict[str, Any]] = []
    for r in rows:
        v = r.get("v")
        if masked:
            v = "***[sensitive]***"
        sample_value, truncated, original_length = _truncate_value(v, trunc)
        distinct_samples.append(
            {"sample_value": sample_value, "original_length": original_length, "truncated": truncated}
        )

    # optional distinct count
    total_distinct_count = None
    try:
        table_info = _db_tool.get_table_info(database_name, table_name)
        row_count = table_info.get("table_rows")
        if isinstance(row_count, int) and row_count <= int(SAMPLE_CONFIG["max_rows_for_distinct_count"]):
            sql_cnt = (
                f"SELECT COUNT(DISTINCT `{column_name}`) AS cnt "
                f"FROM `{database_name}`.`{table_name}`"
            )
            cnt_rows = _db_tool.execute_query(sql_cnt, database=database_name)
            if cnt_rows:
                total_distinct_count = cnt_rows[0].get("cnt")
    except Exception:
        total_distinct_count = None

    return {
        "random_samples": random_samples,
        "distinct_samples": distinct_samples,
        "total_distinct_count": total_distinct_count,
    }


def _sample_table_context(
    database_name: str, table_name: str, focus_column: str, columns_info: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Sample a few table rows including focus column + a few other columns."""
    if not (_is_safe_identifier(database_name) and _is_safe_identifier(table_name) and _is_safe_identifier(focus_column)):
        return None

    n = int(SAMPLE_CONFIG["table_context_n"])
    extra = int(SAMPLE_CONFIG["table_context_extra_cols"])
    trunc = int(SAMPLE_CONFIG["truncate_length"])

    all_cols = [c.get("column_name") for c in columns_info if c.get("column_name")]
    all_cols = [c for c in all_cols if _is_safe_identifier(c)]
    if not all_cols:
        return None

    others = [c for c in all_cols if c != focus_column][:extra]
    headers = [focus_column] + others

    cols_sql = ", ".join([f"`{c}`" for c in headers])
    sql = f"SELECT {cols_sql} FROM `{database_name}`.`{table_name}` LIMIT {n}"
    rows = _db_tool.execute_query(sql, database=database_name)

    sample_rows: list[list[str]] = []
    for r in rows:
        row_out: list[str] = []
        for c in headers:
            v = r.get(c)
            if _is_sensitive_column(c):
                v = "***[sensitive]***"
            s, _, _ = _truncate_value(v, trunc)
            row_out.append(s)
        sample_rows.append(row_out)

    return {"headers": headers, "sample_rows": sample_rows}


def get_column_description_data(database_name: str, table_name: str, column_name: str) -> Dict[str, Any]:
    """Fetch fixed column metadata used for description generation."""
    columns_info = _db_tool.get_table_columns(database_name, table_name)
    column_info = next((col for col in columns_info if col["column_name"] == column_name), None)
    if not column_info:
        logger.warning(
            "未找到列信息: %s.%s.%s",
            database_name,
            table_name,
            column_name,
            database=database_name,
            table=table_name,
            column=column_name,
        )
        return {}

    indexes = _db_tool.get_table_indexes(database_name, table_name)
    column_indexes = [idx for idx in indexes if idx["column_name"] == column_name]

    foreign_keys = _db_tool.get_table_foreign_keys(database_name, table_name)
    column_fk = next((fk for fk in foreign_keys if fk["column_name"] == column_name), None)

    table_info = _db_tool.get_table_info(database_name, table_name)

    samples = _sample_column_values(database_name, table_name, column_name)
    table_context_samples = _sample_table_context(database_name, table_name, column_name, columns_info)

    return {
        "database_name": database_name,
        "table_name": table_name,
        "column_name": column_name,
        "data_type": column_info.get("column_type", ""),
        "charset": column_info.get("character_set_name"),
        "collation": column_info.get("collation_name"),
        "is_nullable": column_info.get("is_nullable") == "YES",
        "default_value": str(column_info.get("column_default", "")) if column_info.get("column_default") else None,
        "comment": column_info.get("column_comment"),
        "ordinal_position": column_info.get("ordinal_position", 0),
        "is_primary_key": column_info.get("column_key") == "PRI",
        "is_foreign_key": column_fk is not None,
        "foreign_key_ref": f"{column_fk['referenced_table_name']}({column_fk['referenced_column_name']})" if column_fk else None,
        "is_auto_increment": "auto_increment" in column_info.get("extra", "").lower(),
        "is_generated": "generated" in column_info.get("extra", "").lower(),
        "generation_expression": None,
        "has_index": len(column_indexes) > 0,
        "indexes": [
            {
                "index_name": idx["index_name"],
                "index_type": idx["index_type"],
                "is_unique": idx["index_name"] == "PRIMARY" or "UNIQUE" in idx["index_name"],
                "column_position": idx["seq_in_index"]
            }
            for idx in column_indexes
        ],
        "engine_specific": {
            "engine": table_info.get("engine", ""),
        },
        "statistics": {
            "row_count": table_info.get("table_rows"),
            "distinct_count": None,
            "null_count": None
        },
        "samples": samples,
        "table_context_samples": table_context_samples,
    }


def build_column_description(metadata: Dict[str, Any]) -> ColumnDescription:
    samples_obj = None
    samples = metadata.get("samples") or {}
    if isinstance(samples, dict):
        rand = samples.get("random_samples") or []
        dist = samples.get("distinct_samples") or []
        total = samples.get("total_distinct_count")
        samples_obj = SamplesInfo(
            random_samples=[SampleData(**x) for x in rand] if isinstance(rand, list) else [],
            distinct_samples=[SampleData(**x) for x in dist] if isinstance(dist, list) else [],
            total_distinct_count=total if isinstance(total, int) else None,
        )

    tcs_obj = None
    tcs = metadata.get("table_context_samples")
    if isinstance(tcs, dict):
        headers = tcs.get("headers") or []
        rows = tcs.get("sample_rows") or []
        if isinstance(headers, list) and isinstance(rows, list):
            tcs_obj = TableContextSamples(headers=[str(h) for h in headers], sample_rows=rows)

    return ColumnDescription(
        database_name=metadata["database_name"],
        table_name=metadata["table_name"],
        column_name=metadata["column_name"],
        data_type=metadata.get("data_type", ""),
        charset=metadata.get("charset"),
        collation=metadata.get("collation"),
        is_nullable=metadata.get("is_nullable", True),
        default_value=metadata.get("default_value"),
        comment=metadata.get("comment"),
        ordinal_position=metadata.get("ordinal_position", 0),
        is_primary_key=metadata.get("is_primary_key", False),
        is_foreign_key=metadata.get("is_foreign_key", False),
        foreign_key_ref=metadata.get("foreign_key_ref"),
        is_auto_increment=metadata.get("is_auto_increment", False),
        is_generated=metadata.get("is_generated", False),
        generation_expression=metadata.get("generation_expression"),
        has_index=metadata.get("has_index", False),
        indexes=[IndexInfo(**idx) for idx in metadata.get("indexes", [])],
        engine_specific=metadata.get("engine_specific", {}),
        statistics=StatisticsInfo(**metadata["statistics"]) if metadata.get("statistics") else None,
        samples=samples_obj or SamplesInfo(),
        table_context_samples=tcs_obj,
    )


def get_column_file_path(database_name: str, table_name: str, column_name: str) -> str:
    """Get column JSON file path (compat wrapper)."""
    return str(DataPaths.default().column_description_path(database_name, table_name, column_name))


def save_column_file(column_desc: ColumnDescription, database_name: str):
    """Save column description file (atomic write + optional backup)."""
    file_path = DataPaths.default().column_description_path(database_name, column_desc.table_name, column_desc.column_name)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    temp_path = file_path.with_suffix(file_path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8") as f:
        json.dump(column_desc.model_dump(), f, ensure_ascii=False, indent=2)

    if file_path.exists():
        backup_path = file_path.with_suffix(file_path.suffix + f".{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak")
        file_path.rename(backup_path)

    temp_path.rename(file_path)


def _column_file_has_summary(file_path: str) -> bool:
    """Quick check: existing file contains non-empty semantic_summary."""
    if not os.path.exists(file_path):
        return False
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        summary = (data.get("semantic_summary") or "").strip()
        keywords = data.get("semantic_keywords") or []
        return bool(summary) and isinstance(keywords, list) and len(keywords) > 0
    except Exception:
        return False


def _process_one_column(
    database_name: str,
    table_name: str,
    column_name: str,
    model_name: str,
) -> Dict[str, Any]:
    """Worker: fetch metadata, save facts file, call LLM, save final file."""
    wall_start = time.time()
    metadata = get_column_description_data(database_name, table_name, column_name)
    if not metadata:
        raise ValueError("Column metadata is empty.")

    # 1) save facts
    column_desc = build_column_description(metadata)
    save_column_file(column_desc, metadata["database_name"])

    # 2) LLM summary + semantic keywords (single call) + final save
    semantic_summary, semantic_keywords = generate_semantic_summary_and_keywords(metadata, model_name=model_name)
    column_desc.semantic_summary = semantic_summary
    column_desc.semantic_keywords = semantic_keywords
    column_desc.metadata.generated_at = datetime.now().isoformat()
    column_desc.metadata.processing_time_seconds = time.time() - wall_start
    save_column_file(column_desc, metadata["database_name"])

    return {
        "metadata": metadata,
        "semantic_summary": semantic_summary,
        "semantic_keywords": semantic_keywords,
        "file_path": get_column_file_path(metadata["database_name"], metadata["table_name"], column_name),
        "processing_time_seconds": column_desc.metadata.processing_time_seconds,
    }


def run_initialize(
    database_names: List[str],
    state_manager: StateManager,
    timestamp: Optional[str] = None,
    model_name: str = "",
) -> Dict[str, Any]:
    """
    简化版 initialize：用最小状态机串行跑完所有列。

    流程：
    - 维护 Database/Table/Column 三层状态
    - 对每一列：固定函数获取 metadata → 先落盘列描述 JSON → 调用 LLM 生成 semantic_summary → 回写 JSON
    """
    if timestamp is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if not model_name:
        from config.app_config import get_app_config

        model_name = get_app_config().stages.initialize.agent.model_name

    logger.workflow_start("initialize_simple", database_names=database_names, timestamp=timestamp)
    start_time = time.time()

    databases: List[DatabaseState] = []
    for db_name in database_names:
        table_states: List[TableState] = []
        for table_name in _db_tool.list_tables(db_name):
            column_states: List[ColumnState] = []
            for col in _db_tool.get_table_columns(db_name, table_name):
                column_name = col["column_name"]
                column_states.append(
                    ColumnState(
                        column_id=f"{db_name}.{table_name}.{column_name}",
                        column_name=column_name,
                        status=TaskStatus.PENDING,
                        metadata={},
                        parent_table_id=f"{db_name}.{table_name}",
                    )
                )
            table_states.append(
                TableState(
                    table_id=f"{db_name}.{table_name}",
                    table_name=table_name,
                    status=TaskStatus.PENDING,
                    metadata={},
                    columns=column_states,
                    parent_database_id=db_name,
                )
            )
        databases.append(
            DatabaseState(
                database_id=db_name,
                database_name=db_name,
                status=TaskStatus.PENDING,
                metadata={},
                tables=table_states,
            )
        )

    max_parallel_columns = int((_COLUMN_AGENT_CFG.parallel or {}).get("max_parallel_columns", 5) or 1)
    if max_parallel_columns < 1:
        max_parallel_columns = 1

    for db_state in databases:
        db_state.status = TaskStatus.PROCESSING
        db_state.start_time = datetime.now()

        for table_state in db_state.tables:
            table_state.status = TaskStatus.PROCESSING
            table_state.start_time = datetime.now()

            # Pre-check existing outputs; schedule remaining columns in parallel.
            to_run: list[ColumnState] = []
            for column_state in table_state.columns:
                file_path = get_column_file_path(
                    db_state.database_name, table_state.table_name, column_state.column_name
                )
                if _column_file_has_summary(file_path):
                    column_state.status = TaskStatus.COMPLETED
                    column_state.result_file_path = file_path
                    continue

                column_state.status = TaskStatus.PROCESSING
                column_state.start_time = datetime.now()
                to_run.append(column_state)

            if to_run:
                logger.info(
                    "并行处理表列",
                    database=db_state.database_name,
                    table=table_state.table_name,
                    pending_columns=len(to_run),
                    max_parallel_columns=max_parallel_columns,
                )

            with ThreadPoolExecutor(max_workers=min(max_parallel_columns, max(1, len(to_run)))) as executor:
                future_map = {
                    executor.submit(
                        _process_one_column,
                        db_state.database_name,
                        table_state.table_name,
                        col_state.column_name,
                        model_name,
                    ): col_state
                    for col_state in to_run
                }

                for future in as_completed(future_map):
                    column_state = future_map[future]
                    try:
                        result = future.result()
                        column_state.metadata = result.get("metadata", {})
                        column_state.semantic_summary = result.get("semantic_summary", "")
                        column_state.result_file_path = result.get(
                            "file_path",
                            get_column_file_path(
                                db_state.database_name, table_state.table_name, column_state.column_name
                            ),
                        )
                        column_state.status = TaskStatus.COMPLETED
                        column_state.end_time = datetime.now()
                    except Exception as e:
                        column_state.status = TaskStatus.FAILED
                        column_state.error_message = str(e)
                        column_state.end_time = datetime.now()
                        logger.exception(
                            f"列处理失败: {column_state.column_id}",
                            exception=e,
                            database=db_state.database_name,
                            table=table_state.table_name,
                            column=column_state.column_name,
                        )

                    # checkpoint：每列一次（在主线程写，避免并发覆盖）
                    state_manager.save_state(db_state, timestamp)

            # 表完成检查
            if all(col.status == TaskStatus.COMPLETED for col in table_state.columns):
                table_state.status = TaskStatus.COMPLETED
                
                # 表信息完善：生成表的 JSON 描述文件
                try:
                    logger.info(
                        "开始生成表描述",
                        database=db_state.database_name,
                        table=table_state.table_name
                    )
                    json_path = generate_table_readme(
                        database_name=db_state.database_name,
                        table_name=table_state.table_name,
                        model_name=model_name,
                    )
                    table_state.metadata["readme_path"] = json_path
                    logger.info(
                        "表描述生成完成",
                        database=db_state.database_name,
                        table=table_state.table_name,
                        path=json_path
                    )
                except Exception as e:
                    logger.exception(
                        "表描述生成失败",
                        exception=e,
                        database=db_state.database_name,
                        table=table_state.table_name
                    )
                    # 不因为描述生成失败而影响表的状态
            
            table_state.end_time = datetime.now()

        # 库完成检查
        if all(tbl.status == TaskStatus.COMPLETED for tbl in db_state.tables):
            db_state.status = TaskStatus.COMPLETED
            
            # 数据库信息完善：生成数据库的 JSON 描述文件
            try:
                logger.info(
                    "开始生成数据库描述",
                    database=db_state.database_name
                )
                json_path = generate_database_readme(
                    database_name=db_state.database_name,
                    model_name=model_name,
                )
                db_state.metadata["readme_path"] = json_path
                logger.info(
                    "数据库描述生成完成",
                    database=db_state.database_name,
                    path=json_path
                )
            except Exception as e:
                logger.exception(
                    "数据库描述生成失败",
                    exception=e,
                    database=db_state.database_name
                )
                # 不因为描述生成失败而影响数据库的状态
        
        db_state.end_time = datetime.now()

    duration = time.time() - start_time
    logger.workflow_end(
        "initialize_simple",
        duration=duration,
        database_count=len(databases),
        table_count=sum(len(db.tables) for db in databases),
        column_count=sum(len(t.columns) for db in databases for t in db.tables),
        timestamp=timestamp,
    )

    return {
        "database_names": database_names,
        "databases": databases,
        "timestamp": timestamp,
    }
