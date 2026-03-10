"""
Initialize agent 对外接口。

提供以下能力：
- 初始化数据库描述生成流程
- 读取列/表/数据库级 JSON 描述（支持单个与批量）
"""

from __future__ import annotations

from typing import Any, Dict, List, Sequence, Union, Optional
import json
from pathlib import Path

from config.app_config import get_app_config
from .run import initialize_databases
from utils.data_paths import DataPaths


JsonDict = Dict[str, Any]
NameOrNames = Union[str, Sequence[str]]


def initialize(
    database_names: Optional[List[str]] = None,
    checkpoint_dir: Optional[str] = None,
    progress_log_dir: Optional[str] = None,
    token_usage_dir: Optional[str] = None,
    model_name: Optional[str] = None,
) -> Dict[str, Any]:
    """对外初始化入口（封装 run.initialize_databases）。"""
    return initialize_databases(
        database_names=database_names or get_app_config().get_initialize_databases(),
        checkpoint_dir=checkpoint_dir,
        progress_log_dir=progress_log_dir,
        token_usage_dir=token_usage_dir,
        model_name=model_name or get_app_config().stages.initialize.agent.model_name,
    )


def _read_json_file(file_path: Path) -> JsonDict:
    if not file_path.exists():
        raise FileNotFoundError(f"JSON file not found: {file_path}")
    with file_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _as_name_list(names: NameOrNames) -> List[str]:
    if isinstance(names, str):
        return [names]
    return list(names)


def get_column_json(
    database_name: str,
    table_name: str,
    column_names: NameOrNames,
) -> Union[JsonDict, List[JsonDict]]:
    """
    读取列级 JSON 文件。

    - 传入单个列名 -> 返回字典
    - 传入列名列表 -> 返回字典列表
    """
    names = _as_name_list(column_names)
    items = [
        _read_json_file(DataPaths.default().column_description_path(database_name, table_name, col_name))
        for col_name in names
    ]
    return items[0] if isinstance(column_names, str) else items


def get_table_json(
    database_name: str,
    table_names: NameOrNames,
) -> Union[JsonDict, List[JsonDict]]:
    """
    读取表级 JSON 文件（TABLE_{table_name}.json）。

    - 传入单个表名 -> 返回字典
    - 传入表名列表 -> 返回字典列表
    """
    names = _as_name_list(table_names)
    items = []
    for table_name in names:
        table_file = DataPaths.default().table_description_path(
            database_name, table_name
        ) / f"TABLE_{table_name}.json"
        items.append(_read_json_file(table_file))
    return items[0] if isinstance(table_names, str) else items


def get_database_json(
    database_names: NameOrNames,
) -> Union[JsonDict, List[JsonDict]]:
    """
    读取数据库级 JSON 文件（DATABASE_{database_name}.json）。

    - 传入单个数据库名 -> 返回字典
    - 传入数据库名列表 -> 返回字典列表
    """
    names = _as_name_list(database_names)
    items = []
    for database_name in names:
        db_file = DataPaths.default().initialize_agent_database_dir(
            database_name
        ) / f"DATABASE_{database_name}.json"
        items.append(_read_json_file(db_file))
    return items[0] if isinstance(database_names, str) else items


__all__ = [
    "initialize",
    "get_column_json",
    "get_table_json",
    "get_database_json",
]
