"""
表和数据库描述生成模块

为数据库和表生成 JSON 格式的描述文件，包含语义总结和关键信息。
"""

from __future__ import annotations

from typing import Dict, Any, List, Optional
import json
from pathlib import Path
from datetime import datetime

from config.app_config import get_app_config
from config.llm_config import get_llm
from stages.general.summary import DEFAULT_MAX_INPUT_LENGTH
from utils.database_tool import _db_tool
from utils.data_paths import DataPaths
from utils.logger import get_logger

logger = get_logger("readme_generator")

# 配置
TARGET_TABLE_TOKEN_COUNT = 100  # 目标 token 数量（表 README）
MAX_TABLE_TOKEN_COUNT = 200  # 最大 token 数量（表 README）
TARGET_DATABASE_TOKEN_COUNT = 150  # 目标 token 数量（数据库 README）
MAX_DATABASE_TOKEN_COUNT = 300  # 最大 token 数量（数据库 README）


def _load_column_descriptions(database_name: str, table_name: str) -> List[Dict[str, Any]]:
    """
    加载表的所有列描述文件
    
    Args:
        database_name: 数据库名称
        table_name: 表名称
    
    Returns:
        列描述列表，按 ordinal_position 排序
    """
    table_dir = DataPaths.default().table_description_path(database_name, table_name)
    column_descriptions = []
    
    if not table_dir.exists():
        logger.warning(
            "表目录不存在",
            database=database_name,
            table=table_name,
            path=str(table_dir)
        )
        return column_descriptions
    
    for json_file in table_dir.glob("*.json"):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                column_descriptions.append(data)
        except Exception as e:
            logger.warning(
                "加载列描述文件失败",
                exception=e,
                database=database_name,
                table=table_name,
                file=str(json_file)
            )
    
    # 按 ordinal_position 排序
    column_descriptions.sort(key=lambda x: x.get("ordinal_position", 0))
    return column_descriptions


def _build_table_summary_prompt(
    database_name: str,
    table_name: str,
    table_info: Dict[str, Any],
    column_descriptions: List[Dict[str, Any]],
) -> str:
    """
    构建表语义总结的提示词
    
    Args:
        database_name: 数据库名称
        table_name: 表名称
        table_info: 表元信息
        column_descriptions: 列描述列表
    
    Returns:
        提示词文本
    """
    # 收集列信息
    column_summaries = []
    for col_desc in column_descriptions:
        col_name = col_desc.get("column_name", "")
        semantic_summary = col_desc.get("semantic_summary", "")
        if semantic_summary:
            column_summaries.append(f"- {col_name}: {semantic_summary}")
    
    # 构建提示词
    prompt_parts = [
        "你是一个数据库专家。请根据以下信息生成表的语义总结。",
        "",
        f"数据库: {database_name}",
        f"表名: {table_name}",
    ]
    
    if table_info.get("table_comment"):
        prompt_parts.append(f"表注释: {table_info.get('table_comment')}")
    
    if table_info.get("table_rows"):
        prompt_parts.append(f"行数: {table_info.get('table_rows'):,}")
    
    prompt_parts.append("")
    prompt_parts.append("列信息：")
    prompt_parts.extend(column_summaries)
    prompt_parts.append("")
    prompt_parts.append(
        "请生成一个极简的表语义总结（20-40字），只说明表的核心用途，不要包含表名、不要重复列信息。"
    )
    prompt_parts.append("要求：直接输出总结文本，不要包含任何格式标记、标题或多余文字。")
    
    return "\n".join(prompt_parts)


def _generate_table_summary(
    database_name: str,
    table_name: str,
    table_info: Dict[str, Any],
    column_descriptions: List[Dict[str, Any]],
    model_name: str = "",
) -> str:
    """
    使用 LLM 生成表的语义总结
    
    Args:
        database_name: 数据库名称
        table_name: 表名称
        table_info: 表元信息
        column_descriptions: 列描述列表
        model_name: 使用的模型名称
    
    Returns:
        表的语义总结
    """
    prompt = _build_table_summary_prompt(
        database_name, table_name, table_info, column_descriptions
    )
    
    try:
        model = get_llm(model_name or get_app_config().stages.general.summary.model_name)
        resp = model.invoke(prompt)
        raw_text = getattr(resp, "content", None)
        if raw_text is None:
            raw_text = str(resp)
        
        summary = raw_text.strip()
        
        # 清理可能的格式标记
        summary = summary.replace("## 概述", "").replace("##", "").strip()
        # 如果包含表名，尝试移除
        if summary.startswith(table_name) or summary.startswith(f"{table_name}表"):
            summary = summary.replace(table_name, "").replace("表", "").strip("：: ")
        
        # 严格限制长度（约 40-80 字符，对应 20-40 token）
        max_chars = 80
        if len(summary) > max_chars:
            summary = summary[:max_chars].rstrip("，。、") + "..."
        
        return summary
    except Exception as e:
        logger.exception(
            "生成表语义总结失败",
            exception=e,
            database=database_name,
            table=table_name
        )
        # 降级：返回基础描述
        table_comment = table_info.get("table_comment", "")
        if table_comment:
            return f"{table_name}表：{table_comment}"
        return f"{table_name}表：用于存储相关业务数据。"


def generate_table_readme(
    database_name: str,
    table_name: str,
    model_name: str = "",
) -> str:
    """
    生成表的 JSON 描述文件
    
    JSON 格式：
    {
        "description": "表的语义总结 - 20-40字",
        "columns": ["列名1", "列名2", ...],
        "rows": 表行数
    }
    
    Args:
        database_name: 数据库名称
        table_name: 表名称
        model_name: 用于生成语义总结的模型名称
    
    Returns:
        生成的 JSON 文件路径
    """
    logger.info(
        "开始生成表描述",
        database=database_name,
        table=table_name
    )
    
    # 获取表元信息
    table_info = _db_tool.get_table_info(database_name, table_name)
    
    # 加载列描述
    column_descriptions = _load_column_descriptions(database_name, table_name)
    
    # 生成表的语义总结
    table_summary = _generate_table_summary(
        database_name, table_name, table_info, column_descriptions, model_name
    )
    
    # 收集列名
    column_names = [col.get("column_name", "") for col in column_descriptions]
    
    # 构建 JSON 数据
    table_data = {
        "description": table_summary,
        "columns": column_names,
        "rows": table_info.get("table_rows") if table_info.get("table_rows") is not None else None,
    }
    
    # 保存 JSON 文件
    table_dir = DataPaths.default().table_description_path(database_name, table_name)
    table_dir.mkdir(parents=True, exist_ok=True)
    
    json_path = table_dir / f"TABLE_{table_name}.json"
    
    # 原子性写入
    temp_path = json_path.with_suffix(".tmp")
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(table_data, f, ensure_ascii=False, indent=2)
    
    # 备份旧文件
    if json_path.exists():
        backup_path = json_path.with_suffix(f".{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak")
        json_path.rename(backup_path)
    
    temp_path.rename(json_path)
    
    logger.info(
        "表描述生成完成",
        database=database_name,
        table=table_name,
        path=str(json_path)
    )
    
    return str(json_path)


def _build_database_summary_prompt(
    database_name: str,
    tables_info: List[Dict[str, Any]],
) -> str:
    """
    构建数据库语义总结的提示词
    
    Args:
        database_name: 数据库名称
        tables_info: 表信息列表，每个包含 table_name, table_comment, table_summary 等
    
    Returns:
        提示词文本
    """
    prompt_parts = [
        "你是一个数据库专家。请根据以下信息生成数据库的语义总结。",
        "",
        f"数据库名: {database_name}",
        f"包含表数量: {len(tables_info)}",
        "",
        "表信息：",
    ]
    
    for table_info in tables_info:
        table_name = table_info.get("table_name", "")
        table_summary = table_info.get("table_summary", "")
        table_comment = table_info.get("table_comment", "")
        
        table_desc = f"- {table_name}"
        if table_summary:
            table_desc += f": {table_summary}"
        elif table_comment:
            table_desc += f": {table_comment}"
        
        prompt_parts.append(table_desc)
    
    prompt_parts.append("")
    prompt_parts.append(
        "请生成一个极简的数据库语义总结（30-60字），只说明数据库的核心业务领域和用途，不要包含数据库名、不要重复表信息。"
    )
    prompt_parts.append("要求：直接输出总结文本，不要包含任何格式标记、标题或多余文字。")
    
    return "\n".join(prompt_parts)


def _generate_database_summary(
    database_name: str,
    tables_info: List[Dict[str, Any]],
    model_name: str = "",
) -> str:
    """
    使用 LLM 生成数据库的语义总结
    
    Args:
        database_name: 数据库名称
        tables_info: 表信息列表
        model_name: 使用的模型名称
    
    Returns:
        数据库的语义总结
    """
    prompt = _build_database_summary_prompt(database_name, tables_info)
    
    try:
        model = get_llm(model_name or get_app_config().stages.general.summary.model_name)
        resp = model.invoke(prompt)
        raw_text = getattr(resp, "content", None)
        if raw_text is None:
            raw_text = str(resp)
        
        summary = raw_text.strip()
        
        # 清理可能的格式标记
        summary = summary.replace("## 概述", "").replace("##", "").strip()
        # 如果包含数据库名，尝试移除
        if summary.startswith(database_name) or summary.startswith(f"{database_name}数据库"):
            summary = summary.replace(database_name, "").replace("数据库", "").strip("：: ")
        
        # 严格限制长度（约 60-120 字符，对应 30-60 token）
        max_chars = 120
        if len(summary) > max_chars:
            summary = summary[:max_chars].rstrip("，。、") + "..."
        
        return summary
    except Exception as e:
        logger.exception(
            "生成数据库语义总结失败",
            exception=e,
            database=database_name
        )
        # 降级：返回基础描述
        return f"{database_name}数据库：包含 {len(tables_info)} 个表，用于存储相关业务数据。"


def _load_table_summaries(database_name: str, tables: List[str]) -> List[Dict[str, Any]]:
    """
    加载所有表的总结信息
    
    Args:
        database_name: 数据库名称
        tables: 表名列表
    
    Returns:
        表信息列表，每个包含 table_name, table_comment, table_summary 等
    """
    tables_info = []
    
    for table_name in tables:
        table_info = _db_tool.get_table_info(database_name, table_name)
        
        # 尝试从表的 JSON 文件中读取总结
        table_summary = ""
        table_json_path = DataPaths.default().table_description_path(database_name, table_name) / f"TABLE_{table_name}.json"
        if table_json_path.exists():
            try:
                with open(table_json_path, "r", encoding="utf-8") as f:
                    table_data = json.load(f)
                    table_summary = table_data.get("description", "")
            except Exception as e:
                logger.warning(
                    "读取表 JSON 失败",
                    exception=e,
                    database=database_name,
                    table=table_name
                )
        
        tables_info.append({
            "table_name": table_name,
            "table_comment": table_info.get("table_comment", ""),
            "table_summary": table_summary,
            "table_rows": table_info.get("table_rows"),
        })
    
    return tables_info


def generate_database_readme(
    database_name: str,
    model_name: str = "",
) -> str:
    """
    生成数据库的 JSON 描述文件
    
    JSON 格式：
    {
        "description": "数据库的语义总结 - 30-60字",
        "tables": ["表名1", "表名2", ...],
        "table_nums": 表数量
    }
    
    Args:
        database_name: 数据库名称
        model_name: 用于生成语义总结的模型名称
    
    Returns:
        生成的 JSON 文件路径
    """
    logger.info(
        "开始生成数据库描述",
        database=database_name
    )
    
    # 获取所有表
    tables = _db_tool.list_tables(database_name)
    
    # 加载表总结信息
    tables_info = _load_table_summaries(database_name, tables)
    
    # 生成数据库的语义总结
    database_summary = _generate_database_summary(
        database_name, tables_info, model_name
    )
    
    # 构建 JSON 数据
    database_data = {
        "description": database_summary,
        "tables": tables,
        "table_nums": len(tables),
    }
    
    # 保存 JSON 文件
    db_dir = DataPaths.default().initialize_agent_database_dir(database_name)
    db_dir.mkdir(parents=True, exist_ok=True)
    
    json_path = db_dir / f"DATABASE_{database_name}.json"
    
    # 原子性写入
    temp_path = json_path.with_suffix(".tmp")
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(database_data, f, ensure_ascii=False, indent=2)
    
    # 备份旧文件
    if json_path.exists():
        backup_path = json_path.with_suffix(f".{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak")
        json_path.rename(backup_path)
    
    temp_path.rename(json_path)
    
    logger.info(
        "数据库描述生成完成",
        database=database_name,
        path=str(json_path)
    )
    
    return str(json_path)


__all__ = [
    "generate_table_readme",
    "generate_database_readme",
    "TARGET_TABLE_TOKEN_COUNT",
    "MAX_TABLE_TOKEN_COUNT",
    "TARGET_DATABASE_TOKEN_COUNT",
    "MAX_DATABASE_TOKEN_COUNT",
]
