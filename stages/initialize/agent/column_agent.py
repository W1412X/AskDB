"""
列描述生成模块（简化版）

只做两件事：
- 基于“列事实信息”构建一次性 prompt
- 调用固定 LLM（一次调用）生成简洁 semantic_summary
"""

from __future__ import annotations

from typing import Any, Dict, Tuple

import json
import re
import time

from config.app_config import get_app_config
from config.llm_config import get_llm
from utils.logger import get_logger

logger = get_logger("column_agent")
_COLUMN_AGENT_CFG = get_app_config().stages.column_agent


def build_prompt(column_metadata: Dict[str, Any]) -> str:
    """基于列事实信息构建提示词（一次性生成）"""
    start_time = time.time()
    column_id = (
        f"{column_metadata.get('database_name', '')}."
        f"{column_metadata.get('table_name', '')}."
        f"{column_metadata.get('column_name', '')}"
    )

    logger.function_call("build_prompt", inputs={"column_id": column_id})

    prompt_parts = [
        "你是一个数据库schema解读专家。你的输出将用于后续向量检索，请避免废话。",
        f"\n数据库: {column_metadata.get('database_name', '')}",
        f"表: {column_metadata.get('table_name', '')}",
        f"列: {column_metadata.get('column_name', '')}",
        f"数据类型: {column_metadata.get('data_type', '')}",
        f"是否可空: {column_metadata.get('is_nullable', '')}",
    ]

    if column_metadata.get("comment"):
        prompt_parts.append(f"列注释: {column_metadata.get('comment')}")

    if column_metadata.get("is_primary_key"):
        prompt_parts.append("约束: 主键")
    if column_metadata.get("is_foreign_key"):
        prompt_parts.append(f"约束: 外键，引用: {column_metadata.get('foreign_key_ref', '')}")
    if column_metadata.get("is_auto_increment"):
        prompt_parts.append("属性: 自增列")

    if column_metadata.get("indexes"):
        prompt_parts.append(f"索引(JSON): {json.dumps(column_metadata.get('indexes'), ensure_ascii=False)}")

    if column_metadata.get("samples"):
        prompt_parts.append(f"\n样本数据(JSON): {json.dumps(column_metadata.get('samples'), ensure_ascii=False)}")
    if column_metadata.get("table_context_samples"):
        prompt_parts.append(
            f"\n表上下文样本(JSON): {json.dumps(column_metadata.get('table_context_samples'), ensure_ascii=False)}"
        )

    col_name = column_metadata.get("column_name", "")
    prompt_parts.append("\n请生成该列的语义总结：描述该列在当前表/数据库/业务中的作用，保持简洁。")
    prompt_parts.append("同时生成该列的语义关键词（用于向量检索）。")
    prompt_parts.append("\n输出格式（必须严格为 JSON，不要 Markdown，不要换行）：")
    prompt_parts.append(
        '{"semantic_summary":"<一句话作用描述，20-80字，缺信息写未知，必须以'
        + str(col_name)
        + '列：开头>","semantic_keywords":["<6-12个中文关键词/短语>"]}'
    )
    prompt_parts.append("\n强约束（必须遵守）：")
    prompt_parts.append("1. 只输出一个 JSON 对象；不要输出任何解释、推理过程、开场白、结尾语。")
    prompt_parts.append("2. semantic_summary 必须聚焦“作用/角色”，不要复述约束/类型等元数据。")
    prompt_parts.append("3. semantic_keywords 必须是中文语义关键词/短语（名词为主），不得包含数据库名/表名/列名、不得使用英文 snake_case。")
    prompt_parts.append("4. 关键词用于向量检索：尽量包含同义词、业务概念、实体+属性组合（如“设备名称/工厂标识/安装日期/设备状态/设备类型”等）。")

    prompt = "\n".join(prompt_parts)
    duration = time.time() - start_time
    logger.function_result(
        "build_prompt",
        result=f"提示词构建完成，长度: {len(prompt)}",
        duration=duration,
        column_id=column_id,
        prompt_length=len(prompt),
    )
    return prompt


def _extract_first_json_object(text: str) -> dict[str, Any] | None:
    """Extract and parse first JSON object from model output."""
    if not text:
        return None
    s = text.strip()
    # Fast path
    if s.startswith("{") and s.endswith("}"):
        try:
            obj = json.loads(s)
            return obj if isinstance(obj, dict) else None
        except Exception:
            pass
    # Fallback: find the first {...} block
    m = re.search(r"\{[\s\S]*\}", s)
    if not m:
        return None
    try:
        obj = json.loads(m.group(0))
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def generate_semantic_summary_and_keywords(
    column_metadata: Dict[str, Any],
    model_name: str = "",
) -> Tuple[str, list[str]]:
    """一次性生成 semantic_summary + semantic_keywords（单次LLM调用）。"""
    start_time = time.time()
    column_id = (
        f"{column_metadata.get('database_name', '')}."
        f"{column_metadata.get('table_name', '')}."
        f"{column_metadata.get('column_name', '')}"
    )

    prompt = build_prompt(column_metadata)
    model = get_llm(model_name or get_app_config().stages.initialize.agent.model_name)

    logger.info("开始生成列语义总结/关键词", column_id=column_id, model_name=model_name)
    resp = model.invoke(prompt)
    raw_text = getattr(resp, "content", None)
    if raw_text is None:
        raw_text = str(resp)

    obj = _extract_first_json_object(str(raw_text))

    semantic_summary = ""
    semantic_keywords: list[str] = []
    if obj:
        semantic_summary = str(obj.get("semantic_summary", "") or "").strip()
        kws = obj.get("semantic_keywords")
        if isinstance(kws, list):
            semantic_keywords = [str(x).strip() for x in kws if str(x).strip()]

    col = str(column_metadata.get("column_name", "")).strip() or "该"
    if not semantic_summary.startswith(f"{col}列"):
        # 如果模型没按格式输出，强制补前缀
        semantic_summary = f"{col}列：{semantic_summary or '未知'}"

    max_length = int((_COLUMN_AGENT_CFG.token.get("max_tokens_per_field") or {}).get("semantic_summary", 1000) or 1000)
    if len(semantic_summary) > max_length:
        semantic_summary = semantic_summary[:max_length] + "...[已截断]"

    # 只做最轻量的规范化：去重 + 截断数量（其余质量靠 prompt 约束）
    seen: set[str] = set()
    deduped: list[str] = []
    for kw in semantic_keywords:
        kw = str(kw).strip()
        if not kw:
            continue
        if kw in seen:
            continue
        seen.add(kw)
        deduped.append(kw)
    semantic_keywords = deduped[:12]

    duration = time.time() - start_time
    logger.info(
        "列语义总结/关键词生成完成",
        column_id=column_id,
        duration=duration,
        semantic_summary_length=len(semantic_summary),
        keyword_count=len(semantic_keywords),
    )
    return semantic_summary, semantic_keywords


__all__ = ["generate_semantic_summary_and_keywords", "build_prompt"]
