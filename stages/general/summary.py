"""
文本摘要生成模块

提供统一的文本摘要功能，支持多种模型选择，对输入文本有最大长度限制。
"""

from __future__ import annotations

from typing import Optional
import time

from config.app_config import get_app_config
from config.llm_config import get_llm
from utils.logger import get_logger

logger = get_logger("summary_agent")

# 默认配置
_SUMMARY_CFG = get_app_config().stages.general.summary
DEFAULT_MAX_INPUT_LENGTH = _SUMMARY_CFG.max_input_length
DEFAULT_MODEL_NAME = _SUMMARY_CFG.model_name


def build_summary_prompt(text: str, max_summary_length: Optional[int] = None) -> str:
    """
    构建摘要提示词
    
    Args:
        text: 待摘要的文本
        max_summary_length: 摘要的最大长度（字符数），如果为None则不限制
    
    Returns:
        构建好的提示词
    """
    prompt_parts = [
        "你是一个专业的文本摘要助手。请对以下文本进行摘要，要求：",
        "1. 保留文本的核心信息和关键要点",
        "2. 摘要应该简洁明了，逻辑清晰",
        "3. 如果原文有多个段落或主题，请保持结构层次",
        "4. 使用中文进行摘要（除非原文是其他语言）",
    ]
    
    if max_summary_length:
        prompt_parts.append(f"5. 摘要长度控制在 {max_summary_length} 字符以内")
    
    prompt_parts.append("\n待摘要文本：")
    prompt_parts.append("---")
    prompt_parts.append(text)
    prompt_parts.append("---")
    prompt_parts.append("\n请生成摘要：")
    
    return "\n".join(prompt_parts)


def summarize_text(
    text: str,
    model_name: str = DEFAULT_MODEL_NAME,
    max_input_length: int = DEFAULT_MAX_INPUT_LENGTH,
    max_summary_length: Optional[int] = None,
) -> str:
    """
    对输入的文本进行摘要
    
    Args:
        text: 待摘要的文本
        model_name: 使用的模型名称，支持 'qwen3-max', 'deepseek-chat', 'gpt-5.2'
        max_input_length: 输入文本的最大长度（字符数），超过此长度将被截断
        max_summary_length: 摘要的最大长度（字符数），如果为None则不限制
    
    Returns:
        摘要结果
    
    Raises:
        ValueError: 当输入文本为空时
    """
    start_time = time.time()
    
    if not text or not text.strip():
        raise ValueError("输入文本不能为空")
    
    # 检查并截断输入文本
    original_length = len(text)
    if original_length > max_input_length:
        logger.warning(
            "输入文本超过最大长度限制，将被截断",
            original_length=original_length,
            max_input_length=max_input_length,
        )
        text = text[:max_input_length] + "\n\n[文本已截断...]"
    
    logger.info(
        "开始生成文本摘要",
        model_name=model_name,
        input_length=len(text),
        max_summary_length=max_summary_length,
    )
    
    # 构建提示词
    prompt = build_summary_prompt(text, max_summary_length)
    
    # 调用LLM生成摘要
    try:
        model = get_llm(model_name)
        resp = model.invoke(prompt)
        raw_text = getattr(resp, "content", None)
        if raw_text is None:
            raw_text = str(resp)
        
        summary = raw_text.strip()
        
        # 如果指定了最大摘要长度，进行截断
        if max_summary_length and len(summary) > max_summary_length:
            logger.warning(
                "摘要超过最大长度限制，将被截断",
                summary_length=len(summary),
                max_summary_length=max_summary_length,
            )
            summary = summary[:max_summary_length] + "..."
        
        duration = time.time() - start_time
        logger.info(
            "文本摘要生成完成",
            model_name=model_name,
            duration=duration,
            input_length=original_length,
            summary_length=len(summary),
        )
        
        return summary
        
    except Exception as e:
        duration = time.time() - start_time
        logger.exception(
            "文本摘要生成失败",
            exception=e,
            model_name=model_name,
            duration=duration,
        )
        raise


__all__ = ["summarize_text", "build_summary_prompt", "DEFAULT_MAX_INPUT_LENGTH", "DEFAULT_MODEL_NAME"]
