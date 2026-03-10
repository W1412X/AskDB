"""
Shared LLM utilities for intent module.

Reuse AutoLink's format-retry mechanism to keep behavior consistent.
"""

from stages.sql_generation.autolink.llm_utils import (
    LLMCallPolicy,
    classify_llm_error,
    default_llm_call_policy,
    invoke_llm,
    invoke_llm_with_format_retry,
    invoke_messages_with_policy,
)

__all__ = [
    "LLMCallPolicy",
    "classify_llm_error",
    "default_llm_call_policy",
    "invoke_llm",
    "invoke_llm_with_format_retry",
    "invoke_messages_with_policy",
]
