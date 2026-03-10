"""
意图分解模块主入口。
"""

from __future__ import annotations

from typing import List, Optional

from config.app_config import get_app_config
from stages.intent_divide.divider import IntentDivider
from stages.intent_divide.models import IntentDivideOutput
from stages.intent_divide.tracing import DivideAudit
from utils.logger import get_logger

logger = get_logger("intent_divide")
_CFG = get_app_config().stages.intent_divide


def divide_intents(
    query: str,
    database_names: List[str],
    model_name: Optional[str] = None,
    max_retry_attempts: Optional[int] = None,
    verbose: bool = False,
) -> IntentDivideOutput:
    if not query or not query.strip():
        raise ValueError("query is required")
    if not database_names:
        database_names = get_app_config().get_default_database_scope()
    if not database_names:
        raise ValueError("database_names is required")

    retries = max_retry_attempts if max_retry_attempts is not None else _CFG.max_retry_attempts
    resolved_model_name = str(model_name or _CFG.model_name)
    divider = IntentDivider(model_name=resolved_model_name, max_retry_attempts=retries, verbose=verbose)
    logger.info(
        "开始意图分解",
        query=query,
        database_names=database_names,
        model_name=resolved_model_name,
        max_retry_attempts=retries,
        verbose=verbose,
    )
    output = divider.divide(query=query, database_names=database_names)
    logger.info("意图分解完成", intents_count=len(output.intents))
    return output


def divide_intents_with_audit(
    query: str,
    database_names: List[str],
    model_name: Optional[str] = None,
    max_retry_attempts: Optional[int] = None,
    verbose: bool = False,
) -> tuple[IntentDivideOutput, DivideAudit]:
    """
    Like divide_intents, but also returns an auditable trace.
    """
    if not query or not query.strip():
        raise ValueError("query is required")
    if not database_names:
        database_names = get_app_config().get_default_database_scope()
    if not database_names:
        raise ValueError("database_names is required")

    retries = max_retry_attempts if max_retry_attempts is not None else _CFG.max_retry_attempts
    resolved_model_name = str(model_name or _CFG.model_name)
    divider = IntentDivider(model_name=resolved_model_name, max_retry_attempts=retries, verbose=verbose)
    logger.info(
        "开始意图分解（带审计）",
        query=query,
        database_names=database_names,
        model_name=resolved_model_name,
        max_retry_attempts=retries,
        verbose=verbose,
    )
    output, audit = divider.divide_with_audit(query=query, database_names=database_names)
    logger.info("意图分解完成（带审计）", intents_count=len(output.intents), trace_id=audit.trace_id)
    return output, audit
