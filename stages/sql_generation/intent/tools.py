"""
Tools exposed to intent module agents.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from config.app_config import get_app_config
from config.llm_config import get_llm

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


@tool(name_or_callable="autolink", description="调用 AutoLink（BUILD/ENRICH/ERROR）生成或修复 schema。")
def autolink_tool(
    request: str,
    request_type: str,
    context: Dict[str, Any],
    schema_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    from stages.sql_generation.autolink import run_autolink

    payload = {
        "request": request,
        "request_type": request_type,
        "schema": schema_data,
        "context": context,
    }
    model_name = str((context or {}).get("model_name") or get_app_config().stages.sql_generation.autolink.model_name)
    out = run_autolink(payload, model=get_llm(model_name))
    return out.model_dump(mode="json", by_alias=True)


@tool(name_or_callable="ask_user", description="生成澄清请求 payload。持久化由上层 runtime 负责。")
def ask_user_tool(
    intent_id: str,
    question_id: str,
    ask: Dict[str, Any],
    acceptance_criteria: List[str],
    priority: int = 1,
    max_turns: int = 3,
    thread_id: str = "",
    state_summary: str = "",
) -> Dict[str, Any]:
    payload = {
        "intent_id": str(intent_id),
        "question_id": str(question_id),
        "priority": int(priority),
        "state_summary": str(state_summary or ""),
        "ask": dict(ask or {}),
        "acceptance_criteria": list(acceptance_criteria or []),
        "max_turns": int(max_turns),
        "thread_id": str(thread_id or ""),
    }
    return {"ok": True, "ticket_payload": payload}
