"""
意图分解工具定义（LangChain Tool）。
"""

from __future__ import annotations

from typing import Any, Dict, List

from langchain.tools import tool

from config.app_config import get_app_config
from stages.initialize.embedding.query import get_columns_by_text

_CFG = get_app_config().stages.intent_divide


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


@tool(
    name_or_callable="query_columns_by_text",
    description="根据文本描述查询相关列。输入text和databases，返回按相似度排序的列信息列表。"
)
def query_columns_by_text_tool(
    text: str,
    databases: List[str],
    top_k: int = _CFG.column_query_top_k,
) -> List[Dict[str, Any]]:
    columns = get_columns_by_text(text=text, databases=databases)
    columns = sorted(columns, key=lambda x: x.get("similarity", 0), reverse=True)
    return columns[:top_k]


def get_intent_divide_tools() -> List[Any]:
    return [query_columns_by_text_tool, ask_user_tool]
