"""
工具注册中心：维护 Tool Agent 白名单与 4 个核心工具。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from stages.sql_generation.autolink.models import ToolAgentName


class ToolRegistry:
    def __init__(self, tools_by_agent: Dict[ToolAgentName, Sequence[Any]]) -> None:
        self._tools_by_agent: Dict[ToolAgentName, List[Any]] = {
            agent: list(tools) for agent, tools in tools_by_agent.items()
        }
        self._tool_index: Dict[str, Any] = {}
        self._build_index()

    def _build_index(self) -> None:
        self._tool_index.clear()
        for agent, tools in self._tools_by_agent.items():
            for tool in tools:
                name = str(getattr(tool, "name", "")).strip()
                if name:
                    self._tool_index[name] = tool

    def get_tools(self, agent: ToolAgentName | str) -> List[Any]:
        name = ToolAgentName(agent) if isinstance(agent, str) else agent
        return list(self._tools_by_agent.get(name, []))

    def get_tool(self, tool_name: str) -> Optional[Any]:
        return self._tool_index.get(tool_name)

    def has_tool(self, tool_name: str) -> bool:
        return tool_name in self._tool_index

    def tool_accepts_param(self, tool_name: str, param_name: str) -> bool:
        tool = self.get_tool(tool_name)
        if not tool:
            return False
        try:
            args = getattr(tool, "args", {}) or {}
            return param_name in args
        except Exception:
            return False


AUTOLINK_SQL_TOOL_NAMES = {"sql_explore", "sql_draft"}


def build_default_registry() -> ToolRegistry:
    from stages.sql_generation.autolink.tools import (
        schema_meta_tool,
        schema_retrieval_tool,
        sql_draft_tool,
        sql_explore_tool,
    )

    return ToolRegistry({
        ToolAgentName.SCHEMA_RETRIEVAL: [schema_retrieval_tool],
        ToolAgentName.SCHEMA_META: [schema_meta_tool],
        ToolAgentName.SCHEMA_EXPLORER: [sql_explore_tool, sql_draft_tool],
    })
