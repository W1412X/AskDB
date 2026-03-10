"""
AutoLink 多 Agent Schema 链接模块。

基于用户需求，输出满足 SQL 生成所需的最小完备 schema（db-tb-column 三层 JSON），
并附带可回放的工作流审计记录。
"""

from stages.sql_generation.autolink.runtime import run_autolink
from stages.sql_generation.autolink.models import (
    AutolinkRequest,
    AutolinkOutput,
    AutolinkContext,
    RequestType,
    RunStatus,
    Schema,
)

__all__ = [
    "run_autolink",
    "AutolinkRequest",
    "AutolinkOutput",
    "AutolinkContext",
    "RequestType",
    "RunStatus",
    "Schema",
]
