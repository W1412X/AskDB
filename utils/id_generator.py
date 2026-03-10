"""
全局 ID 生成器。

目标：
- 全局统一：所有需要生成“唯一 ID”的地方都从这里生成
- 可靠：避免程序重启后因时间/自增/随机种子导致的重复

实现说明：
- 默认使用 `uuid4`（基于系统 CSPRNG），跨进程/重启碰撞概率可忽略
- 格式：`{prefix}_{uuid_hex}` 或仅 `uuid_hex`
"""

from __future__ import annotations

from typing import Callable, Optional
from uuid import uuid4


def new_id(prefix: str | None = None) -> str:
    """
    Generate a globally unique id.

    - If `prefix` is provided (non-empty after strip), returns `{prefix}_{uuid_hex}`.
    - Otherwise returns `uuid_hex`.
    """
    suffix = uuid4().hex  # 32 lowercase hex chars
    if prefix is None:
        return suffix
    normalized = str(prefix).strip().rstrip("_")
    if not normalized:
        return suffix
    return f"{normalized}_{suffix}"


def ensure_id(value: Optional[str], generator: Callable[[], str]) -> str:
    """
    Return `value` if it is non-empty; otherwise generate a new id using `generator`.
    """
    if value is not None and str(value).strip():
        return str(value).strip()
    return generator()


def new_request_id() -> str:
    return new_id("req")


def new_trace_id() -> str:
    return new_id("trace")


def new_plan_id() -> str:
    return new_id("plan")


def new_task_id() -> str:
    return new_id("task")


def new_step_id() -> str:
    return new_id("step")


def new_tool_call_id() -> str:
    return new_id("tool")


def new_event_id() -> str:
    return new_id("evt")

