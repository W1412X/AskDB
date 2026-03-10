"""
sql_generation 阶段入口（当前先提供 DAG 构建与调度入口）。
"""

from __future__ import annotations

from typing import Any, Sequence

from config.app_config import get_app_config
from stages.sql_generation.dag import DAGScheduler, SchedulerConfig


def build_intent_dag_scheduler(
    intents: Sequence[Any],
    *,
    max_concurrency: int | None = None,
) -> DAGScheduler:
    """
    基于上一阶段 intent 列表构建自研 DAG 调度器。
    """
    return DAGScheduler(
        intents=intents,
        config=SchedulerConfig(max_concurrency=int(max_concurrency or get_app_config().stages.sql_generation.pipeline.max_concurrency)),
    )
