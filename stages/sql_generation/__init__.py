"""
SQL generation package.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "build_intent_dag_scheduler",
    "run_sql_generation_stage",
    "resume_sql_generation_stage_after_user_reply",
    "SQLStageResult",
    "StageStatus",
    "WorkerRuntimeConfig",
    "LocalIsolatedThreadWorkerExecutor",
    "run_scheduler_until_blocked_or_done",
]


def build_intent_dag_scheduler(*args: Any, **kwargs: Any) -> Any:
    from stages.sql_generation.main import build_intent_dag_scheduler as _impl

    return _impl(*args, **kwargs)


def run_sql_generation_stage(*args: Any, **kwargs: Any) -> Any:
    from stages.sql_generation.pipeline import run_sql_generation_stage as _impl

    return _impl(*args, **kwargs)


def resume_sql_generation_stage_after_user_reply(*args: Any, **kwargs: Any) -> Any:
    from stages.sql_generation.pipeline import resume_sql_generation_stage_after_user_reply as _impl

    return _impl(*args, **kwargs)


def __getattr__(name: str) -> Any:
    if name in {"SQLStageResult", "StageStatus"}:
        from stages.sql_generation.pipeline import SQLStageResult, StageStatus

        return {"SQLStageResult": SQLStageResult, "StageStatus": StageStatus}[name]
    if name in {"WorkerRuntimeConfig", "LocalIsolatedThreadWorkerExecutor", "run_scheduler_until_blocked_or_done"}:
        from stages.sql_generation.worker_runtime import (
            LocalIsolatedThreadWorkerExecutor,
            WorkerRuntimeConfig,
            run_scheduler_until_blocked_or_done,
        )

        return {
            "WorkerRuntimeConfig": WorkerRuntimeConfig,
            "LocalIsolatedThreadWorkerExecutor": LocalIsolatedThreadWorkerExecutor,
            "run_scheduler_until_blocked_or_done": run_scheduler_until_blocked_or_done,
        }[name]
    raise AttributeError(name)
