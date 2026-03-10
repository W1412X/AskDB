"""\
Entry point for database column initialization.
"""

from typing import List, Optional
from datetime import datetime
import os
import time
from config.app_config import get_app_config
from .workflow import run_initialize
from .state import StateManager
from utils.data_paths import DataPaths
from utils.logger import get_logger

logger = get_logger("agent")


def initialize_databases(
    database_names: Optional[List[str]] = None,
    checkpoint_dir: Optional[str] = None,
    progress_log_dir: Optional[str] = None,  # kept for backward compatibility (unused)
    token_usage_dir: Optional[str] = None,  # kept for backward compatibility (unused)
    model_name: Optional[str] = None,
):
    """Initialize column descriptions for databases (simplified state machine)."""
    resolved_database_names = list(database_names or get_app_config().get_initialize_databases())
    resolved_model_name = str(model_name or get_app_config().stages.initialize.agent.model_name)
    workflow_start_time = time.time()

    logger.workflow_start(
        "initialize_databases",
        database_names=resolved_database_names,
        database_count=len(resolved_database_names)
    )

    try:
        if checkpoint_dir is None:
            checkpoint_dir = str(DataPaths.default().initialize_checkpoints_dir())

        logger.info(
            "初始化目录",
            checkpoint_dir=checkpoint_dir
        )

        state_manager = StateManager(checkpoint_dir)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if progress_log_dir is not None or token_usage_dir is not None:
            logger.warning(
                "简化工作流中忽略 progress_log_dir/token_usage_dir",
                progress_log_dir=progress_log_dir,
                token_usage_dir=token_usage_dir,
            )

        logger.info(
            "运行简化工作流",
            timestamp=timestamp,
            database_count=len(resolved_database_names),
            model_name=resolved_model_name,
        )

        result = run_initialize(
            database_names=resolved_database_names,
            state_manager=state_manager,
            timestamp=timestamp,
            model_name=resolved_model_name,
        )

        workflow_duration = time.time() - workflow_start_time

        databases = result.get("databases", [])
        total_databases = len(databases)
        total_tables = sum(len(db.tables) for db in databases)
        total_columns = sum(
            len(table.columns)
            for db in databases
            for table in db.tables
        )

        logger.workflow_end(
            "initialize_databases",
            duration=workflow_duration,
            total_databases=total_databases,
            total_tables=total_tables,
            total_columns=total_columns
        )

        logger.info(
            "数据库初始化完成",
            duration=workflow_duration,
            total_databases=total_databases,
            total_tables=total_tables,
            total_columns=total_columns
        )

        return result

    except Exception as e:
        workflow_duration = time.time() - workflow_start_time
        logger.exception(
            "数据库初始化失败",
            exception=e,
            duration=workflow_duration,
            database_names=resolved_database_names
        )
        raise


if __name__ == "__main__":
    result = initialize_databases()
    print(result)
