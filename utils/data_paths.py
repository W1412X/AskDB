"""
Centralized project data path utilities.

Goal: avoid repeating ad-hoc `base_dir = dirname(dirname(...(__file__)))` patterns.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DataPaths:
    """
    Project path helper (rooted at `src/`).

    Directory layout expected:
    - <project_root>/data/...
    - <project_root>/stages/...
    - <project_root>/utils/...
    """

    project_root: Path

    @classmethod
    def default(cls) -> "DataPaths":
        # utils/data_paths.py -> utils -> src
        root = Path(__file__).resolve().parents[1]
        return cls(project_root=root)

    # --- base dirs ---
    def data_dir(self) -> Path:
        return self.project_root / "data"

    # --- initialize stage ---
    def initialize_dir(self) -> Path:
        return self.data_dir() / "initialize"

    # --- initialize subdirs (new storage design) ---
    def initialize_agent_dir(self) -> Path:
        """Agent stage outputs (column description JSONs)."""
        return self.initialize_dir() / "agent"

    def initialize_embedding_dir(self) -> Path:
        """Embedding stage outputs (per-column embedding pickles)."""
        return self.initialize_dir() / "embedding"

    def initialize_checkpoints_dir(self) -> Path:
        return self.initialize_dir() / "checkpoints"

    def initialize_progress_dir(self) -> Path:
        return self.initialize_dir() / "progress"

    def initialize_token_usage_dir(self) -> Path:
        return self.initialize_dir() / "token_usage"

    def initialize_agent_database_dir(self, database_name: str) -> Path:
        return self.initialize_agent_dir() / database_name

    def initialize_embedding_database_dir(self, database_name: str) -> Path:
        return self.initialize_embedding_dir() / database_name

    # --- column description JSON (agent stage) ---
    def table_description_path(self, database_name: str, table_name: str) -> Path:
        """Directory for a table's column JSONs."""
        return self.initialize_agent_dir() / database_name / table_name

    def column_description_path(self, database_name: str, table_name: str, column_name: str) -> Path:
        """Path to a column description JSON produced by initialize agent stage."""
        return self.table_description_path(database_name, table_name) / f"{column_name}.json"

    # --- column embedding pickle (embedding stage) ---
    def column_embedding_path(self, database_name: str, table_name: str, column_name: str) -> Path:
        """Path to a per-column embedding pickle produced by embedding stage."""
        return self.initialize_embedding_dir() / database_name / table_name / f"{column_name}.pkl"

    # --- legacy (pre-split) paths (optional compatibility) ---
    def legacy_initialize_database_dir(self, database_name: str) -> Path:
        """Legacy initialize outputs dir: data/initialize/<db> (before split into agent/embedding)."""
        return self.initialize_dir() / database_name

    def legacy_column_description_path(self, database_name: str, table_name: str, column_name: str) -> Path:
        return self.legacy_initialize_database_dir(database_name) / table_name / f"{column_name}.json"

    def find_column_description_path(self, database_name: str, table_name: str, column_name: str) -> Path:
        """
        Prefer new agent path; fall back to legacy path if needed.
        """
        p = self.column_description_path(database_name, table_name, column_name)
        if p.exists():
            return p
        legacy = self.legacy_column_description_path(database_name, table_name, column_name)
        return legacy
    @classmethod
    def model_embedding_path(cls, model_name: str) -> Path:
        """
        Return local embedding model directory path.

        This is a convenience helper so callers can use either:
        - `DataPaths.model_embedding_path("bge-small-zh-v1.5")`
        - `DataPaths.default().model_embedding_path("bge-small-zh-v1.5")`
        """
        return cls.default().data_dir() / "models" / "embedding" / model_name

__all__ = ["DataPaths"]

