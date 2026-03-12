"""
Shared helpers for checking and running initialization (used by CLI and Web API).
"""

from __future__ import annotations

from pathlib import Path
from typing import List

from config import get_app_config
from utils.data_paths import DataPaths


def _column_json_paths(database_name: str) -> List[Path]:
    base = DataPaths.default().initialize_agent_database_dir(database_name)
    if not base.exists():
        return []
    return sorted(
        p
        for p in base.rglob("*.json")
        if not p.name.startswith("TABLE_") and not p.name.startswith("DATABASE_")
    )


def _embedding_paths(database_name: str) -> List[Path]:
    base = DataPaths.default().initialize_embedding_database_dir(database_name)
    if not base.exists():
        return []
    return sorted(base.rglob("*.pkl"))


def _needs_initialize_agent(database_name: str) -> bool:
    return len(_column_json_paths(database_name)) == 0


def _needs_initialize_embedding(database_name: str) -> bool:
    column_jsons = _column_json_paths(database_name)
    if not column_jsons:
        return True
    return len(_embedding_paths(database_name)) < len(column_jsons)


def is_initialized() -> bool:
    """
    True if all target databases have agent outputs and embeddings.
    Used by Web UI to decide whether to allow query.
    """
    cfg = get_app_config()
    target = sorted(
        set(cfg.get_initialize_databases()) | set(cfg.get_default_database_scope())
    )
    if not target:
        return False
    for db in target:
        if _needs_initialize_agent(db) or _needs_initialize_embedding(db):
            return False
    return True


def _apply_hf_endpoint_from_env() -> None:
    """Reload .env and apply HF_ENDPOINT so huggingface_hub uses mirror (must run before any HF download)."""
    import os
    import importlib

    project_root = Path(__file__).resolve().parents[1]
    dotenv_path = project_root / ".env"
    if dotenv_path.exists():
        from dotenv import load_dotenv
        load_dotenv(dotenv_path, override=True)
    _set_hf_endpoint_and_reload(os.environ.get("HF_ENDPOINT"))


def _set_hf_endpoint_and_reload(endpoint: str | None) -> None:
    """Set HF_ENDPOINT and reload huggingface_hub constants so mirror is used (call before any HF access)."""
    import os
    import importlib

    if not endpoint or not str(endpoint).strip():
        return
    ep = str(endpoint).strip().rstrip("/")
    os.environ["HF_ENDPOINT"] = ep
    try:
        import huggingface_hub.constants as hh_constants
        importlib.reload(hh_constants)
    except Exception:
        pass


def run_initialize() -> None:
    """
    Run initialize agent + embedding for all target databases.
    Raises on error.
    """
    _apply_hf_endpoint_from_env()

    from config import get_app_config
    from stages.initialize.agent.run import initialize_databases
    from stages.initialize.embedding.build_embedding import build_embeddings
    from utils.data_paths import DataPaths

    cfg = get_app_config()
    # 尽早应用 stages.json 中的 hf_endpoint，确保后续 HF/ST 请求都走镜像
    emb_cfg = getattr(cfg.stages.initialize, "embedding", None)
    if emb_cfg is not None:
        hf_ep = getattr(emb_cfg, "hf_endpoint", None) or ""
        if hf_ep and str(hf_ep).strip():
            _set_hf_endpoint_and_reload(str(hf_ep).strip())
    target_databases = sorted(
        set(cfg.get_initialize_databases()) | set(cfg.get_default_database_scope())
    )
    if not target_databases:
        raise RuntimeError("No databases configured for initialize/query flow.")

    missing_agent = [db for db in target_databases if _needs_initialize_agent(db)]
    if missing_agent:
        initialize_databases(
            database_names=missing_agent,
            model_name=cfg.stages.initialize.agent.model_name,
        )

    missing_embedding = [db for db in target_databases if _needs_initialize_embedding(db)]
    if missing_embedding:
        emb_cfg = cfg.stages.initialize.embedding
        model_path = None
        local_only = False
        if getattr(emb_cfg, "model_path_name", None):
            candidate = DataPaths.model_embedding_path(str(emb_cfg.model_path_name))
            model_path = str(candidate)
            local_only = candidate.exists()
        hf_endpoint = getattr(emb_cfg, "hf_endpoint", None) or ""
        build_embeddings(
            database_names=missing_embedding,
            model_name=emb_cfg.model_name,
            model_path=model_path,
            hf_endpoint=hf_endpoint.strip() or None,
            normalize_embeddings=emb_cfg.normalize_embeddings,
            batch_size=emb_cfg.batch_size,
            device=emb_cfg.device or None,
            local_files_only=local_only,
            overwrite=emb_cfg.overwrite,
        )
