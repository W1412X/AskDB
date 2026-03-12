"""
Embedding stage runner.

Storage design:
- Column description JSONs: data/initialize/agent/<db>/<table>/<column>.json
- Column embeddings (pickle): data/initialize/embedding/<db>/<table>/<column>.pkl
"""

from __future__ import annotations

import json
import pickle
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
import time

import numpy as np

from utils.data_paths import DataPaths
from utils.embedding import EmbeddingTool
from utils.logger import get_logger
from .build_text import build_semantic_description_from_json_file

logger = get_logger("initialize_embedding")


def _iter_column_json_paths(database_name: str) -> Iterable[Path]:
    base = DataPaths.default().initialize_agent_database_dir(database_name)
    if not base.exists():
        return []
    return base.rglob("*.json")


def embed_column_json(
    json_path: Path,
    embedding_tool: EmbeddingTool,
) -> Tuple[np.ndarray, str, Dict[str, str]]:
    """Read one column json -> build semantic text -> embed -> return."""
    start = time.time()
    with json_path.open("r", encoding="utf-8") as f:
        meta = json.load(f)
    text = build_semantic_description_from_json_file(json_path)
    vec = embedding_tool.embed(text)

    db = str(meta.get("database_name", "")).strip()
    table = str(meta.get("table_name", "")).strip()
    column = str(meta.get("column_name", "")).strip()
    logger.debug(
        "已嵌入单列 JSON",
        json_path=str(json_path),
        database_name=db,
        table_name=table,
        column_name=column,
        text_length=len(text),
        duration=time.time() - start,
    )
    return vec, text, {"database_name": db, "table_name": table, "column_name": column}


def save_column_embedding_pickle(
    database_name: str,
    table_name: str,
    column_name: str,
    embedding: np.ndarray,
    *,
    text: str,
    json_path: Optional[str] = None,
) -> Path:
    out_path = DataPaths.default().column_embedding_path(database_name, table_name, column_name)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "database_name": database_name,
        "table_name": table_name,
        "column_name": column_name,
        "text": text,
        "embedding": np.asarray(embedding, dtype=np.float32),
        "source_json": json_path,
    }
    with out_path.open("wb") as f:
        pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)
    logger.debug(
        "已保存列 embedding pickle",
        out_path=str(out_path),
        database_name=database_name,
        table_name=table_name,
        column_name=column_name,
        embedding_dim=int(np.asarray(embedding).shape[0]) if hasattr(np.asarray(embedding), "shape") else None,
        text_length=len(text),
    )
    return out_path


def build_embeddings_for_database(
    database_name: str,
    embedding_tool: EmbeddingTool,
    *,
    overwrite: bool = False,
) -> List[Path]:
    """Generate per-column embedding pickles for one database."""
    db_start = time.time()
    written: List[Path] = []
    scanned = 0
    skipped = 0
    failed = 0

    agent_dir = DataPaths.default().initialize_agent_database_dir(database_name)
    out_dir = DataPaths.default().initialize_embedding_database_dir(database_name)
    logger.info(
        "开始数据库 embedding",
        database_name=database_name,
        agent_dir=str(agent_dir),
        output_dir=str(out_dir),
        overwrite=overwrite,
    )

    for json_path in _iter_column_json_paths(database_name):
        scanned += 1
        try:
            with json_path.open("r", encoding="utf-8") as f:
                meta = json.load(f)
            db = str(meta.get("database_name", database_name)).strip() or database_name
            table = str(meta.get("table_name", "")).strip()
            column = str(meta.get("column_name", "")).strip()
            if not (table and column):
                skipped += 1
                continue

            out_path = DataPaths.default().column_embedding_path(db, table, column)
            if out_path.exists() and not overwrite:
                skipped += 1
                continue

            col_start = time.time()
            text = build_semantic_description_from_json_file(json_path)
            vec = embedding_tool.embed(text)
            saved = save_column_embedding_pickle(
                db,
                table,
                column,
                vec,
                text=text,
                json_path=str(json_path),
            )
            written.append(saved)
            logger.info(
                "已嵌入列",
                database_name=db,
                table_name=table,
                column_name=column,
                json_path=str(json_path),
                out_path=str(saved),
                duration=time.time() - col_start,
            )
        except Exception as e:
            failed += 1
            logger.exception(
                "列 embedding 失败",
                exception=e,
                database_name=database_name,
                json_path=str(json_path),
            )

    logger.info(
        "数据库 embedding 结束",
        database_name=database_name,
        scanned=scanned,
        written=len(written),
        skipped=skipped,
        failed=failed,
        duration=time.time() - db_start,
    )
    return written


def build_embeddings(
    database_names: List[str],
    *,
    model_name: str = "BAAI/bge-large-zh-v1.5",
    model_path: Optional[str] = None,
    hf_endpoint: Optional[str] = None,
    normalize_embeddings: bool = True,
    batch_size: int = 32,
    device: Optional[str] = None,
    local_files_only: bool = False,
    overwrite: bool = False,
) -> List[Path]:
    """
    Build per-column embedding pickles for multiple databases.
    model_path 指向的目录若不存在会从 HuggingFace（或 hf_endpoint）下载到该目录。
    """
    start = time.time()
    logger.workflow_start(
        "build_embeddings",
        database_names=database_names,
        model_name=model_name,
        normalize_embeddings=normalize_embeddings,
        batch_size=batch_size,
        device=device,
        overwrite=overwrite,
    )
    tool = EmbeddingTool(
        model_name=model_name,
        model_path=model_path,
        hf_endpoint=hf_endpoint,
        normalize_embeddings=normalize_embeddings,
        batch_size=batch_size,
        device=device,
        local_files_only=local_files_only,
    )
    all_written: List[Path] = []
    for db in database_names:
        all_written.extend(build_embeddings_for_database(db, tool, overwrite=overwrite))
    logger.workflow_end(
        "build_embeddings",
        duration=time.time() - start,
        written=len(all_written),
    )
    return all_written
