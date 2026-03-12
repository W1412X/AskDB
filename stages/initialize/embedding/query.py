from typing import List, Dict, Any
from config.app_config import get_app_config
from pathlib import Path
from utils.data_paths import DataPaths
from utils.embedding import EmbeddingTool
import pickle
import numpy as np
import os
from utils.logger import get_logger
logger = get_logger("initialize_embedding_query")
_EMBED_CFG = get_app_config().stages.initialize.embedding

_embedding_tool: EmbeddingTool | None = None
_warned_missing_db_dirs: set[str] = set()


def _get_embedding_tool() -> EmbeddingTool:
    """Lazy-load embedding tool so FastAPI can start without loading the model at import time."""
    global _embedding_tool
    if _embedding_tool is None:
        _embedding_tool = EmbeddingTool(model_path=DataPaths.model_embedding_path(_EMBED_CFG.model_path_name))
    return _embedding_tool
def get_column_embedding(db:str,table:str,column:str) -> np.ndarray:
    """获取指定数据库/表/列中的embedding"""
    embedding_path = DataPaths.default().column_embedding_path(db, table, column)
    with embedding_path.open("rb") as f:
        payload = pickle.load(f)
    # payload is expected to be a dict like {"embedding": np.ndarray, ...}
    if isinstance(payload, dict) and "embedding" in payload:
        return np.asarray(payload["embedding"], dtype=np.float32)
    # fallback: allow raw vector pickle
    return np.asarray(payload, dtype=np.float32)

def get_columns_by_text(text:str,databases: List[str]) -> List[Dict[str, Any]]:
    """获取指定数据库/表/列中的所有列中，对应的embedding与textembedding的相似度，形成一个列表为
    [
        {
            "database_name": "database_name",
            "table_name": "table_name",
            "column_name": "column_name",
            "similarity": 0.99,
        },
        ...
    ]
    
    注意：只处理列级 JSON 文件，会排除 TABLE_*.json 和 DATABASE_*.json 等元数据文件。
    """
    columns=[]
    skipped_metadata = 0
    skipped_missing_embedding = 0
    
    for db_name in databases:
        db_path = DataPaths.default().initialize_agent_database_dir(db_name)
        if not os.path.exists(db_path):
            key = f"{db_name}:{db_path}"
            if key not in _warned_missing_db_dirs:
                _warned_missing_db_dirs.add(key)
                logger.warning(
                    "数据库目录不存在，跳过（每库仅提示一次）",
                    database_name=db_name,
                    path=str(db_path)
                )
            continue
            
        table_paths=os.listdir(db_path)
        for table_path in table_paths:
            table_dir = os.path.join(db_path, table_path)
            if not os.path.isdir(table_dir):
                continue
            column_paths=[i for i in os.listdir(table_dir) if i.endswith(".json")]
            for column_path in column_paths:
                # 排除元数据文件：TABLE_*.json 和 DATABASE_*.json
                if column_path.startswith("TABLE_") or column_path.startswith("DATABASE_"):
                    skipped_metadata += 1
                    continue
                    
                column_name=column_path.split(".")[0]
                columns.append({
                    "database_name": db_name,
                    "table_name": table_path,
                    "column_name": column_name,
                    "similarity": 0,
                })
    
    # 计算相似度，跳过缺失 embedding 的列
    valid_columns = []
    for column in columns:
        try:
            embedding = get_column_embedding(column["database_name"], column["table_name"], column["column_name"])
            similarity = _get_embedding_tool().get_similarity(text, embedding)
            column["similarity"] = similarity
            valid_columns.append(column)
        except FileNotFoundError:
            skipped_missing_embedding += 1
            logger.debug(
                "列 embedding 文件不存在，跳过",
                database_name=column["database_name"],
                table_name=column["table_name"],
                column_name=column["column_name"]
            )
        except Exception as e:
            logger.warning(
                "读取列 embedding 失败，跳过",
                exception=str(e),
                database_name=column["database_name"],
                table_name=column["table_name"],
                column_name=column["column_name"]
            )
    
    if skipped_metadata > 0 or skipped_missing_embedding > 0:
        logger.debug(
            "按文本检索列统计",
            total_candidates=len(columns),
            valid_columns=len(valid_columns),
            skipped_metadata=skipped_metadata,
            skipped_missing_embedding=skipped_missing_embedding
        )
    
    return valid_columns
