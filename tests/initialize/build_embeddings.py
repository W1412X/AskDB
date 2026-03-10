import os 
import sys

# 将src目录添加到Python路径（在导入之前）
src_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, src_dir)

from utils.data_paths import DataPaths
from utils.embedding import EmbeddingTool 
from stages.initialize.embedding.build_embedding import build_embeddings_for_database

if __name__ == "__main__":
    embedding_tool = EmbeddingTool(model_path=DataPaths.model_embedding_path("bge-large-zh-v1.5"))
    build_embeddings_for_database("industrial_monitoring", embedding_tool)