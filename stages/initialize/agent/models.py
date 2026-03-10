"""
数据模型定义
"""

from pydantic import BaseModel
from typing import Optional, List, Dict, Any


class IndexInfo(BaseModel):
    index_name: str
    index_type: str
    is_unique: bool
    column_position: int


class ConstraintInfo(BaseModel):
    constraint_name: str
    constraint_type: str


class StatisticsInfo(BaseModel):
    row_count: Optional[int] = None
    distinct_count: Optional[int] = None
    null_count: Optional[int] = None


class SampleData(BaseModel):
    sample_value: str
    original_length: int
    truncated: bool = False


class SamplesInfo(BaseModel):
    random_samples: List[SampleData] = []
    distinct_samples: List[SampleData] = []
    total_distinct_count: Optional[int] = None



class TableContextSamples(BaseModel):
    headers: List[str] = []
    sample_rows: List[List[str]] = []


class TokenUsage(BaseModel):
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0


class MetadataInfo(BaseModel):
    schema_version: str = "1.0"
    generated_at: str = ""
    generation_confidence: str = "high"
    processing_time_seconds: float = 0.0
    token_usage: TokenUsage = TokenUsage()


class ColumnDescription(BaseModel):
    database_name: str
    table_name: str
    column_name: str
    data_type: str
    charset: Optional[str] = None
    collation: Optional[str] = None
    is_nullable: bool
    default_value: Optional[str] = None
    comment: Optional[str] = None
    ordinal_position: int
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_ref: Optional[str] = None
    is_auto_increment: bool = False
    is_generated: bool = False
    generation_expression: Optional[str] = None
    has_index: bool = False
    indexes: List[IndexInfo] = []
    constraints: List[ConstraintInfo] = []
    engine_specific: Dict[str, Any] = {}
    privileges: List[str] = []
    statistics: Optional[StatisticsInfo] = None
    samples: SamplesInfo = SamplesInfo()
    table_context_samples: Optional[TableContextSamples] = None
    semantic_summary: str = ""
    # 语义关键词：用于向量检索的关键词列表（不要混入 semantic_summary）
    semantic_keywords: List[str] = []
    metadata: MetadataInfo = MetadataInfo()
