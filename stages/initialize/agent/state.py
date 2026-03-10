"""
任务状态管理模块
"""

from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field, asdict
from datetime import datetime
import json
import os
from enum import Enum


class TaskStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class ColumnState:
    column_id: str
    column_name: str
    status: TaskStatus = TaskStatus.PENDING
    metadata: Dict[str, Any] = field(default_factory=dict)
    parent_table_id: str = ""
    result_file_path: str = ""
    semantic_summary: str = ""
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error_message: Optional[str] = None

    def to_dict(self):
        data = asdict(self)
        data["status"] = self.status.value
        if self.start_time:
            data["start_time"] = self.start_time.isoformat()
        if self.end_time:
            data["end_time"] = self.end_time.isoformat()
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        data["status"] = TaskStatus(data["status"])
        if data.get("start_time"):
            data["start_time"] = datetime.fromisoformat(data["start_time"])
        if data.get("end_time"):
            data["end_time"] = datetime.fromisoformat(data["end_time"])
        return cls(**data)


@dataclass
class TableState:
    table_id: str
    table_name: str
    status: TaskStatus = TaskStatus.PENDING
    metadata: Dict[str, Any] = field(default_factory=dict)
    columns: List[ColumnState] = field(default_factory=list)
    parent_database_id: str = ""
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error_message: Optional[str] = None

    def to_dict(self):
        data = asdict(self)
        data["status"] = self.status.value
        data["columns"] = [col.to_dict() for col in self.columns]
        if self.start_time:
            data["start_time"] = self.start_time.isoformat()
        if self.end_time:
            data["end_time"] = self.end_time.isoformat()
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        data["status"] = TaskStatus(data["status"])
        data["columns"] = [ColumnState.from_dict(col) for col in data["columns"]]
        if data.get("start_time"):
            data["start_time"] = datetime.fromisoformat(data["start_time"])
        if data.get("end_time"):
            data["end_time"] = datetime.fromisoformat(data["end_time"])
        return cls(**data)


@dataclass
class DatabaseState:
    database_id: str
    database_name: str
    status: TaskStatus = TaskStatus.PENDING
    metadata: Dict[str, Any] = field(default_factory=dict)
    tables: List[TableState] = field(default_factory=list)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error_message: Optional[str] = None

    def to_dict(self):
        data = asdict(self)
        data["status"] = self.status.value
        data["tables"] = [table.to_dict() for table in self.tables]
        if self.start_time:
            data["start_time"] = self.start_time.isoformat()
        if self.end_time:
            data["end_time"] = self.end_time.isoformat()
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        data["status"] = TaskStatus(data["status"])
        data["tables"] = [TableState.from_dict(table) for table in data["tables"]]
        if data.get("start_time"):
            data["start_time"] = datetime.fromisoformat(data["start_time"])
        if data.get("end_time"):
            data["end_time"] = datetime.fromisoformat(data["end_time"])
        return cls(**data)


class StateManager:
    def __init__(self, checkpoint_dir: str):
        self.checkpoint_dir = checkpoint_dir
        os.makedirs(checkpoint_dir, exist_ok=True)

    def save_state(self, state: DatabaseState, timestamp: str):
        checkpoint_path = os.path.join(self.checkpoint_dir, timestamp, "task_state.json")
        os.makedirs(os.path.dirname(checkpoint_path), exist_ok=True)
        with open(checkpoint_path, "w", encoding="utf-8") as f:
            json.dump(state.to_dict(), f, ensure_ascii=False, indent=2)

    def load_state(self, timestamp: str) -> Optional[DatabaseState]:
        checkpoint_path = os.path.join(self.checkpoint_dir, timestamp, "task_state.json")
        if not os.path.exists(checkpoint_path):
            return None
        with open(checkpoint_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return DatabaseState.from_dict(data)

    def get_latest_checkpoint(self) -> Optional[str]:
        checkpoints = []
        if not os.path.exists(self.checkpoint_dir):
            return None
        for item in os.listdir(self.checkpoint_dir):
            checkpoint_path = os.path.join(self.checkpoint_dir, item, "task_state.json")
            if os.path.exists(checkpoint_path):
                checkpoints.append(item)
        if not checkpoints:
            return None
        return max(checkpoints)
