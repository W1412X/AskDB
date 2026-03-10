from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

# 从项目根目录加载 .env，使 api_key_env / password_env 等生效
_project_root = Path(__file__).resolve().parents[1]
_dotenv_path = _project_root / ".env"
if _dotenv_path.exists():
    from dotenv import load_dotenv
    load_dotenv(_dotenv_path)


def _config_dir() -> Path:
    env_dir = str(os.getenv("APP_CONFIG_DIR") or "").strip()
    if env_dir:
        return Path(env_dir).expanduser().resolve()
    return (Path(__file__).resolve().parent / "json").resolve()


def _load_json_config(filename: str) -> Dict[str, Any]:
    path = _config_dir() / filename
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"config file must contain a JSON object: {path}")
    return data


def _env_override(value: str, env_name: str) -> str:
    env_value = str(os.getenv(env_name) or "").strip()
    return env_value or value


class DatabaseConnectionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    host: str
    port: int = 3306
    user: str
    password: str = ""
    password_env: str = ""
    database: Optional[str] = None
    charset: str = "utf8mb4"
    mincached: int = 1
    maxcached: int = 5
    maxshared: int = 3
    maxconnections: int = 10
    blocking: bool = True
    maxusage: int = 1000
    setsession: List[str] = Field(default_factory=list)
    reset: bool = True
    ping: int = 1


class DatabaseSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    default_connection: str
    default_scope: List[str] = Field(default_factory=list)
    initialize_databases: List[str] = Field(default_factory=list)
    query_databases: List[str] = Field(default_factory=list)
    connections: Dict[str, DatabaseConnectionConfig] = Field(default_factory=dict)


class LLMCallPolicySettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    timeout_seconds: int = 90
    max_transport_retries: int = 2
    max_format_retries: int = 3
    retryable_error_classes: List[str] = Field(default_factory=lambda: ["timeout", "transport", "rate_limit"])


class ModelSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider: str
    model_name: str
    api_key: str = ""
    api_key_env: str = ""
    base_url: str = ""
    base_url_env: str = ""


class ModelsSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    default_model: str
    fallback_order: List[str] = Field(default_factory=list)
    call_policy: LLMCallPolicySettings = Field(default_factory=LLMCallPolicySettings)
    models: Dict[str, ModelSpec] = Field(default_factory=dict)


class IntentDivideSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_name: str
    max_retry_attempts: int = 3
    max_tool_rounds: int = 4
    column_query_top_k: int = 10


class SQLGenerationPipelineSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_name: str
    max_concurrency: int = 3
    max_rows: int = 100
    max_rounds_per_intent: int = 4


class SQLGenerationIntentRuntimeSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_name: str
    max_runtime_iterations: int = 12
    max_no_progress_rounds: int = 3
    max_repeated_error_class: int = 2
    timeout_ms_per_call: int = 30000
    agent_max_retries: int = 3


class SQLGenerationAutolinkSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_name: str
    max_rounds: int = 8
    max_meta_tables: int = 8
    max_explorer_steps: int = 2


class SQLGenerationSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pipeline: SQLGenerationPipelineSettings
    intent_runtime: SQLGenerationIntentRuntimeSettings
    autolink: SQLGenerationAutolinkSettings


class InitializeAgentSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_name: str


class InitializeEmbeddingSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_name: str
    model_path_name: str
    normalize_embeddings: bool = True
    batch_size: int = 32
    device: str = ""
    overwrite: bool = False


class InitializeSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    agent: InitializeAgentSettings
    embedding: InitializeEmbeddingSettings


class GeneralSummarySettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_name: str
    max_input_length: int = 10000


class GeneralSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    summary: GeneralSummarySettings


class ColumnAgentSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sampling: Dict[str, Any] = Field(default_factory=dict)
    token: Dict[str, Any] = Field(default_factory=dict)
    retry: Dict[str, Any] = Field(default_factory=dict)
    parallel: Dict[str, Any] = Field(default_factory=dict)


class StageSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    intent_divide: IntentDivideSettings
    sql_generation: SQLGenerationSettings
    initialize: InitializeSettings
    general: GeneralSettings
    column_agent: ColumnAgentSettings


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    database: DatabaseSettings
    models: ModelsSettings
    stages: StageSettings

    def get_database_connection(self, name: Optional[str] = None) -> DatabaseConnectionConfig:
        connection_name = str(name or self.database.default_connection)
        connection = self.database.connections.get(connection_name)
        if connection is None:
            raise KeyError(f"unknown database connection: {connection_name}")
        password = connection.password
        if connection.password_env:
            password = _env_override(password, connection.password_env)
        return connection.model_copy(update={"password": password})

    def get_default_database_name(self) -> str:
        return str(self.get_database_connection().database or "")

    def get_default_database_scope(self) -> List[str]:
        if self.database.query_databases:
            return list(self.database.query_databases)
        if self.database.default_scope:
            return list(self.database.default_scope)
        default_db = self.get_default_database_name()
        return [default_db] if default_db else []

    def get_initialize_databases(self) -> List[str]:
        if self.database.initialize_databases:
            return list(self.database.initialize_databases)
        return self.get_default_database_scope()

    def get_model(self, alias: Optional[str] = None) -> ModelSpec:
        model_alias = str(alias or self.models.default_model)
        spec = self.models.models.get(model_alias)
        if spec is None:
            raise KeyError(f"unknown model alias: {model_alias}")
        api_key = spec.api_key
        if spec.api_key_env:
            api_key = _env_override(api_key, spec.api_key_env)
        base_url = spec.base_url
        if spec.base_url_env:
            base_url = _env_override(base_url, spec.base_url_env)
        return spec.model_copy(update={"api_key": api_key, "base_url": base_url})

    def get_fallback_model_name(self, current_model_name: str = "") -> str:
        current = str(current_model_name or "").strip()
        for candidate in self.models.fallback_order:
            if candidate and candidate != current and candidate in self.models.models:
                return candidate
        return ""

    def get_stage_model_name(self, stage_path: str) -> str:
        current: Any = self.stages
        for part in [token for token in stage_path.split(".") if token]:
            if not hasattr(current, part):
                break
            current = getattr(current, part)
        model_name = getattr(current, "model_name", "")
        return str(model_name or self.models.default_model)

    def langchain_models_compat(self) -> Dict[str, Dict[str, Any]]:
        result: Dict[str, Dict[str, Any]] = {}
        for alias in self.models.models:
            spec = self.get_model(alias)
            result[alias] = spec.model_dump(mode="json")
        return result

    def database_config_compat(self) -> Dict[str, Any]:
        return self.get_database_connection().model_dump(mode="json")


@lru_cache(maxsize=1)
def get_app_config() -> AppConfig:
    return AppConfig.model_validate(
        {
            "database": _load_json_config("database.json"),
            "models": _load_json_config("models.json"),
            "stages": _load_json_config("stages.json"),
        }
    )


def reload_app_config() -> AppConfig:
    get_app_config.cache_clear()
    return get_app_config()
