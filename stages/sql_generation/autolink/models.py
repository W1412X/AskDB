"""
AutoLink 多 Agent Schema 链接模块数据模型。
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from config.app_config import get_app_config


class RequestType(str, Enum):
    BUILD = "BUILD"
    ENRICH = "ENRICH"
    ERROR = "ERROR"


class RunStatus(str, Enum):
    SUCCESS = "SUCCESS"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    FAILED = "FAILED"


class EventType(str, Enum):
    REQUEST_RECEIVED = "REQUEST_RECEIVED"
    REQUEST_ROUTED = "REQUEST_ROUTED"
    AGENT_INVOKED = "AGENT_INVOKED"
    TOOL_AGENT_INVOKED = "TOOL_AGENT_INVOKED"
    TOOL_AGENT_FINISHED = "TOOL_AGENT_FINISHED"
    TOOL_CALL_STARTED = "TOOL_CALL_STARTED"
    TOOL_CALL_FINISHED = "TOOL_CALL_FINISHED"
    SCHEMA_VALIDATED = "SCHEMA_VALIDATED"
    COMPLETENESS_CHECKED = "COMPLETENESS_CHECKED"
    ROUND_ASSESSMENT = "ROUND_ASSESSMENT"
    RUN_COMPLETED = "RUN_COMPLETED"
    RUN_FAILED = "RUN_FAILED"


class ToolAgentName(str, Enum):
    SCHEMA_RETRIEVAL = "SchemaRetrievalAgent"
    SCHEMA_META = "SchemaMetaAgent"
    SCHEMA_EXPLORER = "SchemaExplorerAgent"


class SchemaFieldLevel(str, Enum):
    DATABASE = "database"
    TABLE = "table"
    COLUMN = "column"


class FieldPriority(str, Enum):
    P0 = "P0"
    P1 = "P1"
    P2 = "P2"
    P3 = "P3"


class ValueSourceType(str, Enum):
    TOOL = "tool"
    LLM_WEAK_SEMANTIC = "llm_weak_semantic"
    RUNTIME_GUARD = "runtime_guard"
    INITIALIZE_JSON = "initialize_json"
    DB_METADATA = "db_metadata"
    DB_SAMPLE = "db_sample"


class WriteOperation(str, Enum):
    SET = "set"
    MERGE = "merge"
    APPEND_UNIQUE = "append_unique"
    REPLACE_IF_BETTER = "replace_if_better"
    REMOVE = "remove"


class ResultMappingOperation(str, Enum):
    SET = "set"
    APPEND_UNIQUE = "append_unique"
    MERGE = "merge"


class AutolinkPhase(str, Enum):
    DISCOVER = "DISCOVER"
    HYDRATE_METADATA = "HYDRATE_METADATA"
    EXPLORE_DATA = "EXPLORE_DATA"
    ASSESS_COMPLETENESS = "ASSESS_COMPLETENESS"
    DONE = "DONE"
    FAILED = "FAILED"


# --- Schema 结构 ---


class ColumnInfo(BaseModel):
    # Keep schema artifact clean and stable: ignore unknown/deprecated fields.
    model_config = ConfigDict(extra="ignore")

    type: str = ""
    description: str = ""
    index: str = ""
    sample_values: List[Any] = Field(default_factory=list)
    field_provenance: Dict[str, Dict[str, Any]] = Field(default_factory=dict, exclude=True)


class TableInfo(BaseModel):
    # Keep schema artifact clean and stable: ignore unknown/deprecated fields.
    model_config = ConfigDict(extra="ignore")

    description: str = ""
    primary_key: Optional[List[str]] = None
    foreign_keys: Optional[List[Dict[str, Any]]] = None
    columns: Dict[str, ColumnInfo] = Field(default_factory=dict)
    field_provenance: Dict[str, Dict[str, Any]] = Field(default_factory=dict, exclude=True)


class DatabaseInfo(BaseModel):
    # Keep schema artifact clean and stable: ignore unknown/deprecated fields.
    model_config = ConfigDict(extra="ignore")

    description: str = ""
    tables: Dict[str, TableInfo] = Field(default_factory=dict)
    field_provenance: Dict[str, Dict[str, Any]] = Field(default_factory=dict, exclude=True)


class Schema(BaseModel):
    # Keep schema artifact clean and stable: ignore unknown/deprecated fields.
    model_config = ConfigDict(extra="ignore")

    databases: Dict[str, DatabaseInfo] = Field(default_factory=dict)


class SchemaEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: ValueSourceType
    target: str
    field: str
    value: Any
    confidence: float = 0.0
    observed_at: str = ""


# --- Unified protocol structures ---


class TargetEntity(BaseModel):
    model_config = ConfigDict(extra="forbid")

    database: str = ""
    table: str = ""
    columns: List[str] = Field(default_factory=list)


class RequirementConstraints(BaseModel):
    model_config = ConfigDict(extra="forbid")

    minimality_preferred: bool = True
    allow_weak_semantic_fill: bool = True
    prefer_strong_metadata: bool = True


class RequirementFocus(BaseModel):
    model_config = ConfigDict(extra="forbid")

    class PrimaryGoal(str, Enum):
        BUILD_MINIMAL_SCHEMA = "build_minimal_schema"
        ENRICH_EXISTING_SCHEMA = "enrich_existing_schema"
        REPAIR_SCHEMA_ERROR = "repair_schema_error"

    primary_goal: PrimaryGoal
    focus_flags: List[str] = Field(default_factory=list)
    target_entities: List[TargetEntity] = Field(default_factory=list)
    constraints: RequirementConstraints = Field(default_factory=RequirementConstraints)
    reason: str = ""


class SchemaFieldTarget(BaseModel):
    model_config = ConfigDict(extra="forbid")

    level: SchemaFieldLevel
    database: str = ""
    table: str = ""
    column: str = ""
    field: str


class FieldRequirementItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target: SchemaFieldTarget
    priority: FieldPriority
    required_by_request: bool = True
    reason: str = ""
    source: str = ""


class FieldRequirementProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requirements: List[FieldRequirementItem] = Field(default_factory=list)
    summary: str = ""


class ValueSource(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_type: ValueSourceType
    source_name: str
    source_ref: str = ""
    confidence: float = 0.0


class WritePolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    only_if_empty: bool = False
    allow_overwrite: bool = False
    require_target_exists: bool = True


class SchemaWrite(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target: SchemaFieldTarget
    operation: WriteOperation
    value: Any
    value_source: ValueSource
    write_policy: WritePolicy = Field(default_factory=WritePolicy)
    reason: str = ""

    @model_validator(mode="after")
    def validate_target_field(self) -> "SchemaWrite":
        """
        Strong schema write contract:
        - Forbid writing container fields (tables/columns/databases) to avoid mixing dicts and Pydantic objects.
        - Enforce per-level allowed leaf fields so LLM outputs are rejected and retried.
        """
        allowed_by_level = {
            SchemaFieldLevel.DATABASE: {"description"},
            SchemaFieldLevel.TABLE: {"description", "primary_key", "foreign_keys"},
            SchemaFieldLevel.COLUMN: {
                "type",
                "description",
                "index",
                "sample_values",
            },
        }
        allowed = allowed_by_level.get(self.target.level, set())
        if self.target.field not in allowed:
            raise ValueError(
                f"invalid schema write target.field={self.target.field!r} for level={self.target.level.value}; "
                f"allowed={sorted(allowed)}"
            )
        return self


class SchemaWritePlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    writes: List[SchemaWrite] = Field(default_factory=list)
    summary: str = ""


class ResultColumnMapping(BaseModel):
    model_config = ConfigDict(extra="forbid")

    result_column: str
    target_column: str
    target_field: str
    operation: ResultMappingOperation
    reason: str = ""


class ResultMapping(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_database: str = ""
    target_table: str
    mappings: List[ResultColumnMapping] = Field(default_factory=list)
    summary: str = ""


# --- 请求/响应 ---


class AutolinkContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    database_scope: List[str] = Field(default_factory=list)
    external_knowledge: str = ""
    sql_dialect: str = "BIGQUERY"
    hints: Dict[str, Any] = Field(default_factory=dict)
    model_name: str = Field(default_factory=lambda: get_app_config().stages.sql_generation.autolink.model_name)
    max_meta_tables: int = Field(default_factory=lambda: get_app_config().stages.sql_generation.autolink.max_meta_tables)
    extensions: Dict[str, Any] = Field(default_factory=dict)


class AutolinkRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    request: str
    request_type: RequestType
    schema_data: Optional[Schema] = Field(default=None, alias="schema")
    context: AutolinkContext = Field(default_factory=AutolinkContext)

    @property
    def schema(self) -> Optional[Schema]:
        """Alias for schema_data (API compatibility)."""
        return self.schema_data
    request_id: Optional[str] = None
    trace_id: Optional[str] = None
    extensions: Dict[str, Any] = Field(default_factory=dict)


class AuditEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str
    request_id: str
    plan_id: str
    step_id: str = ""
    timestamp: str
    event_type: EventType
    payload: Dict[str, Any] = Field(default_factory=dict)


class AuditTrace(BaseModel):
    model_config = ConfigDict(extra="forbid")

    trace_id: str
    events: List[AuditEvent] = Field(default_factory=list)


class AutolinkOutput(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    schema_data: Schema = Field(alias="schema")
    audit: AuditTrace
    status: RunStatus
    errors: List[str] = Field(default_factory=list)

    @property
    def schema(self) -> Schema:
        """Alias for schema_data (API compatibility)."""
        return self.schema_data


# --- Planner 输出 ---


class SubTaskIntent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    class Goal(str, Enum):
        RETRIEVE_RELEVANT_SCHEMA = "retrieve_relevant_schema"
        FETCH_TABLE_METADATA = "fetch_table_metadata"
        COLLECT_SAMPLE_VALUES = "collect_sample_values"
        VALIDATE_SCHEMA_WITH_SQL = "validate_schema_with_sql"
        REPAIR_SCHEMA_ERROR = "repair_schema_error"

    goal: Goal
    target_tables: List[str] = Field(default_factory=list)
    target_columns: List[str] = Field(default_factory=list)
    success_criteria: List[str] = Field(default_factory=list)
    notes: str = ""


class SubTask(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tool_agent_name: ToolAgentName
    task: SubTaskIntent
    expected_output: str = ""


class RequirementPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: bool = True
    summary: str = ""
    findings: List[Dict[str, Any]] = Field(default_factory=list)
    sub_tasks: List[SubTask] = Field(default_factory=list)
    requirement_focus: Optional[RequirementFocus] = None
    field_requirement_profile: Optional[FieldRequirementProfile] = None
    schema_write_plan: SchemaWritePlan = Field(default_factory=SchemaWritePlan)

    @model_validator(mode="after")
    def validate_protocol_fields(self) -> "RequirementPlan":
        if self.requirement_focus is None:
            raise ValueError("requirement_focus is required")
        if self.field_requirement_profile is None:
            raise ValueError("field_requirement_profile is required")
        return self


class PlannerOutput(RequirementPlan):
    pass


# --- Tool Agent 输出 ---


class ToolAttempt(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tool_call_id: str
    tool_name: str
    args: Dict[str, Any] = Field(default_factory=dict)
    duration_ms: int = 0
    ok: bool = True
    result_digest: str = ""
    result_preview: str = ""
    error: str = ""


class ToolAgentOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: bool = True
    summary: str = ""
    observations: List[Dict[str, Any]] = Field(default_factory=list)
    tool_calls: List[ToolAttempt] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    schema_write_plan: SchemaWritePlan = Field(default_factory=SchemaWritePlan)
    result_mapping: Optional[ResultMapping] = None


# --- Round Judge ---


class CompletenessAssessment(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: str = ""
    should_stop: bool = False
    stop_reason: str = ""
    continue_reason: str = ""
    missing_required_fields: List[str] = Field(default_factory=list)
    optional_pending_fields: List[str] = Field(default_factory=list)
    redundant_items: List[str] = Field(default_factory=list)
    new_evidence_summary: List[str] = Field(default_factory=list)
    pruned_items: List[str] = Field(default_factory=list)
    schema_changed: bool = False

    @staticmethod
    def _is_empty_str(value: Any) -> bool:
        return value is None or (isinstance(value, str) and not value.strip())

    @model_validator(mode="after")
    def validate_stop_fields(self) -> "CompletenessAssessment":
        is_initial_empty = (
            not bool(self.should_stop)
            and self._is_empty_str(self.stop_reason)
            and self._is_empty_str(self.continue_reason)
            and not self.missing_required_fields
            and not self.optional_pending_fields
            and not self.redundant_items
            and not self.new_evidence_summary
            and not self.pruned_items
        )
        if is_initial_empty:
            return self
        # If should_stop is true, stop_reason must be provided (non-empty).
        if bool(self.should_stop) and self._is_empty_str(self.stop_reason):
            raise ValueError("stop_reason must be non-empty when should_stop=true")
        # If should_stop is false, stop_reason should be empty to avoid confusing downstream logic.
        if not bool(self.should_stop) and not self._is_empty_str(self.stop_reason):
            raise ValueError("stop_reason must be empty when should_stop=false")
        if not bool(self.should_stop) and self._is_empty_str(self.continue_reason):
            raise ValueError("continue_reason must be non-empty when should_stop=false")
        return self


class RoundJudgeResult(CompletenessAssessment):
    pass


# --- Agent memory ---


class AgentMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    role: str
    content: str


class AgentMemory(BaseModel):
    model_config = ConfigDict(extra="forbid")

    messages: List[AgentMessage] = Field(default_factory=list)
    working_memory: Dict[str, Any] = Field(default_factory=dict)
    latest_schema_snapshot: Dict[str, Any] = Field(default_factory=dict)
    latest_round_summary: Dict[str, Any] = Field(default_factory=dict)


# --- Runtime state ---


class AutolinkState(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    request_id: str = ""
    plan_id: str = ""
    trace_id: str = ""
    request_type: RequestType = RequestType.BUILD
    request: str = ""
    schema_data: Schema = Field(default_factory=Schema, alias="schema")
    context: AutolinkContext = Field(default_factory=AutolinkContext)
    phase: AutolinkPhase = AutolinkPhase.DISCOVER
    evidence: List[SchemaEvidence] = Field(default_factory=list)
    convergence: Dict[str, Any] = Field(
        default_factory=lambda: {
            "state_fingerprint": "",
            "action_fingerprint": "",
            "no_progress_rounds": 0,
            "repeated_error_classes": {},
            "visited_phase_edges": [],
        }
    )

    @property
    def schema(self) -> Schema:
        """Alias for schema_data (API compatibility)."""
        return self.schema_data

    findings: List[Dict[str, Any]] = Field(default_factory=list)
    step_logs: List[Dict[str, Any]] = Field(default_factory=list)
    round: int = 0
    sql_draft_success: bool = False
    sql_draft_count: int = 0
    sql_explore_count: int = 0
    max_rounds: int = 8
    tool_step_limit_per_round: int = 3
    last_tool_results: List[Dict[str, Any]] = Field(default_factory=list)
    schema_stale_count: int = 0
    stop_reason: str = ""
    agent_memories: Dict[str, AgentMemory] = Field(default_factory=dict)
    latest_assessment: CompletenessAssessment = Field(default_factory=CompletenessAssessment)
    last_pruned_items: List[str] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    model_available: bool = False

    @property
    def latest_judge_result(self) -> CompletenessAssessment:
        return self.latest_assessment

    @latest_judge_result.setter
    def latest_judge_result(self, value: CompletenessAssessment) -> None:
        self.latest_assessment = value


def model_dump_jsonable(model: BaseModel) -> Dict[str, Any]:
    return model.model_dump(mode="json")


def render_subtask_intent(intent: SubTaskIntent) -> str:
    parts: List[str] = [intent.goal]
    if intent.target_tables:
        parts.append(f"tables={','.join(intent.target_tables[:3])}")
    if intent.target_columns:
        parts.append(f"columns={','.join(intent.target_columns[:5])}")
    if intent.notes:
        parts.append(intent.notes[:80])
    return " | ".join(parts)
