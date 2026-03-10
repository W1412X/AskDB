"""
Intent SQL generation module models (intent/).
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

class IntentRunStatus(str, Enum):
    SUCCESS = "SUCCESS"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    FAILED = "FAILED"
    WAIT_USER = "WAIT_USER"


class IntentExecutionState(str, Enum):
    INIT = "INIT"
    BUILDING_SCHEMA = "BUILDING_SCHEMA"
    PLANNING_RA = "PLANNING_RA"
    RENDERING_SQL = "RENDERING_SQL"
    VALIDATING_SQL = "VALIDATING_SQL"
    EXECUTING_SQL = "EXECUTING_SQL"
    INTERPRETING_RESULT = "INTERPRETING_RESULT"
    WAITING_USER = "WAITING_USER"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PARTIAL = "PARTIAL"


class StepStatus(str, Enum):
    ADVANCE = "ADVANCE"
    WAIT_USER = "WAIT_USER"
    COMPLETE = "COMPLETE"
    FAIL = "FAIL"


class DialogResolutionType(str, Enum):
    RESOLVED = "RESOLVED"
    ASSUMPTIVE = "ASSUMPTIVE"
    ABANDONED = "ABANDONED"


class Confidence(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class ErrorHint(str, Enum):
    AUTOLINK_ENRICH = "autolink_enrich"
    AUTOLINK_ERROR = "autolink_error"
    ASK_USER = "ask_user"
    RERENDER_SQL = "rerender_sql"
    REPLAN_RA = "replan_ra"
    STOP_UNSAFE = "stop_unsafe"


class IntentError(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str
    message: str
    hint: Optional[ErrorHint] = None


class DependencyPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    direct_facts: List[Dict[str, Any]] = Field(default_factory=list)
    transitive_facts: List[Dict[str, Any]] = Field(default_factory=list)
    missing_dependencies: List[str] = Field(default_factory=list)
    meta: Dict[str, Any] = Field(default_factory=dict)


class JoinType(str, Enum):
    INNER = "inner"
    LEFT = "left"


class CheckSeverity(str, Enum):
    P0 = "P0"
    P1 = "P1"
    P2 = "P2"


class RAEntity(BaseModel):
    model_config = ConfigDict(extra="forbid")

    database: str
    table: str
    columns: List[str] = Field(default_factory=list)


class RAJoin(BaseModel):
    model_config = ConfigDict(extra="forbid")

    left: str
    right: str
    on: List[List[str]] = Field(default_factory=list)
    type: JoinType = JoinType.INNER
    reason: str = ""


class RAFilter(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expr: str
    reason: str = ""
    required: bool = True


class RACheck(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    expr: str
    severity: CheckSeverity = CheckSeverity.P0
    reason: str = ""


class RAOutputContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    row_semantics: str = ""
    required_columns: List[str] = Field(default_factory=list)


class RAPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: bool = True
    summary: str = ""
    entities: List[RAEntity] = Field(default_factory=list)
    joins: List[RAJoin] = Field(default_factory=list)
    filters: List[RAFilter] = Field(default_factory=list)
    checks: List[RACheck] = Field(default_factory=list)
    output_contract: RAOutputContract = Field(default_factory=RAOutputContract)
    assumptions: List[str] = Field(default_factory=list)


class SQLCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sql: str
    rationale: str = ""
    expected_columns: List[str] = Field(default_factory=list)
    assumptions: List[str] = Field(default_factory=list)
    fingerprint: str = ""


class SQLRenderOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: bool = True
    candidates: List[SQLCandidate] = Field(default_factory=list)
    summary: str = ""


class ValidationErrorItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str
    detail: str
    hint: Optional[ErrorHint] = None


class ValidationReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    candidate_index: int
    passed: bool
    errors: List[ValidationErrorItem] = Field(default_factory=list)


class SQLValidateOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: bool = True
    best_candidate_index: int = 0
    reports: List[ValidationReport] = Field(default_factory=list)


class Interpretation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: bool = True
    answer: str
    confidence: Confidence = Confidence.LOW
    assumptions: List[str] = Field(default_factory=list)
    missing_items: List[str] = Field(default_factory=list)


class ValidatedConstraint(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    status: str
    detail: str = ""


class ResultMetric(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    value: Any
    unit: str = ""


class IntentFactsBundle(BaseModel):
    model_config = ConfigDict(extra="forbid")

    entity_keys: List[str] = Field(default_factory=list)
    validated_constraints: List[ValidatedConstraint] = Field(default_factory=list)
    result_metrics: List[ResultMetric] = Field(default_factory=list)
    derived_filters: List[str] = Field(default_factory=list)
    used_tables: List[str] = Field(default_factory=list)
    used_columns: List[str] = Field(default_factory=list)
    assumptions: List[str] = Field(default_factory=list)


class IntentCheckpoint(BaseModel):
    model_config = ConfigDict(extra="forbid")

    intent_id: str
    phase: IntentExecutionState
    input_snapshot: Dict[str, Any] = Field(default_factory=dict)
    artifacts: Dict[str, Any] = Field(default_factory=dict)
    errors: List[str] = Field(default_factory=list)
    resume_token: str = ""
    updated_at: float = 0.0


class StepResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: StepStatus
    next_phase: IntentExecutionState
    artifacts: Dict[str, Any] = Field(default_factory=dict)
    errors: List[IntentError] = Field(default_factory=list)
    wait_ticket: Optional[Dict[str, Any]] = None
    new_evidence: List[str] = Field(default_factory=list)
    error_class: str = ""
    state_delta: Dict[str, Any] = Field(default_factory=dict)


class IntentFinalBundle(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: IntentRunStatus
    interpretation: Optional[Interpretation] = None
    final_sql: str = ""
    final_sql_fingerprint: str = ""
    schema_data: Dict[str, Any] = Field(default_factory=dict, alias="schema")
    exec_raw: Dict[str, Any] = Field(default_factory=dict)
    facts_bundle: IntentFactsBundle = Field(default_factory=IntentFactsBundle)
    assumptions: List[str] = Field(default_factory=list)
    errors: List[IntentError] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_success_has_sql(self) -> "IntentFinalBundle":
        if self.status == IntentRunStatus.SUCCESS and not (self.final_sql or "").strip():
            raise ValueError("SUCCESS requires non-empty final_sql")
        return self

    @property
    def schema(self) -> Dict[str, Any]:
        return self.schema_data


class ClarificationOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resolved: bool
    summary: str = ""
    hints: Dict[str, Any] = Field(default_factory=dict)
    next_ask: Optional[Dict[str, Any]] = None
