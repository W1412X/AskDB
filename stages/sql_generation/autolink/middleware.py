"""
AutoLink 中间件：输入/输出校验、重试、SQL 护栏。
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

from config.app_config import get_app_config
from utils.logger import get_logger

from stages.sql_generation.autolink.models import (
    AutolinkRequest,
    CompletenessAssessment,
    FieldRequirementProfile,
    RequirementPlan,
    RequirementFocus,
    RequestType,
    ResultMapping,
    Schema,
    SchemaWritePlan,
)

logger = get_logger("autolink")


class MiddlewareValidationError(ValueError):
    pass


def _extract_json_from_text(text: str) -> Dict[str, Any]:
    raw = (text or "").strip()
    if not raw:
        raise MiddlewareValidationError("empty llm output")
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    fence_match = re.search(r"```(?:json)?\s*(\{.*\})\s*```", raw, re.DOTALL)
    if fence_match:
        try:
            obj = json.loads(fence_match.group(1))
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass
    left, right = raw.find("{"), raw.rfind("}")
    if left >= 0 and right > left:
        try:
            obj = json.loads(raw[left : right + 1])
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass
    raise MiddlewareValidationError("cannot parse JSON object from llm output")


def validate_request(req: AutolinkRequest) -> None:
    logger.debug("校验请求", request_type=req.request_type.value)
    if not req.request or not str(req.request).strip():
        raise MiddlewareValidationError("request is required")
    if req.request_type not in (RequestType.BUILD, RequestType.ENRICH, RequestType.ERROR):
        raise MiddlewareValidationError("request_type must be BUILD, ENRICH, or ERROR")
    if req.request_type in (RequestType.ENRICH, RequestType.ERROR) and (
        not req.schema_data or not req.schema_data.databases
    ):
        raise MiddlewareValidationError(f"{req.request_type.value} request requires non-empty input schema")
    if not req.context.database_scope:
        default_scope = get_app_config().get_default_database_scope()
        if default_scope:
            req.context.database_scope = list(default_scope)
            return
        try:
            from stages.sql_generation.tools.db import list_databases_tool
            dbs = list_databases_tool.invoke({})
            if dbs:
                req.context.database_scope = [str(dbs[0])] if isinstance(dbs[0], str) else list(dbs)[:1]
            else:
                raise MiddlewareValidationError("database_scope is required and list_databases returned empty")
        except Exception as e:
            logger.warning("校验请求：database_scope 自动填充失败", error=str(e))
            raise MiddlewareValidationError(f"database_scope is required: {e}")


def parse_requirement_plan_output(raw: str) -> RequirementPlan:
    obj = _extract_json_from_text(raw)
    return RequirementPlan.model_validate(obj)


def parse_planner_output(raw: str) -> RequirementPlan:
    return parse_requirement_plan_output(raw)


def parse_completeness_assessment_output(raw: str) -> CompletenessAssessment:
    obj = _extract_json_from_text(raw)
    return CompletenessAssessment.model_validate(obj)


def parse_round_judge_output(raw: str) -> CompletenessAssessment:
    return parse_completeness_assessment_output(raw)


def parse_requirement_focus_output(raw: str) -> RequirementFocus:
    obj = _extract_json_from_text(raw)
    return RequirementFocus.model_validate(obj)


def parse_field_requirement_profile_output(raw: str) -> FieldRequirementProfile:
    obj = _extract_json_from_text(raw)
    return FieldRequirementProfile.model_validate(obj)


def parse_schema_write_plan_output(raw: str) -> SchemaWritePlan:
    obj = _extract_json_from_text(raw)
    return SchemaWritePlan.model_validate(obj)


def parse_result_mapping_output(raw: str) -> ResultMapping:
    obj = _extract_json_from_text(raw)
    return ResultMapping.model_validate(obj)


def validate_schema(schema: Schema) -> Dict[str, Any]:
    """Schema 格式校验。返回 {valid, errors, warnings}。"""
    errors: List[str] = []
    warnings: List[str] = []
    if not schema.databases:
        warnings.append("schema has no databases")
    for db_name, db_info in schema.databases.items():
        if not db_info.tables:
            warnings.append(f"database {db_name} has no tables")
        for tb_name, tb_info in db_info.tables.items():
            if not tb_info.columns:
                warnings.append(f"table {db_name}.{tb_name} has no columns")
    return {"valid": len(errors) == 0, "errors": errors, "warnings": warnings}


def ensure_select_only(sql: str) -> None:
    import sqlparse

    parsed = sqlparse.parse(sql or "")
    if not parsed:
        raise MiddlewareValidationError("empty sql")
    if len(parsed) != 1:
        raise MiddlewareValidationError("multi-statement sql not allowed")
    first = parsed[0].token_first(skip_cm=True, skip_ws=True)
    if first is None:
        raise MiddlewareValidationError("invalid sql")
    if str(first).upper() not in {"SELECT", "WITH"}:
        raise MiddlewareValidationError("only SELECT/WITH allowed")
