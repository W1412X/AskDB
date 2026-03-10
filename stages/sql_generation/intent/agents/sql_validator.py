from __future__ import annotations

from typing import Any, Dict, List, Optional

from stages.sql_generation.intent.models import (
    ErrorHint,
    SQLCandidate,
    SQLValidateOutput,
    ValidationErrorItem,
    ValidationReport,
)
from stages.sql_generation.tools.db import dry_run_tool, fingerprint_sql_tool, parse_sql_tool, validate_sql_select_only_tool


def validate_candidates(
    *,
    candidates: List[SQLCandidate],
    database: Optional[str],
) -> SQLValidateOutput:
    reports: List[ValidationReport] = []
    best_idx = 0
    for idx, cand in enumerate(candidates):
        errors: List[ValidationErrorItem] = []
        sql = str(cand.sql or "").strip()
        if not sql:
            errors.append(ValidationErrorItem(type="EMPTY_SQL", detail="sql is empty", hint=ErrorHint.RERENDER_SQL))
        else:
            select_check = validate_sql_select_only_tool.invoke({"sql": sql})
            if not select_check.get("ok"):
                errors.append(
                    ValidationErrorItem(
                        type="UNSAFE_SQL",
                        detail=str(select_check.get("error") or "unsafe sql"),
                        hint=ErrorHint.STOP_UNSAFE,
                    )
                )
            parsed = parse_sql_tool.invoke({"sql": sql})
            if not parsed.get("ok"):
                errors.append(
                    ValidationErrorItem(
                        type="SQL_PARSE_ERROR",
                        detail=str(parsed.get("error") or "parse_sql failed"),
                        hint=ErrorHint.RERENDER_SQL,
                    )
                )
            dry = dry_run_tool.invoke({"sql": sql, "database": database})
            if not dry.get("ok"):
                detail = str(dry.get("error") or "dry_run failed")
                hint = ErrorHint.AUTOLINK_ERROR if any(x in detail.lower() for x in ("unknown column", "doesn't exist", "unknown table", "1146")) else ErrorHint.RERENDER_SQL
                errors.append(ValidationErrorItem(type="DRY_RUN_FAILED", detail=detail[:400], hint=hint))

        passed = len(errors) == 0
        if passed and idx == 0:
            best_idx = idx
        if passed and not cand.fingerprint:
            fp = fingerprint_sql_tool.invoke({"sql": sql})
            cand.fingerprint = str(fp.get("fingerprint") or "")
        reports.append(ValidationReport(candidate_index=idx, passed=passed, errors=errors))

    # pick first passed candidate
    for r in reports:
        if r.passed:
            best_idx = r.candidate_index
            break
    return SQLValidateOutput(ok=any(r.passed for r in reports), best_candidate_index=best_idx, reports=reports)

