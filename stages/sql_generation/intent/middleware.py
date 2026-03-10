"""
Intent module middleware: strict JSON extraction and parsing helpers.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict


class MiddlewareValidationError(ValueError):
    pass


def safe_json_dumps(value: Any) -> str:
    """
    JSON dump that tolerates common DB/python types (date/datetime/Decimal/Enum/etc).

    This is used for LLM input payloads; it must never crash the pipeline.
    """
    return json.dumps(value, ensure_ascii=False, default=str)


def extract_json_object(text: str) -> Dict[str, Any]:
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
