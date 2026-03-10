"""
Shared LLM call policy for sql_generation.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Sequence, TypeVar

from config.app_config import get_app_config

T = TypeVar("T")

FORMAT_RETRY_APPENDIX = """

【格式错误，请重新输出（会被严格校验）】
上一轮输出校验失败：{error}

强约束（必须全部满足，否则会继续被打回）：
1) 仅输出 1 个纯 JSON 对象（不允许 markdown、代码块、解释文字、前后缀文本）。
2) 字段必须与 system prompt 中“输出严格 JSON”一致；禁止多余字段（extra keys 会被拒绝）。
3) 枚举字段取值必须严格命中允许集合（大小写/下划线必须一致）。
4) 字段类型必须正确：字符串/布尔/数组/对象等不可混用。

请直接输出合规 JSON。"""


@dataclass(frozen=True)
class LLMCallPolicy:
    timeout_seconds: int = 90
    max_transport_retries: int = 2
    max_format_retries: int = 3
    retryable_error_classes: tuple[str, ...] = ("timeout", "transport", "rate_limit")
    fallback_model_name: str = ""
    classify_message_tokens: tuple[str, ...] = field(
        default_factory=lambda: (
            "timeout",
            "timed out",
            "connection",
            "temporarily unavailable",
            "rate limit",
            "429",
            "service unavailable",
            "network",
            "socket",
            "reset by peer",
        )
    )


def _default_fallback_model_name(model_name: str) -> str:
    return get_app_config().get_fallback_model_name(model_name)


def default_llm_call_policy(model_name: str = "") -> LLMCallPolicy:
    call_policy = get_app_config().models.call_policy
    return LLMCallPolicy(
        timeout_seconds=call_policy.timeout_seconds,
        max_transport_retries=call_policy.max_transport_retries,
        max_format_retries=call_policy.max_format_retries,
        retryable_error_classes=tuple(call_policy.retryable_error_classes),
        fallback_model_name=_default_fallback_model_name(model_name),
    )


def classify_llm_error(exc: Exception) -> str:
    name = type(exc).__name__.lower()
    message = str(exc).lower()
    if isinstance(exc, TimeoutError) or isinstance(exc, FutureTimeoutError) or "timeout" in name or "timed out" in message:
        return "timeout"
    if "rate" in name and "limit" in name:
        return "rate_limit"
    if "rate limit" in message or "429" in message:
        return "rate_limit"
    if "auth" in name or "permission" in name or "unauthorized" in message or "forbidden" in message:
        return "auth"
    if "config" in name or "invalid model" in message or "api key" in message:
        return "config"
    if any(token in message for token in ("connection", "network", "socket", "unavailable", "reset by peer", "broken pipe")):
        return "transport"
    return "unknown"


def _resolve_policy(model: Any, policy: Optional[LLMCallPolicy]) -> LLMCallPolicy:
    if policy is not None:
        return policy
    return default_llm_call_policy(str(getattr(model, "_codex_model_name", "") or ""))


def _resolve_model_factory(model: Any, model_factory: Optional[Callable[[str], Any]]) -> Optional[Callable[[str], Any]]:
    if model_factory is not None:
        return model_factory
    factory = getattr(model, "_codex_model_factory", None)
    return factory if callable(factory) else None


def _invoke_model_once(model: Any, messages: Sequence[Any], timeout_seconds: int) -> Any:
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(model.invoke, list(messages))
        try:
            return future.result(timeout=timeout_seconds)
        except FutureTimeoutError as exc:
            future.cancel()
            raise TimeoutError(f"llm invoke timeout after {timeout_seconds}s") from exc


def invoke_messages_with_policy(
    model: Any,
    messages: Sequence[Any],
    *,
    policy: Optional[LLMCallPolicy] = None,
    model_factory: Optional[Callable[[str], Any]] = None,
) -> Any:
    resolved_policy = _resolve_policy(model, policy)
    resolved_factory = _resolve_model_factory(model, model_factory)
    active_model = model
    active_model_name = str(getattr(model, "_codex_model_name", "") or "")
    used_fallback = False
    last_error: Optional[Exception] = None

    while True:
        for attempt in range(resolved_policy.max_transport_retries + 1):
            try:
                return _invoke_model_once(active_model, messages, resolved_policy.timeout_seconds)
            except Exception as exc:
                last_error = exc
                error_class = classify_llm_error(exc)
                if error_class not in resolved_policy.retryable_error_classes or attempt >= resolved_policy.max_transport_retries:
                    break
        fallback_name = str(resolved_policy.fallback_model_name or "").strip()
        if used_fallback or not fallback_name or fallback_name == active_model_name or resolved_factory is None:
            break
        active_model = resolved_factory(fallback_name)
        active_model_name = fallback_name
        used_fallback = True

    if last_error is not None:
        raise last_error
    raise RuntimeError("invoke_messages_with_policy: unreachable")


def invoke_llm(
    model: Any,
    system_content: str,
    user_content: str,
    *,
    policy: Optional[LLMCallPolicy] = None,
    model_factory: Optional[Callable[[str], Any]] = None,
) -> str:
    from langchain_core.messages import HumanMessage, SystemMessage

    messages = [
        SystemMessage(content=system_content),
        HumanMessage(content=user_content),
    ]
    resp = invoke_messages_with_policy(model, messages, policy=policy, model_factory=model_factory)
    return resp.content if hasattr(resp, "content") else str(resp)


def invoke_llm_with_format_retry(
    model: Any,
    system_content: str,
    user_content: str,
    parse_fn: Callable[[str], T],
    max_retries: Optional[int] = None,
    *,
    policy: Optional[LLMCallPolicy] = None,
    model_factory: Optional[Callable[[str], Any]] = None,
) -> T:
    from pydantic import ValidationError

    from stages.sql_generation.autolink.middleware import MiddlewareValidationError

    resolved_policy = _resolve_policy(model, policy)
    retry_count = resolved_policy.max_format_retries if max_retries is None else int(max_retries)
    content = user_content
    last_error: Optional[Exception] = None
    for attempt in range(retry_count + 1):
        try:
            raw = invoke_llm(
                model,
                system_content,
                content,
                policy=resolved_policy,
                model_factory=model_factory,
            )
            return parse_fn(raw)
        except (ValidationError, MiddlewareValidationError) as exc:
            last_error = exc
            if attempt < retry_count:
                content = content + FORMAT_RETRY_APPENDIX.format(error=str(exc))
                continue
            raise
    if last_error is not None:
        raise last_error
    raise RuntimeError("invoke_llm_with_format_retry: unreachable")
