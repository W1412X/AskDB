"""
意图分解核心流程（LangChain tool-calling + 输出校验重试）。
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage, ToolMessage

from config.app_config import get_app_config
from config.llm_config import get_llm
from stages.intent_divide.models import Intent, IntentDivideOutput, IntentDivideStatus
from stages.intent_divide.tools import get_intent_divide_tools
from stages.intent_divide.tracing import DivideAudit
from stages.intent_divide.validator import IntentOutputValidator
from stages.sql_generation.autolink.llm_utils import default_llm_call_policy, invoke_messages_with_policy
from stages.sql_generation.autolink.logging_utils import log_step_input, log_step_output
from utils.id_generator import new_id
from utils.logger import get_logger

logger = get_logger("intent_divide")
_CFG = get_app_config().stages.intent_divide

STRICT_JSON_ARRAY_NOTICE = (
    "重要：你最终必须只输出 1 个 JSON 数组（禁止 markdown/代码块/解释文字/前后缀文本）。\n"
    "数组元素必须是 JSON 对象，字段严格包含:\n"
    "- intent_id\n"
    "- intent_description\n"
    "- dependency_intent_ids (list)\n"
)


def _extract_json_array(text: str) -> List[Dict[str, Any]]:
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```(?:json)?\s*", "", stripped)
        stripped = re.sub(r"\s*```$", "", stripped)
    try:
        data = json.loads(stripped)
        if isinstance(data, list):
            return data
    except Exception:
        pass

    match = re.search(r"\[[\s\S]*\]", text)
    if not match:
        raise ValueError("model output does not contain JSON array")
    data = json.loads(match.group(0))
    if not isinstance(data, list):
        raise ValueError("parsed JSON is not a list")
    return data


def _to_str_content(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        chunks: List[str] = []
        for part in content:
            if isinstance(part, dict) and "text" in part:
                chunks.append(str(part["text"]))
            else:
                chunks.append(str(part))
        return "\n".join(chunks)
    return str(content)


@dataclass
class _AttemptResult:
    intents: List[Intent]
    raw_output: str
    dialog_ticket: Dict[str, Any] | None = None


class IntentDivider:
    def __init__(
        self,
        model_name: str = "",
        max_retry_attempts: int | None = None,
        verbose: bool = False,
    ) -> None:
        self.model_name = str(model_name or _CFG.model_name)
        self.max_retry_attempts = int(max_retry_attempts or _CFG.max_retry_attempts)
        self.verbose = verbose
        self.validator = IntentOutputValidator()
        self.tools = get_intent_divide_tools()
        self.tools_by_name = {t.name: t for t in self.tools}
        self.llm = get_llm(self.model_name).bind_tools(self.tools)
        setattr(self.llm, "_codex_model_name", self.model_name)
        setattr(self.llm, "_codex_model_factory", lambda name: get_llm(name).bind_tools(self.tools))
        self.llm_policy = default_llm_call_policy(self.model_name)

    def divide(self, query: str, database_names: List[str]) -> IntentDivideOutput:
        output, _audit = self.divide_with_audit(query=query, database_names=database_names)
        return output

    def divide_with_audit(self, query: str, database_names: List[str]) -> tuple[IntentDivideOutput, DivideAudit]:
        audit = DivideAudit()
        audit.record("REQUEST", {"query_preview": (query or "")[:200], "database_names": list(database_names or [])})
        errors: List[str] = []
        for attempt in range(1, self.max_retry_attempts + 1):
            if self.verbose:
                logger.info(f"[意图分解] 第 {attempt}/{self.max_retry_attempts} 次尝试开始")
            audit.record("ATTEMPT_START", {"attempt": attempt})
            try:
                result = self._run_attempt(query=query, database_names=database_names, errors=errors)
            except Exception as exc:
                err = f"attempt runtime failed: {type(exc).__name__}: {exc}"
                errors.append(err)
                logger.warning(
                    "意图分解单次尝试失败",
                    attempt=attempt,
                    max_retry_attempts=self.max_retry_attempts,
                    error=err,
                )
                audit.record("ATTEMPT_FAILED", {"attempt": attempt, "error": err})
                continue
            validation = self.validator.validate(result.intents)
            if result.dialog_ticket:
                audit.record("ATTEMPT_WAIT_USER", {"attempt": attempt, "question_id": result.dialog_ticket.get("question_id", "")})
                return (
                    IntentDivideOutput(
                        intents=[],
                        status=IntentDivideStatus.WAIT_USER,
                        dialog_ticket=dict(result.dialog_ticket),
                        errors=[],
                    ),
                    audit,
                )
            if validation.is_valid:
                if self.verbose:
                    logger.info(
                        f"[意图分解] 第 {attempt} 次尝试校验通过 intents={len(result.intents)}"
                    )
                audit.record("ATTEMPT_OK", {"attempt": attempt, "intent_count": len(result.intents)})
                return IntentDivideOutput(intents=result.intents), audit
            errors.append(validation.error_message or "unknown validation error")
            if self.verbose:
                logger.warning(
                    f"[意图分解] 第 {attempt} 次尝试校验失败 error={errors[-1]}"
                )
            logger.warning(
                "意图校验失败",
                attempt=attempt,
                max_retry_attempts=self.max_retry_attempts,
                error=errors[-1],
                model_output=result.raw_output,
            )
            audit.record("ATTEMPT_INVALID", {"attempt": attempt, "error": errors[-1]})
        raise ValueError(
            f"divide intents failed after {self.max_retry_attempts} attempts. "
            f"last_error={errors[-1] if errors else 'unknown'}"
        )

    def _run_attempt(self, query: str, database_names: List[str], errors: List[str]) -> _AttemptResult:
        messages: List[BaseMessage] = [
            SystemMessage(content=self._build_system_prompt()),
            HumanMessage(content=self._build_user_prompt(query=query, database_names=database_names, errors=errors)),
        ]

        final_ai: AIMessage | None = None
        for round_idx in range(1, _CFG.max_tool_rounds + 1):
            if self.verbose:
                logger.info(f"[意图分解] 第 {round_idx}/{_CFG.max_tool_rounds} 轮调用 LLM")
            # 详细日志：LLM 输入（与 autolink 一致，仅写 request 文件）
            _msg_list = []
            for m in messages:
                c = getattr(m, "content", None)
                if c is None:
                    c = str(m)
                else:
                    c = c if isinstance(c, str) else str(c)[:2000]
                if len(c) > 2000:
                    c = c[:2000] + "...[已截断]"
                _msg_list.append({"role": type(m).__name__, "content": c})
            log_step_input(logger, f"intent_divide.llm_round_{round_idx}", {"messages": _msg_list}, round=round_idx)
            ai_msg = invoke_messages_with_policy(
                self.llm,
                messages,
                policy=self.llm_policy,
                model_factory=lambda name: get_llm(name).bind_tools(self.tools),
            )
            if not isinstance(ai_msg, AIMessage):
                ai_msg = AIMessage(content=_to_str_content(ai_msg))
            messages.append(ai_msg)
            final_ai = ai_msg
            # 详细日志：LLM 输出（决策内容、tool_calls）
            _content = _to_str_content(ai_msg.content)
            log_step_output(
                logger,
                f"intent_divide.llm_round_{round_idx}",
                {"content": _content, "tool_calls": ai_msg.tool_calls or []},
                round=round_idx,
            )

            if self.verbose:
                preview = _content
                logger.info(
                    "[意图分解] LLM 输出预览:\n"
                    + (preview[:1200] + ("...[已截断]" if len(preview) > 1200 else ""))
                )

            tool_calls = ai_msg.tool_calls or []
            if not tool_calls:
                if self.verbose:
                    logger.info("[意图分解] 无工具调用，结束当前尝试轮次")
                break

            for call in tool_calls:
                tool_name = call.get("name")
                tool_args = call.get("args", {})
                call_id = call.get("id") or new_id(f"tool_call_{tool_name or 'unknown'}")
                if self.verbose:
                    logger.info(
                        f"[意图分解] 工具调用 name={tool_name} args={json.dumps(tool_args, ensure_ascii=False)}"
                    )
                tool = self.tools_by_name.get(tool_name)
                if tool is None:
                    tool_result = {"error": f"unknown tool: {tool_name}"}
                else:
                    try:
                        tool_result = tool.invoke(tool_args)
                    except Exception as exc:
                        tool_result = {"error": f"tool execution failed: {exc}"}
                if tool_name == "ask_user" and isinstance(tool_result, dict) and tool_result.get("ok") and tool_result.get("ticket_payload"):
                    return _AttemptResult(
                        intents=[],
                        raw_output=json.dumps(tool_result, ensure_ascii=False),
                        dialog_ticket=dict(tool_result.get("ticket_payload") or {}),
                    )
                if self.verbose:
                    result_preview = json.dumps(tool_result, ensure_ascii=False)
                    logger.info(
                        "[意图分解] 工具结果预览:\n"
                        + (result_preview[:1200] + ("...[已截断]" if len(result_preview) > 1200 else ""))
                    )
                messages.append(
                    ToolMessage(
                        content=json.dumps(tool_result, ensure_ascii=False),
                        tool_call_id=call_id,
                    )
                )

        if final_ai is None:
            raise ValueError("llm returned empty response")
        raw_text = _to_str_content(final_ai.content)
        if self.verbose:
            logger.info(
                "[意图分解] 最终原始输出:\n"
                + (raw_text[:3000] + ("...[已截断]" if len(raw_text) > 3000 else ""))
            )
        intents = self._parse_intents(raw_text)
        if self.verbose:
            logger.info(
                "[意图分解] 解析后的意图:\n"
                + json.dumps([x.to_dict() for x in intents], ensure_ascii=False, indent=2)
            )
        return _AttemptResult(intents=intents, raw_output=raw_text)

    def _parse_intents(self, raw_text: str) -> List[Intent]:
        raw_items = _extract_json_array(raw_text)
        intents: List[Intent] = []
        for idx, item in enumerate(raw_items, start=1):
            if not isinstance(item, dict):
                raise ValueError(f"item[{idx}] is not dict")
            intent_id = str(item.get("intent_id") or f"intent_{idx:03d}")
            intent_description = str(item.get("intent_description") or "").strip()
            deps = item.get("dependency_intent_ids", [])
            if deps is None:
                deps = []
            if not isinstance(deps, list):
                deps = [str(deps)]
            dependency_intent_ids = [str(x) for x in deps if str(x).strip()]

            intents.append(
                Intent(
                    intent_id=intent_id,
                    intent_description=intent_description,
                    dependency_intent_ids=dependency_intent_ids,
                )
            )
        return intents

    def _build_system_prompt(self) -> str:
        return (
            STRICT_JSON_ARRAY_NOTICE
            + "\n你是 IntentDivideAgent，负责把用户的自然语言查询拆成可独立执行的 intent 列表。\n"
            "职责边界：\n"
            "1. 只做意图拆分与依赖关系识别，不做 schema 构建，不写 SQL。\n"
            "2. 只有在查询本身确实包含多个可独立执行的子任务时才拆分；否则输出单个 intent。\n"
            "3. dependency_intent_ids 只表达执行前置，不表达语义相似或主题相关。\n"
            "4. intent_description 必须具体、可执行、可直接交给下游 SQL intent runtime。\n"
            "可用工具：\n"
            "- query_columns_by_text: 查询可能相关的列语义，帮助识别实体和约束口径。\n"
            "- ask_user: 当用户问题缺少关键对象、字段或判定口径，导致无法稳定拆分 intent 时，生成澄清 payload。\n"
            "ask_user 的使用原则：\n"
            "1. 仅在缺少关键事实且无法做出稳定 intent 划分时使用。\n"
            "2. ask_user 用于生成澄清请求，不等于最终输出；如使用后仍能给出稳定拆分，最终仍必须输出 JSON intent 数组。\n"
            "3. 不要因为轻微模糊就 ask_user；优先输出最小、保守、可执行的 intent。\n"
            "输出要求：\n"
            "1. 只输出 JSON 数组，不允许解释文本。\n"
            "2. 每个 intent_id 唯一，dependency_intent_ids 只能引用数组内已有 intent_id。\n"
            "不要输出任何解释文本。"
        )

    def _build_user_prompt(self, query: str, database_names: List[str], errors: List[str]) -> str:
        lines = [
            f"用户查询: {query}",
            f"可用数据库: {database_names}",
            "任务要求:",
            "1. 识别最小可执行 intent 集合。",
            "2. 明确每个 intent 的执行目标、实体对象和判定口径。",
            "3. 只有确实存在执行顺序依赖时才填写 dependency_intent_ids。",
            "4. 若缺少关键口径导致无法稳定拆分，可先调用 ask_user 生成澄清 payload。",
        ]
        if errors:
            lines.append("上一轮输出错误如下，请修正:")
            for i, err in enumerate(errors, start=1):
                lines.append(f"{i}. {err}")
        return "\n".join(lines)
