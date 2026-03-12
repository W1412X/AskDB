from __future__ import annotations

from typing import Any, Dict, List


def build_intent_divide_resume_query(
    *,
    original_query: str,
    question_id: str,
    ticket_payload: Dict[str, Any],
    user_messages: List[str],
) -> str:
    """
    When resuming from intent_divide WAIT_USER, include BOTH:
    - the clarifying question (question_id + question text)
    - the user's replies

    This helps the LLM bind user-provided column/table hints back to the question context.
    """
    base = str(original_query or "").strip()
    parts: List[str] = [base] if base else []

    ask = ticket_payload.get("ask")
    question_text = ""
    if isinstance(ask, dict):
        for k in ("question", "text", "prompt", "request", "instruction", "situation", "title"):
            v = str(ask.get(k) or "").strip()
            if v:
                question_text = v
                break

    # Include the original question context if present.
    if str(question_id or "").strip() or question_text or ask:
        lines: List[str] = []
        qid = str(question_id or "").strip()
        header = "当时系统澄清问题"
        if qid:
            header += f"(question_id={qid})"
        lines.append(header + ":")
        if question_text:
            lines.append(f"- 问题: {question_text}")
        elif isinstance(ask, dict) and ask:
            lines.append(f"- 问题: {ask}")
        elif ask:
            lines.append(f"- 问题: {str(ask)}")

        state_summary = str(ticket_payload.get("state_summary") or "").strip()
        if state_summary:
            lines.append(f"- 状态摘要: {state_summary}")

        criteria = ticket_payload.get("acceptance_criteria")
        if isinstance(criteria, list) and [str(x).strip() for x in criteria if str(x).strip()]:
            lines.append("- 验收标准:")
            lines.extend(f"  - {str(x).strip()}" for x in criteria if str(x).strip())

        parts.append("\n".join(lines))

    if user_messages:
        parts.append("用户补充信息:\n" + "\n".join(f"- {m}" for m in user_messages if str(m).strip()))

    return "\n\n".join([p for p in parts if str(p).strip()]).strip()

