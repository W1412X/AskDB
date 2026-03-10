"""
Deterministic guidance (lightweight templates) for per-intent planning.

Injects stable, generic constraints into prompts (read-only, verifiable result set).
"""

from __future__ import annotations


def build_template_guidance() -> str:
    """
    Returns a short, deterministic instruction block injected into prompts.
    """
    common = [
        "必须只读（SELECT/WITH 单语句），并输出可验证的结果集。",
        "优先返回与任务目标相关的样本行及原因/计数，并带上主键或业务键列以便定位。",
        "若目标字段/键存在多个候选且无法判定，应输出 ok=false 并说明需要用户确认的字段/维度。",
        "目标：根据任务语义产出最小可执行的 SQL，并给出结果解释。",
    ]
    return "\n".join(["模板约束："] + [f"- {x}" for x in common])
