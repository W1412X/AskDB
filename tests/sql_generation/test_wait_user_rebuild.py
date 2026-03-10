"""
WAIT_USER dialog repository tests (no LLM/DB required).
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from stages.sql_generation.dag.models import GlobalState, IntentNode
from stages.sql_generation.intent.dialog import get_active_dialog_ticket
from stages.sql_generation.intent.dialog_queue import get_dialog_repository


def test_dialog_repository_roundtrip():
    node = IntentNode(intent_id="I1", description="x", deps=[])
    state = GlobalState(
        intent_map={"I1": node},
        ready_queue=[],
        running_set=set(),
        completed_set=set(),
        dependency_index={},
        remaining_deps_count={"I1": 0},
        tool_registry={},
        config={},
        audit_log=[],
    )
    repo = get_dialog_repository(state)
    created = repo.create_ticket(
        intent_id="I1",
        question_id="Q1",
        phase="BUILDING_SCHEMA",
        payload={"intent_id": "I1", "question_id": "Q1", "priority": 1, "ask": {"situation": "s"}},
        thread_id="thread_test",
    )

    ticket = get_active_dialog_ticket(state)
    assert ticket and ticket["ticket_id"] == created.ticket_id
    assert ticket["intent_id"] == "I1"
    assert ticket["question_id"] == "Q1"
    assert ticket["thread_id"] == "thread_test"

    again = get_active_dialog_ticket(state)
    assert again and again["ticket_id"] == created.ticket_id
