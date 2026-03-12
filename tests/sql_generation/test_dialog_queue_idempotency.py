import sys
import unittest
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from stages.sql_generation.dag.models import GlobalState, IntentNode
from stages.sql_generation.intent.dialog_queue import get_dialog_repository


class TestDialogQueueIdempotency(unittest.TestCase):
    def test_create_ticket_initializes_ask_history(self) -> None:
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
            payload={"intent_id": "I1", "question_id": "Q1", "ask": {"request": "x"}, "acceptance_criteria": []},
            thread_id="thread_test",
        )
        ticket = repo.get_ticket(created.ticket_id)
        assert ticket is not None
        hist = (ticket.payload or {}).get("ask_history")
        self.assertIsInstance(hist, list)
        self.assertTrue(bool(hist))

    def test_record_ask_update_appends_history(self) -> None:
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
            payload={"intent_id": "I1", "question_id": "Q1", "ask": {"request": "x"}, "acceptance_criteria": []},
            thread_id="thread_test",
        )
        repo.record_ask_update(ticket_id=created.ticket_id, ask={"request": "y"}, source="test", summary="s")
        ticket = repo.get_ticket(created.ticket_id)
        assert ticket is not None
        hist = (ticket.payload or {}).get("ask_history")
        self.assertIsInstance(hist, list)
        self.assertGreaterEqual(len(hist), 2)

    def test_append_turn_dedup_by_message_id(self) -> None:
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
            payload={"intent_id": "I1", "question_id": "Q1", "ask": {"request": "x"}, "acceptance_criteria": []},
            thread_id="thread_test",
        )
        repo.append_turn(ticket_id=created.ticket_id, user_message="hello", message_id="m1")
        repo.append_turn(ticket_id=created.ticket_id, user_message="hello", message_id="m1")
        ticket = repo.get_ticket(created.ticket_id)
        assert ticket is not None
        self.assertEqual(len(ticket.turns), 1)
        self.assertEqual(ticket.turns[0].get("message_id"), "m1")

    def test_append_turn_without_message_id_always_appends(self) -> None:
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
            payload={"intent_id": "I1", "question_id": "Q1"},
            thread_id="thread_test",
        )
        repo.append_turn(ticket_id=created.ticket_id, user_message="a")
        repo.append_turn(ticket_id=created.ticket_id, user_message="a")
        ticket = repo.get_ticket(created.ticket_id)
        assert ticket is not None
        self.assertEqual(len(ticket.turns), 2)


if __name__ == "__main__":
    unittest.main()
