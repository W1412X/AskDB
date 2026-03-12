import sys
import unittest
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from stages.sql_generation.divide_resume import build_intent_divide_resume_query


class TestDivideResumeQuery(unittest.TestCase):
    def test_includes_question_and_user_reply(self) -> None:
        q = build_intent_divide_resume_query(
            original_query="帮我查设备名称的来源。",
            question_id="Q_equipment_name_field",
            ticket_payload={
                "ask": {"question": "“设备名称”具体指哪个字段？（例如 equipment_types.name 还是 equipment.name）"},
                "acceptance_criteria": ["明确字段名", "如需 join 请说明关联键"],
            },
            user_messages=["设备名称指 equipment_types.name。"],
        )
        self.assertIn("question_id=Q_equipment_name_field", q)
        self.assertIn("设备名称", q)
        self.assertIn("equipment_types.name", q)
        self.assertIn("用户补充信息", q)

    def test_graceful_without_question(self) -> None:
        q = build_intent_divide_resume_query(
            original_query="x",
            question_id="",
            ticket_payload={},
            user_messages=["y"],
        )
        self.assertIn("x", q)
        self.assertIn("y", q)


if __name__ == "__main__":
    unittest.main()
