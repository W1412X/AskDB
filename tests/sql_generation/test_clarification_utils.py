import sys
import unittest
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from stages.sql_generation.intent.clarification_utils import (
    default_next_ask,
    is_actionable_hints,
    merge_hints,
    normalize_hints,
)


class TestClarificationUtils(unittest.TestCase):
    def test_normalize_hints_whitelist_and_dedup(self) -> None:
        out = normalize_hints(
            {
                "known_tables": ["equipment", "equipment", "  "],
                "known_columns": "equipment.serial_number",
                "time_range": " 2026-01-01~2026-01-31 ",
                "unknown": {"x": 1},
            }
        )
        self.assertEqual(out.get("known_tables"), ["equipment"])
        self.assertEqual(out.get("known_columns"), ["equipment.serial_number"])
        self.assertEqual(out.get("time_range"), "2026-01-01~2026-01-31")
        self.assertNotIn("unknown", out)

    def test_merge_hints_appends_lists(self) -> None:
        merged = merge_hints(
            {"known_tables": ["a"], "keywords": ["k1"]},
            {"known_tables": ["b", "a"], "keywords": ["k2"]},
        )
        self.assertEqual(merged.get("known_tables"), ["a", "b"])
        self.assertEqual(merged.get("keywords"), ["k1", "k2"])

    def test_is_actionable_hints(self) -> None:
        self.assertTrue(is_actionable_hints({"known_columns": ["t.c"]}))
        self.assertTrue(is_actionable_hints({"keywords": ["设备名称=t.name"]}))
        self.assertFalse(is_actionable_hints({"keywords": ["没有表字段"]}))
        self.assertFalse(is_actionable_hints({}))

    def test_default_next_ask_shape(self) -> None:
        ask = default_next_ask()
        self.assertIn("situation", ask)
        self.assertIn("request", ask)
        self.assertIn("why_needed", ask)
        self.assertIsInstance(ask.get("examples"), list)
        self.assertIsInstance(ask.get("constraints"), list)


if __name__ == "__main__":
    unittest.main()

