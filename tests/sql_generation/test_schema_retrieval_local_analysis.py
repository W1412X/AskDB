import sys
import unittest
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from stages.sql_generation.autolink.tools.schema_retrieval import _analyze_local_results


class TestSchemaRetrievalLocalAnalysis(unittest.TestCase):
    def test_low_confidence_rejects(self) -> None:
        ok, safe, diag, err = _analyze_local_results(
            [
                {"table_name": "t1", "column_name": "name", "similarity": 0.12},
                {"table_name": "t2", "column_name": "name", "similarity": 0.11},
            ],
            min_similarity=0.35,
            ambiguity_delta=0.02,
        )
        self.assertFalse(ok)
        self.assertEqual(safe, [])
        self.assertTrue(diag.get("low_confidence"))
        self.assertIn("low_confidence", err)

    def test_ambiguity_rejects_close_scores(self) -> None:
        ok, safe, diag, err = _analyze_local_results(
            [
                {"table_name": "equipment", "column_name": "name", "similarity": 0.52},
                {"table_name": "equipment_types", "column_name": "name", "similarity": 0.51},
                {"table_name": "other", "column_name": "status", "similarity": 0.30},
            ],
            min_similarity=0.35,
            ambiguity_delta=0.02,
        )
        self.assertFalse(ok)
        self.assertEqual(safe, [])
        self.assertTrue(diag.get("ambiguous"))
        self.assertEqual(err, "ambiguous_local_results")

    def test_ok_when_clear_winner(self) -> None:
        ok, safe, diag, err = _analyze_local_results(
            [
                {"table_name": "equipment_types", "column_name": "name", "similarity": 0.60},
                {"table_name": "equipment", "column_name": "name", "similarity": 0.40},
            ],
            min_similarity=0.35,
            ambiguity_delta=0.02,
        )
        self.assertTrue(ok)
        self.assertTrue(bool(safe))
        self.assertEqual(err, "")


if __name__ == "__main__":
    unittest.main()

