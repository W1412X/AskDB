import sys
import tempfile
import unittest
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from utils.data_paths import DataPaths


class TestDataPathsEnsureDirs(unittest.TestCase):
    def test_ensure_base_dirs_creates_layout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = DataPaths(project_root=root)
            paths.ensure_base_dirs()

            self.assertTrue((root / "data").exists())
            self.assertTrue((root / "data" / "initialize" / "agent").exists())
            self.assertTrue((root / "data" / "initialize" / "embedding").exists())
            self.assertTrue((root / "data" / "initialize" / "checkpoints").exists())
            self.assertTrue((root / "data" / "initialize" / "progress").exists())
            self.assertTrue((root / "data" / "initialize" / "token_usage").exists())
            self.assertTrue((root / "data" / "models" / "embedding").exists())
            self.assertTrue((root / "log").exists())


if __name__ == "__main__":
    unittest.main()

