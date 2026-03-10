from __future__ import annotations

import os
import sys

_this_dir = os.path.dirname(os.path.abspath(__file__))
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)

from stage_utils import add_src_to_path, ensure_db_smoke


if __name__ == "__main__":
    add_src_to_path()
    ensure_db_smoke(database="industrial_monitoring")
    print("ok: DB smoke passed (industrial_monitoring tables present)")
