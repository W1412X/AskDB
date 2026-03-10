import argparse
import os
import shutil
from pathlib import Path


def clean_pycache(path: Path) -> None:
    """Recursively delete all __pycache__ directories under `path`."""
    for entry in path.rglob("__pycache__"):
        if entry.is_dir():
            shutil.rmtree(entry, ignore_errors=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Remove __pycache__ directories under a root path.")
    parser.add_argument(
        "root",
        nargs="?",
        default=".",
        help="Root directory to scan for __pycache__ (default is current working directory).",
    )
    args = parser.parse_args()
    root_path = Path(args.root).resolve()
    if not root_path.exists():
        raise SystemExit(f"root path {root_path} does not exist")
    clean_pycache(root_path)
    print(f"Cleaned __pycache__ directories under {root_path}")


if __name__ == "__main__":
    main()
