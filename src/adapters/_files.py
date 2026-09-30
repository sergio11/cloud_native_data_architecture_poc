"""
Modern Data Architecture POC - Shared filesystem bootstrap helper.

Single home for the "ensure the parent directory exists" one-liner repeated
across adapters, so every adapter creates its directories the same way.
"""

from __future__ import annotations

import os
from pathlib import Path


def ensure_parent_dir(file_path: str) -> None:
    """Creates the parent directory of ``file_path`` (no-op when there is none)."""
    Path(os.path.dirname(file_path) or ".").mkdir(parents=True, exist_ok=True)
