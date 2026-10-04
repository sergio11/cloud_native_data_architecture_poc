"""Unit tests: PII salt wiring from the composition root (DIP).

The domain (``src/domain/privacy.py``) is pure and never reads the environment;
only ``src/container.py`` resolves ``PII_HASH_SALT`` and injects it into
``AggregateGoldUseCase`` as the explicit ``privacy_salt`` constructor argument.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.container import Container
from src.domain.privacy import DEFAULT_PII_HASH_SALT


def _container(tmp_path: Path) -> Container:
    return Container(db_path=str(tmp_path / "lakehouse.db"), repo_root=str(tmp_path))


def test_container_injects_env_salt_into_gold_use_case(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """With ``PII_HASH_SALT=abc``, the container-built use case uses "abc"."""
    monkeypatch.setenv("PII_HASH_SALT", "abc")
    use_case = _container(tmp_path).aggregate_gold
    assert use_case._privacy_salt == "abc"


def test_container_falls_back_to_default_salt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Without the env var, the container injects the documented default salt."""
    monkeypatch.delenv("PII_HASH_SALT", raising=False)
    use_case = _container(tmp_path).aggregate_gold
    assert use_case._privacy_salt == DEFAULT_PII_HASH_SALT
