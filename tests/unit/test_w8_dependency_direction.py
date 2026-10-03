"""
W8 architecture gate: hexagonal dependency DIRECTION enforced via stdlib AST.

No import-linter (dev-only dependency ban): this test parses every ``src/``
module with :mod:`ast` and asserts the dependency rules that the hexagonal
design promises but nothing else enforces.

Forbidden-import matrix (derived 2026-10-03 from the ACTUAL imports of the
then-clean tree; the allowlists are ratchets - adding a new import outside the
allowlist fails the test and must be justified in review):

=================  =========================================  =========================
Layer              MAY import                                 MUST NOT import
=================  =========================================  =========================
src/domain/**      stdlib: __future__, collections, datetime,  src.ports, src.use_cases,
                   logging, typing, dataclasses, enum,          src.adapters, src.container,
                   hashlib; src.domain.**                      ANY other top-level module
                                                               (sqlite3, yaml, requests,
                                                               pathlib, os, sys, json,
                                                               urllib, http, kafka, ... are
                                                               therefore all forbidden)
src/ports/**       stdlib: __future__, typing;                 src.use_cases, src.adapters,
                   src.domain.**, src.ports.**                 src.container; any other module
src/use_cases/**   stdlib: __future__, logging, typing,        src.adapters, src.container
                   dataclasses, datetime, pathlib, uuid;       (also caught by the allowlist);
                   third-party: jsonschema (observed,          any other third-party module
                   opportunistic import inside the use case);
                   src.domain.**, src.ports.**, src.use_cases.**
src/adapters/**    anything EXCEPT the two columns at right    src.use_cases, src.container
src/container.py   (composition root) - no rules               -
=================  =========================================  =========================

TYPE_CHECKING policy: ``if TYPE_CHECKING:`` imports are read separately and
subject to the SAME rules as runtime imports. In particular an adapter may not
import ``src.use_cases`` even for annotations (the dependency must not exist in
any form), and the domain layer may import nothing beyond its allowlist even
under TYPE_CHECKING.

The policy engine is also fed SYNTHETIC sources (``test_engine_*``) so a rule
that silently stops matching cannot rot unnoticed.
"""

from __future__ import annotations

import ast
import sys
from dataclasses import dataclass, field
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"


# --------------------------------------------------------------------------
# Policy model
# --------------------------------------------------------------------------
@dataclass(frozen=True)
class LayerPolicy:
    """Import policy for one hexagonal layer (directory under ``src/``)."""

    #: Dotted prefixes of ``src.*`` modules this layer may import.
    allowed_src: frozenset[str]
    #: Top-level (stdlib / third-party) module names this layer may import.
    #: ``None`` means "unrestricted except for ``forbidden_src``" (adapters).
    allowed_top_level: frozenset[str] | None
    #: Dotted prefixes that must never appear, even if an allowlist would
    #: somehow match (explicit, self-documenting layering rules).
    forbidden_src: tuple[str, ...] = ()
    #: Named infra modules forbidden by design (checked explicitly so the
    #: failure message names the offender; they are also absent from
    #: ``allowed_top_level``).
    forbidden_infra: tuple[str, ...] = ()


#: Stdlib infrastructure that must never leak into the pure core layers.
#: (``kafka`` is third-party but listed for the same reason.)
_INFRA = (
    "sqlite3",
    "yaml",
    "requests",
    "pathlib",
    "os",
    "sys",
    "json",
    "urllib",
    "http",
    "kafka",
    "socket",
    "subprocess",
    "tempfile",
    "shutil",
    "glob",
    "importlib",
    "threading",
    "multiprocessing",
    "pickle",
)

POLICIES: dict[str, LayerPolicy] = {
    "domain": LayerPolicy(
        allowed_src=frozenset({"src.domain"}),
        allowed_top_level=frozenset(
            {
                "__future__",
                "collections",
                "datetime",
                "enum",
                "hashlib",
                "logging",
                "dataclasses",
                "typing",
            }
        ),
        forbidden_src=("src.ports", "src.use_cases", "src.adapters", "src.container"),
        forbidden_infra=_INFRA,
    ),
    "ports": LayerPolicy(
        allowed_src=frozenset({"src.domain", "src.ports"}),
        allowed_top_level=frozenset({"__future__", "typing"}),
        forbidden_src=("src.use_cases", "src.adapters", "src.container"),
        forbidden_infra=_INFRA,
    ),
    "use_cases": LayerPolicy(
        allowed_src=frozenset({"src.domain", "src.ports", "src.use_cases"}),
        # pathlib (PurePath only, no I/O) and jsonschema (opportunistic
        # governance validation) are observed in the layer today: ratchet,
        # not endorsement.
        allowed_top_level=frozenset(
            {
                "__future__",
                "dataclasses",
                "datetime",
                "jsonschema",
                "logging",
                "pathlib",
                "typing",
                "uuid",
            }
        ),
        forbidden_src=("src.adapters", "src.container"),
        forbidden_infra=_INFRA,
    ),
    "adapters": LayerPolicy(
        # Adapters talk to the outside world: no top-level allowlist.
        allowed_src=frozenset({"src.adapters", "src.domain", "src.ports"}),
        allowed_top_level=None,
        forbidden_src=("src.use_cases", "src.container"),
    ),
}


# --------------------------------------------------------------------------
# AST import extraction
# --------------------------------------------------------------------------
@dataclass
class FileImports:
    """Imports of one module, split by execution context."""

    runtime: set[str] = field(default_factory=set)
    type_checking: set[str] = field(default_factory=set)

    @property
    def all(self) -> set[str]:
        return self.runtime | self.type_checking


def _is_typecheck_test(node: ast.expr) -> bool:
    if isinstance(node, ast.Name):
        return node.id == "TYPE_CHECKING"
    if isinstance(node, ast.Attribute):
        return node.attr == "TYPE_CHECKING"
    if (
        isinstance(node, ast.UnaryOp)
        and isinstance(node.op, ast.Not)
        and isinstance(node.operand, ast.Name)
        and node.operand.id == "TYPE_CHECKING"
    ):
        return True
    return False


def _module_parts_for(filename: str) -> list[str]:
    """Module parts of ``filename`` relative to the ``src`` package."""
    path = Path(filename)
    try:
        rel = path.resolve().relative_to(SRC_ROOT.resolve())
    except ValueError:
        return [path.stem]
    parts = list(rel.parts)
    if parts and parts[-1].endswith(".py"):
        parts[-1] = parts[-1][:-3]
    if parts and parts[-1] == "__init__":
        parts = parts[:-1]
    return parts


def _resolve_relative(file_parts: list[str], level: int, module: str | None) -> str | None:
    """Resolves a PEP 328 relative import to an absolute dotted name."""
    package = list(file_parts[:-1]) if file_parts else []
    up = level - 1
    if up > len(package):
        return None  # would escape the src package: treat as unresolvable
    base = ["src"] + package[: len(package) - up]
    if module:
        base += module.split(".")
    return ".".join(base)


def collect_imports(source: str, filename: str = "<synthetic>") -> FileImports:
    """Extracts absolute module names imported by ``source``.

    Relative imports are resolved against ``filename``'s package (a rare but
    legal style), yielding absolute ``src.*`` names when they stay inside the
    tree. Imports nested under ``if TYPE_CHECKING:`` (including nested deeper
    inside a TYPE_CHECKING block) are recorded separately.
    """
    tree = ast.parse(source, filename)
    result = FileImports()
    file_parts = _module_parts_for(filename)

    class Visitor(ast.NodeVisitor):
        def __init__(self) -> None:
            self.tc_depth = 0

        def visit_If(self, node: ast.If) -> None:
            self.visit(node.test)
            if _is_typecheck_test(node.test):
                self.tc_depth += 1
                for stmt in node.body:
                    self.visit(stmt)
                self.tc_depth -= 1
            else:
                for stmt in node.body:
                    self.visit(stmt)
            for stmt in node.orelse:
                self.visit(stmt)

        def visit_Import(self, node: ast.Import) -> None:
            target = result.type_checking if self.tc_depth else result.runtime
            for alias in node.names:
                target.add(alias.name)

        def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
            target = result.type_checking if self.tc_depth else result.runtime
            if node.level > 0:
                resolved = _resolve_relative(file_parts, node.level, node.module)
                if resolved is not None:
                    target.add(resolved)
                if node.module is None:
                    # `from . import name` - each alias is a submodule here.
                    for alias in node.names:
                        child = _resolve_relative(file_parts, node.level, alias.name)
                        if child is not None:
                            target.add(child)
                return
            if node.module is not None:
                target.add(node.module)
            else:  # pragma: no cover - ast never emits level==0 with module None
                for alias in node.names:
                    target.add(alias.name)

    Visitor().visit(tree)
    return result


# --------------------------------------------------------------------------
# Policy evaluation
# --------------------------------------------------------------------------
def violations_for(layer: str, module_name: str) -> list[str]:
    """Returns human-readable violations of ``layer``'s policy for one import."""
    policy = POLICIES[layer]
    for prefix in policy.forbidden_src:
        if module_name == prefix or module_name.startswith(prefix + "."):
            # Explicit layering message wins over allowlist noise.
            return [f"{layer} must not import {module_name} (forbidden layer {prefix})"]
    if module_name == "src" or module_name.startswith("src."):
        for allowed in policy.allowed_src:
            if module_name == allowed or module_name.startswith(allowed + "."):
                return []
        return [f"{layer} must not import {module_name} (allowed src packages: {sorted(policy.allowed_src)})"]
    if policy.allowed_top_level is None:
        return []
    top_level = module_name.split(".")[0]
    if top_level in policy.allowed_top_level:
        return []
    infra = ""
    if top_level in policy.forbidden_infra or module_name in policy.forbidden_infra:
        infra = f"; '{top_level}' is forbidden infrastructure for this layer"
    return [f"{layer} must not import '{module_name}' (allowlist: {sorted(policy.allowed_top_level)}){infra}"]


def _display(path: Path) -> str:
    """Repo-relative path when possible (fake trees outside the repo fall back)."""
    try:
        return path.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def scan_layer(layer: str) -> list[str]:
    """Collects policy violations for every module of one layer on disk."""
    layer_dir = SRC_ROOT / layer
    assert layer_dir.is_dir(), f"expected layer directory {layer_dir} to exist"
    problems: list[str] = []
    for path in sorted(layer_dir.rglob("*.py")):
        imports = collect_imports(path.read_text(encoding="utf-8"), filename=str(path))
        for module_name in sorted(imports.all):
            for problem in violations_for(layer, module_name):
                problems.append(f"{_display(path)}: {problem}")
    return problems


# --------------------------------------------------------------------------
# Tests: the live tree
# --------------------------------------------------------------------------
def test_src_inventory_is_scanned() -> None:
    """Guard against a vacuous pass (broken path => zero files => green)."""
    py_files = list(SRC_ROOT.rglob("*.py"))
    assert len(py_files) >= 20, f"expected to scan the full src tree, found {len(py_files)} files"
    assert (SRC_ROOT / "domain").is_dir()
    assert (SRC_ROOT / "adapters").is_dir()


def test_domain_imports_only_pure_stdlib_and_itself() -> None:
    problems = scan_layer("domain")
    assert problems == [], "\n".join(problems)


def test_ports_import_only_pure_stdlib_domain_and_ports() -> None:
    problems = scan_layer("ports")
    assert problems == [], "\n".join(problems)


def test_use_cases_import_no_adapters_or_container() -> None:
    problems = scan_layer("use_cases")
    assert problems == [], "\n".join(problems)


def test_adapters_never_import_use_cases_or_container() -> None:
    problems = scan_layer("adapters")
    assert problems == [], "\n".join(problems)


def test_domain_infrastructure_modules_are_forbidden_by_name() -> None:
    """Explicit matrix check: the infra list from the design doc is absent.

    Redundant with the allowlist (these names are not allowlisted) but kept as
    a named, self-documenting assertion - and as a canary that the allowlist
    itself was not carelessly widened to include I/O modules.
    """
    policy = POLICIES["domain"]
    assert policy.allowed_top_level is not None
    for module_name in policy.forbidden_infra:
        assert module_name not in policy.allowed_top_level
    seen: set[str] = set()
    for path in sorted((SRC_ROOT / "domain").rglob("*.py")):
        seen |= collect_imports(path.read_text(encoding="utf-8"), filename=str(path)).all
    offenders = sorted(m for m in seen if m.split(".")[0] in policy.forbidden_infra)
    assert offenders == [], f"domain imports infrastructure modules: {offenders}"


# --------------------------------------------------------------------------
# Tests: the policy engine itself (synthetic sources - the engine must bite)
# --------------------------------------------------------------------------
def test_engine_flags_domain_importing_adapter() -> None:
    imports = collect_imports("from src.adapters.foo import bar\n")
    assert imports.runtime == {"src.adapters.foo"}
    assert violations_for("domain", "src.adapters.foo")
    assert violations_for("domain", "src.adapters")  # prefix rule


def test_engine_flags_domain_importing_infra_stdlib() -> None:
    for statement in ("import os", "import sqlite3", "import json", "from pathlib import Path"):
        imports = collect_imports(statement + "\n")
        name = sorted(imports.runtime)[0]
        assert violations_for("domain", name), f"{statement} should violate domain policy"
        assert violations_for("ports", name), f"{statement} should violate ports policy"


def test_engine_flags_use_cases_importing_container() -> None:
    imports = collect_imports("from src.container import container\n")
    assert imports.runtime == {"src.container"}
    assert violations_for("use_cases", "src.container")
    assert violations_for("adapters", "src.container")


def test_engine_flags_adapter_importing_use_case_even_under_type_checking() -> None:
    source = (
        "from __future__ import annotations\n"
        "if TYPE_CHECKING:\n"
        "    from src.use_cases.ingest_bronze import IngestBronzeUseCase\n"
    )
    imports = collect_imports(source)
    assert imports.runtime == {"__future__"}
    assert imports.type_checking == {"src.use_cases.ingest_bronze"}
    assert violations_for("adapters", "src.use_cases.ingest_bronze")


def test_engine_allows_legal_imports() -> None:
    legal = {
        "domain": ["from src.domain.entities import Order", "import hashlib", "import logging"],
        "ports": ["from src.domain.entities import Order", "import typing"],
        "use_cases": [
            "from src.ports.outbound import EventBrokerPort",
            "from src.domain.naming import stream_table",
        ],
        "adapters": [
            "import sqlite3",
            "from src.ports.outbound import EventBrokerPort",
            "import requests",
        ],
    }
    for layer, statements in legal.items():
        for statement in statements:
            imports = collect_imports(statement + "\n")
            for name in imports.all:
                assert violations_for(layer, name) == [], f"{layer}: {statement} unexpectedly violated"


def test_engine_resolves_relative_imports_inside_domain() -> None:
    imports = collect_imports(
        "from .entities import Order\nfrom . import naming\n",
        filename=str(SRC_ROOT / "domain" / "quality_gate.py"),
    )
    assert "src.domain.entities" in imports.runtime
    assert "src.domain.naming" in imports.runtime
    assert violations_for("domain", "src.domain.entities") == []
    assert violations_for("domain", "src.domain.naming") == []


def test_scan_layer_flags_violating_files_on_disk(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """End-to-end negative control for the on-disk scan (fake src tree).

    Proves ``scan_layer`` surfaces violations found in real files - without
    planting anything under the repository's actual ``src/``.
    """
    fake_src = tmp_path / "src"
    (fake_src / "domain").mkdir(parents=True)
    (fake_src / "domain" / "good.py").write_text(
        "import hashlib\nfrom src.domain.entities import Order\n", encoding="utf-8"
    )
    (fake_src / "domain" / "bad.py").write_text(
        "import sqlite3\nfrom src.adapters.memory import MemStorage\n", encoding="utf-8"
    )
    monkeypatch.setattr(sys.modules[__name__], "SRC_ROOT", fake_src)

    problems = scan_layer("domain")

    joined = "\n".join(problems)
    assert "domain/bad.py" in joined and "sqlite3" in joined
    assert "domain/bad.py" in joined and "src.adapters.memory" in joined
    assert "domain/good.py" not in joined
    assert len(problems) == 2, f"expected exactly the 2 violations, got:\n{joined}"
