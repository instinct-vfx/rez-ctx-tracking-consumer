from __future__ import annotations

from pathlib import Path

import pytest

from rez_ctx_consumer.migrations import MigrationError, discover


def write(root: Path, files: dict[str, str]) -> Path:
    for name, content in files.items():
        (root / name).write_bytes(content.encode())
    return root


def test_packaged_migrations() -> None:
    migrations = discover()
    assert [m.version for m in migrations] == list(range(1, len(migrations) + 1))
    assert migrations[0].name == "init"
    assert "CREATE TABLE rez_context_event" in migrations[0].sql


def test_sorted_by_version(tmp_path: Path) -> None:
    write(tmp_path, {"0002_b.sql": "B", "0001_a.sql": "A", "README.md": "ignored"})
    assert [(m.version, m.name) for m in discover(tmp_path)] == [(1, "a"), (2, "b")]


@pytest.mark.parametrize(
    "names",
    [
        ["0001_a.sql", "0003_c.sql"],  # gap
        ["0002_b.sql"],  # doesn't start at 1
        ["0001_a.sql", "0001_b.sql"],  # duplicate
    ],
)
def test_numbering_must_be_contiguous(tmp_path: Path, names: list[str]) -> None:
    write(tmp_path, dict.fromkeys(names, "SELECT 1;"))
    with pytest.raises(MigrationError, match="without gaps"):
        discover(tmp_path)


@pytest.mark.parametrize("name", ["1_a.sql", "0001-a.sql", "0001_A.sql"])
def test_bad_file_names(tmp_path: Path, name: str) -> None:
    write(tmp_path, {name: "SELECT 1;"})
    with pytest.raises(MigrationError, match="NNNN_name"):
        discover(tmp_path)


@pytest.mark.parametrize("newline", ["\n", "\r\n"])
def test_checksum_ignores_line_endings(tmp_path: Path, newline: str) -> None:
    # A Windows checkout with git autocrlf must not look like an edited migration.
    write(tmp_path, {"0001_a.sql": f"SELECT 1;{newline}SELECT 2;{newline}"})
    [migration] = discover(tmp_path)
    assert migration.sql == "SELECT 1;\nSELECT 2;\n"
