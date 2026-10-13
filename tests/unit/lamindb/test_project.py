"""Repository facts that must agree with each other; release.yml checks the tag against the same versions."""

from __future__ import annotations

import re
import sys
from pathlib import Path, PurePosixPath

import pytest
import yaml
from packaging.requirements import Requirement
from packaging.version import Version

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

ROOT = Path(__file__).parents[3]
PYPROJECT = tomllib.loads((ROOT / "pyproject.toml").read_text())
QA_WORKFLOW = yaml.safe_load((ROOT / ".github" / "workflows" / "qa.yml").read_text())
READ_BY_TESTS = (
    "provider.yaml",
    ".github/workflows/qa.yml",
    "docs/changelog.md",
    "tests/system/lamindb/example_lamindb_lineage.py",
    "dev/dags/lamindb_showcase.py",
    "dev/dags/lamindb_showcase_events.py",
)


def test_newest_changelog_section_is_the_provider_version() -> None:
    changelog = (ROOT / "docs" / "changelog.md").read_text()
    headings = re.findall(r"^## (.+?)\s*$", changelog, flags=re.MULTILINE)
    assert headings, "docs/changelog.md has no version section"
    provider_yaml = yaml.safe_load((ROOT / "provider.yaml").read_text())
    assert headings[0] == provider_yaml["versions"][0]


def test_compat_job_covers_the_airflow_floor() -> None:
    dependencies = (Requirement(r) for r in PYPROJECT["project"]["dependencies"])
    airflow = next(r for r in dependencies if r.name == "apache-airflow")
    tested = [Version(v) for v in QA_WORKFLOW["jobs"]["compat"]["strategy"]["matrix"]["airflow-version"]]
    assert all(v in airflow.specifier for v in tested)
    floor = next(Version(s.version) for s in airflow.specifier if s.operator == ">=")
    assert min(tested) == floor


@pytest.mark.parametrize("path", READ_BY_TESTS)
def test_sdist_ships_the_files_the_tests_read(path: str) -> None:
    included = PYPROJECT["tool"]["hatch"]["build"]["targets"]["sdist"]["only-include"]
    assert (ROOT / path).exists()
    assert any(PurePosixPath(path).is_relative_to(entry) for entry in included)
