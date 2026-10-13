set shell := ['sh', '-cu']

src := "src tests dev"
# throwaway lamindb instance and Airflow home for the integration and system tests
lamin_home := justfile_directory() / ".cache/lamin-home"
airflow_e2e_home := justfile_directory() / ".cache/airflow-e2e"

@_:
    just --list

# Install the package with the dev group, and the pre-commit hooks
[group('development')]
dev:
    uv sync --group dev
    uvx pre-commit install

[group('development')]
format:
    uv run ruff format {{ src }}
    uv run ruff check {{ src }} --fix

# Lint, format and type check, as the CI does
[group('development')]
lint: && typecheck
    uv run ruff check {{ src }}
    uv run ruff format {{ src }} --check

# mypy on Python 3.10, as the CI does; in .cache/py310 so .venv is left alone
[group('development')]
typecheck:
    UV_PROJECT_ENVIRONMENT=.cache/py310 uv run --quiet --python 3.10 --group dev mypy

[group('development')]
chore: format lint

# Run the pre-commit hooks on all files
[group('development')]
pc:
    uvx pre-commit run --all-files

# Unit tests, e.g. `just test -k filters`
[group('test')]
test *args:
    uv run pytest tests/unit {{ args }}

[group('test')]
test-cov:
    uv run pytest tests/unit --cov=src/airflow/providers/lamindb --cov-report=term-missing

# Integration tests against a throwaway local lamindb instance
[group('test')]
test-integration *args: _lamin-instance
    HOME="{{ lamin_home }}" LAMINDB_INTEGRATION_TEST=1 .venv/bin/pytest tests/integration {{ args }}

# System tests through dag.test(), with Airflow's task runner and a throwaway lamindb instance
[group('test')]
test-system *args: _lamin-instance
    #!/usr/bin/env sh
    set -eu
    export HOME="{{ lamin_home }}" AIRFLOW_HOME="{{ airflow_e2e_home }}" AIRFLOW__CORE__LOAD_EXAMPLES=False
    export AIRFLOW__CORE__DAGS_FOLDER="$PWD/tests/system/lamindb"
    .venv/bin/airflow db migrate > /dev/null
    LAMINDB_E2E_TEST=1 .venv/bin/pytest tests/system {{ args }}

# HOME is redirected for lamindb only; .venv/bin is called directly so uv keeps its own cache
_lamin-instance:
    #!/usr/bin/env sh
    set -eu
    uv sync --quiet --group dev
    [ -d "{{ lamin_home }}/store" ] && exit 0
    mkdir -p "{{ lamin_home }}"
    HOME="{{ lamin_home }}" .venv/bin/lamin init --storage "{{ lamin_home }}/store" --name airflowtest

# Tests against Airflow releases with their official constraints, as the compat job does, e.g. `just test-compat 3.3.0`; default: the versions in qa.yml
[group('test')]
test-compat *versions:
    #!/usr/bin/env sh
    set -eu
    versions="{{ versions }}"
    [ -n "$versions" ] || versions=$(sed -n 's/^ *airflow-version: \[\(.*\)\]/\1/p' .github/workflows/qa.yml | tr -d '",')
    for v in $versions; do
        venv="$PWD/.cache/compat/$v"
        echo "== Airflow $v in $venv"
        uv venv --quiet --allow-existing --python 3.12 "$venv"
        export VIRTUAL_ENV="$venv"
        uv pip install --quiet "apache-airflow==$v" apache-airflow-providers-standard \
            apache-airflow-providers-cncf-kubernetes apache-airflow-providers-common-compat \
            pytest pytest-asyncio respx pyyaml jsonschema \
            -c "https://raw.githubusercontent.com/apache/airflow/constraints-$v/constraints-3.12.txt"
        uv pip install --quiet --no-deps -e .
        uv pip check
        "$venv/bin/python" -m pytest tests/unit -q
    done

# Unit tests with every direct dependency at its lowest allowed version on Python 3.10, as the lowest-direct job does
[group('test')]
test-lowest *args:
    #!/usr/bin/env sh
    set -eu
    copy="$PWD/.cache/lowest"
    mkdir -p "$copy"
    rsync -a --delete --exclude .git --exclude .venv --exclude .cache --exclude .lamin ./ "$copy/"
    cd "$copy"
    uv sync --quiet --resolution lowest-direct --group dev --python 3.10
    uv run --no-sync pytest tests/unit -q {{ args }}

[group('test')]
test-all: test test-integration test-system

# Lint, unit tests, package build and smoke test, as the CI does
[group('test')]
check: lint test build smoke

# Build the sdist and wheel into dist/ and check their metadata
[group('package')]
build:
    rm -rf dist
    uv build
    uvx 'twine>=7.0.0' check dist/*

# Import the package without the dev dependencies, lamindb or the cncf.kubernetes extra
[group('package')]
smoke:
    uv run --isolated --no-default-groups python dev/smoke.py

[group('package')]
lock:
    uv lock

# Upgrade the locked dependencies to their latest allowed versions
[group('package')]
upgrade:
    uv lock --upgrade

[group('package')]
versions:
    @git tag -l "v*" --sort=-v:refname | head -10

# Set <ver> in pyproject.toml, __init__.py and provider.yaml once it heads docs/changelog.rst, e.g. `just bump 0.2.0`
[group('package')]
bump ver:
    #!/usr/bin/env python3
    import re
    from pathlib import Path

    ver = "{{ ver }}"
    if not re.fullmatch(r"\d+\.\d+\.\d+((a|b|rc)\d+)?", ver):
        raise SystemExit(f"not a version: {ver}")

    # check before writing, so a failed bump leaves every file untouched
    headings = re.findall(r"^(\S.*?)\s*\n\.{3,}\s*$", Path("docs/changelog.rst").read_text(), flags=re.M)
    if not headings or headings[0] != ver:
        raise SystemExit(f"add a '{ver}' section at the top of docs/changelog.rst, then run `just bump {ver}` again")

    def sub(path, pattern, repl):
        file = Path(path)
        text, n = re.subn(pattern, repl, file.read_text(), count=1, flags=re.M)
        if n != 1:
            raise SystemExit(f"{path}: no match for {pattern}")
        file.write_text(text)

    sub("pyproject.toml", r'^version = ".*"$', f'version = "{ver}"')
    sub("src/airflow/providers/lamindb/__init__.py", r'^__version__ = ".*"$', f'__version__ = "{ver}"')
    if not re.search(rf"^versions:\n  - {re.escape(ver)}$", Path("provider.yaml").read_text(), flags=re.M):
        sub("provider.yaml", r"^versions:\n", f"versions:\n  - {ver}\n")
    print(f"at {ver}; merge to main, then run `just release {ver}`")

# Tag and push v<ver> from a clean main; the release workflow builds and publishes it
[group('publish')]
release ver:
    #!/usr/bin/env sh
    set -eu
    provider=$(awk '/^versions:/ { getline; sub(/^ *- */, ""); print; exit }' provider.yaml)
    [ "$provider" = "{{ ver }}" ] || { echo "provider.yaml is at $provider, not {{ ver }}; run just bump {{ ver }}"; exit 1; }
    [ "$(git branch --show-current)" = "main" ] || { echo "release from main"; exit 1; }
    [ -z "$(git status --porcelain)" ] || { echo "the working tree is not clean"; exit 1; }
    git fetch --quiet origin main
    [ "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)" ] || { echo "main is not at origin/main"; exit 1; }
    git tag -a "v{{ ver }}" -m "Release v{{ ver }}"
    git push origin "v{{ ver }}"
    echo "Pushed v{{ ver }}; follow the release at $(gh repo view --json url --jq .url)/actions"

[group('maintenance')]
clean:
    rm -rf dist .cache .pytest_cache .ruff_cache .mypy_cache .coverage coverage.xml
