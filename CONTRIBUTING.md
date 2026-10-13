# Contributing

You need [uv](https://docs.astral.sh/uv/) and [just](https://just.systems/). `just` lists every
recipe.

```bash
just dev     # uv sync --group dev, and install the pre-commit hooks
just check   # lint, mypy, unit tests, package build and smoke test, as the CI does
```

## Layout

The layout follows the provider packages in the `apache/airflow` repository: `provider.yaml`,
`get_provider_info.py`, `tests/unit`, `tests/integration`, `tests/system` and `docs`. Keep
`provider.yaml` and `get_provider_info.py` in sync; a unit test checks that they match. The example
DAGs in `tests/system/lamindb` are included in the docs, so keep them readable.

## Checks

| Recipe | What it runs |
| --- | --- |
| `just format` | `ruff format` and `ruff check --fix` |
| `just lint` | ruff and mypy; mypy runs on Python 3.10 in `.cache/py310`, so `.venv` is left alone |
| `just test` | unit tests; lamindb and the LaminHub API are faked, e.g. `just test -k filters` |
| `just test-integration` | the shipped sources against a throwaway local lamindb instance |
| `just test-system` | the example DAGs through `dag.test()`: Airflow's task runner and real virtualenvs |
| `just test-compat` | unit tests against Airflow releases with their official constraints |
| `just test-lowest` | unit tests with every direct dependency at its lowest allowed version |
| `just smoke` | imports the package without the dev dependencies, lamindb or the kubernetes extra |
| `just pc` | the pre-commit hooks on all files |

The integration and system tests keep their lamindb instance and Airflow home in `.cache/`, never in
your real `~/.lamin`. `just clean` removes them.

## Releasing

1. Add a section for the new version at the top of `docs/changelog.rst`.
2. `just bump <version>` sets it in `pyproject.toml`, `__init__.py` and `provider.yaml`.
3. Merge to `main`, then `just release <version>` tags `v<version>` and pushes the tag.

The release workflow runs the QA workflow, checks the tag against those files, then publishes to PyPI
and creates the GitHub release.
