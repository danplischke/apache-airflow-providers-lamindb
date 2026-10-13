# Contributing

You need [uv](https://docs.astral.sh/uv/) and [just](https://just.systems/). `just` lists every
recipe.

```bash
just dev     # uv sync --group dev --group docs, and install the pre-commit hooks
just check   # lint, mypy, unit tests, docs, package build and smoke test, as the CI does
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
| `just docs-build` | the docs, with the same strict settings as the CI |
| `just pc` | the pre-commit hooks on all files |

The integration and system tests keep their lamindb instance and Airflow home in `.cache/`, never in
your real `~/.lamin`. `just clean` removes them.

## Trying it in Airflow

`just standalone` starts Airflow at http://localhost:8080 with the sample DAGs in `dev/dags`, logged
in as admin. Its home is `.airflow/`; `just airflow-reset` deletes it. `just airflow <args>` runs the
Airflow CLI against the same setup, e.g. `just airflow dags test lamindb_showcase`.

The sample DAGs record lineage into the throwaway lamindb instance `anonymous/airflowtest` in
`.cache/lamin-home`; open it with `HOME=.cache/lamin-home .venv/bin/lamin`. To use a hosted
instance instead, export the connection first. That also enables `lamindb_showcase_on_table`, which
the artifact trigger runs for every table `lamindb_showcase` saves:

```bash
export AIRFLOW_CONN_LAMINDB_DEFAULT='{"conn_type": "lamindb", "password": "<API key>", "extra": {"instance": "<owner>/<name>"}}'
just standalone
```

## Docs

The docs in `docs/` are built with [Zensical](https://zensical.org) and published to
[GitHub Pages](https://danplischke.github.io/apache-airflow-providers-lamindb/) on every push to
`main`. `just docs` serves them locally, and `just docs-build` builds them with the same strict
settings as the CI. The navigation is in `zensical.toml`. The API reference in `docs/api.md` is
generated from the docstrings: write them in Markdown, and link to other objects with
``[`F`][airflow.providers.lamindb.utils.filters.F]``, or ``[`F`][]`` within the same module. The guides
include sections of the example DAGs marked with `# --8<-- [start:name]` and `# --8<-- [end:name]`.

## Releasing

1. Add a `## <version>` section at the top of `docs/changelog.md`.
2. `just bump <version>` adds it to the `versions` in `provider.yaml`.
3. Merge to `main`, then `just release <version>` tags `v<version>` and pushes the tag.

The package version comes from the git tag through hatch-vcs; between releases it is a `.dev`
version. The release workflow runs the QA workflow, checks the tag against `provider.yaml` and the
changelog, then publishes to PyPI and creates the GitHub release.
