"""An installation's LaminDB setup, named in the ``[lamindb] setup_class`` Airflow option.

test_example_lamindb_lineage.py sets ``AIRFLOW__LAMINDB__SETUP_CLASS=system.lamindb.site_setup.SiteSetup``.
"""

from __future__ import annotations

import textwrap

from airflow.providers.lamindb.execution.setup import LaminDBSetup


# --8<-- [start:howto_lamindb_setup_class]
class SiteSetup(LaminDBSetup):
    def bootstrap_source(self) -> str:
        # runs in the virtualenv before lamindb is imported, e.g. to install a certificate bundle
        return textwrap.dedent(
            """
            import os, sys
            os.environ["SITE_LAMINDB_PRELOADED"] = str("lamindb" in sys.modules)
            """
        )

    def execution_env(self, context) -> dict[str, str]:
        # resolved on the worker when the task runs, never rendered into template fields
        from airflow.sdk import Variable

        return {"SITE_TOKEN": Variable.get("site_token")}


# --8<-- [end:howto_lamindb_setup_class]
