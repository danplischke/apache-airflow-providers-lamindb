.. _howto/connection:lamindb:

LaminDB Connection
==================

The ``lamindb`` connection type connects Airflow to a LaminDB instance hosted on
`LaminHub <https://lamin.ai>`__ through the `LaminHub REST API <https://docs.lamin.ai/rest>`__.

The default connection ID is ``lamindb_default``.

Configuring the Connection
--------------------------

API Key (password)
    A Lamin API key. It is exchanged for a short-lived access token that the hook refreshes
    automatically. Leave it empty for anonymous access to public instances. For production, use the
    API key of a bot account that has read access to the instance.

Instance (``instance`` extra)
    The instance slug ``owner/name``, e.g. ``laminlabs/lamindata``. Hooks, triggers and sensors
    accept an ``instance`` argument to override it, the lineage operators and decorators a
    ``lamindb_instance`` argument, so one connection can serve several instances of the same account.

LaminHub API URL (host, optional)
    The LaminHub REST API used to exchange the API key and to look up the instance. Defaults to
    ``https://aws.us-east-1.lamin.ai/api``. Requests to the instance itself automatically go to the
    instance's regional API, so hosted instances in any region work with the default. Set this for
    on-prem LaminHub deployments. The lineage operators don't use it: lamindb finds the instance itself.

    The URL is the LaminHub deployment's base URL followed by ``/api``. LaminHub's hosted deployments
    are listed under `Deployments <https://docs.lamin.ai/rest#deployments>`__ in the LaminHub REST
    docs: ``https://aws.us-east-1.lamin.ai``, ``https://aws.us-west-2.lamin.ai``,
    ``https://aws.eu-central-1.lamin.ai`` and ``https://aws.eu-west-2.lamin.ai``. On-prem
    deployments have their own; ask your LaminHub administrator for its base URL. To check a URL,
    open ``<base URL>/_docs`` (the Swagger UI of the API, e.g. https://aws.us-east-1.lamin.ai/_docs)
    or test the connection.

Cache directory (``cache_dir`` extra, optional)
    Absolute path of a lamindb cache that the virtualenvs of the lineage operators and
    ``@task.lamindb_venv`` steps share, for example a directory on a volume mounted on every worker.
    It's passed as ``LAMIN_CACHE_DIR``, so artifacts that one task loaded or saved are not
    downloaded again by the next task. Without it, lamindb uses its default cache directory on each
    worker. Override it per task with ``lamindb_cache_dir``.

    The cache directory must be writable by every task that uses it. ``@task.lamindb_k8s`` pods
    don't get it: mount a volume into the pod and set ``LAMIN_CACHE_DIR`` in its ``env_vars``.

Lineage operators and decorators
--------------------------------

The flow operators and ``@task.lamindb_venv`` steps pass the connection to lamindb in their virtualenv:
the API key as ``LAMIN_API_KEY`` and a temporary, empty ``LAMIN_SETTINGS_DIR``, so lamindb never reads
the worker's ``~/.lamin``. Both are set only while the task runs and are never rendered. With a
connection, the virtualenv installs the instance's lamindb version unless ``lamindb_version`` is given.

``@task.lamindb_k8s`` pods only get the instance from the connection. Mount the API key from a
Kubernetes secret, because environment variables of a pod are readable in its spec.

Pass ``lamindb_conn_id`` to use another connection for a task, or ``lamindb_conn_id=None`` to use
lamindb's own configuration (``LAMIN_API_KEY``, ``LAMIN_CURRENT_INSTANCE``, ``~/.lamin``) instead.

``lamindb_conn_id``, ``lamindb_instance`` and ``lamindb_cache_dir`` are templated in the flow operators
and the step decorators, and ``lamindb_conn_id`` and ``instance`` in the sensors, so an upstream task
can choose them at run time:

.. code-block:: python

    from airflow.providers.lamindb.operators.flow import LaminDBFlowFinishOperator, LaminDBFlowInitOperator
    from airflow.sdk import task

    CONN_ID = "{{ ti.xcom_pull(task_ids='pick_connection') }}"


    @task
    def pick_connection() -> str:
        return "lamindb_staging"


    @task.lamindb_venv(lamindb_conn_id=CONN_ID)
    def process(): ...


    init = LaminDBFlowInitOperator(lamindb_conn_id=CONN_ID)
    pick_connection() >> init
    with init >> LaminDBFlowFinishOperator(lamindb_conn_id=CONN_ID):
        process()

The flow init task runs before every step, so the task that picks the connection must be upstream of
it. Triggers can't be templated: Airflow creates them from the DAG file when it parses it, so their
``lamindb_conn_id`` must be a fixed value.

Examples
--------

Using an environment variable:

.. code-block:: bash

    export AIRFLOW_CONN_LAMINDB_DEFAULT='{
        "conn_type": "lamindb",
        "password": "<lamin-api-key>",
        "extra": {"instance": "my-org/my-instance", "cache_dir": "/shared/lamindb-cache"}
    }'

Using the CLI:

.. code-block:: bash

    airflow connections add lamindb_default \
        --conn-type lamindb \
        --conn-password '<lamin-api-key>' \
        --conn-extra '{"instance": "my-org/my-instance"}'

Anonymous access to a public instance, without a connection:

.. code-block:: python

    from airflow.providers.lamindb.hooks.lamindb import LaminDBHook
    from airflow.providers.lamindb.utils.filters import ArtifactField, F

    hook = LaminDBHook(lamindb_conn_id=None, instance="laminlabs/lamindata")
    hook.query_records("core.artifact", F(ArtifactField.SUFFIX) == ".csv", order_by=["-created_at"], limit=5)

See :doc:`../filters` for building filters.

Testing the connection runs ``LaminDBHook.test_connection``, which resolves the instance and reads its
database write log. Airflow disables connection testing by default; enable it with
``[core] test_connection = Enabled``.
