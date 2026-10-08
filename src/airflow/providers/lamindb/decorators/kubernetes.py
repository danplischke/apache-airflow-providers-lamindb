"""``@task.lamindb_k8s``: run the function as a LaminDB step in a Kubernetes pod."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from airflow.providers.lamindb.hooks.lamindb import LaminDBHook
from airflow.providers.lamindb.operators.flow import wire_flow_tasks
from airflow.providers.lamindb.utils.remote import RemoteLaminDBStepMixin
from airflow.sdk.bases.decorator import task_decorator_factory


def _lamindb_k8s_decorated_operator_class() -> type:
    from airflow.providers.cncf.kubernetes.decorators.kubernetes import _KubernetesDecoratedOperator

    class LaminDBK8sDecoratedOperator(RemoteLaminDBStepMixin, _KubernetesDecoratedOperator):
        """``@task.lamindb_k8s``: LaminDB step in a Kubernetes pod."""

        custom_operator_name = "@task.lamindb_k8s"

        # BaseOperatorMeta expects the most-derived class to define __init__; the lamindb
        # arguments are repeated here so that DAG default_args reach them
        def __init__(
            self,
            *,
            lamindb_conn_id: str | None = LaminDBHook.default_conn_name,
            lamindb_instance: str | None = None,
            **kwargs: Any,
        ) -> None:
            super().__init__(lamindb_conn_id=lamindb_conn_id, lamindb_instance=lamindb_instance, **kwargs)
            if self.auto_flow:
                wire_flow_tasks(
                    self, lamindb_conn_id=self.lamindb_conn_id, lamindb_instance=self.lamindb_instance
                )

    return LaminDBK8sDecoratedOperator


def lamindb_k8s_task(
    python_callable: Callable[..., Any] | None = None,
    multiple_outputs: bool | None = None,
    **kwargs: Any,
):
    """``@task.lamindb_k8s``: run the function as a LaminDB step in a Kubernetes pod.

    Accepts every ``@task.kubernetes`` argument, plus ``lamindb_conn_id`` (Airflow
    connection of type ``lamindb``; the pod only gets its instance, not its API key),
    ``lamindb_instance`` (instance slug; overrides the connection's), ``auto_flow`` (wire
    flow operators, which run in a virtualenv on the worker, if the DAG has none yet;
    default ``True``) and ``track`` (record the call in LaminDB; with ``False`` it runs
    exactly as under ``@task.kubernetes``). The image must have lamindb installed and
    credentials for the instance, e.g. ``LAMIN_API_KEY`` from a Kubernetes secret. The
    worker needs no lamindb. Requires ``pip install "lamindb-airflow[cncf.kubernetes]"``.

    The step needs the ``lamindb_default`` connection (or the one named by
    ``lamindb_conn_id``) and fails if it is missing. Pass ``lamindb_conn_id=None`` to
    use only the pod's own lamindb configuration, together with ``lamindb_instance`` to
    pin the instance. The auto-wired flow operators get the same ``lamindb_conn_id``, so
    with ``None`` they use the worker's lamindb configuration (``LAMIN_API_KEY``,
    ``LAMIN_CURRENT_INSTANCE``, ``~/.lamin``) in their virtualenv.
    """
    try:
        operator_class = _lamindb_k8s_decorated_operator_class()
    except ImportError as e:
        raise ImportError(
            "@task.lamindb_k8s requires apache-airflow-providers-cncf-kubernetes. "
            'Install with: pip install "lamindb-airflow[cncf.kubernetes]"'
        ) from e

    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=operator_class,
        **kwargs,
    )
