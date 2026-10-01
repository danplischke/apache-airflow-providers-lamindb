"""``@task.lamindb_k8s``: run the function as a LaminDB step in a Kubernetes pod."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from airflow.sdk.bases.decorator import task_decorator_factory

from airflow.providers.lamindb.operators.flow import wire_flow_tasks
from airflow.providers.lamindb.utils.remote import RemoteLaminDBStepMixin


def _lamindb_k8s_decorated_operator_class() -> type:
    from airflow.providers.cncf.kubernetes.decorators.kubernetes import _KubernetesDecoratedOperator

    class LaminDBK8sDecoratedOperator(RemoteLaminDBStepMixin, _KubernetesDecoratedOperator):  # type: ignore[misc]
        """``@task.lamindb_k8s``: LaminDB step in a Kubernetes pod."""

        custom_operator_name = "@task.lamindb_k8s"

        # BaseOperatorMeta expects the most-derived class to define __init__
        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            if self.auto_flow:
                wire_flow_tasks(self, venv=True, lamindb_instance=self.lamindb_instance)

    return LaminDBK8sDecoratedOperator


def lamindb_k8s_task(
    python_callable: Callable[..., Any] | None = None,
    multiple_outputs: bool | None = None,
    **kwargs: Any,
):
    """``@task.lamindb_k8s``: run the function as a LaminDB step in a Kubernetes pod.

    Accepts every ``@task.kubernetes`` argument, plus ``lamindb_instance`` (instance
    slug to connect to), ``auto_flow`` (wire virtualenv flow operators on the
    worker if the DAG has none yet; default ``True``) and ``track`` (record the call
    in LaminDB; with ``False`` it runs exactly as under ``@task.kubernetes``). The image must have ``lamindb`` installed and credentials for
    the instance, e.g. ``LAMIN_API_KEY`` from a Kubernetes secret. The worker needs no
    lamindb. Requires ``pip install "lamindb-airflow[cncf.kubernetes]"``.
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
