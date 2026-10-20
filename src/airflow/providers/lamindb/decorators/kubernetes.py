"""``@task.lamindb_k8s``: run the function as a LaminDB step in a Kubernetes pod."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from airflow.providers.lamindb.hooks.lamindb import LaminDBHook
from airflow.providers.lamindb.utils.remote import (
    ExcludeTemplated,
    LaminDBSetup,
    RemoteLaminDBStepMixin,
    reject_auto_flow,
    resolve_setup,
)
from airflow.sdk.bases.decorator import task_decorator_factory


def _lamindb_k8s_decorated_operator_class() -> type:
    from airflow.providers.cncf.kubernetes.decorators.kubernetes import _KubernetesDecoratedOperator

    class LaminDBK8sDecoratedOperator(RemoteLaminDBStepMixin, _KubernetesDecoratedOperator):
        """``@task.lamindb_k8s``: LaminDB step in a Kubernetes pod."""

        custom_operator_name = "@task.lamindb_k8s"
        template_fields: Sequence[str] = (
            *_KubernetesDecoratedOperator.template_fields,
            "lamindb_scope",
            "lamindb_conn_id",
            "lamindb_instance",
            "lamindb_space",
            "lamindb_branch",
        )

        # BaseOperatorMeta expects the most-derived class to define __init__; the lamindb
        # arguments are repeated here so that DAG default_args reach them
        def __init__(
            self,
            *,
            lamindb_conn_id: str | None = LaminDBHook.default_conn_name,
            lamindb_instance: str | None = None,
            lamindb_space: str | None = None,
            lamindb_branch: str | None = None,
            lamindb_setup: LaminDBSetup | None = None,
            lamindb_exclude_params: Sequence[str] = (),
            lamindb_exclude_templated: ExcludeTemplated = "secrets",
            **kwargs: Any,
        ) -> None:
            super().__init__(
                lamindb_conn_id=lamindb_conn_id,
                lamindb_instance=lamindb_instance,
                lamindb_space=lamindb_space,
                lamindb_branch=lamindb_branch,
                lamindb_setup=lamindb_setup,
                lamindb_exclude_params=lamindb_exclude_params,
                lamindb_exclude_templated=lamindb_exclude_templated,
                **kwargs,
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
    ``lamindb_instance`` (instance slug; overrides the connection's), ``lamindb_space``
    and ``lamindb_branch`` (space and branch of the step run and the records it saves; default to
    the flow run's, if the step is in a ``LaminDBFlow``; all four templated),
    ``track`` (record the call in LaminDB; with
    ``False`` it runs exactly as under ``@task.kubernetes``), ``connect`` (with ``track=False``,
    still connect the pod to the instance, without recording anything), ``lamindb_setup`` (a
    ``LaminDBSetup`` whose bootstrap code runs in the pod), and ``lamindb_exclude_params``
    and ``lamindb_exclude_templated`` (arguments not to record, as for ``@task.lamindb_venv``).
    The image must have lamindb 2.8.1 or newer installed (the step fails on older versions)
    and credentials for the instance, e.g. ``LAMIN_API_KEY`` from a Kubernetes secret. The
    worker needs no lamindb. Requires
    ``pip install "apache-airflow-providers-lamindb[k8s]"``.

    The step needs the ``lamindb_default`` connection (or the one named by
    ``lamindb_conn_id``) and fails if it is missing. Pass ``lamindb_conn_id=None`` to
    use only the pod's own lamindb configuration, together with ``lamindb_instance`` to
    pin the instance. The steps are standalone unless created inside a ``LaminDBFlow`` block, whose
    flow operators run in a virtualenv on the worker and take their own ``lamindb_conn_id``.
    """
    reject_auto_flow(kwargs)
    try:
        operator_class = _lamindb_k8s_decorated_operator_class()
    except ImportError as e:
        raise ImportError(
            "@task.lamindb_k8s requires apache-airflow-providers-cncf-kubernetes. "
            'Install with: pip install "apache-airflow-providers-lamindb[k8s]"'
        ) from e

    # mapped steps aren't initialised when the DAG is parsed; check the [lamindb] setup_class here
    if kwargs.get("track", True) or kwargs.get("connect", False):
        resolve_setup(kwargs.get("lamindb_setup"))
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=operator_class,
        **kwargs,
    )
