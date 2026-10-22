"""Worker-side models; only their plain dictionaries are sent to remote runtimes."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class _ExecutionModel(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", hide_input_in_errors=True)


class FlowRunParameters(_ExecutionModel):
    """Fixed Airflow run identity and timing fields, with unrestricted DAG configuration."""

    dag_id: str
    run_id: str
    run_type: str | None = None
    logical_date: str | None = None
    data_interval_start: str | None = None
    data_interval_end: str | None = None
    conf: dict[str, Any] | None = Field(default=None, repr=False)

    @field_validator("run_type", "logical_date", "data_interval_start", "data_interval_end", mode="before")
    @classmethod
    def _stringify_context_value(cls, value: Any) -> str | None:
        return str(value) if value is not None else None


class ConnectedStepConfig(_ExecutionModel):
    """Connection context for an untracked remote callable."""

    instance: str | None = None
    space: str | None = None
    branch: str | None = None


class TrackedStepConfig(ConnectedStepConfig):
    """Arguments passed to the dependency-free remote ``run_step`` runtime."""

    flow_run_reference: str | None
    source_code: str = Field(repr=False)
    fileloc: str | None = None
    step_reference: str | None = None
    exclude_params: list[str] = Field(default_factory=list)


class FlowStartConfig(ConnectedStepConfig):
    """Arguments passed to the remote ``start_flow_run`` runtime."""

    reference: str
    path: str
    entrypoint: str
    params: FlowRunParameters = Field(repr=False)


class FlowFinishConfig(_ExecutionModel):
    """Arguments passed to the remote ``finish_flow_run`` runtime."""

    instance: str | None = None
    reference: str
    success: bool
