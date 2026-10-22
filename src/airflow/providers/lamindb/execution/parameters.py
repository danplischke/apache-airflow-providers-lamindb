"""Which arguments of a step are recorded: templated and secret parameters."""

from __future__ import annotations

import inspect
import logging
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import suppress
from typing import Any, Literal

log = logging.getLogger(__name__)


def space_name(space: Any) -> str | None:
    """A rendered ``lamindb_space`` or ``lamindb_branch`` as a string; empty means lamindb's default."""
    return str(space).strip() or None if space is not None else None


ExcludeTemplated = Literal["secrets", "all", "none"]
"""Which templated arguments a step leaves out of its recorded parameters."""


SECRET_TEMPLATE_NAMES = frozenset({"var", "conn"})
"""Template variables that read Airflow Variables and Connections, where secrets come from."""


def _named_arguments(
    fn: Callable[..., Any], op_args: Sequence[Any] | None, op_kwargs: dict[str, Any] | None
) -> Iterator[tuple[str, Any]]:
    """Yield the parameter name of each argument; ``*args`` and ``**kwargs`` entries as in ``runtime``."""
    parameters = list(inspect.signature(fn).parameters.values())
    positional = [p.name for p in parameters if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
    var_positional = next((p.name for p in parameters if p.kind is p.VAR_POSITIONAL), None)
    for index, value in enumerate(op_args or ()):
        name = positional[index] if index < len(positional) else var_positional
        if name is not None:
            yield name, value
    yield from (op_kwargs or {}).items()


def check_exclude_params(fn: Callable[..., Any], names: Iterable[str]) -> None:
    """Fail for names that can't be arguments of ``fn``."""
    parameters = inspect.signature(fn).parameters
    if any(p.kind is p.VAR_KEYWORD for p in parameters.values()):
        return
    unknown = sorted(set(names) - set(parameters))
    if unknown:
        raise ValueError(
            f"lamindb_exclude_params names {unknown}, which are not parameters of {fn.__name__}()"
        )


def _references_template(source: str, jinja_env: Any, secrets_only: bool) -> bool:
    from jinja2 import TemplateSyntaxError, nodes

    if jinja_env.variable_start_string not in source and jinja_env.block_start_string not in source:
        return False
    try:
        tree = jinja_env.parse(source)
    except TemplateSyntaxError:
        return False  # rendering fails with the reason
    if secrets_only:
        return any(node.name in SECRET_TEMPLATE_NAMES for node in tree.find_all(nodes.Name))
    return any(not isinstance(node, (nodes.Output, nodes.TemplateData)) for node in tree.find_all(nodes.Node))


def is_templated(
    value: Any,
    jinja_env: Any,
    *,
    secrets_only: bool,
    template_ext: Sequence[str] = (),
    skip_oids: Iterable[int] = (),
) -> bool:
    """Whether Airflow renders a template in ``value`` that reads a Variable or Connection, or any template.

    Walks what Airflow renders: strings, files named by ``template_ext``, containers and nested template
    fields. Values in ``skip_oids``, resolved from XComs for mapped tasks, are not rendered.
    """
    skip = set(skip_oids)

    def walk(value: Any) -> bool:
        if id(value) in skip:
            return False
        skip.add(id(value))
        if isinstance(value, str):
            source = value
            if template_ext and value.endswith(tuple(template_ext)):
                with suppress(Exception):  # rendering fails with the reason
                    source = jinja_env.loader.get_source(jinja_env, value)[0]
            return _references_template(source, jinja_env, secrets_only)
        if isinstance(value, dict):
            return any(walk(item) for item in value.values())
        if isinstance(value, (list, tuple, set, frozenset)):
            return any(walk(item) for item in value)
        try:
            fields = list(getattr(value, "template_fields", None) or ())
        except Exception:
            return False
        return any(walk(getattr(value, name, None)) for name in fields)

    return walk(value)


def templated_parameters(
    step: Any, jinja_env: Any, mode: ExcludeTemplated, skip_oids: Iterable[int] = ()
) -> set[str]:
    """Parameters of a decorated step whose arguments contain templates of the kind ``mode`` names."""
    if mode == "none":
        return set()
    oids = set(skip_oids)
    return {
        name
        for name, value in _named_arguments(step.python_callable, step.op_args, step.op_kwargs)
        if is_templated(
            value,
            jinja_env,
            secrets_only=mode == "secrets",
            template_ext=step.template_ext,
            skip_oids=oids,
        )
    }


def _secrets_redact() -> Callable[..., Any] | None:
    try:
        from airflow.sdk._shared.secrets_masker import redact
    except ImportError:
        try:
            from airflow.sdk.execution_time.secrets_masker import redact  # type: ignore[no-redef]
        except ImportError:
            return None
    return redact


def masked_parameters(
    fn: Callable[..., Any], op_args: Sequence[Any] | None, op_kwargs: dict[str, Any] | None
) -> set[str]:
    """Parameters whose argument contains a secret Airflow masks in logs, such as a Connection password.

    Only values count: Airflow matches sensitive names by substring, so ``tokenizer`` or
    ``secretory_cells`` would count as secret. lamindb already redacts parameters with sensitive
    names, matching whole words.
    """
    redact = _secrets_redact()
    if redact is None:
        return set()
    names = set()
    for name, value in _named_arguments(fn, op_args, op_kwargs):
        try:
            if bool(redact(value) != value):
                names.add(name)
        except Exception:
            log.debug("Could not check parameter %r for secrets", name, exc_info=True)
    return names
