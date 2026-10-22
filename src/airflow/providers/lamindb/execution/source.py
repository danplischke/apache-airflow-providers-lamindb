"""Run LaminDB bookkeeping in a virtualenv or pod, so the worker needs no lamindb.

Airflow's virtualenv and Kubernetes operators ship the callable's *source text* to
the remote interpreter and call it by name. We keep that mechanism: the shipped
script embeds ``airflow.providers.lamindb.execution.runtime`` as source and calls into it.
Only lamindb has to be installed remotely; nothing is pickled by reference.
"""

from __future__ import annotations

import ast
import inspect
import keyword
import textwrap
from typing import Any

from airflow.providers.lamindb.execution import runtime

_RUNTIME_LOADER = "_lamindb_airflow_runtime"
_USER_FN = "_lamindb_airflow_user_fn"
_STEP = "_lamindb_airflow_step"

# FunctionDef.type_params exists from Python 3.12 on; ast.unparse reads it there
_TYPE_PARAMS: dict[str, Any] = {"type_params": []} if "type_params" in ast.FunctionDef._fields else {}


def build_remote_step_source(
    *,
    user_source: str,
    callable_name: str,
    config: dict[str, Any],
    runtime_function: str = "run_step",
    bootstrap: str = "",
) -> str:
    """Append a wrapper to ``user_source`` and rebind ``callable_name`` to it.

    The operator's script template then calls the wrapper, which runs the user
    function via ``runtime.<runtime_function>`` (``run_step`` records it, ``run_connected``
    only connects) with ``config`` as keyword arguments. ``bootstrap`` runs first.
    ``user_source`` is kept as is, with its comments.
    """
    # def _lamindb_airflow_step(*args, **kwargs):
    #     return _lamindb_airflow_runtime().<runtime_function>(
    #         _lamindb_airflow_user_fn, args, kwargs, **config
    #     )
    # _lamindb_airflow_user_fn = <callable_name>
    # <callable_name> = _lamindb_airflow_step
    wrapper = [
        _function(
            _STEP,
            ast.arguments(
                posonlyargs=[],
                args=[],
                vararg=ast.arg(arg="args"),
                kwonlyargs=[],
                kw_defaults=[],
                kwarg=ast.arg(arg="kwargs"),
                defaults=[],
            ),
            _call(
                _runtime_attribute(runtime_function),
                [_name(_USER_FN), _name("args"), _name("kwargs")],
                [ast.keyword(arg=None, value=_literal("config", config))],
            ),
        ),
        _assign(_USER_FN, _name(callable_name)),
        _assign(callable_name, _name(_STEP)),
    ]
    return _join(_bootstrap_source(bootstrap), user_source, _runtime_loader_source(), _unparse(wrapper))


def build_remote_flow_source(*, function_name: str, runtime_function: str, bootstrap: str = "") -> str:
    """Define ``function_name(lamindb_airflow_config)`` calling ``runtime.<runtime_function>``.

    The generated function returns the run uid. ``bootstrap`` runs first.
    """
    # def <function_name>(lamindb_airflow_config):
    #     return _lamindb_airflow_runtime().<runtime_function>(**lamindb_airflow_config).uid
    run = _call(
        _runtime_attribute(runtime_function),
        keywords=[ast.keyword(arg=None, value=_name("lamindb_airflow_config"))],
    )
    function = _function(
        function_name,
        _positional_arguments("lamindb_airflow_config"),
        ast.Attribute(value=run, attr="uid", ctx=ast.Load()),
    )
    return _join(_bootstrap_source(bootstrap), _runtime_loader_source(), _unparse([function]))


def _runtime_loader_source() -> str:
    """Define ``_lamindb_airflow_runtime()``, which executes the runtime's source as its own module."""
    # def _lamindb_airflow_runtime():
    #     import types
    #     module = types.ModuleType("lamindb_airflow_runtime")
    #     exec(<runtime source>, module.__dict__)
    #     return module
    loader = ast.FunctionDef(
        name=_RUNTIME_LOADER,
        args=_positional_arguments(),
        body=[
            ast.Import(names=[ast.alias(name="types")]),
            _assign(
                "module",
                _call(
                    _attribute(_name("types"), "ModuleType"),
                    [_literal("module name", "lamindb_airflow_runtime")],
                ),
            ),
            ast.Expr(
                _call(
                    _name("exec"),
                    [
                        _literal("runtime source", inspect.getsource(runtime)),
                        _attribute(_name("module"), "__dict__"),
                    ],
                )
            ),
            ast.Return(_name("module")),
        ],
        decorator_list=[],
        returns=None,
        type_comment=None,
        **_TYPE_PARAMS,
    )
    return _unparse([loader])


def _bootstrap_source(bootstrap: str) -> str:
    """Run ``bootstrap`` as its own module, so it may be indented or start with ``from __future__``."""
    bootstrap = textwrap.dedent(bootstrap).strip()
    if not bootstrap:
        return ""
    # exec(compile(<bootstrap>, "<lamindb_setup.bootstrap_source>", "exec", dont_inherit=True),
    #      {"__name__": "lamindb_setup_bootstrap"})
    compiled = _call(
        _name("compile"),
        [
            _literal("bootstrap", bootstrap),
            _literal("file name", "<lamindb_setup.bootstrap_source>"),
            _literal("mode", "exec"),
        ],
        [ast.keyword(arg="dont_inherit", value=_literal("dont_inherit", True))],
    )
    namespace = _literal("namespace", {"__name__": "lamindb_setup_bootstrap"})
    return "# lamindb_setup.bootstrap_source()\n" + _unparse(
        [ast.Expr(_call(_name("exec"), [compiled, namespace]))]
    )


def _identifier(name: str) -> str:
    if not name.isidentifier() or keyword.iskeyword(name):
        raise ValueError(f"{name!r} is not a Python identifier")
    return name


def _name(name: str, ctx: ast.expr_context | None = None) -> ast.Name:
    return ast.Name(id=_identifier(name), ctx=ctx or ast.Load())


def _attribute(value: ast.expr, attr: str) -> ast.Attribute:
    return ast.Attribute(value=value, attr=_identifier(attr), ctx=ast.Load())


def _runtime_attribute(function: str) -> ast.Attribute:
    """``_lamindb_airflow_runtime().<function>``"""
    return _attribute(_call(_name(_RUNTIME_LOADER)), function)


def _call(
    func: ast.expr, args: list[ast.expr] | None = None, keywords: list[ast.keyword] | None = None
) -> ast.Call:
    return ast.Call(func=func, args=args or [], keywords=keywords or [])


def _assign(target: str, value: ast.expr) -> ast.Assign:
    return ast.Assign(targets=[_name(target, ast.Store())], value=value, type_comment=None)


def _positional_arguments(*names: str) -> ast.arguments:
    return ast.arguments(
        posonlyargs=[],
        args=[ast.arg(arg=_identifier(name)) for name in names],
        vararg=None,
        kwonlyargs=[],
        kw_defaults=[],
        kwarg=None,
        defaults=[],
    )


def _function(name: str, arguments: ast.arguments, returns: ast.expr) -> ast.FunctionDef:
    """``def <name>(<arguments>): return <returns>``"""
    return ast.FunctionDef(
        name=_identifier(name),
        args=arguments,
        body=[ast.Return(returns)],
        decorator_list=[],
        returns=None,
        type_comment=None,
        **_TYPE_PARAMS,
    )


def _literal(name: str, value: Any) -> ast.expr:
    """``value`` as a literal expression; containers become displays of their items."""
    if value is None or isinstance(value, (bool, int, float, str, bytes)):
        return ast.Constant(value=value)
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        return ast.Dict(
            keys=[ast.Constant(value=key) for key in value],
            values=[_literal(name, item) for item in value.values()],
        )
    if isinstance(value, list):
        return ast.List(elts=[_literal(name, item) for item in value], ctx=ast.Load())
    if isinstance(value, tuple):
        return ast.Tuple(elts=[_literal(name, item) for item in value], ctx=ast.Load())
    raise ValueError(f"{name} cannot be embedded in the remote script: {value!r} is not a Python literal")


def _unparse(statements: list[ast.stmt]) -> str:
    return ast.unparse(ast.fix_missing_locations(ast.Module(body=statements, type_ignores=[])))


def _join(*sources: str) -> str:
    return "\n\n\n".join(source.strip("\n") for source in sources if source) + "\n"
