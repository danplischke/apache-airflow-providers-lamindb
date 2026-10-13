"""
Build, validate and evaluate LaminHub REST filters.

Filters select records by their fields. Build them with :class:`F` and the field enums, or write the
JSON form of the `LaminHub REST API <https://docs.lamin.ai/rest>`__ directly. These are equivalent::

    F(ArtifactField.KEY).startswith("raw/") & (F(ArtifactField.SIZE) > 1_000_000)
    {"and": [{"key": {"startswith": "raw/"}}, {"size": {"gt": 1000000}}]}
"""

from __future__ import annotations

import datetime
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any, NoReturn

from airflow.providers.lamindb.utils.enums import (
    REGISTRY_FIELDS,
    ArtifactField,
    ArtifactKind,
    BranchField,
    CollectionField,
    FeatureField,
    FilterOperator,
    LaminDBRegistry,
    ProjectField,
    RecordField,
    ReferenceField,
    RegistryField,
    RunField,
    RunStatus,
    SchemaField,
    SpaceField,
    StorageField,
    TransformField,
    TransformKind,
    ULabelField,
    UserField,
    plain_value,
)

__all__ = [
    "REGISTRY_FIELDS",
    "And",
    "ArtifactField",
    "ArtifactKind",
    "BranchField",
    "CollectionField",
    "Condition",
    "Count",
    "F",
    "FeatureField",
    "Filter",
    "FilterExpression",
    "FilterLike",
    "FilterOperator",
    "LaminDBRegistry",
    "Or",
    "ProjectField",
    "RecordField",
    "ReferenceField",
    "RegistryField",
    "RunField",
    "RunStatus",
    "SchemaField",
    "SpaceField",
    "StorageField",
    "TransformField",
    "TransformKind",
    "ULabelField",
    "UserField",
    "combine_filters",
    "filter_field_paths",
    "is_local_filter",
    "matches_filter",
    "normalize_filter",
    "normalize_order_by",
]

Filter = Mapping[str, Any]
"""A LaminHub REST filter in its JSON form, e.g. ``{"key": {"eq": "raw/a.csv"}}``."""

_BOOL_HINT = (
    "Filters can't be used as booleans: combine them with & and | instead of 'and' and 'or', and write "
    "ranges as (F(field) >= low) & (F(field) < high) instead of low <= F(field) < high."
)
_PRECEDENCE_HINT = (
    "Fields can't be combined with & and |; wrap comparisons in parentheses first, "
    "e.g. (F('suffix') == '.csv') & (F('size') > 0)."
)
_NEGATION_HINT = "LaminHub filters can't be negated; use ne() (!=), not_in() or is_not_null() instead."


def _member_name(member: RegistryField) -> str:
    return f"{type(member).__name__}.{member.name}"


def _describe(part: str) -> str:
    return _member_name(part) if isinstance(part, RegistryField) else repr(part)


def _is_template(value: Any) -> bool:
    return isinstance(value, str) and "{{" in value


def _json_value(value: Any) -> Any:
    """Convert enum members, dates and collections in a filter value to their JSON form."""
    value = plain_value(value)
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, (set, frozenset)):
        items = [_json_value(item) for item in value]
        # sets have no order; sort them so that the filter (and a trigger's cursor key) is stable
        try:
            return sorted(items)
        except TypeError:
            return sorted(items, key=repr)
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, Mapping):
        return {str(plain_value(key)): _json_value(item) for key, item in value.items()}
    return value


def _is_json(value: Any) -> bool:
    if value is None or isinstance(value, (bool, int, float, str)):
        return True
    if isinstance(value, list):
        return all(_is_json(item) for item in value)
    if isinstance(value, dict):
        return all(_is_json(item) for item in value.values())
    return False


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_comparable(value: Any) -> bool:
    return isinstance(value, str) or _is_number(value)


def _is_scalar(value: Any) -> bool:
    return isinstance(value, (str, bool)) or _is_number(value)


def _is_count(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


_VALUE_CHECKS: dict[FilterOperator, tuple[Callable[[Any], bool], str]] = {
    FilterOperator.GT: (_is_comparable, "a string or a number"),
    FilterOperator.GTE: (_is_comparable, "a string or a number"),
    FilterOperator.LT: (_is_comparable, "a string or a number"),
    FilterOperator.LTE: (_is_comparable, "a string or a number"),
    FilterOperator.IN: (lambda value: isinstance(value, list), "a list of values"),
    FilterOperator.NOT_IN: (lambda value: isinstance(value, list), "a list of values"),
    FilterOperator.IS_NULL: (lambda value: isinstance(value, bool), "True or False"),
    FilterOperator.STARTSWITH: (lambda value: isinstance(value, str), "a string"),
    FilterOperator.ENDSWITH: (lambda value: isinstance(value, str), "a string"),
    FilterOperator.CONTAINS: (_is_scalar, "a string, number or boolean"),
    **{operator: (_is_count, "an integer") for operator in FilterOperator if operator.is_count},
}


def _check_value(operator: FilterOperator, value: Any, path: str) -> Any:
    """Validate the value of a condition and return its JSON form."""
    if isinstance(value, (F, Count, FilterExpression)):
        raise TypeError(f"Can't compare {path!r} with {value!r}: filters compare fields with values")
    value = _json_value(value)
    if _is_template(value):
        return value  # rendered by Airflow before the query
    is_valid, expected = _VALUE_CHECKS.get(operator, (_is_json, "a JSON value"))
    if not is_valid(value) or not _is_json(value):
        raise TypeError(f"Operator {operator.value!r} on {path!r} expects {expected}, got {value!r}")
    return value


def _check_relation_step(relation: RegistryField, part: str) -> None:
    """Check that ``part`` can follow the field enum member ``relation`` in a field path."""
    if relation.related is None:
        raise ValueError(
            f"{_member_name(relation)} is not a relation, so F() can't follow it to {_describe(part)}"
        )
    expected = REGISTRY_FIELDS.get(relation.related)
    if isinstance(part, RegistryField) and expected is not None and not isinstance(part, expected):
        raise ValueError(
            f"{_member_name(relation)} points to {relation.related}: follow it with {expected.__name__} "
            f"members, not {_member_name(part)}"
        )


def _relation_hint(relation: RegistryField) -> str:
    target = REGISTRY_FIELDS.get(relation.related) if relation.related else None
    target_id = f"{target.__name__}.ID" if target else "'id'"
    hint = (
        f"{_member_name(relation)} is a relation to {relation.related}; compare a field of the related "
        f"records instead, e.g. F({_member_name(relation)}, {target_id})"
    )
    foreign_key = type(relation).__members__.get(f"{relation.name}_ID")
    if foreign_key is not None:
        hint += f" or F({_member_name(foreign_key)})"
    return hint + ", or count them with .count()"


class F:
    """
    A field to filter on, e.g. ``F(ArtifactField.KEY)``, ``F("key")`` or ``F("created_by.handle")``.

    Compare it with a value to get a :class:`Condition`, using Python's comparison operators (``==``,
    ``!=``, ``>``, ``>=``, ``<``, ``<=``) or the methods of this class. Combine conditions with ``&``
    (and) and ``|`` (or); as ``&`` and ``|`` bind more tightly than comparisons, wrap comparisons in
    parentheses::

        (F(ArtifactField.SUFFIX) == ".parquet") & (F(ArtifactField.SIZE) > 1_000_000)
        F(ArtifactField.KEY).startswith("raw/") | F(ArtifactField.KIND).is_in([ArtifactKind.DATASET])

    Pass several fields to follow relations: ``F(ArtifactField.CREATED_BY, UserField.HANDLE)`` is the
    path ``created_by.handle``. Index JSON fields with keys: ``F(RunField.PARAMS)["learning_rate"]``.

    :param path: Field enum members or field names, e.g. ``ArtifactField.KEY`` or ``"key"``.
    """

    __slots__ = ("_path", "_relation")

    def __init__(self, *path: str) -> None:
        if not path:
            raise ValueError("F() needs a field, e.g. F(ArtifactField.KEY) or F('key')")
        previous: str | None = None
        for part in path:
            if not isinstance(part, str) or not part:
                raise TypeError(f"Fields must be field enum members or non-empty strings, got {part!r}")
            if isinstance(previous, RegistryField):
                _check_relation_step(previous, part)
            previous = part
        self._path = ".".join(plain_value(part) for part in path)
        self._relation = previous if isinstance(previous, RegistryField) and previous.related else None

    @property
    def path(self) -> str:
        """The field path in the LaminHub REST format, e.g. ``created_by.handle``."""
        return self._path

    def __repr__(self) -> str:
        return f"F({self._path!r})"

    def __getitem__(self, key: str) -> F:
        """Select a key of a JSON field, e.g. ``F(RunField.PARAMS)["learning_rate"]``."""
        if not isinstance(key, str) or not key or '"' in key:
            raise ValueError(f"JSON keys must be non-empty strings without double quotes, got {key!r}")
        if self._relation is not None:
            raise ValueError(f"{_member_name(self._relation)} is a relation, not a JSON field")
        return F(f'{self._path}["{key}"]')

    def op(self, operator: FilterOperator | str, value: Any) -> Condition:
        """Compare with any :class:`FilterOperator`, e.g. ``F("key").op(FilterOperator.EQ, "a.csv")``."""
        return Condition(self, operator, value)

    def eq(self, value: Any) -> Condition:
        """Equal to ``value`` (``==``); ``None`` selects ``null`` values."""
        return self.is_null() if value is None else Condition(self, FilterOperator.EQ, value)

    def ne(self, value: Any) -> Condition:
        """Distinct from ``value``, including ``null`` values (``!=``); ``None`` selects non-null values."""
        return self.is_not_null() if value is None else Condition(self, FilterOperator.NE, value)

    def gt(self, value: str | float | datetime.date) -> Condition:
        """Greater than ``value`` (``>``)."""
        return Condition(self, FilterOperator.GT, value)

    def gte(self, value: str | float | datetime.date) -> Condition:
        """Greater than or equal to ``value`` (``>=``)."""
        return Condition(self, FilterOperator.GTE, value)

    def lt(self, value: str | float | datetime.date) -> Condition:
        """Less than ``value`` (``<``)."""
        return Condition(self, FilterOperator.LT, value)

    def lte(self, value: str | float | datetime.date) -> Condition:
        """Less than or equal to ``value`` (``<=``)."""
        return Condition(self, FilterOperator.LTE, value)

    def is_in(self, values: Iterable[Any]) -> Condition:
        """Equal to any of ``values``; an empty list matches nothing."""
        return Condition(self, FilterOperator.IN, _as_list(values))

    def not_in(self, values: Iterable[Any]) -> Condition:
        """Equal to none of ``values``."""
        return Condition(self, FilterOperator.NOT_IN, _as_list(values))

    def is_null(self) -> Condition:
        """The field is ``null``."""
        return Condition(self, FilterOperator.IS_NULL, True)

    def is_not_null(self) -> Condition:
        """The field is not ``null``."""
        return Condition(self, FilterOperator.IS_NULL, False)

    def startswith(self, prefix: str) -> Condition:
        """The string starts with ``prefix`` (case-sensitive)."""
        return Condition(self, FilterOperator.STARTSWITH, prefix)

    def endswith(self, suffix: str) -> Condition:
        """The string ends with ``suffix`` (case-sensitive)."""
        return Condition(self, FilterOperator.ENDSWITH, suffix)

    def contains(self, value: str | float | bool) -> Condition:
        """The string contains ``value`` (case-sensitive), or the JSON array has the element ``value``."""
        return Condition(self, FilterOperator.CONTAINS, value)

    def count(self) -> Count:
        """The number of related records, e.g. ``F(TransformField.RUNS).count() >= 2``."""
        return Count(self)

    def _count_path(self) -> str:
        if "[" in self._path:
            raise ValueError(f"Can't count JSON field {self._path!r}; count() applies to relations")
        if self._relation is not None or "." not in self._path:
            return f"{self._path}.id"
        return self._path

    def __eq__(self, value: object) -> Condition:  # type: ignore[override]
        return self.eq(value)

    def __ne__(self, value: object) -> Condition:  # type: ignore[override]
        return self.ne(value)

    def __gt__(self, value: Any) -> Condition:
        return self.gt(value)

    def __ge__(self, value: Any) -> Condition:
        return self.gte(value)

    def __lt__(self, value: Any) -> Condition:
        return self.lt(value)

    def __le__(self, value: Any) -> Condition:
        return self.lte(value)

    def __bool__(self) -> NoReturn:
        raise TypeError(_BOOL_HINT)

    def __and__(self, other: Any) -> NoReturn:
        raise TypeError(_PRECEDENCE_HINT)

    __rand__ = __or__ = __ror__ = __and__

    def __invert__(self) -> NoReturn:
        raise TypeError(_NEGATION_HINT)


def _as_list(values: Iterable[Any]) -> Any:
    if isinstance(values, (list, tuple, set, frozenset)):
        return values
    if isinstance(values, Iterable) and not isinstance(values, (str, bytes, Mapping)):
        return list(values)
    return values  # rejected by the condition, unless it is a template


class Count:
    """The number of related records of a relation, see :meth:`F.count`."""

    __slots__ = ("_field",)

    def __init__(self, field: F) -> None:
        field._count_path()  # fail early for fields that can't be counted
        self._field = field

    def __repr__(self) -> str:
        return f"{self._field!r}.count()"

    def eq(self, n: int) -> Condition:
        """Exactly ``n`` related records (``==``)."""
        return Condition(self._field, FilterOperator.COUNT_EQ, n)

    def ne(self, n: int) -> Condition:
        """Not exactly ``n`` related records (``!=``)."""
        return Condition(self._field, FilterOperator.COUNT_NE, n)

    def gt(self, n: int) -> Condition:
        """More than ``n`` related records (``>``)."""
        return Condition(self._field, FilterOperator.COUNT_GT, n)

    def gte(self, n: int) -> Condition:
        """At least ``n`` related records (``>=``)."""
        return Condition(self._field, FilterOperator.COUNT_GTE, n)

    def lt(self, n: int) -> Condition:
        """Fewer than ``n`` related records (``<``)."""
        return Condition(self._field, FilterOperator.COUNT_LT, n)

    def lte(self, n: int) -> Condition:
        """At most ``n`` related records (``<=``)."""
        return Condition(self._field, FilterOperator.COUNT_LTE, n)

    def __eq__(self, n: object) -> Condition:  # type: ignore[override]
        return Condition(self._field, FilterOperator.COUNT_EQ, n)

    def __ne__(self, n: object) -> Condition:  # type: ignore[override]
        return Condition(self._field, FilterOperator.COUNT_NE, n)

    def __gt__(self, n: int) -> Condition:
        return self.gt(n)

    def __ge__(self, n: int) -> Condition:
        return self.gte(n)

    def __lt__(self, n: int) -> Condition:
        return self.lt(n)

    def __le__(self, n: int) -> Condition:
        return self.lte(n)

    def __bool__(self) -> NoReturn:
        raise TypeError(_BOOL_HINT)


class FilterExpression:
    """
    Base class of filters built in Python: :class:`Condition`, :class:`And` and :class:`Or`.

    Combine filters with ``&`` (and) and ``|`` (or); the other operand may also be a LaminHub REST
    filter dict. :meth:`to_dict` returns the JSON form that the LaminHub REST API expects.
    """

    __slots__ = ()

    def to_dict(self) -> dict[str, Any]:
        """Return the filter in the JSON form of the LaminHub REST API."""
        raise NotImplementedError

    @staticmethod
    def from_dict(node: Mapping[str, Any]) -> FilterExpression:
        """Parse and validate a LaminHub REST filter, e.g. ``{"key": {"eq": "raw/a.csv"}}``."""
        if not isinstance(node, Mapping) or len(node) != 1:
            raise ValueError(
                f"Filters must have exactly one key, a field or 'and'/'or', got {node!r}; "
                "combine conditions with {'and': [...]}"
            )
        ((key, value),) = node.items()
        if plain_value(key) in ("and", "or"):
            group = plain_value(key)
            if not isinstance(value, (list, tuple)) or not value:
                raise ValueError(f"{group!r} expects a non-empty list of filters, got {value!r}")
            return (And if group == "and" else Or)(*value)
        if not isinstance(value, Mapping) or len(value) != 1:
            raise ValueError(
                f"The condition on {key!r} must have exactly one operator, e.g. {{'eq': ...}}, got {value!r}"
            )
        ((operator, operand),) = value.items()
        return Condition(key, operator, operand)

    def __and__(self, other: FilterLike) -> And:
        if not isinstance(other, (FilterExpression, Mapping)):
            return NotImplemented
        return And(*_flatten(And, self), *_flatten(And, other))

    def __rand__(self, other: FilterLike) -> And:
        if not isinstance(other, Mapping):
            return NotImplemented
        return And(other, *_flatten(And, self))

    def __or__(self, other: FilterLike) -> Or:
        if not isinstance(other, (FilterExpression, Mapping)):
            return NotImplemented
        return Or(*_flatten(Or, self), *_flatten(Or, other))

    def __ror__(self, other: FilterLike) -> Or:
        if not isinstance(other, Mapping):
            return NotImplemented
        return Or(other, *_flatten(Or, self))

    def __invert__(self) -> NoReturn:
        raise TypeError(_NEGATION_HINT)

    def __bool__(self) -> NoReturn:
        raise TypeError(_BOOL_HINT)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, FilterExpression):
            return self.to_dict() == other.to_dict()
        if isinstance(other, Mapping):
            return self.to_dict() == other
        return NotImplemented

    __hash__ = None  # type: ignore[assignment]

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.to_dict()!r})"


FilterLike = Mapping[str, Any] | FilterExpression
"""A filter built with :class:`F` or a LaminHub REST filter dict."""


def _flatten(group: type[_Group], filter: FilterLike) -> tuple[FilterLike, ...]:
    return filter.filters if isinstance(filter, group) else (filter,)


def _as_expression(filter: Any) -> FilterExpression:
    if isinstance(filter, FilterExpression):
        return filter
    if isinstance(filter, Mapping):
        if not filter:
            raise ValueError("Empty filters ({}) aren't allowed inside 'and' and 'or'")
        return FilterExpression.from_dict(filter)
    if isinstance(filter, (F, Count)):
        raise TypeError(f"{filter!r} is not a filter; compare it with a value, e.g. {filter!r} == value")
    raise TypeError(
        f"Expected a filter such as F('key') == 'a.csv' or {{'key': {{'eq': 'a.csv'}}}}, got {filter!r}"
    )


class Condition(FilterExpression):
    """
    A condition on a field, e.g. ``Condition(ArtifactField.KEY, FilterOperator.STARTSWITH, "raw/")``.

    Comparing an :class:`F` creates conditions too, e.g. ``F(ArtifactField.KEY).startswith("raw/")``.

    :param field: The field, as an :class:`F`, a field enum member or a field name.
    :param operator: A :class:`FilterOperator` or its name, e.g. ``"startswith"``.
    :param value: The value to compare with; enum members, dates, tuples and sets are converted to
        their JSON form.
    """

    __slots__ = ("operator", "path", "value")

    def __init__(self, field: F | str, operator: FilterOperator | str, value: Any) -> None:
        if not isinstance(field, F):
            field = F(field)
        try:
            operator = FilterOperator(operator)
        except ValueError:
            raise ValueError(
                f"Unknown filter operator {plain_value(operator)!r} on {field.path!r}; expected one of "
                f"{', '.join(op.value for op in FilterOperator)}"
            ) from None
        if operator.is_count:
            path = field._count_path()
        elif field._relation is not None:
            raise ValueError(_relation_hint(field._relation))
        else:
            path = field.path
        self.path = path
        self.operator = operator
        self.value = _check_value(operator, value, path)

    def to_dict(self) -> dict[str, Any]:
        return {self.path: {self.operator.value: self.value}}


class _Group(FilterExpression):
    __slots__ = ("filters",)
    _key = ""

    def __init__(self, *filters: FilterLike) -> None:
        if not filters:
            raise ValueError(f"{type(self).__name__}() needs at least one filter")
        self.filters: tuple[FilterExpression, ...] = tuple(_as_expression(f) for f in filters)

    def to_dict(self) -> dict[str, Any]:
        return {self._key: [f.to_dict() for f in self.filters]}


class And(_Group):
    """All filters match, e.g. ``And(F("suffix") == ".csv", F("size") > 0)``; like ``&``."""

    __slots__ = ()
    _key = "and"


class Or(_Group):
    """At least one filter matches, e.g. ``Or(F("suffix") == ".csv", F("suffix") == ".tsv")``; like ``|``."""

    __slots__ = ()
    _key = "or"


def normalize_filter(filter: FilterLike | None) -> dict[str, Any] | None:
    """
    Validate a filter and return its JSON form, or ``None`` for no filter.

    Accepts filters built with :class:`F` and LaminHub REST filter dicts, and raises ``ValueError`` or
    ``TypeError`` for invalid filters, such as unknown operators.
    """
    if filter is None or (isinstance(filter, Mapping) and not filter):
        return None
    return _as_expression(filter).to_dict()


def _json_form(filter: FilterLike | None) -> dict[str, Any] | None:
    if isinstance(filter, FilterExpression):
        return filter.to_dict()
    return dict(filter) if filter else None


def combine_filters(*filters: FilterLike | None) -> dict[str, Any] | None:
    """
    Combine LaminHub REST filter nodes with a logical ``and``.

    Empty or ``None`` filters are skipped. Returns ``None`` if nothing is left.
    """
    parts = [part for part in map(_json_form, filters) if part]
    if not parts:
        return None
    if len(parts) == 1:
        return parts[0]
    return {"and": parts}


def normalize_order_by(order_by: Sequence[str | Mapping[str, Any]] | None) -> list[dict[str, Any]]:
    """
    Normalize ``order_by`` to the LaminHub REST format.

    Accepts ``{"field": "created_at", "descending": True}`` dicts or strings such as
    ``"created_at"`` / ``"-created_at"`` (a leading ``-`` means descending). Defaults to ``id``.
    """
    if not order_by:
        return [{"field": "id", "descending": False}]
    normalized: list[dict[str, Any]] = []
    for item in order_by:
        if isinstance(item, str):
            item = plain_value(item)
            descending = item.startswith("-")
            normalized.append({"field": item.lstrip("-"), "descending": descending})
        else:
            normalized.append(
                {"field": plain_value(item["field"]), "descending": bool(item.get("descending", False))}
            )
    return normalized


def filter_field_paths(node: FilterLike | None) -> set[str]:
    """Return all field paths referenced by a filter node."""
    node = _json_form(node)
    if not node:
        return set()
    paths: set[str] = set()
    for key, value in node.items():
        if key in ("and", "or"):
            for child in value:
                paths |= filter_field_paths(child)
        else:
            paths.add(key)
    return paths


def is_local_filter(node: FilterLike | None) -> bool:
    """Whether a filter only references direct (non-relation, non-JSON) fields of a record."""
    return all("." not in path and "[" not in path for path in filter_field_paths(node))


_MISSING = object()


def matches_filter(node: FilterLike | None, row: Mapping[str, Any]) -> bool:
    """
    Evaluate a LaminHub REST filter against a plain record dict.

    Only direct fields are supported (see :func:`is_local_filter`). Foreign keys can be
    referenced either by relation name (``run``) or by column (``run_id``). This is used for
    records that no longer exist on LaminHub (hard deletes), where the filter can't be
    evaluated server-side.
    """
    node = _json_form(node)
    if not node:
        return True
    if len(node) != 1:
        raise ValueError(f"Filter nodes must have exactly one key, got {sorted(node)}")
    ((key, value),) = node.items()
    if key == "and":
        return all(matches_filter(child, row) for child in value)
    if key == "or":
        return any(matches_filter(child, row) for child in value)
    if "." in key or "[" in key:
        raise ValueError(f"Filter path {key!r} can't be evaluated locally; only direct fields are supported")
    field_value = row.get(key, _MISSING)
    if field_value is _MISSING:
        field_value = row.get(f"{key}_id", _MISSING)
    if field_value is _MISSING:
        raise ValueError(f"Field {key!r} is not present in the record")
    if not isinstance(value, Mapping) or len(value) != 1:
        raise ValueError(f"Field condition for {key!r} must have exactly one operator")
    ((operator, operand),) = value.items()
    return _apply_operator(operator, field_value, operand)


def _apply_operator(operator: str, value: Any, operand: Any) -> bool:
    if operator == "eq":
        return bool(value == operand)
    if operator == "ne":
        return bool(value != operand)
    if operator == "in":
        return value in _operand_list(operand)
    if operator == "notin":
        return value not in _operand_list(operand)
    if operator == "isnull":
        return (value is None) == bool(operand)
    if value is None:
        return False
    if operator == "gt":
        return bool(value > operand)
    if operator == "gte":
        return bool(value >= operand)
    if operator == "lt":
        return bool(value < operand)
    if operator == "lte":
        return bool(value <= operand)
    if operator == "startswith":
        return isinstance(value, str) and value.startswith(operand)
    if operator == "endswith":
        return isinstance(value, str) and value.endswith(operand)
    if operator == "contains":
        if isinstance(value, str):
            return isinstance(operand, str) and operand in value
        if isinstance(value, list):
            return operand in value
        return False
    raise ValueError(f"Filter operator {operator!r} can't be evaluated locally")


def _operand_list(operand: Any) -> list[Any]:
    if isinstance(operand, Iterable) and not isinstance(operand, (str, bytes, Mapping)):
        return list(operand)
    raise ValueError("'in' / 'notin' operators require a list")
