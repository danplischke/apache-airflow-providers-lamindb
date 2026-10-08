from __future__ import annotations

import datetime
import json
import pickle

import pytest

from airflow.providers.lamindb.utils.filters import (
    And,
    ArtifactField,
    ArtifactKind,
    Condition,
    F,
    FilterExpression,
    FilterOperator,
    Or,
    RunField,
    RunStatus,
    TransformField,
    UserField,
    combine_filters,
    filter_field_paths,
    is_local_filter,
    matches_filter,
    normalize_filter,
    normalize_order_by,
)


def test_combine_filters():
    assert combine_filters() is None
    assert combine_filters(None, {}) is None
    assert combine_filters({"a": {"eq": 1}}, None) == {"a": {"eq": 1}}
    assert combine_filters({"a": {"eq": 1}}, {"b": {"eq": 2}}) == {
        "and": [{"a": {"eq": 1}}, {"b": {"eq": 2}}]
    }
    assert combine_filters(F("a") == 1, None, {"b": {"eq": 2}}) == {
        "and": [{"a": {"eq": 1}}, {"b": {"eq": 2}}]
    }


def test_normalize_order_by():
    assert normalize_order_by(None) == [{"field": "id", "descending": False}]
    assert normalize_order_by(["-created_at", "key", {"field": "size", "descending": True}]) == [
        {"field": "created_at", "descending": True},
        {"field": "key", "descending": False},
        {"field": "size", "descending": True},
    ]
    assert normalize_order_by([ArtifactField.CREATED_AT, {"field": ArtifactField.SIZE}]) == [
        {"field": "created_at", "descending": False},
        {"field": "size", "descending": False},
    ]


def test_field_paths_and_locality():
    node = {"and": [{"key": {"eq": "a"}}, {"or": [{"created_by.handle": {"eq": "x"}}, {"size": {"gt": 1}}]}]}
    assert filter_field_paths(node) == {"key", "created_by.handle", "size"}
    assert not is_local_filter(node)
    assert not is_local_filter({'_aux["so"]': {"eq": 1}})
    assert is_local_filter({"or": [{"key": {"isnull": True}}, {"kind": {"notin": ["x"]}}]})
    assert is_local_filter(None)
    assert is_local_filter(F(ArtifactField.RUN_ID) == 1)
    assert not is_local_filter(F(ArtifactField.CREATED_BY, UserField.HANDLE) == "x")


ROW = {"id": 1, "key": "raw/a.csv", "suffix": ".csv", "size": 10, "kind": None, "run_id": 5, "tags": ["x"]}


@pytest.mark.parametrize(
    ("node", "expected"),
    [
        (None, True),
        ({"key": {"eq": "raw/a.csv"}}, True),
        ({"key": {"ne": "raw/a.csv"}}, False),
        ({"suffix": {"in": [".csv", ".tsv"]}}, True),
        ({"suffix": {"notin": [".csv"]}}, False),
        ({"kind": {"isnull": True}}, True),
        ({"kind": {"isnull": False}}, False),
        ({"size": {"gt": 5}}, True),
        ({"size": {"gte": 10}}, True),
        ({"size": {"lt": 10}}, False),
        ({"size": {"lte": 10}}, True),
        ({"kind": {"gt": 1}}, False),
        ({"key": {"startswith": "raw/"}}, True),
        ({"key": {"endswith": ".csv"}}, True),
        ({"key": {"contains": "a.c"}}, True),
        ({"tags": {"contains": "x"}}, True),
        ({"size": {"contains": 1}}, False),
        ({"run": {"eq": 5}}, True),
        ({"and": [{"size": {"gt": 5}}, {"suffix": {"eq": ".csv"}}]}, True),
        ({"or": [{"size": {"gt": 50}}, {"suffix": {"eq": ".tsv"}}]}, False),
    ],
)
def test_matches_filter(node, expected):
    assert matches_filter(node, ROW) is expected
    if node is not None:
        assert matches_filter(FilterExpression.from_dict(node), ROW) is expected


@pytest.mark.parametrize(
    ("node", "message"),
    [
        ({"created_by.handle": {"eq": "x"}}, "can't be evaluated locally"),
        ({"missing": {"eq": 1}}, "not present"),
        ({"key": {"eq": 1, "ne": 2}}, "exactly one operator"),
        ({"key": {"eq": 1}, "size": {"eq": 1}}, "exactly one key"),
        ({"size": {"count_gt": 1}}, "can't be evaluated locally"),
        ({"size": {"in": "abc"}}, "require a list"),
    ],
)
def test_matches_filter_errors(node, message):
    with pytest.raises(ValueError, match=message):
        matches_filter(node, ROW)


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        (F(ArtifactField.KEY) == "raw/a.csv", {"key": {"eq": "raw/a.csv"}}),
        (F("key") != "raw/a.csv", {"key": {"ne": "raw/a.csv"}}),
        (F(ArtifactField.SIZE) > 10, {"size": {"gt": 10}}),
        (F(ArtifactField.SIZE) >= 10, {"size": {"gte": 10}}),
        (F(ArtifactField.SIZE) < 10.5, {"size": {"lt": 10.5}}),
        (F(ArtifactField.SIZE) <= 10, {"size": {"lte": 10}}),
        (10 < F(ArtifactField.SIZE), {"size": {"gt": 10}}),  # noqa: SIM300
        (F(ArtifactField.KIND) == None, {"kind": {"isnull": True}}),  # noqa: E711
        (F(ArtifactField.KIND) != None, {"kind": {"isnull": False}}),  # noqa: E711
        (F(ArtifactField.SIZE).eq(1), {"size": {"eq": 1}}),
        (F(ArtifactField.SIZE).ne(1), {"size": {"ne": 1}}),
        (F(ArtifactField.SIZE).gt(1), {"size": {"gt": 1}}),
        (F(ArtifactField.SIZE).gte(1), {"size": {"gte": 1}}),
        (F(ArtifactField.SIZE).lt(1), {"size": {"lt": 1}}),
        (F(ArtifactField.SIZE).lte(1), {"size": {"lte": 1}}),
        (F(ArtifactField.SUFFIX).is_in([".csv", ".tsv"]), {"suffix": {"in": [".csv", ".tsv"]}}),
        (F(ArtifactField.SUFFIX).is_in((".csv",)), {"suffix": {"in": [".csv"]}}),
        (F(ArtifactField.SUFFIX).is_in({".tsv", ".csv"}), {"suffix": {"in": [".csv", ".tsv"]}}),
        (F(ArtifactField.SUFFIX).is_in(s for s in [".csv"]), {"suffix": {"in": [".csv"]}}),
        (F(ArtifactField.SUFFIX).is_in([]), {"suffix": {"in": []}}),
        (F(ArtifactField.SUFFIX).not_in([".csv"]), {"suffix": {"notin": [".csv"]}}),
        (F(ArtifactField.KIND).is_null(), {"kind": {"isnull": True}}),
        (F(ArtifactField.KIND).is_not_null(), {"kind": {"isnull": False}}),
        (F(ArtifactField.KEY).startswith("raw/"), {"key": {"startswith": "raw/"}}),
        (F(ArtifactField.KEY).endswith(".csv"), {"key": {"endswith": ".csv"}}),
        (F(ArtifactField.KEY).contains("sample"), {"key": {"contains": "sample"}}),
        (F(ArtifactField.KEY).op(FilterOperator.ENDSWITH, ".csv"), {"key": {"endswith": ".csv"}}),
        (F(ArtifactField.KEY).op("endswith", ".csv"), {"key": {"endswith": ".csv"}}),
        (Condition(ArtifactField.KEY, FilterOperator.EQ, "a"), {"key": {"eq": "a"}}),
        (Condition("key", "notin", ("a", "b")), {"key": {"notin": ["a", "b"]}}),
        # enum members, dates and templates as values
        (F(ArtifactField.KIND) == ArtifactKind.DATASET, {"kind": {"eq": "dataset"}}),
        (F(ArtifactField.KIND).is_in([ArtifactKind.MODEL]), {"kind": {"in": ["model"]}}),
        (F(RunField.STATUS_CODE) == RunStatus.COMPLETED, {"_status_code": {"eq": 0}}),
        (
            F(ArtifactField.CREATED_AT) >= datetime.datetime(2025, 1, 2, 3, 4, tzinfo=datetime.timezone.utc),
            {"created_at": {"gte": "2025-01-02T03:04:00+00:00"}},
        ),
        (F(ArtifactField.CREATED_AT) < datetime.date(2025, 1, 2), {"created_at": {"lt": "2025-01-02"}}),
        (F(ArtifactField.KEY) == "{{ params.key }}", {"key": {"eq": "{{ params.key }}"}}),
        (F(ArtifactField.SUFFIX).is_in("{{ params.suffixes }}"), {"suffix": {"in": "{{ params.suffixes }}"}}),
        # relations, JSON fields and counts
        (F(ArtifactField.CREATED_BY, UserField.HANDLE) == "alice", {"created_by.handle": {"eq": "alice"}}),
        (F("created_by.handle") == "alice", {"created_by.handle": {"eq": "alice"}}),
        (
            F(ArtifactField.RUN, RunField.TRANSFORM, TransformField.KEY) == "a.py",
            {"run.transform.key": {"eq": "a.py"}},
        ),
        (F(ArtifactField.CREATED_BY, "handle") == "alice", {"created_by.handle": {"eq": "alice"}}),
        (F(RunField.PARAMS)["learning_rate"] > 0.1, {'params["learning_rate"]': {"gt": 0.1}}),
        (F(RunField.PARAMS)["a"]["b"].is_null(), {'params["a"]["b"]': {"isnull": True}}),
        (F(TransformField.RUNS).count() >= 2, {"runs.id": {"count_gte": 2}}),
        (F(TransformField.RUNS).count() == 0, {"runs.id": {"count_eq": 0}}),
        (F(TransformField.RUNS).count() != 0, {"runs.id": {"count_ne": 0}}),
        (F(TransformField.RUNS).count() > 0, {"runs.id": {"count_gt": 0}}),
        (F(TransformField.RUNS).count() < 3, {"runs.id": {"count_lt": 3}}),
        (F(TransformField.RUNS).count() <= 3, {"runs.id": {"count_lte": 3}}),
        (F(TransformField.RUNS).count().gte(2), {"runs.id": {"count_gte": 2}}),
        (F("runs").count().eq(1), {"runs.id": {"count_eq": 1}}),
        (F("runs.uid").count().lt(1), {"runs.uid": {"count_lt": 1}}),
        (Condition(TransformField.RUNS, "count_gt", 1), {"runs.id": {"count_gt": 1}}),
    ],
)
def test_builder(expression, expected):
    assert isinstance(expression, Condition)
    assert expression.to_dict() == expected
    assert expression == expected
    assert normalize_filter(expression) == expected
    assert normalize_filter(expected) == expected


def test_builder_paths_are_plain_strings():
    node = normalize_filter(F(ArtifactField.KIND) == ArtifactKind.DATASET)
    ((path, condition),) = node.items()
    ((operator, value),) = condition.items()
    assert (type(path), type(operator), type(value)) == (str, str, str)
    assert json.dumps(node) == '{"kind": {"eq": "dataset"}}'


def test_combining():
    csv = F(ArtifactField.SUFFIX) == ".csv"
    raw = F(ArtifactField.KEY).startswith("raw/")
    large = F(ArtifactField.SIZE) > 100
    assert (csv & raw & large).to_dict() == {
        "and": [{"suffix": {"eq": ".csv"}}, {"key": {"startswith": "raw/"}}, {"size": {"gt": 100}}]
    }
    assert (csv | raw | large).to_dict() == {
        "or": [{"suffix": {"eq": ".csv"}}, {"key": {"startswith": "raw/"}}, {"size": {"gt": 100}}]
    }
    assert ((csv | raw) & large).to_dict() == {
        "and": [{"or": [{"suffix": {"eq": ".csv"}}, {"key": {"startswith": "raw/"}}]}, {"size": {"gt": 100}}]
    }
    assert (csv & (raw | large)) == And(csv, Or(raw, large))
    assert (csv & {"size": {"gt": 1}}).to_dict() == {"and": [{"suffix": {"eq": ".csv"}}, {"size": {"gt": 1}}]}
    assert ({"size": {"gt": 1}} & csv).to_dict() == {"and": [{"size": {"gt": 1}}, {"suffix": {"eq": ".csv"}}]}
    assert ({"size": {"gt": 1}} | csv).to_dict() == {"or": [{"size": {"gt": 1}}, {"suffix": {"eq": ".csv"}}]}
    assert And(csv).to_dict() == {"and": [{"suffix": {"eq": ".csv"}}]}
    assert And(And(csv, raw), large).to_dict() == {
        "and": [{"and": [csv.to_dict(), raw.to_dict()]}, large.to_dict()]
    }
    assert repr(csv) == "Condition({'suffix': {'eq': '.csv'}})"
    assert repr(F(TransformField.RUNS).count()) == "F('runs').count()"


def test_expressions_can_be_pickled():
    expression = (F(ArtifactField.KIND) == ArtifactKind.DATASET) & (F(TransformField.RUNS).count() > 1)
    assert pickle.loads(pickle.dumps(expression)) == expression
    field = pickle.loads(pickle.dumps(F(ArtifactField.CREATED_BY)))
    with pytest.raises(ValueError, match="is a relation"):
        field == 1  # noqa: B015


@pytest.mark.parametrize(
    ("node", "expected"),
    [
        (None, None),
        ({}, None),
        ({"id": {"in": (1, 2)}}, {"id": {"in": [1, 2]}}),
        ({ArtifactField.KEY: {FilterOperator.EQ: ArtifactKind.MODEL}}, {"key": {"eq": "model"}}),
        ({"runs": {"count_gte": 2}}, {"runs.id": {"count_gte": 2}}),
        (
            {"and": [{"a": {"eq": 1}}, {"or": [{"b": {"isnull": True}}, {"c": {"in": []}}]}]},
            {"and": [{"a": {"eq": 1}}, {"or": [{"b": {"isnull": True}}, {"c": {"in": []}}]}]},
        ),
        ({"and": [{"and": [{"a": {"eq": 1}}]}]}, {"and": [{"and": [{"a": {"eq": 1}}]}]}),
        ({"params": {"eq": {"a": [1, None]}}}, {"params": {"eq": {"a": [1, None]}}}),
        ({"size": {"gt": "{{ params.size }}"}}, {"size": {"gt": "{{ params.size }}"}}),
    ],
)
def test_normalize_filter(node, expected):
    assert normalize_filter(node) == expected


@pytest.mark.parametrize(
    ("node", "error", "message"),
    [
        (
            {"key": {"equals": "a"}},
            ValueError,
            "Unknown filter operator 'equals' on 'key'; expected one of eq",
        ),
        ({"key": {"eq": "a"}, "size": {"gt": 1}}, ValueError, "exactly one key"),
        ({"key": {"eq": "a", "ne": "b"}}, ValueError, "exactly one operator"),
        ({"key": "a"}, ValueError, "exactly one operator"),
        ({"and": []}, ValueError, "non-empty list"),
        ({"or": {"a": {"eq": 1}}}, ValueError, "non-empty list"),
        ({"and": [{}]}, ValueError, "Empty filters"),
        ({"and": ["a"]}, TypeError, "Expected a filter"),
        ({"": {"eq": 1}}, TypeError, "non-empty strings"),
        ({1: {"eq": 1}}, TypeError, "non-empty strings"),
        ({"suffix": {"in": ".csv"}}, TypeError, "'in' on 'suffix' expects a list of values"),
        ({"suffix": {"notin": {"a": 1}}}, TypeError, "'notin' on 'suffix' expects a list of values"),
        ({"kind": {"isnull": "yes"}}, TypeError, "expects True or False"),
        ({"key": {"startswith": 1}}, TypeError, "expects a string"),
        ({"size": {"gt": None}}, TypeError, "expects a string or a number"),
        ({"size": {"gt": True}}, TypeError, "expects a string or a number"),
        ({"size": {"gt": [1]}}, TypeError, "expects a string or a number"),
        ({"tags": {"contains": None}}, TypeError, "expects a string, number or boolean"),
        ({"runs.id": {"count_gt": 1.5}}, TypeError, "expects an integer"),
        ({"runs.id": {"count_gt": True}}, TypeError, "expects an integer"),
        ({"key": {"eq": object()}}, TypeError, "expects a JSON value"),
        ({ArtifactField.CREATED_BY: {"eq": 1}}, ValueError, "is a relation to core.user"),
        ({'params["a"]': {"count_gt": 1}}, ValueError, "Can't count JSON field"),
    ],
)
def test_normalize_filter_errors(node, error, message):
    with pytest.raises(error, match=message):
        normalize_filter(node)


def test_normalize_filter_rejects_non_filters():
    with pytest.raises(TypeError, match=r"F\('key'\) is not a filter; compare it with a value"):
        normalize_filter(F("key"))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="Expected a filter"):
        normalize_filter("key == 'a'")  # type: ignore[arg-type]


def test_relations_are_checked():
    with pytest.raises(
        ValueError,
        match=r"ArtifactField.KEY is not a relation, so F\(\) can't follow it to UserField.HANDLE",
    ):
        F(ArtifactField.KEY, UserField.HANDLE)
    with pytest.raises(ValueError, match=r"ArtifactField\.KEY is not a relation"):
        F(ArtifactField.KEY, "handle")
    with pytest.raises(
        ValueError, match=r"points to core\.user: follow it with UserField members, not RunField\.NAME"
    ):
        F(ArtifactField.CREATED_BY, RunField.NAME)
    with pytest.raises(
        ValueError,
        match=r"ArtifactField.CREATED_BY is a relation to core.user; compare a field of the related records "
        r"instead, e.g. F\(ArtifactField.CREATED_BY, UserField.ID\) or F\(ArtifactField.CREATED_BY_ID\), "
        r"or count them with .count\(\)",
    ):
        F(ArtifactField.CREATED_BY) == 1  # noqa: B015
    with pytest.raises(ValueError, match=r"e.g. F\(ArtifactField.ULABELS, ULabelField.ID\), or count"):
        F(ArtifactField.ULABELS).is_in([1])
    with pytest.raises(ValueError, match="is a relation, not a JSON field"):
        F(ArtifactField.RUN)["a"]
    # plain strings aren't checked
    assert (F("created_by") == 1).to_dict() == {"created_by": {"eq": 1}}


@pytest.mark.parametrize(
    ("build", "error", "message"),
    [
        (lambda: F(), ValueError, "F\\(\\) needs a field"),
        (lambda: F(1), TypeError, "non-empty strings"),
        (lambda: F("a", ""), TypeError, "non-empty strings"),
        (lambda: F("params")['a"b'], ValueError, "without double quotes"),
        (lambda: F("params")[1], ValueError, "JSON keys must be non-empty strings"),
        (lambda: F("a").op("equals", 1), ValueError, "Unknown filter operator 'equals'"),
        (lambda: F("a") == F("b"), TypeError, "filters compare fields with values"),
        (lambda: F("a") == (F("b") == 1), TypeError, "filters compare fields with values"),
        (lambda: F("a").is_in("abc"), TypeError, "expects a list of values"),
        (lambda: F("a").is_in(None), TypeError, "expects a list of values"),
        (lambda: F("a") > None, TypeError, "expects a string or a number"),
        (lambda: F("runs").count() >= "2", TypeError, "expects an integer"),
        (lambda: F("params")["a"].count(), ValueError, "Can't count JSON field"),
        (lambda: F("params")["a"].count() > 1, ValueError, "Can't count JSON field"),
        (lambda: And(), ValueError, "needs at least one filter"),
        (lambda: Or(F("a")), TypeError, "is not a filter"),
    ],
)
def test_builder_errors(build, error, message):
    with pytest.raises(error, match=message):
        build()


@pytest.mark.parametrize(
    "misuse",
    [
        lambda: 1 < F("size") < 5,
        lambda: (F("a") == 1) and (F("b") == 2),
        lambda: bool(F("a") == 1),
        lambda: bool(F("a")),
        lambda: bool(F("runs").count()),
    ],
)
def test_filters_are_not_booleans(misuse):
    with pytest.raises(TypeError, match="combine them with & and \\| instead of 'and' and 'or'"):
        misuse()


@pytest.mark.parametrize(
    "misuse",
    [
        lambda: F("a") == 1 & F("b") == 2,
        lambda: F("a") & F("b"),
        lambda: F("a") | (F("b") == 1),
        lambda: (F("a") == 1) | F("b"),
    ],
)
def test_fields_need_parentheses(misuse):
    with pytest.raises(TypeError, match="wrap comparisons in parentheses"):
        misuse()


def test_filters_cant_be_negated():
    with pytest.raises(TypeError, match="can't be negated"):
        ~(F("a") == 1)
    with pytest.raises(TypeError, match="can't be negated"):
        ~F("a")


def test_combining_with_other_types_fails():
    with pytest.raises(TypeError):
        (F("a") == 1) & 1  # type: ignore[operator]
    with pytest.raises(TypeError):
        "a" | (F("a") == 1)  # type: ignore[operator]
    assert (F("a") == 1) != "a"
