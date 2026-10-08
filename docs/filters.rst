Filters
=======

Sensors, triggers and the hook select records with filters, for example "artifacts whose key starts
with ``raw/``". Build filters in Python with
:class:`~airflow.providers.lamindb.utils.filters.F` and the enums in
:mod:`airflow.providers.lamindb.utils.filters`, so that your IDE suggests the fields and operators and
mistakes show up when the DAG is parsed:

.. code-block:: python

    from airflow.providers.lamindb.utils.filters import ArtifactField, ArtifactKind, F

    large_raw_datasets = (
        F(ArtifactField.KEY).startswith("raw/")
        & (F(ArtifactField.KIND) == ArtifactKind.DATASET)
        & (F(ArtifactField.SIZE) > 1_000_000)
    )

Pass the filter as ``filter`` to
:class:`~airflow.providers.lamindb.sensors.records.LaminDBRecordSensor`,
:class:`~airflow.providers.lamindb.sensors.records.LaminDBArtifactSensor`,
:class:`~airflow.providers.lamindb.triggers.records.LaminDBRecordEventTrigger`,
:class:`~airflow.providers.lamindb.triggers.records.LaminDBArtifactEventTrigger` and to the hook's
``query_records``. They convert it to the JSON form of the `LaminHub REST API
<https://docs.lamin.ai/rest>`__. You can also pass that JSON form directly:

.. code-block:: python

    {
        "and": [
            {"key": {"startswith": "raw/"}},
            {"kind": {"eq": "dataset"}},
            {"size": {"gt": 1000000}},
        ]
    }

Fields
------

``F`` takes a member of a field enum, such as ``ArtifactField.KEY``, or a field name, such as ``"key"``.
There is a field enum for each core registry: ``ArtifactField``, ``CollectionField``, ``RunField``,
``TransformField``, ``RecordField``, ``ULabelField``, ``FeatureField``, ``SchemaField``,
``ProjectField``, ``ReferenceField``, ``StorageField``, ``BranchField``, ``SpaceField`` and
``UserField``. The enums list the fields of LaminDB 2.10; use strings for other fields, for example of
``bionty`` registries: ``F("ontology_id")``.

``LaminDBRegistry`` lists the registries for the ``registry`` argument, for example
``LaminDBRegistry.RUN`` (``"core.run"``) or ``LaminDBRegistry.BIONTY_CELL_TYPE``
(``"bionty.celltype"``).

Relations
    Pass several fields to ``F`` to follow relations. ``F(ArtifactField.CREATED_BY, UserField.HANDLE)``
    is the handle of the user who created an artifact (``created_by.handle``), and
    ``F(RunField.TRANSFORM, TransformField.KEY)`` the key of a run's transform. ``F`` checks that the
    fields fit together. A relation can't be compared by itself; compare a field of the related
    records or the foreign key, e.g. ``F(ArtifactField.RUN_ID) == 42``.

Counting related records
    ``F(TransformField.RUNS).count() >= 2`` selects transforms with at least two runs.

JSON fields
    Index JSON fields with keys: ``F(RunField.PARAMS)["learning_rate"] > 0.01``.

Operators
---------

Compare fields with Python's comparison operators or with the methods of ``F``. Values can be enum
members (``ArtifactKind.DATASET``, ``RunStatus.COMPLETED``), dates and datetimes, which are converted
to their JSON form.

.. list-table::
    :header-rows: 1

    * - ``F`` method
      - Python operator
      - REST operator
    * - ``eq(value)`` / ``ne(value)``
      - ``==`` / ``!=``
      - ``eq`` / ``ne``
    * - ``gt(value)`` / ``gte(value)``
      - ``>`` / ``>=``
      - ``gt`` / ``gte``
    * - ``lt(value)`` / ``lte(value)``
      - ``<`` / ``<=``
      - ``lt`` / ``lte``
    * - ``is_in(values)`` / ``not_in(values)``
      -
      - ``in`` / ``notin``
    * - ``is_null()`` / ``is_not_null()``
      - ``== None`` / ``!= None``
      - ``isnull``
    * - ``startswith(prefix)`` / ``endswith(suffix)``
      -
      - ``startswith`` / ``endswith``
    * - ``contains(value)``
      -
      - ``contains``
    * - ``count()``, compared with a number
      - e.g. ``F(...).count() >= 2``
      - ``count_eq``, ``count_gte``, ...
    * - ``op(FilterOperator.EQ, value)``
      -
      - any operator

``FilterOperator`` lists all operators. ``Condition(field, operator, value)`` builds a condition
without ``F``, e.g. ``Condition(ArtifactField.KEY, FilterOperator.STARTSWITH, "raw/")``.

Combining filters
-----------------

Combine filters with ``&`` (and) and ``|`` (or), or with ``And(...)`` and ``Or(...)``. Python evaluates
``&`` and ``|`` before comparisons, so wrap comparisons in parentheses:

.. code-block:: python

    from airflow.providers.lamindb.utils.filters import And, ArtifactField, F, Or

    csv_or_parquet = (F(ArtifactField.SUFFIX) == ".csv") | (F(ArtifactField.SUFFIX) == ".parquet")
    same = Or(F(ArtifactField.SUFFIX) == ".csv", F(ArtifactField.SUFFIX) == ".parquet")
    also_same = F(ArtifactField.SUFFIX).is_in([".csv", ".parquet"])

    recent_csv = And(F(ArtifactField.SUFFIX) == ".csv", F(ArtifactField.CREATED_AT) >= "2026-01-01")

LaminHub filters can't be negated; use ``!=``, ``not_in()`` or ``is_not_null()`` instead. Python's
``and``, ``or``, ``not`` and chained comparisons such as ``1 < F(ArtifactField.SIZE) < 10`` raise a
``TypeError`` that explains the alternative.

Templates and validation
------------------------

``filter`` is a template field of the sensors, so values can be Jinja templates:
``F(ArtifactField.KEY) == "{{ params.key }}"``.

Filters are validated when they are passed to a sensor, trigger or the hook, so that for example an
unknown operator such as ``{"key": {"equals": "a"}}`` fails when the DAG is parsed and not only when
the query runs.
