"""
Enums for building LaminHub REST filters: registries, filter operators, registry fields and values.

The field enums reflect the core registries of LaminDB 2.10. Use them with
:class:`~airflow.providers.lamindb.utils.filters.F`, for example ``F(ArtifactField.KEY) == "raw/a.csv"``.
Plain strings work for every field, including fields of other schema modules such as ``bionty``.
"""

from __future__ import annotations

from enum import Enum, IntEnum
from typing import Any


def plain_value(value: Any) -> Any:
    """Return the value of enum members, e.g. ``"core.run"`` for ``LaminDBRegistry.RUN``."""
    return value.value if isinstance(value, Enum) else value


class _StrEnum(str, Enum):
    """A ``str`` enum that formats as its value (like :class:`enum.StrEnum`, which needs Python 3.11)."""

    def __str__(self) -> str:
        return str(self.value)


class LaminDBRegistry(_StrEnum):
    """Registries (``module.model``) to pass as ``registry`` to sensors, triggers and the hook."""

    ARTIFACT = "core.artifact"
    COLLECTION = "core.collection"
    RUN = "core.run"
    TRANSFORM = "core.transform"
    RECORD = "core.record"
    ULABEL = "core.ulabel"
    FEATURE = "core.feature"
    SCHEMA = "core.schema"
    PROJECT = "core.project"
    REFERENCE = "core.reference"
    STORAGE = "core.storage"
    BRANCH = "core.branch"
    SPACE = "core.space"
    USER = "core.user"
    # registries of the bionty schema module
    BIONTY_CELL_LINE = "bionty.cellline"
    BIONTY_CELL_MARKER = "bionty.cellmarker"
    BIONTY_CELL_TYPE = "bionty.celltype"
    BIONTY_DEVELOPMENTAL_STAGE = "bionty.developmentalstage"
    BIONTY_DISEASE = "bionty.disease"
    BIONTY_ETHNICITY = "bionty.ethnicity"
    BIONTY_EXPERIMENTAL_FACTOR = "bionty.experimentalfactor"
    BIONTY_GENE = "bionty.gene"
    BIONTY_ORGANISM = "bionty.organism"
    BIONTY_PATHWAY = "bionty.pathway"
    BIONTY_PHENOTYPE = "bionty.phenotype"
    BIONTY_PROTEIN = "bionty.protein"
    BIONTY_SOURCE = "bionty.source"
    BIONTY_TISSUE = "bionty.tissue"


class FilterOperator(_StrEnum):
    """Operators of LaminHub REST filters, see https://docs.lamin.ai/rest."""

    EQ = "eq"
    """Equal to the value."""
    NE = "ne"
    """Distinct from the value, including ``null`` on SQL columns."""
    GT = "gt"
    """Greater than the value (a string or number)."""
    GTE = "gte"
    """Greater than or equal to the value (a string or number)."""
    LT = "lt"
    """Less than the value (a string or number)."""
    LTE = "lte"
    """Less than or equal to the value (a string or number)."""
    IN = "in"
    """Equal to any of the listed values; an empty list matches nothing."""
    NOT_IN = "notin"
    """Equal to none of the listed values."""
    IS_NULL = "isnull"
    """``True`` selects ``null`` values, ``False`` selects non-null values."""
    STARTSWITH = "startswith"
    """Case-sensitive prefix."""
    ENDSWITH = "endswith"
    """Case-sensitive suffix."""
    CONTAINS = "contains"
    """Case-sensitive substring of strings, or element of JSON arrays."""
    COUNT_EQ = "count_eq"
    """Number of related records equals the value."""
    COUNT_NE = "count_ne"
    """Number of related records differs from the value."""
    COUNT_GT = "count_gt"
    """Number of related records is greater than the value."""
    COUNT_GTE = "count_gte"
    """Number of related records is at least the value."""
    COUNT_LT = "count_lt"
    """Number of related records is less than the value."""
    COUNT_LTE = "count_lte"
    """Number of related records is at most the value."""

    @property
    def is_count(self) -> bool:
        """Whether the operator compares the number of related records."""
        return self.value.startswith("count_")


class ArtifactKind(_StrEnum):
    """Values of :attr:`ArtifactField.KIND`."""

    DATASET = "dataset"
    MODEL = "model"
    PLAN = "plan"
    LAMINDB_RUN = "__lamindb_run__"
    """Created by LaminDB for runs (logs, reports, environments)."""
    LAMINDB_CONFIG = "__lamindb_config__"
    """Created by LaminDB for configurations."""


class TransformKind(_StrEnum):
    """Values of :attr:`TransformField.KIND`."""

    PIPELINE = "pipeline"
    NOTEBOOK = "notebook"
    SCRIPT = "script"
    FUNCTION = "function"


class RunStatus(IntEnum):
    """Run statuses, as the codes stored in :attr:`RunField.STATUS_CODE`."""

    SCHEDULED = -3
    RESTARTED = -2
    STARTED = -1
    COMPLETED = 0
    ERRORED = 1
    ABORTED = 2


class RegistryField(_StrEnum):
    """
    Base class of the registry field enums.

    Members that are relations know the registry they point to (:attr:`related`), so that
    :class:`~airflow.providers.lamindb.utils.filters.F` can follow them, for example
    ``F(ArtifactField.CREATED_BY, UserField.HANDLE)``. Foreign keys (``..._ID`` members) hold the id of
    the related record and can be compared directly.
    """

    related: LaminDBRegistry | None

    def __new__(cls, value: str, related: LaminDBRegistry | None = None) -> RegistryField:
        member = str.__new__(cls, value)
        member._value_ = value
        member.related = related
        return member


class ArtifactField(RegistryField):
    """Fields of ``core.artifact`` (:attr:`LaminDBRegistry.ARTIFACT`)."""

    ID = "id"
    UID = "uid"
    CREATED_AT = "created_at"
    DESCRIPTION = "description"
    EXTRA_DATA = "extra_data"
    HASH = "hash"
    IS_LATEST = "is_latest"
    IS_LOCKED = "is_locked"
    KEY = "key"
    KIND = "kind"
    N_FILES = "n_files"
    N_OBSERVATIONS = "n_observations"
    OTYPE = "otype"
    SIZE = "size"
    SUFFIX = "suffix"
    UPDATED_AT = "updated_at"
    VERSION_TAG = "version_tag"
    # foreign keys
    BRANCH_ID = "branch_id"
    CREATED_BY_ID = "created_by_id"
    CREATED_ON_ID = "created_on_id"
    RUN_ID = "run_id"
    SCHEMA_ID = "schema_id"
    SPACE_ID = "space_id"
    STORAGE_ID = "storage_id"
    # relations to one record
    BRANCH = "branch", LaminDBRegistry.BRANCH
    CREATED_BY = "created_by", LaminDBRegistry.USER
    CREATED_ON = "created_on", LaminDBRegistry.BRANCH
    RUN = "run", LaminDBRegistry.RUN
    SCHEMA = "schema", LaminDBRegistry.SCHEMA
    SPACE = "space", LaminDBRegistry.SPACE
    STORAGE = "storage", LaminDBRegistry.STORAGE
    # relations to many records
    ARTIFACTS = "artifacts", LaminDBRegistry.ARTIFACT
    COLLECTIONS = "collections", LaminDBRegistry.COLLECTION
    INPUT_OF_RUNS = "input_of_runs", LaminDBRegistry.RUN
    LINKED_BY_ARTIFACTS = "linked_by_artifacts", LaminDBRegistry.ARTIFACT
    LINKED_BY_RUNS = "linked_by_runs", LaminDBRegistry.RUN
    LINKED_IN_RECORDS = "linked_in_records", LaminDBRegistry.RECORD
    PROJECTS = "projects", LaminDBRegistry.PROJECT
    RECORDS = "records", LaminDBRegistry.RECORD
    RECREATING_RUNS = "recreating_runs", LaminDBRegistry.RUN
    REFERENCES = "references", LaminDBRegistry.REFERENCE
    RUNS = "runs", LaminDBRegistry.RUN
    SCHEMAS = "schemas", LaminDBRegistry.SCHEMA
    ULABELS = "ulabels", LaminDBRegistry.ULABEL
    USERS = "users", LaminDBRegistry.USER


class CollectionField(RegistryField):
    """Fields of ``core.collection`` (:attr:`LaminDBRegistry.COLLECTION`)."""

    ID = "id"
    UID = "uid"
    CREATED_AT = "created_at"
    DESCRIPTION = "description"
    HASH = "hash"
    IS_LATEST = "is_latest"
    IS_LOCKED = "is_locked"
    KEY = "key"
    REFERENCE = "reference"
    REFERENCE_TYPE = "reference_type"
    UPDATED_AT = "updated_at"
    VERSION_TAG = "version_tag"
    # foreign keys
    BRANCH_ID = "branch_id"
    CREATED_BY_ID = "created_by_id"
    CREATED_ON_ID = "created_on_id"
    META_ARTIFACT_ID = "meta_artifact_id"
    RUN_ID = "run_id"
    SCHEMA_ID = "schema_id"
    SPACE_ID = "space_id"
    # relations to one record
    BRANCH = "branch", LaminDBRegistry.BRANCH
    CREATED_BY = "created_by", LaminDBRegistry.USER
    CREATED_ON = "created_on", LaminDBRegistry.BRANCH
    META_ARTIFACT = "meta_artifact", LaminDBRegistry.ARTIFACT
    RUN = "run", LaminDBRegistry.RUN
    SCHEMA = "schema", LaminDBRegistry.SCHEMA
    SPACE = "space", LaminDBRegistry.SPACE
    # relations to many records
    ARTIFACTS = "artifacts", LaminDBRegistry.ARTIFACT
    INPUT_OF_RUNS = "input_of_runs", LaminDBRegistry.RUN
    LINKED_IN_RECORDS = "linked_in_records", LaminDBRegistry.RECORD
    PROJECTS = "projects", LaminDBRegistry.PROJECT
    RECORDS = "records", LaminDBRegistry.RECORD
    RECREATING_RUNS = "recreating_runs", LaminDBRegistry.RUN
    REFERENCES = "references", LaminDBRegistry.REFERENCE
    ULABELS = "ulabels", LaminDBRegistry.ULABEL


class RunField(RegistryField):
    """Fields of ``core.run`` (:attr:`LaminDBRegistry.RUN`)."""

    ID = "id"
    UID = "uid"
    CLI_ARGS = "cli_args"
    CREATED_AT = "created_at"
    DESCRIPTION = "description"
    ENTRYPOINT = "entrypoint"
    EXTRA_DATA = "extra_data"
    FINISHED_AT = "finished_at"
    IS_LOCKED = "is_locked"
    NAME = "name"
    PARAMS = "params"
    REFERENCE = "reference"
    REFERENCE_TYPE = "reference_type"
    STARTED_AT = "started_at"
    STATUS_CODE = "_status_code"
    """The run status as a code, compare it with :class:`RunStatus`."""
    UPDATED_AT = "updated_at"
    # foreign keys
    BRANCH_ID = "branch_id"
    CREATED_BY_ID = "created_by_id"
    CREATED_ON_ID = "created_on_id"
    ENVIRONMENT_ID = "environment_id"
    INITIATED_BY_RUN_ID = "initiated_by_run_id"
    PLAN_ID = "plan_id"
    REPORT_ID = "report_id"
    SPACE_ID = "space_id"
    TRANSFORM_ID = "transform_id"
    # relations to one record
    BRANCH = "branch", LaminDBRegistry.BRANCH
    CREATED_BY = "created_by", LaminDBRegistry.USER
    CREATED_ON = "created_on", LaminDBRegistry.BRANCH
    ENVIRONMENT = "environment", LaminDBRegistry.ARTIFACT
    INITIATED_BY_RUN = "initiated_by_run", LaminDBRegistry.RUN
    PLAN = "plan", LaminDBRegistry.ARTIFACT
    REPORT = "report", LaminDBRegistry.ARTIFACT
    SPACE = "space", LaminDBRegistry.SPACE
    TRANSFORM = "transform", LaminDBRegistry.TRANSFORM
    # relations to many records
    ARTIFACTS = "artifacts", LaminDBRegistry.ARTIFACT
    INPUT_ARTIFACTS = "input_artifacts", LaminDBRegistry.ARTIFACT
    INPUT_COLLECTIONS = "input_collections", LaminDBRegistry.COLLECTION
    INPUT_RECORDS = "input_records", LaminDBRegistry.RECORD
    LINKED_ARTIFACTS = "linked_artifacts", LaminDBRegistry.ARTIFACT
    LINKED_IN_RECORDS = "linked_in_records", LaminDBRegistry.RECORD
    OUTPUT_ARTIFACTS = "output_artifacts", LaminDBRegistry.ARTIFACT
    OUTPUT_COLLECTIONS = "output_collections", LaminDBRegistry.COLLECTION
    OUTPUT_RECORDS = "output_records", LaminDBRegistry.RECORD
    OUTPUT_TRANSFORMS = "output_transforms", LaminDBRegistry.TRANSFORM
    PROJECTS = "projects", LaminDBRegistry.PROJECT
    RECORDS = "records", LaminDBRegistry.RECORD
    RECREATED_ARTIFACTS = "recreated_artifacts", LaminDBRegistry.ARTIFACT
    RECREATED_COLLECTIONS = "recreated_collections", LaminDBRegistry.COLLECTION
    ULABELS = "ulabels", LaminDBRegistry.ULABEL


class TransformField(RegistryField):
    """Fields of ``core.transform`` (:attr:`LaminDBRegistry.TRANSFORM`)."""

    ID = "id"
    UID = "uid"
    CREATED_AT = "created_at"
    DESCRIPTION = "description"
    HASH = "hash"
    IS_LATEST = "is_latest"
    IS_LOCKED = "is_locked"
    KEY = "key"
    KIND = "kind"
    REFERENCE = "reference"
    REFERENCE_TYPE = "reference_type"
    SOURCE_CODE = "source_code"
    UPDATED_AT = "updated_at"
    VERSION_TAG = "version_tag"
    # foreign keys
    BRANCH_ID = "branch_id"
    CREATED_BY_ID = "created_by_id"
    CREATED_ON_ID = "created_on_id"
    ENVIRONMENT_ID = "environment_id"
    PLAN_ID = "plan_id"
    RUN_ID = "run_id"
    SPACE_ID = "space_id"
    # relations to one record
    BRANCH = "branch", LaminDBRegistry.BRANCH
    CREATED_BY = "created_by", LaminDBRegistry.USER
    CREATED_ON = "created_on", LaminDBRegistry.BRANCH
    ENVIRONMENT = "environment", LaminDBRegistry.ARTIFACT
    PLAN = "plan", LaminDBRegistry.ARTIFACT
    RUN = "run", LaminDBRegistry.RUN
    SPACE = "space", LaminDBRegistry.SPACE
    # relations to many records
    LINKED_IN_RECORDS = "linked_in_records", LaminDBRegistry.RECORD
    PREDECESSORS = "predecessors", LaminDBRegistry.TRANSFORM
    PROJECTS = "projects", LaminDBRegistry.PROJECT
    RECORDS = "records", LaminDBRegistry.RECORD
    REFERENCES = "references", LaminDBRegistry.REFERENCE
    RUNS = "runs", LaminDBRegistry.RUN
    SUCCESSORS = "successors", LaminDBRegistry.TRANSFORM
    ULABELS = "ulabels", LaminDBRegistry.ULABEL


class RecordField(RegistryField):
    """Fields of ``core.record`` (:attr:`LaminDBRegistry.RECORD`)."""

    ID = "id"
    UID = "uid"
    CREATED_AT = "created_at"
    DESCRIPTION = "description"
    EXTRA_DATA = "extra_data"
    IS_LOCKED = "is_locked"
    IS_TYPE = "is_type"
    NAME = "name"
    REFERENCE = "reference"
    REFERENCE_TYPE = "reference_type"
    UPDATED_AT = "updated_at"
    # foreign keys
    BRANCH_ID = "branch_id"
    CREATED_BY_ID = "created_by_id"
    CREATED_ON_ID = "created_on_id"
    RUN_ID = "run_id"
    SCHEMA_ID = "schema_id"
    SPACE_ID = "space_id"
    TYPE_ID = "type_id"
    # relations to one record
    BRANCH = "branch", LaminDBRegistry.BRANCH
    CREATED_BY = "created_by", LaminDBRegistry.USER
    CREATED_ON = "created_on", LaminDBRegistry.BRANCH
    RUN = "run", LaminDBRegistry.RUN
    SCHEMA = "schema", LaminDBRegistry.SCHEMA
    SPACE = "space", LaminDBRegistry.SPACE
    TYPE = "type", LaminDBRegistry.RECORD
    # relations to many records
    ARTIFACTS = "artifacts", LaminDBRegistry.ARTIFACT
    CHILDREN = "children", LaminDBRegistry.RECORD
    COLLECTIONS = "collections", LaminDBRegistry.COLLECTION
    INPUT_OF_RUNS = "input_of_runs", LaminDBRegistry.RUN
    LINKED_ARTIFACTS = "linked_artifacts", LaminDBRegistry.ARTIFACT
    LINKED_COLLECTIONS = "linked_collections", LaminDBRegistry.COLLECTION
    LINKED_IN_RECORDS = "linked_in_records", LaminDBRegistry.RECORD
    LINKED_PROJECTS = "linked_projects", LaminDBRegistry.PROJECT
    LINKED_RECORDS = "linked_records", LaminDBRegistry.RECORD
    LINKED_REFERENCES = "linked_references", LaminDBRegistry.REFERENCE
    LINKED_RUNS = "linked_runs", LaminDBRegistry.RUN
    LINKED_TRANSFORMS = "linked_transforms", LaminDBRegistry.TRANSFORM
    LINKED_ULABELS = "linked_ulabels", LaminDBRegistry.ULABEL
    LINKED_USERS = "linked_users", LaminDBRegistry.USER
    PARENTS = "parents", LaminDBRegistry.RECORD
    PROJECTS = "projects", LaminDBRegistry.PROJECT
    REFERENCES = "references", LaminDBRegistry.REFERENCE
    RUNS = "runs", LaminDBRegistry.RUN
    TRANSFORMS = "transforms", LaminDBRegistry.TRANSFORM


class ULabelField(RegistryField):
    """Fields of ``core.ulabel`` (:attr:`LaminDBRegistry.ULABEL`)."""

    ID = "id"
    UID = "uid"
    CREATED_AT = "created_at"
    DESCRIPTION = "description"
    IS_LOCKED = "is_locked"
    IS_TYPE = "is_type"
    NAME = "name"
    REFERENCE = "reference"
    REFERENCE_TYPE = "reference_type"
    UPDATED_AT = "updated_at"
    # foreign keys
    BRANCH_ID = "branch_id"
    CREATED_BY_ID = "created_by_id"
    CREATED_ON_ID = "created_on_id"
    RUN_ID = "run_id"
    SPACE_ID = "space_id"
    TYPE_ID = "type_id"
    # relations to one record
    BRANCH = "branch", LaminDBRegistry.BRANCH
    CREATED_BY = "created_by", LaminDBRegistry.USER
    CREATED_ON = "created_on", LaminDBRegistry.BRANCH
    RUN = "run", LaminDBRegistry.RUN
    SPACE = "space", LaminDBRegistry.SPACE
    TYPE = "type", LaminDBRegistry.ULABEL
    # relations to many records
    ARTIFACTS = "artifacts", LaminDBRegistry.ARTIFACT
    BRANCHES = "branches", LaminDBRegistry.BRANCH
    CHILDREN = "children", LaminDBRegistry.ULABEL
    COLLECTIONS = "collections", LaminDBRegistry.COLLECTION
    LINKED_IN_RECORDS = "linked_in_records", LaminDBRegistry.RECORD
    PARENTS = "parents", LaminDBRegistry.ULABEL
    PROJECTS = "projects", LaminDBRegistry.PROJECT
    RUNS = "runs", LaminDBRegistry.RUN
    TRANSFORMS = "transforms", LaminDBRegistry.TRANSFORM


class FeatureField(RegistryField):
    """Fields of ``core.feature`` (:attr:`LaminDBRegistry.FEATURE`)."""

    ID = "id"
    UID = "uid"
    ARRAY_RANK = "array_rank"
    ARRAY_SHAPE = "array_shape"
    ARRAY_SIZE = "array_size"
    COERCE = "coerce"
    CREATED_AT = "created_at"
    DEFAULT_VALUE = "default_value"
    DESCRIPTION = "description"
    IS_LOCKED = "is_locked"
    IS_TYPE = "is_type"
    NAME = "name"
    NULLABLE = "nullable"
    SYNONYMS = "synonyms"
    UNIT = "unit"
    UPDATED_AT = "updated_at"
    # foreign keys
    BRANCH_ID = "branch_id"
    CREATED_BY_ID = "created_by_id"
    CREATED_ON_ID = "created_on_id"
    RUN_ID = "run_id"
    SPACE_ID = "space_id"
    TYPE_ID = "type_id"
    # relations to one record
    BRANCH = "branch", LaminDBRegistry.BRANCH
    CREATED_BY = "created_by", LaminDBRegistry.USER
    CREATED_ON = "created_on", LaminDBRegistry.BRANCH
    RUN = "run", LaminDBRegistry.RUN
    SPACE = "space", LaminDBRegistry.SPACE
    TYPE = "type", LaminDBRegistry.FEATURE
    # relations to many records
    PROJECTS = "projects", LaminDBRegistry.PROJECT
    SCHEMAS = "schemas", LaminDBRegistry.SCHEMA


class SchemaField(RegistryField):
    """Fields of ``core.schema`` (:attr:`LaminDBRegistry.SCHEMA`)."""

    ID = "id"
    UID = "uid"
    COERCE = "coerce"
    CREATED_AT = "created_at"
    DESCRIPTION = "description"
    FLEXIBLE = "flexible"
    HASH = "hash"
    IS_LOCKED = "is_locked"
    IS_TYPE = "is_type"
    ITYPE = "itype"
    MAXIMAL_SET = "maximal_set"
    MINIMAL_SET = "minimal_set"
    N_MEMBERS = "n_members"
    NAME = "name"
    ORDERED_SET = "ordered_set"
    OTYPE = "otype"
    SUFFIX = "suffix"
    UPDATED_AT = "updated_at"
    # foreign keys
    BRANCH_ID = "branch_id"
    CREATED_BY_ID = "created_by_id"
    CREATED_ON_ID = "created_on_id"
    RUN_ID = "run_id"
    SPACE_ID = "space_id"
    TYPE_ID = "type_id"
    # relations to one record
    BRANCH = "branch", LaminDBRegistry.BRANCH
    CREATED_BY = "created_by", LaminDBRegistry.USER
    CREATED_ON = "created_on", LaminDBRegistry.BRANCH
    RUN = "run", LaminDBRegistry.RUN
    SPACE = "space", LaminDBRegistry.SPACE
    TYPE = "type", LaminDBRegistry.SCHEMA
    # relations to many records
    ARTIFACTS = "artifacts", LaminDBRegistry.ARTIFACT
    COMPONENTS = "components", LaminDBRegistry.SCHEMA
    COMPOSITES = "composites", LaminDBRegistry.SCHEMA
    FEATURES = "features", LaminDBRegistry.FEATURE
    PROJECTS = "projects", LaminDBRegistry.PROJECT
    RECORDS = "records", LaminDBRegistry.RECORD
    VALIDATED_ARTIFACTS = "validated_artifacts", LaminDBRegistry.ARTIFACT
    VALIDATED_COLLECTIONS = "validated_collections", LaminDBRegistry.COLLECTION


class ProjectField(RegistryField):
    """Fields of ``core.project`` (:attr:`LaminDBRegistry.PROJECT`)."""

    ID = "id"
    UID = "uid"
    ABBR = "abbr"
    CREATED_AT = "created_at"
    DESCRIPTION = "description"
    END_DATE = "end_date"
    IS_LOCKED = "is_locked"
    IS_TYPE = "is_type"
    NAME = "name"
    START_DATE = "start_date"
    UPDATED_AT = "updated_at"
    URL = "url"
    # foreign keys
    BRANCH_ID = "branch_id"
    CREATED_BY_ID = "created_by_id"
    CREATED_ON_ID = "created_on_id"
    RUN_ID = "run_id"
    SPACE_ID = "space_id"
    TYPE_ID = "type_id"
    # relations to one record
    BRANCH = "branch", LaminDBRegistry.BRANCH
    CREATED_BY = "created_by", LaminDBRegistry.USER
    CREATED_ON = "created_on", LaminDBRegistry.BRANCH
    RUN = "run", LaminDBRegistry.RUN
    SPACE = "space", LaminDBRegistry.SPACE
    TYPE = "type", LaminDBRegistry.PROJECT
    # relations to many records
    ARTIFACTS = "artifacts", LaminDBRegistry.ARTIFACT
    BRANCHES = "branches", LaminDBRegistry.BRANCH
    CHILDREN = "children", LaminDBRegistry.PROJECT
    COLLECTIONS = "collections", LaminDBRegistry.COLLECTION
    FEATURES = "features", LaminDBRegistry.FEATURE
    LINKED_IN_RECORDS = "linked_in_records", LaminDBRegistry.RECORD
    PARENTS = "parents", LaminDBRegistry.PROJECT
    PREDECESSORS = "predecessors", LaminDBRegistry.PROJECT
    RECORDS = "records", LaminDBRegistry.RECORD
    REFERENCES = "references", LaminDBRegistry.REFERENCE
    RUNS = "runs", LaminDBRegistry.RUN
    SCHEMAS = "schemas", LaminDBRegistry.SCHEMA
    SUCCESSORS = "successors", LaminDBRegistry.PROJECT
    TRANSFORMS = "transforms", LaminDBRegistry.TRANSFORM
    ULABELS = "ulabels", LaminDBRegistry.ULABEL
    USERS = "users", LaminDBRegistry.USER


class ReferenceField(RegistryField):
    """Fields of ``core.reference`` (:attr:`LaminDBRegistry.REFERENCE`)."""

    ID = "id"
    UID = "uid"
    ABBR = "abbr"
    CREATED_AT = "created_at"
    DATE = "date"
    DESCRIPTION = "description"
    DOI = "doi"
    IS_LOCKED = "is_locked"
    IS_TYPE = "is_type"
    NAME = "name"
    PUBMED_ID = "pubmed_id"
    TEXT = "text"
    UPDATED_AT = "updated_at"
    URL = "url"
    # foreign keys
    BRANCH_ID = "branch_id"
    CREATED_BY_ID = "created_by_id"
    CREATED_ON_ID = "created_on_id"
    RUN_ID = "run_id"
    SPACE_ID = "space_id"
    TYPE_ID = "type_id"
    # relations to one record
    BRANCH = "branch", LaminDBRegistry.BRANCH
    CREATED_BY = "created_by", LaminDBRegistry.USER
    CREATED_ON = "created_on", LaminDBRegistry.BRANCH
    RUN = "run", LaminDBRegistry.RUN
    SPACE = "space", LaminDBRegistry.SPACE
    TYPE = "type", LaminDBRegistry.REFERENCE
    # relations to many records
    ARTIFACTS = "artifacts", LaminDBRegistry.ARTIFACT
    COLLECTIONS = "collections", LaminDBRegistry.COLLECTION
    LINKED_IN_RECORDS = "linked_in_records", LaminDBRegistry.RECORD
    PROJECTS = "projects", LaminDBRegistry.PROJECT
    RECORDS = "records", LaminDBRegistry.RECORD
    TRANSFORMS = "transforms", LaminDBRegistry.TRANSFORM


class StorageField(RegistryField):
    """Fields of ``core.storage`` (:attr:`LaminDBRegistry.STORAGE`)."""

    ID = "id"
    UID = "uid"
    CREATED_AT = "created_at"
    DESCRIPTION = "description"
    INSTANCE_UID = "instance_uid"
    IS_LOCKED = "is_locked"
    REGION = "region"
    ROOT = "root"
    TYPE = "type"
    UPDATED_AT = "updated_at"
    # foreign keys
    BRANCH_ID = "branch_id"
    CREATED_BY_ID = "created_by_id"
    CREATED_ON_ID = "created_on_id"
    RUN_ID = "run_id"
    SPACE_ID = "space_id"
    # relations to one record
    BRANCH = "branch", LaminDBRegistry.BRANCH
    CREATED_BY = "created_by", LaminDBRegistry.USER
    CREATED_ON = "created_on", LaminDBRegistry.BRANCH
    RUN = "run", LaminDBRegistry.RUN
    SPACE = "space", LaminDBRegistry.SPACE
    # relations to many records
    ARTIFACTS = "artifacts", LaminDBRegistry.ARTIFACT


class BranchField(RegistryField):
    """Fields of ``core.branch`` (:attr:`LaminDBRegistry.BRANCH`)."""

    ID = "id"
    UID = "uid"
    CREATED_AT = "created_at"
    DESCRIPTION = "description"
    NAME = "name"
    # foreign keys
    CREATED_BY_ID = "created_by_id"
    SPACE_ID = "space_id"
    # relations to one record
    CREATED_BY = "created_by", LaminDBRegistry.USER
    SPACE = "space", LaminDBRegistry.SPACE
    # relations to many records
    PROJECTS = "projects", LaminDBRegistry.PROJECT
    ULABELS = "ulabels", LaminDBRegistry.ULABEL
    USERS = "users", LaminDBRegistry.USER


class SpaceField(RegistryField):
    """Fields of ``core.space`` (:attr:`LaminDBRegistry.SPACE`)."""

    ID = "id"
    UID = "uid"
    CREATED_AT = "created_at"
    DESCRIPTION = "description"
    NAME = "name"
    # foreign keys
    CREATED_BY_ID = "created_by_id"
    # relations to one record
    CREATED_BY = "created_by", LaminDBRegistry.USER


class UserField(RegistryField):
    """Fields of ``core.user`` (:attr:`LaminDBRegistry.USER`)."""

    ID = "id"
    UID = "uid"
    CREATED_AT = "created_at"
    HANDLE = "handle"
    NAME = "name"
    UPDATED_AT = "updated_at"
    # relations to many records
    ARTIFACTS = "artifacts", LaminDBRegistry.ARTIFACT
    BRANCHES = "branches", LaminDBRegistry.BRANCH
    CREATED_ARTIFACTS = "created_artifacts", LaminDBRegistry.ARTIFACT
    CREATED_RUNS = "created_runs", LaminDBRegistry.RUN
    CREATED_TRANSFORMS = "created_transforms", LaminDBRegistry.TRANSFORM
    LINKED_IN_RECORDS = "linked_in_records", LaminDBRegistry.RECORD
    PROJECTS = "projects", LaminDBRegistry.PROJECT


REGISTRY_FIELDS: dict[LaminDBRegistry, type[RegistryField]] = {
    LaminDBRegistry.ARTIFACT: ArtifactField,
    LaminDBRegistry.COLLECTION: CollectionField,
    LaminDBRegistry.RUN: RunField,
    LaminDBRegistry.TRANSFORM: TransformField,
    LaminDBRegistry.RECORD: RecordField,
    LaminDBRegistry.ULABEL: ULabelField,
    LaminDBRegistry.FEATURE: FeatureField,
    LaminDBRegistry.SCHEMA: SchemaField,
    LaminDBRegistry.PROJECT: ProjectField,
    LaminDBRegistry.REFERENCE: ReferenceField,
    LaminDBRegistry.STORAGE: StorageField,
    LaminDBRegistry.BRANCH: BranchField,
    LaminDBRegistry.SPACE: SpaceField,
    LaminDBRegistry.USER: UserField,
}
"""The field enum of each registry."""
