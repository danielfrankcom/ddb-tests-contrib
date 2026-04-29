"""Tests for $out stage."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast

import pytest
from bson import (
    Binary,
    Code,
    Decimal128,
    Int64,
    MaxKey,
    MinKey,
    ObjectId,
    Regex,
    Timestamp,
)
from bson.raw_bson import RawBSONDocument

from documentdb_tests.compatibility.tests.core.operator.stages.utils.stage_test_case import (
    StageTestCase,
    populate_collection,
)
from documentdb_tests.framework.assertions import (
    assertFailure,
    assertFailureCode,
    assertResult,
    assertSuccess,
)
from documentdb_tests.framework.bson_helpers import build_raw_bson_doc
from documentdb_tests.framework.error_codes import (
    BAD_VALUE_ERROR,
    COMMAND_NOT_SUPPORTED_ON_VIEW_ERROR,
    DOCUMENT_VALIDATION_FAILURE_ERROR,
    DUPLICATE_KEY_ERROR,
    FACET_OUT_NOT_ALLOWED_ERROR,
    IDL_DUPLICATE_FIELD_ERROR,
    ILLEGAL_OPERATION_ERROR,
    INVALID_NAMESPACE_ERROR,
    INVALID_OPTIONS_ERROR,
    INVALID_VIEW_PIPELINE_ERROR,
    LOOKUP_OUT_NOT_ALLOWED_ERROR,
    NO_SUCH_KEY_ERROR,
    OUT_ARGUMENT_TYPE_ERROR,
    OUT_CAPPED_COLLECTION_ERROR,
    OUT_NOT_LAST_STAGE_ERROR,
    OUT_RESTRICTED_DATABASE_ERROR,
    OUT_SPECIAL_COLLECTION_ERROR,
    OUT_TIMESERIES_COLLECTION_TYPE_ERROR,
    OUT_TIMESERIES_OPTIONS_MISMATCH_ERROR,
    TYPE_MISMATCH_ERROR,
    UNAUTHORIZED_ERROR,
    UNION_WITH_OUT_NOT_ALLOWED_ERROR,
    UNRECOGNIZED_COMMAND_FIELD_ERROR,
)
from documentdb_tests.framework.executor import execute_command
from documentdb_tests.framework.parametrize import pytest_params
from documentdb_tests.framework.test_constants import (
    DECIMAL128_HALF,
    DECIMAL128_INFINITY,
    DECIMAL128_NAN,
    DECIMAL128_NEGATIVE_INFINITY,
    DECIMAL128_NEGATIVE_ONE_AND_HALF,
    DECIMAL128_NEGATIVE_ZERO,
    DECIMAL128_ONE_AND_HALF,
    DECIMAL128_TWO_AND_HALF,
    DOUBLE_MAX,
    DOUBLE_MAX_SAFE_INTEGER,
    DOUBLE_MIN_SUBNORMAL,
    DOUBLE_NEGATIVE_ZERO,
    FLOAT_INFINITY,
    FLOAT_NAN,
    FLOAT_NEGATIVE_INFINITY,
    INT32_MAX,
    INT32_MIN,
    INT32_OVERFLOW,
    INT32_UNDERFLOW,
)


@dataclass(frozen=True)
class OutTestCase(StageTestCase):
    """Test case for $out stage tests with collection verification."""

    target_coll: str = "target"
    target_db: str | None = None
    out_spec: Any = None
    expected_type: str = "collection"
    expected_options: dict[str, Any] | None = None


def _build_out_stage(collection, test_case: OutTestCase) -> dict[str, Any]:
    """Build the $out stage spec from the test case."""
    db_name = test_case.target_db or collection.database.name
    target = test_case.target_coll
    if test_case.out_spec is not None or test_case.target_db is not None:
        spec: dict[str, Any] = {"db": db_name, "coll": target}
        if test_case.out_spec:
            spec.update(test_case.out_spec)
        return {"$out": spec}
    return {"$out": target}


def _resolve_pipeline(pipeline: list[dict[str, Any]], db_name: str) -> list[dict[str, Any]]:
    """Replace __DB__ placeholders in a pipeline with the actual database name."""
    resolved = []
    for stage in pipeline:
        resolved.append({k: _resolve_value(v, db_name) for k, v in stage.items()})
    return resolved


def _resolve_value(value: Any, db_name: str) -> Any:
    """Recursively replace __DB__ string placeholders."""
    if value == "__DB__":
        return db_name
    if isinstance(value, dict):
        return {k: _resolve_value(v, db_name) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_value(v, db_name) for v in value]
    return value


# Property [Syntax Forms]: $out accepts a string (same-database output), a
# document with db/coll (cross-database output), or a document with db/coll
# and timeseries (time series collection output), and each form writes the
# pipeline results to the specified target.
OUT_SYNTAX_FORMS_TESTS: list[OutTestCase] = [
    OutTestCase(
        "string_form_same_database",
        docs=[{"_id": 1, "value": 10}, {"_id": 2, "value": 20}],
        target_coll="syntax_string_target",
        out_spec=None,
        expected_type="collection",
        expected_options={},
        msg="$out string form should write results to a collection in the same database",
    ),
    OutTestCase(
        "document_form_db_and_coll",
        docs=[{"_id": 1, "value": 10}, {"_id": 2, "value": 20}],
        target_coll="syntax_doc_target",
        out_spec={},
        expected_type="collection",
        expected_options={},
        msg="$out document form with db and coll should write results to the specified collection",
    ),
    OutTestCase(
        "document_form_with_timeseries",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "value": 10}],
        target_coll="syntax_ts_target",
        out_spec={"timeseries": {"timeField": "ts"}},
        expected_type="timeseries",
        expected_options={
            "timeseries": {
                "timeField": "ts",
                "granularity": "seconds",
                "bucketMaxSpanSeconds": 3_600,
            }
        },
        msg="$out document form with timeseries should create a time series collection",
    ),
]

# Property [Null as Absent]: null values for timeseries and its sub-fields
# (metaField, granularity, bucketMaxSpanSeconds, bucketRoundingSeconds) are
# treated as absent, producing the same collection as if the field were omitted.
OUT_NULL_SUCCESS_TESTS: list[OutTestCase] = [
    OutTestCase(
        "null_timeseries_regular_collection",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "value": 10}],
        target_coll="target_ts_null",
        out_spec={"timeseries": None},
        expected_type="collection",
        expected_options={},
        msg="$out should treat timeseries null as absent and create a regular collection",
    ),
    OutTestCase(
        "null_meta_field_omitted",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "value": 10}],
        target_coll="target_meta_null",
        out_spec={"timeseries": {"timeField": "ts", "metaField": None}},
        expected_type="timeseries",
        expected_options={
            "timeseries": {
                "timeField": "ts",
                "granularity": "seconds",
                "bucketMaxSpanSeconds": 3_600,
            }
        },
        msg="$out should treat metaField null as absent and omit it from timeseries options",
    ),
    OutTestCase(
        "null_granularity_defaults_to_seconds",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "value": 10}],
        target_coll="target_gran_null",
        out_spec={"timeseries": {"timeField": "ts", "granularity": None}},
        expected_type="timeseries",
        expected_options={
            "timeseries": {
                "timeField": "ts",
                "granularity": "seconds",
                "bucketMaxSpanSeconds": 3_600,
            }
        },
        msg="$out should treat granularity null as absent and default to 'seconds'",
    ),
    OutTestCase(
        "null_bucket_params_defaults_to_granularity",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "value": 10}],
        target_coll="target_bucket_null",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": None,
                "bucketRoundingSeconds": None,
            }
        },
        expected_type="timeseries",
        expected_options={
            "timeseries": {
                "timeField": "ts",
                "granularity": "seconds",
                "bucketMaxSpanSeconds": 3_600,
            }
        },
        msg=(
            "$out should treat null bucketMaxSpanSeconds and bucketRoundingSeconds"
            " as absent and default to granularity-based bucketing"
        ),
    ),
]

# Property [Collection Name Acceptance]: any non-empty string of non-null
# bytes that does not match a rejection rule is accepted as a collection name.
OUT_COLLECTION_NAME_ACCEPTANCE_TESTS: list[OutTestCase] = [
    OutTestCase(
        "control_character",
        docs=[{"_id": 1}],
        target_coll="\x01",
        expected_type="collection",
        expected_options={},
        msg="$out should accept a control character as a collection name",
    ),
    OutTestCase(
        "embedded_control_character",
        docs=[{"_id": 1}],
        target_coll="test\x1fcoll",
        expected_type="collection",
        expected_options={},
        msg="$out should accept embedded control characters in a collection name",
    ),
    OutTestCase(
        "unicode_no_break_space",
        docs=[{"_id": 1}],
        target_coll="\u00a0",
        expected_type="collection",
        expected_options={},
        msg="$out should accept Unicode no-break space as a collection name",
    ),
    OutTestCase(
        "zero_width_space",
        docs=[{"_id": 1}],
        target_coll="\u200b",
        expected_type="collection",
        expected_options={},
        msg="$out should accept zero-width space as a collection name",
    ),
    OutTestCase(
        "bom_character",
        docs=[{"_id": 1}],
        target_coll="\ufeff",
        expected_type="collection",
        expected_options={},
        msg="$out should accept BOM character as a collection name",
    ),
    OutTestCase(
        "emoji",
        docs=[{"_id": 1}],
        target_coll="\U0001f389",
        expected_type="collection",
        expected_options={},
        msg="$out should accept emoji as a collection name",
    ),
    OutTestCase(
        "cjk_characters",
        docs=[{"_id": 1}],
        target_coll="\u4e2d\u6587",
        expected_type="collection",
        expected_options={},
        msg="$out should accept CJK characters as a collection name",
    ),
    OutTestCase(
        "punctuation",
        docs=[{"_id": 1}],
        target_coll="a!@#b",
        expected_type="collection",
        expected_options={},
        msg="$out should accept punctuation in a collection name",
    ),
    OutTestCase(
        "single_character",
        docs=[{"_id": 1}],
        target_coll="a",
        expected_type="collection",
        expected_options={},
        msg="$out should accept a single-character collection name",
    ),
    OutTestCase(
        "single_digit",
        docs=[{"_id": 1}],
        target_coll="1",
        expected_type="collection",
        expected_options={},
        msg="$out should accept a single-digit collection name",
    ),
    OutTestCase(
        "digits_only",
        docs=[{"_id": 1}],
        target_coll="123",
        expected_type="collection",
        expected_options={},
        msg="$out should accept a digits-only collection name",
    ),
    OutTestCase(
        "temp_prefix",
        docs=[{"_id": 1}],
        target_coll="tmp.agg_out.",
        expected_type="collection",
        expected_options={},
        msg="$out should accept the tmp.agg_out. prefix as a regular collection name",
    ),
]

# Property [Timeseries Collection Creation]: $out creates a new time
# series collection when valid timeseries options are provided and the
# target does not exist, including edge cases where metaField is "_id" or
# matches timeField.
OUT_TIMESERIES_CREATION_TESTS: list[OutTestCase] = [
    OutTestCase(
        "ts_meta_field_is_id",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "value": 10}],
        target_coll="ts_creation_meta_id",
        out_spec={"timeseries": {"timeField": "ts", "metaField": "_id"}},
        expected_type="timeseries",
        expected_options={
            "timeseries": {
                "timeField": "ts",
                "metaField": "_id",
                "granularity": "seconds",
                "bucketMaxSpanSeconds": 3_600,
            }
        },
        msg='$out should accept metaField set to "_id" without error',
    ),
    OutTestCase(
        "ts_meta_field_same_as_time_field",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "value": 10}],
        target_coll="ts_creation_meta_same",
        out_spec={"timeseries": {"timeField": "ts", "metaField": "ts"}},
        expected_type="timeseries",
        expected_options={
            "timeseries": {
                "timeField": "ts",
                "metaField": "ts",
                "granularity": "seconds",
                "bucketMaxSpanSeconds": 3_600,
            }
        },
        msg="$out should accept metaField set to the same value as timeField without error",
    ),
]

# Property [Bucket Param Type Acceptance]: bucketMaxSpanSeconds and
# bucketRoundingSeconds accept int32, Int64, float, and Decimal128, and the
# equality check between them is type-insensitive.
OUT_BUCKET_PARAM_TYPE_ACCEPTANCE_TESTS: list[OutTestCase] = [
    OutTestCase(
        "bucket_int32",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_int32",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": 100,
                "bucketRoundingSeconds": 100,
            }
        },
        expected_type="timeseries",
        expected_options={
            "timeseries": {
                "timeField": "ts",
                "bucketRoundingSeconds": 100,
                "bucketMaxSpanSeconds": 100,
            }
        },
        msg="$out should accept int32 for bucket parameters",
    ),
    OutTestCase(
        "bucket_int64",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_int64",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": Int64(100),
                "bucketRoundingSeconds": Int64(100),
            }
        },
        expected_type="timeseries",
        expected_options={
            "timeseries": {
                "timeField": "ts",
                "bucketRoundingSeconds": 100,
                "bucketMaxSpanSeconds": 100,
            }
        },
        msg="$out should accept Int64 for bucket parameters",
    ),
    OutTestCase(
        "bucket_float",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_float",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": 100.0,
                "bucketRoundingSeconds": 100.0,
            }
        },
        expected_type="timeseries",
        expected_options={
            "timeseries": {
                "timeField": "ts",
                "bucketRoundingSeconds": 100,
                "bucketMaxSpanSeconds": 100,
            }
        },
        msg="$out should accept float for bucket parameters",
    ),
    OutTestCase(
        "bucket_decimal128",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_decimal128",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": Decimal128("100"),
                "bucketRoundingSeconds": Decimal128("100"),
            }
        },
        expected_type="timeseries",
        expected_options={
            "timeseries": {
                "timeField": "ts",
                "bucketRoundingSeconds": 100,
                "bucketMaxSpanSeconds": 100,
            }
        },
        msg="$out should accept Decimal128 for bucket parameters",
    ),
    OutTestCase(
        "bucket_cross_int32_int64",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_cross_i32_i64",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": 100,
                "bucketRoundingSeconds": Int64(100),
            }
        },
        expected_type="timeseries",
        expected_options={
            "timeseries": {
                "timeField": "ts",
                "bucketRoundingSeconds": 100,
                "bucketMaxSpanSeconds": 100,
            }
        },
        msg="$out should accept cross-type int32/Int64 bucket parameters",
    ),
    OutTestCase(
        "bucket_cross_float_decimal128",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_cross_f_d128",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": 100.0,
                "bucketRoundingSeconds": Decimal128("100"),
            }
        },
        expected_type="timeseries",
        expected_options={
            "timeseries": {
                "timeField": "ts",
                "bucketRoundingSeconds": 100,
                "bucketMaxSpanSeconds": 100,
            }
        },
        msg="$out should accept cross-type float/Decimal128 bucket parameters",
    ),
    OutTestCase(
        "bucket_float_truncation_success",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_float_trunc",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": 1.5,
                "bucketRoundingSeconds": 1.5,
            }
        },
        expected_type="timeseries",
        expected_options={
            "timeseries": {
                "timeField": "ts",
                "bucketRoundingSeconds": 1,
                "bucketMaxSpanSeconds": 1,
            }
        },
        msg="$out should truncate float 1.5 to int32 1 for bucket parameters",
    ),
    OutTestCase(
        "bucket_decimal128_bankers_rounding",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_dec_bankers",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": DECIMAL128_ONE_AND_HALF,
                "bucketRoundingSeconds": DECIMAL128_ONE_AND_HALF,
            }
        },
        expected_type="timeseries",
        expected_options={
            "timeseries": {
                "timeField": "ts",
                "bucketRoundingSeconds": 2,
                "bucketMaxSpanSeconds": 2,
            }
        },
        msg="$out should round Decimal128 1.5 to 2 (banker's rounding) for bucket parameters",
    ),
    OutTestCase(
        "bucket_decimal128_bankers_round_down",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_dec_bank_dn",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": DECIMAL128_TWO_AND_HALF,
                "bucketRoundingSeconds": DECIMAL128_TWO_AND_HALF,
            }
        },
        expected_type="timeseries",
        expected_options={
            "timeseries": {
                "timeField": "ts",
                "bucketRoundingSeconds": 2,
                "bucketMaxSpanSeconds": 2,
            }
        },
        msg="$out should round Decimal128 2.5 to 2 (banker's rounding) for bucket parameters",
    ),
    OutTestCase(
        "bucket_cross_coerced_equality",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_cross_coerce",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": 2,
                "bucketRoundingSeconds": DECIMAL128_ONE_AND_HALF,
            }
        },
        expected_type="timeseries",
        expected_options={
            "timeseries": {
                "timeField": "ts",
                "bucketRoundingSeconds": 2,
                "bucketMaxSpanSeconds": 2,
            }
        },
        msg=(
            "$out should accept cross-type bucket params when coerced values are"
            " equal (int32 2 and Decimal128 1.5 -> 2)"
        ),
    ),
    OutTestCase(
        "bucket_range_min",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_range_min",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": 1,
                "bucketRoundingSeconds": 1,
            }
        },
        expected_type="timeseries",
        expected_options={
            "timeseries": {
                "timeField": "ts",
                "bucketRoundingSeconds": 1,
                "bucketMaxSpanSeconds": 1,
            }
        },
        msg="$out should accept bucket parameters at the minimum valid value (1)",
    ),
    OutTestCase(
        "bucket_range_max",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_range_max",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": 31_536_000,
                "bucketRoundingSeconds": 31_536_000,
            }
        },
        expected_type="timeseries",
        expected_options={
            "timeseries": {
                "timeField": "ts",
                "bucketRoundingSeconds": 31_536_000,
                "bucketMaxSpanSeconds": 31_536_000,
            }
        },
        msg="$out should accept bucket parameters at the maximum valid value (31536000)",
    ),
]

# Property [Timeseries Granularity]: valid granularity values ("seconds",
# "minutes", "hours") are accepted and each produces the corresponding
# bucketMaxSpanSeconds default.
OUT_TIMESERIES_GRANULARITY_TESTS: list[OutTestCase] = [
    OutTestCase(
        "granularity_seconds",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="ts_gran_seconds",
        out_spec={"timeseries": {"timeField": "ts", "granularity": "seconds"}},
        expected_type="timeseries",
        expected_options={
            "timeseries": {
                "timeField": "ts",
                "granularity": "seconds",
                "bucketMaxSpanSeconds": 3_600,
            }
        },
        msg="$out should accept granularity 'seconds'",
    ),
    OutTestCase(
        "granularity_minutes",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="ts_gran_minutes",
        out_spec={"timeseries": {"timeField": "ts", "granularity": "minutes"}},
        expected_type="timeseries",
        expected_options={
            "timeseries": {
                "timeField": "ts",
                "granularity": "minutes",
                "bucketMaxSpanSeconds": 86_400,
            }
        },
        msg="$out should accept granularity 'minutes'",
    ),
    OutTestCase(
        "granularity_hours",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="ts_gran_hours",
        out_spec={"timeseries": {"timeField": "ts", "granularity": "hours"}},
        expected_type="timeseries",
        expected_options={
            "timeseries": {
                "timeField": "ts",
                "granularity": "hours",
                "bucketMaxSpanSeconds": 2_592_000,
            }
        },
        msg="$out should accept granularity 'hours'",
    ),
]

OUT_SUCCESS_TESTS = (
    OUT_SYNTAX_FORMS_TESTS
    + OUT_NULL_SUCCESS_TESTS
    + OUT_COLLECTION_NAME_ACCEPTANCE_TESTS
    + OUT_TIMESERIES_CREATION_TESTS
    + OUT_BUCKET_PARAM_TYPE_ACCEPTANCE_TESTS
    + OUT_TIMESERIES_GRANULARITY_TESTS
)


@pytest.mark.aggregate
@pytest.mark.parametrize("test_case", pytest_params(OUT_SUCCESS_TESTS))
def test_out_success(collection, test_case: OutTestCase):
    """Test $out writes results and creates the correct collection type."""
    populate_collection(collection, test_case)
    out_stage = _build_out_stage(collection, test_case)
    execute_command(
        collection,
        {"aggregate": collection.name, "pipeline": [out_stage], "cursor": {}},
    )
    result = execute_command(
        collection,
        {"listCollections": 1, "filter": {"name": test_case.target_coll}},
    )
    expected_info = {
        "name": test_case.target_coll,
        "type": test_case.expected_type,
        "options": test_case.expected_options,
    }
    assertSuccess(
        result,
        [expected_info],
        msg=test_case.msg,
        transform=lambda docs: [
            {"name": d["name"], "type": d["type"], "options": d.get("options", {})} for d in docs
        ],
    )


# Property [Timeseries DateTime Acceptance]: all datetime boundary values
# are accepted as timeField values when writing to a timeseries collection
# via $out, including Unix epoch, pre-epoch, far future, minimum datetime,
# and millisecond precision.
OUT_TIMESERIES_DATETIME_ACCEPTANCE_TESTS: list[OutTestCase] = [
    OutTestCase(
        "ts_datetime_epoch",
        docs=[{"_id": 1, "ts": datetime(1970, 1, 1), "v": 1}],
        target_coll="ts_dt_epoch",
        out_spec={"timeseries": {"timeField": "ts"}},
        expected=[{"ts": datetime(1970, 1, 1), "v": 1}],
        msg="$out timeseries should accept Unix epoch as timeField value",
    ),
    OutTestCase(
        "ts_datetime_pre_epoch",
        docs=[{"_id": 1, "ts": datetime(1960, 6, 15), "v": 2}],
        target_coll="ts_dt_pre_epoch",
        out_spec={"timeseries": {"timeField": "ts"}},
        expected=[{"ts": datetime(1960, 6, 15), "v": 2}],
        msg="$out timeseries should accept pre-epoch dates as timeField value",
    ),
    OutTestCase(
        "ts_datetime_far_future",
        docs=[{"_id": 1, "ts": datetime(9999, 12, 31, 23, 59, 59), "v": 3}],
        target_coll="ts_dt_far_future",
        out_spec={"timeseries": {"timeField": "ts"}},
        expected=[{"ts": datetime(9999, 12, 31, 23, 59, 59), "v": 3}],
        msg="$out timeseries should accept far future dates as timeField value",
    ),
    OutTestCase(
        "ts_datetime_minimum",
        docs=[{"_id": 1, "ts": datetime(1, 1, 1), "v": 4}],
        target_coll="ts_dt_minimum",
        out_spec={"timeseries": {"timeField": "ts"}},
        expected=[{"ts": datetime(1, 1, 1), "v": 4}],
        msg="$out timeseries should accept minimum datetime (0001-01-01) as timeField value",
    ),
    OutTestCase(
        "ts_datetime_millisecond_precision",
        docs=[{"_id": 1, "ts": datetime(2024, 6, 15, 12, 30, 45, 123_000), "v": 5}],
        target_coll="ts_dt_millis",
        out_spec={"timeseries": {"timeField": "ts"}},
        expected=[{"ts": datetime(2024, 6, 15, 12, 30, 45, 123_000), "v": 5}],
        msg="$out timeseries should accept datetimes with millisecond precision as timeField value",
    ),
]


@pytest.mark.aggregate
@pytest.mark.parametrize("test_case", pytest_params(OUT_TIMESERIES_DATETIME_ACCEPTANCE_TESTS))
def test_out_timeseries_datetime_acceptance(collection, test_case: OutTestCase):
    """Test $out timeseries accepts datetime boundary values as timeField."""
    populate_collection(collection, test_case)
    out_stage = _build_out_stage(collection, test_case)
    execute_command(
        collection,
        {"aggregate": collection.name, "pipeline": [out_stage], "cursor": {}},
    )
    result = execute_command(
        collection,
        {
            "find": test_case.target_coll,
            "filter": {},
            "projection": {"_id": 0, "ts": 1, "v": 1},
        },
    )
    assertSuccess(result, test_case.expected, msg=test_case.msg)


# Property [Timeseries Existing Collection]: writing to an existing time
# series collection succeeds both with matching timeseries options and
# without specifying timeseries options (string and document form).
@pytest.mark.aggregate
@pytest.mark.parametrize(
    "out_stage_builder",
    [
        pytest.param(
            lambda db_name, coll_name: {
                "$out": {
                    "db": db_name,
                    "coll": coll_name,
                    "timeseries": {"timeField": "ts"},
                }
            },
            id="matching_timeseries_options",
        ),
        pytest.param(
            lambda db_name, coll_name: {"$out": coll_name},
            id="string_form_no_timeseries",
        ),
        pytest.param(
            lambda db_name, coll_name: {"$out": {"db": db_name, "coll": coll_name}},
            id="document_form_no_timeseries",
        ),
    ],
)
def test_out_timeseries_existing(collection, out_stage_builder):
    """Test $out writes to an existing time series collection."""
    db = collection.database
    target_name = "ts_existing_target"
    db.drop_collection(target_name)
    db.command({"create": target_name, "timeseries": {"timeField": "ts"}})
    populate_collection(
        collection,
        StageTestCase(
            id="ts_existing",
            docs=[{"_id": 1, "ts": datetime(2024, 6, 1), "value": 60}],
            msg="$out should write to an existing time series collection",
        ),
    )
    out_stage = out_stage_builder(db.name, target_name)
    execute_command(
        collection,
        {"aggregate": collection.name, "pipeline": [out_stage], "cursor": {}},
    )
    result = execute_command(
        collection,
        {"find": target_name, "filter": {}, "projection": {"_id": 0, "ts": 1, "value": 1}},
    )
    assertSuccess(
        result,
        [{"ts": datetime(2024, 6, 1), "value": 60}],
        msg="$out should successfully write to an existing time series collection",
    )


# Property [Timeseries Cross-Database]: $out creates a time series collection
# in a different database when timeseries options are specified with a
# cross-database target.
@pytest.mark.aggregate
def test_out_timeseries_cross_db(collection):
    """Test $out creates a time series collection in a different database."""
    populate_collection(
        collection,
        StageTestCase(
            id="ts_cross_db",
            docs=[{"_id": 1, "ts": datetime(2024, 7, 1), "value": 70}],
            msg="$out should create a time series collection in a different database",
        ),
    )
    client = collection.database.client
    cross_db_name = collection.database.name + "_ts_cross"
    client.drop_database(cross_db_name)
    try:
        out_stage = {
            "$out": {
                "db": cross_db_name,
                "coll": "ts_cross_target",
                "timeseries": {"timeField": "ts"},
            }
        }
        execute_command(
            collection,
            {"aggregate": collection.name, "pipeline": [out_stage], "cursor": {}},
        )
        cross_db = client[cross_db_name]
        result = execute_command(
            cross_db["ts_cross_target"],
            {"listCollections": 1, "filter": {"name": "ts_cross_target"}},
        )
        assertSuccess(
            result,
            [
                {
                    "name": "ts_cross_target",
                    "type": "timeseries",
                    "options": {
                        "timeseries": {
                            "timeField": "ts",
                            "granularity": "seconds",
                            "bucketMaxSpanSeconds": 3_600,
                        }
                    },
                }
            ],
            msg="$out should create a time series collection in a different database",
            transform=lambda docs: [
                {
                    "name": d["name"],
                    "type": d["type"],
                    "options": d.get("options", {}),
                }
                for d in docs
            ],
        )
    finally:
        client.drop_database(cross_db_name)


# Property [Database Name Acceptance]: any non-empty string of non-null
# bytes that does not contain a slash, backslash, dot, ASCII space, or dollar
# prefix is accepted as a database name.
OUT_DATABASE_NAME_ACCEPTANCE_TESTS: list[OutTestCase] = [
    OutTestCase(
        "db_control_character",
        docs=[{"_id": 1}],
        target_coll="target",
        target_db="\x01",
        msg="$out should accept a control character as a database name",
    ),
    OutTestCase(
        "db_unicode_no_break_space",
        docs=[{"_id": 1}],
        target_coll="target",
        target_db="\u00a0",
        msg="$out should accept Unicode no-break space as a database name",
    ),
    OutTestCase(
        "db_zero_width_space",
        docs=[{"_id": 1}],
        target_coll="target",
        target_db="\u200b",
        msg="$out should accept zero-width space as a database name",
    ),
    OutTestCase(
        "db_emoji",
        docs=[{"_id": 1}],
        target_coll="target",
        target_db="\U0001f389",
        msg="$out should accept emoji as a database name",
    ),
    OutTestCase(
        "db_cjk_characters",
        docs=[{"_id": 1}],
        target_coll="target",
        target_db="\u4e2d\u6587",
        msg="$out should accept CJK characters as a database name",
    ),
    OutTestCase(
        "db_punctuation",
        docs=[{"_id": 1}],
        target_coll="target",
        target_db="a!@#b",
        msg="$out should accept punctuation in a database name",
    ),
    OutTestCase(
        "db_single_character",
        docs=[{"_id": 1}],
        target_coll="target",
        target_db="a",
        msg="$out should accept a single-character database name",
    ),
    OutTestCase(
        "db_digits_only",
        docs=[{"_id": 1}],
        target_coll="target",
        target_db="123",
        msg="$out should accept a digits-only database name",
    ),
]


@pytest.mark.aggregate
@pytest.mark.parametrize("test_case", pytest_params(OUT_DATABASE_NAME_ACCEPTANCE_TESTS))
def test_out_database_name_acceptance(collection, test_case: OutTestCase):
    """Test $out accepts various character classes as database names."""
    populate_collection(collection, test_case)
    db_name = test_case.target_db  # type: ignore[arg-type]
    client = collection.database.client
    client.drop_database(db_name)
    try:
        out_stage = {"$out": {"db": db_name, "coll": test_case.target_coll}}
        execute_command(
            collection,
            {"aggregate": collection.name, "pipeline": [out_stage], "cursor": {}},
        )
        target_db = client[db_name]
        result = execute_command(
            target_db[test_case.target_coll],
            {"listCollections": 1, "filter": {"name": test_case.target_coll}},
        )
        assertSuccess(
            result,
            [{"name": test_case.target_coll, "type": "collection", "options": {}}],
            msg=test_case.msg,
            transform=lambda docs: [
                {"name": d["name"], "type": d["type"], "options": d.get("options", {})}
                for d in docs
            ],
        )
    finally:
        client.drop_database(db_name)


# Property [Collection Creation]: $out creates a new collection (and database
# if needed) when the target does not exist, and an empty pipeline result
# creates an empty collection or empties an existing one.
OUT_COLLECTION_CREATION_TESTS: list[OutTestCase] = [
    OutTestCase(
        "new_collection_created",
        docs=[{"_id": 1, "value": 10}, {"_id": 2, "value": 20}],
        target_coll="creation_new_target",
        out_spec=None,
        expected=[{"_id": 1, "value": 10}, {"_id": 2, "value": 20}],
        msg="$out should create a new collection when the target does not exist",
    ),
    OutTestCase(
        "new_database_created",
        docs=[{"_id": 1, "value": 10}],
        target_coll="creation_cross_db_target",
        target_db="__CROSS_DB__",
        expected=[{"_id": 1, "value": 10}],
        msg="$out should create a new database when the output database does not exist",
    ),
    OutTestCase(
        "empty_pipeline_creates_empty_collection",
        docs=[],
        target_coll="creation_empty_target",
        out_spec=None,
        expected=[],
        msg="$out with no documents should create an empty collection",
    ),
    OutTestCase(
        "empty_pipeline_empties_existing_collection",
        docs=[],
        target_coll="creation_emptied_target",
        out_spec=None,
        expected=[],
        msg="$out with no documents should empty an existing collection",
    ),
]


@pytest.mark.aggregate
@pytest.mark.parametrize("test_case", pytest_params(OUT_COLLECTION_CREATION_TESTS))
def test_out_collection_creation(collection, test_case: OutTestCase):
    """Test $out creates collections and databases as needed."""
    populate_collection(collection, test_case)
    db = collection.database
    client = db.client
    cross_db_name = db.name + "_cross"
    if test_case.id == "empty_pipeline_empties_existing_collection":
        db[test_case.target_coll].insert_one({"_id": 99, "old": True})
    # Replace placeholder with a unique cross-database name.
    if test_case.target_db == "__CROSS_DB__":
        client.drop_database(cross_db_name)
        effective_case = OutTestCase(
            id=test_case.id,
            docs=test_case.docs,
            target_coll=test_case.target_coll,
            target_db=cross_db_name,
            expected=test_case.expected,
            msg=test_case.msg,
        )
    else:
        effective_case = test_case
    try:
        out_stage = _build_out_stage(collection, effective_case)
        execute_command(
            collection,
            {"aggregate": collection.name, "pipeline": [out_stage], "cursor": {}},
        )
        target_db = client[effective_case.target_db] if effective_case.target_db else db
        target_coll = target_db[effective_case.target_coll]
        # Use listCollections to verify the collection exists. This is
        # necessary because find on a non-existent collection also returns
        # an empty firstBatch, which would make empty-result cases pass
        # even if $out never created the target.
        result = execute_command(
            target_coll,
            {"listCollections": 1, "filter": {"name": effective_case.target_coll}},
        )
        expected_docs = effective_case.expected
        assertSuccess(
            result,
            {"exists": True, "docs": expected_docs},
            msg=effective_case.msg,
            transform=lambda r: {
                "exists": len(r) == 1,
                "docs": sorted(
                    target_coll.find(
                        {},
                        {k: 1 for d in (expected_docs or [{}]) for k in d},
                    ),
                    key=lambda d: d.get("_id", 0),
                ),
            },
        )
    finally:
        if effective_case.target_db and effective_case.target_db != db.name:
            client.drop_database(effective_case.target_db)


# Property [Collection Replacement - Atomic Replace]: an existing collection
# is atomically replaced with the new pipeline results upon $out completion.
@pytest.mark.aggregate
def test_out_replacement_atomic(collection):
    """Test $out atomically replaces an existing collection with new results."""
    db = collection.database
    target_name = "replacement_atomic_target"
    populate_collection(
        collection,
        StageTestCase(
            id="replacement_atomic",
            docs=[{"_id": 10, "new": True}, {"_id": 20, "new": True}],
            msg="$out should atomically replace existing collection",
        ),
    )
    db[target_name].insert_many([{"_id": 1, "old": True}, {"_id": 2, "old": True}])
    execute_command(
        collection,
        {"aggregate": collection.name, "pipeline": [{"$out": target_name}], "cursor": {}},
    )
    result = execute_command(collection, {"find": target_name, "filter": {}, "sort": {"_id": 1}})
    assertSuccess(
        result,
        [{"_id": 10, "new": True}, {"_id": 20, "new": True}],
        msg="$out should replace existing documents with new pipeline results",
    )


# Property [Collection Replacement - Index Preservation]: indexes from the
# previous collection are preserved after $out replaces its contents.
@pytest.mark.aggregate
def test_out_replacement_preserves_indexes(collection):
    """Test $out preserves indexes from the previous collection after replacement."""
    db = collection.database
    target_name = "replacement_idx_target"
    populate_collection(
        collection,
        StageTestCase(
            id="replacement_idx",
            docs=[{"_id": 10, "x": 100}, {"_id": 20, "x": 200}],
            msg="$out should preserve indexes after replacement",
        ),
    )
    db[target_name].insert_one({"_id": 1, "x": 1})
    db[target_name].create_index("x", name="x_idx", unique=True)
    execute_command(
        collection,
        {"aggregate": collection.name, "pipeline": [{"$out": target_name}], "cursor": {}},
    )
    result = execute_command(
        collection,
        {"listIndexes": target_name},
    )
    assertSuccess(
        result,
        [{"name": "_id_"}, {"name": "x_idx", "unique": True}],
        msg="$out should preserve indexes from the previous collection",
        transform=lambda docs: [
            {"name": d["name"], **({"unique": d["unique"]} if d.get("unique") else {})}
            for d in sorted(docs, key=lambda d: d["name"])
        ],
    )


# Property [Collection Replacement - Self-Replacement]: writing to the same
# collection as the input succeeds and the collection contains the transformed
# results.
@pytest.mark.aggregate
def test_out_replacement_self(collection):
    """Test $out self-replacement writes transformed results back to the source."""
    populate_collection(
        collection,
        StageTestCase(
            id="replacement_self",
            docs=[{"_id": 1, "value": 10}, {"_id": 2, "value": 20}],
            msg="$out should support self-replacement",
        ),
    )
    execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [
                {"$addFields": {"doubled": {"$multiply": ["$value", 2]}}},
                {"$out": collection.name},
            ],
            "cursor": {},
        },
    )
    result = execute_command(
        collection, {"find": collection.name, "filter": {}, "sort": {"_id": 1}}
    )
    assertSuccess(
        result,
        [{"_id": 1, "value": 10, "doubled": 20}, {"_id": 2, "value": 20, "doubled": 40}],
        msg="$out self-replacement should contain transformed results",
    )


# Property [Collection Replacement - Failure Rollback]: if the aggregation
# fails during $out, the pre-existing collection and its indexes are unchanged.
@pytest.mark.aggregate
def test_out_replacement_failure_unchanged(collection):
    """Test $out leaves the pre-existing collection unchanged on failure."""
    db = collection.database
    target_name = "replacement_fail_target"
    populate_collection(
        collection,
        StageTestCase(
            id="replacement_fail",
            docs=[{"_id": 10, "x": 1}, {"_id": 20, "x": 1}],
            msg="$out failure should leave existing collection unchanged",
        ),
    )
    db[target_name].insert_many([{"_id": 1, "x": 1}, {"_id": 2, "x": 2}])
    db[target_name].create_index("x", unique=True)
    execute_command(
        collection,
        {"aggregate": collection.name, "pipeline": [{"$out": target_name}], "cursor": {}},
    )
    # The aggregation fails due to unique index violation; verify the
    # pre-existing collection and its indexes are unchanged.
    idx_result = execute_command(
        collection,
        {"listIndexes": target_name},
    )
    target = db[target_name]
    assertSuccess(
        idx_result,
        {
            "docs": [{"_id": 1, "x": 1}, {"_id": 2, "x": 2}],
            "indexes": [{"name": "_id_"}, {"name": "x_1", "unique": True}],
        },
        msg="$out failure should leave pre-existing collection and indexes unchanged",
        transform=lambda idx_docs: {
            "docs": sorted(target.find({}, {"_id": 1, "x": 1}), key=lambda d: d["_id"]),
            "indexes": [
                {
                    "name": d["name"],
                    **({"unique": d["unique"]} if d.get("unique") else {}),
                }
                for d in sorted(idx_docs, key=lambda d: d["name"])
            ],
        },
    )


@pytest.mark.aggregate
def test_out_temp_collection_during_execution(collection):
    """Test $out uses a temporary collection that is cleaned up after completion."""
    docs = [{"_id": i, "value": i} for i in range(10_000)]
    populate_collection(
        collection,
        StageTestCase(
            id="temp_coll",
            docs=docs,
            msg="$out should use a temp collection during execution",
        ),
    )
    db = collection.database
    target_coll = "creation_temp_target"

    found_tmp: list[str] = []
    stop = threading.Event()

    def poll_collections() -> None:
        while not stop.is_set():
            try:
                names = db.list_collection_names()
                for name in names:
                    if name.startswith("tmp.agg_out."):
                        found_tmp.append(name)
                        return
            except Exception:
                pass

    t = threading.Thread(target=poll_collections, daemon=True)
    t.start()

    execute_command(
        collection,
        {"aggregate": collection.name, "pipeline": [{"$out": target_coll}], "cursor": {}},
    )

    stop.set()
    t.join(timeout=5)

    # Verify temp collection was observed during execution and cleaned up after.
    result = execute_command(
        collection,
        {"listCollections": 1, "filter": {"name": {"$regex": "^tmp\\.agg_out\\."}}},
    )
    assertSuccess(
        result,
        {"observed": True, "remaining": []},
        raw_res=True,
        msg="$out should use a temp collection during execution and clean it up after",
        transform=lambda r: {
            "observed": len(found_tmp) > 0,
            "remaining": [d["name"] for d in r["cursor"]["firstBatch"]],
        },
    )


# Property [Write Behavior - Auto-Generated _id]: documents with _id removed
# via a pipeline stage receive auto-generated ObjectId _id values in the
# output collection.
@pytest.mark.aggregate
def test_out_auto_generated_id(collection):
    """Test $out auto-generates ObjectId _id when _id is removed."""
    populate_collection(
        collection,
        StageTestCase(
            id="auto_id",
            docs=[{"_id": 1, "value": 10}, {"_id": 2, "value": 20}],
            msg="$out should auto-generate ObjectId _id values",
        ),
    )
    target_name = "write_auto_id_target"
    execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [{"$unset": "_id"}, {"$out": target_name}],
            "cursor": {},
        },
    )
    result = execute_command(
        collection,
        {"find": target_name, "filter": {}, "sort": {"value": 1}},
    )
    assertSuccess(
        result,
        [{"value": 10, "is_objectid": True}, {"value": 20, "is_objectid": True}],
        msg="$out should auto-generate ObjectId _id when _id is removed",
        transform=lambda docs: [
            {"value": d["value"], "is_objectid": isinstance(d["_id"], ObjectId)} for d in docs
        ],
    )


# Property [Write Behavior - Empty Cursor]: the aggregation cursor returned
# by a pipeline ending with $out contains an empty result list.
@pytest.mark.aggregate
def test_out_empty_cursor(collection):
    """Test $out returns an empty cursor result."""
    populate_collection(
        collection,
        StageTestCase(
            id="empty_cursor",
            docs=[{"_id": 1, "value": 10}],
            msg="$out should return an empty cursor",
        ),
    )
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [{"$out": "write_cursor_target"}],
            "cursor": {},
        },
    )
    assertSuccess(
        result,
        [],
        msg="$out aggregation cursor should return an empty result list",
    )


# Property [Write Behavior - Explain No Write]: explain does not perform the
# write - the target collection is not created or modified.
@pytest.mark.aggregate
def test_out_explain_no_write(collection):
    """Test explain with $out does not create or modify the target collection."""
    populate_collection(
        collection,
        StageTestCase(
            id="explain_no_write",
            docs=[{"_id": 1, "value": 10}],
            msg="explain should not perform the $out write",
        ),
    )
    target_name = "write_explain_target"
    execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [{"$out": target_name}],
            "cursor": {},
            "explain": True,
        },
    )
    result = execute_command(
        collection,
        {"listCollections": 1, "filter": {"name": target_name}},
    )
    assertSuccess(
        result,
        [],
        msg="explain with $out should not create the target collection",
    )


@pytest.mark.aggregate
def test_out_explain_no_modify(collection):
    """Test explain with $out does not modify an existing target collection."""
    populate_collection(
        collection,
        StageTestCase(
            id="explain_no_modify",
            docs=[{"_id": 10, "new": True}],
            msg="explain should not modify existing target collection",
        ),
    )
    db = collection.database
    target_name = "write_explain_existing_target"
    db[target_name].insert_many([{"_id": 1, "old": True}, {"_id": 2, "old": True}])
    execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [{"$out": target_name}],
            "cursor": {},
            "explain": True,
        },
    )
    result = execute_command(
        collection,
        {"find": target_name, "filter": {}, "sort": {"_id": 1}},
    )
    assertSuccess(
        result,
        [{"_id": 1, "old": True}, {"_id": 2, "old": True}],
        msg="explain with $out should not modify existing target collection",
    )


# Property [Write Behavior - Idempotent]: running the same $out pipeline to
# the same target twice produces the same result in the target collection.
@pytest.mark.aggregate
def test_out_idempotent(collection):
    """Test $out is idempotent when run twice to the same target."""
    populate_collection(
        collection,
        StageTestCase(
            id="idempotent",
            docs=[{"_id": 1, "value": 10}, {"_id": 2, "value": 20}],
            msg="$out should be idempotent",
        ),
    )
    target_name = "write_idempotent_target"
    pipeline = [{"$out": target_name}]
    execute_command(
        collection,
        {"aggregate": collection.name, "pipeline": pipeline, "cursor": {}},
    )
    execute_command(
        collection,
        {"aggregate": collection.name, "pipeline": pipeline, "cursor": {}},
    )
    result = execute_command(
        collection,
        {"find": target_name, "filter": {}, "sort": {"_id": 1}},
    )
    assertSuccess(
        result,
        [{"_id": 1, "value": 10}, {"_id": 2, "value": 20}],
        msg="$out should produce the same result when run twice to the same target",
    )


# Property [Write Behavior - BSON Round-Trip]: all BSON types representable
# by pymongo round-trip through $out without modification.
@pytest.mark.aggregate
def test_out_bson_round_trip(collection):
    """Test all BSON types round-trip through $out without modification."""
    bson_doc = {
        "_id": 1,
        "double_val": 3.14,
        "string_val": "hello",
        "object_val": {"nested": True},
        "array_val": [1, 2, 3],
        "binary_val": Binary(b"\x01\x02\x03"),
        "objectid_val": ObjectId("507f1f77bcf86cd799439011"),
        "bool_val": True,
        "date_val": datetime(2024, 1, 1),
        "null_val": None,
        "regex_val": Regex("abc", "i"),
        "int32_val": 42,
        "timestamp_val": Timestamp(1_234_567_890, 1),
        "int64_val": Int64(9_876_543_210),
        "decimal128_val": Decimal128("123.456"),
        "minkey_val": MinKey(),
        "maxkey_val": MaxKey(),
        "code_val": Code("function() {}"),
        "code_ws_val": Code("function() {}", {"x": 1}),
    }
    populate_collection(
        collection,
        StageTestCase(
            id="bson_round_trip",
            docs=[bson_doc],
            msg="all BSON types should round-trip through $out",
        ),
    )
    target_name = "write_bson_target"
    execute_command(
        collection,
        {"aggregate": collection.name, "pipeline": [{"$out": target_name}], "cursor": {}},
    )
    source_result = execute_command(
        collection,
        {"find": collection.name, "filter": {}},
    )
    target_result = execute_command(
        collection,
        {"find": target_name, "filter": {}},
    )
    assertSuccess(
        target_result,
        cast(dict, source_result)["cursor"]["firstBatch"],
        msg="all BSON types should round-trip through $out without modification",
    )


# Property [Write Behavior - Large Documents]: documents up to 15 MB are
# written successfully through $out.
@pytest.mark.aggregate
def test_out_large_document(collection):
    """Test $out writes documents up to 15 MB successfully."""
    large_str = "x" * (15 * 1_024 * 1_024)
    populate_collection(
        collection,
        StageTestCase(
            id="large_doc",
            docs=[{"_id": 1, "data": large_str}],
            msg="$out should write large documents up to 15 MB",
        ),
    )
    target_name = "write_large_target"
    execute_command(
        collection,
        {"aggregate": collection.name, "pipeline": [{"$out": target_name}], "cursor": {}},
    )
    result = execute_command(
        collection,
        {"find": target_name, "filter": {}, "projection": {"_id": 1}},
    )
    assertSuccess(
        result,
        [{"_id": 1}],
        msg="$out should successfully write a 15 MB document",
    )


# Property [No Unicode Normalization - Collections]: precomposed and combining
# forms of the same character create separate, distinct collections - no
# Unicode normalization is applied to collection names.
@pytest.mark.aggregate
def test_out_no_unicode_normalization(collection):
    """Test $out treats precomposed and combining Unicode forms as distinct collection names."""
    populate_collection(
        collection,
        StageTestCase(
            id="no_normalization",
            docs=[{"_id": 1, "form": "precomposed"}, {"_id": 2, "form": "combining"}],
            msg="$out should not normalize Unicode collection names",
        ),
    )
    # U+00E9 (precomposed e-acute, 2 UTF-8 bytes)
    precomposed = "\u00e9"
    # U+0065 U+0301 (e + combining acute, 3 UTF-8 bytes)
    combining = "\u0065\u0301"

    execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [
                {"$match": {"_id": 1}},
                {"$out": precomposed},
            ],
            "cursor": {},
        },
    )
    execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [
                {"$match": {"_id": 2}},
                {"$out": combining},
            ],
            "cursor": {},
        },
    )
    db = collection.database
    precomposed_docs = list(db[precomposed].find({}, {"_id": 1, "form": 1}))
    combining_docs = list(db[combining].find({}, {"_id": 1, "form": 1}))
    result = execute_command(
        collection,
        {"listCollections": 1, "filter": {"name": {"$in": [precomposed, combining]}}},
    )
    assertSuccess(
        result,
        {
            "precomposed_docs": [{"_id": 1, "form": "precomposed"}],
            "combining_docs": [{"_id": 2, "form": "combining"}],
            "collection_count": 2,
        },
        msg="$out should create separate collections for precomposed and combining forms",
        transform=lambda docs: {
            "precomposed_docs": precomposed_docs,
            "combining_docs": combining_docs,
            "collection_count": len(docs),
        },
    )


# Property [No Unicode Normalization - Databases]: precomposed and combining
# forms of the same character create separate, distinct databases - no Unicode
# normalization is applied to database names.
@pytest.mark.aggregate
def test_out_no_unicode_normalization_database(collection):
    """Test $out treats precomposed and combining Unicode forms as distinct database names."""
    populate_collection(
        collection,
        StageTestCase(
            id="no_normalization_db",
            docs=[{"_id": 1, "form": "precomposed"}, {"_id": 2, "form": "combining"}],
            msg="$out should not normalize Unicode database names",
        ),
    )
    # U+00E9 (precomposed e-acute, 2 UTF-8 bytes)
    precomposed_db = "\u00e9"
    # U+0065 U+0301 (e + combining acute, 3 UTF-8 bytes)
    combining_db = "\u0065\u0301"
    client = collection.database.client
    client.drop_database(precomposed_db)
    client.drop_database(combining_db)
    try:
        execute_command(
            collection,
            {
                "aggregate": collection.name,
                "pipeline": [
                    {"$match": {"_id": 1}},
                    {"$out": {"db": precomposed_db, "coll": "target"}},
                ],
                "cursor": {},
            },
        )
        execute_command(
            collection,
            {
                "aggregate": collection.name,
                "pipeline": [
                    {"$match": {"_id": 2}},
                    {"$out": {"db": combining_db, "coll": "target"}},
                ],
                "cursor": {},
            },
        )
        precomposed_docs = list(client[precomposed_db]["target"].find({}, {"_id": 1, "form": 1}))
        combining_docs = list(client[combining_db]["target"].find({}, {"_id": 1, "form": 1}))
        db_names = client.list_database_names()
        assertSuccess(
            {"cursor": {"firstBatch": [{"result": True}]}},
            {
                "precomposed_docs": [{"_id": 1, "form": "precomposed"}],
                "combining_docs": [{"_id": 2, "form": "combining"}],
                "both_exist": True,
            },
            msg="$out should create separate databases for precomposed and combining forms",
            transform=lambda _: {
                "precomposed_docs": precomposed_docs,
                "combining_docs": combining_docs,
                "both_exist": precomposed_db in db_names and combining_db in db_names,
            },
        )
    finally:
        client.drop_database(precomposed_db)
        client.drop_database(combining_db)


# Property [Bucket Param Range Validation]: bucket parameter values outside
# the valid range (1 to 31536000) after numeric coercion to int32 produce
# error code 2.
OUT_BUCKET_PARAM_RANGE_ERROR_TESTS: list[OutTestCase] = [
    OutTestCase(
        "bucket_zero",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_err_zero",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": 0,
                "bucketRoundingSeconds": 0,
            }
        },
        msg="$out should reject bucket parameter value 0 (below minimum)",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "bucket_negative",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_err_neg",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": -1,
                "bucketRoundingSeconds": -1,
            }
        },
        msg="$out should reject negative bucket parameter values",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "bucket_above_max",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_err_above",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": 31_536_001,
                "bucketRoundingSeconds": 31_536_001,
            }
        },
        msg="$out should reject bucket parameter values above 31536000",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "bucket_float_truncates_to_zero",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_err_f_zero",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": 0.5,
                "bucketRoundingSeconds": 0.5,
            }
        },
        msg="$out should reject float 0.5 (truncates to 0, below minimum)",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "bucket_float_negative_truncation",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_err_f_neg",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": -1.5,
                "bucketRoundingSeconds": -1.5,
            }
        },
        msg="$out should reject float -1.5 (truncates to -1, below minimum)",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "bucket_float_nan",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_err_f_nan",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": FLOAT_NAN,
                "bucketRoundingSeconds": FLOAT_NAN,
            }
        },
        msg="$out should reject float NaN (converts to 0, below minimum)",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "bucket_float_neg_zero",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_err_f_nz",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": DOUBLE_NEGATIVE_ZERO,
                "bucketRoundingSeconds": DOUBLE_NEGATIVE_ZERO,
            }
        },
        msg="$out should reject float negative zero (converts to 0, below minimum)",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "bucket_float_inf",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_err_f_inf",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": FLOAT_INFINITY,
                "bucketRoundingSeconds": FLOAT_INFINITY,
            }
        },
        msg="$out should reject float +Infinity (clamps to int32 max, above max range)",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "bucket_float_neg_inf",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_err_f_ninf",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": FLOAT_NEGATIVE_INFINITY,
                "bucketRoundingSeconds": FLOAT_NEGATIVE_INFINITY,
            }
        },
        msg="$out should reject float -Infinity (clamps to int32 min, below minimum)",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "bucket_float_subnormal",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_err_f_sub",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": DOUBLE_MIN_SUBNORMAL,
                "bucketRoundingSeconds": DOUBLE_MIN_SUBNORMAL,
            }
        },
        msg="$out should reject float subnormal (truncates to 0, below minimum)",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "bucket_decimal128_neg_rounds",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_err_d_neg",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": DECIMAL128_NEGATIVE_ONE_AND_HALF,
                "bucketRoundingSeconds": DECIMAL128_NEGATIVE_ONE_AND_HALF,
            }
        },
        msg="$out should reject Decimal128 -1.5 (rounds to -2, below minimum)",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "bucket_decimal128_half_to_zero",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_err_d_half",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": DECIMAL128_HALF,
                "bucketRoundingSeconds": DECIMAL128_HALF,
            }
        },
        msg="$out should reject Decimal128 0.5 (banker's rounds to 0, below minimum)",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "bucket_decimal128_nan",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_err_d_nan",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": DECIMAL128_NAN,
                "bucketRoundingSeconds": DECIMAL128_NAN,
            }
        },
        msg="$out should reject Decimal128 NaN (converts to 0, below minimum)",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "bucket_decimal128_inf",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_err_d_inf",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": DECIMAL128_INFINITY,
                "bucketRoundingSeconds": DECIMAL128_INFINITY,
            }
        },
        msg="$out should reject Decimal128 +Infinity (clamps to int32 max, above max range)",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "bucket_decimal128_neg_inf",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_err_d_ninf",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": DECIMAL128_NEGATIVE_INFINITY,
                "bucketRoundingSeconds": DECIMAL128_NEGATIVE_INFINITY,
            }
        },
        msg="$out should reject Decimal128 -Infinity (clamps to int32 min, below minimum)",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "bucket_decimal128_neg_zero",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_err_d_nz",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": DECIMAL128_NEGATIVE_ZERO,
                "bucketRoundingSeconds": DECIMAL128_NEGATIVE_ZERO,
            }
        },
        msg="$out should reject Decimal128 -0 (converts to 0, below minimum)",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "bucket_int64_above_int32_max",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_err_i64_hi",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": Int64(INT32_OVERFLOW),
                "bucketRoundingSeconds": Int64(INT32_OVERFLOW),
            }
        },
        msg="$out should reject Int64 above int32 max (clamps to int32 max, above max range)",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "bucket_int64_int32_min",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_err_i64_lo",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": Int64(INT32_MIN),
                "bucketRoundingSeconds": Int64(INT32_MIN),
            }
        },
        msg="$out should reject Int64 at int32 min (below minimum)",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "bucket_int64_below_int32_min",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_err_i64_uf",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": Int64(INT32_UNDERFLOW),
                "bucketRoundingSeconds": Int64(INT32_UNDERFLOW),
            }
        },
        msg="$out should reject Int64 below int32 min (clamps to int32 min, below minimum)",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "bucket_float_max_safe_int",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_err_f_msi",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": DOUBLE_MAX_SAFE_INTEGER,
                "bucketRoundingSeconds": DOUBLE_MAX_SAFE_INTEGER,
            }
        },
        msg="$out should reject float max safe integer (clamps to int32 max, above max range)",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "bucket_float_dbl_max",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_err_dblmax",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": DOUBLE_MAX,
                "bucketRoundingSeconds": DOUBLE_MAX,
            }
        },
        msg="$out should reject float DBL_MAX (clamps to int32 max, above max range)",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "bucket_decimal128_large",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_err_d_lg",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": Decimal128("1E+100"),
                "bucketRoundingSeconds": Decimal128("1E+100"),
            }
        },
        msg="$out should reject Decimal128 1E+100 (clamps to int32 max, above max range)",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "bucket_int32_max",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_err_i32max",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": INT32_MAX,
                "bucketRoundingSeconds": INT32_MAX,
            }
        },
        msg="$out should reject int32 max (above max range 31536000)",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "bucket_int32_min",
        docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
        target_coll="bucket_err_i32min",
        out_spec={
            "timeseries": {
                "timeField": "ts",
                "bucketMaxSpanSeconds": INT32_MIN,
                "bucketRoundingSeconds": INT32_MIN,
            }
        },
        msg="$out should reject int32 min (below minimum)",
        error_code=BAD_VALUE_ERROR,
    ),
]

# Property [Null as Missing (Errors)]: null values for db, coll, and
# timeField are treated as missing rather than as type errors, and a null
# bucket parameter paired with a valid one produces an incomplete-pair
# error.
OUT_NULL_MISSING_ERROR_TESTS: list[OutTestCase] = [
    OutTestCase(
        "null_db_missing",
        docs=[{"_id": 1}],
        target_coll="target",
        pipeline=[{"$out": {"db": None, "coll": "target"}}],
        msg="$out should treat null db as missing, not as a type error",
        error_code=NO_SUCH_KEY_ERROR,
    ),
    OutTestCase(
        "null_coll_missing",
        docs=[{"_id": 1}],
        target_coll="target",
        pipeline=[{"$out": {"db": "__DB__", "coll": None}}],
        msg="$out should treat null coll as missing, not as a type error",
        error_code=NO_SUCH_KEY_ERROR,
    ),
    OutTestCase(
        "null_time_field_missing",
        docs=[{"_id": 1}],
        target_coll="target",
        pipeline=[{"$out": {"db": "__DB__", "coll": "target", "timeseries": {"timeField": None}}}],
        msg="$out should treat null timeField as missing, not as a type error",
        error_code=NO_SUCH_KEY_ERROR,
    ),
    OutTestCase(
        "null_bucket_max_with_valid_rounding",
        docs=[{"_id": 1}],
        target_coll="target",
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketMaxSpanSeconds": None,
                        "bucketRoundingSeconds": 100,
                    },
                }
            }
        ],
        msg=(
            "$out should reject null bucketMaxSpanSeconds paired with valid"
            " bucketRoundingSeconds as an incomplete pair"
        ),
        error_code=INVALID_OPTIONS_ERROR,
    ),
    OutTestCase(
        "null_bucket_rounding_with_valid_max",
        docs=[{"_id": 1}],
        target_coll="target",
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketMaxSpanSeconds": 100,
                        "bucketRoundingSeconds": None,
                    },
                }
            }
        ],
        msg=(
            "$out should reject null bucketRoundingSeconds paired with valid"
            " bucketMaxSpanSeconds as an incomplete pair"
        ),
        error_code=INVALID_OPTIONS_ERROR,
    ),
]

# Property [Stage Argument Type Errors]: any type other than string or
# document produces a stage argument type error, including arrays regardless
# of contents, size, nesting, or element types.
OUT_STAGE_ARGUMENT_TYPE_ERROR_TESTS: list[OutTestCase] = [
    OutTestCase(
        "arg_type_int32",
        docs=[{"_id": 1}],
        pipeline=[{"$out": 42}],
        msg="$out should reject int32 argument",
        error_code=OUT_ARGUMENT_TYPE_ERROR,
    ),
    OutTestCase(
        "arg_type_int64",
        docs=[{"_id": 1}],
        pipeline=[{"$out": Int64(42)}],
        msg="$out should reject Int64 argument",
        error_code=OUT_ARGUMENT_TYPE_ERROR,
    ),
    OutTestCase(
        "arg_type_float",
        docs=[{"_id": 1}],
        pipeline=[{"$out": 3.14}],
        msg="$out should reject float argument",
        error_code=OUT_ARGUMENT_TYPE_ERROR,
    ),
    OutTestCase(
        "arg_type_decimal128",
        docs=[{"_id": 1}],
        pipeline=[{"$out": Decimal128("99.9")}],
        msg="$out should reject Decimal128 argument",
        error_code=OUT_ARGUMENT_TYPE_ERROR,
    ),
    OutTestCase(
        "arg_type_bool",
        docs=[{"_id": 1}],
        pipeline=[{"$out": True}],
        msg="$out should reject boolean argument",
        error_code=OUT_ARGUMENT_TYPE_ERROR,
    ),
    OutTestCase(
        "arg_type_null",
        docs=[{"_id": 1}],
        pipeline=[{"$out": None}],
        msg="$out should reject null argument",
        error_code=OUT_ARGUMENT_TYPE_ERROR,
    ),
    OutTestCase(
        "arg_type_binary",
        docs=[{"_id": 1}],
        pipeline=[{"$out": Binary(b"\x01")}],
        msg="$out should reject Binary argument",
        error_code=OUT_ARGUMENT_TYPE_ERROR,
    ),
    OutTestCase(
        "arg_type_objectid",
        docs=[{"_id": 1}],
        pipeline=[{"$out": ObjectId("507f1f77bcf86cd799439011")}],
        msg="$out should reject ObjectId argument",
        error_code=OUT_ARGUMENT_TYPE_ERROR,
    ),
    OutTestCase(
        "arg_type_datetime",
        docs=[{"_id": 1}],
        pipeline=[{"$out": datetime(2024, 1, 1)}],
        msg="$out should reject datetime argument",
        error_code=OUT_ARGUMENT_TYPE_ERROR,
    ),
    OutTestCase(
        "arg_type_regex",
        docs=[{"_id": 1}],
        pipeline=[{"$out": Regex("abc")}],
        msg="$out should reject Regex argument",
        error_code=OUT_ARGUMENT_TYPE_ERROR,
    ),
    OutTestCase(
        "arg_type_timestamp",
        docs=[{"_id": 1}],
        pipeline=[{"$out": Timestamp(1, 1)}],
        msg="$out should reject Timestamp argument",
        error_code=OUT_ARGUMENT_TYPE_ERROR,
    ),
    OutTestCase(
        "arg_type_minkey",
        docs=[{"_id": 1}],
        pipeline=[{"$out": MinKey()}],
        msg="$out should reject MinKey argument",
        error_code=OUT_ARGUMENT_TYPE_ERROR,
    ),
    OutTestCase(
        "arg_type_maxkey",
        docs=[{"_id": 1}],
        pipeline=[{"$out": MaxKey()}],
        msg="$out should reject MaxKey argument",
        error_code=OUT_ARGUMENT_TYPE_ERROR,
    ),
    OutTestCase(
        "arg_type_code",
        docs=[{"_id": 1}],
        pipeline=[{"$out": Code("function() {}")}],
        msg="$out should reject Code argument",
        error_code=OUT_ARGUMENT_TYPE_ERROR,
    ),
    OutTestCase(
        "arg_type_code_with_scope",
        docs=[{"_id": 1}],
        pipeline=[{"$out": Code("function() {}", {"x": 1})}],
        msg="$out should reject Code with scope argument",
        error_code=OUT_ARGUMENT_TYPE_ERROR,
    ),
    OutTestCase(
        "arg_type_array_empty",
        docs=[{"_id": 1}],
        pipeline=[{"$out": []}],
        msg="$out should reject empty array argument",
        error_code=OUT_ARGUMENT_TYPE_ERROR,
    ),
    OutTestCase(
        "arg_type_array_of_string",
        docs=[{"_id": 1}],
        pipeline=[{"$out": ["target"]}],
        msg="$out should reject array containing a string",
        error_code=OUT_ARGUMENT_TYPE_ERROR,
    ),
    OutTestCase(
        "arg_type_array_of_document",
        docs=[{"_id": 1}],
        pipeline=[{"$out": [{"db": "test", "coll": "target"}]}],
        msg="$out should reject array containing a document",
        error_code=OUT_ARGUMENT_TYPE_ERROR,
    ),
    OutTestCase(
        "arg_type_array_nested",
        docs=[{"_id": 1}],
        pipeline=[{"$out": [[1, 2]]}],
        msg="$out should reject nested array argument",
        error_code=OUT_ARGUMENT_TYPE_ERROR,
    ),
    OutTestCase(
        "arg_type_array_mixed_types",
        docs=[{"_id": 1}],
        pipeline=[{"$out": [1, "a", None]}],
        msg="$out should reject array with mixed element types",
        error_code=OUT_ARGUMENT_TYPE_ERROR,
    ),
]

# Property [Document Form Field Type Errors]: non-string types for db or
# coll in document form produce a type mismatch error, with db checked
# before coll when both have type errors.
OUT_DOCUMENT_FIELD_TYPE_ERROR_TESTS: list[OutTestCase] = [
    OutTestCase(
        "db_type_int32",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": 42, "coll": "target"}}],
        msg="$out should reject int32 db as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "db_type_bool",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": True, "coll": "target"}}],
        msg="$out should reject bool db as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "db_type_array",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": ["test"], "coll": "target"}}],
        msg="$out should reject array db as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "db_type_object",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": {"x": 1}, "coll": "target"}}],
        msg="$out should reject object db as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "db_type_int64",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": Int64(1), "coll": "target"}}],
        msg="$out should reject Int64 db as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "db_type_double",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": 1.0, "coll": "target"}}],
        msg="$out should reject double db as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "db_type_decimal128",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": Decimal128("1"), "coll": "target"}}],
        msg="$out should reject Decimal128 db as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "db_type_objectid",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": ObjectId(), "coll": "target"}}],
        msg="$out should reject ObjectId db as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "db_type_datetime",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": datetime(2024, 1, 1), "coll": "target"}}],
        msg="$out should reject datetime db as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "db_type_binary",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": Binary(b"\x01"), "coll": "target"}}],
        msg="$out should reject Binary db as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "db_type_regex",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": Regex("abc"), "coll": "target"}}],
        msg="$out should reject Regex db as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "db_type_timestamp",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": Timestamp(1, 1), "coll": "target"}}],
        msg="$out should reject Timestamp db as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "db_type_minkey",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": MinKey(), "coll": "target"}}],
        msg="$out should reject MinKey db as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "db_type_maxkey",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": MaxKey(), "coll": "target"}}],
        msg="$out should reject MaxKey db as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "db_type_code",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": Code("function() {}"), "coll": "target"}}],
        msg="$out should reject Code db as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "db_type_code_with_scope",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": Code("function() {}", {"x": 1}), "coll": "target"}}],
        msg="$out should reject Code with scope db as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "coll_type_int32",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": 42}}],
        msg="$out should reject int32 coll as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "coll_type_bool",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": True}}],
        msg="$out should reject bool coll as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "coll_type_array",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": ["target"]}}],
        msg="$out should reject array coll as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "coll_type_object",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": {"x": 1}}}],
        msg="$out should reject object coll as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "coll_type_int64",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": Int64(1)}}],
        msg="$out should reject Int64 coll as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "coll_type_double",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": 1.0}}],
        msg="$out should reject double coll as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "coll_type_decimal128",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": Decimal128("1")}}],
        msg="$out should reject Decimal128 coll as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "coll_type_objectid",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": ObjectId()}}],
        msg="$out should reject ObjectId coll as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "coll_type_datetime",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": datetime(2024, 1, 1)}}],
        msg="$out should reject datetime coll as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "coll_type_binary",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": Binary(b"\x01")}}],
        msg="$out should reject Binary coll as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "coll_type_regex",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": Regex("abc")}}],
        msg="$out should reject Regex coll as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "coll_type_timestamp",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": Timestamp(1, 1)}}],
        msg="$out should reject Timestamp coll as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "coll_type_minkey",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": MinKey()}}],
        msg="$out should reject MinKey coll as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "coll_type_maxkey",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": MaxKey()}}],
        msg="$out should reject MaxKey coll as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "coll_type_code",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": Code("function() {}")}}],
        msg="$out should reject Code coll as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "coll_type_code_with_scope",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": Code("function() {}", {"x": 1})}}],
        msg="$out should reject Code with scope coll as a type mismatch",
        error_code=TYPE_MISMATCH_ERROR,
    ),
]

# Property [Document Form Unknown Fields]: any field other than db, coll,
# and timeseries in the document form is rejected as an unknown field, and
# field name matching is case-sensitive and whitespace-sensitive.
OUT_UNKNOWN_FIELD_ERROR_TESTS: list[OutTestCase] = [
    OutTestCase(
        "unknown_field",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": "target", "extra": "x"}}],
        msg="$out should reject unknown field 'extra' in document form",
        error_code=UNRECOGNIZED_COMMAND_FIELD_ERROR,
    ),
    OutTestCase(
        "unknown_field_case_sensitive_db",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"Db": "__DB__", "coll": "target"}}],
        msg="$out should reject 'Db' as unknown (case-sensitive)",
        error_code=UNRECOGNIZED_COMMAND_FIELD_ERROR,
    ),
    OutTestCase(
        "unknown_field_case_sensitive_coll",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "Coll": "target"}}],
        msg="$out should reject 'Coll' as unknown (case-sensitive)",
        error_code=UNRECOGNIZED_COMMAND_FIELD_ERROR,
    ),
    OutTestCase(
        "unknown_field_case_sensitive_timeseries",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": "target", "Timeseries": {"timeField": "ts"}}}],
        msg="$out should reject 'Timeseries' as unknown (case-sensitive)",
        error_code=UNRECOGNIZED_COMMAND_FIELD_ERROR,
    ),
    OutTestCase(
        "unknown_field_whitespace_sensitive_db",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db ": "__DB__", "coll": "target"}}],
        msg="$out should reject 'db ' as unknown (whitespace-sensitive)",
        error_code=UNRECOGNIZED_COMMAND_FIELD_ERROR,
    ),
    OutTestCase(
        "unknown_field_whitespace_sensitive_coll",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", " coll": "target"}}],
        msg="$out should reject ' coll' as unknown (whitespace-sensitive)",
        error_code=UNRECOGNIZED_COMMAND_FIELD_ERROR,
    ),
    OutTestCase(
        "expression_like_object",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": "target", "$expr": {"$literal": 1}}}],
        msg="$out should treat expression-like objects as unknown fields",
        error_code=UNRECOGNIZED_COMMAND_FIELD_ERROR,
    ),
    OutTestCase(
        "expression_like_dollar_prefix",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": "target", "$merge": "x"}}],
        msg="$out should treat $-prefixed fields as unknown fields",
        error_code=UNRECOGNIZED_COMMAND_FIELD_ERROR,
    ),
]

# Property [Collection Name Validation Errors]: invalid collection names
# are rejected with the appropriate error code based on the violation type,
# with namespace errors taking precedence over illegal operation errors.
OUT_COLLECTION_NAME_VALIDATION_ERROR_TESTS: list[OutTestCase] = [
    OutTestCase(
        "coll_empty_string",
        docs=[{"_id": 1}],
        pipeline=[{"$out": ""}],
        msg="$out should reject empty string collection name as invalid namespace",
        error_code=INVALID_NAMESPACE_ERROR,
    ),
    OutTestCase(
        "coll_null_byte",
        docs=[{"_id": 1}],
        pipeline=[{"$out": "test\x00coll"}],
        msg="$out should reject collection name containing null byte as invalid namespace",
        error_code=INVALID_NAMESPACE_ERROR,
    ),
    OutTestCase(
        "coll_leading_dot",
        docs=[{"_id": 1}],
        pipeline=[{"$out": ".leading"}],
        msg="$out should reject collection name with leading dot as invalid namespace",
        error_code=INVALID_NAMESPACE_ERROR,
    ),
    OutTestCase(
        "coll_system_prefix",
        docs=[{"_id": 1}],
        pipeline=[{"$out": "system.test"}],
        msg="$out should reject system. prefix collection name as unauthorized",
        error_code=UNAUTHORIZED_ERROR,
    ),
    OutTestCase(
        "coll_system_buckets_prefix",
        docs=[{"_id": 1}],
        pipeline=[{"$out": "system.buckets.test"}],
        msg="$out should reject system.buckets. prefix as a special collection",
        error_code=OUT_SPECIAL_COLLECTION_ERROR,
    ),
    OutTestCase(
        "coll_dollar_prefix",
        docs=[{"_id": 1}],
        pipeline=[{"$out": "$name"}],
        msg="$out should reject dollar-prefixed collection name as illegal operation",
        error_code=ILLEGAL_OPERATION_ERROR,
    ),
    OutTestCase(
        "coll_double_dollar_prefix",
        docs=[{"_id": 1}],
        pipeline=[{"$out": "$$name"}],
        msg="$out should reject double-dollar-prefixed collection name as illegal operation",
        error_code=ILLEGAL_OPERATION_ERROR,
    ),
    OutTestCase(
        "coll_bare_dollar",
        docs=[{"_id": 1}],
        pipeline=[{"$out": "$"}],
        msg="$out should reject bare dollar collection name as illegal operation",
        error_code=ILLEGAL_OPERATION_ERROR,
    ),
    OutTestCase(
        "coll_bare_double_dollar",
        docs=[{"_id": 1}],
        pipeline=[{"$out": "$$"}],
        msg="$out should reject bare double-dollar collection name as illegal operation",
        error_code=ILLEGAL_OPERATION_ERROR,
    ),
    OutTestCase(
        "coll_namespace_exceeds_255_bytes",
        docs=[{"_id": 1}],
        pipeline=[{"$out": "a" * 255}],
        msg="$out should reject namespace exceeding 255 bytes as illegal operation",
        error_code=ILLEGAL_OPERATION_ERROR,
    ),
]

# Property [Database Name Validation Errors]: invalid database names
# containing dots, slashes, backslashes, ASCII spaces, null bytes, dollar
# prefixes, or empty strings are rejected as invalid namespaces.
OUT_DATABASE_NAME_VALIDATION_ERROR_TESTS: list[OutTestCase] = [
    OutTestCase(
        "db_empty_string",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "", "coll": "target"}}],
        msg="$out should reject empty string database name",
        error_code=INVALID_NAMESPACE_ERROR,
    ),
    OutTestCase(
        "db_leading_dot",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": ".leading", "coll": "target"}}],
        msg="$out should reject database name with leading dot",
        error_code=INVALID_NAMESPACE_ERROR,
    ),
    OutTestCase(
        "db_middle_dot",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "a.b", "coll": "target"}}],
        msg="$out should reject database name with middle dot",
        error_code=INVALID_NAMESPACE_ERROR,
    ),
    OutTestCase(
        "db_trailing_dot",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "trailing.", "coll": "target"}}],
        msg="$out should reject database name with trailing dot",
        error_code=INVALID_NAMESPACE_ERROR,
    ),
    OutTestCase(
        "db_dollar_prefix",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "$name", "coll": "target"}}],
        msg="$out should reject dollar-prefixed database name",
        error_code=INVALID_NAMESPACE_ERROR,
    ),
    OutTestCase(
        "db_bare_dollar",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "$", "coll": "target"}}],
        msg="$out should reject bare dollar database name",
        error_code=INVALID_NAMESPACE_ERROR,
    ),
    OutTestCase(
        "db_bare_double_dollar",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "$$", "coll": "target"}}],
        msg="$out should reject bare double-dollar database name",
        error_code=INVALID_NAMESPACE_ERROR,
    ),
    OutTestCase(
        "db_null_byte",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "test\x00db", "coll": "target"}}],
        msg="$out should reject database name containing null byte",
        error_code=INVALID_NAMESPACE_ERROR,
    ),
    OutTestCase(
        "db_slash",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "a/b", "coll": "target"}}],
        msg="$out should reject database name containing slash",
        error_code=INVALID_NAMESPACE_ERROR,
    ),
    OutTestCase(
        "db_backslash",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "a\\b", "coll": "target"}}],
        msg="$out should reject database name containing backslash",
        error_code=INVALID_NAMESPACE_ERROR,
    ),
    OutTestCase(
        "db_ascii_space",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": " ", "coll": "target"}}],
        msg="$out should reject database name that is a single ASCII space",
        error_code=INVALID_NAMESPACE_ERROR,
    ),
    OutTestCase(
        "db_ascii_space_mixed",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "a b", "coll": "target"}}],
        msg="$out should reject database name containing an ASCII space",
        error_code=INVALID_NAMESPACE_ERROR,
    ),
]

# Property [Restricted Database Errors]: writing to the reserved system
# databases (admin, config, local) produces a restricted database error.
OUT_RESTRICTED_DATABASE_ERROR_TESTS: list[OutTestCase] = [
    OutTestCase(
        "restricted_db_admin",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "admin", "coll": "target"}}],
        msg="$out should reject writing to the admin database",
        error_code=OUT_RESTRICTED_DATABASE_ERROR,
    ),
    OutTestCase(
        "restricted_db_config",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "config", "coll": "target"}}],
        msg="$out should reject writing to the config database",
        error_code=OUT_RESTRICTED_DATABASE_ERROR,
    ),
    OutTestCase(
        "restricted_db_local",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "local", "coll": "target"}}],
        msg="$out should reject writing to the local database",
        error_code=OUT_RESTRICTED_DATABASE_ERROR,
    ),
]

# Property [Pipeline Position Errors]: $out must be the last stage in a
# pipeline - placing it before another stage, duplicating it, or combining
# it with $merge in either order produces a pipeline position error.
OUT_PIPELINE_POSITION_ERROR_TESTS: list[OutTestCase] = [
    OutTestCase(
        "out_not_last_stage",
        docs=[{"_id": 1}],
        pipeline=[{"$out": "target"}, {"$match": {"_id": 1}}],
        msg="$out not as the last stage should produce a pipeline position error",
        error_code=OUT_NOT_LAST_STAGE_ERROR,
    ),
    OutTestCase(
        "two_out_stages",
        docs=[{"_id": 1}],
        pipeline=[{"$out": "target1"}, {"$out": "target2"}],
        msg="Two $out stages in the same pipeline should produce a pipeline position error",
        error_code=OUT_NOT_LAST_STAGE_ERROR,
    ),
    OutTestCase(
        "out_then_merge",
        docs=[{"_id": 1}],
        pipeline=[{"$out": "target"}, {"$merge": {"into": "target2"}}],
        msg="$out followed by $merge should produce a pipeline position error",
        error_code=OUT_NOT_LAST_STAGE_ERROR,
    ),
    OutTestCase(
        "merge_then_out",
        docs=[{"_id": 1}],
        pipeline=[{"$merge": {"into": "target"}}, {"$out": "target2"}],
        msg="$merge followed by $out should produce a pipeline position error",
        error_code=OUT_NOT_LAST_STAGE_ERROR,
    ),
]

# Property [Nested Pipeline Restriction Errors]: $out is not allowed inside
# nested pipelines ($lookup, $facet, $unionWith) or in view definitions, and
# the innermost nesting restriction applies when stages are nested.
OUT_NESTED_PIPELINE_RESTRICTION_ERROR_TESTS: list[OutTestCase] = [
    OutTestCase(
        "out_inside_lookup",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$lookup": {
                    "from": "other",
                    "pipeline": [{"$out": "target"}],
                    "as": "result",
                }
            }
        ],
        msg="$out inside a $lookup nested pipeline should be rejected",
        error_code=LOOKUP_OUT_NOT_ALLOWED_ERROR,
    ),
    OutTestCase(
        "out_inside_facet",
        docs=[{"_id": 1}],
        pipeline=[{"$facet": {"branch": [{"$out": "target"}]}}],
        msg="$out inside a $facet nested pipeline should be rejected",
        error_code=FACET_OUT_NOT_ALLOWED_ERROR,
    ),
    OutTestCase(
        "out_inside_union_with",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$unionWith": {
                    "coll": "other",
                    "pipeline": [{"$out": "target"}],
                }
            }
        ],
        msg="$out inside a $unionWith nested pipeline should be rejected",
        error_code=UNION_WITH_OUT_NOT_ALLOWED_ERROR,
    ),
    OutTestCase(
        "out_inside_lookup_inside_facet",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$facet": {
                    "branch": [
                        {
                            "$lookup": {
                                "from": "other",
                                "pipeline": [{"$out": "target"}],
                                "as": "r",
                            }
                        }
                    ]
                }
            }
        ],
        msg=(
            "$out nested inside $lookup inside $facet should produce the"
            " innermost nesting error ($lookup restriction)"
        ),
        error_code=LOOKUP_OUT_NOT_ALLOWED_ERROR,
    ),
]

# Property [Timeseries Field Type Errors]: all timeseries sub-fields reject
# non-accepted types with a type mismatch error - timeseries accepts only
# object, timeField/metaField/granularity accept only string, and
# bucketMaxSpanSeconds/bucketRoundingSeconds accept only numeric types
# (int32, Int64, float, Decimal128).
OUT_TIMESERIES_FIELD_TYPE_ERROR_TESTS: list[OutTestCase] = [
    OutTestCase(
        "ts_field_type_int32",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": "target", "timeseries": 42}}],
        msg="$out should reject int32 as timeseries field type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_field_type_int64",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": "target", "timeseries": Int64(42)}}],
        msg="$out should reject int64 as timeseries field type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_field_type_float",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": "target", "timeseries": 3.14}}],
        msg="$out should reject float as timeseries field type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_field_type_decimal128",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": "target", "timeseries": Decimal128("99.9")}}],
        msg="$out should reject decimal128 as timeseries field type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_field_type_bool",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": "target", "timeseries": True}}],
        msg="$out should reject bool as timeseries field type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_field_type_string",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": "target", "timeseries": "invalid"}}],
        msg="$out should reject string as timeseries field type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_field_type_array_empty",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": "target", "timeseries": []}}],
        msg="$out should reject array_empty as timeseries field type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_field_type_array_with_object",
        docs=[{"_id": 1}],
        pipeline=[
            {"$out": {"db": "__DB__", "coll": "target", "timeseries": [{"timeField": "ts"}]}}
        ],
        msg="$out should reject array_with_object as timeseries field type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_field_type_binary",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": "target", "timeseries": Binary(b"\x01")}}],
        msg="$out should reject binary as timeseries field type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_field_type_objectid",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": ObjectId("507f1f77bcf86cd799439011"),
                }
            }
        ],
        msg="$out should reject objectid as timeseries field type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_field_type_datetime",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": "target", "timeseries": datetime(2024, 1, 1)}}],
        msg="$out should reject datetime as timeseries field type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_field_type_regex",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": "target", "timeseries": Regex("abc")}}],
        msg="$out should reject regex as timeseries field type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_field_type_timestamp",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": "target", "timeseries": Timestamp(1, 1)}}],
        msg="$out should reject timestamp as timeseries field type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_field_type_minkey",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": "target", "timeseries": MinKey()}}],
        msg="$out should reject minkey as timeseries field type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_field_type_maxkey",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": "target", "timeseries": MaxKey()}}],
        msg="$out should reject maxkey as timeseries field type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_field_type_code",
        docs=[{"_id": 1}],
        pipeline=[
            {"$out": {"db": "__DB__", "coll": "target", "timeseries": Code("function() {}")}}
        ],
        msg="$out should reject code as timeseries field type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_field_type_code_with_scope",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": Code("function() {}", {"x": 1}),
                }
            }
        ],
        msg="$out should reject code_with_scope as timeseries field type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_time_field_type_int32",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": "target", "timeseries": {"timeField": 42}}}],
        msg="$out should reject int32 as timeField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_time_field_type_int64",
        docs=[{"_id": 1}],
        pipeline=[
            {"$out": {"db": "__DB__", "coll": "target", "timeseries": {"timeField": Int64(42)}}}
        ],
        msg="$out should reject int64 as timeField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_time_field_type_float",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": "target", "timeseries": {"timeField": 3.14}}}],
        msg="$out should reject float as timeField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_time_field_type_decimal128",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": Decimal128("99.9")},
                }
            }
        ],
        msg="$out should reject decimal128 as timeField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_time_field_type_bool",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": "target", "timeseries": {"timeField": True}}}],
        msg="$out should reject bool as timeField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_time_field_type_array_with_object",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": [{"timeField": "ts"}]},
                }
            }
        ],
        msg="$out should reject array_with_object as timeField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_time_field_type_binary",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": Binary(b"\x01")},
                }
            }
        ],
        msg="$out should reject binary as timeField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_time_field_type_objectid",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": ObjectId("507f1f77bcf86cd799439011")},
                }
            }
        ],
        msg="$out should reject objectid as timeField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_time_field_type_datetime",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": datetime(2024, 1, 1)},
                }
            }
        ],
        msg="$out should reject datetime as timeField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_time_field_type_regex",
        docs=[{"_id": 1}],
        pipeline=[
            {"$out": {"db": "__DB__", "coll": "target", "timeseries": {"timeField": Regex("abc")}}}
        ],
        msg="$out should reject regex as timeField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_time_field_type_timestamp",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": Timestamp(1, 1)},
                }
            }
        ],
        msg="$out should reject timestamp as timeField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_time_field_type_minkey",
        docs=[{"_id": 1}],
        pipeline=[
            {"$out": {"db": "__DB__", "coll": "target", "timeseries": {"timeField": MinKey()}}}
        ],
        msg="$out should reject minkey as timeField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_time_field_type_maxkey",
        docs=[{"_id": 1}],
        pipeline=[
            {"$out": {"db": "__DB__", "coll": "target", "timeseries": {"timeField": MaxKey()}}}
        ],
        msg="$out should reject maxkey as timeField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_time_field_type_code",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": Code("function() {}")},
                }
            }
        ],
        msg="$out should reject code as timeField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_time_field_type_code_with_scope",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": Code("function() {}", {"x": 1})},
                }
            }
        ],
        msg="$out should reject code_with_scope as timeField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_time_field_type_object",
        docs=[{"_id": 1}],
        pipeline=[
            {"$out": {"db": "__DB__", "coll": "target", "timeseries": {"timeField": {"x": 1}}}}
        ],
        msg="$out should reject object as timeField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_meta_field_type_int32",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "metaField": 42},
                }
            }
        ],
        msg="$out should reject int32 as metaField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_meta_field_type_int64",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "metaField": Int64(42)},
                }
            }
        ],
        msg="$out should reject int64 as metaField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_meta_field_type_float",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "metaField": 3.14},
                }
            }
        ],
        msg="$out should reject float as metaField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_meta_field_type_decimal128",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "metaField": Decimal128("99.9")},
                }
            }
        ],
        msg="$out should reject decimal128 as metaField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_meta_field_type_bool",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "metaField": True},
                }
            }
        ],
        msg="$out should reject bool as metaField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_meta_field_type_array_with_object",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "metaField": [{"timeField": "ts"}]},
                }
            }
        ],
        msg="$out should reject array_with_object as metaField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_meta_field_type_binary",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "metaField": Binary(b"\x01")},
                }
            }
        ],
        msg="$out should reject binary as metaField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_meta_field_type_objectid",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "metaField": ObjectId("507f1f77bcf86cd799439011"),
                    },
                }
            }
        ],
        msg="$out should reject objectid as metaField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_meta_field_type_datetime",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "metaField": datetime(2024, 1, 1)},
                }
            }
        ],
        msg="$out should reject datetime as metaField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_meta_field_type_regex",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "metaField": Regex("abc")},
                }
            }
        ],
        msg="$out should reject regex as metaField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_meta_field_type_timestamp",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "metaField": Timestamp(1, 1)},
                }
            }
        ],
        msg="$out should reject timestamp as metaField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_meta_field_type_minkey",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "metaField": MinKey()},
                }
            }
        ],
        msg="$out should reject minkey as metaField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_meta_field_type_maxkey",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "metaField": MaxKey()},
                }
            }
        ],
        msg="$out should reject maxkey as metaField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_meta_field_type_code",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "metaField": Code("function() {}")},
                }
            }
        ],
        msg="$out should reject code as metaField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_meta_field_type_code_with_scope",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "metaField": Code("function() {}", {"x": 1})},
                }
            }
        ],
        msg="$out should reject code_with_scope as metaField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_meta_field_type_object",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "metaField": {"x": 1}},
                }
            }
        ],
        msg="$out should reject object as metaField type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_granularity_type_int32",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "granularity": 42},
                }
            }
        ],
        msg="$out should reject int32 as granularity type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_granularity_type_int64",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "granularity": Int64(42)},
                }
            }
        ],
        msg="$out should reject int64 as granularity type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_granularity_type_float",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "granularity": 3.14},
                }
            }
        ],
        msg="$out should reject float as granularity type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_granularity_type_decimal128",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "granularity": Decimal128("99.9")},
                }
            }
        ],
        msg="$out should reject decimal128 as granularity type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_granularity_type_bool",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "granularity": True},
                }
            }
        ],
        msg="$out should reject bool as granularity type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_granularity_type_array_with_object",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "granularity": [{"timeField": "ts"}]},
                }
            }
        ],
        msg="$out should reject array_with_object as granularity type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_granularity_type_binary",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "granularity": Binary(b"\x01")},
                }
            }
        ],
        msg="$out should reject binary as granularity type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_granularity_type_objectid",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "granularity": ObjectId("507f1f77bcf86cd799439011"),
                    },
                }
            }
        ],
        msg="$out should reject objectid as granularity type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_granularity_type_datetime",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "granularity": datetime(2024, 1, 1)},
                }
            }
        ],
        msg="$out should reject datetime as granularity type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_granularity_type_regex",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "granularity": Regex("abc")},
                }
            }
        ],
        msg="$out should reject regex as granularity type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_granularity_type_timestamp",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "granularity": Timestamp(1, 1)},
                }
            }
        ],
        msg="$out should reject timestamp as granularity type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_granularity_type_minkey",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "granularity": MinKey()},
                }
            }
        ],
        msg="$out should reject minkey as granularity type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_granularity_type_maxkey",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "granularity": MaxKey()},
                }
            }
        ],
        msg="$out should reject maxkey as granularity type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_granularity_type_code",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "granularity": Code("function() {}")},
                }
            }
        ],
        msg="$out should reject code as granularity type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_granularity_type_code_with_scope",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "granularity": Code("function() {}", {"x": 1}),
                    },
                }
            }
        ],
        msg="$out should reject code_with_scope as granularity type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_granularity_type_object",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "granularity": {"x": 1}},
                }
            }
        ],
        msg="$out should reject object as granularity type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_bucket_max_type_bool",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketMaxSpanSeconds": True,
                        "bucketRoundingSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject bool as bucketMaxSpanSeconds type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_bucket_max_type_string",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketMaxSpanSeconds": "invalid",
                        "bucketRoundingSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject string as bucketMaxSpanSeconds type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_bucket_max_type_array_with_object",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketMaxSpanSeconds": [{"timeField": "ts"}],
                        "bucketRoundingSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject array_with_object as bucketMaxSpanSeconds type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_bucket_max_type_binary",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketMaxSpanSeconds": Binary(b"\x01"),
                        "bucketRoundingSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject binary as bucketMaxSpanSeconds type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_bucket_max_type_objectid",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketMaxSpanSeconds": ObjectId("507f1f77bcf86cd799439011"),
                        "bucketRoundingSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject objectid as bucketMaxSpanSeconds type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_bucket_max_type_datetime",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketMaxSpanSeconds": datetime(2024, 1, 1),
                        "bucketRoundingSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject datetime as bucketMaxSpanSeconds type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_bucket_max_type_regex",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketMaxSpanSeconds": Regex("abc"),
                        "bucketRoundingSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject regex as bucketMaxSpanSeconds type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_bucket_max_type_timestamp",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketMaxSpanSeconds": Timestamp(1, 1),
                        "bucketRoundingSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject timestamp as bucketMaxSpanSeconds type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_bucket_max_type_minkey",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketMaxSpanSeconds": MinKey(),
                        "bucketRoundingSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject minkey as bucketMaxSpanSeconds type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_bucket_max_type_maxkey",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketMaxSpanSeconds": MaxKey(),
                        "bucketRoundingSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject maxkey as bucketMaxSpanSeconds type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_bucket_max_type_code",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketMaxSpanSeconds": Code("function() {}"),
                        "bucketRoundingSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject code as bucketMaxSpanSeconds type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_bucket_max_type_code_with_scope",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketMaxSpanSeconds": Code("function() {}", {"x": 1}),
                        "bucketRoundingSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject code_with_scope as bucketMaxSpanSeconds type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_bucket_max_type_object",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketMaxSpanSeconds": {"x": 1},
                        "bucketRoundingSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject object as bucketMaxSpanSeconds type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_bucket_round_type_bool",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketRoundingSeconds": True,
                        "bucketMaxSpanSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject bool as bucketRoundingSeconds type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_bucket_round_type_string",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketRoundingSeconds": "invalid",
                        "bucketMaxSpanSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject string as bucketRoundingSeconds type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_bucket_round_type_array_with_object",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketRoundingSeconds": [{"timeField": "ts"}],
                        "bucketMaxSpanSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject array_with_object as bucketRoundingSeconds type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_bucket_round_type_binary",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketRoundingSeconds": Binary(b"\x01"),
                        "bucketMaxSpanSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject binary as bucketRoundingSeconds type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_bucket_round_type_objectid",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketRoundingSeconds": ObjectId("507f1f77bcf86cd799439011"),
                        "bucketMaxSpanSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject objectid as bucketRoundingSeconds type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_bucket_round_type_datetime",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketRoundingSeconds": datetime(2024, 1, 1),
                        "bucketMaxSpanSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject datetime as bucketRoundingSeconds type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_bucket_round_type_regex",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketRoundingSeconds": Regex("abc"),
                        "bucketMaxSpanSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject regex as bucketRoundingSeconds type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_bucket_round_type_timestamp",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketRoundingSeconds": Timestamp(1, 1),
                        "bucketMaxSpanSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject timestamp as bucketRoundingSeconds type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_bucket_round_type_minkey",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketRoundingSeconds": MinKey(),
                        "bucketMaxSpanSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject minkey as bucketRoundingSeconds type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_bucket_round_type_maxkey",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketRoundingSeconds": MaxKey(),
                        "bucketMaxSpanSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject maxkey as bucketRoundingSeconds type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_bucket_round_type_code",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketRoundingSeconds": Code("function() {}"),
                        "bucketMaxSpanSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject code as bucketRoundingSeconds type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_bucket_round_type_code_with_scope",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketRoundingSeconds": Code("function() {}", {"x": 1}),
                        "bucketMaxSpanSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject code_with_scope as bucketRoundingSeconds type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "ts_bucket_round_type_object",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketRoundingSeconds": {"x": 1},
                        "bucketMaxSpanSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject object as bucketRoundingSeconds type",
        error_code=TYPE_MISMATCH_ERROR,
    ),
]

# Property [Timeseries Missing and Unknown Field Errors]: missing timeField
# inside the timeseries document produces a missing key error, and unknown
# fields inside the timeseries document produce an unrecognized field error.
OUT_TIMESERIES_MISSING_UNKNOWN_FIELD_ERROR_TESTS: list[OutTestCase] = [
    OutTestCase(
        "ts_missing_time_field_empty_ts",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "coll": "target", "timeseries": {}}}],
        msg="$out should reject an empty timeseries document (missing timeField)",
        error_code=NO_SUCH_KEY_ERROR,
    ),
    OutTestCase(
        "ts_missing_time_field_with_meta",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"metaField": "m"},
                }
            }
        ],
        msg="$out should reject timeseries with metaField but missing timeField",
        error_code=NO_SUCH_KEY_ERROR,
    ),
    OutTestCase(
        "ts_unknown_field",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "extra": "x"},
                }
            }
        ],
        msg="$out should reject unknown field inside timeseries document",
        error_code=UNRECOGNIZED_COMMAND_FIELD_ERROR,
    ),
    OutTestCase(
        "ts_unknown_field_case_sensitive",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "TimeField": "ts2"},
                }
            }
        ],
        msg="$out should reject case-variant field name inside timeseries as unknown",
        error_code=UNRECOGNIZED_COMMAND_FIELD_ERROR,
    ),
]

# Property [Timeseries Granularity Errors]: invalid granularity strings
# produce error code 2 because validation is case-sensitive and only
# "seconds", "minutes", and "hours" are accepted.
OUT_TIMESERIES_GRANULARITY_ERROR_TESTS: list[OutTestCase] = [
    OutTestCase(
        "granularity_capitalized",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "granularity": "Seconds"},
                }
            }
        ],
        msg="$out should reject capitalized 'Seconds' as an invalid granularity",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "granularity_all_caps",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "granularity": "HOURS"},
                }
            }
        ],
        msg="$out should reject all-caps 'HOURS' as an invalid granularity",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "granularity_empty_string",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "granularity": ""},
                }
            }
        ],
        msg="$out should reject empty string as an invalid granularity",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "granularity_arbitrary_string",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "granularity": "invalid"},
                }
            }
        ],
        msg="$out should reject an arbitrary string as an invalid granularity",
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "granularity_singular_form",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": "ts", "granularity": "second"},
                }
            }
        ],
        msg="$out should reject singular form 'second' as an invalid granularity",
        error_code=BAD_VALUE_ERROR,
    ),
]

# Property [Bucket Param Pairing Errors]: bucketMaxSpanSeconds and
# bucketRoundingSeconds must be specified together, must be equal, and
# cannot be combined with granularity.
OUT_BUCKET_PARAM_PAIRING_ERROR_TESTS: list[OutTestCase] = [
    OutTestCase(
        "bucket_max_without_rounding",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketMaxSpanSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject bucketMaxSpanSeconds without bucketRoundingSeconds",
        error_code=INVALID_OPTIONS_ERROR,
    ),
    OutTestCase(
        "bucket_rounding_without_max",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketRoundingSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject bucketRoundingSeconds without bucketMaxSpanSeconds",
        error_code=INVALID_OPTIONS_ERROR,
    ),
    OutTestCase(
        "bucket_params_not_equal",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "bucketMaxSpanSeconds": 100,
                        "bucketRoundingSeconds": 200,
                    },
                }
            }
        ],
        msg="$out should reject unequal bucketMaxSpanSeconds and bucketRoundingSeconds",
        error_code=INVALID_OPTIONS_ERROR,
    ),
    OutTestCase(
        "granularity_with_bucket_params",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {
                        "timeField": "ts",
                        "granularity": "seconds",
                        "bucketMaxSpanSeconds": 100,
                        "bucketRoundingSeconds": 100,
                    },
                }
            }
        ],
        msg="$out should reject granularity combined with bucket parameters",
        error_code=INVALID_OPTIONS_ERROR,
    ),
]

# Property [Timeseries Document Errors]: $out fails with error code 2
# when writing a document whose timeField value is not a valid datetime or
# when the timeField is missing entirely.
OUT_TIMESERIES_DOCUMENT_ERROR_TESTS: list[OutTestCase] = [
    OutTestCase(
        "ts_doc_non_date_time_field",
        docs=[{"_id": 1, "ts": "not_a_date", "v": 1}],
        target_coll="ts_doc_err_nondate",
        out_spec={"timeseries": {"timeField": "ts"}},
        msg=(
            "$out should fail when writing a document with a non-date"
            " value in the timeField to a timeseries collection"
        ),
        error_code=BAD_VALUE_ERROR,
    ),
    OutTestCase(
        "ts_doc_missing_time_field",
        docs=[{"_id": 1, "v": 1}],
        target_coll="ts_doc_err_missing",
        out_spec={"timeseries": {"timeField": "ts"}},
        msg=(
            "$out should fail when writing a document missing the"
            " timeField entirely to a timeseries collection"
        ),
        error_code=BAD_VALUE_ERROR,
    ),
]

# Property [Error Precedence]: validation errors follow a strict
# precedence order where higher-priority checks mask lower-priority ones
# when multiple error conditions are present simultaneously.
OUT_ERROR_PRECEDENCE_TESTS: list[OutTestCase] = [
    OutTestCase(
        "prec_stage_type_over_nested",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$lookup": {
                    "from": "other",
                    "pipeline": [{"$out": 42}],
                    "as": "r",
                }
            }
        ],
        msg=(
            "stage argument type error should take precedence over"
            " nested pipeline restriction error"
        ),
        error_code=OUT_ARGUMENT_TYPE_ERROR,
    ),
    OutTestCase(
        "prec_field_type_over_unknown",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": 42, "coll": "target", "extra": "x"}}],
        msg="field type error should take precedence over unknown field error",
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "prec_unknown_over_missing",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "__DB__", "extra": "x"}}],
        msg="unknown field error should take precedence over missing field error",
        error_code=UNRECOGNIZED_COMMAND_FIELD_ERROR,
    ),
    OutTestCase(
        "prec_missing_over_namespace",
        docs=[{"_id": 1}],
        pipeline=[{"$out": {"db": "", "coll": None}}],
        msg="missing field error should take precedence over namespace validation error",
        error_code=NO_SUCH_KEY_ERROR,
    ),
    OutTestCase(
        "prec_namespace_over_pipeline_position",
        docs=[{"_id": 1}],
        pipeline=[{"$out": ""}, {"$match": {"_id": 1}}],
        msg="namespace validation error should take precedence over position error",
        error_code=INVALID_NAMESPACE_ERROR,
    ),
    OutTestCase(
        "prec_system_prefix_over_pipeline_position",
        docs=[{"_id": 1}],
        pipeline=[{"$out": "system.test"}, {"$match": {"_id": 1}}],
        msg="system prefix error should take precedence over pipeline position error",
        error_code=UNAUTHORIZED_ERROR,
    ),
    OutTestCase(
        "prec_system_buckets_over_pipeline_position",
        docs=[{"_id": 1}],
        pipeline=[{"$out": "system.buckets.test"}, {"$match": {"_id": 1}}],
        msg="system.buckets error should take precedence over pipeline position error",
        error_code=OUT_SPECIAL_COLLECTION_ERROR,
    ),
    OutTestCase(
        "prec_namespace_over_coll_name",
        docs=[{"_id": 1}],
        pipeline=[{"$out": "$test\x00"}],
        msg=(
            "namespace validation error should take precedence"
            " over collection name validation error"
        ),
        error_code=INVALID_NAMESPACE_ERROR,
    ),
    OutTestCase(
        "prec_system_prefix_over_namespace_length",
        docs=[{"_id": 1}],
        pipeline=[{"$out": "system." + "a" * 248}],
        msg="system prefix error should take precedence over namespace length error",
        error_code=UNAUTHORIZED_ERROR,
    ),
    OutTestCase(
        "prec_nested_over_pipeline_position",
        docs=[{"_id": 1}],
        pipeline=[{"$facet": {"branch": [{"$out": "target"}, {"$match": {}}]}}],
        msg=(
            "nested pipeline restriction error should take precedence"
            " over pipeline position error when both apply inside a nested pipeline"
        ),
        error_code=FACET_OUT_NOT_ALLOWED_ERROR,
    ),
    OutTestCase(
        "prec_ts_field_type_over_unknown",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"timeField": 42, "extra": "x"},
                }
            }
        ],
        msg=(
            "timeseries field type error should take precedence"
            " over timeseries unknown field error"
        ),
        error_code=TYPE_MISMATCH_ERROR,
    ),
    OutTestCase(
        "prec_ts_unknown_over_missing",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$out": {
                    "db": "__DB__",
                    "coll": "target",
                    "timeseries": {"extra": "x"},
                }
            }
        ],
        msg=(
            "timeseries unknown field error should take precedence"
            " over timeseries missing required field error"
        ),
        error_code=UNRECOGNIZED_COMMAND_FIELD_ERROR,
    ),
]

OUT_ERROR_TESTS = (
    OUT_BUCKET_PARAM_RANGE_ERROR_TESTS
    + OUT_NULL_MISSING_ERROR_TESTS
    + OUT_STAGE_ARGUMENT_TYPE_ERROR_TESTS
    + OUT_DOCUMENT_FIELD_TYPE_ERROR_TESTS
    + OUT_UNKNOWN_FIELD_ERROR_TESTS
    + OUT_COLLECTION_NAME_VALIDATION_ERROR_TESTS
    + OUT_DATABASE_NAME_VALIDATION_ERROR_TESTS
    + OUT_RESTRICTED_DATABASE_ERROR_TESTS
    + OUT_PIPELINE_POSITION_ERROR_TESTS
    + OUT_NESTED_PIPELINE_RESTRICTION_ERROR_TESTS
    + OUT_TIMESERIES_FIELD_TYPE_ERROR_TESTS
    + OUT_TIMESERIES_MISSING_UNKNOWN_FIELD_ERROR_TESTS
    + OUT_TIMESERIES_GRANULARITY_ERROR_TESTS
    + OUT_BUCKET_PARAM_PAIRING_ERROR_TESTS
    + OUT_TIMESERIES_DOCUMENT_ERROR_TESTS
    + OUT_ERROR_PRECEDENCE_TESTS
)


@pytest.mark.aggregate
@pytest.mark.parametrize("test_case", pytest_params(OUT_ERROR_TESTS))
def test_out_error(collection, test_case: OutTestCase):
    """Test $out rejects invalid configurations with the expected error code."""
    populate_collection(collection, test_case)
    if test_case.pipeline:
        pipeline = _resolve_pipeline(test_case.pipeline, collection.database.name)
    else:
        pipeline = [_build_out_stage(collection, test_case)]
    result = execute_command(
        collection,
        {"aggregate": collection.name, "pipeline": pipeline, "cursor": {}},
    )
    assertResult(result, error_code=test_case.error_code, msg=test_case.msg)


# Property [Target Collection Restriction Errors]: $out rejects writing to
# capped collections and views, and writing to a view with timeseries options
# produces a timeseries collection type error instead of the view-specific
# error.
@pytest.mark.aggregate
def test_out_capped_collection_error(collection):
    """Test $out rejects writing to a capped collection."""
    populate_collection(
        collection,
        StageTestCase(
            id="capped_target",
            docs=[{"_id": 1, "value": 10}],
            msg="$out should reject writing to a capped collection",
        ),
    )
    db = collection.database
    target_name = "capped_out_target"
    db.drop_collection(target_name)
    db.create_collection(target_name, capped=True, size=1_048_576)
    result = execute_command(
        collection,
        {"aggregate": collection.name, "pipeline": [{"$out": target_name}], "cursor": {}},
    )
    assertFailureCode(
        result,
        OUT_CAPPED_COLLECTION_ERROR,
        msg="$out should reject writing to a capped collection",
    )


@pytest.mark.aggregate
def test_out_view_error(collection):
    """Test $out rejects writing to a view."""
    populate_collection(
        collection,
        StageTestCase(
            id="view_target",
            docs=[{"_id": 1, "value": 10}],
            msg="$out should reject writing to a view",
        ),
    )
    db = collection.database
    target_name = "view_out_target"
    db.drop_collection(target_name)
    db.command({"create": target_name, "viewOn": collection.name, "pipeline": []})
    result = execute_command(
        collection,
        {"aggregate": collection.name, "pipeline": [{"$out": target_name}], "cursor": {}},
    )
    assertFailureCode(
        result,
        COMMAND_NOT_SUPPORTED_ON_VIEW_ERROR,
        msg="$out should reject writing to a view",
    )


@pytest.mark.aggregate
def test_out_view_with_timeseries_error(collection):
    """Test $out to a view with timeseries options produces a timeseries error."""
    populate_collection(
        collection,
        StageTestCase(
            id="view_ts_target",
            docs=[{"_id": 1, "value": 10}],
            msg="$out to a view with timeseries should produce a timeseries error",
        ),
    )
    db = collection.database
    target_name = "view_ts_out_target"
    db.drop_collection(target_name)
    db.command({"create": target_name, "viewOn": collection.name, "pipeline": []})
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [
                {
                    "$out": {
                        "db": db.name,
                        "coll": target_name,
                        "timeseries": {"timeField": "ts"},
                    }
                }
            ],
            "cursor": {},
        },
    )
    assertFailureCode(
        result,
        OUT_TIMESERIES_COLLECTION_TYPE_ERROR,
        msg=(
            "$out to a view with timeseries options should produce a timeseries"
            " collection type error, not the view-specific error"
        ),
    )


# Property [Timeseries Existing Collection Errors]: writing with timeseries
# options to an existing regular collection produces a timeseries collection
# type error, and writing with mismatched timeseries options to an existing
# time series collection produces a timeseries options mismatch error
# regardless of which option differs.
@pytest.mark.aggregate
def test_out_timeseries_to_regular_collection_error(collection):
    """Test $out with timeseries options to an existing regular collection fails."""
    db = collection.database
    target_name = "ts_to_regular_target"
    db.drop_collection(target_name)
    db.create_collection(target_name)
    populate_collection(
        collection,
        StageTestCase(
            id="ts_to_regular",
            docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
            msg="$out with timeseries to a regular collection should fail",
        ),
    )
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [
                {
                    "$out": {
                        "db": db.name,
                        "coll": target_name,
                        "timeseries": {"timeField": "ts"},
                    }
                }
            ],
            "cursor": {},
        },
    )
    assertFailureCode(
        result,
        OUT_TIMESERIES_COLLECTION_TYPE_ERROR,
        msg=(
            "$out with timeseries options to an existing regular collection"
            " should produce a timeseries collection type error"
        ),
    )


@pytest.mark.aggregate
@pytest.mark.parametrize(
    "existing_opts,mismatched_opts",
    [
        pytest.param(
            {"timeField": "ts"},
            {"timeField": "other"},
            id="different_time_field",
        ),
        pytest.param(
            {"timeField": "ts"},
            {"timeField": "ts", "metaField": "m"},
            id="meta_field_present_vs_absent",
        ),
        pytest.param(
            {"timeField": "ts", "metaField": "m"},
            {"timeField": "ts", "metaField": "other"},
            id="different_meta_field",
        ),
        pytest.param(
            {"timeField": "ts", "granularity": "seconds"},
            {"timeField": "ts", "granularity": "hours"},
            id="different_granularity",
        ),
        pytest.param(
            {
                "timeField": "ts",
                "bucketMaxSpanSeconds": 100,
                "bucketRoundingSeconds": 100,
            },
            {"timeField": "ts", "granularity": "hours"},
            id="granularity_vs_bucket_options",
        ),
        pytest.param(
            {
                "timeField": "ts",
                "bucketMaxSpanSeconds": 100,
                "bucketRoundingSeconds": 100,
            },
            {
                "timeField": "ts",
                "bucketMaxSpanSeconds": 200,
                "bucketRoundingSeconds": 200,
            },
            id="different_bucket_values",
        ),
    ],
)
def test_out_timeseries_mismatch_error(collection, existing_opts, mismatched_opts):
    """Test $out with mismatched timeseries options to an existing time series collection fails."""
    db = collection.database
    target_name = "ts_mismatch_target"
    db.drop_collection(target_name)
    db.command({"create": target_name, "timeseries": existing_opts})
    populate_collection(
        collection,
        StageTestCase(
            id="ts_mismatch",
            docs=[{"_id": 1, "ts": datetime(2024, 1, 1), "v": 1}],
            msg="$out with mismatched timeseries options should fail",
        ),
    )
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [
                {
                    "$out": {
                        "db": db.name,
                        "coll": target_name,
                        "timeseries": mismatched_opts,
                    }
                }
            ],
            "cursor": {},
        },
    )
    assertFailureCode(
        result,
        OUT_TIMESERIES_OPTIONS_MISMATCH_ERROR,
        msg=(
            "$out with mismatched timeseries options to an existing time series"
            " collection should produce a timeseries options mismatch error"
        ),
    )


# Property [Index Constraint Errors]: unique index violations (including
# compound unique indexes) and duplicate _id values in the output produce a
# duplicate key error, and when a unique index violation occurs writing to a
# nonexistent target, the target collection is not created.
@pytest.mark.aggregate
def test_out_unique_index_violation(collection):
    """Test $out produces a duplicate key error on unique index violation."""
    db = collection.database
    target_name = "idx_unique_target"
    populate_collection(
        collection,
        StageTestCase(
            id="idx_unique",
            docs=[{"_id": 1, "x": 1}, {"_id": 2, "x": 1}],
            msg="$out should produce a duplicate key error on unique index violation",
        ),
    )
    db[target_name].insert_many([{"_id": 90, "x": 90}, {"_id": 91, "x": 91}])
    db[target_name].create_index("x", unique=True)
    result = execute_command(
        collection,
        {"aggregate": collection.name, "pipeline": [{"$out": target_name}], "cursor": {}},
    )
    assertFailureCode(
        result,
        DUPLICATE_KEY_ERROR,
        msg="$out should produce a duplicate key error on unique index violation",
    )


@pytest.mark.aggregate
def test_out_compound_unique_index_violation(collection):
    """Test $out produces a duplicate key error on compound unique index violation."""
    db = collection.database
    target_name = "idx_compound_target"
    populate_collection(
        collection,
        StageTestCase(
            id="idx_compound",
            docs=[{"_id": 1, "a": 1, "b": 2}, {"_id": 2, "a": 1, "b": 2}],
            msg="$out should produce a duplicate key error on compound unique index violation",
        ),
    )
    db[target_name].insert_one({"_id": 99, "a": 99, "b": 99})
    db[target_name].create_index([("a", 1), ("b", 1)], unique=True)
    result = execute_command(
        collection,
        {"aggregate": collection.name, "pipeline": [{"$out": target_name}], "cursor": {}},
    )
    assertFailureCode(
        result,
        DUPLICATE_KEY_ERROR,
        msg="$out should produce a duplicate key error on compound unique index violation",
    )


@pytest.mark.aggregate
def test_out_duplicate_id_error(collection):
    """Test $out produces a duplicate key error when output contains duplicate _id values."""
    populate_collection(
        collection,
        StageTestCase(
            id="idx_dup_id",
            docs=[{"_id": 1, "x": 1}, {"_id": 2, "x": 2}],
            msg="$out should produce a duplicate key error on duplicate _id in output",
        ),
    )
    target_name = "idx_dup_id_target"
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [
                {"$unset": "_id"},
                {"$addFields": {"_id": "same"}},
                {"$out": target_name},
            ],
            "cursor": {},
        },
    )
    assertFailureCode(
        result,
        DUPLICATE_KEY_ERROR,
        msg="$out should produce a duplicate key error when output contains duplicate _id values",
    )


@pytest.mark.aggregate
def test_out_unique_violation_nonexistent_target_not_created(collection):
    """Test $out does not create the target when a unique index violation occurs."""
    db = collection.database
    target_name = "idx_nonexist_target"
    db.drop_collection(target_name)
    populate_collection(
        collection,
        StageTestCase(
            id="idx_nonexist",
            docs=[{"_id": 1, "x": 1}, {"_id": 2, "x": 2}],
            msg="$out should not create target on unique index violation",
        ),
    )
    execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [
                {"$unset": "_id"},
                {"$addFields": {"_id": "same"}},
                {"$out": target_name},
            ],
            "cursor": {},
        },
    )
    result = execute_command(
        collection,
        {"listCollections": 1, "filter": {"name": target_name}},
    )
    assertSuccess(
        result,
        [],
        msg="$out should not create the target collection when a unique index violation occurs",
    )


# Property [Nested Pipeline Restriction - View Definition]: $out in a view
# definition is rejected, but $out from a view source (not in the view
# definition) succeeds.
@pytest.mark.aggregate
def test_out_in_view_definition_error(collection):
    """Test $out in a view definition is rejected."""
    populate_collection(
        collection,
        StageTestCase(
            id="view_def_out",
            docs=[{"_id": 1, "value": 10}],
            msg="$out in a view definition should be rejected",
        ),
    )
    result = execute_command(
        collection,
        {
            "create": "bad_view",
            "viewOn": collection.name,
            "pipeline": [{"$out": "target"}],
        },
    )
    assertFailureCode(
        result,
        INVALID_VIEW_PIPELINE_ERROR,
        msg="$out in a view definition should produce an invalid view pipeline error",
    )


@pytest.mark.aggregate
def test_out_from_view_source_succeeds(collection):
    """Test $out from a view source succeeds."""
    populate_collection(
        collection,
        StageTestCase(
            id="view_source_out",
            docs=[{"_id": 1, "value": 10}],
            msg="$out from a view source should succeed",
        ),
    )
    db = collection.database
    view_name = "good_view_for_out"
    db.drop_collection(view_name)
    db.command(
        {"create": view_name, "viewOn": collection.name, "pipeline": [{"$match": {"_id": 1}}]}
    )
    target_name = "view_source_out_target"
    execute_command(
        db[view_name],
        {
            "aggregate": view_name,
            "pipeline": [{"$out": target_name}],
            "cursor": {},
        },
    )
    result = execute_command(
        collection,
        {"find": target_name, "filter": {}},
    )
    assertSuccess(
        result,
        [{"_id": 1, "value": 10}],
        msg="$out from a view source should write the view's results to the target collection",
    )


# Property [Null Missing Check Order]: when both db and coll are null/missing,
# the error message references coll (not db), confirming coll is checked first.
@pytest.mark.aggregate
def test_out_both_null_coll_checked_first(collection):
    """Test $out checks coll before db when both are null/missing."""
    populate_collection(
        collection,
        StageTestCase(
            id="both_null_coll_first",
            docs=[{"_id": 1}],
            msg="$out should check coll before db when both are null/missing",
        ),
    )
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [{"$out": {"db": None, "coll": None}}],
            "cursor": {},
        },
    )
    assertFailure(
        result,
        {"code": NO_SUCH_KEY_ERROR, "references_coll": True},
        msg="$out should check coll before db when both are null/missing",
        transform=lambda err: {
            "code": err["code"],
            "references_coll": "coll" in err["msg"],
        },
    )


# Property [Document Form Type Error Check Order]: when both db and coll have
# type errors, the error message references db (not coll), confirming db is
# checked first.
@pytest.mark.aggregate
def test_out_type_error_db_checked_first(collection):
    """Test $out checks db type before coll type when both have type errors."""
    populate_collection(
        collection,
        StageTestCase(
            id="type_err_db_first",
            docs=[{"_id": 1}],
            msg="$out should check db type before coll type",
        ),
    )
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [{"$out": {"db": 42, "coll": True}}],
            "cursor": {},
        },
    )
    assertFailure(
        result,
        {"code": TYPE_MISMATCH_ERROR, "references_db": True},
        msg="$out should check db type before coll type when both have type errors",
        transform=lambda err: {
            "code": err["code"],
            "references_db": "$out.db" in err["msg"],
        },
    )


# Property [Error Precedence - Timeseries Type Check Order]: within the
# timeseries document, timeField type is checked before metaField type.
@pytest.mark.aggregate
def test_out_ts_type_checks_time_before_meta(collection):
    """Test $out checks timeField type before metaField type in timeseries."""
    populate_collection(
        collection,
        StageTestCase(
            id="ts_type_order",
            docs=[{"_id": 1}],
            msg="$out should check timeField type before metaField type",
        ),
    )
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [
                {
                    "$out": {
                        "db": collection.database.name,
                        "coll": "target",
                        "timeseries": {"timeField": 42, "metaField": 42},
                    }
                }
            ],
            "cursor": {},
        },
    )
    assertFailure(
        result,
        {"code": TYPE_MISMATCH_ERROR, "references_time_field": True},
        msg="$out should check timeField type before metaField type in timeseries",
        transform=lambda err: {
            "code": err["code"],
            "references_time_field": "timeField" in err["msg"],
        },
    )


# Property [Document Form Duplicate Fields]: duplicate db or coll fields in
# the document form produce a duplicate field error.
@pytest.mark.aggregate
@pytest.mark.parametrize(
    "fields,field_name",
    [
        pytest.param(
            [("db", "__DB__"), ("coll", "target"), ("db", "__DB__")],
            "db",
            id="duplicate_db",
        ),
        pytest.param(
            [("db", "__DB__"), ("coll", "target"), ("coll", "target")],
            "coll",
            id="duplicate_coll",
        ),
    ],
)
def test_out_duplicate_field_error(collection, fields, field_name):
    """Test $out rejects duplicate fields in document form."""
    populate_collection(
        collection,
        StageTestCase(
            id="dup_field",
            docs=[{"_id": 1}],
            msg=f"$out should reject duplicate {field_name!r} field",
        ),
    )
    resolved_fields = [(k, collection.database.name if v == "__DB__" else v) for k, v in fields]
    stage = _build_raw_out_stage(resolved_fields)
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [stage],
            "cursor": {},
        },
    )
    assertFailureCode(
        result,
        IDL_DUPLICATE_FIELD_ERROR,
        msg=f"$out should reject duplicate {field_name!r} field in document form",
    )


def _build_raw_out_stage(fields: list[tuple[str, Any]]) -> RawBSONDocument:
    """Build a raw BSON $out stage from ordered (key, value) pairs."""
    out_doc = build_raw_bson_doc(fields)
    stage_elements = b"\x03$out\x00" + out_doc.raw
    doc_len = 4 + len(stage_elements) + 1
    return RawBSONDocument(doc_len.to_bytes(4, "little") + stage_elements + b"\x00")


# Property [Error Precedence - Unknown Over Duplicate]: unknown field
# errors take precedence over duplicate field errors when both conditions
# are present in the $out document form.
@pytest.mark.aggregate
def test_out_prec_unknown_over_duplicate(collection):
    """Test unknown field error takes precedence over duplicate field error."""
    populate_collection(
        collection,
        StageTestCase(
            id="prec_unk_dup",
            docs=[{"_id": 1}],
            msg="unknown field error should take precedence over duplicate field error",
        ),
    )
    db_name = collection.database.name
    stage = _build_raw_out_stage(
        [("db", db_name), ("coll", "target"), ("extra", "x"), ("db", db_name)]
    )
    result = execute_command(
        collection,
        {"aggregate": collection.name, "pipeline": [stage], "cursor": {}},
    )
    assertFailureCode(
        result,
        UNRECOGNIZED_COMMAND_FIELD_ERROR,
        msg="unknown field error should take precedence over duplicate field error",
    )


# Property [Error Precedence - Duplicate Over Missing]: duplicate field
# errors take precedence over missing required field errors when both
# conditions are present in the $out document form.
@pytest.mark.aggregate
def test_out_prec_duplicate_over_missing(collection):
    """Test duplicate field error takes precedence over missing field error."""
    populate_collection(
        collection,
        StageTestCase(
            id="prec_dup_miss",
            docs=[{"_id": 1}],
            msg="duplicate field error should take precedence over missing field error",
        ),
    )
    db_name = collection.database.name
    stage = _build_raw_out_stage([("db", db_name), ("db", db_name)])
    result = execute_command(
        collection,
        {"aggregate": collection.name, "pipeline": [stage], "cursor": {}},
    )
    assertFailureCode(
        result,
        IDL_DUPLICATE_FIELD_ERROR,
        msg="duplicate field error should take precedence over missing field error",
    )


# Property [Aggregation Options]: standard aggregation options (collation,
# hint, maxTimeMS, allowDiskUse, bypassDocumentValidation) are accepted
# with $out pipelines.
@pytest.mark.aggregate
@pytest.mark.parametrize(
    "extra_opts",
    [
        pytest.param({"collation": {"locale": "en", "strength": 2}}, id="collation"),
        pytest.param({"hint": "_id_"}, id="hint"),
        pytest.param({"maxTimeMS": 60_000}, id="maxTimeMS"),
        pytest.param({"allowDiskUse": True}, id="allowDiskUse"),
        pytest.param({"bypassDocumentValidation": True}, id="bypassDocumentValidation"),
    ],
)
def test_out_aggregation_options(collection, extra_opts):
    """Test $out succeeds with standard aggregation options."""
    populate_collection(
        collection,
        StageTestCase(
            id="agg_opts",
            docs=[{"_id": 1, "value": 10}],
            msg="$out should accept standard aggregation options",
        ),
    )
    target_name = "agg_opts_target"
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [{"$out": target_name}],
            "cursor": {},
            **extra_opts,
        },
    )
    assertSuccess(
        result,
        [],
        msg=f"$out should succeed with aggregation options {extra_opts!r}",
    )


# Property [Read Concern Acceptance]: non-linearizable read concerns
# (majority, local, available) are accepted with $out pipelines.
@pytest.mark.aggregate
@pytest.mark.parametrize(
    "read_concern_level",
    [
        pytest.param("majority", id="majority"),
        pytest.param("local", id="local"),
        pytest.param("available", id="available"),
    ],
)
def test_out_read_concern_acceptance(collection, read_concern_level):
    """Test $out succeeds with non-linearizable read concern levels."""
    populate_collection(
        collection,
        StageTestCase(
            id="read_concern",
            docs=[{"_id": 1, "value": 10}],
            msg="$out should accept non-linearizable read concerns",
        ),
    )
    target_name = f"rc_{read_concern_level}_target"
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [{"$out": target_name}],
            "cursor": {},
            "readConcern": {"level": read_concern_level},
        },
    )
    assertSuccess(
        result,
        [],
        msg=f"$out should succeed with readConcern level {read_concern_level!r}",
    )


# Property [Read Concern Errors]: linearizable read concern with $out
# produces an invalid options error.
@pytest.mark.aggregate
def test_out_read_concern_linearizable_error(collection):
    """Test $out rejects linearizable read concern."""
    populate_collection(
        collection,
        StageTestCase(
            id="rc_linearizable",
            docs=[{"_id": 1, "value": 10}],
            msg="$out should reject linearizable read concern",
        ),
    )
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [{"$out": "rc_linearizable_target"}],
            "cursor": {},
            "readConcern": {"level": "linearizable"},
        },
    )
    assertFailureCode(
        result,
        INVALID_OPTIONS_ERROR,
        msg="$out should reject linearizable read concern",
    )


# Property [Schema Validation Success]: when the target collection has
# validationAction set to warn the write succeeds, and
# bypassDocumentValidation bypasses schema validation errors.
@pytest.mark.aggregate
@pytest.mark.parametrize(
    "validation_action,bypass",
    [
        pytest.param("warn", False, id="validation_action_warn"),
        pytest.param("error", True, id="bypass_document_validation"),
    ],
)
def test_out_schema_validation_success(collection, validation_action, bypass):
    """Test $out succeeds when schema validation is warn or bypassed."""
    populate_collection(
        collection,
        StageTestCase(
            id="schema_val",
            docs=[{"_id": 1, "value": "not_a_number"}],
            msg="$out should succeed with schema validation warn or bypass",
        ),
    )
    db = collection.database
    target_name = f"schema_val_{validation_action}_{bypass}_target"
    db.drop_collection(target_name)
    db.command(
        {
            "create": target_name,
            "validator": {
                "$jsonSchema": {
                    "bsonType": "object",
                    "required": ["value"],
                    "properties": {"value": {"bsonType": "int"}},
                }
            },
            "validationAction": validation_action,
        }
    )
    cmd: dict[str, Any] = {
        "aggregate": collection.name,
        "pipeline": [{"$out": target_name}],
        "cursor": {},
    }
    if bypass:
        cmd["bypassDocumentValidation"] = True
    result = execute_command(collection, cmd)
    assertSuccess(
        result,
        [{"_id": 1, "value": "not_a_number"}],
        msg=(
            f"$out should succeed with validationAction={validation_action!r}"
            f" and bypass={bypass!r}"
        ),
        transform=lambda _: list(db[target_name].find({}, {"_id": 1, "value": 1})),
    )


# Property [Schema Validation Errors]: when the target collection has
# validationAction set to error and an invalid document is produced, the
# write fails with a document validation failure error and the pre-existing
# collection is unchanged.
@pytest.mark.aggregate
def test_out_schema_validation_error(collection):
    """Test $out fails with schema validation error and leaves existing data unchanged."""
    populate_collection(
        collection,
        StageTestCase(
            id="schema_val_err",
            docs=[{"_id": 1, "value": "not_a_number"}],
            msg="$out should fail with schema validation error",
        ),
    )
    db = collection.database
    target_name = "schema_val_error_target"
    db.drop_collection(target_name)
    db.command(
        {
            "create": target_name,
            "validator": {
                "$jsonSchema": {
                    "bsonType": "object",
                    "required": ["value"],
                    "properties": {"value": {"bsonType": "int"}},
                }
            },
            "validationAction": "error",
        }
    )
    db[target_name].insert_one({"_id": 99, "value": 42})
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [{"$out": target_name}],
            "cursor": {},
        },
    )
    assertFailure(
        result,
        {"code": DOCUMENT_VALIDATION_FAILURE_ERROR, "unchanged": [{"_id": 99, "value": 42}]},
        msg=(
            "$out should fail with document validation failure when validationAction"
            " is error and the pre-existing collection should be unchanged"
        ),
        transform=lambda err: {
            "code": err["code"],
            "unchanged": list(db[target_name].find({}, {"_id": 1, "value": 1})),
        },
    )


def _execute_in_transaction(collection, command: dict[str, Any]) -> Any:
    """Execute a command inside a transaction, returning the result or exception."""
    client = collection.database.client
    with client.start_session() as session:
        session.start_transaction()
        try:
            return collection.database.command(command, session=session)
        except Exception as e:
            return e
        finally:
            session.abort_transaction()


# Property [Transaction Errors]: using $out inside a transaction produces
# an error.
@pytest.mark.aggregate
def test_out_transaction_error(collection):
    """Test $out inside a transaction produces an error."""
    populate_collection(
        collection,
        StageTestCase(
            id="transaction_out",
            docs=[{"_id": 1, "value": 10}],
            msg="$out inside a transaction should produce an error",
        ),
    )
    # Verify the pipeline works outside a transaction first.
    execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [{"$out": "txn_target"}],
            "cursor": {},
        },
    )
    result = _execute_in_transaction(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [{"$out": "txn_target"}],
            "cursor": {},
        },
    )
    assertFailureCode(
        result,
        ILLEGAL_OPERATION_ERROR,
        msg="$out inside a transaction should produce an error",
    )


# Property [Byte-Based Namespace Limit]: the namespace length limit (255
# bytes) is byte-based, not character-based - multi-byte characters consume
# more of the limit per character than single-byte characters.
@pytest.mark.aggregate
def test_out_byte_based_namespace_limit(collection):
    """Test $out namespace limit is byte-based, not character-based."""
    populate_collection(
        collection,
        StageTestCase(
            id="byte_limit",
            docs=[{"_id": 1}],
            msg="$out namespace limit should be byte-based",
        ),
    )
    db_name = collection.database.name
    # Namespace = db_name + "." + coll_name; limit is 255 bytes.
    prefix_bytes = len(db_name.encode("utf-8")) + 1
    max_coll_bytes = 255 - prefix_bytes

    # CJK character U+4E2D is 3 bytes in UTF-8. Use enough CJK characters
    # to exceed the byte limit while staying under the character count that
    # would fit with single-byte characters.
    cjk_char_count = (max_coll_bytes // 3) + 1
    cjk_name = "\u4e2d" * cjk_char_count
    # The CJK name has fewer characters than max_coll_bytes but exceeds
    # the byte limit.
    result = execute_command(
        collection,
        {"aggregate": collection.name, "pipeline": [{"$out": cjk_name}], "cursor": {}},
    )
    assertFailureCode(
        result,
        ILLEGAL_OPERATION_ERROR,
        msg=(
            "$out should reject a collection name that exceeds 255 namespace bytes"
            " even though the character count is within the single-byte limit"
        ),
    )
