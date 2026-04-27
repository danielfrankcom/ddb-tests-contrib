"""Tests for $lookup error precedence ordering."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from documentdb_tests.compatibility.tests.core.operator.stages.utils.stage_test_case import (
    StageTestCase,
    populate_collection,
)
from documentdb_tests.framework.assertions import assertFailure
from documentdb_tests.framework.error_codes import (
    FAILED_TO_PARSE_ERROR,
    FIELD_PATH_DOLLAR_PREFIX_ERROR,
    INVALID_NAMESPACE_ERROR,
    MISSING_REQUIRED_FIELD_ERROR,
    TYPE_MISMATCH_ERROR,
    UNRECOGNIZED_COMMAND_FIELD_ERROR,
)
from documentdb_tests.framework.executor import execute_command
from documentdb_tests.framework.parametrize import pytest_params


@dataclass(frozen=True)
class LookupPrecedenceTestCase(StageTestCase):
    """Test case for $lookup error precedence tests."""

    lookup_spec: dict[str, Any] | None = None
    expected_msg_contains: str | None = None


# Property [Error Precedence]: when multiple parameter errors are present
# simultaneously, the server validates parameters in a fixed order and
# reports the first error encountered.
LOOKUP_ERROR_PRECEDENCE_TESTS: list[LookupPrecedenceTestCase] = [
    LookupPrecedenceTestCase(
        "from_type_over_pipeline_type",
        docs=[{"_id": 1}],
        lookup_spec={"from": 123, "pipeline": 123, "as": "j"},
        error_code=FAILED_TO_PARSE_ERROR,
        msg="$lookup from type error should take precedence over pipeline type error",
    ),
    LookupPrecedenceTestCase(
        "pipeline_type_over_let_type",
        docs=[{"_id": 1}],
        lookup_spec={"from": "x", "let": 123, "pipeline": 123, "as": "j"},
        error_code=TYPE_MISMATCH_ERROR,
        expected_msg_contains="pipeline",
        msg=(
            "$lookup pipeline type error should take"
            " precedence over let type error"
            " regardless of field order"
        ),
    ),
    LookupPrecedenceTestCase(
        "let_type_over_localfield_type",
        docs=[{"_id": 1}],
        lookup_spec={
            "from": "x",
            "let": 123,
            "pipeline": [],
            "localField": 123,
            "foreignField": "ff",
            "as": "j",
        },
        error_code=TYPE_MISMATCH_ERROR,
        expected_msg_contains="$lookup.let",
        msg="$lookup let type error should take precedence over localField type error",
    ),
    LookupPrecedenceTestCase(
        "localfield_type_over_foreignfield_type",
        docs=[{"_id": 1}],
        lookup_spec={"from": "x", "localField": 123, "foreignField": 123, "as": "j"},
        error_code=TYPE_MISMATCH_ERROR,
        expected_msg_contains="$lookup.localField",
        msg="$lookup localField type error should take precedence over foreignField type error",
    ),
    LookupPrecedenceTestCase(
        "foreignfield_type_over_as_type",
        docs=[{"_id": 1}],
        lookup_spec={"from": "x", "localField": "lf", "foreignField": 123, "as": 123},
        error_code=TYPE_MISMATCH_ERROR,
        expected_msg_contains="$lookup.foreignField",
        msg="$lookup foreignField type error should take precedence over as type error",
    ),
    LookupPrecedenceTestCase(
        "as_type_over_unknown_field",
        docs=[{"_id": 1}],
        lookup_spec={
            "from": "x",
            "localField": "lf",
            "foreignField": "ff",
            "as": 123,
            "unknown": 1,
        },
        error_code=TYPE_MISMATCH_ERROR,
        msg="$lookup as type error should take precedence over unknown field error",
    ),
    LookupPrecedenceTestCase(
        "unknown_field_over_missing_as",
        docs=[{"_id": 1}],
        lookup_spec={
            "from": "x",
            "localField": "lf",
            "foreignField": "ff",
            "unknown": 1,
        },
        error_code=UNRECOGNIZED_COMMAND_FIELD_ERROR,
        msg="$lookup unknown field error should fire before missing as error",
    ),
    LookupPrecedenceTestCase(
        "missing_as_over_namespace",
        docs=[{"_id": 1}],
        lookup_spec={"from": "foo", "pipeline": [{"$documents": [{"x": 1}]}]},
        error_code=MISSING_REQUIRED_FIELD_ERROR,
        msg="$lookup missing as error should fire before namespace error",
    ),
    LookupPrecedenceTestCase(
        "namespace_over_path_validation",
        docs=[{"_id": 1}],
        lookup_spec={"from": "", "localField": "$bad", "foreignField": "ff", "as": "j"},
        error_code=INVALID_NAMESPACE_ERROR,
        msg="$lookup namespace error should fire before path validation errors",
    ),
    LookupPrecedenceTestCase(
        "as_path_over_localfield_path",
        docs=[{"_id": 1}],
        lookup_spec={
            "from": "x",
            "localField": ".bad",
            "foreignField": "ff",
            "as": "$bad",
        },
        error_code=FIELD_PATH_DOLLAR_PREFIX_ERROR,
        msg=(
            "$lookup as path validation error should fire"
            " before localField/foreignField path"
            " validation errors"
        ),
    ),
]


@pytest.mark.aggregate
@pytest.mark.parametrize("test_case", pytest_params(LOOKUP_ERROR_PRECEDENCE_TESTS))
def test_lookup_error_precedence(
    collection: Any,
    test_case: LookupPrecedenceTestCase,
) -> None:
    """Test $lookup error precedence ordering."""
    populate_collection(collection, test_case)
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [{"$lookup": test_case.lookup_spec}],
            "cursor": {},
        },
    )
    expected_err: dict[str, Any] = {"code": test_case.error_code}
    if test_case.expected_msg_contains is not None:
        expected_err["msg_contains"] = True
    substr = test_case.expected_msg_contains
    assertFailure(
        result,
        expected_err,
        msg=test_case.msg,
        transform=lambda actual: {
            "code": actual.get("code"),
            **({"msg_contains": substr in actual.get("msg", "")} if substr is not None else {}),
        },
    )
