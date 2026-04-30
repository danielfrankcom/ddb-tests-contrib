"""Tests for $lookup error precedence ordering."""

from __future__ import annotations

from typing import Any

import pytest

from documentdb_tests.compatibility.tests.core.operator.stages.lookup.utils.lookup_common import (
    LookupTestCase,
    build_lookup_command,
    setup_lookup,
)
from documentdb_tests.framework.assertions import assertResult
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

# Property [Error Precedence]: when multiple parameter errors are present
# simultaneously, the server validates parameters in a fixed order and
# reports the first error encountered.
LOOKUP_ERROR_PRECEDENCE_TESTS: list[LookupTestCase] = [
    LookupTestCase(
        "from_type_over_pipeline_type",
        docs=[{"_id": 1}],
        pipeline=[{"$lookup": {"from": 123, "pipeline": 123, "as": "j"}}],
        error_code=FAILED_TO_PARSE_ERROR,
        msg="$lookup from type error should take precedence over pipeline type error",
    ),
    LookupTestCase(
        "as_type_over_unknown_field",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$lookup": {
                    "from": "x",
                    "localField": "lf",
                    "foreignField": "ff",
                    "as": 123,
                    "unknown": 1,
                }
            }
        ],
        error_code=TYPE_MISMATCH_ERROR,
        msg="$lookup as type error should take precedence over unknown field error",
    ),
    LookupTestCase(
        "unknown_field_over_missing_as",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$lookup": {
                    "from": "x",
                    "localField": "lf",
                    "foreignField": "ff",
                    "unknown": 1,
                }
            }
        ],
        error_code=UNRECOGNIZED_COMMAND_FIELD_ERROR,
        msg="$lookup unknown field error should fire before missing as error",
    ),
    LookupTestCase(
        "missing_as_over_namespace",
        docs=[{"_id": 1}],
        pipeline=[{"$lookup": {"from": "foo", "pipeline": [{"$documents": [{"x": 1}]}]}}],
        error_code=MISSING_REQUIRED_FIELD_ERROR,
        msg="$lookup missing as error should fire before namespace error",
    ),
    LookupTestCase(
        "namespace_over_path_validation",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$lookup": {
                    "from": "",
                    "localField": "$bad",
                    "foreignField": "ff",
                    "as": "j",
                }
            }
        ],
        error_code=INVALID_NAMESPACE_ERROR,
        msg="$lookup namespace error should fire before path validation errors",
    ),
    LookupTestCase(
        "as_path_over_localfield_path",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$lookup": {
                    "from": "x",
                    "localField": ".bad",
                    "foreignField": "ff",
                    "as": "$bad",
                }
            }
        ],
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
    test_case: LookupTestCase,
) -> None:
    """Test $lookup error precedence ordering."""
    with setup_lookup(collection, test_case) as foreign_name:
        command = build_lookup_command(collection, test_case, foreign_name)
        result = execute_command(collection, command)
        assertResult(
            result,
            error_code=test_case.error_code,
            msg=test_case.msg,
        )
