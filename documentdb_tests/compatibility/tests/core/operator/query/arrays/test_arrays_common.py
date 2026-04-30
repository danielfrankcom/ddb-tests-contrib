"""
Tests for cross-operator interactions between array query operators.

Validates $all with $elemMatch sub-expressions including single and multiple
$elemMatch conditions, and mixed $elemMatch with plain value error handling.
"""

import pytest

from documentdb_tests.compatibility.tests.core.operator.query.utils.query_test_case import (
    QueryTestCase,
)
from documentdb_tests.framework.assertions import assertFailureCode, assertSuccess
from documentdb_tests.framework.error_codes import BAD_VALUE_ERROR
from documentdb_tests.framework.executor import execute_command
from documentdb_tests.framework.parametrize import pytest_params

ALL_ELEMMATCH_TESTS: list[QueryTestCase] = [
    QueryTestCase(
        id="single_elemmatch",
        filter={"a": {"$all": [{"$elemMatch": {"x": 1, "y": 2}}]}},
        doc=[
            {"_id": 1, "a": [{"x": 1, "y": 2}]},
            {"_id": 2, "a": [{"x": 1, "y": 3}]},
            {"_id": 3, "a": [{"x": 1}, {"y": 2}]},
        ],
        expected=[{"_id": 1, "a": [{"x": 1, "y": 2}]}],
        msg="$all with single $elemMatch should match",
    ),
    QueryTestCase(
        id="multiple_elemmatch",
        filter={
            "a": {
                "$all": [
                    {"$elemMatch": {"x": 1, "y": 2}},
                    {"$elemMatch": {"x": 2, "y": 3}},
                ]
            }
        },
        doc=[
            {"_id": 1, "a": [{"x": 1, "y": 2}, {"x": 2, "y": 3}]},
            {"_id": 2, "a": [{"x": 1, "y": 2}]},
            {"_id": 3, "a": [{"x": 1, "y": 2}, {"x": 2, "y": 1}]},
        ],
        expected=[{"_id": 1, "a": [{"x": 1, "y": 2}, {"x": 2, "y": 3}]}],
        msg="$all with multiple $elemMatch should require all conditions met",
    ),
]


@pytest.mark.parametrize("test", pytest_params(ALL_ELEMMATCH_TESTS))
def test_all_with_elemmatch(collection, test):
    """Test $all with $elemMatch sub-expressions."""
    collection.insert_many(test.doc)
    result = execute_command(collection, {"find": collection.name, "filter": test.filter})
    assertSuccess(result, test.expected, ignore_doc_order=True)


MIXED_ELEMMATCH_ERROR_TESTS: list[QueryTestCase] = [
    QueryTestCase(
        id="mixed_elemmatch_and_value",
        filter={"a": {"$all": [{"$elemMatch": {"x": 1}}, "y"]}},
        error_code=BAD_VALUE_ERROR,
        msg="Mixed $elemMatch and plain values should produce error",
    ),
]


@pytest.mark.parametrize("test", pytest_params(MIXED_ELEMMATCH_ERROR_TESTS))
def test_all_mixed_elemmatch_error(collection, test):
    """Test $all with mixed $elemMatch and plain values produces error."""
    collection.insert_many([{"_id": 1, "a": [{"x": 1}, "y"]}])
    result = execute_command(collection, {"find": collection.name, "filter": test.filter})
    assertFailureCode(result, test.error_code)
