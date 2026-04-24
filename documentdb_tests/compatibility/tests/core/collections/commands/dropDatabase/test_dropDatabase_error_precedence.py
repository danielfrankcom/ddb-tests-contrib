from __future__ import annotations

import pytest
from bson.son import SON

from documentdb_tests.compatibility.tests.core.collections.commands.utils.command_test_case import (
    CommandTestCase,
)
from documentdb_tests.framework.assertions import assertResult
from documentdb_tests.framework.error_codes import (
    DROP_DATABASE_VALUE_ERROR,
    FAILED_TO_PARSE_ERROR,
    TYPE_MISMATCH_ERROR,
    UNRECOGNIZED_COMMAND_FIELD_ERROR,
)
from documentdb_tests.framework.executor import execute_command
from documentdb_tests.framework.parametrize import pytest_params

# Property [Error Precedence]: writeConcern type validation takes
# precedence over all other errors, writeConcern value validation takes
# precedence over unknown field and dropDatabase errors, unknown field
# rejection takes precedence over dropDatabase validation errors, and
# comment does not affect error behavior.
ERROR_PRECEDENCE_TESTS: list[CommandTestCase] = [
    CommandTestCase(
        command=SON([("dropDatabase", 0), ("writeConcern", "bad")]),
        error_code=TYPE_MISMATCH_ERROR,
        msg="writeConcern type error should take precedence over dropDatabase value error",
        id="wc_type_over_dd_value",
    ),
    CommandTestCase(
        command=SON([("dropDatabase", None), ("writeConcern", "bad")]),
        error_code=TYPE_MISMATCH_ERROR,
        msg="writeConcern type error should take precedence over dropDatabase null/missing error",
        id="wc_type_over_dd_null",
    ),
    CommandTestCase(
        command=SON([("dropDatabase", 1), ("writeConcern", "bad"), ("unknownField", 1)]),
        error_code=TYPE_MISMATCH_ERROR,
        msg="writeConcern type error should take precedence over unknown field error",
        id="wc_type_over_unknown_field",
    ),
    CommandTestCase(
        command=SON([("dropDatabase", True), ("writeConcern", {"w": -1})]),
        error_code=FAILED_TO_PARSE_ERROR,
        msg="writeConcern value error should take precedence over dropDatabase type error",
        id="wc_value_over_dd_type",
    ),
    CommandTestCase(
        command=SON([("dropDatabase", 0), ("writeConcern", {"w": -1})]),
        error_code=FAILED_TO_PARSE_ERROR,
        msg="writeConcern value error should take precedence over dropDatabase value error",
        id="wc_value_over_dd_value",
    ),
    CommandTestCase(
        command=SON([("dropDatabase", None), ("writeConcern", {"w": -1})]),
        error_code=FAILED_TO_PARSE_ERROR,
        msg="writeConcern value error should take precedence over dropDatabase null/missing error",
        id="wc_value_over_dd_null",
    ),
    CommandTestCase(
        command=SON([("dropDatabase", 1), ("writeConcern", {"w": -1, "unknownField": 1})]),
        error_code=FAILED_TO_PARSE_ERROR,
        msg="writeConcern value error should take precedence over unknown field error",
        id="wc_value_over_unknown_field",
    ),
    CommandTestCase(
        command=SON([("dropDatabase", True), ("writeConcern", {"unknownField": 1})]),
        error_code=UNRECOGNIZED_COMMAND_FIELD_ERROR,
        msg="Unknown field error should take precedence over dropDatabase type error",
        id="unknown_field_over_dd_type",
    ),
    CommandTestCase(
        command=SON([("dropDatabase", 0), ("writeConcern", {"unknownField": 1})]),
        error_code=UNRECOGNIZED_COMMAND_FIELD_ERROR,
        msg="Unknown field error should take precedence over dropDatabase value error",
        id="unknown_field_over_dd_value",
    ),
    CommandTestCase(
        command=SON([("dropDatabase", None), ("writeConcern", {"unknownField": 1})]),
        error_code=UNRECOGNIZED_COMMAND_FIELD_ERROR,
        msg="Unknown field error should take precedence over dropDatabase null/missing error",
        id="unknown_field_over_dd_null",
    ),
    CommandTestCase(
        command=SON([("dropDatabase", True), ("comment", "test")]),
        error_code=TYPE_MISMATCH_ERROR,
        msg="Comment should not affect dropDatabase type error behavior",
        id="comment_no_effect_dd_type",
    ),
    CommandTestCase(
        command=SON([("dropDatabase", 0), ("comment", "test")]),
        error_code=DROP_DATABASE_VALUE_ERROR,
        msg="Comment should not affect dropDatabase value error behavior",
        id="comment_no_effect_dd_value",
    ),
]


@pytest.mark.collection_mgmt
@pytest.mark.parametrize("test", pytest_params(ERROR_PRECEDENCE_TESTS))
def test_dropDatabase_error_precedence(database_client, collection, register_db_cleanup, test):
    """Test dropDatabase command inputs and error handling."""
    coll = test.prepare(database_client, collection)
    result = execute_command(coll, test.command)
    assertResult(
        result,
        expected=test.expected,
        error_code=test.error_code,
        msg=test.msg,
        raw_res=True,
    )
