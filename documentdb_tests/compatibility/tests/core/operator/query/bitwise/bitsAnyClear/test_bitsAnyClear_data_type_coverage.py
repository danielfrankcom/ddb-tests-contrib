"""
Tests for $bitsAnyClear data type coverage.

Validates matching behavior across valid field types (int32, int64, double, Decimal128, BinData),
negative zero handling, numeric cross-type equivalence (int32/int64/double/Decimal128),
and core semantics (none clear, empty array).

Invalid field type skips, non-representable value skips, and null/missing field handling
are covered by test_bitwise_common.py.
"""

import pytest
from bson import Binary, Decimal128, Int64

from documentdb_tests.compatibility.tests.core.operator.query.utils.query_test_case import (
    QueryTestCase,
)
from documentdb_tests.framework.assertions import assertSuccess
from documentdb_tests.framework.executor import execute_command
from documentdb_tests.framework.parametrize import pytest_params
from documentdb_tests.framework.test_constants import DOUBLE_NEGATIVE_ZERO

VALID_FIELD_TYPE_TESTS: list[QueryTestCase] = [
    QueryTestCase(
        id="int32_field_any_clear",
        filter={"a": {"$bitsAnyClear": 35}},
        doc=[{"_id": 1, "a": 54}, {"_id": 2, "a": 35}],
        expected=[{"_id": 1, "a": 54}],
        msg="Int32 54 (110110): bit 0 of mask 35 (100011) is clear → matches",
    ),
    QueryTestCase(
        id="int32_field_all_set_no_match",
        filter={"a": {"$bitsAnyClear": 255}},
        doc=[{"_id": 1, "a": 255}],
        expected=[],
        msg="Int32 255 has all bits 0-7 set, no bits clear → does not match",
    ),
    QueryTestCase(
        id="int32_field_zero_all_clear",
        filter={"a": {"$bitsAnyClear": 255}},
        doc=[{"_id": 1, "a": 0}],
        expected=[{"_id": 1, "a": 0}],
        msg="Int32 0 has all bits clear → matches any non-zero bitmask",
    ),
    QueryTestCase(
        id="int64_field_any_clear",
        filter={"a": {"$bitsAnyClear": 35}},
        doc=[{"_id": 1, "a": Int64(54)}, {"_id": 2, "a": Int64(35)}],
        expected=[{"_id": 1, "a": Int64(54)}],
        msg="Int64 field with any bit clear matches",
    ),
    QueryTestCase(
        id="double_field_whole_number",
        filter={"a": {"$bitsAnyClear": 1}},
        doc=[{"_id": 1, "a": 20.0}, {"_id": 2, "a": 1.0}],
        expected=[{"_id": 1, "a": 20.0}],
        msg="Double (integer-representable) field treated as integer",
    ),
    QueryTestCase(
        id="decimal128_field_any_clear",
        filter={"a": {"$bitsAnyClear": 35}},
        doc=[{"_id": 1, "a": Decimal128("54")}, {"_id": 2, "a": Decimal128("35")}],
        expected=[{"_id": 1, "a": Decimal128("54")}],
        msg="Decimal128 integer value works like int",
    ),
    QueryTestCase(
        id="bindata_field_bit0_clear",
        filter={"a": {"$bitsAnyClear": 1}},
        doc=[{"_id": 1, "a": Binary(b"\x06", 128)}, {"_id": 2, "a": Binary(b"\x01", 128)}],
        expected=[{"_id": 1, "a": Binary(b"\x06", 128)}],
        msg="BinData field with bit 0 clear matches numeric bitmask",
    ),
    QueryTestCase(
        id="negative_zero_matches",
        filter={"a": {"$bitsAnyClear": 1}},
        doc=[{"_id": 1, "a": DOUBLE_NEGATIVE_ZERO}, {"_id": 2, "a": 1}],
        expected=[{"_id": 1, "a": DOUBLE_NEGATIVE_ZERO}],
        msg="Negative zero treated as integer 0, bit 0 is clear",
    ),
]

NUMERIC_EQUIVALENCE_TESTS: list[QueryTestCase] = [
    QueryTestCase(
        id="numeric_cross_type_equivalence",
        filter={"a": {"$bitsAnyClear": [0, 1]}},
        doc=[
            {"_id": 1, "a": 20},
            {"_id": 2, "a": Int64(20)},
            {"_id": 3, "a": 20.0},
            {"_id": 4, "a": Decimal128("20")},
            {"_id": 5, "a": 3},
        ],
        expected=[
            {"_id": 1, "a": 20},
            {"_id": 2, "a": Int64(20)},
            {"_id": 3, "a": 20.0},
            {"_id": 4, "a": Decimal128("20")},
        ],
        msg="Int32, Int64, Double, Decimal128 of same value produce same match result",
    ),
]

CORE_SEMANTICS_TESTS: list[QueryTestCase] = [
    QueryTestCase(
        id="any_clear_none_queried_clear",
        filter={"a": {"$bitsAnyClear": 35}},
        doc=[{"_id": 1, "a": 35}],
        expected=[],
        msg="NO queried bits clear (all set) → does NOT match",
    ),
    QueryTestCase(
        id="empty_array_no_match",
        filter={"a": {"$bitsAnyClear": 1}},
        doc=[{"_id": 1, "a": []}, {"_id": 2, "a": 0}],
        expected=[{"_id": 2, "a": 0}],
        msg="Empty array field does not match",
    ),
]

ALL_TESTS = VALID_FIELD_TYPE_TESTS + NUMERIC_EQUIVALENCE_TESTS + CORE_SEMANTICS_TESTS


@pytest.mark.parametrize("test", pytest_params(ALL_TESTS))
def test_bitsAnyClear_data_type_coverage(collection, test):
    """Test $bitsAnyClear matching behavior across valid BSON field types."""
    collection.insert_many(test.doc)
    result = execute_command(collection, {"find": collection.name, "filter": test.filter})
    assertSuccess(result, test.expected, ignore_doc_order=True)
