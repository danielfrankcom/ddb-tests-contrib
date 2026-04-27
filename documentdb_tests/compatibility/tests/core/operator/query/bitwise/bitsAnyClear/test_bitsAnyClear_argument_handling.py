"""
Tests for $bitsAnyClear valid argument handling.

Validates valid bitmask forms (numeric, BinData, position list).
Error cases are covered in test_bitwise_common.py.
"""

import pytest
from bson import Binary, Decimal128, Int64

from documentdb_tests.compatibility.tests.core.operator.query.utils.query_test_case import (
    QueryTestCase,
)
from documentdb_tests.framework.assertions import assertSuccess
from documentdb_tests.framework.executor import execute_command
from documentdb_tests.framework.parametrize import pytest_params
from documentdb_tests.framework.test_constants import (
    DECIMAL128_NEGATIVE_ZERO,
    DECIMAL128_ZERO,
    DOUBLE_NEGATIVE_ZERO,
    INT32_MAX,
    INT64_MAX,
)

VALID_NUMERIC_BITMASK_TESTS: list[QueryTestCase] = [
    QueryTestCase(
        id="numeric_bitmask_zero",
        filter={"a": {"$bitsAnyClear": 0}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": 255}, {"_id": 3, "a": -1}],
        expected=[],
        msg="Bitmask 0 has no bits to check, matches nothing",
    ),
    QueryTestCase(
        id="numeric_bitmask_1",
        filter={"a": {"$bitsAnyClear": 1}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": 1}, {"_id": 3, "a": 2}],
        expected=[{"_id": 1, "a": 0}, {"_id": 3, "a": 2}],
        msg="Bitmask 1 checks bit 0; matches where bit 0 is clear",
    ),
    QueryTestCase(
        id="numeric_bitmask_35",
        filter={"a": {"$bitsAnyClear": 35}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": 35}, {"_id": 3, "a": 54}],
        expected=[{"_id": 1, "a": 0}, {"_id": 3, "a": 54}],
        msg="Bitmask 35 (bits 0,1,5) matches where ANY of those bits is clear; "
        "also serves as numeric form baseline for three-form equivalence ",
    ),
    QueryTestCase(
        id="numeric_bitmask_255",
        filter={"a": {"$bitsAnyClear": 255}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": 255}, {"_id": 3, "a": 54}],
        expected=[{"_id": 1, "a": 0}, {"_id": 3, "a": 54}],
        msg="Bitmask 255 (bits 0-7) matches where any of bits 0-7 is clear",
    ),
    QueryTestCase(
        id="numeric_bitmask_int64_max",
        filter={"a": {"$bitsAnyClear": INT64_MAX}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": 1}],
        expected=[{"_id": 1, "a": 0}, {"_id": 2, "a": 1}],
        msg="INT64_MAX as bitmask: any value with any of bits 0-62 clear matches",
    ),
    QueryTestCase(
        id="numeric_bitmask_double_whole_3",
        filter={"a": {"$bitsAnyClear": 3.0}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": 3}],
        expected=[{"_id": 1, "a": 0}],
        msg="Whole-number double 3.0 accepted as equivalent to int 3",
    ),
    QueryTestCase(
        id="numeric_bitmask_double_zero",
        filter={"a": {"$bitsAnyClear": 0.0}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": 255}],
        expected=[],
        msg="Double 0.0 as bitmask: no bits to check, matches nothing",
    ),
    QueryTestCase(
        id="numeric_bitmask_negative_zero",
        filter={"a": {"$bitsAnyClear": DOUBLE_NEGATIVE_ZERO}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": 255}],
        expected=[],
        msg="Negative zero -0.0 treated as 0, no bits to check",
    ),
    QueryTestCase(
        id="numeric_bitmask_int64_35",
        filter={"a": {"$bitsAnyClear": Int64(35)}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": 35}],
        expected=[{"_id": 1, "a": 0}],
        msg="Int64(35) accepted as numeric bitmask",
    ),
    QueryTestCase(
        id="numeric_bitmask_int64_zero",
        filter={"a": {"$bitsAnyClear": Int64(0)}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": 255}],
        expected=[],
        msg="Int64(0) as bitmask: no bits to check",
    ),
    QueryTestCase(
        id="numeric_bitmask_decimal128_35",
        filter={"a": {"$bitsAnyClear": Decimal128("35")}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": 35}],
        expected=[{"_id": 1, "a": 0}],
        msg="Decimal128 within int64 range accepted as numeric bitmask",
    ),
    QueryTestCase(
        id="numeric_bitmask_decimal128_zero",
        filter={"a": {"$bitsAnyClear": DECIMAL128_ZERO}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": 255}],
        expected=[],
        msg="Decimal128('0') as bitmask: no bits to check",
    ),
    QueryTestCase(
        id="numeric_bitmask_decimal128_negative_zero",
        filter={"a": {"$bitsAnyClear": DECIMAL128_NEGATIVE_ZERO}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": 255}],
        expected=[],
        msg="Decimal128('-0') treated as 0, no bits to check",
    ),
]

VALID_BINDATA_BITMASK_TESTS: list[QueryTestCase] = [
    QueryTestCase(
        id="bindata_bitmask_empty",
        filter={"a": {"$bitsAnyClear": Binary(b"")}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": 255}],
        expected=[],
        msg="Empty BinData bitmask: no bits to check, matches nothing",
    ),
    QueryTestCase(
        id="bindata_bitmask_all_zeros",
        filter={"a": {"$bitsAnyClear": Binary(b"\x00")}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": 255}],
        expected=[],
        msg="BinData 0x00: no bits set in mask, matches nothing",
    ),
    QueryTestCase(
        id="bindata_bitmask_all_ones",
        filter={"a": {"$bitsAnyClear": Binary(b"\xff")}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": 255}, {"_id": 3, "a": 54}],
        expected=[{"_id": 1, "a": 0}, {"_id": 3, "a": 54}],
        msg="BinData 0xFF: matches where any of bits 0-7 is clear",
    ),
    QueryTestCase(
        id="bindata_bitmask_subtype_1",
        filter={"a": {"$bitsAnyClear": Binary(b"\x03", 1)}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": 3}],
        expected=[{"_id": 1, "a": 0}],
        msg="BinData with subtype 1 works as bitmask",
    ),
]

VALID_POSITION_LIST_TESTS: list[QueryTestCase] = [
    QueryTestCase(
        id="position_list_empty",
        filter={"a": {"$bitsAnyClear": []}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": 255}],
        expected=[],
        msg="Empty position list: no positions to check, matches nothing",
    ),
    QueryTestCase(
        id="position_list_single_bit0",
        filter={"a": {"$bitsAnyClear": [0]}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": 1}, {"_id": 3, "a": 2}],
        expected=[{"_id": 1, "a": 0}, {"_id": 3, "a": 2}],
        msg="Single position [0] checks bit 0",
    ),
    QueryTestCase(
        id="position_list_multiple",
        filter={"a": {"$bitsAnyClear": [1, 5]}},
        doc=[{"_id": 1, "a": 20}, {"_id": 2, "a": 15}, {"_id": 3, "a": 54}],
        expected=[{"_id": 1, "a": 20}, {"_id": 2, "a": 15}],
        msg="Positions [1,5] matches where bit 1 or bit 5 is clear",
    ),
    QueryTestCase(
        id="position_list_all_byte_bits",
        filter={"a": {"$bitsAnyClear": [0, 1, 2, 3, 4, 5, 6, 7]}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": 255}, {"_id": 3, "a": 54}],
        expected=[{"_id": 1, "a": 0}, {"_id": 3, "a": 54}],
        msg="All 8 bit positions: matches where any of bits 0-7 is clear",
    ),
    QueryTestCase(
        id="position_list_large_position",
        filter={"a": {"$bitsAnyClear": [100]}},
        doc=[{"_id": 1, "a": 5}, {"_id": 2, "a": -1}],
        expected=[{"_id": 1, "a": 5}],
        msg="Large position [100]: clear for positive (zero-extended), "
        "set for negative (sign-extended)",
    ),
    QueryTestCase(
        id="position_list_decimal128_values",
        filter={"a": {"$bitsAnyClear": [Decimal128("0"), Decimal128("1")]}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": 3}],
        expected=[{"_id": 1, "a": 0}],
        msg="Decimal128 integer values accepted as positions",
    ),
    QueryTestCase(
        id="position_list_all_64_bits",
        filter={"a": {"$bitsAnyClear": list(range(64))}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": -1}],
        expected=[{"_id": 1, "a": 0}],
        msg="All 64 positions [0..63]: -1 has all bits set so no match; 0 has all clear",
    ),
    QueryTestCase(
        id="position_list_duplicate_positions",
        filter={"a": {"$bitsAnyClear": [0, 0, 0]}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": 1}],
        expected=[{"_id": 1, "a": 0}],
        msg="Duplicate positions behave same as deduplicated [0]",
    ),
    QueryTestCase(
        id="position_list_unsorted",
        filter={"a": {"$bitsAnyClear": [5, 1, 3]}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": 42}, {"_id": 3, "a": 54}],
        expected=[{"_id": 1, "a": 0}, {"_id": 3, "a": 54}],
        msg="Unsorted positions [5,1,3] accepted; "
        "42 (101010) has all 3 set, 54 (110110) has bit 3 clear",
    ),
    QueryTestCase(
        id="position_list_mid_range_bits",
        filter={"a": {"$bitsAnyClear": [30, 32, 34, 36, 38, 40]}},
        doc=[
            {"_id": 1, "a": 0},
            {"_id": 2, "a": Int64(10737418240)},
            {"_id": 3, "a": Int64(1465657589760)},
        ],
        expected=[{"_id": 1, "a": 0}, {"_id": 2, "a": Int64(10737418240)}],
        msg="Mid-range positions spanning byte boundaries 3-5; "
        "doc 3 has all queried bits set so no match",
    ),
    QueryTestCase(
        id="position_list_int32_max",
        filter={"a": {"$bitsAnyClear": [INT32_MAX]}},
        doc=[{"_id": 1, "a": 5}, {"_id": 2, "a": -1}],
        expected=[{"_id": 1, "a": 5}],
        msg="INT32_MAX as position accepted",
    ),
]

ALL_VALID_TESTS = (
    VALID_NUMERIC_BITMASK_TESTS + VALID_BINDATA_BITMASK_TESTS + VALID_POSITION_LIST_TESTS
)


@pytest.mark.parametrize("test", pytest_params(ALL_VALID_TESTS))
def test_bitsAnyClear_valid_arguments(collection, test):
    """Test $bitsAnyClear with valid bitmask forms (numeric, BinData, position list)."""
    collection.insert_many(test.doc)
    result = execute_command(collection, {"find": collection.name, "filter": test.filter})
    assertSuccess(result, test.expected, ignore_doc_order=True)
