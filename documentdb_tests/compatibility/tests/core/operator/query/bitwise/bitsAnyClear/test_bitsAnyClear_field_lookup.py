"""
Tests for $bitsAnyClear field lookup patterns, cross-type combinations, and algebraic properties.

Validates simple/array field paths, deeply nested paths through arrays of objects,
_id field queries, three-form equivalence (numeric, position list, BinData),
and position list order invariance.
"""

import pytest
from bson import Binary, ObjectId

from documentdb_tests.compatibility.tests.core.operator.query.utils.query_test_case import (
    QueryTestCase,
)
from documentdb_tests.framework.assertions import assertSuccess
from documentdb_tests.framework.executor import execute_command
from documentdb_tests.framework.parametrize import pytest_params

FIELD_LOOKUP_TESTS: list[QueryTestCase] = [
    QueryTestCase(
        id="simple_field_path",
        filter={"a": {"$bitsAnyClear": [0, 1]}},
        doc=[{"_id": 1, "a": 20}, {"_id": 2, "a": 3}],
        expected=[{"_id": 1, "a": 20}],
        msg="Simple field path: 20 (10100) has bits 0,1 clear; 3 (11) has both set",
    ),
    QueryTestCase(
        id="non_existent_nested_field_no_match",
        filter={"a.missing": {"$bitsAnyClear": 255}},
        doc=[{"_id": 1, "a": {"b": 20}}],
        expected=[],
        msg="Non-existent nested field does not match",
    ),
    QueryTestCase(
        id="array_field_any_element_match",
        filter={"a": {"$bitsAnyClear": 35}},
        doc=[{"_id": 1, "a": [54, 35]}, {"_id": 2, "a": [35, 35]}],
        expected=[{"_id": 1, "a": [54, 35]}],
        msg="Array field matches if any element satisfies",
    ),
    QueryTestCase(
        id="array_index_path",
        filter={"a.0": {"$bitsAnyClear": 35}},
        doc=[{"_id": 1, "a": [54, 35]}, {"_id": 2, "a": [35, 54]}],
        expected=[{"_id": 1, "a": [54, 35]}],
        msg="Array index path checks element at index 0",
    ),
    QueryTestCase(
        id="deep_nested_path_through_array_of_objects",
        filter={"a.b.c.d": {"$bitsAnyClear": 35}},
        doc=[
            {"_id": 1, "a": {"b": [{"c": {"d": 54}}, {"c": {"d": 35}}]}},
            {"_id": 2, "a": {"b": [{"c": {"d": 35}}, {"c": {"d": 35}}]}},
        ],
        expected=[{"_id": 1, "a": {"b": [{"c": {"d": 54}}, {"c": {"d": 35}}]}}],
        msg="Deep nested path traverses array of objects at intermediate level",
    ),
    QueryTestCase(
        id="composite_array_path",
        filter={"a.b": {"$bitsAnyClear": 35}},
        doc=[
            {"_id": 1, "a": [{"b": 54}, {"b": 35}]},
            {"_id": 2, "a": [{"b": 35}, {"b": 35}]},
        ],
        expected=[{"_id": 1, "a": [{"b": 54}, {"b": 35}]}],
        msg="Composite array path: matches if any element's b satisfies",
    ),
    QueryTestCase(
        id="id_field_numeric",
        filter={"_id": {"$bitsAnyClear": 1}},
        doc=[{"_id": 0, "a": 1}, {"_id": 1, "a": 2}, {"_id": 2, "a": 3}],
        expected=[{"_id": 0, "a": 1}, {"_id": 2, "a": 3}],
        msg="$bitsAnyClear on numeric _id field",
    ),
    QueryTestCase(
        id="id_field_objectid_no_match",
        filter={"_id": {"$bitsAnyClear": 1}},
        doc=[{"_id": ObjectId(), "a": 1}],
        expected=[],
        msg="$bitsAnyClear on ObjectId _id does not match",
    ),
    QueryTestCase(
        id="array_mixed_types",
        filter={"a": {"$bitsAnyClear": 35}},
        doc=[
            {"_id": 1, "a": [54, "hello", None]},
            {"_id": 2, "a": ["hello", None]},
        ],
        expected=[{"_id": 1, "a": [54, "hello", None]}],
        msg="Array with mixed types: matches if any numeric element satisfies",
    ),
]

ALGEBRAIC_TESTS: list[QueryTestCase] = [
    QueryTestCase(
        id="position_order_invariance_asc",
        filter={"a": {"$bitsAnyClear": [0, 1, 5]}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": 35}, {"_id": 3, "a": 54}],
        expected=[{"_id": 1, "a": 0}, {"_id": 3, "a": 54}],
        msg="Position list [0,1,5] ascending order; also serves as position-list form "
        "for three-form equivalence with numeric 35 and BinData 0x23",
    ),
    QueryTestCase(
        id="position_order_invariance_desc",
        filter={"a": {"$bitsAnyClear": [5, 1, 0]}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": 35}, {"_id": 3, "a": 54}],
        expected=[{"_id": 1, "a": 0}, {"_id": 3, "a": 54}],
        msg="Position list [5,1,0] descending order produces same result",
    ),
    QueryTestCase(
        id="three_form_equivalence_bindata",
        filter={"a": {"$bitsAnyClear": Binary(b"\x23")}},
        doc=[{"_id": 1, "a": 0}, {"_id": 2, "a": 35}, {"_id": 3, "a": 54}],
        expected=[{"_id": 1, "a": 0}, {"_id": 3, "a": 54}],
        msg="BinData 0x23 (bits 0,1,5) equivalent to numeric 35 and position list [0,1,5]",
    ),
]

ALL_TESTS = FIELD_LOOKUP_TESTS + ALGEBRAIC_TESTS


@pytest.mark.parametrize("test", pytest_params(ALL_TESTS))
def test_bitsAnyClear_field_lookup(collection, test):
    """Test $bitsAnyClear field lookup, cross-type combinations, and algebraic properties."""
    collection.insert_many(test.doc)
    result = execute_command(collection, {"find": collection.name, "filter": test.filter})
    assertSuccess(result, test.expected, ignore_doc_order=True)
