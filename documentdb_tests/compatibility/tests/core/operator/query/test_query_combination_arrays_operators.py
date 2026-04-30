"""
Tests for array operator cross-operator combinations.

Validates $size and $all combined with logical operators ($not, $and, $or, $nor),
comparison operators ($gt, $gte, $lt, $lte, $eq, $ne, $in, $nin, $exists, $type, $where).

"""

import pytest

from documentdb_tests.compatibility.tests.core.operator.query.utils.query_test_case import (
    QueryTestCase,
)
from documentdb_tests.framework.assertions import assertSuccess
from documentdb_tests.framework.executor import execute_command
from documentdb_tests.framework.parametrize import pytest_params

SIZE_ARRAY_OPS_TESTS: list[QueryTestCase] = [
    QueryTestCase(
        id="size_and_all_exact",
        filter={"a": {"$size": 2, "$all": ["a", "b"]}},
        doc=[
            {"_id": 1, "a": ["a", "b"]},
            {"_id": 2, "a": ["a", "b", "c"]},
            {"_id": 3, "a": ["a"]},
        ],
        expected=[{"_id": 1, "a": ["a", "b"]}],
        msg="$size 2 with $all ['a','b'] matches exact",
    ),
    QueryTestCase(
        id="size_larger_than_all",
        filter={"a": {"$size": 3, "$all": ["a", "b"]}},
        doc=[
            {"_id": 1, "a": ["a", "b", "c"]},
            {"_id": 2, "a": ["a", "b"]},
        ],
        expected=[{"_id": 1, "a": ["a", "b", "c"]}],
        msg="$size 3 with $all ['a','b'] matches size 3 containing both",
    ),
    QueryTestCase(
        id="size_and_elemmatch",
        filter={"a": {"$size": 1, "$elemMatch": {"$gt": 5}}},
        doc=[
            {"_id": 1, "a": [10]},
            {"_id": 2, "a": [3]},
            {"_id": 3, "a": [10, 20]},
        ],
        expected=[{"_id": 1, "a": [10]}],
        msg="$size 1 with $elemMatch {$gt: 5} matches single-element array with condition",
    ),
]

SIZE_LOGICAL_TESTS: list[QueryTestCase] = [
    QueryTestCase(
        id="not_size_2_matches_other_sizes",
        filter={"a": {"$not": {"$size": 2}}},
        doc=[
            {"_id": 1, "a": [1]},
            {"_id": 2, "a": [1, 2]},
            {"_id": 3, "a": [1, 2, 3]},
        ],
        expected=[{"_id": 1, "a": [1]}, {"_id": 3, "a": [1, 2, 3]}],
        msg="$not $size 2 matches arrays of size != 2",
    ),
    QueryTestCase(
        id="and_size_and_all",
        filter={"$and": [{"a": {"$size": 2}}, {"a": {"$all": ["a"]}}]},
        doc=[
            {"_id": 1, "a": ["a", "b"]},
            {"_id": 2, "a": ["c", "d"]},
            {"_id": 3, "a": ["a"]},
        ],
        expected=[{"_id": 1, "a": ["a", "b"]}],
        msg="$and with $size 2 and $all ['a']",
    ),
    QueryTestCase(
        id="or_size_1_or_2",
        filter={"$or": [{"a": {"$size": 1}}, {"a": {"$size": 2}}]},
        doc=[
            {"_id": 1, "a": [1]},
            {"_id": 2, "a": [1, 2]},
            {"_id": 3, "a": [1, 2, 3]},
        ],
        expected=[{"_id": 1, "a": [1]}, {"_id": 2, "a": [1, 2]}],
        msg="$or with $size 1 or $size 2",
    ),
    QueryTestCase(
        id="nor_size_0",
        filter={"$nor": [{"a": {"$size": 0}}]},
        doc=[
            {"_id": 1, "a": []},
            {"_id": 2, "a": [1]},
            {"_id": 3, "a": "x"},
            {"_id": 4, "b": 1},
        ],
        expected=[
            {"_id": 2, "a": [1]},
            {"_id": 3, "a": "x"},
            {"_id": 4, "b": 1},
        ],
        msg="$nor with $size 0 matches non-empty arrays and non-array fields",
    ),
    QueryTestCase(
        id="multi_field_size",
        filter={"a": {"$size": 2}, "b": {"$size": 1}},
        doc=[
            {"_id": 1, "a": [1, 2], "b": [3]},
            {"_id": 2, "a": [1, 2], "b": [3, 4]},
            {"_id": 3, "a": [1], "b": [3]},
        ],
        expected=[{"_id": 1, "a": [1, 2], "b": [3]}],
        msg="$size on multiple fields matches only when both conditions met",
    ),
    QueryTestCase(
        id="and_contradictory_size",
        filter={"$and": [{"a": {"$size": 1}}, {"a": {"$size": 2}}]},
        doc=[
            {"_id": 1, "a": [1]},
            {"_id": 2, "a": [1, 2]},
        ],
        expected=[],
        msg="$and with contradictory $size values returns no matches",
    ),
]

SIZE_COMPARISON_TESTS: list[QueryTestCase] = [
    QueryTestCase(
        id="size_and_exists",
        filter={"a": {"$size": 2, "$exists": True}},
        doc=[
            {"_id": 1, "a": [1, 2]},
            {"_id": 2, "b": 1},
        ],
        expected=[{"_id": 1, "a": [1, 2]}],
        msg="$size 2 with $exists true",
    ),
    QueryTestCase(
        id="size_and_ne_null",
        filter={"a": {"$size": 2, "$ne": None}},
        doc=[
            {"_id": 1, "a": [1, 2]},
            {"_id": 2, "a": None},
        ],
        expected=[{"_id": 1, "a": [1, 2]}],
        msg="$size 2 with $ne null",
    ),
    QueryTestCase(
        id="size_and_type_array",
        filter={"a": {"$size": 2, "$type": "array"}},
        doc=[
            {"_id": 1, "a": [1, 2]},
            {"_id": 2, "a": "x"},
        ],
        expected=[{"_id": 1, "a": [1, 2]}],
        msg="$size 2 with $type array (redundant but valid)",
    ),
    QueryTestCase(
        id="size_with_where",
        filter={"a": {"$size": 1}, "$where": "true"},
        doc=[
            {"_id": 1, "a": []},
            {"_id": 2, "a": [1]},
            {"_id": 3, "a": [1, 2]},
            {"_id": 4, "a": [1, 2, 3]},
        ],
        expected=[{"_id": 2, "a": [1]}],
        msg="$size 1 combined with $where 'true' returns matching doc",
    ),
]

SIZE_COMBINATION_TESTS = SIZE_ARRAY_OPS_TESTS + SIZE_LOGICAL_TESTS + SIZE_COMPARISON_TESTS


@pytest.mark.parametrize("test", pytest_params(SIZE_COMBINATION_TESTS))
def test_size_combination(collection, test):
    """Parametrized test for $size combined with array, logical, and comparison operators."""
    if test.doc:
        collection.insert_many(test.doc)
    result = execute_command(collection, {"find": collection.name, "filter": test.filter})
    assertSuccess(result, test.expected, ignore_doc_order=True)


ALL_CROSS_OPERATOR_TESTS: list[QueryTestCase] = [
    QueryTestCase(
        id="all_with_size",
        filter={"a": {"$all": ["x", "y"], "$size": 2}},
        doc=[
            {"_id": 1, "a": ["x", "y"]},
            {"_id": 2, "a": ["x", "y", "z"]},
            {"_id": 3, "a": ["x"]},
        ],
        expected=[{"_id": 1, "a": ["x", "y"]}],
        msg="$all combined with $size should require both conditions",
    ),
    QueryTestCase(
        id="all_with_nin",
        filter={"a": {"$all": ["x"], "$nin": ["y"]}},
        doc=[
            {"_id": 1, "a": ["x", "y"]},
            {"_id": 2, "a": ["x"]},
        ],
        expected=[{"_id": 2, "a": ["x"]}],
        msg="$all combined with $nin should require both conditions",
    ),
    QueryTestCase(
        id="all_with_type",
        filter={"a": {"$all": ["x"], "$type": "array"}},
        doc=[
            {"_id": 1, "a": ["x"]},
            {"_id": 2, "a": "x"},
        ],
        expected=[{"_id": 1, "a": ["x"]}],
        msg="$all combined with $type should require both conditions",
    ),
    QueryTestCase(
        id="all_with_exists",
        filter={"a": {"$all": ["x"], "$exists": True}},
        doc=[
            {"_id": 1, "a": ["x"]},
            {"_id": 2, "b": 1},
        ],
        expected=[{"_id": 1, "a": ["x"]}],
        msg="$all combined with $exists should require both conditions",
    ),
]

ALL_LOGICAL_TESTS: list[QueryTestCase] = [
    QueryTestCase(
        id="all_in_and",
        filter={"$and": [{"a": {"$all": ["x", "y"]}}, {"b": 1}]},
        doc=[
            {"_id": 1, "a": ["x", "y"], "b": 1},
            {"_id": 2, "a": ["x", "y"], "b": 2},
        ],
        expected=[{"_id": 1, "a": ["x", "y"], "b": 1}],
        msg="$all in $and with conditions on other fields should work",
    ),
    QueryTestCase(
        id="all_in_or",
        filter={"$or": [{"a": {"$all": ["x", "y"]}}, {"b": 1}]},
        doc=[
            {"_id": 1, "a": ["x", "y"]},
            {"_id": 2, "a": ["z"], "b": 1},
            {"_id": 3, "a": ["z"]},
        ],
        expected=[{"_id": 1, "a": ["x", "y"]}, {"_id": 2, "a": ["z"], "b": 1}],
        msg="$all in $or with other conditions should work",
    ),
    QueryTestCase(
        id="all_nor",
        filter={"$nor": [{"a": {"$all": ["x", "y"]}}]},
        doc=[
            {"_id": 1, "a": ["x", "y"]},
            {"_id": 2, "a": ["x"]},
        ],
        expected=[{"_id": 2, "a": ["x"]}],
        msg="$nor with $all should exclude matching documents",
    ),
]

ALL_NEGATION_TESTS: list[QueryTestCase] = [
    QueryTestCase(
        id="not_all_excludes_matching",
        filter={"a": {"$not": {"$all": ["x", "y"]}}},
        doc=[
            {"_id": 1, "a": ["x", "y"]},
            {"_id": 2, "a": ["x"]},
            {"_id": 3, "a": ["z"]},
        ],
        expected=[{"_id": 2, "a": ["x"]}, {"_id": 3, "a": ["z"]}],
        msg="$not with $all should exclude docs containing both",
    ),
    QueryTestCase(
        id="not_all_includes_null",
        filter={"a": {"$not": {"$all": ["x"]}}},
        doc=[{"_id": 1, "a": ["x"]}, {"_id": 2, "a": None}],
        expected=[{"_id": 2, "a": None}],
        msg="$not with $all should include documents where field is null",
    ),
    QueryTestCase(
        id="not_all_includes_missing",
        filter={"a": {"$not": {"$all": ["x"]}}},
        doc=[{"_id": 1, "a": ["x"]}, {"_id": 2, "b": 1}],
        expected=[{"_id": 2, "b": 1}],
        msg="$not with $all should include documents where field is missing",
    ),
    QueryTestCase(
        id="not_all_containing_all_elements",
        filter={"a": {"$not": {"$all": ["x", "y"]}}},
        doc=[{"_id": 1, "a": ["x", "y"]}],
        expected=[],
        msg="$not with $all containing all array elements should return no documents",
    ),
    QueryTestCase(
        id="not_all_containing_nonexistent",
        filter={"a": {"$not": {"$all": ["z"]}}},
        doc=[{"_id": 1, "a": ["x", "y"]}],
        expected=[{"_id": 1, "a": ["x", "y"]}],
        msg="$not with $all containing non-existent element should return the document",
    ),
]

ALL_COMPARISON_TESTS: list[QueryTestCase] = [
    QueryTestCase(
        id="all_with_in",
        filter={"a": {"$all": ["x", "y"], "$in": ["x", "z"]}},
        doc=[
            {"_id": 1, "a": ["x", "y"]},
            {"_id": 2, "a": ["x"]},
        ],
        expected=[{"_id": 1, "a": ["x", "y"]}],
        msg="$all with $in should require both conditions",
    ),
    QueryTestCase(
        id="all_with_gt",
        filter={"a": {"$all": [1, 2], "$gt": 0}},
        doc=[
            {"_id": 1, "a": [1, 2, 3]},
            {"_id": 2, "a": [1]},
        ],
        expected=[{"_id": 1, "a": [1, 2, 3]}],
        msg="$all with $gt should require both conditions",
    ),
    QueryTestCase(
        id="all_with_eq",
        filter={"a": {"$all": ["x"], "$eq": "x"}},
        doc=[{"_id": 1, "a": "x"}, {"_id": 2, "a": ["x", "y"]}],
        expected=[{"_id": 1, "a": "x"}, {"_id": 2, "a": ["x", "y"]}],
        msg="$all with $eq should require both conditions",
    ),
    QueryTestCase(
        id="all_with_ne",
        filter={"a": {"$all": ["x"], "$ne": "y"}},
        doc=[
            {"_id": 1, "a": ["x"]},
            {"_id": 2, "a": ["x", "y"]},
        ],
        expected=[{"_id": 1, "a": ["x"]}],
        msg="$all with $ne should require both conditions",
    ),
    QueryTestCase(
        id="all_with_gte",
        filter={"a": {"$all": [1, 2], "$gte": 1}},
        doc=[
            {"_id": 1, "a": [1, 2, 3]},
            {"_id": 2, "a": [1]},
        ],
        expected=[{"_id": 1, "a": [1, 2, 3]}],
        msg="$all with $gte should require both conditions",
    ),
    QueryTestCase(
        id="all_with_lt",
        filter={"a": {"$all": [1, 2], "$lt": 5}},
        doc=[
            {"_id": 1, "a": [1, 2, 3]},
            {"_id": 2, "a": [1]},
        ],
        expected=[{"_id": 1, "a": [1, 2, 3]}],
        msg="$all with $lt should require both conditions",
    ),
    QueryTestCase(
        id="all_with_lte",
        filter={"a": {"$all": [1, 2], "$lte": 2}},
        doc=[
            {"_id": 1, "a": [1, 2, 3]},
            {"_id": 2, "a": [1]},
        ],
        expected=[{"_id": 1, "a": [1, 2, 3]}],
        msg="$all with $lte should require both conditions",
    ),
]

ALL_COMBINATION_TESTS = (
    ALL_CROSS_OPERATOR_TESTS + ALL_LOGICAL_TESTS + ALL_NEGATION_TESTS + ALL_COMPARISON_TESTS
)


@pytest.mark.parametrize("test", pytest_params(ALL_COMBINATION_TESTS))
def test_all_combination(collection, test):
    """Parametrized test for $all combined with other operators."""
    collection.insert_many(test.doc)
    result = execute_command(collection, {"find": collection.name, "filter": test.filter})
    assertSuccess(result, test.expected, ignore_doc_order=True)
