"""Aggregation $group stage tests."""

from __future__ import annotations

from datetime import datetime

import pytest
from bson import SON, Binary, Code, Decimal128, Int64, MaxKey, MinKey, ObjectId, Regex, Timestamp

from documentdb_tests.compatibility.tests.core.operator.stages.utils.stage_test_case import (
    StageTestCase,
    populate_collection,
)
from documentdb_tests.framework.assertions import (
    assertResult,
    assertSuccess,
    assertSuccessNaN,
)
from documentdb_tests.framework.error_codes import (
    ACCUMULATOR_MISSING_ACCUMULATE_ARGS_ERROR,
    ACCUMULATOR_NULL_FUNCTION_ERROR,
    BAD_VALUE_ERROR,
    BSON_OBJECT_TOO_LARGE_ERROR,
    CLUSTER_TIME_STANDALONE_ERROR,
    FAILED_TO_PARSE_ERROR,
    FIELD_PATH_DOT_ERROR,
    FIELD_PATH_EMPTY_COMPONENT_ERROR,
    GROUP_ACCUMULATOR_ARRAY_ARGUMENT_ERROR,
    GROUP_ACCUMULATOR_DOLLAR_FIELD_NAME_ERROR,
    GROUP_ACCUMULATOR_DOT_FIELD_NAME_ERROR,
    GROUP_ACCUMULATOR_INVALID_VALUE_ERROR,
    GROUP_ACCUMULATOR_MULTIPLE_KEYS_ERROR,
    GROUP_INCLUSION_STYLE_ERROR,
    GROUP_MISSING_ID_ERROR,
    GROUP_NON_OBJECT_ERROR,
    GROUP_UNKNOWN_OPERATOR_ERROR,
    INVALID_DOLLAR_FIELD_PATH,
    MERGE_OBJECTS_NON_OBJECT_ERROR,
    MISSING_REQUIRED_FIELD_ERROR,
    N_ACCUMULATOR_INVALID_N_ERROR,
    N_ACCUMULATOR_MISSING_N_FIRSTN_FAMILY_ERROR,
    N_ACCUMULATOR_MISSING_N_TOPN_FAMILY_ERROR,
    PERCENTILE_INVALID_P_FIELD_ERROR,
    PERCENTILE_INVALID_P_VALUE_ERROR,
    QUERY_EXCEEDED_MEMORY_NO_DISK_USE_ERROR,
    TYPE_MISMATCH_ERROR,
    UNRECOGNIZED_EXPRESSION_ERROR,
)
from documentdb_tests.framework.executor import execute_command
from documentdb_tests.framework.parametrize import pytest_params
from documentdb_tests.framework.test_constants import (
    DECIMAL128_INFINITY,
    DECIMAL128_INT64_OVERFLOW,
    DECIMAL128_LARGE_EXPONENT,
    DECIMAL128_MIN,
    DECIMAL128_MIN_POSITIVE,
    DECIMAL128_NEGATIVE_INFINITY,
    DECIMAL128_NEGATIVE_ZERO,
    DECIMAL128_ONE_AND_HALF,
    DECIMAL128_TRAILING_ZERO,
    DECIMAL128_TWO_AND_HALF,
    DECIMAL128_ZERO,
    DOUBLE_FROM_INT64_MAX,
    DOUBLE_MAX,
    DOUBLE_MAX_SAFE_INTEGER,
    DOUBLE_MIN,
    DOUBLE_MIN_SUBNORMAL,
    DOUBLE_NEGATIVE_ZERO,
    DOUBLE_ZERO,
    FLOAT_INFINITY,
    FLOAT_NAN,
    FLOAT_NEGATIVE_INFINITY,
    INT32_MAX,
    INT32_MAX_MINUS_1,
    INT32_MIN,
    INT32_MIN_PLUS_1,
    INT64_MAX,
    INT64_MAX_MINUS_1,
    INT64_MIN,
    INT64_MIN_PLUS_1,
    INT64_ZERO,
    TS_MAX_SIGNED32,
    TS_MAX_UNSIGNED32,
)

# Property [Simple _id Null Equivalence]: in a simple (non-compound) _id, a
# missing field reference, $literal null, an expression evaluating to null, and
# $$REMOVE all evaluate to null and group together with explicit null.
GROUP_SIMPLE_NULL_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="missing_field_groups_with_null",
        docs=[
            {"_id": 1, "v": "a"},
            {"_id": 2},
            {"_id": 3, "v": None},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[
            {"_id": "a", "ids": [1]},
            {"_id": None, "ids": [2, 3]},
        ],
        msg="Missing field reference and explicit null should group together",
    ),
    StageTestCase(
        id="literal_null_groups_all",
        docs=[{"_id": 1, "v": 10}, {"_id": 2, "v": 20}],
        pipeline=[{"$group": {"_id": {"$literal": None}, "ids": {"$push": "$_id"}}}],
        expected=[{"_id": None, "ids": [1, 2]}],
        msg="$literal null should group all documents under null",
    ),
    StageTestCase(
        id="expression_null_groups_with_null",
        docs=[
            {"_id": 1, "v": None},
            {"_id": 2, "v": 10},
        ],
        pipeline=[
            {
                "$group": {
                    "_id": {"$cond": [{"$eq": ["$v", None]}, None, "$v"]},
                    "ids": {"$push": "$_id"},
                }
            }
        ],
        expected=[
            {"_id": None, "ids": [1]},
            {"_id": 10, "ids": [2]},
        ],
        msg="Expression evaluating to null should group with explicit null",
    ),
    StageTestCase(
        id="remove_in_simple_id",
        docs=[{"_id": 1, "v": 10}, {"_id": 2, "v": 20}],
        pipeline=[{"$group": {"_id": "$$REMOVE", "ids": {"$push": "$_id"}}}],
        expected=[{"_id": None, "ids": [1, 2]}],
        msg="$$REMOVE in simple _id should evaluate to null, grouping all docs",
    ),
]

# Property [Compound _id Null vs Missing]: in a compound _id, a field set to
# null produces a key containing that field with a null value, while $$REMOVE
# or a missing field reference omits the field entirely, producing a different
# group key.
GROUP_COMPOUND_NULL_MISSING_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="compound_null_vs_missing_field",
        docs=[
            {"_id": 1, "v": None},
            {"_id": 2},
            {"_id": 3, "v": "hello"},
        ],
        pipeline=[{"$group": {"_id": {"x": "$v", "y": "constant"}, "ids": {"$push": "$_id"}}}],
        expected=[
            {"_id": {"x": None, "y": "constant"}, "ids": [1]},
            {"_id": {"y": "constant"}, "ids": [2]},
            {"_id": {"x": "hello", "y": "constant"}, "ids": [3]},
        ],
        msg=(
            "Compound _id: null field produces {x: null, y: ...} while missing"
            " field omits x, producing separate groups"
        ),
    ),
    StageTestCase(
        id="compound_remove_matches_missing",
        docs=[
            {"_id": 1, "v": "remove_me"},
            {"_id": 2},
        ],
        pipeline=[
            {
                "$group": {
                    "_id": {
                        "x": {
                            "$cond": [
                                {"$eq": ["$v", "remove_me"]},
                                "$$REMOVE",
                                "$v",
                            ]
                        },
                        "y": "constant",
                    },
                    "ids": {"$push": "$_id"},
                }
            }
        ],
        expected=[{"_id": {"y": "constant"}, "ids": [1, 2]}],
        msg=(
            "$$REMOVE in compound _id removes the field, producing the same"
            " group key as a document where that field is missing"
        ),
    ),
    StageTestCase(
        id="compound_remove_vs_null_separate",
        docs=[
            {"_id": 1, "v": "remove_me"},
            {"_id": 2, "v": "null_me"},
        ],
        pipeline=[
            {
                "$group": {
                    "_id": {
                        "x": {
                            "$cond": [
                                {"$eq": ["$v", "remove_me"]},
                                "$$REMOVE",
                                None,
                            ]
                        },
                        "y": "constant",
                    },
                    "ids": {"$push": "$_id"},
                }
            }
        ],
        expected=[
            {"_id": {"y": "constant"}, "ids": [1]},
            {"_id": {"x": None, "y": "constant"}, "ids": [2]},
        ],
        msg=(
            "$$REMOVE and null in compound _id produce different group keys:"
            " removed field vs present-with-null"
        ),
    ),
]

GROUP_NULL_MISSING_TESTS = GROUP_SIMPLE_NULL_TESTS + GROUP_COMPOUND_NULL_MISSING_TESTS

# Property [Distinct Group Keys]: each distinct _id value produces exactly one
# output document, and the output _id field reflects the evaluated group key.
GROUP_DISTINCT_KEY_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="distinct_values_produce_one_doc_each",
        docs=[
            {"_id": 1, "v": "a"},
            {"_id": 2, "v": "b"},
            {"_id": 3, "v": "a"},
            {"_id": 4, "v": "c"},
            {"_id": 5, "v": "b"},
        ],
        pipeline=[{"$group": {"_id": "$v", "count": {"$sum": 1}}}],
        expected=[
            {"_id": "a", "count": 2},
            {"_id": "b", "count": 2},
            {"_id": "c", "count": 1},
        ],
        msg="Each distinct _id value should produce exactly one output document",
    ),
    StageTestCase(
        id="output_id_reflects_group_key",
        docs=[{"_id": 1, "v": 42}, {"_id": 2, "v": 42}],
        pipeline=[{"$group": {"_id": "$v", "count": {"$sum": 1}}}],
        expected=[{"_id": 42, "count": 2}],
        msg="Output _id should reflect the evaluated group key value",
    ),
]

# Property [Null and Constant _id]: _id set to null or any other constant
# produces a single group that aggregates all input documents.
GROUP_CONSTANT_ID_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="null_id_single_group",
        docs=[{"_id": 1, "v": 10}, {"_id": 2, "v": 20}, {"_id": 3, "v": 30}],
        pipeline=[{"$group": {"_id": None, "total": {"$sum": "$v"}}}],
        expected=[{"_id": None, "total": 60}],
        msg="_id: null should produce a single group aggregating all documents",
    ),
    StageTestCase(
        id="constant_int_id_single_group",
        docs=[{"_id": 1, "v": 10}, {"_id": 2, "v": 20}],
        pipeline=[{"$group": {"_id": 1, "total": {"$sum": "$v"}}}],
        expected=[{"_id": 1, "total": 30}],
        msg="_id: 1 should produce a single group aggregating all documents",
    ),
]

# Property [_id Accepts Field References, Expressions, and Compound Keys]: _id
# can be a field reference, an expression (such as $cond or $add), or a
# subdocument with multiple fields, and each form correctly determines the
# group key.
GROUP_ID_FORMS_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="expression_id_cond",
        docs=[
            {"_id": 1, "score": 80},
            {"_id": 2, "score": 40},
            {"_id": 3, "score": 90},
        ],
        pipeline=[
            {
                "$group": {
                    "_id": {"$cond": [{"$gte": ["$score", 50]}, "pass", "fail"]},
                    "count": {"$sum": 1},
                }
            }
        ],
        expected=[
            {"_id": "pass", "count": 2},
            {"_id": "fail", "count": 1},
        ],
        msg="_id as a $cond expression should group by the expression result",
    ),
    StageTestCase(
        id="compound_key_id",
        docs=[
            {"_id": 1, "dept": "eng", "level": "senior"},
            {"_id": 2, "dept": "eng", "level": "junior"},
            {"_id": 3, "dept": "eng", "level": "senior"},
        ],
        pipeline=[
            {
                "$group": {
                    "_id": {"dept": "$dept", "level": "$level"},
                    "count": {"$sum": 1},
                }
            }
        ],
        expected=[
            {"_id": {"dept": "eng", "level": "senior"}, "count": 2},
            {"_id": {"dept": "eng", "level": "junior"}, "count": 1},
        ],
        msg="_id as a compound key should group by the subdocument",
    ),
    StageTestCase(
        id="id_add_expression",
        docs=[
            {"_id": 1, "x": 10, "y": 5},
            {"_id": 2, "x": 10, "y": 5},
            {"_id": 3, "x": 20, "y": 3},
        ],
        pipeline=[{"$group": {"_id": {"$add": ["$x", "$y"]}, "count": {"$sum": 1}}}],
        expected=[
            {"_id": 15, "count": 2},
            {"_id": 23, "count": 1},
        ],
        msg="_id with $add expression should group by the computed sum",
    ),
]

# Property [String Literal vs Field Reference]: a non-$-prefixed string in _id
# is a constant value, while a $-prefixed string is a field reference.
GROUP_STRING_LITERAL_VS_REF_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="non_dollar_string_is_constant",
        docs=[{"_id": 1, "v": 10}, {"_id": 2, "v": 20}],
        pipeline=[{"$group": {"_id": "hello", "count": {"$sum": 1}}}],
        expected=[{"_id": "hello", "count": 2}],
        msg=(
            "Non-$-prefixed string in _id should be treated as a constant,"
            " grouping all documents together"
        ),
    ),
    StageTestCase(
        id="dollar_string_is_field_ref",
        docs=[
            {"_id": 1, "hello": "x"},
            {"_id": 2, "hello": "y"},
            {"_id": 3, "hello": "x"},
        ],
        pipeline=[{"$group": {"_id": "$hello", "count": {"$sum": 1}}}],
        expected=[
            {"_id": "x", "count": 2},
            {"_id": "y", "count": 1},
        ],
        msg="$-prefixed string in _id should be treated as a field reference",
    ),
]

GROUP_KEY_BEHAVIOR_TESTS = (
    GROUP_DISTINCT_KEY_TESTS
    + GROUP_CONSTANT_ID_TESTS
    + GROUP_ID_FORMS_TESTS
    + GROUP_STRING_LITERAL_VS_REF_TESTS
)

# Property [Numeric Grouping Equivalence]: numeric types with the same
# mathematical value (int32, Int64, double, Decimal128) group together into one
# group, and Decimal128 values differing only in trailing zeros are equivalent.
GROUP_NUMERIC_EQUIVALENCE_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="int32_int64_double_decimal128_same_value",
        docs=[
            {"_id": 1, "v": 1},
            {"_id": 2, "v": Int64(1)},
            {"_id": 3, "v": 1.0},
            {"_id": 4, "v": Decimal128("1")},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[{"_id": 1, "ids": [1, 2, 3, 4]}],
        msg="int32, Int64, double, and Decimal128 of the same value should group together",
    ),
    StageTestCase(
        id="decimal128_trailing_zeros",
        docs=[
            {"_id": 1, "v": Decimal128("1")},
            {"_id": 2, "v": DECIMAL128_TRAILING_ZERO},
            {"_id": 3, "v": Decimal128("1.00")},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[{"_id": Decimal128("1"), "ids": [1, 2, 3]}],
        msg="Decimal128 values differing only in trailing zeros should group together",
    ),
]

# Property [Numeric Group _id Type]: the output _id type for a numeric group
# is the type of the first document encountered in that group.
GROUP_NUMERIC_ID_TYPE_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="numeric_group_id_type_is_first_doc_type",
        docs=[
            {"_id": 1, "v": Int64(1)},
            {"_id": 2, "v": 1},
            {"_id": 3, "v": 1.0},
            {"_id": 4, "v": Decimal128("1")},
        ],
        pipeline=[
            {"$group": {"_id": "$v", "count": {"$sum": 1}}},
            {"$project": {"id_type": {"$type": "$_id"}}},
        ],
        expected=[{"_id": Int64(1), "id_type": "long"}],
        msg="Output _id type should be the type of the first document encountered",
    ),
]

# Property [Zero Variant Equivalence]: all zero representations (-0.0, 0.0,
# int32(0), Int64(0), Decimal128("0"), Decimal128("-0")) produce one group.
GROUP_ZERO_EQUIVALENCE_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="all_zero_variants_group_together",
        docs=[
            {"_id": 1, "v": DOUBLE_NEGATIVE_ZERO},
            {"_id": 2, "v": DOUBLE_ZERO},
            {"_id": 3, "v": 0},
            {"_id": 4, "v": INT64_ZERO},
            {"_id": 5, "v": DECIMAL128_ZERO},
            {"_id": 6, "v": DECIMAL128_NEGATIVE_ZERO},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[{"_id": DOUBLE_NEGATIVE_ZERO, "ids": [1, 2, 3, 4, 5, 6]}],
        msg="All zero variants should produce one group",
    ),
]

# Property [Infinity Equivalence]: positive infinity from float and Decimal128
# group together, and negative infinity from float and Decimal128 group
# together.
GROUP_INFINITY_EQUIVALENCE_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="positive_infinity_variants",
        docs=[
            {"_id": 1, "v": FLOAT_INFINITY},
            {"_id": 2, "v": DECIMAL128_INFINITY},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[{"_id": FLOAT_INFINITY, "ids": [1, 2]}],
        msg="float inf and Decimal128 Infinity should group together",
    ),
    StageTestCase(
        id="negative_infinity_variants",
        docs=[
            {"_id": 1, "v": FLOAT_NEGATIVE_INFINITY},
            {"_id": 2, "v": DECIMAL128_NEGATIVE_INFINITY},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[{"_id": FLOAT_NEGATIVE_INFINITY, "ids": [1, 2]}],
        msg="float -inf and Decimal128 -Infinity should group together",
    ),
]

# Property [Boolean vs Numeric Distinction]: boolean values do not group with
# their numeric equivalents - True and 1 produce separate groups, as do False
# and 0.
GROUP_BOOL_NUMERIC_DISTINCTION_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="true_not_equal_to_one",
        docs=[
            {"_id": 1, "v": True},
            {"_id": 2, "v": 1},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[
            {"_id": True, "ids": [1]},
            {"_id": 1, "ids": [2]},
        ],
        msg="True and 1 should produce separate groups",
    ),
    StageTestCase(
        id="false_not_equal_to_zero",
        docs=[
            {"_id": 1, "v": False},
            {"_id": 2, "v": 0},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[
            {"_id": False, "ids": [1]},
            {"_id": 0, "ids": [2]},
        ],
        msg="False and 0 should produce separate groups",
    ),
]

# Property [String Comparison Strictness]: string grouping is case-sensitive,
# does not apply Unicode normalization, and treats null bytes as significant.
GROUP_STRING_COMPARISON_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="case_sensitive_strings",
        docs=[
            {"_id": 1, "v": "a"},
            {"_id": 2, "v": "A"},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[
            {"_id": "A", "ids": [2]},
            {"_id": "a", "ids": [1]},
        ],
        msg="String comparison should be case-sensitive",
    ),
    StageTestCase(
        id="no_unicode_normalization",
        docs=[
            {"_id": 1, "v": "\u00e9"},
            {"_id": 2, "v": "e\u0301"},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[
            {"_id": "\u00e9", "ids": [1]},
            {"_id": "e\u0301", "ids": [2]},
        ],
        msg="Precomposed and decomposed Unicode forms should produce separate groups",
    ),
    StageTestCase(
        id="null_bytes_significant",
        docs=[
            {"_id": 1, "v": "a\x00b"},
            {"_id": 2, "v": "a"},
            {"_id": 3, "v": "ab"},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[
            {"_id": "a", "ids": [2]},
            {"_id": "a\x00b", "ids": [1]},
            {"_id": "ab", "ids": [3]},
        ],
        msg="Null bytes should be significant in string grouping",
    ),
]

# Property [Regex Flag Distinction]: regex values with the same pattern but
# different flags produce separate groups.
GROUP_REGEX_FLAG_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="regex_different_flags_separate",
        docs=[
            {"_id": 1, "v": Regex("abc", "i")},
            {"_id": 2, "v": Regex("abc", "m")},
            {"_id": 3, "v": Regex("abc", "i")},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[
            {"_id": Regex("abc", "i"), "ids": [1, 3]},
            {"_id": Regex("abc", "m"), "ids": [2]},
        ],
        msg="Regex with same pattern but different flags should produce separate groups",
    ),
]

# Property [Code Scope Distinction]: Code without scope, Code with empty
# scope, and Code with non-empty scope produce three separate groups.
GROUP_CODE_SCOPE_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="code_scope_variants_separate",
        docs=[
            {"_id": 1, "v": Code("function(){}")},
            {"_id": 2, "v": Code("function(){}", {})},
            {"_id": 3, "v": Code("function(){}", {"x": 1})},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[
            {"_id": Code("function(){}"), "ids": [1]},
            {"_id": Code("function(){}", {}), "ids": [2]},
            {"_id": Code("function(){}", {"x": 1}), "ids": [3]},
        ],
        msg="Code, Code with empty scope, and Code with non-empty scope should be separate groups",
    ),
]

# Property [Binary Subtype Distinction]: binary values with the same data but
# different subtypes produce separate groups.
GROUP_BINARY_SUBTYPE_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="binary_different_subtypes_separate",
        docs=[
            {"_id": 1, "v": Binary(b"hello", 0)},
            {"_id": 2, "v": Binary(b"hello", 5)},
            {"_id": 3, "v": Binary(b"hello", 0)},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[
            {"_id": b"hello", "ids": [1, 3]},
            {"_id": Binary(b"hello", 5), "ids": [2]},
        ],
        msg="Binary with same data but different subtypes should produce separate groups",
    ),
]

# Property [Compound _id Field Order]: in a compound _id, field order matters
# for grouping - documents with the same fields in different order produce
# separate groups.
GROUP_COMPOUND_FIELD_ORDER_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="compound_id_field_order_matters",
        docs=[
            {"_id": 1, "v": SON([("a", 1), ("b", 2)])},
            {"_id": 2, "v": SON([("b", 2), ("a", 1)])},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[
            {"_id": {"a": 1, "b": 2}, "ids": [1]},
            {"_id": {"b": 2, "a": 1}, "ids": [2]},
        ],
        msg="Compound _id with different field order should produce separate groups",
    ),
]

# Property [Array Grouping Equivalence]: numeric equivalence applies within
# arrays, boolean vs numeric distinction applies within arrays, and array
# element order matters for grouping.
GROUP_ARRAY_EQUIVALENCE_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="array_numeric_equivalence",
        docs=[
            {"_id": 1, "v": [1]},
            {"_id": 2, "v": [Int64(1)]},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[{"_id": [1], "ids": [1, 2]}],
        msg="Arrays with numerically equivalent elements should group together",
    ),
    StageTestCase(
        id="array_bool_vs_numeric_distinct",
        docs=[
            {"_id": 1, "v": [True]},
            {"_id": 2, "v": [1]},
            {"_id": 3, "v": [False]},
            {"_id": 4, "v": [0]},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[
            {"_id": [0], "ids": [4]},
            {"_id": [1], "ids": [2]},
            {"_id": [False], "ids": [3]},
            {"_id": [True], "ids": [1]},
        ],
        msg="[True] and [1], [False] and [0] should produce separate groups",
    ),
    StageTestCase(
        id="array_element_order_matters",
        docs=[
            {"_id": 1, "v": [1, 2]},
            {"_id": 2, "v": [2, 1]},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[
            {"_id": [1, 2], "ids": [1]},
            {"_id": [2, 1], "ids": [2]},
        ],
        msg="Arrays with different element order should produce separate groups",
    ),
]

# Property [IEEE 754 Precision Boundary]: when a double cannot exactly
# represent an Int64 value, they form separate groups; a double that rounds to
# a representable value groups with the integer matching that rounded value.
GROUP_IEEE754_PRECISION_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="double_int64_precision_loss_separate",
        docs=[
            {"_id": 1, "v": DOUBLE_FROM_INT64_MAX},
            {"_id": 2, "v": INT64_MAX},
        ],
        pipeline=[{"$group": {"_id": "$v", "count": {"$sum": 1}}}],
        expected=[
            {"_id": INT64_MAX, "count": 1},
            {"_id": DOUBLE_FROM_INT64_MAX, "count": 1},
        ],
        msg="Double that cannot exactly represent Int64 max should form a separate group",
    ),
    StageTestCase(
        id="double_rounds_to_nearest_groups_with_int",
        docs=[
            {"_id": 1, "v": float(DOUBLE_MAX_SAFE_INTEGER + 1)},
            {"_id": 2, "v": Int64(DOUBLE_MAX_SAFE_INTEGER)},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[{"_id": float(DOUBLE_MAX_SAFE_INTEGER), "ids": [1, 2]}],
        msg=(
            "Double that rounds to nearest representable value should group"
            " with the matching integer"
        ),
    ),
]

GROUP_GROUPING_EQUIVALENCE_TESTS = (
    GROUP_NUMERIC_EQUIVALENCE_TESTS
    + GROUP_NUMERIC_ID_TYPE_TESTS
    + GROUP_ZERO_EQUIVALENCE_TESTS
    + GROUP_INFINITY_EQUIVALENCE_TESTS
    + GROUP_BOOL_NUMERIC_DISTINCTION_TESTS
    + GROUP_STRING_COMPARISON_TESTS
    + GROUP_REGEX_FLAG_TESTS
    + GROUP_CODE_SCOPE_TESTS
    + GROUP_BINARY_SUBTYPE_TESTS
    + GROUP_COMPOUND_FIELD_ORDER_TESTS
    + GROUP_ARRAY_EQUIVALENCE_TESTS
    + GROUP_IEEE754_PRECISION_TESTS
)

# Property [Literal BSON Type Acceptance (_id)]: all standard BSON types are
# accepted as literal _id values, object _id values are accepted when fields
# contain non-numeric/non-bool values or are wrapped with $literal, and empty
# object {} groups all documents together and is distinct from null.
GROUP_TYPE_ACCEPTANCE_ID_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="id_int64",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": Int64(42), "count": {"$sum": 1}}}],
        expected=[{"_id": Int64(42), "count": 2}],
        msg="Int64 literal _id should be accepted",
    ),
    StageTestCase(
        id="id_double",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": 3.14, "count": {"$sum": 1}}}],
        expected=[{"_id": 3.14, "count": 2}],
        msg="double literal _id should be accepted",
    ),
    StageTestCase(
        id="id_decimal128",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": Decimal128("3.14"), "count": {"$sum": 1}}}],
        expected=[{"_id": Decimal128("3.14"), "count": 2}],
        msg="Decimal128 literal _id should be accepted",
    ),
    StageTestCase(
        id="id_bool",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": True, "count": {"$sum": 1}}}],
        expected=[{"_id": True, "count": 2}],
        msg="bool literal _id should be accepted",
    ),
    StageTestCase(
        id="id_array",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": [1, "a"], "count": {"$sum": 1}}}],
        expected=[{"_id": [1, "a"], "count": 2}],
        msg="array literal _id should be accepted",
    ),
    StageTestCase(
        id="id_objectid",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": ObjectId("507f1f77bcf86cd799439011"), "count": {"$sum": 1}}}],
        expected=[{"_id": ObjectId("507f1f77bcf86cd799439011"), "count": 2}],
        msg="ObjectId literal _id should be accepted",
    ),
    StageTestCase(
        id="id_datetime",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": datetime(2024, 1, 1), "count": {"$sum": 1}}}],
        expected=[{"_id": datetime(2024, 1, 1), "count": 2}],
        msg="datetime literal _id should be accepted",
    ),
    StageTestCase(
        id="id_timestamp",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": Timestamp(1, 1), "count": {"$sum": 1}}}],
        expected=[{"_id": Timestamp(1, 1), "count": 2}],
        msg="Timestamp literal _id should be accepted",
    ),
    StageTestCase(
        id="id_binary",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": Binary(b"data"), "count": {"$sum": 1}}}],
        expected=[{"_id": b"data", "count": 2}],
        msg="Binary literal _id should be accepted",
    ),
    StageTestCase(
        id="id_regex",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": Regex("abc", "i"), "count": {"$sum": 1}}}],
        expected=[{"_id": Regex("abc", "i"), "count": 2}],
        msg="Regex literal _id should be accepted",
    ),
    StageTestCase(
        id="id_code",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": Code("function(){}"), "count": {"$sum": 1}}}],
        expected=[{"_id": Code("function(){}"), "count": 2}],
        msg="Code literal _id should be accepted",
    ),
    StageTestCase(
        id="id_code_with_scope",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": Code("function(){}", {"x": 1}), "count": {"$sum": 1}}}],
        expected=[{"_id": Code("function(){}", {"x": 1}), "count": 2}],
        msg="Code with scope literal _id should be accepted",
    ),
    StageTestCase(
        id="id_minkey",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": MinKey(), "count": {"$sum": 1}}}],
        expected=[{"_id": MinKey(), "count": 2}],
        msg="MinKey literal _id should be accepted",
    ),
    StageTestCase(
        id="id_maxkey",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": MaxKey(), "count": {"$sum": 1}}}],
        expected=[{"_id": MaxKey(), "count": 2}],
        msg="MaxKey literal _id should be accepted",
    ),
    StageTestCase(
        id="id_object_with_non_numeric_fields",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[
            {"$group": {"_id": {"a": "hello", "b": None, "c": [1, 2]}, "count": {"$sum": 1}}}
        ],
        expected=[{"_id": {"a": "hello", "b": None, "c": [1, 2]}, "count": 2}],
        msg="Object _id with non-numeric/non-bool field values should be accepted",
    ),
    StageTestCase(
        id="id_empty_object",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": {}, "count": {"$sum": 1}}}],
        expected=[{"_id": {}, "count": 2}],
        msg="Empty object {} as _id should group all documents together",
    ),
    StageTestCase(
        id="id_empty_object_distinct_from_null",
        docs=[{"_id": 1, "v": "use_empty"}, {"_id": 2, "v": "use_null"}],
        pipeline=[
            {
                "$group": {
                    "_id": {"$cond": [{"$eq": ["$v", "use_empty"]}, {}, None]},
                    "ids": {"$push": "$_id"},
                }
            }
        ],
        expected=[
            {"_id": {}, "ids": [1]},
            {"_id": None, "ids": [2]},
        ],
        msg="Empty object {} and null should produce separate groups",
    ),
]

# Property [Numeric Boundary Values in _id]: int32, Int64, double, and
# Decimal128 boundary values are accepted as _id and form distinct groups.
GROUP_NUMERIC_BOUNDARY_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="int32_boundary_values",
        docs=[
            {"_id": 1, "v": INT32_MAX},
            {"_id": 2, "v": INT32_MIN},
            {"_id": 3, "v": INT32_MAX_MINUS_1},
            {"_id": 4, "v": INT32_MIN_PLUS_1},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[
            {"_id": INT32_MAX, "ids": [1]},
            {"_id": INT32_MIN, "ids": [2]},
            {"_id": INT32_MAX_MINUS_1, "ids": [3]},
            {"_id": INT32_MIN_PLUS_1, "ids": [4]},
        ],
        msg="int32 boundary values should form distinct groups",
    ),
    StageTestCase(
        id="int64_boundary_values",
        docs=[
            {"_id": 1, "v": INT64_MAX},
            {"_id": 2, "v": INT64_MIN},
            {"_id": 3, "v": INT64_MAX_MINUS_1},
            {"_id": 4, "v": INT64_MIN_PLUS_1},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[
            {"_id": INT64_MAX, "ids": [1]},
            {"_id": INT64_MIN, "ids": [2]},
            {"_id": INT64_MAX_MINUS_1, "ids": [3]},
            {"_id": INT64_MIN_PLUS_1, "ids": [4]},
        ],
        msg="Int64 boundary values should form distinct groups",
    ),
    StageTestCase(
        id="double_special_values",
        docs=[
            {"_id": 1, "v": DOUBLE_MAX},
            {"_id": 2, "v": DOUBLE_MIN},
            {"_id": 3, "v": DOUBLE_MIN_SUBNORMAL},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[
            {"_id": DOUBLE_MAX, "ids": [1]},
            {"_id": DOUBLE_MIN, "ids": [2]},
            {"_id": DOUBLE_MIN_SUBNORMAL, "ids": [3]},
        ],
        msg="Double boundary and subnormal values should be accepted as _id",
    ),
    StageTestCase(
        id="decimal128_extreme_exponents",
        docs=[
            {"_id": 1, "v": DECIMAL128_MIN_POSITIVE},
            {"_id": 2, "v": DECIMAL128_LARGE_EXPONENT},
            {"_id": 3, "v": DECIMAL128_MIN},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[
            {"_id": DECIMAL128_MIN_POSITIVE, "ids": [1]},
            {"_id": DECIMAL128_LARGE_EXPONENT, "ids": [2]},
            {"_id": DECIMAL128_MIN, "ids": [3]},
        ],
        msg="Decimal128 extreme exponent values should be preserved exactly",
    ),
    StageTestCase(
        id="decimal128_int64_overflow_separate",
        docs=[
            {"_id": 1, "v": INT64_MAX},
            {"_id": 2, "v": DECIMAL128_INT64_OVERFLOW},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[
            {"_id": INT64_MAX, "ids": [1]},
            {"_id": DECIMAL128_INT64_OVERFLOW, "ids": [2]},
        ],
        msg="Decimal128 at Int64 overflow boundary should form a separate group"
        " from the maximum Int64 value",
    ),
]

# Property [Array _id Values]: arrays of any shape are accepted as _id and
# their structure is preserved in the output.
GROUP_ARRAY_ID_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="empty_array_id",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": [], "count": {"$sum": 1}}}],
        expected=[{"_id": [], "count": 2}],
        msg="Empty array [] as _id should group all documents together",
    ),
    StageTestCase(
        id="single_element_array_id",
        docs=[
            {"_id": 1, "v": [10]},
            {"_id": 2, "v": [20]},
            {"_id": 3, "v": [10]},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[
            {"_id": [10], "ids": [1, 3]},
            {"_id": [20], "ids": [2]},
        ],
        msg="Single-element array should be accepted as _id",
    ),
    StageTestCase(
        id="multi_element_array_id",
        docs=[
            {"_id": 1, "v": [1, 2, 3]},
            {"_id": 2, "v": [4, 5, 6]},
            {"_id": 3, "v": [1, 2, 3]},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[
            {"_id": [1, 2, 3], "ids": [1, 3]},
            {"_id": [4, 5, 6], "ids": [2]},
        ],
        msg="Multi-element array should be accepted as _id",
    ),
    StageTestCase(
        id="nested_array_id",
        docs=[
            {"_id": 1, "v": [[1, 2], [3]]},
            {"_id": 2, "v": [[1, 2], [3]]},
            {"_id": 3, "v": [[1], [2, 3]]},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[
            {"_id": [[1, 2], [3]], "ids": [1, 2]},
            {"_id": [[1], [2, 3]], "ids": [3]},
        ],
        msg="Nested array should be accepted as _id and structure preserved",
    ),
    StageTestCase(
        id="deeply_nested_array_id",
        docs=[
            {"_id": 1, "v": [[[1]]]},
            {"_id": 2, "v": [[[1]]]},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[{"_id": [[[1]]], "ids": [1, 2]}],
        msg="Deeply nested array should be accepted as _id",
    ),
    StageTestCase(
        id="large_array_id",
        docs=[
            {"_id": 1, "v": list(range(150))},
            {"_id": 2, "v": list(range(150))},
        ],
        pipeline=[{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
        expected=[{"_id": list(range(150)), "ids": [1, 2]}],
        msg="Large array (150 elements) should be accepted as _id",
    ),
]

# Property [Accumulator Field Acceptance]: zero or more accumulator fields
# are accepted alongside _id, and accumulator field names become output field
# names.
GROUP_ACCUMULATOR_FIELD_ACCEPTANCE_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="zero_accumulators",
        docs=[{"_id": 1, "v": "a"}, {"_id": 2, "v": "b"}, {"_id": 3, "v": "a"}],
        pipeline=[{"$group": {"_id": "$v"}}],
        expected=[{"_id": "a"}, {"_id": "b"}],
        msg="$group with zero accumulators should produce one document per group",
    ),
    StageTestCase(
        id="multiple_accumulators",
        docs=[
            {"_id": 1, "v": "a", "x": 10, "y": 100},
            {"_id": 2, "v": "a", "x": 20, "y": 200},
        ],
        pipeline=[
            {
                "$group": {
                    "_id": "$v",
                    "sum_x": {"$sum": "$x"},
                    "avg_y": {"$avg": "$y"},
                    "count": {"$sum": 1},
                }
            }
        ],
        expected=[{"_id": "a", "sum_x": 30, "avg_y": 150.0, "count": 2}],
        msg="Multiple accumulators should all be computed and appear in output",
    ),
]

# Property [Valid Accumulator Operators]: the following accumulator operators
# are accepted in $group without error: $sum, $avg, $min, $max, $first, $last,
# $push, $addToSet, $count, $mergeObjects, $stdDevPop, $stdDevSamp,
# $setUnion, $concatArrays, $firstN, $lastN, $maxN, $minN, $top, $bottom,
# $topN, $bottomN, $median, $percentile, and $accumulator.
GROUP_VALID_ACCUMULATOR_OPERATORS_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="accumulator_sum",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[{"$group": {"_id": "$v", "r": {"$sum": "$x"}}}],
        expected=[{"_id": "a", "r": 5}],
        msg="$sum should be a valid accumulator",
    ),
    StageTestCase(
        id="accumulator_avg",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[{"$group": {"_id": "$v", "r": {"$avg": "$x"}}}],
        expected=[{"_id": "a", "r": 5.0}],
        msg="$avg should be a valid accumulator",
    ),
    StageTestCase(
        id="accumulator_min",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[{"$group": {"_id": "$v", "r": {"$min": "$x"}}}],
        expected=[{"_id": "a", "r": 5}],
        msg="$min should be a valid accumulator",
    ),
    StageTestCase(
        id="accumulator_max",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[{"$group": {"_id": "$v", "r": {"$max": "$x"}}}],
        expected=[{"_id": "a", "r": 5}],
        msg="$max should be a valid accumulator",
    ),
    StageTestCase(
        id="accumulator_first",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[{"$group": {"_id": "$v", "r": {"$first": "$x"}}}],
        expected=[{"_id": "a", "r": 5}],
        msg="$first should be a valid accumulator",
    ),
    StageTestCase(
        id="accumulator_last",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[{"$group": {"_id": "$v", "r": {"$last": "$x"}}}],
        expected=[{"_id": "a", "r": 5}],
        msg="$last should be a valid accumulator",
    ),
    StageTestCase(
        id="accumulator_push",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[{"$group": {"_id": "$v", "r": {"$push": "$x"}}}],
        expected=[{"_id": "a", "r": [5]}],
        msg="$push should be a valid accumulator",
    ),
    StageTestCase(
        id="accumulator_addtoset",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[{"$group": {"_id": "$v", "r": {"$addToSet": "$x"}}}],
        expected=[{"_id": "a", "r": [5]}],
        msg="$addToSet should be a valid accumulator",
    ),
    StageTestCase(
        id="accumulator_count",
        docs=[{"_id": 1, "v": "a"}, {"_id": 2, "v": "a"}],
        pipeline=[{"$group": {"_id": "$v", "r": {"$count": {}}}}],
        expected=[{"_id": "a", "r": 2}],
        msg="$count accumulator should be valid and count documents in the group",
    ),
    StageTestCase(
        id="accumulator_mergeobjects",
        docs=[{"_id": 1, "v": "a", "x": {"k": 1}}],
        pipeline=[{"$group": {"_id": "$v", "r": {"$mergeObjects": "$x"}}}],
        expected=[{"_id": "a", "r": {"k": 1}}],
        msg="$mergeObjects should be a valid accumulator",
    ),
    StageTestCase(
        id="accumulator_stddevpop",
        docs=[{"_id": 1, "v": "a", "x": 3}, {"_id": 2, "v": "a", "x": 7}],
        pipeline=[{"$group": {"_id": "$v", "r": {"$stdDevPop": "$x"}}}],
        expected=[{"_id": "a", "r": 2.0}],
        msg="$stdDevPop should be a valid accumulator",
    ),
    StageTestCase(
        id="accumulator_stddevsamp",
        docs=[{"_id": 1, "v": "a", "x": 3}, {"_id": 2, "v": "a", "x": 7}],
        pipeline=[{"$group": {"_id": "$v", "r": {"$stdDevSamp": "$x"}}}],
        expected=[{"_id": "a", "r": 2.8284271247461903}],
        msg="$stdDevSamp should be a valid accumulator",
    ),
    StageTestCase(
        id="accumulator_setunion",
        docs=[{"_id": 1, "v": "a", "x": [1]}],
        pipeline=[{"$group": {"_id": "$v", "r": {"$setUnion": "$x"}}}],
        expected=[{"_id": "a", "r": [1]}],
        msg="$setUnion should be a valid accumulator in $group",
    ),
    StageTestCase(
        id="accumulator_concatarrays",
        docs=[{"_id": 1, "v": "a", "x": [1]}],
        pipeline=[{"$group": {"_id": "$v", "r": {"$concatArrays": "$x"}}}],
        expected=[{"_id": "a", "r": [1]}],
        msg="$concatArrays should be a valid accumulator in $group",
    ),
    StageTestCase(
        id="accumulator_firstn",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[{"$group": {"_id": "$v", "r": {"$firstN": {"input": "$x", "n": 1}}}}],
        expected=[{"_id": "a", "r": [5]}],
        msg="$firstN should be a valid accumulator",
    ),
    StageTestCase(
        id="accumulator_lastn",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[{"$group": {"_id": "$v", "r": {"$lastN": {"input": "$x", "n": 1}}}}],
        expected=[{"_id": "a", "r": [5]}],
        msg="$lastN should be a valid accumulator",
    ),
    StageTestCase(
        id="accumulator_maxn",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[{"$group": {"_id": "$v", "r": {"$maxN": {"input": "$x", "n": 1}}}}],
        expected=[{"_id": "a", "r": [5]}],
        msg="$maxN should be a valid accumulator",
    ),
    StageTestCase(
        id="accumulator_minn",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[{"$group": {"_id": "$v", "r": {"$minN": {"input": "$x", "n": 1}}}}],
        expected=[{"_id": "a", "r": [5]}],
        msg="$minN should be a valid accumulator",
    ),
    StageTestCase(
        id="accumulator_top",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[
            {
                "$group": {
                    "_id": "$v",
                    "r": {"$top": {"sortBy": {"x": 1}, "output": "$x"}},
                }
            }
        ],
        expected=[{"_id": "a", "r": 5}],
        msg="$top should be a valid accumulator",
    ),
    StageTestCase(
        id="accumulator_bottom",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[
            {
                "$group": {
                    "_id": "$v",
                    "r": {"$bottom": {"sortBy": {"x": 1}, "output": "$x"}},
                }
            }
        ],
        expected=[{"_id": "a", "r": 5}],
        msg="$bottom should be a valid accumulator",
    ),
    StageTestCase(
        id="accumulator_topn",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[
            {
                "$group": {
                    "_id": "$v",
                    "r": {"$topN": {"sortBy": {"x": 1}, "output": "$x", "n": 1}},
                }
            }
        ],
        expected=[{"_id": "a", "r": [5]}],
        msg="$topN should be a valid accumulator",
    ),
    StageTestCase(
        id="accumulator_bottomn",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[
            {
                "$group": {
                    "_id": "$v",
                    "r": {
                        "$bottomN": {
                            "sortBy": {"x": 1},
                            "output": "$x",
                            "n": 1,
                        }
                    },
                }
            }
        ],
        expected=[{"_id": "a", "r": [5]}],
        msg="$bottomN should be a valid accumulator",
    ),
    StageTestCase(
        id="accumulator_median",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[
            {
                "$group": {
                    "_id": "$v",
                    "r": {"$median": {"input": "$x", "method": "approximate"}},
                }
            }
        ],
        expected=[{"_id": "a", "r": 5.0}],
        msg="$median should be a valid accumulator",
    ),
    StageTestCase(
        id="accumulator_percentile",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[
            {
                "$group": {
                    "_id": "$v",
                    "r": {
                        "$percentile": {
                            "input": "$x",
                            "p": [0.5],
                            "method": "approximate",
                        }
                    },
                }
            }
        ],
        expected=[{"_id": "a", "r": [5.0]}],
        msg="$percentile should be a valid accumulator",
    ),
    StageTestCase(
        id="accumulator_custom_js",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[
            {
                "$group": {
                    "_id": "$v",
                    "r": {
                        "$accumulator": {
                            "init": "function() { return 0; }",
                            "accumulate": "function(state, val) { return state + val; }",
                            "accumulateArgs": ["$x"],
                            "merge": "function(s1, s2) { return s1 + s2; }",
                            "lang": "js",
                        }
                    },
                }
            }
        ],
        expected=[{"_id": "a", "r": 5.0}],
        msg="$accumulator (custom JavaScript) should be a valid accumulator",
    ),
]

# Property [$count Equivalence with $sum: 1]: $count produces the same result
# as $sum: 1 for counting documents in a group.
GROUP_COUNT_SUM_EQUIVALENCE_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="accumulator_count_equals_sum_1",
        docs=[{"_id": 1, "v": "a"}, {"_id": 2, "v": "a"}, {"_id": 3, "v": "b"}],
        pipeline=[
            {
                "$group": {
                    "_id": "$v",
                    "cnt": {"$count": {}},
                    "sum_cnt": {"$sum": 1},
                }
            }
        ],
        expected=[
            {"_id": "a", "cnt": 2, "sum_cnt": 2},
            {"_id": "b", "cnt": 1, "sum_cnt": 1},
        ],
        msg="$count should produce the same result as $sum: 1",
    ),
]

# Property [Array Accumulation Across Documents]: $setUnion unions arrays and
# $concatArrays concatenates arrays across multiple documents within a group.
GROUP_ARRAY_ACCUMULATION_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="setunion_accumulates_across_docs",
        docs=[
            {"_id": 1, "v": "a", "x": [1, 2]},
            {"_id": 2, "v": "a", "x": [2, 3]},
            {"_id": 3, "v": "a", "x": [3, 4]},
        ],
        pipeline=[
            {"$group": {"_id": "$v", "r": {"$setUnion": "$x"}}},
            {"$project": {"r": {"$sortArray": {"input": "$r", "sortBy": 1}}}},
        ],
        expected=[{"_id": "a", "r": [1, 2, 3, 4]}],
        msg="$setUnion should union arrays across multiple documents",
    ),
    StageTestCase(
        id="concatarrays_accumulates_across_docs",
        docs=[
            {"_id": 1, "v": "a", "x": [1, 2]},
            {"_id": 2, "v": "a", "x": [3, 4]},
            {"_id": 3, "v": "a", "x": [5]},
        ],
        pipeline=[{"$group": {"_id": "$v", "r": {"$concatArrays": "$x"}}}],
        expected=[{"_id": "a", "r": [1, 2, 3, 4, 5]}],
        msg="$concatArrays should concatenate arrays across multiple documents",
    ),
]

# Property [Accumulator Field Name Flexibility]: empty string, _id (shadowing
# the group key via SON), duplicate names (last definition wins via SON),
# Unicode, emoji, spaces, tabs, long names, __proto__, constructor, $ in
# non-initial position, control characters, NBSP, zero-width space, and BOM
# are all accepted as accumulator field names.
GROUP_ACCUMULATOR_FIELD_NAME_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="empty_string_field_name",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[{"$group": {"_id": "$v", "": {"$sum": "$x"}}}],
        expected=[{"_id": "a", "": 5}],
        msg="Empty string should be accepted as an accumulator field name",
    ),
    StageTestCase(
        id="id_shadows_group_key",
        docs=[{"_id": 1, "v": "a", "x": 10}, {"_id": 2, "v": "a", "x": 20}],
        pipeline=[{"$group": SON([("_id", "$v"), ("_id", {"$sum": 1})])}],
        expected=[{"_id": 1}],
        msg="_id as accumulator field name shadows the group key (last definition wins via SON)",
    ),
    StageTestCase(
        id="duplicate_field_last_wins",
        docs=[{"_id": 1, "v": "a", "x": 10, "y": 100}],
        pipeline=[
            {
                "$group": SON(
                    [
                        ("_id", "$v"),
                        ("total", {"$sum": "$x"}),
                        ("total", {"$sum": "$y"}),
                    ]
                )
            }
        ],
        expected=[{"_id": "a", "total": 100}],
        msg="Duplicate accumulator field names: last definition wins via SON",
    ),
    StageTestCase(
        id="unicode_field_name",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[{"$group": {"_id": "$v", "\u00e9": {"$sum": "$x"}}}],
        expected=[{"_id": "a", "\u00e9": 5}],
        msg="Unicode characters should be accepted as accumulator field names",
    ),
    StageTestCase(
        id="emoji_field_name",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[{"$group": {"_id": "$v", "\U0001f600": {"$sum": "$x"}}}],
        expected=[{"_id": "a", "\U0001f600": 5}],
        msg="Emoji should be accepted as accumulator field names",
    ),
    StageTestCase(
        id="space_field_name",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[{"$group": {"_id": "$v", " ": {"$sum": "$x"}}}],
        expected=[{"_id": "a", " ": 5}],
        msg="Space should be accepted as an accumulator field name",
    ),
    StageTestCase(
        id="tab_field_name",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[{"$group": {"_id": "$v", "\t": {"$sum": "$x"}}}],
        expected=[{"_id": "a", "\t": 5}],
        msg="Tab should be accepted as an accumulator field name",
    ),
    StageTestCase(
        id="long_field_name",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[{"$group": {"_id": "$v", "a" * 200: {"$sum": "$x"}}}],
        expected=[{"_id": "a", "a" * 200: 5}],
        msg="Long field names (200 chars) should be accepted",
    ),
    StageTestCase(
        id="proto_field_name",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[{"$group": {"_id": "$v", "__proto__": {"$sum": "$x"}}}],
        expected=[{"_id": "a", "__proto__": 5}],
        msg="__proto__ should be accepted as an accumulator field name",
    ),
    StageTestCase(
        id="constructor_field_name",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[{"$group": {"_id": "$v", "constructor": {"$sum": "$x"}}}],
        expected=[{"_id": "a", "constructor": 5}],
        msg="constructor should be accepted as an accumulator field name",
    ),
    StageTestCase(
        id="dollar_non_initial_field_name",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[{"$group": {"_id": "$v", "a$b": {"$sum": "$x"}}}],
        expected=[{"_id": "a", "a$b": 5}],
        msg="$ in non-initial position should be accepted in accumulator field names",
    ),
    StageTestCase(
        id="control_char_field_name",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[{"$group": {"_id": "$v", "\x01": {"$sum": "$x"}}}],
        expected=[{"_id": "a", "\x01": 5}],
        msg="Control characters should be accepted as accumulator field names",
    ),
    StageTestCase(
        id="nbsp_field_name",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[{"$group": {"_id": "$v", "\u00a0": {"$sum": "$x"}}}],
        expected=[{"_id": "a", "\u00a0": 5}],
        msg="NBSP should be accepted as an accumulator field name",
    ),
    StageTestCase(
        id="zero_width_space_field_name",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[{"$group": {"_id": "$v", "\u200b": {"$sum": "$x"}}}],
        expected=[{"_id": "a", "\u200b": 5}],
        msg="Zero-width space should be accepted as an accumulator field name",
    ),
    StageTestCase(
        id="bom_field_name",
        docs=[{"_id": 1, "v": "a", "x": 5}],
        pipeline=[{"$group": {"_id": "$v", "\ufeff": {"$sum": "$x"}}}],
        expected=[{"_id": "a", "\ufeff": 5}],
        msg="BOM should be accepted as an accumulator field name",
    ),
]

# Property [Accumulator References Input Fields]: accumulator expressions
# reference input document fields, not sibling accumulator output fields.
GROUP_ACCUMULATOR_INPUT_REF_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="accumulator_refs_input_not_sibling",
        docs=[
            {"_id": 1, "v": "a", "total": 99, "x": 5},
            {"_id": 2, "v": "a", "total": 88, "x": 3},
        ],
        pipeline=[
            {
                "$group": {
                    "_id": "$v",
                    "computed_total": {"$sum": "$x"},
                    "ref_total": {"$sum": "$total"},
                }
            }
        ],
        expected=[{"_id": "a", "computed_total": 8, "ref_total": 187}],
        msg=(
            "Accumulator expressions should reference input document fields,"
            " not sibling accumulator output fields"
        ),
    ),
]

# Property [Accumulator Null and Missing - Ignoring Accumulators]: $sum, $avg,
# $min, $max, $first, $last, $push, $addToSet, $mergeObjects, $stdDevPop,
# $stdDevSamp, $firstN, $lastN, $maxN, $minN, $top, $bottom, $topN, $bottomN,
# $median, $percentile, and $accumulator each handle null and missing inputs
# according to their documented semantics without producing an error.
GROUP_ACCUMULATOR_NULL_MISSING_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="sum_null_and_missing_ignored",
        docs=[{"_id": 1, "v": None}, {"_id": 2}, {"_id": 3, "v": 10}],
        pipeline=[{"$group": {"_id": None, "r": {"$sum": "$v"}}}],
        expected=[{"_id": None, "r": 10}],
        msg="$sum should ignore null and missing values",
    ),
    StageTestCase(
        id="sum_all_null_produces_zero",
        docs=[{"_id": 1, "v": None}, {"_id": 2, "v": None}],
        pipeline=[{"$group": {"_id": None, "r": {"$sum": "$v"}}}],
        expected=[{"_id": None, "r": 0}],
        msg="$sum with all-null input should produce 0",
    ),
    StageTestCase(
        id="sum_all_missing_produces_zero",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": None, "r": {"$sum": "$v"}}}],
        expected=[{"_id": None, "r": 0}],
        msg="$sum with all-missing input should produce 0",
    ),
    StageTestCase(
        id="avg_excludes_null_missing_bool",
        docs=[
            {"_id": 1, "v": None},
            {"_id": 2},
            {"_id": 3, "v": True},
            {"_id": 4, "v": 10},
        ],
        pipeline=[{"$group": {"_id": None, "r": {"$avg": "$v"}}}],
        expected=[{"_id": None, "r": 10.0}],
        msg="$avg should exclude null, missing, and boolean values",
    ),
    StageTestCase(
        id="avg_all_null_produces_null",
        docs=[{"_id": 1, "v": None}, {"_id": 2, "v": None}],
        pipeline=[{"$group": {"_id": None, "r": {"$avg": "$v"}}}],
        expected=[{"_id": None, "r": None}],
        msg="$avg with all-null input should produce null",
    ),
    StageTestCase(
        id="avg_all_missing_produces_null",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": None, "r": {"$avg": "$v"}}}],
        expected=[{"_id": None, "r": None}],
        msg="$avg with all-missing input should produce null",
    ),
    StageTestCase(
        id="avg_all_bool_produces_null",
        docs=[{"_id": 1, "v": True}, {"_id": 2, "v": False}],
        pipeline=[{"$group": {"_id": None, "r": {"$avg": "$v"}}}],
        expected=[{"_id": None, "r": None}],
        msg="$avg with all-boolean input should produce null",
    ),
    StageTestCase(
        id="min_max_null_and_missing_ignored",
        docs=[{"_id": 1, "v": None}, {"_id": 2}, {"_id": 3, "v": 5}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "mn": {"$min": "$v"},
                    "mx": {"$max": "$v"},
                }
            }
        ],
        expected=[{"_id": None, "mn": 5, "mx": 5}],
        msg="$min and $max should ignore null and missing values",
    ),
    StageTestCase(
        id="min_max_all_null_produces_null",
        docs=[{"_id": 1, "v": None}, {"_id": 2, "v": None}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "mn": {"$min": "$v"},
                    "mx": {"$max": "$v"},
                }
            }
        ],
        expected=[{"_id": None, "mn": None, "mx": None}],
        msg="$min and $max with all-null input should produce null",
    ),
    StageTestCase(
        id="min_max_all_missing_produces_null",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "mn": {"$min": "$v"},
                    "mx": {"$max": "$v"},
                }
            }
        ],
        expected=[{"_id": None, "mn": None, "mx": None}],
        msg="$min and $max with all-missing input should produce null",
    ),
    StageTestCase(
        id="first_missing_produces_null",
        docs=[{"_id": 1}, {"_id": 2, "v": 10}],
        pipeline=[
            {"$sort": {"_id": 1}},
            {"$group": {"_id": None, "r": {"$first": "$v"}}},
        ],
        expected=[{"_id": None, "r": None}],
        msg="$first on a missing field should produce null",
    ),
    StageTestCase(
        id="last_missing_produces_null",
        docs=[{"_id": 1, "v": 10}, {"_id": 2}],
        pipeline=[
            {"$sort": {"_id": 1}},
            {"$group": {"_id": None, "r": {"$last": "$v"}}},
        ],
        expected=[{"_id": None, "r": None}],
        msg="$last on a missing field should produce null",
    ),
    StageTestCase(
        id="push_null_included_missing_excluded",
        docs=[{"_id": 1, "v": None}, {"_id": 2}, {"_id": 3, "v": 5}],
        pipeline=[
            {"$sort": {"_id": 1}},
            {"$group": {"_id": None, "r": {"$push": "$v"}}},
        ],
        expected=[{"_id": None, "r": [None, 5]}],
        msg="$push should include null and exclude missing values",
    ),
    StageTestCase(
        id="push_all_missing_produces_empty",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": None, "r": {"$push": "$v"}}}],
        expected=[{"_id": None, "r": []}],
        msg="$push with all-missing input should produce an empty array",
    ),
    StageTestCase(
        id="addtoset_null_included_missing_excluded",
        docs=[
            {"_id": 1, "v": None},
            {"_id": 2},
            {"_id": 3, "v": None},
            {"_id": 4, "v": 5},
        ],
        pipeline=[
            {"$group": {"_id": None, "r": {"$addToSet": "$v"}}},
            {"$project": {"r": {"$sortArray": {"input": "$r", "sortBy": 1}}}},
        ],
        expected=[{"_id": None, "r": [None, 5]}],
        msg="$addToSet should include null (deduplicated to one) and exclude missing values",
    ),
    StageTestCase(
        id="addtoset_all_missing_produces_empty",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": None, "r": {"$addToSet": "$v"}}}],
        expected=[{"_id": None, "r": []}],
        msg="$addToSet with all-missing input should produce an empty array",
    ),
    StageTestCase(
        id="mergeobjects_null_skipped",
        docs=[
            {"_id": 1, "v": None},
            {"_id": 2},
            {"_id": 3, "v": {"a": 1}},
        ],
        pipeline=[
            {"$sort": {"_id": 1}},
            {"$group": {"_id": None, "r": {"$mergeObjects": "$v"}}},
        ],
        expected=[{"_id": None, "r": {"a": 1}}],
        msg="$mergeObjects should skip null values",
    ),
    StageTestCase(
        id="mergeobjects_all_null_produces_empty_object",
        docs=[{"_id": 1, "v": None}, {"_id": 2, "v": None}],
        pipeline=[{"$group": {"_id": None, "r": {"$mergeObjects": "$v"}}}],
        expected=[{"_id": None, "r": {}}],
        msg="$mergeObjects with all-null input should produce {}",
    ),
    StageTestCase(
        id="mergeobjects_all_missing_produces_empty_object",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": None, "r": {"$mergeObjects": "$v"}}}],
        expected=[{"_id": None, "r": {}}],
        msg="$mergeObjects with all-missing input should produce {}",
    ),
    StageTestCase(
        id="setunion_all_missing_produces_empty",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": None, "r": {"$setUnion": "$v"}}}],
        expected=[{"_id": None, "r": []}],
        msg="$setUnion with all-missing input should produce an empty array",
    ),
    StageTestCase(
        id="concatarrays_all_missing_produces_empty",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": None, "r": {"$concatArrays": "$v"}}}],
        expected=[{"_id": None, "r": []}],
        msg="$concatArrays with all-missing input should produce an empty array",
    ),
    StageTestCase(
        id="stddevpop_all_null_produces_null",
        docs=[{"_id": 1, "v": None}, {"_id": 2, "v": None}],
        pipeline=[{"$group": {"_id": None, "r": {"$stdDevPop": "$v"}}}],
        expected=[{"_id": None, "r": None}],
        msg="$stdDevPop with all-null input should produce null",
    ),
    StageTestCase(
        id="stddevsamp_all_null_produces_null",
        docs=[{"_id": 1, "v": None}, {"_id": 2, "v": None}],
        pipeline=[{"$group": {"_id": None, "r": {"$stdDevSamp": "$v"}}}],
        expected=[{"_id": None, "r": None}],
        msg="$stdDevSamp with all-null input should produce null",
    ),
    StageTestCase(
        id="top_all_null_output_produces_none",
        docs=[{"_id": 1, "v": None, "s": 1}, {"_id": 2, "v": None, "s": 2}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {"$top": {"sortBy": {"s": 1}, "output": "$v"}},
                }
            }
        ],
        expected=[{"_id": None, "r": None}],
        msg="$top with all-null output field should produce None",
    ),
    StageTestCase(
        id="top_all_missing_output_produces_none",
        docs=[{"_id": 1, "s": 1}, {"_id": 2, "s": 2}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {"$top": {"sortBy": {"s": 1}, "output": "$v"}},
                }
            }
        ],
        expected=[{"_id": None, "r": None}],
        msg="$top with all-missing output field should produce None",
    ),
    StageTestCase(
        id="bottom_all_null_output_produces_none",
        docs=[{"_id": 1, "v": None, "s": 1}, {"_id": 2, "v": None, "s": 2}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {"$bottom": {"sortBy": {"s": 1}, "output": "$v"}},
                }
            }
        ],
        expected=[{"_id": None, "r": None}],
        msg="$bottom with all-null output field should produce None",
    ),
    StageTestCase(
        id="bottom_all_missing_output_produces_none",
        docs=[{"_id": 1, "s": 1}, {"_id": 2, "s": 2}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {"$bottom": {"sortBy": {"s": 1}, "output": "$v"}},
                }
            }
        ],
        expected=[{"_id": None, "r": None}],
        msg="$bottom with all-missing output field should produce None",
    ),
    StageTestCase(
        id="topn_null_and_missing_included",
        docs=[
            {"_id": 1, "v": None, "s": 2},
            {"_id": 2, "s": 1},
            {"_id": 3, "v": "hello", "s": 3},
        ],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {
                        "$topN": {
                            "sortBy": {"s": 1},
                            "output": "$v",
                            "n": 3,
                        }
                    },
                }
            }
        ],
        expected=[{"_id": None, "r": [None, None, "hello"]}],
        msg="$topN should include null and missing values as None in the output array",
    ),
    StageTestCase(
        id="bottomn_null_and_missing_included",
        docs=[
            {"_id": 1, "v": None, "s": 2},
            {"_id": 2, "s": 1},
            {"_id": 3, "v": "hello", "s": 3},
        ],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {
                        "$bottomN": {
                            "sortBy": {"s": 1},
                            "output": "$v",
                            "n": 3,
                        }
                    },
                }
            }
        ],
        expected=[{"_id": None, "r": [None, None, "hello"]}],
        msg="$bottomN should include null and missing values as None in the output array",
    ),
    StageTestCase(
        id="firstn_null_and_missing_included",
        docs=[{"_id": 1, "v": None}, {"_id": 2}, {"_id": 3, "v": 5}],
        pipeline=[
            {"$sort": {"_id": 1}},
            {
                "$group": {
                    "_id": None,
                    "r": {"$firstN": {"input": "$v", "n": 3}},
                }
            },
        ],
        expected=[{"_id": None, "r": [None, None, 5]}],
        msg="$firstN should include null and missing values as None in the output array",
    ),
    StageTestCase(
        id="lastn_null_and_missing_included",
        docs=[{"_id": 1, "v": None}, {"_id": 2}, {"_id": 3, "v": 5}],
        pipeline=[
            {"$sort": {"_id": 1}},
            {
                "$group": {
                    "_id": None,
                    "r": {"$lastN": {"input": "$v", "n": 3}},
                }
            },
        ],
        expected=[{"_id": None, "r": [None, None, 5]}],
        msg="$lastN should include null and missing values as None in the output array",
    ),
    StageTestCase(
        id="maxn_null_and_missing_excluded",
        docs=[
            {"_id": 1, "v": None},
            {"_id": 2},
            {"_id": 3, "v": 5},
            {"_id": 4, "v": 3},
        ],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {"$maxN": {"input": "$v", "n": 10}},
                }
            }
        ],
        expected=[{"_id": None, "r": [5, 3]}],
        msg="$maxN should exclude null and missing values from the output",
    ),
    StageTestCase(
        id="minn_null_and_missing_excluded",
        docs=[
            {"_id": 1, "v": None},
            {"_id": 2},
            {"_id": 3, "v": 5},
            {"_id": 4, "v": 3},
        ],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {"$minN": {"input": "$v", "n": 10}},
                }
            }
        ],
        expected=[{"_id": None, "r": [3, 5]}],
        msg="$minN should exclude null and missing values from the output",
    ),
    StageTestCase(
        id="maxn_all_null_produces_empty",
        docs=[{"_id": 1, "v": None}, {"_id": 2, "v": None}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {"$maxN": {"input": "$v", "n": 10}},
                }
            }
        ],
        expected=[{"_id": None, "r": []}],
        msg="$maxN with all-null input should produce an empty array",
    ),
    StageTestCase(
        id="maxn_all_missing_produces_empty",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {"$maxN": {"input": "$v", "n": 10}},
                }
            }
        ],
        expected=[{"_id": None, "r": []}],
        msg="$maxN with all-missing input should produce an empty array",
    ),
    StageTestCase(
        id="minn_all_null_produces_empty",
        docs=[{"_id": 1, "v": None}, {"_id": 2, "v": None}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {"$minN": {"input": "$v", "n": 10}},
                }
            }
        ],
        expected=[{"_id": None, "r": []}],
        msg="$minN with all-null input should produce an empty array",
    ),
    StageTestCase(
        id="minn_all_missing_produces_empty",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {"$minN": {"input": "$v", "n": 10}},
                }
            }
        ],
        expected=[{"_id": None, "r": []}],
        msg="$minN with all-missing input should produce an empty array",
    ),
    StageTestCase(
        id="median_ignores_null_missing_nonnumeric",
        docs=[
            {"_id": 1, "v": None},
            {"_id": 2},
            {"_id": 3, "v": "hello"},
            {"_id": 4, "v": 10},
            {"_id": 5, "v": 20},
        ],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {
                        "$median": {
                            "input": "$v",
                            "method": "approximate",
                        }
                    },
                }
            }
        ],
        expected=[{"_id": None, "r": 10.0}],
        msg="$median should silently ignore null, missing, and non-numeric values",
    ),
    StageTestCase(
        id="median_all_null_produces_none",
        docs=[{"_id": 1, "v": None}, {"_id": 2, "v": None}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {
                        "$median": {
                            "input": "$v",
                            "method": "approximate",
                        }
                    },
                }
            }
        ],
        expected=[{"_id": None, "r": None}],
        msg="$median with all-null input should produce None",
    ),
    StageTestCase(
        id="median_all_missing_produces_none",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {
                        "$median": {
                            "input": "$v",
                            "method": "approximate",
                        }
                    },
                }
            }
        ],
        expected=[{"_id": None, "r": None}],
        msg="$median with all-missing input should produce None",
    ),
    StageTestCase(
        id="percentile_ignores_null_missing_nonnumeric",
        docs=[
            {"_id": 1, "v": None},
            {"_id": 2},
            {"_id": 3, "v": "hello"},
            {"_id": 4, "v": 10},
            {"_id": 5, "v": 20},
        ],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {
                        "$percentile": {
                            "input": "$v",
                            "p": [0.5],
                            "method": "approximate",
                        }
                    },
                }
            }
        ],
        expected=[{"_id": None, "r": [10.0]}],
        msg="$percentile should silently ignore null, missing, and non-numeric values",
    ),
    StageTestCase(
        id="percentile_all_null_produces_list_none",
        docs=[{"_id": 1, "v": None}, {"_id": 2, "v": None}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {
                        "$percentile": {
                            "input": "$v",
                            "p": [0.5],
                            "method": "approximate",
                        }
                    },
                }
            }
        ],
        expected=[{"_id": None, "r": [None]}],
        msg="$percentile with all-null input should produce [None]",
    ),
    StageTestCase(
        id="percentile_all_missing_produces_list_none",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {
                        "$percentile": {
                            "input": "$v",
                            "p": [0.5],
                            "method": "approximate",
                        }
                    },
                }
            }
        ],
        expected=[{"_id": None, "r": [None]}],
        msg="$percentile with all-missing input should produce [None]",
    ),
    StageTestCase(
        id="accumulator_js_null_passed_as_null",
        docs=[{"_id": 1, "v": None}, {"_id": 2}, {"_id": 3, "v": 5}],
        pipeline=[
            {"$sort": {"_id": 1}},
            {
                "$group": {
                    "_id": None,
                    "r": {
                        "$accumulator": {
                            "init": "function() { return []; }",
                            "accumulate": "function(state, val) { state.push(val); return state; }",
                            "accumulateArgs": ["$v"],
                            "merge": "function(s1, s2) { return s1.concat(s2); }",
                            "lang": "js",
                        }
                    },
                }
            },
        ],
        expected=[{"_id": None, "r": [None, None, 5.0]}],
        msg=(
            "$accumulator should pass null as null and missing as undefined"
            " to the accumulate function"
        ),
    ),
]

# Property [$stdDevPop/$stdDevSamp Behavior]: $stdDevPop and $stdDevSamp with
# all identical values produce zero, and $stdDevSamp with a single value
# produces null (sample standard deviation is undefined for n=1).
GROUP_STDDEV_BEHAVIOR_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="stddevpop_all_same_produces_zero",
        docs=[{"_id": 1, "v": 5}, {"_id": 2, "v": 5}],
        pipeline=[{"$group": {"_id": None, "r": {"$stdDevPop": "$v"}}}],
        expected=[{"_id": None, "r": DOUBLE_ZERO}],
        msg="$stdDevPop with all same values should produce 0.0",
    ),
    StageTestCase(
        id="stddevsamp_all_same_produces_zero",
        docs=[{"_id": 1, "v": 5}, {"_id": 2, "v": 5}],
        pipeline=[{"$group": {"_id": None, "r": {"$stdDevSamp": "$v"}}}],
        expected=[{"_id": None, "r": DOUBLE_ZERO}],
        msg="$stdDevSamp with all same values should produce 0.0",
    ),
    StageTestCase(
        id="stddevsamp_single_value_produces_null",
        docs=[{"_id": 1, "v": 5}],
        pipeline=[{"$group": {"_id": None, "r": {"$stdDevSamp": "$v"}}}],
        expected=[{"_id": None, "r": None}],
        msg="$stdDevSamp with a single value should produce null",
    ),
]

# Property [$top/$bottom Sort Key Null/Missing Ordering]: null and missing
# sort key values are treated as the lowest value in ascending order for
# $top, $bottom, $topN, and $bottomN.
GROUP_TOP_BOTTOM_SORT_KEY_NULL_MISSING_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="top_null_sort_is_lowest",
        docs=[
            {"_id": 1, "v": "a", "s": None},
            {"_id": 2, "v": "b", "s": 1},
        ],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {"$top": {"sortBy": {"s": 1}, "output": "$v"}},
                }
            }
        ],
        expected=[{"_id": None, "r": "a"}],
        msg="$top should treat null sort field as lowest in ascending order",
    ),
    StageTestCase(
        id="top_missing_sort_is_lowest",
        docs=[
            {"_id": 1, "v": "a"},
            {"_id": 2, "v": "b", "s": 1},
        ],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {"$top": {"sortBy": {"s": 1}, "output": "$v"}},
                }
            }
        ],
        expected=[{"_id": None, "r": "a"}],
        msg="$top should treat missing sort field as lowest in ascending order",
    ),
    StageTestCase(
        id="bottom_null_sort_is_lowest",
        docs=[
            {"_id": 1, "v": "a", "s": None},
            {"_id": 2, "v": "b", "s": 1},
        ],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {"$bottom": {"sortBy": {"s": 1}, "output": "$v"}},
                }
            }
        ],
        expected=[{"_id": None, "r": "b"}],
        msg="$bottom should treat null sort field as lowest in ascending order",
    ),
    StageTestCase(
        id="bottom_missing_sort_is_lowest",
        docs=[
            {"_id": 1, "v": "a"},
            {"_id": 2, "v": "b", "s": 1},
        ],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {"$bottom": {"sortBy": {"s": 1}, "output": "$v"}},
                }
            }
        ],
        expected=[{"_id": None, "r": "b"}],
        msg="$bottom should treat missing sort field as lowest in ascending order",
    ),
    StageTestCase(
        id="topn_null_sort_is_lowest",
        docs=[
            {"_id": 1, "v": "a", "s": None},
            {"_id": 2, "v": "b", "s": 1},
        ],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {
                        "$topN": {
                            "sortBy": {"s": 1},
                            "output": "$v",
                            "n": 1,
                        }
                    },
                }
            }
        ],
        expected=[{"_id": None, "r": ["a"]}],
        msg="$topN should treat null sort field as lowest in ascending order",
    ),
    StageTestCase(
        id="topn_missing_sort_is_lowest",
        docs=[
            {"_id": 1, "v": "a"},
            {"_id": 2, "v": "b", "s": 1},
        ],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {
                        "$topN": {
                            "sortBy": {"s": 1},
                            "output": "$v",
                            "n": 1,
                        }
                    },
                }
            }
        ],
        expected=[{"_id": None, "r": ["a"]}],
        msg="$topN should treat missing sort field as lowest in ascending order",
    ),
    StageTestCase(
        id="bottomn_null_sort_is_lowest",
        docs=[
            {"_id": 1, "v": "a", "s": None},
            {"_id": 2, "v": "b", "s": 1},
        ],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {
                        "$bottomN": {
                            "sortBy": {"s": 1},
                            "output": "$v",
                            "n": 1,
                        }
                    },
                }
            }
        ],
        expected=[{"_id": None, "r": ["b"]}],
        msg="$bottomN should treat null sort field as lowest in ascending order",
    ),
    StageTestCase(
        id="bottomn_missing_sort_is_lowest",
        docs=[
            {"_id": 1, "v": "a"},
            {"_id": 2, "v": "b", "s": 1},
        ],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {
                        "$bottomN": {
                            "sortBy": {"s": 1},
                            "output": "$v",
                            "n": 1,
                        }
                    },
                }
            }
        ],
        expected=[{"_id": None, "r": ["b"]}],
        msg="$bottomN should treat missing sort field as lowest in ascending order",
    ),
]

GROUP_ACCUMULATOR_SPEC_TESTS = (
    GROUP_ACCUMULATOR_FIELD_ACCEPTANCE_TESTS
    + GROUP_VALID_ACCUMULATOR_OPERATORS_TESTS
    + GROUP_COUNT_SUM_EQUIVALENCE_TESTS
    + GROUP_ARRAY_ACCUMULATION_TESTS
    + GROUP_ACCUMULATOR_FIELD_NAME_TESTS
    + GROUP_ACCUMULATOR_INPUT_REF_TESTS
    + GROUP_ACCUMULATOR_NULL_MISSING_TESTS
    + GROUP_STDDEV_BEHAVIOR_TESTS
    + GROUP_TOP_BOTTOM_SORT_KEY_NULL_MISSING_TESTS
)

# Property [$sum Return Type Promotion]: $sum promotes the result type
# according to the numeric type hierarchy (int32 < Int64 < double <
# Decimal128), overflows int32 to Int64 and Int64 to double, produces
# infinity on double overflow, and ignores booleans and non-numeric types.
GROUP_SUM_TYPE_PROMOTION_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="sum_int32_int32_returns_int",
        docs=[{"_id": 1, "v": 5}, {"_id": 2, "v": 10}],
        pipeline=[
            {"$group": {"_id": None, "r": {"$sum": "$v"}}},
            {"$project": {"r": 1, "t": {"$type": "$r"}}},
        ],
        expected=[{"_id": None, "r": 15, "t": "int"}],
        msg="int32 + int32 should produce int32",
    ),
    StageTestCase(
        id="sum_int32_int64_returns_long",
        docs=[{"_id": 1, "v": 5}, {"_id": 2, "v": Int64(10)}],
        pipeline=[
            {"$group": {"_id": None, "r": {"$sum": "$v"}}},
            {"$project": {"r": 1, "t": {"$type": "$r"}}},
        ],
        expected=[{"_id": None, "r": Int64(15), "t": "long"}],
        msg="int32 + Int64 should produce Int64",
    ),
    StageTestCase(
        id="sum_int32_double_returns_double",
        docs=[{"_id": 1, "v": 5}, {"_id": 2, "v": 10.5}],
        pipeline=[
            {"$group": {"_id": None, "r": {"$sum": "$v"}}},
            {"$project": {"r": 1, "t": {"$type": "$r"}}},
        ],
        expected=[{"_id": None, "r": 15.5, "t": "double"}],
        msg="int32 + double should produce double",
    ),
    StageTestCase(
        id="sum_int32_decimal128_returns_decimal",
        docs=[{"_id": 1, "v": 5}, {"_id": 2, "v": Decimal128("10")}],
        pipeline=[
            {"$group": {"_id": None, "r": {"$sum": "$v"}}},
            {"$project": {"r": 1, "t": {"$type": "$r"}}},
        ],
        expected=[{"_id": None, "r": Decimal128("15"), "t": "decimal"}],
        msg="int32 + Decimal128 should produce Decimal128",
    ),
    StageTestCase(
        id="sum_int64_double_returns_double",
        docs=[{"_id": 1, "v": Int64(5)}, {"_id": 2, "v": 10.5}],
        pipeline=[
            {"$group": {"_id": None, "r": {"$sum": "$v"}}},
            {"$project": {"r": 1, "t": {"$type": "$r"}}},
        ],
        expected=[{"_id": None, "r": 15.5, "t": "double"}],
        msg="Int64 + double should produce double",
    ),
    StageTestCase(
        id="sum_int64_decimal128_returns_decimal",
        docs=[{"_id": 1, "v": Int64(5)}, {"_id": 2, "v": Decimal128("10")}],
        pipeline=[
            {"$group": {"_id": None, "r": {"$sum": "$v"}}},
            {"$project": {"r": 1, "t": {"$type": "$r"}}},
        ],
        expected=[{"_id": None, "r": Decimal128("15"), "t": "decimal"}],
        msg="Int64 + Decimal128 should produce Decimal128",
    ),
    StageTestCase(
        id="sum_double_double_returns_double",
        docs=[{"_id": 1, "v": 1.5}, {"_id": 2, "v": 2.5}],
        pipeline=[
            {"$group": {"_id": None, "r": {"$sum": "$v"}}},
            {"$project": {"r": 1, "t": {"$type": "$r"}}},
        ],
        expected=[{"_id": None, "r": 4.0, "t": "double"}],
        msg="double + double should produce double",
    ),
    StageTestCase(
        id="sum_decimal128_decimal128_returns_decimal",
        docs=[
            {"_id": 1, "v": DECIMAL128_ONE_AND_HALF},
            {"_id": 2, "v": DECIMAL128_TWO_AND_HALF},
        ],
        pipeline=[
            {"$group": {"_id": None, "r": {"$sum": "$v"}}},
            {"$project": {"r": 1, "t": {"$type": "$r"}}},
        ],
        expected=[{"_id": None, "r": Decimal128("4.0"), "t": "decimal"}],
        msg="Decimal128 + Decimal128 should produce Decimal128",
    ),
    StageTestCase(
        id="sum_int32_overflow_promotes_to_int64",
        docs=[{"_id": 1, "v": INT32_MAX}, {"_id": 2, "v": 1}],
        pipeline=[
            {"$group": {"_id": None, "r": {"$sum": "$v"}}},
            {"$project": {"r": 1, "t": {"$type": "$r"}}},
        ],
        expected=[{"_id": None, "r": Int64(INT32_MAX + 1), "t": "long"}],
        msg="int32 overflow should promote to Int64",
    ),
    StageTestCase(
        id="sum_int64_overflow_promotes_to_double",
        docs=[{"_id": 1, "v": INT64_MAX}, {"_id": 2, "v": Int64(1)}],
        pipeline=[
            {"$group": {"_id": None, "r": {"$sum": "$v"}}},
            {"$project": {"r": 1, "t": {"$type": "$r"}}},
        ],
        expected=[{"_id": None, "r": DOUBLE_FROM_INT64_MAX, "t": "double"}],
        msg="Int64 overflow should promote to double, not Decimal128",
    ),
    StageTestCase(
        id="sum_double_overflow_produces_infinity",
        docs=[{"_id": 1, "v": DOUBLE_MAX}, {"_id": 2, "v": DOUBLE_MAX}],
        pipeline=[
            {"$group": {"_id": None, "r": {"$sum": "$v"}}},
            {"$project": {"r": 1, "t": {"$type": "$r"}}},
        ],
        expected=[{"_id": None, "r": FLOAT_INFINITY, "t": "double"}],
        msg="double overflow (DBL_MAX + DBL_MAX) should produce infinity",
    ),
    StageTestCase(
        id="sum_inf_plus_inf_produces_infinity",
        docs=[{"_id": 1, "v": FLOAT_INFINITY}, {"_id": 2, "v": FLOAT_INFINITY}],
        pipeline=[{"$group": {"_id": None, "r": {"$sum": "$v"}}}],
        expected=[{"_id": None, "r": FLOAT_INFINITY}],
        msg="inf + inf should produce infinity",
    ),
    StageTestCase(
        id="sum_ignores_booleans",
        docs=[
            {"_id": 1, "v": True},
            {"_id": 2, "v": False},
            {"_id": 3, "v": 10},
        ],
        pipeline=[{"$group": {"_id": None, "r": {"$sum": "$v"}}}],
        expected=[{"_id": None, "r": 10}],
        msg="$sum should ignore boolean values",
    ),
    StageTestCase(
        id="sum_ignores_literal_boolean",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": None, "r": {"$sum": True}}}],
        expected=[{"_id": None, "r": 0}],
        msg="$sum should ignore literal boolean values (contribute 0)",
    ),
    StageTestCase(
        id="sum_ignores_non_numeric_types",
        docs=[
            {"_id": 1, "v": "hello"},
            {"_id": 2, "v": [1, 2]},
            {"_id": 3, "v": {"a": 1}},
            {"_id": 4, "v": 10},
        ],
        pipeline=[{"$group": {"_id": None, "r": {"$sum": "$v"}}}],
        expected=[{"_id": None, "r": 10}],
        msg="$sum should ignore non-numeric types (string, array, object)",
    ),
    StageTestCase(
        id="sum_all_booleans_produces_zero",
        docs=[{"_id": 1, "v": True}, {"_id": 2, "v": False}],
        pipeline=[{"$group": {"_id": None, "r": {"$sum": "$v"}}}],
        expected=[{"_id": None, "r": 0}],
        msg="$sum with all-boolean input should produce 0",
    ),
]

# Property [$avg Return Type]: $avg of integer inputs (int32 or Int64)
# produces a double result, while $avg of Decimal128 inputs produces a
# Decimal128 result.
GROUP_AVG_RETURN_TYPE_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="avg_int32_returns_double",
        docs=[{"_id": 1, "v": 10}, {"_id": 2, "v": 20}],
        pipeline=[
            {"$group": {"_id": None, "r": {"$avg": "$v"}}},
            {"$project": {"r": 1, "t": {"$type": "$r"}}},
        ],
        expected=[{"_id": None, "r": 15.0, "t": "double"}],
        msg="$avg of int32 inputs should produce a double result",
    ),
    StageTestCase(
        id="avg_int64_returns_double",
        docs=[{"_id": 1, "v": Int64(10)}, {"_id": 2, "v": Int64(20)}],
        pipeline=[
            {"$group": {"_id": None, "r": {"$avg": "$v"}}},
            {"$project": {"r": 1, "t": {"$type": "$r"}}},
        ],
        expected=[{"_id": None, "r": 15.0, "t": "double"}],
        msg="$avg of Int64 inputs should produce a double result",
    ),
    StageTestCase(
        id="avg_decimal128_returns_decimal",
        docs=[{"_id": 1, "v": Decimal128("10")}, {"_id": 2, "v": Decimal128("20")}],
        pipeline=[
            {"$group": {"_id": None, "r": {"$avg": "$v"}}},
            {"$project": {"r": 1, "t": {"$type": "$r"}}},
        ],
        expected=[{"_id": None, "r": Decimal128("15"), "t": "decimal"}],
        msg="$avg of Decimal128 inputs should produce a Decimal128 result",
    ),
]

# Property [$addToSet Deduplication Rules]: $addToSet deduplicates numeric
# equivalents (int32, Int64, double, Decimal128), deduplicates -0.0 and 0.0,
# does not deduplicate booleans with numeric equivalents, and treats object
# key order and array element order as significant for deduplication.
GROUP_ADDTOSET_DEDUP_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="addtoset_numeric_equivalents_dedup",
        docs=[
            {"_id": 1, "v": 1},
            {"_id": 2, "v": Int64(1)},
            {"_id": 3, "v": 1.0},
            {"_id": 4, "v": Decimal128("1")},
        ],
        pipeline=[
            {"$group": {"_id": None, "r": {"$addToSet": "$v"}}},
        ],
        expected=[{"_id": None, "r": [1]}],
        msg=(
            "Numeric equivalents (int32, Int64, double, Decimal128) should"
            " deduplicate to one entry"
        ),
    ),
    StageTestCase(
        id="addtoset_bool_not_dedup_with_numeric",
        docs=[
            {"_id": 1, "v": 1},
            {"_id": 2, "v": True},
            {"_id": 3, "v": 0},
            {"_id": 4, "v": False},
        ],
        pipeline=[
            {"$group": {"_id": None, "r": {"$addToSet": "$v"}}},
            {"$project": {"r": {"$sortArray": {"input": "$r", "sortBy": 1}}}},
        ],
        expected=[{"_id": None, "r": [0, 1, False, True]}],
        msg="Boolean values should not be deduplicated with numeric equivalents",
    ),
    StageTestCase(
        id="addtoset_negative_zero_and_zero_dedup",
        docs=[
            {"_id": 1, "v": DOUBLE_NEGATIVE_ZERO},
            {"_id": 2, "v": DOUBLE_ZERO},
            {"_id": 3, "v": 0},
        ],
        pipeline=[
            {"$group": {"_id": None, "r": {"$addToSet": "$v"}}},
        ],
        expected=[{"_id": None, "r": [DOUBLE_NEGATIVE_ZERO]}],
        msg="-0.0 and 0.0 should be deduplicated",
    ),
    StageTestCase(
        id="addtoset_object_key_order_matters",
        docs=[
            {"_id": 1, "v": SON([("a", 1), ("b", 2)])},
            {"_id": 2, "v": SON([("b", 2), ("a", 1)])},
        ],
        pipeline=[
            {"$group": {"_id": None, "r": {"$addToSet": "$v"}}},
            {"$project": {"r": {"$sortArray": {"input": "$r", "sortBy": 1}}}},
        ],
        expected=[{"_id": None, "r": [{"a": 1, "b": 2}, {"b": 2, "a": 1}]}],
        msg="Object key order should matter for $addToSet deduplication",
    ),
    StageTestCase(
        id="addtoset_array_element_order_matters",
        docs=[
            {"_id": 1, "v": [1, 2]},
            {"_id": 2, "v": [2, 1]},
        ],
        pipeline=[
            {"$group": {"_id": None, "r": {"$addToSet": "$v"}}},
            {"$project": {"r": {"$sortArray": {"input": "$r", "sortBy": 1}}}},
        ],
        expected=[{"_id": None, "r": [[1, 2], [2, 1]]}],
        msg="Array element order should matter for $addToSet deduplication",
    ),
]

# Property [$min/$max BSON Comparison Order]: $min and $max follow BSON
# comparison order for mixed types (MinKey < numbers < string < object <
# Binary < ObjectId < bool < datetime < Timestamp < Regex < Code <
# CodeWithScope < MaxKey), and for booleans False < True.
GROUP_MIN_MAX_MIXED_TYPES_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="min_max_bson_order_all_types",
        docs=[
            {"_id": 1, "v": MinKey()},
            {"_id": 2, "v": 42},
            {"_id": 3, "v": "hello"},
            {"_id": 4, "v": {"a": 1}},
            {"_id": 5, "v": Binary(b"data")},
            {"_id": 6, "v": ObjectId("507f1f77bcf86cd799439011")},
            {"_id": 7, "v": True},
            {"_id": 8, "v": datetime(2024, 1, 1)},
            {"_id": 9, "v": Timestamp(1, 1)},
            {"_id": 10, "v": Regex("abc", "i")},
            {"_id": 11, "v": Code("function(){}")},
            {"_id": 12, "v": Code("function(){}", {"x": 1})},
            {"_id": 13, "v": MaxKey()},
        ],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "mn": {"$min": "$v"},
                    "mx": {"$max": "$v"},
                }
            }
        ],
        expected=[{"_id": None, "mn": MinKey(), "mx": MaxKey()}],
        msg=(
            "$min should return MinKey and $max should return MaxKey when all"
            " BSON types are present"
        ),
    ),
    StageTestCase(
        id="min_minkey_less_than_number",
        docs=[{"_id": 1, "v": MinKey()}, {"_id": 2, "v": 42}],
        pipeline=[{"$group": {"_id": None, "mn": {"$min": "$v"}, "mx": {"$max": "$v"}}}],
        expected=[{"_id": None, "mn": MinKey(), "mx": 42}],
        msg="MinKey should compare less than numbers in BSON order",
    ),
    StageTestCase(
        id="min_number_less_than_string",
        docs=[{"_id": 1, "v": 42}, {"_id": 2, "v": "hello"}],
        pipeline=[{"$group": {"_id": None, "mn": {"$min": "$v"}, "mx": {"$max": "$v"}}}],
        expected=[{"_id": None, "mn": 42, "mx": "hello"}],
        msg="Numbers should compare less than strings in BSON order",
    ),
    StageTestCase(
        id="min_string_less_than_object",
        docs=[{"_id": 1, "v": "hello"}, {"_id": 2, "v": {"a": 1}}],
        pipeline=[{"$group": {"_id": None, "mn": {"$min": "$v"}, "mx": {"$max": "$v"}}}],
        expected=[{"_id": None, "mn": "hello", "mx": {"a": 1}}],
        msg="Strings should compare less than objects in BSON order",
    ),
    StageTestCase(
        id="min_object_less_than_binary",
        docs=[{"_id": 1, "v": {"a": 1}}, {"_id": 2, "v": Binary(b"data")}],
        pipeline=[{"$group": {"_id": None, "mn": {"$min": "$v"}, "mx": {"$max": "$v"}}}],
        expected=[{"_id": None, "mn": {"a": 1}, "mx": b"data"}],
        msg="Objects should compare less than Binary in BSON order",
    ),
    StageTestCase(
        id="min_binary_less_than_objectid",
        docs=[
            {"_id": 1, "v": Binary(b"data")},
            {"_id": 2, "v": ObjectId("507f1f77bcf86cd799439011")},
        ],
        pipeline=[{"$group": {"_id": None, "mn": {"$min": "$v"}, "mx": {"$max": "$v"}}}],
        expected=[
            {
                "_id": None,
                "mn": b"data",
                "mx": ObjectId("507f1f77bcf86cd799439011"),
            }
        ],
        msg="Binary should compare less than ObjectId in BSON order",
    ),
    StageTestCase(
        id="min_objectid_less_than_bool",
        docs=[
            {"_id": 1, "v": ObjectId("507f1f77bcf86cd799439011")},
            {"_id": 2, "v": True},
        ],
        pipeline=[{"$group": {"_id": None, "mn": {"$min": "$v"}, "mx": {"$max": "$v"}}}],
        expected=[{"_id": None, "mn": ObjectId("507f1f77bcf86cd799439011"), "mx": True}],
        msg="ObjectId should compare less than bool in BSON order",
    ),
    StageTestCase(
        id="min_bool_less_than_datetime",
        docs=[{"_id": 1, "v": True}, {"_id": 2, "v": datetime(2024, 1, 1)}],
        pipeline=[{"$group": {"_id": None, "mn": {"$min": "$v"}, "mx": {"$max": "$v"}}}],
        expected=[{"_id": None, "mn": True, "mx": datetime(2024, 1, 1)}],
        msg="Bool should compare less than datetime in BSON order",
    ),
    StageTestCase(
        id="min_datetime_less_than_timestamp",
        docs=[
            {"_id": 1, "v": datetime(2024, 1, 1)},
            {"_id": 2, "v": Timestamp(1, 1)},
        ],
        pipeline=[{"$group": {"_id": None, "mn": {"$min": "$v"}, "mx": {"$max": "$v"}}}],
        expected=[{"_id": None, "mn": datetime(2024, 1, 1), "mx": Timestamp(1, 1)}],
        msg="Datetime should compare less than Timestamp in BSON order",
    ),
    StageTestCase(
        id="min_timestamp_less_than_regex",
        docs=[
            {"_id": 1, "v": Timestamp(1, 1)},
            {"_id": 2, "v": Regex("abc", "i")},
        ],
        pipeline=[{"$group": {"_id": None, "mn": {"$min": "$v"}, "mx": {"$max": "$v"}}}],
        expected=[{"_id": None, "mn": Timestamp(1, 1), "mx": Regex("abc", "i")}],
        msg="Timestamp should compare less than Regex in BSON order",
    ),
    StageTestCase(
        id="min_regex_less_than_code",
        docs=[
            {"_id": 1, "v": Regex("abc", "i")},
            {"_id": 2, "v": Code("function(){}")},
        ],
        pipeline=[{"$group": {"_id": None, "mn": {"$min": "$v"}, "mx": {"$max": "$v"}}}],
        expected=[{"_id": None, "mn": Regex("abc", "i"), "mx": Code("function(){}")}],
        msg="Regex should compare less than Code in BSON order",
    ),
    StageTestCase(
        id="min_code_less_than_code_with_scope",
        docs=[
            {"_id": 1, "v": Code("function(){}")},
            {"_id": 2, "v": Code("function(){}", {"x": 1})},
        ],
        pipeline=[{"$group": {"_id": None, "mn": {"$min": "$v"}, "mx": {"$max": "$v"}}}],
        expected=[
            {
                "_id": None,
                "mn": Code("function(){}"),
                "mx": Code("function(){}", {"x": 1}),
            }
        ],
        msg="Code should compare less than Code with scope in BSON order",
    ),
    StageTestCase(
        id="min_code_with_scope_less_than_maxkey",
        docs=[
            {"_id": 1, "v": Code("function(){}", {"x": 1})},
            {"_id": 2, "v": MaxKey()},
        ],
        pipeline=[{"$group": {"_id": None, "mn": {"$min": "$v"}, "mx": {"$max": "$v"}}}],
        expected=[
            {
                "_id": None,
                "mn": Code("function(){}", {"x": 1}),
                "mx": MaxKey(),
            }
        ],
        msg="Code with scope should compare less than MaxKey in BSON order",
    ),
    StageTestCase(
        id="min_bool_false_less_than_true",
        docs=[{"_id": 1, "v": True}, {"_id": 2, "v": False}],
        pipeline=[{"$group": {"_id": None, "mn": {"$min": "$v"}, "mx": {"$max": "$v"}}}],
        expected=[{"_id": None, "mn": False, "mx": True}],
        msg="For boolean values, False should compare less than True",
    ),
]

# Property [Accumulator Expression Acceptance]: accumulator expressions accept
# expressions (verified by one representative, $cond), and a nested
# accumulator in expression position (inner $sum sums an array, outer $sum is
# the accumulator).
GROUP_ACCUMULATOR_EXPRESSION_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="accumulator_cond_expression",
        docs=[
            {"_id": 1, "g": "a", "x": 10},
            {"_id": 2, "g": "a", "x": -5},
        ],
        pipeline=[
            {
                "$group": {
                    "_id": "$g",
                    "r": {"$sum": {"$cond": [{"$gte": ["$x", 0]}, "$x", 0]}},
                }
            }
        ],
        expected=[{"_id": "a", "r": 10}],
        msg="Accumulator with $cond expression should evaluate per document",
    ),
    StageTestCase(
        id="nested_accumulator_in_expression_position",
        docs=[
            {"_id": 1, "g": "a", "v": [1, 2, 3]},
            {"_id": 2, "g": "a", "v": [4, 5]},
        ],
        pipeline=[{"$group": {"_id": "$g", "r": {"$sum": {"$sum": "$v"}}}}],
        expected=[{"_id": "a", "r": 15}],
        msg=(
            "Nested accumulator in expression position: inner $sum sums the"
            " array, outer $sum accumulates across documents"
        ),
    ),
]

# Property [System Variables in Expressions]: $$ROOT and $$CURRENT work in
# accumulator expressions and _id, $literal prevents field reference
# interpretation in both _id and accumulator expressions, and $$REMOVE in
# $push produces an empty array.
GROUP_SYSTEM_VARIABLE_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="push_root_collects_full_docs",
        docs=[
            {"_id": 1, "g": "a", "x": 10},
            {"_id": 2, "g": "a", "x": 20},
        ],
        pipeline=[
            {"$sort": {"_id": 1}},
            {"$group": {"_id": "$g", "r": {"$push": "$$ROOT"}}},
        ],
        expected=[
            {
                "_id": "a",
                "r": [
                    {"_id": 1, "g": "a", "x": 10},
                    {"_id": 2, "g": "a", "x": 20},
                ],
            }
        ],
        msg="$$ROOT in $push should collect full input documents",
    ),
    StageTestCase(
        id="push_current_collects_full_docs",
        docs=[
            {"_id": 1, "g": "a", "x": 10},
            {"_id": 2, "g": "a", "x": 20},
        ],
        pipeline=[
            {"$sort": {"_id": 1}},
            {"$group": {"_id": "$g", "r": {"$push": "$$CURRENT"}}},
        ],
        expected=[
            {
                "_id": "a",
                "r": [
                    {"_id": 1, "g": "a", "x": 10},
                    {"_id": 2, "g": "a", "x": 20},
                ],
            }
        ],
        msg="$$CURRENT in $push should collect full input documents",
    ),
    StageTestCase(
        id="root_in_id_groups_by_entire_doc",
        docs=[
            {"_id": 1, "x": 10},
            {"_id": 2, "x": 20},
            {"_id": 3, "x": 10},
        ],
        pipeline=[{"$group": {"_id": "$$ROOT", "count": {"$sum": 1}}}],
        expected=[
            {"_id": {"_id": 1, "x": 10}, "count": 1},
            {"_id": {"_id": 2, "x": 20}, "count": 1},
            {"_id": {"_id": 3, "x": 10}, "count": 1},
        ],
        msg="$$ROOT in _id should group by the entire input document",
    ),
    StageTestCase(
        id="current_in_id_groups_by_entire_doc",
        docs=[
            {"_id": 1, "x": 10},
            {"_id": 2, "x": 20},
            {"_id": 3, "x": 10},
        ],
        pipeline=[{"$group": {"_id": "$$CURRENT", "count": {"$sum": 1}}}],
        expected=[
            {"_id": {"_id": 1, "x": 10}, "count": 1},
            {"_id": {"_id": 2, "x": 20}, "count": 1},
            {"_id": {"_id": 3, "x": 10}, "count": 1},
        ],
        msg="$$CURRENT in _id should group by the entire input document",
    ),
    StageTestCase(
        id="literal_prevents_field_ref_in_id",
        docs=[{"_id": 1, "v": "hello"}, {"_id": 2, "v": "world"}],
        pipeline=[{"$group": {"_id": {"$literal": "$v"}, "count": {"$sum": 1}}}],
        expected=[{"_id": "$v", "count": 2}],
        msg=(
            "$literal in _id should prevent field reference interpretation,"
            " treating '$v' as a string constant"
        ),
    ),
    StageTestCase(
        id="literal_prevents_field_ref_in_accumulator",
        docs=[{"_id": 1, "g": "a"}, {"_id": 2, "g": "a"}],
        pipeline=[
            {"$sort": {"_id": 1}},
            {
                "$group": {
                    "_id": "$g",
                    "r": {"$push": {"$literal": "$nonexistent"}},
                }
            },
        ],
        expected=[{"_id": "a", "r": ["$nonexistent", "$nonexistent"]}],
        msg=(
            "$literal in accumulator should prevent field reference"
            " interpretation, treating '$nonexistent' as a string constant"
        ),
    ),
    StageTestCase(
        id="remove_in_push_produces_empty_array",
        docs=[{"_id": 1, "g": "a"}, {"_id": 2, "g": "a"}],
        pipeline=[{"$group": {"_id": "$g", "r": {"$push": "$$REMOVE"}}}],
        expected=[{"_id": "a", "r": []}],
        msg="$$REMOVE in $push should produce an empty array, not an array of nulls",
    ),
]

GROUP_EXPRESSION_ARGS_TESTS = GROUP_ACCUMULATOR_EXPRESSION_TESTS + GROUP_SYSTEM_VARIABLE_TESTS

# Property [Empty and Non-Existent Input]: $group on an empty or non-existent
# collection produces zero output documents regardless of the _id expression.
GROUP_EMPTY_INPUT_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="empty_collection_field_ref",
        docs=[],
        pipeline=[{"$group": {"_id": "$v"}}],
        expected=[],
        msg="Empty collection should produce zero output documents",
    ),
    StageTestCase(
        id="nonexistent_collection_field_ref",
        docs=None,
        pipeline=[{"$group": {"_id": "$v"}}],
        expected=[],
        msg="Non-existent collection should produce zero output documents",
    ),
    StageTestCase(
        id="empty_collection_null_id",
        docs=[],
        pipeline=[{"$group": {"_id": None}}],
        expected=[],
        msg="_id: null on empty collection should produce zero output documents",
    ),
    StageTestCase(
        id="empty_collection_constant_id",
        docs=[],
        pipeline=[{"$group": {"_id": 1}}],
        expected=[],
        msg="_id: <constant> on empty collection should produce zero output documents",
    ),
]

# Property [Re-Grouping]: $group output can be consumed by subsequent $group
# stages, including triple-$group pipelines.
GROUP_RE_GROUPING_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="double_group",
        docs=[
            {"_id": 1, "dept": "eng", "x": 10},
            {"_id": 2, "dept": "eng", "x": 20},
            {"_id": 3, "dept": "sales", "x": 30},
            {"_id": 4, "dept": "sales", "x": 40},
        ],
        pipeline=[
            {"$group": {"_id": "$dept", "total": {"$sum": "$x"}}},
            {"$group": {"_id": None, "grand_total": {"$sum": "$total"}}},
        ],
        expected=[{"_id": None, "grand_total": 100}],
        msg="$group output should be consumable by a subsequent $group stage",
    ),
    StageTestCase(
        id="triple_group",
        docs=[
            {"_id": 1, "dept": "eng", "team": "a", "x": 5},
            {"_id": 2, "dept": "eng", "team": "a", "x": 15},
            {"_id": 3, "dept": "eng", "team": "b", "x": 10},
            {"_id": 4, "dept": "sales", "team": "c", "x": 20},
        ],
        pipeline=[
            {"$group": {"_id": {"dept": "$dept", "team": "$team"}, "team_total": {"$sum": "$x"}}},
            {"$group": {"_id": "$_id.dept", "dept_total": {"$sum": "$team_total"}}},
            {"$group": {"_id": None, "grand_total": {"$sum": "$dept_total"}}},
        ],
        expected=[{"_id": None, "grand_total": 50}],
        msg="Triple $group pipeline should work correctly",
    ),
]

# Property [$push Preserves Document Order]: $push preserves document order
# within groups when preceded by $sort.
GROUP_PUSH_ORDER_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="push_preserves_order_after_sort",
        docs=[
            {"_id": 1, "g": "a", "v": 3},
            {"_id": 2, "g": "a", "v": 1},
            {"_id": 3, "g": "a", "v": 2},
        ],
        pipeline=[
            {"$sort": {"v": 1}},
            {"$group": {"_id": "$g", "vals": {"$push": "$v"}}},
        ],
        expected=[{"_id": "a", "vals": [1, 2, 3]}],
        msg="$push should preserve document order within groups when preceded by $sort",
    ),
]

# Property [Timestamp Literal _id Not Replaced]: Timestamp(0, 0) used as a
# literal _id value is not replaced by the server, and Timestamp boundary
# values are accepted.
GROUP_TIMESTAMP_ID_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="timestamp_zero_zero_not_replaced",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": Timestamp(0, 0), "count": {"$sum": 1}}}],
        expected=[{"_id": Timestamp(0, 0), "count": 2}],
        msg=(
            "Timestamp(0, 0) as literal _id should not be replaced by the"
            " server (server-side replacement only occurs on insert)"
        ),
    ),
    StageTestCase(
        id="timestamp_max_time_component",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": TS_MAX_UNSIGNED32, "count": {"$sum": 1}}}],
        expected=[{"_id": TS_MAX_UNSIGNED32, "count": 2}],
        msg="Timestamp with max unsigned 32-bit time component should be accepted as _id",
    ),
    StageTestCase(
        id="timestamp_max_inc_component",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": Timestamp(1, 4_294_967_295), "count": {"$sum": 1}}}],
        expected=[{"_id": Timestamp(1, 4_294_967_295), "count": 2}],
        msg="Timestamp with max increment component should be accepted as _id",
    ),
    StageTestCase(
        id="timestamp_max_signed32_time",
        docs=[{"_id": 1}, {"_id": 2}],
        pipeline=[{"$group": {"_id": TS_MAX_SIGNED32, "count": {"$sum": 1}}}],
        expected=[{"_id": TS_MAX_SIGNED32, "count": 2}],
        msg="Timestamp with max signed 32-bit time component should be accepted as _id",
    ),
]

# Property [Compound _id Field Name Acceptance]: _id is accepted as a field
# name within a compound _id key, and Unicode, emoji, spaces, tabs, and long
# names are accepted as compound _id field names.
GROUP_COMPOUND_ID_FIELD_NAME_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="compound_id_with_id_field_name",
        docs=[{"_id": 1, "v": "a"}, {"_id": 2, "v": "b"}],
        pipeline=[{"$group": {"_id": {"_id": "$v"}, "count": {"$sum": 1}}}],
        expected=[
            {"_id": {"_id": "a"}, "count": 1},
            {"_id": {"_id": "b"}, "count": 1},
        ],
        msg="_id as a field name within a compound _id key should be allowed",
    ),
    StageTestCase(
        id="compound_id_unicode_field_name",
        docs=[{"_id": 1, "v": 10}],
        pipeline=[{"$group": {"_id": {"\u00e9": "$v"}, "count": {"$sum": 1}}}],
        expected=[{"_id": {"\u00e9": 10}, "count": 1}],
        msg="Unicode characters should be accepted as compound _id field names",
    ),
    StageTestCase(
        id="compound_id_emoji_field_name",
        docs=[{"_id": 1, "v": 10}],
        pipeline=[{"$group": {"_id": {"\U0001f600": "$v"}, "count": {"$sum": 1}}}],
        expected=[{"_id": {"\U0001f600": 10}, "count": 1}],
        msg="Emoji should be accepted as compound _id field names",
    ),
    StageTestCase(
        id="compound_id_space_field_name",
        docs=[{"_id": 1, "v": 10}],
        pipeline=[{"$group": {"_id": {" ": "$v"}, "count": {"$sum": 1}}}],
        expected=[{"_id": {" ": 10}, "count": 1}],
        msg="Space should be accepted as a compound _id field name",
    ),
    StageTestCase(
        id="compound_id_tab_field_name",
        docs=[{"_id": 1, "v": 10}],
        pipeline=[{"$group": {"_id": {"\t": "$v"}, "count": {"$sum": 1}}}],
        expected=[{"_id": {"\t": 10}, "count": 1}],
        msg="Tab should be accepted as a compound _id field name",
    ),
    StageTestCase(
        id="compound_id_long_field_name",
        docs=[{"_id": 1, "v": 10}],
        pipeline=[{"$group": {"_id": {"a" * 200: "$v"}, "count": {"$sum": 1}}}],
        expected=[{"_id": {"a" * 200: 10}, "count": 1}],
        msg="Long field names (200 chars) should be accepted as compound _id field names",
    ),
]

# Property [Inclusion-Style Detection Bypass]: nested objects with numeric
# values do not trigger the inclusion-style check, and $literal wrapping
# bypasses it.
GROUP_INCLUSION_STYLE_BYPASS_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="inclusion_nested_numeric_ok",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": {"a": {"b": 1}}, "count": {"$sum": 1}}}],
        expected=[{"_id": {"a": {"b": 1}}, "count": 1}],
        msg="Nested object with numeric value should not trigger inclusion-style error",
    ),
    StageTestCase(
        id="inclusion_literal_bypass",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": {"a": {"$literal": 1}}, "count": {"$sum": 1}}}],
        expected=[{"_id": {"a": 1}, "count": 1}],
        msg="$literal wrapping should bypass the inclusion-style check",
    ),
]

GROUP_SUCCESS_TESTS = (
    GROUP_NULL_MISSING_TESTS
    + GROUP_KEY_BEHAVIOR_TESTS
    + GROUP_GROUPING_EQUIVALENCE_TESTS
    + GROUP_TYPE_ACCEPTANCE_ID_TESTS
    + GROUP_NUMERIC_BOUNDARY_TESTS
    + GROUP_ARRAY_ID_TESTS
    + GROUP_ACCUMULATOR_SPEC_TESTS
    + GROUP_SUM_TYPE_PROMOTION_TESTS
    + GROUP_AVG_RETURN_TYPE_TESTS
    + GROUP_ADDTOSET_DEDUP_TESTS
    + GROUP_MIN_MAX_MIXED_TYPES_TESTS
    + GROUP_EXPRESSION_ARGS_TESTS
    + GROUP_EMPTY_INPUT_TESTS
    + GROUP_RE_GROUPING_TESTS
    + GROUP_PUSH_ORDER_TESTS
    + GROUP_TIMESTAMP_ID_TESTS
    + GROUP_COMPOUND_ID_FIELD_NAME_TESTS
    + GROUP_INCLUSION_STYLE_BYPASS_TESTS
)


@pytest.mark.aggregate
@pytest.mark.parametrize("test_case", pytest_params(GROUP_SUCCESS_TESTS))
def test_group_stage(collection, test_case: StageTestCase):
    """Test $group stage success cases."""
    populate_collection(collection, test_case)
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": test_case.pipeline,
            "cursor": {},
        },
    )
    assertResult(
        result,
        expected=test_case.expected,
        error_code=test_case.error_code,
        msg=test_case.msg,
        ignore_doc_order=True,
    )


# Property [Accumulator Null and Missing - Error Cases]: $mergeObjects errors
# on non-object input, and $setUnion and $concatArrays error on null or
# non-array input.
GROUP_ACCUMULATOR_NULL_MISSING_ERROR_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="mergeobjects_non_object_error",
        docs=[{"_id": 1, "v": "hello"}],
        pipeline=[{"$group": {"_id": None, "r": {"$mergeObjects": "$v"}}}],
        error_code=MERGE_OBJECTS_NON_OBJECT_ERROR,
        msg="$mergeObjects should error on non-object input",
    ),
    StageTestCase(
        id="setunion_null_error",
        docs=[{"_id": 1, "v": None}],
        pipeline=[{"$group": {"_id": None, "r": {"$setUnion": "$v"}}}],
        error_code=TYPE_MISMATCH_ERROR,
        msg="$setUnion should error on null input",
    ),
    StageTestCase(
        id="setunion_non_array_error",
        docs=[{"_id": 1, "v": "hello"}],
        pipeline=[{"$group": {"_id": None, "r": {"$setUnion": "$v"}}}],
        error_code=TYPE_MISMATCH_ERROR,
        msg="$setUnion should error on non-array input",
    ),
    StageTestCase(
        id="concatarrays_null_error",
        docs=[{"_id": 1, "v": None}],
        pipeline=[{"$group": {"_id": None, "r": {"$concatArrays": "$v"}}}],
        error_code=TYPE_MISMATCH_ERROR,
        msg="$concatArrays should error on null input",
    ),
    StageTestCase(
        id="concatarrays_non_array_error",
        docs=[{"_id": 1, "v": 42}],
        pipeline=[{"$group": {"_id": None, "r": {"$concatArrays": "$v"}}}],
        error_code=TYPE_MISMATCH_ERROR,
        msg="$concatArrays should error on non-array input",
    ),
]

# Property [Structural Errors on Empty and Non-Existent Collections]: structural
# validation errors fire even when the collection is empty or does not exist.
GROUP_EMPTY_INPUT_ERROR_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="empty_collection_non_object_error",
        docs=[],
        pipeline=[{"$group": "not_an_object"}],
        error_code=GROUP_NON_OBJECT_ERROR,
        msg="Non-object $group argument should error even on empty collection",
    ),
    StageTestCase(
        id="nonexistent_collection_non_object_error",
        docs=None,
        pipeline=[{"$group": "not_an_object"}],
        error_code=GROUP_NON_OBJECT_ERROR,
        msg="Non-object $group argument should error even on non-existent collection",
    ),
    StageTestCase(
        id="empty_collection_missing_id_error",
        docs=[],
        pipeline=[{"$group": {}}],
        error_code=GROUP_MISSING_ID_ERROR,
        msg="Missing _id should error even on empty collection",
    ),
    StageTestCase(
        id="nonexistent_collection_missing_id_error",
        docs=None,
        pipeline=[{"$group": {}}],
        error_code=GROUP_MISSING_ID_ERROR,
        msg="Missing _id should error even on non-existent collection",
    ),
]

# Property [Stage Argument Type Rejection]: all non-object BSON types as the
# $group stage argument produce an error.
GROUP_STAGE_ARGUMENT_ERROR_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="stage_arg_string",
        docs=[{"_id": 1}],
        pipeline=[{"$group": "not_an_object"}],
        error_code=GROUP_NON_OBJECT_ERROR,
        msg="String as $group argument should produce an error",
    ),
    StageTestCase(
        id="stage_arg_int32",
        docs=[{"_id": 1}],
        pipeline=[{"$group": 1}],
        error_code=GROUP_NON_OBJECT_ERROR,
        msg="int32 as $group argument should produce an error",
    ),
    StageTestCase(
        id="stage_arg_int64",
        docs=[{"_id": 1}],
        pipeline=[{"$group": Int64(1)}],
        error_code=GROUP_NON_OBJECT_ERROR,
        msg="Int64 as $group argument should produce an error",
    ),
    StageTestCase(
        id="stage_arg_double",
        docs=[{"_id": 1}],
        pipeline=[{"$group": 1.5}],
        error_code=GROUP_NON_OBJECT_ERROR,
        msg="double as $group argument should produce an error",
    ),
    StageTestCase(
        id="stage_arg_decimal128",
        docs=[{"_id": 1}],
        pipeline=[{"$group": Decimal128("1")}],
        error_code=GROUP_NON_OBJECT_ERROR,
        msg="Decimal128 as $group argument should produce an error",
    ),
    StageTestCase(
        id="stage_arg_bool",
        docs=[{"_id": 1}],
        pipeline=[{"$group": True}],
        error_code=GROUP_NON_OBJECT_ERROR,
        msg="bool as $group argument should produce an error",
    ),
    StageTestCase(
        id="stage_arg_null",
        docs=[{"_id": 1}],
        pipeline=[{"$group": None}],
        error_code=GROUP_NON_OBJECT_ERROR,
        msg="null as $group argument should produce an error",
    ),
    StageTestCase(
        id="stage_arg_array",
        docs=[{"_id": 1}],
        pipeline=[{"$group": [1, 2]}],
        error_code=GROUP_NON_OBJECT_ERROR,
        msg="array as $group argument should produce an error",
    ),
    StageTestCase(
        id="stage_arg_objectid",
        docs=[{"_id": 1}],
        pipeline=[{"$group": ObjectId("507f1f77bcf86cd799439011")}],
        error_code=GROUP_NON_OBJECT_ERROR,
        msg="ObjectId as $group argument should produce an error",
    ),
    StageTestCase(
        id="stage_arg_datetime",
        docs=[{"_id": 1}],
        pipeline=[{"$group": datetime(2024, 1, 1)}],
        error_code=GROUP_NON_OBJECT_ERROR,
        msg="datetime as $group argument should produce an error",
    ),
    StageTestCase(
        id="stage_arg_timestamp",
        docs=[{"_id": 1}],
        pipeline=[{"$group": Timestamp(1, 1)}],
        error_code=GROUP_NON_OBJECT_ERROR,
        msg="Timestamp as $group argument should produce an error",
    ),
    StageTestCase(
        id="stage_arg_binary",
        docs=[{"_id": 1}],
        pipeline=[{"$group": Binary(b"data")}],
        error_code=GROUP_NON_OBJECT_ERROR,
        msg="Binary as $group argument should produce an error",
    ),
    StageTestCase(
        id="stage_arg_regex",
        docs=[{"_id": 1}],
        pipeline=[{"$group": Regex("abc", "i")}],
        error_code=GROUP_NON_OBJECT_ERROR,
        msg="Regex as $group argument should produce an error",
    ),
    StageTestCase(
        id="stage_arg_code",
        docs=[{"_id": 1}],
        pipeline=[{"$group": Code("function(){}")}],
        error_code=GROUP_NON_OBJECT_ERROR,
        msg="Code as $group argument should produce an error",
    ),
    StageTestCase(
        id="stage_arg_code_with_scope",
        docs=[{"_id": 1}],
        pipeline=[{"$group": Code("function(){}", {"x": 1})}],
        error_code=GROUP_NON_OBJECT_ERROR,
        msg="Code with scope as $group argument should produce an error",
    ),
    StageTestCase(
        id="stage_arg_minkey",
        docs=[{"_id": 1}],
        pipeline=[{"$group": MinKey()}],
        error_code=GROUP_NON_OBJECT_ERROR,
        msg="MinKey as $group argument should produce an error",
    ),
    StageTestCase(
        id="stage_arg_maxkey",
        docs=[{"_id": 1}],
        pipeline=[{"$group": MaxKey()}],
        error_code=GROUP_NON_OBJECT_ERROR,
        msg="MaxKey as $group argument should produce an error",
    ),
]

# Property [Missing _id Error]: omitting _id from the $group document produces
# an error, even on empty or non-existent collections.
GROUP_MISSING_ID_ERROR_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="missing_id_with_accumulator",
        docs=[{"_id": 1, "v": 10}],
        pipeline=[{"$group": {"total": {"$sum": "$v"}}}],
        error_code=GROUP_MISSING_ID_ERROR,
        msg="Omitting _id from $group should produce an error",
    ),
]

# Property [Compound _id Field Name Errors]: dots, empty strings, and
# $-prefixed names in compound _id field names produce errors.
GROUP_COMPOUND_ID_FIELD_NAME_ERROR_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="compound_id_dot_in_field_name",
        docs=[{"_id": 1, "v": 10}],
        pipeline=[{"$group": {"_id": {"a.b": "$v"}}}],
        error_code=FIELD_PATH_DOT_ERROR,
        msg="Dot in compound _id field name should produce an error",
    ),
    StageTestCase(
        id="compound_id_empty_string_field_name",
        docs=[{"_id": 1, "v": 10}],
        pipeline=[{"$group": {"_id": {"": "$v"}}}],
        error_code=FIELD_PATH_EMPTY_COMPONENT_ERROR,
        msg="Empty string as compound _id field name should produce an error",
    ),
    StageTestCase(
        id="compound_id_dollar_prefix_field_name",
        docs=[{"_id": 1, "v": 10}],
        pipeline=[{"$group": {"_id": {"$bad": "$v"}}}],
        error_code=UNRECOGNIZED_EXPRESSION_ERROR,
        msg="$-prefixed compound _id field name should produce an error",
    ),
]

# Property [Inclusion-Style Detection Errors (Compound _id)]: any top-level
# field in a compound _id with a numeric (int32, Int64, double, Decimal128) or
# boolean value triggers an error; only top-level fields are checked, and
# $literal wrapping bypasses the check.
GROUP_INCLUSION_STYLE_ERROR_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="inclusion_int32",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": {"a": 1}}}],
        error_code=GROUP_INCLUSION_STYLE_ERROR,
        msg="int32 value in compound _id should trigger inclusion-style error",
    ),
    StageTestCase(
        id="inclusion_int64",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": {"a": Int64(1)}}}],
        error_code=GROUP_INCLUSION_STYLE_ERROR,
        msg="Int64 value in compound _id should trigger inclusion-style error",
    ),
    StageTestCase(
        id="inclusion_double",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": {"a": 1.5}}}],
        error_code=GROUP_INCLUSION_STYLE_ERROR,
        msg="double value in compound _id should trigger inclusion-style error",
    ),
    StageTestCase(
        id="inclusion_decimal128",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": {"a": Decimal128("1")}}}],
        error_code=GROUP_INCLUSION_STYLE_ERROR,
        msg="Decimal128 value in compound _id should trigger inclusion-style error",
    ),
    StageTestCase(
        id="inclusion_bool_true",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": {"a": True}}}],
        error_code=GROUP_INCLUSION_STYLE_ERROR,
        msg="boolean true in compound _id should trigger inclusion-style error",
    ),
    StageTestCase(
        id="inclusion_bool_false",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": {"a": False}}}],
        error_code=GROUP_INCLUSION_STYLE_ERROR,
        msg="boolean false in compound _id should trigger inclusion-style error",
    ),
]

# Property [Accumulator Field Name Errors]: $-prefixed accumulator field
# names and dot-containing accumulator field names are rejected.
GROUP_ACCUMULATOR_FIELD_NAME_ERROR_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="accumulator_dollar_prefix_field_name",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "$bad": {"$sum": 1}}}],
        error_code=GROUP_ACCUMULATOR_DOLLAR_FIELD_NAME_ERROR,
        msg="$-prefixed accumulator field name should produce an error",
    ),
    StageTestCase(
        id="accumulator_dot_field_name",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "a.b": {"$sum": 1}}}],
        error_code=GROUP_ACCUMULATOR_DOT_FIELD_NAME_ERROR,
        msg="Dot-containing accumulator field name should produce an error",
    ),
]

# Property [Accumulator Non-Object Value Rejection]: all non-object BSON
# types, empty objects, objects with a non-$-prefixed key, and objects with an
# empty string key as accumulator field value produce an error.
GROUP_ACCUMULATOR_NON_OBJECT_VALUE_ERROR_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="accum_value_int32",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": 1}}],
        error_code=GROUP_ACCUMULATOR_INVALID_VALUE_ERROR,
        msg="int32 as accumulator value should produce an error",
    ),
    StageTestCase(
        id="accum_value_int64",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": Int64(1)}}],
        error_code=GROUP_ACCUMULATOR_INVALID_VALUE_ERROR,
        msg="Int64 as accumulator value should produce an error",
    ),
    StageTestCase(
        id="accum_value_double",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": 1.5}}],
        error_code=GROUP_ACCUMULATOR_INVALID_VALUE_ERROR,
        msg="double as accumulator value should produce an error",
    ),
    StageTestCase(
        id="accum_value_decimal128",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": Decimal128("1")}}],
        error_code=GROUP_ACCUMULATOR_INVALID_VALUE_ERROR,
        msg="Decimal128 as accumulator value should produce an error",
    ),
    StageTestCase(
        id="accum_value_string",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": "hello"}}],
        error_code=GROUP_ACCUMULATOR_INVALID_VALUE_ERROR,
        msg="string as accumulator value should produce an error",
    ),
    StageTestCase(
        id="accum_value_bool",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": True}}],
        error_code=GROUP_ACCUMULATOR_INVALID_VALUE_ERROR,
        msg="bool as accumulator value should produce an error",
    ),
    StageTestCase(
        id="accum_value_null",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": None}}],
        error_code=GROUP_ACCUMULATOR_INVALID_VALUE_ERROR,
        msg="null as accumulator value should produce an error",
    ),
    StageTestCase(
        id="accum_value_array",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": [1, 2]}}],
        error_code=GROUP_ACCUMULATOR_INVALID_VALUE_ERROR,
        msg="array as accumulator value should produce an error",
    ),
    StageTestCase(
        id="accum_value_objectid",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": ObjectId("507f1f77bcf86cd799439011")}}],
        error_code=GROUP_ACCUMULATOR_INVALID_VALUE_ERROR,
        msg="ObjectId as accumulator value should produce an error",
    ),
    StageTestCase(
        id="accum_value_datetime",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": datetime(2024, 1, 1)}}],
        error_code=GROUP_ACCUMULATOR_INVALID_VALUE_ERROR,
        msg="datetime as accumulator value should produce an error",
    ),
    StageTestCase(
        id="accum_value_timestamp",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": Timestamp(1, 1)}}],
        error_code=GROUP_ACCUMULATOR_INVALID_VALUE_ERROR,
        msg="Timestamp as accumulator value should produce an error",
    ),
    StageTestCase(
        id="accum_value_binary",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": Binary(b"data")}}],
        error_code=GROUP_ACCUMULATOR_INVALID_VALUE_ERROR,
        msg="Binary as accumulator value should produce an error",
    ),
    StageTestCase(
        id="accum_value_regex",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": Regex("abc", "i")}}],
        error_code=GROUP_ACCUMULATOR_INVALID_VALUE_ERROR,
        msg="Regex as accumulator value should produce an error",
    ),
    StageTestCase(
        id="accum_value_code",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": Code("function(){}")}}],
        error_code=GROUP_ACCUMULATOR_INVALID_VALUE_ERROR,
        msg="Code as accumulator value should produce an error",
    ),
    StageTestCase(
        id="accum_value_code_with_scope",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": Code("function(){}", {"x": 1})}}],
        error_code=GROUP_ACCUMULATOR_INVALID_VALUE_ERROR,
        msg="Code with scope as accumulator value should produce an error",
    ),
    StageTestCase(
        id="accum_value_minkey",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": MinKey()}}],
        error_code=GROUP_ACCUMULATOR_INVALID_VALUE_ERROR,
        msg="MinKey as accumulator value should produce an error",
    ),
    StageTestCase(
        id="accum_value_maxkey",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": MaxKey()}}],
        error_code=GROUP_ACCUMULATOR_INVALID_VALUE_ERROR,
        msg="MaxKey as accumulator value should produce an error",
    ),
    StageTestCase(
        id="accum_value_empty_object",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": {}}}],
        error_code=GROUP_ACCUMULATOR_INVALID_VALUE_ERROR,
        msg="Empty object as accumulator value should produce an error",
    ),
    StageTestCase(
        id="accum_value_non_dollar_key",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": {"bad": 1}}}],
        error_code=GROUP_ACCUMULATOR_INVALID_VALUE_ERROR,
        msg="Object with non-$-prefixed key as accumulator value should produce an error",
    ),
    StageTestCase(
        id="accum_value_empty_string_key",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": {"": 1}}}],
        error_code=GROUP_ACCUMULATOR_INVALID_VALUE_ERROR,
        msg="Object with empty string key as accumulator value should produce an error",
    ),
]

# Property [Accumulator Array Argument Rejection]: array literal arguments to
# accumulators and non-accumulator expressions with array arguments in
# accumulator position produce an error.
GROUP_ACCUMULATOR_ARRAY_ARGUMENT_ERROR_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="accum_array_arg_sum",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": {"$sum": [1, 2]}}}],
        error_code=GROUP_ACCUMULATOR_ARRAY_ARGUMENT_ERROR,
        msg="$sum with array literal argument should produce an error",
    ),
    StageTestCase(
        id="accum_array_arg_non_accumulator",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": {"$add": [1, 2]}}}],
        error_code=GROUP_ACCUMULATOR_ARRAY_ARGUMENT_ERROR,
        msg="Non-accumulator expression with array argument should produce an error",
    ),
]

# Property [Accumulator Unknown Operator Rejection]: bare $, $$-prefixed keys,
# unknown $-prefixed operators, and non-accumulator expressions with non-array
# arguments in accumulator position produce an error.
GROUP_ACCUMULATOR_UNKNOWN_OPERATOR_ERROR_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="accum_non_accumulator_non_array",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": {"$add": 1}}}],
        error_code=GROUP_UNKNOWN_OPERATOR_ERROR,
        msg="Non-accumulator expression with non-array argument should produce an error",
    ),
    StageTestCase(
        id="accum_bare_dollar",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": {"$": 1}}}],
        error_code=GROUP_UNKNOWN_OPERATOR_ERROR,
        msg="Bare $ as accumulator key should produce an error",
    ),
    StageTestCase(
        id="accum_double_dollar_key",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": {"$$ROOT": 1}}}],
        error_code=GROUP_UNKNOWN_OPERATOR_ERROR,
        msg="$$-prefixed key in accumulator object should produce an error",
    ),
    StageTestCase(
        id="accum_unknown_dollar_op",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": {"$fakeOp": 1}}}],
        error_code=GROUP_UNKNOWN_OPERATOR_ERROR,
        msg="Unknown $-prefixed operator should produce an error",
    ),
]

# Property [Accumulator Multiple Keys Rejection]: mixed $-prefixed and
# non-$-prefixed keys or multiple $-prefixed keys in an accumulator object
# produce an error.
GROUP_ACCUMULATOR_MULTIPLE_KEYS_ERROR_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="accum_mixed_keys",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": SON([("$sum", 1), ("extra", 2)])}}],
        error_code=GROUP_ACCUMULATOR_MULTIPLE_KEYS_ERROR,
        msg="Mixed $-prefixed and non-$-prefixed keys should produce an error",
    ),
    StageTestCase(
        id="accum_multiple_dollar_keys",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": SON([("$sum", 1), ("$avg", "$v")]),
                }
            }
        ],
        error_code=GROUP_ACCUMULATOR_MULTIPLE_KEYS_ERROR,
        msg="Multiple $-prefixed keys in accumulator object should produce an error",
    ),
]

# Property [$count Non-Empty Argument]: $count rejects any non-empty
# argument with an error, and the array argument check fires before the
# $count-specific validation.
GROUP_COUNT_ACCUMULATOR_VALIDATION_ERROR_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="count_with_string_argument",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": {"$count": "hello"}}}],
        error_code=TYPE_MISMATCH_ERROR,
        msg="$count with a non-empty string argument should produce an error",
    ),
    StageTestCase(
        id="count_with_empty_array_argument",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": None, "r": {"$count": []}}}],
        error_code=GROUP_ACCUMULATOR_ARRAY_ARGUMENT_ERROR,
        msg="$count with an empty array fires the array check before $count validation",
    ),
]

# Property [String _id Reference Errors]: bare "$" in _id produces an
# invalid field path error, bare "$$" produces a failed-to-parse error, and
# $$CLUSTER_TIME in _id produces a standalone-mode error.
GROUP_STRING_ID_REFERENCE_ERROR_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="bare_dollar_in_id",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": "$"}}],
        error_code=INVALID_DOLLAR_FIELD_PATH,
        msg="Bare '$' in _id should produce an invalid field path error",
    ),
    StageTestCase(
        id="bare_double_dollar_in_id",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": "$$"}}],
        error_code=FAILED_TO_PARSE_ERROR,
        msg="Bare '$$' in _id should produce a failed-to-parse error",
    ),
    StageTestCase(
        id="cluster_time_in_id_standalone",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"_id": "$$CLUSTER_TIME"}}],
        error_code=CLUSTER_TIME_STANDALONE_ERROR,
        msg="$$CLUSTER_TIME in _id should produce an error in standalone mode",
    ),
]

# Property [N-Accumulator Invalid n]: $topN, $bottomN, $firstN, $lastN,
# $maxN, and $minN reject null, string, zero, and negative values for n.
GROUP_N_ACCUMULATOR_INVALID_N_ERROR_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="topn_null_n",
        docs=[{"_id": 1, "v": 5, "s": 1}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {"$topN": {"sortBy": {"s": 1}, "output": "$v", "n": None}},
                }
            }
        ],
        error_code=N_ACCUMULATOR_INVALID_N_ERROR,
        msg="$topN with null n should produce an error",
    ),
    StageTestCase(
        id="topn_string_n",
        docs=[{"_id": 1, "v": 5, "s": 1}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {"$topN": {"sortBy": {"s": 1}, "output": "$v", "n": "bad"}},
                }
            }
        ],
        error_code=N_ACCUMULATOR_INVALID_N_ERROR,
        msg="$topN with string n should produce an error",
    ),
    StageTestCase(
        id="topn_zero_n",
        docs=[{"_id": 1, "v": 5, "s": 1}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {"$topN": {"sortBy": {"s": 1}, "output": "$v", "n": 0}},
                }
            }
        ],
        error_code=N_ACCUMULATOR_INVALID_N_ERROR,
        msg="$topN with n=0 should produce an error",
    ),
    StageTestCase(
        id="topn_negative_n",
        docs=[{"_id": 1, "v": 5, "s": 1}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {"$topN": {"sortBy": {"s": 1}, "output": "$v", "n": -1}},
                }
            }
        ],
        error_code=N_ACCUMULATOR_INVALID_N_ERROR,
        msg="$topN with negative n should produce an error",
    ),
    StageTestCase(
        id="bottomn_null_n",
        docs=[{"_id": 1, "v": 5, "s": 1}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {"$bottomN": {"sortBy": {"s": 1}, "output": "$v", "n": None}},
                }
            }
        ],
        error_code=N_ACCUMULATOR_INVALID_N_ERROR,
        msg="$bottomN with null n should produce an error",
    ),
    StageTestCase(
        id="bottomn_string_n",
        docs=[{"_id": 1, "v": 5, "s": 1}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {"$bottomN": {"sortBy": {"s": 1}, "output": "$v", "n": "bad"}},
                }
            }
        ],
        error_code=N_ACCUMULATOR_INVALID_N_ERROR,
        msg="$bottomN with string n should produce an error",
    ),
    StageTestCase(
        id="bottomn_zero_n",
        docs=[{"_id": 1, "v": 5, "s": 1}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {"$bottomN": {"sortBy": {"s": 1}, "output": "$v", "n": 0}},
                }
            }
        ],
        error_code=N_ACCUMULATOR_INVALID_N_ERROR,
        msg="$bottomN with n=0 should produce an error",
    ),
    StageTestCase(
        id="bottomn_negative_n",
        docs=[{"_id": 1, "v": 5, "s": 1}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {"$bottomN": {"sortBy": {"s": 1}, "output": "$v", "n": -1}},
                }
            }
        ],
        error_code=N_ACCUMULATOR_INVALID_N_ERROR,
        msg="$bottomN with negative n should produce an error",
    ),
    StageTestCase(
        id="firstn_null_n",
        docs=[{"_id": 1, "v": 5}],
        pipeline=[{"$group": {"_id": None, "r": {"$firstN": {"input": "$v", "n": None}}}}],
        error_code=N_ACCUMULATOR_INVALID_N_ERROR,
        msg="$firstN with null n should produce an error",
    ),
    StageTestCase(
        id="firstn_string_n",
        docs=[{"_id": 1, "v": 5}],
        pipeline=[{"$group": {"_id": None, "r": {"$firstN": {"input": "$v", "n": "bad"}}}}],
        error_code=N_ACCUMULATOR_INVALID_N_ERROR,
        msg="$firstN with string n should produce an error",
    ),
    StageTestCase(
        id="firstn_zero_n",
        docs=[{"_id": 1, "v": 5}],
        pipeline=[{"$group": {"_id": None, "r": {"$firstN": {"input": "$v", "n": 0}}}}],
        error_code=N_ACCUMULATOR_INVALID_N_ERROR,
        msg="$firstN with n=0 should produce an error",
    ),
    StageTestCase(
        id="firstn_negative_n",
        docs=[{"_id": 1, "v": 5}],
        pipeline=[{"$group": {"_id": None, "r": {"$firstN": {"input": "$v", "n": -1}}}}],
        error_code=N_ACCUMULATOR_INVALID_N_ERROR,
        msg="$firstN with negative n should produce an error",
    ),
    StageTestCase(
        id="lastn_null_n",
        docs=[{"_id": 1, "v": 5}],
        pipeline=[{"$group": {"_id": None, "r": {"$lastN": {"input": "$v", "n": None}}}}],
        error_code=N_ACCUMULATOR_INVALID_N_ERROR,
        msg="$lastN with null n should produce an error",
    ),
    StageTestCase(
        id="lastn_string_n",
        docs=[{"_id": 1, "v": 5}],
        pipeline=[{"$group": {"_id": None, "r": {"$lastN": {"input": "$v", "n": "bad"}}}}],
        error_code=N_ACCUMULATOR_INVALID_N_ERROR,
        msg="$lastN with string n should produce an error",
    ),
    StageTestCase(
        id="lastn_zero_n",
        docs=[{"_id": 1, "v": 5}],
        pipeline=[{"$group": {"_id": None, "r": {"$lastN": {"input": "$v", "n": 0}}}}],
        error_code=N_ACCUMULATOR_INVALID_N_ERROR,
        msg="$lastN with n=0 should produce an error",
    ),
    StageTestCase(
        id="lastn_negative_n",
        docs=[{"_id": 1, "v": 5}],
        pipeline=[{"$group": {"_id": None, "r": {"$lastN": {"input": "$v", "n": -1}}}}],
        error_code=N_ACCUMULATOR_INVALID_N_ERROR,
        msg="$lastN with negative n should produce an error",
    ),
    StageTestCase(
        id="maxn_null_n",
        docs=[{"_id": 1, "v": 5}],
        pipeline=[{"$group": {"_id": None, "r": {"$maxN": {"input": "$v", "n": None}}}}],
        error_code=N_ACCUMULATOR_INVALID_N_ERROR,
        msg="$maxN with null n should produce an error",
    ),
    StageTestCase(
        id="maxn_string_n",
        docs=[{"_id": 1, "v": 5}],
        pipeline=[{"$group": {"_id": None, "r": {"$maxN": {"input": "$v", "n": "bad"}}}}],
        error_code=N_ACCUMULATOR_INVALID_N_ERROR,
        msg="$maxN with string n should produce an error",
    ),
    StageTestCase(
        id="maxn_zero_n",
        docs=[{"_id": 1, "v": 5}],
        pipeline=[{"$group": {"_id": None, "r": {"$maxN": {"input": "$v", "n": 0}}}}],
        error_code=N_ACCUMULATOR_INVALID_N_ERROR,
        msg="$maxN with n=0 should produce an error",
    ),
    StageTestCase(
        id="maxn_negative_n",
        docs=[{"_id": 1, "v": 5}],
        pipeline=[{"$group": {"_id": None, "r": {"$maxN": {"input": "$v", "n": -1}}}}],
        error_code=N_ACCUMULATOR_INVALID_N_ERROR,
        msg="$maxN with negative n should produce an error",
    ),
    StageTestCase(
        id="minn_null_n",
        docs=[{"_id": 1, "v": 5}],
        pipeline=[{"$group": {"_id": None, "r": {"$minN": {"input": "$v", "n": None}}}}],
        error_code=N_ACCUMULATOR_INVALID_N_ERROR,
        msg="$minN with null n should produce an error",
    ),
    StageTestCase(
        id="minn_string_n",
        docs=[{"_id": 1, "v": 5}],
        pipeline=[{"$group": {"_id": None, "r": {"$minN": {"input": "$v", "n": "bad"}}}}],
        error_code=N_ACCUMULATOR_INVALID_N_ERROR,
        msg="$minN with string n should produce an error",
    ),
    StageTestCase(
        id="minn_zero_n",
        docs=[{"_id": 1, "v": 5}],
        pipeline=[{"$group": {"_id": None, "r": {"$minN": {"input": "$v", "n": 0}}}}],
        error_code=N_ACCUMULATOR_INVALID_N_ERROR,
        msg="$minN with n=0 should produce an error",
    ),
    StageTestCase(
        id="minn_negative_n",
        docs=[{"_id": 1, "v": 5}],
        pipeline=[{"$group": {"_id": None, "r": {"$minN": {"input": "$v", "n": -1}}}}],
        error_code=N_ACCUMULATOR_INVALID_N_ERROR,
        msg="$minN with negative n should produce an error",
    ),
]

# Property [N-Accumulator Missing n]: missing n produces different error codes
# by accumulator family - $topN/$bottomN and $firstN/$lastN/$maxN/$minN use
# distinct error codes.
GROUP_N_ACCUMULATOR_MISSING_N_ERROR_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="topn_missing_n",
        docs=[{"_id": 1, "v": 5, "s": 1}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {"$topN": {"sortBy": {"s": 1}, "output": "$v"}},
                }
            }
        ],
        error_code=N_ACCUMULATOR_MISSING_N_TOPN_FAMILY_ERROR,
        msg="$topN with missing n should produce an error",
    ),
    StageTestCase(
        id="bottomn_missing_n",
        docs=[{"_id": 1, "v": 5, "s": 1}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {"$bottomN": {"sortBy": {"s": 1}, "output": "$v"}},
                }
            }
        ],
        error_code=N_ACCUMULATOR_MISSING_N_TOPN_FAMILY_ERROR,
        msg="$bottomN with missing n should produce an error",
    ),
    StageTestCase(
        id="firstn_missing_n",
        docs=[{"_id": 1, "v": 5}],
        pipeline=[{"$group": {"_id": None, "r": {"$firstN": {"input": "$v"}}}}],
        error_code=N_ACCUMULATOR_MISSING_N_FIRSTN_FAMILY_ERROR,
        msg="$firstN with missing n should produce an error",
    ),
    StageTestCase(
        id="lastn_missing_n",
        docs=[{"_id": 1, "v": 5}],
        pipeline=[{"$group": {"_id": None, "r": {"$lastN": {"input": "$v"}}}}],
        error_code=N_ACCUMULATOR_MISSING_N_FIRSTN_FAMILY_ERROR,
        msg="$lastN with missing n should produce an error",
    ),
    StageTestCase(
        id="maxn_missing_n",
        docs=[{"_id": 1, "v": 5}],
        pipeline=[{"$group": {"_id": None, "r": {"$maxN": {"input": "$v"}}}}],
        error_code=N_ACCUMULATOR_MISSING_N_FIRSTN_FAMILY_ERROR,
        msg="$maxN with missing n should produce an error",
    ),
    StageTestCase(
        id="minn_missing_n",
        docs=[{"_id": 1, "v": 5}],
        pipeline=[{"$group": {"_id": None, "r": {"$minN": {"input": "$v"}}}}],
        error_code=N_ACCUMULATOR_MISSING_N_FIRSTN_FAMILY_ERROR,
        msg="$minN with missing n should produce an error",
    ),
]

# Property [$accumulator Validation Errors]: $accumulator rejects null
# init/accumulate/merge functions and requires accumulateArgs to be present.
GROUP_ACCUMULATOR_JS_VALIDATION_ERROR_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="accumulator_null_init",
        docs=[{"_id": 1, "x": 5}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {
                        "$accumulator": {
                            "init": None,
                            "accumulate": "function(s, v) { return s + v; }",
                            "accumulateArgs": ["$x"],
                            "merge": "function(s1, s2) { return s1 + s2; }",
                            "lang": "js",
                        }
                    },
                }
            }
        ],
        error_code=ACCUMULATOR_NULL_FUNCTION_ERROR,
        msg="$accumulator with null init should produce an error",
    ),
    StageTestCase(
        id="accumulator_null_accumulate",
        docs=[{"_id": 1, "x": 5}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {
                        "$accumulator": {
                            "init": "function() { return 0; }",
                            "accumulate": None,
                            "accumulateArgs": ["$x"],
                            "merge": "function(s1, s2) { return s1 + s2; }",
                            "lang": "js",
                        }
                    },
                }
            }
        ],
        error_code=ACCUMULATOR_NULL_FUNCTION_ERROR,
        msg="$accumulator with null accumulate should produce an error",
    ),
    StageTestCase(
        id="accumulator_null_merge",
        docs=[{"_id": 1, "x": 5}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {
                        "$accumulator": {
                            "init": "function() { return 0; }",
                            "accumulate": "function(s, v) { return s + v; }",
                            "accumulateArgs": ["$x"],
                            "merge": None,
                            "lang": "js",
                        }
                    },
                }
            }
        ],
        error_code=ACCUMULATOR_NULL_FUNCTION_ERROR,
        msg="$accumulator with null merge should produce an error",
    ),
    StageTestCase(
        id="accumulator_missing_accumulate_args",
        docs=[{"_id": 1, "x": 5}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {
                        "$accumulator": {
                            "init": "function() { return 0; }",
                            "accumulate": "function(s, v) { return s + v; }",
                            "merge": "function(s1, s2) { return s1 + s2; }",
                            "lang": "js",
                        }
                    },
                }
            }
        ],
        error_code=ACCUMULATOR_MISSING_ACCUMULATE_ARGS_ERROR,
        msg="$accumulator with missing accumulateArgs should produce an error",
    ),
]

# Property [$percentile Parameter Validation]: $percentile rejects invalid
# percentile values outside [0, 1], empty or null p arrays, invalid method
# strings, and missing method field.
GROUP_PERCENTILE_PARAM_VALIDATION_ERROR_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="percentile_p_value_above_one",
        docs=[{"_id": 1, "x": 5}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {
                        "$percentile": {
                            "input": "$x",
                            "p": [1.5],
                            "method": "approximate",
                        }
                    },
                }
            }
        ],
        error_code=PERCENTILE_INVALID_P_VALUE_ERROR,
        msg="$percentile with p value > 1 should produce an error",
    ),
    StageTestCase(
        id="percentile_p_value_below_zero",
        docs=[{"_id": 1, "x": 5}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {
                        "$percentile": {
                            "input": "$x",
                            "p": [-0.1],
                            "method": "approximate",
                        }
                    },
                }
            }
        ],
        error_code=PERCENTILE_INVALID_P_VALUE_ERROR,
        msg="$percentile with p value < 0 should produce an error",
    ),
    StageTestCase(
        id="percentile_empty_p_array",
        docs=[{"_id": 1, "x": 5}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {
                        "$percentile": {
                            "input": "$x",
                            "p": [],
                            "method": "approximate",
                        }
                    },
                }
            }
        ],
        error_code=PERCENTILE_INVALID_P_FIELD_ERROR,
        msg="$percentile with empty p array should produce an error",
    ),
    StageTestCase(
        id="percentile_null_p",
        docs=[{"_id": 1, "x": 5}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {
                        "$percentile": {
                            "input": "$x",
                            "p": None,
                            "method": "approximate",
                        }
                    },
                }
            }
        ],
        error_code=PERCENTILE_INVALID_P_FIELD_ERROR,
        msg="$percentile with null p should produce an error",
    ),
    StageTestCase(
        id="percentile_invalid_method",
        docs=[{"_id": 1, "x": 5}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {
                        "$percentile": {
                            "input": "$x",
                            "p": [0.5],
                            "method": "invalid",
                        }
                    },
                }
            }
        ],
        error_code=BAD_VALUE_ERROR,
        msg="$percentile with invalid method should produce an error",
    ),
    StageTestCase(
        id="percentile_missing_method",
        docs=[{"_id": 1, "x": 5}],
        pipeline=[
            {
                "$group": {
                    "_id": None,
                    "r": {
                        "$percentile": {
                            "input": "$x",
                            "p": [0.5],
                        }
                    },
                }
            }
        ],
        error_code=MISSING_REQUIRED_FIELD_ERROR,
        msg="$percentile with missing method should produce an error",
    ),
]

# Property [Error Precedence]: errors are checked in this order: non-object
# stage argument before inclusion-style detection in compound _id before
# field-level errors in document order before missing _id, and when multiple
# fields have errors the first invalid field in document order determines the
# error code.
GROUP_ERROR_PRECEDENCE_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="precedence_inclusion_over_dollar_field",
        docs=[{"_id": 1}],
        pipeline=[{"$group": SON([("_id", {"a": 1}), ("$bad", {"$sum": 1})])}],
        error_code=GROUP_INCLUSION_STYLE_ERROR,
        msg="Inclusion-style error should take precedence over $-prefixed field name error",
    ),
    StageTestCase(
        id="precedence_dollar_field_over_missing_id",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"$bad": {"$sum": 1}}}],
        error_code=GROUP_ACCUMULATOR_DOLLAR_FIELD_NAME_ERROR,
        msg="$-prefixed field name error should take precedence over missing _id",
    ),
    StageTestCase(
        id="precedence_invalid_value_over_missing_id",
        docs=[{"_id": 1}],
        pipeline=[{"$group": {"total": "not_an_object"}}],
        error_code=GROUP_ACCUMULATOR_INVALID_VALUE_ERROR,
        msg="Non-object accumulator value error should take precedence over missing _id",
    ),
    StageTestCase(
        id="precedence_first_field_dollar_before_dot",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$group": SON(
                    [
                        ("_id", None),
                        ("$bad", {"$sum": 1}),
                        ("a.b", {"$sum": 1}),
                    ]
                )
            }
        ],
        error_code=GROUP_ACCUMULATOR_DOLLAR_FIELD_NAME_ERROR,
        msg="First invalid field ($-prefix) in document order should determine the error code",
    ),
    StageTestCase(
        id="precedence_first_field_dot_before_dollar",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$group": SON(
                    [
                        ("_id", None),
                        ("a.b", {"$sum": 1}),
                        ("$bad", {"$sum": 1}),
                    ]
                )
            }
        ],
        error_code=GROUP_ACCUMULATOR_DOT_FIELD_NAME_ERROR,
        msg="First invalid field (dot) in document order should determine the error code",
    ),
    StageTestCase(
        id="precedence_first_field_multiple_keys_before_dollar",
        docs=[{"_id": 1}],
        pipeline=[
            {
                "$group": SON(
                    [
                        ("_id", None),
                        ("r", {"$sum": 1, "$avg": 1}),
                        ("$bad", {"$sum": 1}),
                    ]
                )
            }
        ],
        error_code=GROUP_ACCUMULATOR_MULTIPLE_KEYS_ERROR,
        msg="Multiple keys error in first invalid field should take precedence over later $-prefix",
    ),
]

GROUP_ERROR_TESTS = (
    GROUP_ACCUMULATOR_NULL_MISSING_ERROR_TESTS
    + GROUP_EMPTY_INPUT_ERROR_TESTS
    + GROUP_STAGE_ARGUMENT_ERROR_TESTS
    + GROUP_MISSING_ID_ERROR_TESTS
    + GROUP_COMPOUND_ID_FIELD_NAME_ERROR_TESTS
    + GROUP_INCLUSION_STYLE_ERROR_TESTS
    + GROUP_ACCUMULATOR_FIELD_NAME_ERROR_TESTS
    + GROUP_ACCUMULATOR_NON_OBJECT_VALUE_ERROR_TESTS
    + GROUP_ACCUMULATOR_ARRAY_ARGUMENT_ERROR_TESTS
    + GROUP_ACCUMULATOR_UNKNOWN_OPERATOR_ERROR_TESTS
    + GROUP_ACCUMULATOR_MULTIPLE_KEYS_ERROR_TESTS
    + GROUP_COUNT_ACCUMULATOR_VALIDATION_ERROR_TESTS
    + GROUP_STRING_ID_REFERENCE_ERROR_TESTS
    + GROUP_N_ACCUMULATOR_INVALID_N_ERROR_TESTS
    + GROUP_N_ACCUMULATOR_MISSING_N_ERROR_TESTS
    + GROUP_ACCUMULATOR_JS_VALIDATION_ERROR_TESTS
    + GROUP_PERCENTILE_PARAM_VALIDATION_ERROR_TESTS
    + GROUP_ERROR_PRECEDENCE_TESTS
)


@pytest.mark.aggregate
@pytest.mark.parametrize("test_case", pytest_params(GROUP_ERROR_TESTS))
def test_group_stage_error(collection, test_case: StageTestCase):
    """Test $group stage error cases."""
    populate_collection(collection, test_case)
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": test_case.pipeline,
            "cursor": {},
        },
    )
    assertResult(
        result,
        error_code=test_case.error_code,
        msg=test_case.msg,
    )


# Property [Output Field Order]: output field order matches the specification
# order, not alphabetical order.
@pytest.mark.aggregate
def test_group_output_field_order(collection):
    """Test output field order matches specification order."""
    collection.insert_many([{"_id": 1, "v": "a", "x": 10}])
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [
                {
                    "$group": {
                        "_id": "$v",
                        "z_field": {"$sum": 1},
                        "a_field": {"$sum": "$x"},
                    }
                }
            ],
            "cursor": {},
        },
    )
    assertSuccess(
        result,
        [["_id", "z_field", "a_field"]],
        msg="Output field order should match specification order",
        transform=lambda docs: [list(d.keys()) for d in docs],
    )


# Property [Non-Deterministic Output Order]: $group does not guarantee any
# ordering of its output documents.
@pytest.mark.aggregate
def test_group_non_deterministic_output_order(collection):
    """Test $group output order is not guaranteed."""
    collection.insert_many([{"_id": i, "v": f"g{i}"} for i in range(10)])
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [{"$group": {"_id": "$v", "count": {"$sum": 1}}}],
            "cursor": {},
        },
    )
    assertSuccess(
        result,
        [True],
        msg="$group should produce all groups regardless of output order",
        transform=lambda docs: [
            len(docs) == 10
            and sorted(d["_id"] for d in docs) == [f"g{i}" for i in range(10)]
            and all(d["count"] == 1 for d in docs)
        ],
    )


# Property [_id Type Preservation]: the _id type is preserved for all BSON
# types in the output.
@pytest.mark.aggregate
def test_group_id_type_preserved(collection):
    """Test _id type is preserved for all BSON types in the output."""
    collection.insert_many(
        [
            {"_id": 1, "v": Int64(42)},
            {"_id": 2, "v": 3.14},
            {"_id": 3, "v": Decimal128("9.99")},
            {"_id": 4, "v": True},
            {"_id": 5, "v": ObjectId("507f1f77bcf86cd799439011")},
            {"_id": 6, "v": datetime(2024, 1, 1)},
            {"_id": 7, "v": Timestamp(1, 1)},
            {"_id": 8, "v": Binary(b"data")},
            {"_id": 9, "v": Regex("abc", "i")},
            {"_id": 10, "v": Code("function(){}")},
            {"_id": 11, "v": MinKey()},
            {"_id": 12, "v": MaxKey()},
            {"_id": 13, "v": "hello"},
            {"_id": 14, "v": 99},
            {"_id": 15, "v": None},
        ]
    )
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [{"$group": {"_id": "$v", "count": {"$sum": 1}}}],
            "cursor": {},
        },
    )
    expected_types = {
        Int64,
        float,
        Decimal128,
        bool,
        ObjectId,
        datetime,
        Timestamp,
        bytes,
        Regex,
        Code,
        MinKey,
        MaxKey,
        str,
        int,
        type(None),
    }
    assertSuccess(
        result,
        [True],
        msg="_id type should be preserved for all BSON types in the output",
        transform=lambda docs: [{type(d["_id"]) for d in docs} == expected_types],
    )


# Property [Blocking Stage]: $group is a blocking stage that waits for all
# input before producing output.
@pytest.mark.aggregate
def test_group_blocking_stage(collection):
    """Test $group is a blocking stage."""
    collection.insert_many([{"_id": i, "v": i % 3} for i in range(9)])
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [
                {"$group": {"_id": "$v", "total": {"$sum": 1}}},
                {
                    "$group": {
                        "_id": None,
                        "group_count": {"$sum": 1},
                        "doc_sum": {"$sum": "$total"},
                    }
                },
            ],
            "cursor": {},
        },
    )
    assertSuccess(
        result,
        [{"_id": None, "group_count": 3, "doc_sum": 9}],
        msg=(
            "$group should consume all input before producing output;"
            " a subsequent $group should see all groups"
        ),
    )


# Property [$sum NaN Contamination]: any NaN input makes the $sum result
# NaN, and inf + (-inf) produces NaN.
GROUP_SUM_NAN_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="sum_nan_contaminates",
        docs=[{"_id": 1, "v": FLOAT_NAN}, {"_id": 2, "v": 10}],
        pipeline=[{"$group": {"_id": None, "r": {"$sum": "$v"}}}],
        expected=[{"_id": None, "r": FLOAT_NAN}],
        msg="NaN input should contaminate the $sum result",
    ),
    StageTestCase(
        id="sum_inf_plus_neg_inf_produces_nan",
        docs=[
            {"_id": 1, "v": FLOAT_INFINITY},
            {"_id": 2, "v": FLOAT_NEGATIVE_INFINITY},
        ],
        pipeline=[{"$group": {"_id": None, "r": {"$sum": "$v"}}}],
        expected=[{"_id": None, "r": FLOAT_NAN}],
        msg="inf + (-inf) should produce NaN",
    ),
]


@pytest.mark.aggregate
@pytest.mark.parametrize("test_case", pytest_params(GROUP_SUM_NAN_TESTS))
def test_group_sum_nan(collection, test_case: StageTestCase):
    """Test $sum NaN contamination cases."""
    populate_collection(collection, test_case)
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": test_case.pipeline,
            "cursor": {},
        },
    )
    assertSuccessNaN(
        result,
        test_case.expected,
        msg=test_case.msg,
    )


@pytest.mark.aggregate
def test_group_nan_equivalence(collection):
    """Test NaN variants group together."""
    collection.insert_many(
        [
            {"_id": 1, "v": FLOAT_NAN},
            {"_id": 2, "v": Decimal128("NaN")},
        ]
    )
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
            "cursor": {},
        },
    )
    assertSuccessNaN(
        result,
        [{"_id": FLOAT_NAN, "ids": [1, 2]}],
        msg="float NaN and Decimal128 NaN should group together",
    )


# Property [$addToSet NaN Deduplication]: $addToSet deduplicates NaN
# variants (float NaN and Decimal128 NaN) to one entry.
GROUP_ADDTOSET_NAN_DEDUP_TESTS: list[StageTestCase] = [
    StageTestCase(
        id="addtoset_nan_dedup",
        docs=[
            {"_id": 1, "v": FLOAT_NAN},
            {"_id": 2, "v": Decimal128("NaN")},
            {"_id": 3, "v": FLOAT_NAN},
        ],
        pipeline=[
            {"$group": {"_id": None, "r": {"$addToSet": "$v"}}},
        ],
        expected=[{"_id": None, "r": [FLOAT_NAN]}],
        msg="NaN variants (float NaN and Decimal128 NaN) should deduplicate to one entry",
    ),
]


@pytest.mark.aggregate
@pytest.mark.parametrize("test_case", pytest_params(GROUP_ADDTOSET_NAN_DEDUP_TESTS))
def test_group_addtoset_nan(collection, test_case: StageTestCase):
    """Test $addToSet NaN deduplication."""
    populate_collection(collection, test_case)
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": test_case.pipeline,
            "cursor": {},
        },
    )
    assertSuccessNaN(
        result,
        test_case.expected,
        msg=test_case.msg,
    )


@pytest.mark.aggregate
def test_group_collation_affects_grouping(collection):
    """Test collation affects grouping equivalence."""
    collection.insert_many(
        [
            {"_id": 1, "v": "hello"},
            {"_id": 2, "v": "Hello"},
        ]
    )
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [{"$group": {"_id": "$v", "ids": {"$push": "$_id"}}}],
            "cursor": {},
            "collation": {"locale": "en", "strength": 2},
        },
    )
    assertSuccess(
        result,
        [{"_id": "hello", "ids": [1, 2]}],
        msg="Case-insensitive collation should merge differently-cased strings into one group",
    )


# Property [Large Input]: 1000 distinct group key values produce 1000 output
# groups without issues.
@pytest.mark.aggregate
def test_group_large_input(collection):
    """Test 1000 distinct group keys produce 1000 output groups."""
    collection.insert_many([{"_id": i, "v": i} for i in range(1_000)])
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [{"$group": {"_id": "$v", "count": {"$sum": 1}}}],
            "cursor": {"batchSize": 10_000},
        },
    )
    assertSuccess(
        result,
        [True],
        msg="1000 distinct group keys should produce 1000 output groups",
        transform=lambda docs: [len(docs) == 1_000 and all(d["count"] == 1 for d in docs)],
    )


# Property [$$NOW in _id]: $$NOW returns the same datetime for the entire
# pipeline, so all documents group together under a single datetime key.
@pytest.mark.aggregate
def test_group_now_in_id(collection):
    """Test $$NOW in _id groups all documents together."""
    collection.insert_many([{"_id": 1, "x": 10}, {"_id": 2, "x": 20}])
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [{"$group": {"_id": "$$NOW", "count": {"$sum": 1}}}],
            "cursor": {},
        },
    )
    assertSuccess(
        result,
        [True],
        msg="$$NOW in _id should group all documents together under a single datetime",
        transform=lambda docs: [
            len(docs) == 1 and isinstance(docs[0]["_id"], datetime) and docs[0]["count"] == 2
        ],
    )


# Property [Memory Limit with allowDiskUse]: when $group exceeds 100 MB of
# RAM, it succeeds with allowDiskUse true (spilling to disk) and fails with
# allowDiskUse false (memory-exceeded error).
@pytest.mark.aggregate
def test_group_memory_limit_allow_disk_use(collection):
    """Test $group spills to disk when allowDiskUse is true."""
    big_str = "x" * 1_000
    for i in range(0, 120_000, 10_000):
        collection.insert_many(
            [{"_id": j, "v": big_str + str(j)} for j in range(i, min(i + 10_000, 120_000))]
        )
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [{"$group": {"_id": "$v", "count": {"$sum": 1}}}],
            "cursor": {"batchSize": 200_000},
            "allowDiskUse": True,
        },
    )
    assertSuccess(
        result,
        [True],
        msg="$group exceeding 100 MB should succeed with allowDiskUse true",
        transform=lambda docs: [len(docs) > 0],
    )


@pytest.mark.aggregate
def test_group_memory_limit_no_disk_use(collection):
    """Test $group errors when exceeding memory limit without allowDiskUse."""
    big_str = "x" * 1_000
    for i in range(0, 120_000, 10_000):
        collection.insert_many(
            [{"_id": j, "v": big_str + str(j)} for j in range(i, min(i + 10_000, 120_000))]
        )
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [{"$group": {"_id": "$v", "count": {"$sum": 1}}}],
            "cursor": {},
            "allowDiskUse": False,
        },
    )
    assertResult(
        result,
        error_code=QUERY_EXCEEDED_MEMORY_NO_DISK_USE_ERROR,
        msg="$group exceeding 100 MB should error with allowDiskUse false",
    )


# Property [Output Document Size Limit]: the output document size limit is the
# wire protocol message size of 33,619,968 bytes; exceeding it produces an
# oversized-document error, but a subsequent stage that trims the document
# below the limit allows the pipeline to succeed.
@pytest.mark.aggregate
def test_group_output_between_16mb_and_33mb_succeeds(collection):
    """Test $group succeeds when output is between 16 MB and 33 MB."""
    big_str = "x" * 10_000
    collection.insert_many([{"_id": i, "v": big_str} for i in range(2_000)])
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [{"$group": {"_id": None, "all": {"$push": "$v"}}}],
            "cursor": {},
            "allowDiskUse": True,
        },
    )
    assertSuccess(
        result,
        [True],
        msg=(
            "Output document between 16 MB and 33 MB should succeed,"
            " confirming the limit is the wire protocol size, not the 16 MB"
            " BSON limit"
        ),
        transform=lambda docs: [len(docs) == 1 and len(docs[0]["all"]) == 2_000],
    )


@pytest.mark.aggregate
def test_group_output_document_size_limit(collection):
    """Test $group errors when output document exceeds wire protocol size."""
    big_str = "x" * 10_000
    collection.insert_many([{"_id": i, "v": big_str} for i in range(5_000)])
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [{"$group": {"_id": None, "all": {"$push": "$v"}}}],
            "cursor": {},
            "allowDiskUse": True,
        },
    )
    assertResult(
        result,
        error_code=BSON_OBJECT_TOO_LARGE_ERROR,
        msg="Output document exceeding wire protocol size should produce an error",
    )


@pytest.mark.aggregate
def test_group_oversized_intermediate_trimmed(collection):
    """Test subsequent stage trimming oversized intermediate document succeeds."""
    big_str = "x" * 10_000
    collection.insert_many([{"_id": i, "v": big_str} for i in range(5_000)])
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [
                {"$group": {"_id": None, "all": {"$push": "$v"}, "count": {"$sum": 1}}},
                {"$project": {"count": 1}},
            ],
            "cursor": {},
            "allowDiskUse": True,
        },
    )
    assertSuccess(
        result,
        [{"_id": None, "count": 5_000}],
        msg=(
            "Pipeline should succeed when a subsequent stage trims an oversized"
            " intermediate document below the limit"
        ),
    )
