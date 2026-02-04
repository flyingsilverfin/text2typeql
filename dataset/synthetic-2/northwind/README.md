# Northwind Dataset

**Source:** `synthetic_gpt4o_demodbs` (Neo4j text2cypher)

**Total valid queries: 807**

Products, orders, suppliers, customers, categories.

## Current Status
- `queries.csv`: 780 converted queries
- 27 failed queries

Total: 780 + 27 = 807 / 807 ✓

## Failed Queries

### Query 66
**Error:** TypeQL does not support string-to-numeric conversion. The freight attribute is string type but the query requires numeric comparison (toFloat). Lexicographic string comparison would give incorrect results.

### Query 78
**Error:** Schema has freight as string type; TypeQL has no toFloat() conversion function to enable mean() aggregation on string values

### Query 98
**Error:** Cannot convert string to numeric for comparison. Schema stores freight as string type, Cypher uses toFloat() for numeric comparison. TypeQL has no string-to-number conversion function.

### Query 145
**Error:** Cannot convert - freight attribute is type string in schema but query requires numeric comparison (toFloat conversion). TypeQL cannot cast string to number.

### Query 148
**Error:** Unsupported: freight attribute is stored as string type, TypeQL cannot convert string to number for SUM aggregation (toFloat equivalent not supported)

### Query 161
**Error:** Schema has freight as string type but query requires numeric comparison (toFloat). TypeQL has no string-to-number conversion function.

### Query 167
**Error:** TypeQL cannot convert string to numeric at runtime - freight is stored as string but Cypher uses toFloat() for numeric comparison

### Query 176
**Error:** freight attribute is string type; TypeQL cannot cast strings to numbers for numeric comparison (Cypher uses toFloat())

### Query 261
**Error:** TypeQL cannot sum string attributes - freight is defined as string type and TypeQL has no toFloat() type casting function

### Query 262
**Error:** Unsupported: collect() and UNWIND for array operations - TypeQL has no array aggregation

### Query 269
**Error:** Unsupported: toFloat() conversion - freight is stored as string in schema, TypeQL cannot convert string to number for numeric sorting

### Query 308
**Error:** TypeQL does not support string-to-number conversion (toFloat). Schema has freight as string type.

### Query 322
**Error:** TypeQL does not support string-to-number casting (toFloat). Schema has freight as string type.

### Query 375
**Error:** Schema has freight as string but query requires numeric comparison. Cypher uses toFloat() cast which has no TypeQL equivalent. String comparison would give incorrect alphabetical ordering.

### Query 399
**Error:** Unsupported: COLLECT(), REDUCE with RANGE, SIZE(), and array indexing [i] for computing cumulative price variation

### Query 402
**Error:** TypeQL cannot cast string attributes to numbers. Schema defines freight as string type but query requires numeric comparison via toFloat().

### Query 470
**Error:** Unsupported: toFloat() type conversion. Schema stores freight as string but query requires numeric comparison. TypeQL cannot convert string to number at runtime.

### Query 515
**Error:** TypeQL does not support string-to-numeric type conversion (toFloat). Schema defines freight as string but sum() requires numeric type.

### Query 520
**Error:** TypeQL does not support runtime type conversion. Cypher uses toFloat() to convert string freight to number for comparison, but TypeQL has no equivalent - string comparison would be lexicographic, not numeric.

### Query 522
**Error:** TypeQL cannot convert string to numeric for aggregation. Schema defines freight as string type but query requires AVG(toFloat(freight)). No type casting available in TypeQL.

### Query 634
**Error:** Unsupported: freight is stored as string but query requires numeric comparison (toFloat). TypeQL has no string-to-number conversion function.

### Query 637
**Error:** Unsupported: toFloat() string-to-number conversion. TypeQL cannot convert string freight attribute to numeric for comparison.

### Query 672
**Error:** Unsupported: toFloat() type conversion - freight attribute is string type in schema, TypeQL has no string-to-number conversion function for numeric comparison

### Query 689
**Error:** Schema mismatch: No employee entity or processed relation in schema. Order has employee_id attribute but no employee entity to return contactName/contactTitle from.

### Query 718
**Error:** TypeQL cannot cast string to numeric for comparison. Schema defines freight as string, but query requires toFloat() conversion for numeric comparison > 50.

### Query 767
**Error:** Unsupported: freight attribute is string type and TypeQL has no toFloat() or type casting function to convert string to numeric for sum() aggregation

### Query 789
**Error:** TypeQL does not support string-to-numeric conversion. The freight attribute is string type but Cypher uses toFloat() for numeric sorting.

