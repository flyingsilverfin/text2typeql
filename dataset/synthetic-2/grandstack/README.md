# GRANDstack Dataset

**Source:** `synthetic_gpt4o_demodbs` (Neo4j text2cypher)

**Total valid queries: 807**

Businesses, reviews, users, categories.

## Current Status
- `queries.csv`: 793 converted queries
- 14 failed queries

Total: 793 + 14 = 807 / 807 ✓

## Failed Queries

### Query 26
**Error:** Unsupported: collect() and size() have no TypeQL equivalent. Query requires collecting distinct category names into a list and computing its size.

### Query 36
**Error:** Cypher uses collect(r) to aggregate review objects into a list. TypeQL has no collect() equivalent. Additionally, returning both an aggregation (avg stars) and individual review details requires re-matching after reduce, which is not supported.

### Query 174
**Error:** Unsupported feature: size() string length function has no TypeQL equivalent. Cannot sort by string length.

### Query 179
**Error:** Cypher uses collect() to gather user IDs into a list and compare against a fixed array. collect() is not supported in TypeQL 3.0.

### Query 202
**Error:** Cypher uses collect() to aggregate categories into a list per business. collect() is unsupported in TypeQL.

### Query 219
**Error:** Unsupported features: COLLECT(), SIZE(), and array slicing [0..3] have no TypeQL equivalents

### Query 229
**Error:** Unsupported features: collect(), array slicing [0..3], and size() are not available in TypeQL 3.0. The query requires collecting dates into a list, slicing the first 3, and checking the list size.

### Query 238
**Error:** Unsupported: size() string length function has no TypeQL equivalent

### Query 356
**Error:** Neo4j Point type property access (location.latitude) not supported. Schema stores location as string; no numeric latitude attribute available for comparison.

### Query 460
**Error:** Cypher uses Neo4j spatial POINT type with .latitude sub-property access. TypeQL schema stores location as string; no way to extract latitude from a string attribute (no split, substring, or point type support).

### Query 513
**Error:** Unsupported feature: collect() aggregation is not available in TypeQL

### Query 693
**Error:** Cypher uses size() for string length which is unsupported in TypeQL

### Query 745
**Error:** Unsupported feature: size() string length function has no TypeQL equivalent

### Query 766
**Error:** Cypher uses collect() with object literal to aggregate review properties into a list of maps, which is unsupported in TypeQL

