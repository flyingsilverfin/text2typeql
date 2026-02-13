# GRANDstack Dataset

**Source:** `synthetic_gpt4o_demodbs` (Neo4j text2cypher)

**Total valid queries: 807**

Businesses, reviews, users, categories.

## Current Status
- `queries.csv`: 805 converted queries
- 2 failed queries

Total: 805 + 2 = 807 / 807 ✓

## Failed Queries

### Query 356
**Error:** Neo4j Point type property access (location.latitude) not supported. Schema stores location as string; no numeric latitude attribute available for comparison.

### Query 460
**Error:** Cypher uses Neo4j spatial POINT type with .latitude sub-property access. TypeQL schema stores location as string; no way to extract latitude from a string attribute (no split, substring, or point type support).

## Resolved Queries

### Queries 26, 36, 179, 219, 229 (resolved)
Previously failed due to `collect()` + computation (`size()`, `WHERE =`, array slicing). Converted using custom functions: `category_count()` for distinct count, `avg_stars()` for mean aggregation, `reviewed_by()` for per-user existence checks, `latest_review_date()` for chained reduce, and `distinct_date_count()` for date filtering.

### Queries 202, 513, 766 (resolved)
Previously failed due to `collect()` for display grouping. Converted as flat rows — each (entity, detail) pair is a separate result row instead of a grouped list.

### Queries 174, 238, 693, 745 (resolved)
Previously failed due to `size()` string length. Converted using `len()` (TypeDB 3.8+).
