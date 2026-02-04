# Game of Thrones Dataset

**Source:** `synthetic_gpt4o_demodbs` (Neo4j text2cypher)

**Total valid queries: 393**

Characters, houses, battles, allegiances.

## Current Status
- `queries.csv`: 384 converted queries
- 9 failed queries

Total: 384 + 9 = 393 / 393 ✓

## Failed Queries

### Query 119
**Error:** Unsupported: array element access (array[N]) not available in TypeQL 3.0

### Query 121
**Error:** Unsupported feature: array index access (c.fastrf_embedding[9]) not available in TypeQL

### Query 202
**Error:** Unsupported feature: array indexing (array[N]) is not available in TypeQL

### Query 227
**Error:** Unsupported: array indexing (c.fastrf_embedding[0]) is not available in TypeQL

### Query 250
**Error:** Unsupported: array element access (array[N]) - Cypher uses c.fastrf_embedding[0] which has no TypeQL equivalent

### Query 314
**Error:** Unsupported: array element access (array[N]) not available in TypeQL

### Query 326
**Error:** TypeQL has no percentileCont or percentile aggregation function. Cannot compute dynamic statistical thresholds.

### Query 374
**Error:** Unsupported: array element access (array[N]). TypeQL multi-cardinality attributes have no positional indexing.

### Query 389
**Error:** Unsupported: array element access (array[N]) not available in TypeQL

