# Network Dataset

**Source:** `synthetic_gpt4o_demodbs` (Neo4j text2cypher)

**Total valid queries: 625**

Data centers, machines, software, network topology.

## Current Status
- `queries.csv`: 620 converted queries
- 5 failed queries

Total: 620 + 5 = 625 / 625 ✓

## Schema Notes
- `Zone` (Neo4j label) renamed to `network_zone` (reserved keyword in TypeQL)
- `Type` (Neo4j label) renamed to `machine_type_spec` (reserved keyword in TypeQL)

## Failed Queries

### Query 347
**Error:** Cypher uses COLLECT(DISTINCT ...) which maps to collect() - unsupported in TypeQL 3.0

### Query 382
**Error:** Cypher uses collect() which is unsupported in TypeQL 3.0

### Query 525
**Error:** Unsupported: requires COLLECT() and array slicing [0..3] for per-zone top-3 ranking, which TypeQL does not support

### Query 527
**Error:** collect() is unsupported in TypeQL. Additionally, version entity cannot play depends_on:dependent role per schema, so the OPTIONAL MATCH dependency pattern is not expressible.

### Query 623
**Error:** Schema mismatch: interface entity does not have name attribute in TypeQL schema. Cypher references interface.name which does not exist.

## Resolved Queries

### Queries 1, 201, 228, 259, 603 (resolved)
Previously failed due to variable-length path traversal (`[:PREVIOUS*]`, `[:PREVIOUS*1..N]`). Converted using recursive stream functions that traverse the `previous` relation transitively. TypeDB tables recursive functions to avoid cycles and returns results breadth-first.

### Query 397 (resolved)
Previously failed due to `[:DEPENDS_ON*]` transitive closure. Converted using recursive stream function with `$dep is $sw` identity base case to include direct dependencies in results.

### Query 560 (resolved)
Previously failed due to `[:PREVIOUS*1..3]` + `collect()`. The VLP was handled by recursive stream function; `collect()` was replaced with flat rows (version + optional software name).
