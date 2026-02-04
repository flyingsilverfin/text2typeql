# Network Dataset

**Source:** `synthetic_gpt4o_demodbs` (Neo4j text2cypher)

**Total valid queries: 625**

Data centers, machines, software, network topology.

## Current Status
- `queries.csv`: 613 converted queries
- 12 failed queries

Total: 613 + 12 = 625 / 625 ✓

## Schema Notes
- `Zone` (Neo4j label) renamed to `network_zone` (reserved keyword in TypeQL)
- `Type` (Neo4j label) renamed to `machine_type_spec` (reserved keyword in TypeQL)

## Failed Queries

### Query 1
**Error:** Cypher uses transitive closure ([:PREVIOUS*]) which is not supported in TypeQL 3.0. No recursive or transitive relation traversal available.

### Query 201
**Error:** Cypher uses variable-length path traversal ([:PREVIOUS*]) for transitive closure, which is not supported in TypeQL 3.0

### Query 228
**Error:** Variable-length path traversal (PREVIOUS*1..5) not supported in TypeQL - no recursive or transitive path matching available, and try-block scoping prevents chaining optional hops

### Query 259
**Error:** Variable-length path traversal (PREVIOUS*1..5) is not supported in TypeQL. TypeQL has no recursive or transitive path matching.

### Query 347
**Error:** Cypher uses COLLECT(DISTINCT ...) which maps to collect() - unsupported in TypeQL 3.0

### Query 382
**Error:** Cypher uses collect() which is unsupported in TypeQL 3.0

### Query 397
**Error:** Cypher uses variable-length path traversal ([:DEPENDS_ON*]) for transitive closure to find indirect dependencies. TypeQL 3.0 does not support recursive or transitive relation traversal.

### Query 525
**Error:** Unsupported: requires COLLECT() and array slicing [0..3] for per-zone top-3 ranking, which TypeQL does not support

### Query 527
**Error:** collect() is unsupported in TypeQL. Additionally, version entity cannot play depends_on:dependent role per schema, so the OPTIONAL MATCH dependency pattern is not expressible.

### Query 560
**Error:** Unsupported: collect() aggregation and variable-length path traversal (*1..3) have no TypeQL equivalent

### Query 603
**Error:** Unsupported: Variable-length path traversal ([:PREVIOUS*]) - TypeQL does not support transitive closure

### Query 623
**Error:** Schema mismatch: interface entity does not have name attribute in TypeQL schema. Cypher references interface.name which does not exist.

