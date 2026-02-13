# Offshore Leaks Dataset

**Source:** `synthetic_gpt4o_demodbs` (Neo4j text2cypher)

**Total valid queries: 507**

Entities, officers, intermediaries, addresses.

## Current Status
- `queries.csv`: 498 converted queries
- 9 failed queries

Total: 498 + 9 = 507 / 507 ✓

## Failed Queries

### Query 41
**Error:** Schema mismatch: Cypher uses similar relation between Entity and Other, but in TypeQL schema the other entity does not play similar relation roles (only intermediary and officer can participate in similar relation)

### Query 61
**Error:** Schema mismatch: The other entity type does not play any role in the similar relation in the TypeQL schema. Only intermediary and officer can participate in similar relationships.

### Query 111
**Error:** Unsupported: date field filtering by month - TypeQL date type does not support string pattern matching or month extraction functions

### Query 146
**Error:** Unsupported: TypeQL 3.0 does not support SPLIT() string function, array indexing [N], or date component extraction functions like year(). Cannot extract and compare year from date values.

### Query 168
**Error:** Schema mismatch: offshore_entity does not play any role in the similar relation, and other entity does not have address attribute. The similar relation only connects intermediary and officer entities.

### Query 372
**Error:** Requires substring() to extract year from date - TypeQL has no date component extraction or string substring functions

### Query 399
**Error:** TypeQL does not support substring() function or date part extraction - cannot compare years from date values

### Query 437
**Error:** TypeQL does not support dynamic CONTAINS between two variables. The like operator requires a literal pattern string, not a variable reference.

### Query 472
**Error:** Schema mismatch: Cypher references Entity-[:similar]->Other relationship but in TypeQL schema, offshore_entity and other do not play similar roles. Only intermediary and officer can participate in similar relations.

## Resolved Queries

### Query 103 (resolved)
Previously failed due to schema mismatch (officer vs intermediary types). Reinterpreted from English: intermediaries connected to >1 offshore entity, using `entity_count()` custom function.

### Query 370 (resolved)
Previously failed due to `collect()` + `size()`. Converted using `address_count()` function counting distinct postal_address entities per offshore_entity via `registered_address` relation.

### Query 476 (resolved)
Previously failed due to `split()` + `size()` on semicolon-delimited string. Reinterpreted using TypeQL multi-cardinality: count distinct `former_name` attributes > 1.

### Query 324 (resolved)
Previously failed due to officer entity missing `status` attribute. Added `owns status` to officer entity in schema, then converted using simple attribute filter.

### Query 491 (resolved)
Previously failed due to `STARTS WITH 'FEB-2013'` string pattern matching on dates. Converted using date range comparison: `$d >= 2013-02-01; $d < 2013-03-01;`.

