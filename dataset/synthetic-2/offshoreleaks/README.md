# Offshore Leaks Dataset

**Source:** `synthetic_gpt4o_demodbs` (Neo4j text2cypher)

**Total valid queries: 507**

Entities, officers, intermediaries, addresses.

## Current Status
- `queries.csv`: 493 converted queries
- 14 failed queries

Total: 493 + 14 = 507 / 507 ✓

## Failed Queries

### Query 41
**Error:** Schema mismatch: Cypher uses similar relation between Entity and Other, but in TypeQL schema the other entity does not play similar relation roles (only intermediary and officer can participate in similar relation)

### Query 61
**Error:** Schema mismatch: The other entity type does not play any role in the similar relation in the TypeQL schema. Only intermediary and officer can participate in similar relationships.

### Query 103
**Error:** Schema mismatch: Cypher query requires Officer nodes to participate in intermediary_of relations, but in TypeQL schema officer and intermediary are separate entity types - only intermediary can play intermediary_of:intermediary role

### Query 111
**Error:** Unsupported: date field filtering by month - TypeQL date type does not support string pattern matching or month extraction functions

### Query 146
**Error:** Unsupported: TypeQL 3.0 does not support SPLIT() string function, array indexing [N], or date component extraction functions like year(). Cannot extract and compare year from date values.

### Query 168
**Error:** Schema mismatch: offshore_entity does not play any role in the similar relation, and other entity does not have address attribute. The similar relation only connects intermediary and officer entities.

### Query 324
**Error:** Schema mismatch: officer entity does not have status attribute in TypeQL schema

### Query 370
**Error:** Unsupported: collect() and size() functions required to gather and count addresses per entity

### Query 372
**Error:** Requires substring() to extract year from date - TypeQL has no date component extraction or string substring functions

### Query 399
**Error:** TypeQL does not support substring() function or date part extraction - cannot compare years from date values

### Query 437
**Error:** TypeQL does not support dynamic CONTAINS between two variables. The like operator requires a literal pattern string, not a variable reference.

### Query 472
**Error:** Schema mismatch: Cypher references Entity-[:similar]->Other relationship but in TypeQL schema, offshore_entity and other do not play similar roles. Only intermediary and officer can participate in similar relations.

### Query 476
**Error:** Unsupported: split() and size() string functions not available in TypeQL

### Query 491
**Error:** TypeQL date type does not support string pattern matching (STARTS WITH). Cannot extract month/year from date values.

