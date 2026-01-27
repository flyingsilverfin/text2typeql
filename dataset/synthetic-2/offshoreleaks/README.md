# Offshore Leaks Dataset

**Source:** `synthetic_gpt4o_demodbs` (Neo4j text2cypher)

**Total valid queries: 507**

Offshore entities, intermediaries, officers, addresses.

## Current Status
- `queries.csv`: 0 converted queries
- 0 failed queries

Total: 0 + 0 = 0 / 507 pending

## Schema Notes
- `entity` (Neo4j label) renamed to `offshore_entity` (reserved keyword in TypeQL)
- `address` (Neo4j label) renamed to `postal_address` (name collision with `address` attribute)
- `registered_address` role renamed to `registered_addr` (role/relation name collision)
## Failed Queries

_None yet._
