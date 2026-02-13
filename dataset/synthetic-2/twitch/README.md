# Twitch Dataset

**Source:** `synthetic_gpt4o_demodbs` (Neo4j text2cypher)

**Total valid queries: 576**

Streams, users, games, teams, follows.

## Current Status
- `queries.csv`: 571 converted queries
- 5 failed queries

Total: 571 + 5 = 576 / 576 ✓

## Failed Queries

### Query 92
**Error:** Unsupported: Cypher `g.name[0]` (string indexing) for comparing first letters of language and game names has no TypeQL equivalent.

### Query 131
**Error:** Cypher `r.since` references non-existent property on VIP relationship (Neo4j schema has no `rel_props`). Cannot compute VIP duration.

### Query 141
**Error:** Unsupported: Cypher `substring()` for comparing first letters of game and team names has no TypeQL equivalent.

### Query 360
**Error:** Variable-length path `[:MODERATOR*]` (transitive closure) not supported in TypeQL.

### Query 419
**Error:** Unsupported: Cypher `split()` and `size()` for word counting has no TypeQL equivalent.

## Conversion Notes

### Query 120: hardcoded date cutoff
"Most chatters in the past month" — Cypher uses `datetime() - duration('P1M')`. Converted with hardcoded cutoff `>= 2021-04-01T00:00:00` (dataset max stream date is 2021-05-09).
