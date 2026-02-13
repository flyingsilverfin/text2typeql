# Twitch Dataset

**Source:** `synthetic_gpt4o_demodbs` (Neo4j text2cypher)

**Total valid queries: 576**

Streams, users, games, teams, follows.

## Current Status
- `queries.csv`: 570 converted queries
- 6 failed queries

Total: 570 + 6 = 576 / 576 ✓

## Failed Queries

### Query 92
**Error:** Unsupported: Cypher `g.name[0]` (string indexing) for comparing first letters of language and game names has no TypeQL equivalent.

### Query 120
**Error:** Requires current date (`datetime() - duration('P1M')`) for "past month" — TypeQL has no `now()` function.

### Query 131
**Error:** Requires date arithmetic (`duration.inSeconds(r.since, datetime()).years > 3`) — TypeQL has no `now()` function.

### Query 141
**Error:** Unsupported: Cypher `substring()` for comparing first letters of game and team names has no TypeQL equivalent.

### Query 360
**Error:** Variable-length path `[:MODERATOR*]` (transitive closure) not supported in TypeQL.

### Query 419
**Error:** Unsupported: Cypher `split()` and `size()` for word counting has no TypeQL equivalent.
