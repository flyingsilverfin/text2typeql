# NeoFlix Dataset

**Source:** `synthetic_gpt4o_demodbs` (Neo4j text2cypher)

**Total valid queries: 923**

Movies, ratings, genres, subscriptions.

## Current Status
- `queries.csv`: 915 converted queries
- 8 failed queries

Total: 915 + 8 = 923 / 923 ✓

## Failed Queries

### Query 40
**Error:** Unsupported: Cypher uses `split()` and `size()` for word counting, which have no TypeQL equivalent.

### Query 156
**Error:** Unsupported: Cypher uses `split()` and `size()` for word counting, which have no TypeQL equivalent.

### Query 171
**Error:** Unsupported: TypeQL has no date component extraction (year) or modulo arithmetic on datetime values, required for leap year calculation.

### Query 573
**Error:** Unsupported: date arithmetic (`latest_date - earliest_date`) and `collect`/`UNWIND` have no TypeQL equivalent.

### Query 581
**Error:** Requires date component extraction (`release_date.year`) and modulo arithmetic (`%` operator), both unsupported in TypeQL.

### Query 621
**Error:** Requires current date (`date().year - 5`) — TypeQL has no `now()` function.

### Query 752
**Error:** Unsupported: `size()` and `split()` string functions have no TypeQL equivalent.

### Query 829
**Error:** Unsupported: `apoc.text.levenshteinDistance()` string distance function has no TypeQL equivalent.
