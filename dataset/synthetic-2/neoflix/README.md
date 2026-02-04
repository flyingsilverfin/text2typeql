# NeoFlix Dataset

**Source:** `synthetic_gpt4o_demodbs` (Neo4j text2cypher)

**Total valid queries: 923**

Movies, ratings, genres, subscriptions.

## Current Status
- `queries.csv`: 913 converted queries
- 10 failed queries

Total: 913 + 10 = 923 / 923 ✓

## Failed Queries

### Query 32
**Error:** Cypher references non-existent properties: m.awards and m.awards_count do not exist in the neoflix schema. No equivalent attributes available.

### Query 40
**Error:** Unsupported: Cypher uses split() and size() for word counting, which have no TypeQL equivalent.

### Query 156
**Error:** Unsupported: Cypher uses split() and size() string functions which have no TypeQL equivalent

### Query 171
**Error:** Unsupported: TypeQL has no date component extraction (year) or modulo arithmetic on datetime values, which are required for leap year calculation.

### Query 573
**Error:** Unsupported: date arithmetic (latest_date - earliest_date) and collect/UNWIND have no TypeQL equivalent

### Query 581
**Error:** Requires date component extraction (release_date.year) and modulo arithmetic (% operator), both unsupported in TypeQL 3.0

### Query 621
**Error:** Requires date arithmetic (date().year - 5) to compute a dynamic date threshold. TypeQL has no now() or dynamic date functions.

### Query 752
**Error:** Unsupported features: size() and split() string functions have no TypeQL equivalent

### Query 758
**Error:** Unsupported: size() for string length has no TypeQL equivalent

### Query 829
**Error:** Unsupported: apoc.text.levenshteinDistance() string distance function has no TypeQL equivalent

