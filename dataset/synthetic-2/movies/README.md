# Movies Dataset

**Source:** `synthetic_gpt4o_demodbs` (Neo4j text2cypher)

**Total valid queries: 738**

Movies, people, genres, reviews.

## Current Status
- `queries.csv`: 728 converted queries
- 10 failed queries

Total: 728 + 10 = 738 / 738 ✓

## Failed Queries

### Query 11
**Error:** Unsupported feature: size() on array property. Cypher uses size(r.roles) to count elements in a list-valued property, which has no TypeQL equivalent.

### Query 80
**Error:** Cypher uses split() which is unsupported in TypeQL. No equivalent string splitting function available.

### Query 234
**Error:** Cypher uses collect() which has no TypeQL equivalent. TypeQL does not support collect/aggregation into lists.

### Query 274
**Error:** Cypher uses split() which is unsupported in TypeQL

### Query 339
**Error:** Unsupported: requires collect(), REDUCE (array flatten), SIZE, and apoc.coll.toSet for counting unique roles across multiple relations. Also requires two separate aggregations (movie count and unique role count) per person, which cannot be combined in a single TypeQL reduce.

### Query 452
**Error:** Unsupported feature: collect() is required to aggregate actor names per movie but is not available in TypeQL 3.0

### Query 580
**Error:** Unsupported: Cypher substring() for dynamic first-character comparison between two variables has no TypeQL equivalent. TypeQL lacks substring/left string extraction functions.

### Query 604
**Error:** Schema mismatch: follows relation connects person-to-person only; movie does not play the followed role. Cypher assumes Person-[:FOLLOWS]->Movie which is not supported.

### Query 611
**Error:** Schema has no created_at attribute on any relation type. The Cypher query references r.created_at which does not exist in the schema, making this query unconvertible.

### Query 703
**Error:** Unsupported feature: size() on list/array. TypeQL has no equivalent for checking the length of a multi-value attribute collection.

