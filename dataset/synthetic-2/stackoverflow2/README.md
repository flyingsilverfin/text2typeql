# Stack Overflow 2 Dataset

**Source:** `synthetic_gpt4o_demodbs` (Neo4j text2cypher)

**Total valid queries: 307**

Questions, answers, comments, tags, users.

## Current Status
- `queries.csv`: 298 converted queries
- 9 failed queries

Total: 298 + 9 = 307 / 307 ✓

## Failed Queries

### Query 91
**Error:** Schema mismatch: Cypher queries COMMENTED_ON relation on Answer entities, but TypeQL schema only defines commented_on relating comment to question, not to answer. Answer entity does not play any role in commented_on relation.

### Query 94
**Error:** Unsupported: date extraction from epoch timestamps (date(datetime({epochSeconds: ...}))) not available in TypeQL

### Query 98
**Error:** Unsupported: size() function for string length not available in TypeQL

### Query 169
**Error:** Unsupported: date arithmetic (timestamp() - 31536000 for one year ago calculation)

### Query 253
**Error:** collect() function not supported in TypeQL - cannot aggregate tags into a list

### Query 259
**Error:** COLLECT() function is unsupported in TypeQL - cannot aggregate values into an array

### Query 260
**Error:** collect() function is not supported in TypeQL - cannot aggregate tags into an array per question

### Query 261
**Error:** Requires nested subqueries with two-stage aggregation (first limit to top 3 questions by view_count, then sum comment counts per user). TypeQL 3.0 does not support subqueries or CTEs.

### Query 288
**Error:** Cypher uses collect() to aggregate tags into a list, which is not supported in TypeQL. TypeQL has no equivalent to collect/aggregate into arrays.

