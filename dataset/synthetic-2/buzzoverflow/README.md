# BuzzOverflow Dataset

**Source:** `synthetic_gpt4o_demodbs` (Neo4j text2cypher)

**Total valid queries: 592**

Q&A posts, users, tags, answers, comments.

## Current Status
- `queries.csv`: 578 converted queries
- 14 failed queries

Total: 578 + 14 = 592 / 592 ✓

## Failed Queries

### Query 34
**Error:** Unsupported: date component extraction (month/year from datetime) required to filter questions answered within the same month

### Query 36
**Error:** Unsupported: size() string length function needed for ORDER BY size(q.text)

### Query 60
**Error:** Cypher uses split() and size() for word counting, which are unsupported in TypeQL

### Query 110
**Error:** Unsupported: requires size() on regex group extraction (apoc.text.regexGroups) to count URL occurrences within a string. TypeQL has no string function to count pattern occurrences within an attribute value.

### Query 135
**Error:** Unsupported: size() string length function has no TypeQL equivalent. Cannot sort by title length.

### Query 138
**Error:** Unsupported: size() string length function has no TypeQL equivalent

### Query 139
**Error:** Unsupported: TypeQL has no function to extract hour/time components from datetime values (date arithmetic)

### Query 177
**Error:** Cypher uses size() for string length which is unsupported in TypeQL

### Query 345
**Error:** Unsupported feature: size() string length function has no TypeQL equivalent

### Query 451
**Error:** Unsupported: size() string length function has no TypeQL equivalent

### Query 454
**Error:** Unsupported: size() on apoc.text.regexGroups() - counting regex pattern occurrences within a string has no TypeQL equivalent

### Query 526
**Error:** Unsupported features: size() and split() are not available in TypeQL. Cannot compute word count from text content.

### Query 574
**Error:** TypeQL does not support date component extraction functions (month, day from datetime). Cannot filter by specific month/day values.

### Query 579
**Error:** Uses size() with apoc.text.regexGroups() to count URL occurrences - TypeQL has no regex counting or size() function

