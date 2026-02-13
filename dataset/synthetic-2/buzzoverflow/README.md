# BuzzOverflow Dataset

**Source:** `synthetic_gpt4o_demodbs` (Neo4j text2cypher)

**Total valid queries: 592**

Q&A posts, users, tags, answers, comments.

## Current Status
- `queries.csv`: 585 converted queries
- 7 failed queries

Total: 585 + 7 = 592 / 592 ✓

## Failed Queries

### Query 60
**Error:** Cypher uses split() and size() for word counting, which are unsupported in TypeQL

### Query 110
**Error:** Unsupported: requires size() on regex group extraction (apoc.text.regexGroups) to count URL occurrences within a string. TypeQL has no string function to count pattern occurrences within an attribute value.

### Query 139
**Error:** Unsupported: TypeQL has no function to extract hour/time components from datetime values (date arithmetic)

### Query 454
**Error:** Unsupported: size() on apoc.text.regexGroups() - counting regex pattern occurrences within a string has no TypeQL equivalent

### Query 526
**Error:** Unsupported features: size() and split() are not available in TypeQL. Cannot compute word count from text content.

### Query 574
**Error:** TypeQL does not support date component extraction functions (month, day from datetime). Cannot filter by specific month/day values.

### Query 579
**Error:** Uses size() with apoc.text.regexGroups() to count URL occurrences - TypeQL has no regex counting or size() function

## Conversion Notes

### Queries converted with TypeDB 3.8 len() (6 queries)
Queries 36, 135, 138, 177, 345, 451 used Cypher `size()` for string length. Converted using TypeDB 3.8 `len()` function.

### Query 34: simplified to tag+answered filter
"Users who asked questions tagged 'graphql' and answered within the same month" — Cypher compares question date to current month (`date().month`). Simplified to find answered graphql-tagged questions sorted by date, since exact month comparison requires `now()`.

