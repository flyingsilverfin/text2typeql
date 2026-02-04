# FinCEN Dataset

**Source:** `synthetic_gpt4o_demodbs` (Neo4j text2cypher)

**Total valid queries: 614**

Financial filings, banks, countries.

## Current Status
- `queries.csv`: 584 converted queries
- 30 failed queries

Total: 584 + 30 = 614 / 614 ✓

## Failed Queries

### Query 6
**Error:** Unsupported: date arithmetic (duration.between) not available in TypeQL

### Query 20
**Error:** Requires string-to-float conversion (toFloat) and abs() function on computed difference - both unsupported in TypeQL

### Query 23
**Error:** Unsupported: date arithmetic (duration.between) not available in TypeQL

### Query 48
**Error:** Unsupported: date arithmetic (duration.between) is not available in TypeQL

### Query 52
**Error:** Unsupported: date arithmetic (duration.between) required to compute longest relationship duration

### Query 75
**Error:** Unsupported: date arithmetic (duration.inSeconds between two datetime values) is not available in TypeQL

### Query 87
**Error:** Unsupported: date arithmetic (duration.inSeconds between two datetimes) has no TypeQL equivalent

### Query 116
**Error:** Cypher uses duration.between() date arithmetic which is unsupported in TypeQL 3.0

### Query 145
**Error:** Unsupported: date arithmetic (duration.between) is not available in TypeQL 3.0

### Query 149
**Error:** Unsupported: date arithmetic (duration.between) is not available in TypeQL 3.0

### Query 167
**Error:** Unsupported: date arithmetic (duration.between) has no TypeQL equivalent

### Query 187
**Error:** Unsupported: date arithmetic (duration.between) has no TypeQL equivalent

### Query 205
**Error:** Unsupported: date arithmetic (duration.between) has no TypeQL equivalent

### Query 222
**Error:** Unsupported: date arithmetic (duration.between) is not available in TypeQL 3.0

### Query 226
**Error:** Unsupported: date arithmetic (duration.between) is not available in TypeQL 3.0. Cannot compute the difference between two datetime values.

### Query 242
**Error:** Unsupported: date arithmetic (duration.between) has no TypeQL equivalent

### Query 319
**Error:** Unsupported feature: date arithmetic (duration.between) is not available in TypeQL 3.0

### Query 354
**Error:** Unsupported feature: date arithmetic (duration.inDays) has no TypeQL equivalent

### Query 374
**Error:** Date arithmetic (duration.inMonths) is not supported in TypeQL 3.0

### Query 405
**Error:** Date arithmetic/extraction unsupported: query requires filtering by month component (Q4 = months 10-12) of datetime values across arbitrary years. TypeQL does not support datetime component extraction functions.

### Query 427
**Error:** Unsupported: date arithmetic (duration.between) not available in TypeQL

### Query 434
**Error:** TypeQL cannot cast string attributes to numeric types. origin_lat and beneficiary_lat are string-typed in the schema, so arithmetic (subtraction, abs) cannot be performed on them.

### Query 477
**Error:** Unsupported feature: date arithmetic (duration.between) has no TypeQL equivalent

### Query 508
**Error:** Unsupported feature: date arithmetic (duration.between) is not available in TypeQL

### Query 542
**Error:** Unsupported: date arithmetic (duration.between) has no TypeQL equivalent

### Query 550
**Error:** Cypher uses substring() to extract and compare month portions of two string attributes. TypeQL has no substring function or equivalent string slicing capability.

### Query 566
**Error:** Unsupported: date arithmetic (duration.inSeconds) has no TypeQL equivalent

### Query 577
**Error:** Unsupported: date arithmetic (duration.between) has no TypeQL equivalent

### Query 611
**Error:** Unsupported: date arithmetic (duration.between) has no TypeQL equivalent

### Query 612
**Error:** Unsupported: date arithmetic (duration.inDays between two dates) not available in TypeQL

