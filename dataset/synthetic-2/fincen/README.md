# FinCEN Dataset

**Source:** `synthetic_gpt4o_demodbs` (Neo4j text2cypher)

**Total valid queries: 614**

Financial filings, banks, countries.

## Current Status
- `queries.csv`: 609 converted queries
- 5 failed queries

Total: 609 + 5 = 614 / 614 ✓

## Failed Queries

### Query 20
**Error:** Requires string-to-float conversion (toFloat) and abs() function on computed difference - both unsupported in TypeQL

### Query 374
**Error:** Duration of "exactly one month" — months have variable days (28-31), so duration equality comparison is not expressible in TypeQL

### Query 405
**Error:** Date arithmetic/extraction unsupported: query requires filtering by month component (Q4 = months 10-12) of datetime values across arbitrary years. TypeQL does not support datetime component extraction functions.

### Query 434
**Error:** TypeQL cannot cast string attributes to numeric types. origin_lat and beneficiary_lat are string-typed in the schema, so arithmetic (subtraction, abs) cannot be performed on them.

### Query 550
**Error:** Cypher uses substring() to extract and compare month portions of two string attributes. TypeQL has no substring function or equivalent string slicing capability.

## Conversion Notes

### Queries converted with TypeDB 3.8 datetime arithmetic (25 queries)

Queries 6, 23, 48, 52, 75, 87, 116, 145, 149, 167, 187, 205, 222, 226, 242, 319, 354, 427, 477, 508, 542, 566, 577, 611, 612 used Cypher `duration.between()` / `duration.inSeconds()` / `duration.inDays()` to compute filing duration. Converted using TypeDB 3.8 datetime subtraction: `let $diff = $period_end - $period_begin;` with `sort $diff` for ordering by duration.

