# Recommendations Dataset

**Source:** `synthetic_gpt4o_demodbs` (Neo4j text2cypher)

**Total valid queries: 775**

Movies, users, ratings, genres.

## Current Status
- `queries.csv`: 764 converted queries
- 11 failed queries

Total: 764 + 11 = 775 / 775 ✓

## Failed Queries

### Query 29
**Error:** Unsupported: `apoc.date.fields(m.released).dayOfWeek` — TypeQL has no day-of-week extraction function.

### Query 396
**Error:** Unsupported: `date(d.born).year` — TypeQL cannot extract year component from datetime for comparison with movie year.

### Query 408
**Error:** Unsupported: `date(m.released) = date(a.born)` — TypeQL cannot compare date portions of datetime values directly.

### Query 447
**Error:** Unsupported: `split()` and `size()` for word counting in bio text has no TypeQL equivalent.

### Query 455
**Error:** Unsupported: `duration.inDays()` and epoch timestamp conversion — TypeQL cannot compute duration between dates or convert epoch seconds.

### Query 481
**Error:** Unsupported: `split()` for counting quote characters in plot text has no TypeQL equivalent.

### Query 537
**Error:** Unsupported: `split()` and `size()` for word counting in plot text has no TypeQL equivalent.

### Query 609
**Error:** Unsupported: `datetime({epochSeconds: r.timestamp})` — TypeQL cannot convert epoch timestamps to datetime for comparison.

### Query 676
**Error:** Unsupported: `date(m.released).dayOfWeek` — TypeQL has no day-of-week extraction function.

### Query 703
**Error:** Unsupported: same as Query 609 — epoch timestamp conversion not available.

### Query 733
**Error:** Unsupported: `date(u.born)` — TypeQL cannot extract date from a string birth year for comparison.

## Conversion Notes

### Query 529: hardcoded date cutoff
"Top 5 movies released in the last 5 years" — Cypher uses `date().year - 5`. Converted with hardcoded cutoff `released >= 2017-01-01` (dataset is from ~2022).
