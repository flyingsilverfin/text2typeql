# Neoflix Dataset

**Total queries in original dataset: 915**

## Current Status
- `queries.csv`: 910 successfully converted and validated queries
- 5 queries that cannot be converted (documented below in Failed Queries)

Total: 910 + 5 = 915 ✓

## Failed Queries (5 total)

### Query 217
**Reason:** No date component extraction (`dayOfWeek`) in TypeQL — cannot filter by day of week.
```cypher
MATCH (m:Movie)
WHERE date(m.release_date).dayOfWeek IN [5, 6, 7]
RETURN m.title, m.release_date
ORDER BY m.release_date
LIMIT 5
```

### Query 240
**Reason:** No datetime component extraction (`year`, `month`, `day`) — cannot check for leap year (Feb 29).
```cypher
MATCH (a:Adult)
WHERE a.release_date IS NOT NULL AND
      date(a.release_date).year % 4 = 0 AND
      date(a.release_date).month = 2 AND
      date(a.release_date).day = 29
RETURN a.title
LIMIT 3
```

### Query 444
**Reason:** No relative date calculation — cannot express `date() - duration('P5Y')`.
```cypher
MATCH (m:Movie)
WHERE m.release_date >= date() - duration('P5Y')
RETURN m.title, m.popularity
ORDER BY m.popularity DESC
LIMIT 3
```

### Query 566
**Reason:** No datetime component extraction or modulo operations — cannot check `year % 4 == 0`.
```cypher
MATCH (m:Movie)
WHERE m.release_date IS NOT NULL AND date(m.release_date).year % 4 = 0
RETURN m.title, m.release_date
```

### Query 904
**Reason:** No `size(split())` — cannot count words in a string.
```cypher
MATCH (m:Movie)
WHERE m.overview IS NOT NULL
RETURN m.title, m.overview, size(split(m.overview, ' ')) AS wordCount
ORDER BY wordCount DESC
LIMIT 5
```

## Original Cypher Errors

No original Cypher interpretation errors were identified in this dataset. All semantic review issues were TypeQL translation errors (missing filters, wrong output fields) that were fixed during the review process.
