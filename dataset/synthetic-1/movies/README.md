# Movies Dataset

**Source:** `synthetic_opus_demodbs` (Neo4j text2cypher)

**Total queries in original dataset: 729**

## Current Status
- `queries.csv`: 723 successfully converted and validated queries
- 6 queries that cannot be converted (documented below in Failed Queries)

Total: 723 + 6 = 729 ✓

## Failed Queries (6 total)

### Query 267
**Reason:** TypeQL lacks `size()` for arrays — cannot count roles per individual ACTED_IN relationship. Unlike the 9 role-count queries converted using a `role_count(movie)` function, this needs per-relationship `size(r.roles) = 3`.
```cypher
MATCH (m:Movie)<-[r:ACTED_IN]-(p:Person)
WHERE size(r.roles) = 3
RETURN m.title
```

### Query 379
**Reason:** No string length function — cannot filter by `size(tagline) > 30`.
```cypher
MATCH (m:Movie)
WHERE m.tagline IS NOT NULL AND size(m.tagline) > 30
RETURN m.title, m.tagline
```

### Query 408
**Reason:** No string length function — cannot sort by `size(p.name)`.
```cypher
MATCH (p:Person)-[:ACTED_IN]->(:Movie)
RETURN p.name AS name
ORDER BY size(p.name) DESC
LIMIT 1
```

### Query 459
**Reason:** No string length function — cannot sort by `size(tagline)`.
```cypher
MATCH (m:Movie)
WHERE m.tagline IS NOT NULL
RETURN m.title, m.tagline
ORDER BY size(m.tagline) DESC
LIMIT 3
```

### Query 484
**Reason:** TypeQL lacks `size()` for arrays — cannot sort by per-relationship role count.
```cypher
MATCH (p:Person)-[r:ACTED_IN]->(m:Movie)
RETURN p.name AS person, m.title AS movie, r.roles AS roles
ORDER BY size(r.roles) DESC
LIMIT 3
```

### Query 486
**Reason:** No `left()` / `substring()` function — cannot extract and compare first characters of two names.
```cypher
MATCH (p1:Person)-[:FOLLOWS]->(p2:Person)
WHERE left(p1.name, 1) = left(p2.name, 1)
RETURN p1.name
LIMIT 3
```

## Original Cypher Errors

No original Cypher interpretation errors were identified in this dataset. All semantic review issues were TypeQL translation errors (missing relation constraints, missing filters) that were fixed during the review process.
