# Game of Thrones Dataset

**Source:** `synthetic_opus_demodbs` (Neo4j text2cypher)

**Total queries in original dataset: 392**

## Current Status
- `queries.csv`: 381 successfully converted and validated queries
- 11 queries that cannot be converted (documented below in Failed Queries)

Total: 381 + 11 = 392 ✓

## Failed Queries (11 total)

All 11 failures are due to `fastrf_embedding` being stored as a scalar `double` in the TypeQL schema, with no array indexing or iteration support in TypeQL.

### Query 7
**Reason:** Array iteration (`any(x IN ... WHERE ...)`) not supported in TypeQL.
```cypher
MATCH (c:Character)
WHERE any(x IN c.fastrf_embedding WHERE x > 1)
RETURN c.name
LIMIT 3
```

### Query 16
**Reason:** Array indexing (`[0]`) not supported in TypeQL.
```cypher
MATCH (c:Character)
WHERE c.fastrf_embedding[0] < 0
RETURN c.name
```

### Query 63
**Reason:** Array indexing (`[0]`) not supported in TypeQL.
```cypher
MATCH (c:Character)
WHERE c.fastrf_embedding[0] >= 0
RETURN c.name
LIMIT 5
```

### Query 87
**Reason:** Array indexing (`[0]`) not supported in TypeQL.
```cypher
MATCH (c:Character)
WHERE c.fastrf_embedding[0] < 0
RETURN c.name
LIMIT 5
```

### Query 104
**Reason:** Array iteration (`any(x IN ... WHERE ...)`) not supported in TypeQL.
```cypher
MATCH (c:Character)
WHERE any(x IN c.fastrf_embedding WHERE x > 0.5)
RETURN c
```

### Query 122
**Reason:** Array indexing (`[0]`) not supported in TypeQL.
```cypher
MATCH (c:Character)
WHERE c.fastrf_embedding[0] > 0.5
RETURN c.name
```

### Query 137
**Reason:** Array indexing (`[9]`) not supported in TypeQL.
```cypher
MATCH (c:Character)
WHERE c.fastrf_embedding[9] < -0.5
RETURN c.name
```

### Query 150
**Reason:** Array indexing (`[4]`) not supported in TypeQL.
```cypher
MATCH (c:Character)
WHERE c.fastrf_embedding[4] > 0.5
RETURN c.name
```

### Query 343
**Reason:** Array indexing (`[0]`) not supported in TypeQL.
```cypher
MATCH (c:Character)
WHERE c.fastrf_embedding[0] > 0.5
RETURN c.name, c.fastrf_embedding
LIMIT 3
```

### Query 349
**Reason:** Array min/max operations not supported in TypeQL.
```cypher
MATCH (c:Character)
WHERE c.fastrf_embedding IS NOT NULL
WITH c, max(c.fastrf_embedding) AS maxVal, min(c.fastrf_embedding) AS minVal
RETURN c.name AS character, maxVal - minVal AS embeddingRange
ORDER BY embeddingRange DESC
LIMIT 3
```

### Query 368
**Reason:** Array indexing (`[-1]`) not supported in TypeQL.
```cypher
MATCH (c:Character)
WHERE c.fastrf_embedding[-1] < -0.5
RETURN c.name
LIMIT 3
```

## Original Cypher Errors

No original Cypher interpretation errors were identified in this dataset. Semantic review found TypeQL translation errors (wrong sort direction for "lowest" queries, missing aggregations) that were all fixed during the review process.
