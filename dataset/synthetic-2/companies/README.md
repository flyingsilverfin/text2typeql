# Companies Dataset

**Source:** `synthetic_gpt4o_demodbs` (Neo4j text2cypher)

**Total valid queries: 966**

Companies, people, investments, locations.

## Current Status
- `queries.csv`: 949 converted queries
- 17 failed queries

Total: 949 + 17 = 966 / 966 ✓

## Failed Queries

### Cypher references non-existent Neo4j schema elements (11 queries)

These queries contain Cypher that references labels, properties, or relationships that don't exist in the Neo4j schema (`neo4j_schema.json`). The LLM hallucinated schema elements based on world knowledge rather than the actual data model.

#### Query 18
**Cypher error:** `Article-[:MENTIONS]->Person` — Neo4j MENTIONS relationship only goes `Article→Organization`. Person is not a valid target for MENTIONS.
```cypher
MATCH (a1:Article)-[:MENTIONS]->(ceo)
```

#### Query 29
**Cypher error:** `IN_COUNTRY {capital: true}` — the IN_COUNTRY relationship has no properties in the Neo4j schema (`rel_props` is empty).
```cypher
MATCH (capital:City)-[:IN_COUNTRY {capital: true}]->(co)
```

#### Query 210
**Cypher error:** Same as Query 29 — `IN_COUNTRY {capital: true}` references a non-existent relationship property.

#### Query 246
**Cypher error:** `HAS_PARENT|HAS_CHILD*0..->Country` — these relationships only connect `Person→Person`. There is no path from Person to Country in the schema.
```cypher
(ceo)-[:HAS_PARENT|HAS_CHILD*0..]->(ceoCountry:Country)
```

#### Query 280
**Cypher error:** `r.since` on HAS_COMPETITOR — the Neo4j schema has no relationship properties at all (`rel_props` is empty). The `since` property is hallucinated.
```cypher
MATCH (o1:Organization)-[r:HAS_COMPETITOR]->(o2:Organization)
WHERE r.since <= date().year - 5
```

#### Query 410
**Cypher error:** `country.capital` — the Country node has no `capital` property (valid: `name`, `id`, `summary`).
```cypher
WHERE city.name = country.capital
```

#### Query 611
**Cypher error:** Same as Query 280 — `r.since` on HAS_COMPETITOR is hallucinated.

#### Query 682
**Cypher error:** Same as Query 410 — `country.capital` doesn't exist.

#### Query 721
**Cypher error:** `ceo.gender` — Person has no `gender` property (valid: `name`, `id`, `summary`).
```cypher
WHERE category.name = "Healthcare" AND ceo.gender = "Female"
```

#### Query 929
**Cypher error:** `IN_COUNTRY {isCapital: true}` — same issue as Query 29, non-existent relationship property.

#### Query 944
**Cypher error:** `HAS_NATIONALITY` — this relationship type doesn't exist in the Neo4j schema.
```cypher
(ceo)-[:HAS_NATIONALITY]->(ceoCountry:Country)
```

### Unsupported TypeQL features (6 queries)

These queries require features that TypeQL does not support.

#### Query 337
**Error:** Requires current date (`datetime().year`) for "changed CEOs in the last year" — TypeQL has no `now()` function.

#### Query 341
**Error:** Requires current date (`date() - duration({days: 30})`) for "recent articles" — TypeQL has no `now()` function.

#### Query 380
**Error:** `COLLECT(a)[..3]` array slicing — TypeQL has no per-group top-N or array slicing.

#### Query 484
**Error:** `split()` function and array indexing for extracting last name from full name — TypeQL has no string split function.

#### Query 593
**Error:** Same as Query 337 — requires `datetime().year` for "changed CEO in the past year".

#### Query 662
**Error:** `[:HAS_SUPPLIER*]` variable-length path traversal (transitive closure) — TypeQL does not support unbounded path traversal.

## Conversion Notes

### Queries converted with approximations

Several queries used Cypher `WITH...ORDER BY...LIMIT` subquery patterns (get N entities first, then expand). These were converted to flat TypeQL joins with a single `sort`/`limit` at the end, which limits result rows rather than the intermediate entity set. This approximation was validated against the same pattern used in synthetic-1 (queries 5, 687).

| Index | Pattern | Approximation |
|-------|---------|---------------|
| 85 | Board members of first 3 Technology orgs | Flat join, limit 3 rows |
| 372 | Board members of first 3 public orgs | Flat join, limit 3 rows |
| 890 | Top 3 orgs with CEO in latest articles | Sort by article date, limit 3 rows |
| 905 | First 3 investors in Accenture + other investments | Flat join, all results |

### Queries converted with hardcoded dates

Queries 637 and 723 ask about CEO tenure "over a decade". The Cypher references `Person.startDate` (which doesn't exist in Neo4j), but the TypeQL schema correctly models `start-date` as an attribute of the `ceo_of` relation. The date threshold was hardcoded to `2012-01-01` based on the dataset creation date (~2022).

### Query 598: collect() dropped

"Which organizations have a CEO who is also an investor in other organizations?" — Cypher uses `collect()` for display grouping. TypeQL returns flat (org, ceo, investedOrg) rows instead.
