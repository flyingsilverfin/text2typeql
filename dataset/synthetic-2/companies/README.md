# Companies Dataset

**Source:** `synthetic_gpt4o_demodbs` (Neo4j text2cypher)

**Total valid queries: 966**

Companies, people, investments, locations.

## Current Status
- `queries.csv`: 941 converted queries
- 25 failed queries

Total: 941 + 25 = 966 / 966 ✓

## Failed Queries

### Query 18
**Error:** Schema mismatch: Cypher uses Article-[:MENTIONS]->Person but TypeQL schema only allows mentions(article, mentioned) where mentioned is organization or city. Person entity does not play mentions:mentioned role.

### Query 29
**Error:** Schema limitation: Cypher uses IN_COUNTRY {capital: true} relation property to identify capital cities, but the TypeQL location-contains relation has no attributes. The capital concept is not modeled in the TypeQL schema.

### Query 85
**Error:** Unsupported: collect() aggregation and intermediate WITH...ORDER BY...LIMIT subquery pipeline cannot be expressed in TypeQL

### Query 210
**Error:** Schema mismatch: Cypher references {capital: true} property on IN_COUNTRY relationship, but no such property exists in either the Neo4j or TypeQL schema. The query concept (filtering by capital city status) cannot be represented.

### Query 246
**Error:** Schema has no relation connecting person to country. The Cypher uses variable-length HAS_PARENT|HAS_CHILD paths from person to country, but parent_of only relates person-to-person. Person nationality cannot be expressed in this schema.

### Query 280
**Error:** Unsupported: competes_with relation has no since attribute in schema, and date arithmetic (date().year - 5) is not supported in TypeQL

### Query 337
**Error:** Unsupported: date arithmetic (datetime().year extraction and current date comparison) has no TypeQL equivalent

### Query 341
**Error:** Unsupported feature: date arithmetic (date() - duration({days: 30})) is not available in TypeQL

### Query 372
**Error:** Unsupported: Cypher uses collect() to aggregate board members per organization, and requires a subquery pattern (LIMIT organizations first, then expand to board members). TypeQL limit applies to the entire result set, not a subset of variables, and collect() is not supported.

### Query 378
**Error:** Cypher size() string length function has no TypeQL equivalent

### Query 380
**Error:** Unsupported: COLLECT()[..N] array slicing. Cypher takes top-3 most recent articles per organization via COLLECT + array slice, then averages sentiment. TypeQL has no per-group top-N or array slicing capability.

### Query 410
**Error:** Schema mismatch: Cypher references country.capital but the TypeQL schema has no capital attribute on country entity. Cannot faithfully convert.

### Query 484
**Error:** Unsupported: split() function and array indexing for extracting last name from full name

### Query 593
**Error:** Unsupported: date arithmetic (datetime().year - 1) not available in TypeQL

### Query 598
**Error:** collect() function not supported in TypeQL

### Query 611
**Error:** Schema mismatch: competes_with relation has no since/start-date attribute in TypeQL schema. Also, date arithmetic (date().year - 10) is unsupported in TypeQL.

### Query 637
**Error:** Date arithmetic not supported in TypeQL (cannot calculate years from start-date to current date)

### Query 662
**Error:** Variable-length path traversal (transitive closure) not supported in TypeQL. Cypher uses [:HAS_SUPPLIER*] to match arbitrary-depth paths.

### Query 682
**Error:** Schema mismatch: Cypher references country.capital but country entity has no capital attribute in schema

### Query 721
**Error:** Schema mismatch: person entity has no gender attribute in TypeQL schema

### Query 723
**Error:** Date arithmetic (datetime().year - datetime(x).year) is not supported in TypeQL

### Query 890
**Error:** Unsupported: collect()[0] array indexing to get first element per group. TypeQL does not support array indexing operations.

### Query 905
**Error:** Unsupported: WITH...LIMIT (intermediate result limiting) followed by further MATCH - TypeQL does not support correlated subquery/lateral join patterns where LIMIT applies to intermediate results before expansion

### Query 929
**Error:** Schema mismatch: TypeQL schema has no isCapital attribute or equivalent to distinguish capital cities from non-capital cities. The Cypher query relies on IN_COUNTRY {isCapital: true} which has no TypeQL equivalent.

### Query 944
**Error:** Schema missing HAS_NATIONALITY relation - no way to represent person nationality in TypeQL schema

