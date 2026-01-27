# Recommendations Dataset

**Total queries in original dataset: 753**

## Current Status
- `queries.csv`: 741 successfully converted and validated queries
- 12 queries that cannot be converted (documented below in Failed Queries)

Total: 741 + 12 = 753 ✓

## Failed Queries (12 total)

### Query 404
**Reason:** No `weekday` date component extraction in TypeQL.
```cypher
MATCH (m:Movie)
WHERE date(m.released).weekday = 5
RETURN m.title, m.released
ORDER BY m.released DESC
LIMIT 3
```

### Query 447
**Reason:** No epoch timestamp conversion (`epochSeconds`) in TypeQL.
```cypher
MATCH (u:User)-[r:RATED]->(m:Movie)
WHERE m.released = date(datetime({epochSeconds: r.timestamp}))
RETURN u, r, m
```

### Query 484
**Reason:** `collect()` aggregation has no direct TypeQL equivalent.
```cypher
MATCH (m:Movie)<-[:DIRECTED]-(d:Director)
WITH d, m
ORDER BY m.imdbRating DESC
WITH d, collect(m)[0..5] AS topMovies
RETURN d.name AS director,
       [movie IN topMovies | movie.title] AS movies,
       [movie IN topMovies | movie.imdbRating] AS ratings
```

### Query 537
**Reason:** No epoch timestamp conversion (`epochSeconds`) in TypeQL.
```cypher
MATCH (u:User)-[r:RATED]->(m:Movie)
WHERE m.released = date(datetime({epochSeconds: r.timestamp}))
RETURN u.name AS user, m.title AS movie, m.released AS releaseDate
ORDER BY r.timestamp
LIMIT 3
```

### Query 570
**Reason:** Duration functions (`duration.between()`) not available in TypeQL.
```cypher
MATCH (m:Movie)<-[r:RATED]-(u:User)
WITH m, r, u,
     datetime(m.released) AS releasedDate,
     datetime({epochSeconds: r.timestamp}) AS ratingDate
WITH m, duration.between(releasedDate, ratingDate).days AS daysBetween
ORDER BY daysBetween
LIMIT 5
RETURN m.title AS movie, daysBetween AS daysBetweenReleaseAndRating
```

### Query 577
**Reason:** No `size(split())` — cannot count words in a string.
```cypher
MATCH (m:Movie)<-[:DIRECTED]-(d:Director)
WHERE size(split(d.bio, ' ')) > 500
RETURN m.title
LIMIT 3
```

### Query 591
**Reason:** No `.year` date component extraction — cannot compare release year to rating year.
```cypher
MATCH (m:Movie)<-[r:RATED]-(u:User)
WHERE datetime(m.released).year = datetime(r.timestamp).year
WITH m, count(r) AS ratingCount
ORDER BY ratingCount DESC
LIMIT 5
RETURN m.title AS movie, ratingCount AS ratingsInFirstYear
```

### Query 593
**Reason:** No `size(split())` — cannot count quote delimiters in a string.
```cypher
MATCH (m:Movie)
WHERE m.plot IS NOT NULL
RETURN m.title, m.plot
ORDER BY size(split(m.plot, '"')) DESC
LIMIT 5
```

### Query 615
**Reason:** No date comparison functions — cannot compare release date to actor birth date.
```cypher
MATCH (actor:Actor)-[:ACTED_IN]->(movie:Movie)
WHERE date(movie.released) = date(actor.born)
RETURN actor.name AS actor, movie.title AS movie, movie.released AS releaseDate
```

### Query 626
**Reason:** No `.year` property accessor — cannot extract year from born date.
```cypher
MATCH (d:Director)-[:DIRECTED]->(m:Movie)
WHERE m.year = d.born.year
RETURN d.name AS director, m.title AS movie, m.year AS year
```

### Query 681
**Reason:** No `size(split())` — cannot count words in a string.
```cypher
MATCH (m:Movie)
WHERE size(split(m.plot, ' ')) > 100
RETURN m.title, m.plot
ORDER BY size(split(m.plot, ' ')) DESC
LIMIT 5
```

### Query 738
**Reason:** Duration functions (`date() - duration('P5Y')`) not available in TypeQL.
```cypher
MATCH (m:Movie)
WHERE m.released IS NOT NULL AND date(m.released) > date() - duration('P5Y')
RETURN m.title, m.released, m.imdbRating
ORDER BY m.imdbRating DESC
LIMIT 5
```

## Original Cypher Errors

During conversion, TypeDB's richer type system and semantic review caught cases where the original Cypher did not correctly answer the English question.

### Wrong counting target (1 query)

| Index | Question | Cypher error |
|-------|----------|-------------|
| 28 | "First 5 movies rated by users from more than 10 different countries" | `count(DISTINCT u.name) AS numCountries` — counts distinct user names, not distinct countries. Variable is misleadingly named `numCountries`. |

**How TypeDB caught it:** The TypeQL schema has an explicit `located_in` relation connecting users to countries. The TypeQL correctly counts distinct countries: `located_in (resident: $u, country: $c); select $c; distinct; return count`. Counting a generic property like `u.name` and calling it "countries" is not possible in a typed system.

### Hardcoded workaround instead of schema attribute (1 query)

| Index | Question | Cypher error |
|-------|----------|-------------|
| 624 | "Movies rated by both male and female users" | `WHERE exists{ (m)<-[:RATED]-(:User {name: 'Omar Huffman'}) } AND exists{ (m)<-[:RATED]-(:User {name: 'Myrtle Potter'}) }` — hardcodes two specific users instead of filtering by gender. |

**How TypeDB caught it:** The TypeQL schema has an explicit `gender` attribute on User. The TypeQL correctly filters by gender: `$u1 isa user, has gender "male"; $u2 isa user, has gender "female"`. The Cypher workaround assumes specific users represent genders, which fails for any other data.

### Non-existent attribute reference (2 queries)

| Index | Question | Cypher error |
|-------|----------|-------------|
| 22 | "Top 3 movies rated by the oldest users" | `ORDER BY u.born ASC` — User entity has no `born` attribute in the original schema (only Person does) |
| 242 | "First 3 movies rated by the youngest users" | `ORDER BY u.born DESC` — same non-existent attribute |

**How TypeDB caught it:** TypeDB type-checks every attribute access against the schema. `User` cannot have `born` unless explicitly declared. The TypeQL schema added an `age` attribute to User, and the queries use `sort $a desc` (oldest = highest age) and `sort $a asc` (youngest = lowest age) respectively.
