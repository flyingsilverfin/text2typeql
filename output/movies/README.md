# Movies Dataset

**Total queries in original dataset: 729**

## Final Status
- `queries.csv`: 723 successfully converted and validated queries
- `failed.csv`: 6 queries that cannot be converted (TypeQL limitations)

Total: 723 + 6 = 729 ✓

## Failed Queries

### String length (`size()`) - 4 queries

TypeQL has no string length function. Cypher's `size(string)` returns character count, which is used to filter or sort by string length.

| Index | Question | Reason |
|-------|----------|--------|
| 379 | Movies with taglines longer than 30 characters? | Cannot filter by `size(tagline) > 30` |
| 408 | Who has the longest name among all actors? | Cannot sort by `size(p.name)` |
| 459 | 3 movies with the longest taglines? | Cannot sort by `size(tagline)` |
| 486 | Persons who follow someone with the same first letter in their name | No `substring()`/`left()` to extract and compare first characters |

### Role list counting - 2 queries

These use `size(r.roles)` per individual ACTED_IN relationship (per actor-movie pair), not per movie. Unlike the 9 role-count queries converted using a `role_count(movie)` function, these require counting roles on a specific relationship instance.

| Index | Question | Reason |
|-------|----------|--------|
| 267 | Movies with exactly 3 roles in ACTED_IN | Needs per-relationship `size(r.roles) = 3`, not total roles per movie |
| 484 | Top 3 longest relationships by roles between a person and a movie | Needs per-relationship role count to sort by |
