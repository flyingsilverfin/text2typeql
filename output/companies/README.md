# Companies Dataset

**Total queries in original dataset: 933**

## Final Status
- `queries.csv`: 929 successfully converted and validated queries
- `failed.csv`: 4 queries that cannot be converted (TypeQL limitations)

Total: 929 + 4 = 933 ✓

## Schema Notes
- The `mentions` relation uses a generic `mentioned` role (not `organization`) so both organizations and cities can be mentioned in articles.

## Failed Queries

| Index | Question | Reason |
|-------|----------|--------|
| 341 | Names of orgs with a CEO younger than 40? | No string manipulation (`left()`) or date functions (`date().year`) to extract birth year from `person_id` and compute age |
| 708 | 3 orgs with the most detailed summaries? | No string length function (`size()` equivalent) to order by summary length |
| 783 | Orgs that have Accenture as their CEO? | Semantically invalid: Accenture is an organization, not a person, so it cannot be a CEO |
| 843 | CEOs of dissolved orgs mentioned in recent articles? | No relative date support: requires `now()` for `datetime() - duration()` calculation |
