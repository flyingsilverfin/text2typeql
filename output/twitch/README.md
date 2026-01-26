# Twitch Dataset

**Total queries in original dataset: 561**

## Current Status
- `queries.csv`: 544 successfully converted and validated queries
- `failed.csv`: 17 queries that cannot be converted (TypeQL limitations)

The sum of queries across all CSV files must equal 561.

## Failed Query Categories (17 total)

| Count | Reason |
|-------|--------|
| 6 | String functions (`size()`, `split()`, `left()`) not available in TypeQL |
| 4 | Schema mismatch: User/Team entities lack `followers` attribute (only Stream has it) |
| 3 | Schema error: Cypher references wrong relationship direction or missing attributes |
| 2 | Datetime arithmetic (`datetime() - duration()`) not available |
| 2 | String length sorting (`ORDER BY size(...)`) not available |
