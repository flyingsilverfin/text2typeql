# Twitch Dataset

**Total queries in original dataset: 561**

## Current Status
- `queries.csv`: 553 successfully converted and validated queries
- `failed.csv`: 8 queries that cannot be converted (TypeQL limitations)

The sum of queries across all CSV files must equal 561.

## Failed Query Categories (8 total)

| Count | Reason |
|-------|--------|
| 2 | String function: `left()` for first-letter comparison not available |
| 2 | Schema incompatibility: Cypher references User→Stream `PLAYS` relation that doesn't exist |
| 1 | Schema mismatch: User entity lacks `description` attribute + requires `split()` |
| 1 | Schema incompatibility: no timestamp on VIP relation for duration calculation |
| 1 | Datetime arithmetic (`datetime() - duration()`) not available |
| 1 | String function: `size()` for shortest name ordering not available |
