# Twitter Dataset

**Total queries in original dataset: 493**

## Current Status
- `queries.csv`: 487 successfully converted and validated queries
- `failed.csv`: 6 queries that cannot be converted (TypeQL/schema limitations)

The sum of queries across all CSV files must equal 493.

## Failed Query Categories (6 total)

| Count | Reason |
|-------|--------|
| 2 | Schema limitation: `follows` relation has no timestamp - cannot determine recency |
| 1 | Schema limitation: only `me` entity can play `amplifier` role in `amplifies` |
| 1 | Schema limitation: `amplifies` is user-to-user, not a tweet relation |
| 1 | Schema mismatch: `mentions` direction (tweet→user vs user→tweet) |
| 1 | No string similarity functions in TypeQL |
