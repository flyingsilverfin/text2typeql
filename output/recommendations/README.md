# Recommendations Dataset

**Total queries in original dataset: 753**

## Current Status
- `queries.csv`: 710 successfully converted and validated queries
- `failed.csv`: 43 queries that cannot be converted (TypeQL limitations)

The sum of queries across all CSV files must equal 753.

## Failed Query Categories (43 total)

| Count | Reason |
|-------|--------|
| 13 | `size()` on string property - no string length function in TypeQL |
| 5 | Award/WON entities don't exist in schema |
| 5 | Schema mismatch/limitation (user gender, poster cardinality, etc.) |
| 3 | Duration functions (`duration()`, `duration.between()`) not available |
| 3 | `size(split())` - no string split or word count in TypeQL |
| 2 | Regex matching patterns (gender detection via name prefix) |
| 2 | User age attribute doesn't exist in schema |
| 2 | Epoch timestamp conversion (`epochSeconds`) not available |
| 2 | `collect()` aggregation has no direct TypeQL equivalent |
| 2 | No matching conversion pattern found |
| 1 | `diedIn` attribute doesn't exist in schema |
| 1 | `weekday` function not available in TypeQL |
| 1 | `.year` date component extraction not available |
| 1 | Array indexing (`languages[0]`) not available in TypeQL |
