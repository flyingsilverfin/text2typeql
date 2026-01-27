# Recommendations Dataset

**Total queries in original dataset: 753**

## Current Status
- `queries.csv`: 741 successfully converted and validated queries
- `failed.csv`: 12 queries that cannot be converted (TypeQL limitations)

The sum of queries across all CSV files must equal 753.

## Failed Query Categories (12 total)

| Count | Reason |
|-------|--------|
| 3 | `size(split())` - no string split or word count in TypeQL |
| 2 | Duration functions (`duration()`, `duration.between()`) not available |
| 2 | Epoch timestamp conversion (`epochSeconds`) not available |
| 2 | No matching conversion pattern found (date comparisons) |
| 1 | `weekday` function not available in TypeQL |
| 1 | `collect()` aggregation has no direct TypeQL equivalent |
| 1 | `.year` date component extraction not available |
