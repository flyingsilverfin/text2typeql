# Game of Thrones Dataset

**Total queries in original dataset: 392**

## Final Status
- `queries.csv`: 381 successfully converted and validated queries
- `failed.csv`: 11 queries that cannot be converted (TypeQL limitations)

Total: 381 + 11 = 392 ✓

## Failed Queries

All 11 failures are due to `fastrf_embedding` being stored as a scalar `double`, with no array indexing or iteration support in TypeQL.

| Index | Question | Reason |
|-------|----------|--------|
| 7 | Characters whose fastrf_embedding includes a value > 1 | Array indexing/iteration not supported |
| 16 | Characters with fastrf_embedding first element below 0 | Array indexing/iteration not supported |
| 63 | Characters with fastrf_embedding starting with a positive number (first 5) | Array indexing/iteration not supported |
| 87 | Characters with fastrf_embedding starting with a negative number (first 5) | Array indexing/iteration not supported |
| 104 | Characters with fastrf_embedding values > 0.5 in any dimension | Array indexing/iteration not supported |
| 122 | Characters with fastrf_embedding first element > 0.5 | Array indexing/iteration not supported |
| 137 | Characters whose fastrf_embedding tenth element < -0.5 | Array indexing/iteration not supported |
| 150 | Characters whose fastrf_embedding fifth element > 0.5 | Array indexing/iteration not supported |
| 343 | 3 characters with fastrf_embedding first element > 0.5 | Array indexing/iteration not supported |
| 349 | 3 characters with most diverse fastrf_embedding values (range) | Array min/max operations not supported |
| 368 | Characters with fastrf_embedding last position < -0.5 (limit 3) | Array indexing/iteration not supported |
