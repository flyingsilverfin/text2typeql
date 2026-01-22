# Text2TypeQL - Progress

## Current Status

**Phase**: Query Validation Complete (Post-TypeDB Revalidation)
**Last Updated**: 2026-01-21

### Conversion Progress (After TypeDB Validation + Semantic Review + Fixes)

| Database | Valid | Failed | Failed Review | Total | % |
|----------|-------|--------|---------------|-------|---|
| twitter | 440 | 53 | 0 | 493 | 89% |
| twitch | 506 | 55 | 0 | 561 | 90% |
| recommendations | 555 | 198 | 0 | 753 | 74% |
| movies | 568 | 161 | 0 | 729 | 78% |
| neoflix | 868 | 47 | 0 | 915 | 95% |
| companies | 833 | 100 | 0 | 933 | 89% |
| gameofthrones | 342 | 50 | 0 | 392 | 87% |
| **Total** | **4112** | **664** | **0** | **4776** | **86%** |

### Validation Summary

After full TypeDB validation, semantic review, and fixes:
- **Valid queries**: 4112 (86%) - Execute correctly against TypeDB
- **Validation failures**: 664 (14%) - TypeDB syntax/type errors or unconverted
- **Semantic failures**: 0 (all fixed or moved to failed.csv)

### Common Validation Failures (in failed.csv)

| Error Type | Count | Description |
|------------|-------|-------------|
| Dropped during processing | ~500 | Complex patterns not yet converted |
| size() function | ~50 | TypeQL lacks string/list length functions |
| GROUP BY + HAVING | ~30 | TypeQL reduce cannot filter aggregated results |
| Date arithmetic | ~10 | TypeQL lacks date/duration calculations |
| Array index access | ~20 | TypeQL doesn't support array[N] syntax |
| Complex subqueries | ~50 | Multi-step WITH clauses not directly translatable |

### Completed Setup
- [x] Project setup and structure
- [x] Neo4j dataset cloned (`data/text2cypher/`)
- [x] All 7 schemas converted to TypeQL (`output/*/schema.tql`)
- [x] TypeDB databases created with schemas loaded
- [x] Agent-based conversion skill created (`.claude/skills/convert-query.md`)
- [x] Helper script for query extraction (`scripts/get_query.py`)

## How to Resume

### 1. Start TypeDB
```bash
typedb server --development-mode.enabled=true
```

### 2. Check Current Progress
```bash
python3 -c "
import csv
for db in ['twitter', 'twitch', 'recommendations', 'movies', 'neoflix', 'companies', 'gameofthrones']:
    try:
        with open(f'output/{db}/queries.csv', 'r') as f:
            # Get last index
            rows = list(csv.DictReader(f))
            last = max(int(r['original_index']) for r in rows) if rows else -1
            print(f'{db}: {len(rows)} done, resume from index {last + 1}')
    except:
        print(f'{db}: 0 done, start from index 0')
"
```

### 3. Spawn Continuation Agents

For each database that needs more work, spawn a Task agent with:
- `subagent_type`: `general-purpose`
- Start index: see "Next Index" column above
- Include TypeDB 3.0 syntax in prompt

Example prompt structure:
```
Convert Cypher queries to TypeQL for the <database> database.
Start from index <next_index>.

Setup:
- Schema: /opt/text2typeql/output/<database>/schema.tql
- Get query: python3 /opt/text2typeql/scripts/get_query.py <database> <index>
- TypeDB database: text2typeql_<database>
- Output: Append to /opt/text2typeql/output/<database>/queries.csv

TypeDB 3.0 syntax:
- Relations: relation_type (role: $var, role: $var);
- Fetch: fetch { "prop": $entity.prop };
- Order: match → sort → limit → fetch/reduce
```

## Known Limitations

Some Cypher patterns require advanced TypeQL features:

| Cypher Pattern | TypeQL Solution |
|----------------|-----------------|
| `WITH x, count(y) WHERE count > N` | Chained reduce: `reduce ... match $count > N;` |
| `ORDER BY a / b` | Let expression: `let $ratio = $a / $b; sort $ratio;` |
| `count(DISTINCT ...)` with filter | Custom function with `distinct; return count;` |
| Multiple OPTIONAL MATCH counts | Type variables with `or` blocks |
| `abs(a - b)` for sorting | Let expression: `let $diff = abs($a - $b);` |
| `sum(r.weight) GROUP BY x` | Grouped reduce: `reduce $sum = sum($w) groupby $x; match $x has name $n;` |
| `x.prop + y.prop + z.prop` | Let expression: `let $total = $a + $b + $c;` |
| `IN [list]` filter | Or blocks: `{ $x == 1; } or { $x == 2; };` |
| `CONTAINS 'substring'` | Like pattern: `$var like ".*substring.*";` |

Patterns that remain unsupported:
- `size(property)` - No direct equivalent for string/list length
- `array[-1]` - Array index access not supported in TypeQL

See `docs/suggestions.md` for validated examples of advanced patterns.

These are recorded in `failed.csv` with error descriptions.

## File Locations

| File | Purpose |
|------|---------|
| `output/<db>/queries.csv` | Successful conversions |
| `output/<db>/failed.csv` | Failed conversions (TypeDB limitations) |
| `output/<db>/failed_review.csv` | Failed semantic review (TypeQL doesn't match question) |
| `scripts/get_query.py` | Extract query by index |
| `scripts/review_helper.py` | Move queries during review |
| `.claude/skills/convert-query.md` | Conversion skill reference |
