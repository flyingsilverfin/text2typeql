# Text2TypeQL - Progress

## Current Status

**Phase**: Query Conversion (Agent-Based)
**Last Updated**: 2026-01-20

### Conversion Progress

| Database | Converted | Failed | Failed Review | Total | % |
|----------|-----------|--------|---------------|-------|---|
| twitter | 493 | 0 | 0 | 493 | 100% |
| twitch | 506 | 42 | 13 | 561 | 100% |
| recommendations | 555 | 73 | 125 | 753 | 100% |
| movies | 18 | 1 | 0 | 729 | 3% |
| neoflix | 885 | 30 | 0 | 915 | 100% |
| companies | 0 | 0 | 0 | 933 | 0% |
| gameofthrones | 0 | 0 | 0 | 392 | 0% |
| **Total** | **2457** | **146** | **138** | **4776** | **57%** |

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

Patterns that remain unsupported:
- `size(property)` - No direct equivalent for string/list length

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
