# Text2TypeQL - Progress

## Current Status

**Phase**: Query Conversion (Agent-Based)
**Last Updated**: 2025-01-19

### Conversion Progress

| Database | Converted | Failed | Total | % | Next Index |
|----------|-----------|--------|-------|---|------------|
| twitter | 240 | 1 | 493 | 49% | 241 |
| twitch | 172 | 0 | 561 | 31% | 172 |
| recommendations | 200 | 0 | 753 | 27% | 200 |
| movies | 0 | 0 | 729 | 0% | 0 |
| neoflix | 0 | 0 | 915 | 0% | 0 |
| companies | 0 | 0 | 933 | 0% | 0 |
| gameofthrones | 0 | 0 | 392 | 0% | 0 |
| **Total** | **612** | **1** | **4776** | **13%** | |

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

Some Cypher patterns can't be directly converted to TypeQL:
- `WHERE count{...} > N` - Can't filter on reduce result in same query
- `size(property)` - No direct equivalent for string/list length

These are recorded in `failed.csv` with error descriptions.

## File Locations

| File | Purpose |
|------|---------|
| `output/<db>/queries.csv` | Successful conversions |
| `output/<db>/failed.csv` | Failed after 3 attempts |
| `scripts/get_query.py` | Extract query by index |
| `.claude/skills/convert-query.md` | Conversion skill reference |
