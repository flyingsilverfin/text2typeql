# Skill: convert-query

Convert a single Cypher query to TypeQL.

## Usage
```
/convert-query <database> <index> [--source failed_review]
```

By default, reads from the original Neo4j dataset. Use `--source failed_review` to re-convert queries from `failed_review.csv`.

## CRITICAL: Use Validation Script

**ALWAYS validate using the script - NEVER use typedb console directly:**
```bash
python3 pipeline/scripts/validate_typeql.py <database> --file /tmp/query.tql
```
Returns "OK" on success, error message on failure. This is the ONLY way to validate.

## CRITICAL: Context Management

**NEVER read entire CSV files.** Use these scripts instead:

```bash
# Check if already converted
python3 pipeline/scripts/csv_read_row.py dataset/<db>/queries.csv <index> --exists

# Append success
python3 pipeline/scripts/csv_append_row.py dataset/<db>/queries.csv '{"original_index": N, "question": "...", "cypher": "...", "typeql": "..."}'

# Append failure
python3 pipeline/scripts/csv_append_row.py dataset/<db>/failed.csv '{"original_index": N, "question": "...", "cypher": "...", "error": "..."}'
```

**Be concise.** Do not output full queries in explanations. Stop immediately after writing result.

---

## Conversion Steps

### 1. Check if Already Done
```bash
python3 pipeline/scripts/csv_read_row.py dataset/<database>/queries.csv <index> --exists
python3 pipeline/scripts/csv_read_row.py dataset/<database>/failed.csv <index> --exists
```
If either returns `true`, report "already processed" and STOP.

**Exception**: When using `--source failed_review`, skip this check (we're re-converting).

### 2. Get Query

**Default (from original dataset):**
```bash
python3 pipeline/scripts/get_query.py <database> <index>
```

**From failed_review.csv:**
```bash
python3 pipeline/scripts/csv_read_row.py dataset/<database>/failed_review.csv <index>
```
This returns JSON with `original_index`, `question`, `cypher`, `typeql` (previous attempt), and `review_reason`.

### 3. Load Schema
Read `dataset/<database>/schema.tql` for entity types, relations, and role names.

### 4. Convert to TypeQL

**Key TypeDB 3.0 rules:**
- Order: `match` → `sort` → `limit` → `fetch`/`reduce`
- Relations: `relation_type (role: $var, role: $var);`
- Fetch: `fetch { "prop": $entity.prop };`
- Bind for filter/sort: `$p has age $a; $a > 25; sort $a;`

For complex patterns, read `pipeline/docs/typeql_reference.md`.

### 5. Validate Against TypeDB

```bash
# Write query to temp file and validate
cat > /tmp/query.tql << 'EOF'
<your typeql here>
EOF

python3 pipeline/scripts/validate_typeql.py <database> --file /tmp/query.tql
# Returns "OK" and exit 0 on success, error message and exit 1 on failure
```

Or validate inline (careful with escaping):
```bash
python3 pipeline/scripts/validate_typeql.py <database> 'match $x isa organization; limit 1; fetch { "n": $x.name };'
```

### 6. Semantic Review

Before saving, verify (without looking at Cypher):
- Returns correct entity type (question asks for users → return users)
- Includes ALL conditions from question
- Correct aggregation if asking for "top N" or "count"
- Relation directions correct

### 7. Write Result

**Success (from original dataset):**
```bash
python3 pipeline/scripts/csv_append_row.py dataset/<database>/queries.csv '{"original_index": <index>, "question": "<escaped>", "cypher": "<escaped>", "typeql": "<escaped>"}'
```

**Success (from failed_review.csv):**
```bash
python3 pipeline/scripts/csv_move_row.py dataset/<database>/failed_review.csv dataset/<database>/queries.csv <index> '{"typeql": "<new_typeql>"}'
```

**Failure after 3 attempts (from original dataset):**
```bash
python3 pipeline/scripts/csv_append_row.py dataset/<database>/failed.csv '{"original_index": <index>, "question": "<escaped>", "cypher": "<escaped>", "error": "<reason>"}'
```

**Failure after 3 attempts (from failed_review.csv):**
```bash
python3 pipeline/scripts/csv_move_row.py dataset/<database>/failed_review.csv dataset/<database>/failed.csv <index> '{"error": "<reason>"}'
```

**JSON escaping:** Use `json.dumps()` in Python or escape `"` as `\"` and newlines as `\n`.

---

## Quick TypeQL Reference

```typeql
# Entity with attribute
$p isa person, has name "John";

# Relation
acted_in (actor: $p, film: $m);

# Fetch directly
fetch { "name": $p.name };

# Grouped count
reduce $count = count($m) groupby $p;
sort $count desc;
limit 5;

# Negation
not { follows (follower: $p); };

# Optional (OPTIONAL MATCH)
try { $p has nickname $n; };

# Disjunction
{ $x has status "A"; } or { $x has status "B"; };
```

## Common Errors

| Error | Fix |
|-------|-----|
| Wrong order | `match` before `sort` before `limit` before `fetch` |
| Unknown role | Check schema for exact role names |
| Can't use relation var | Bind: `$rel isa type (role: $x);` |
| Variable scoping | Don't define vars inside `or {}` blocks if needed outside |

## Unsupported (→ failed.csv)

`size()`, `collect()`, `array[N]`, date arithmetic, `split()`, `left()`
