# Text2TypeQL - Project Notes

## Important: First Steps

Always read the following files when starting a session:
- `README.md` - Project overview and setup instructions
- `progress.md` - Current progress and next steps (if it exists)
- `plan.md` - Implementation plan (if it exists)
- `output/<database>/README.md` - Query counts for each database being worked on

## Updating Documentation

When learning new TypeQL patterns or syntax rules, update ALL of these files:
- `CLAUDE.md` - This file (quick reference)
- `.claude/skills/convert-query.md` - Conversion skill (comprehensive reference)
- `docs/suggestions.md` - Validated examples of advanced patterns

## Project Overview

Pipeline to convert Neo4j text2cypher datasets to TypeQL format for training text-to-TypeQL models.

**Source**: https://github.com/neo4j-labs/text2cypher (datasets/synthetic_opus_demodbs/)

## Important: Sequential Processing

**DO NOT process queries in parallel.** Multiple agents writing to the same CSV file can cause race conditions and data loss. Always process queries one at a time, waiting for each to complete before starting the next.

## Available Skills

Use this skill for query conversion (requires TypeDB server running):

- `/convert-query <database> <index>` - Convert a single Cypher query to TypeQL with validation, writes directly to CSV

## Quick Start

```bash
# Start TypeDB server (must be running for validation)
typedb server --development-mode.enabled=true

# Run pipeline
python main.py setup                      # Clone Neo4j dataset
python main.py list-schemas               # List available schemas
python main.py convert-schema movies      # Convert schema

# Use skill for query conversion (agent-based, no API costs)
/convert-query movies 0
```

## Agent-Based Query Conversion

**IMPORTANT: Always use the `convert-query-runner` subagent for TypeQL query generation and conversion.** Do NOT write TypeQL queries directly in the main conversation — always delegate to the specialized subagent via `Task tool with subagent_type=convert-query-runner`. This ensures consistent validation, semantic review, and CSV routing.

The subagent handles the full pipeline:

1. **Get query**: `python3 scripts/get_query.py <database> <index>` (or from `failed_review.csv`)
2. **Load schema**: `output/<database>/schema.tql`
3. **Convert** using TypeDB 3.0 syntax
4. **Validate** against TypeDB using: `python3 scripts/validate_typeql.py <database> --file /tmp/query.tql`
5. **Semantic review**: Verify TypeQL answers the English question (ignore Cypher)
6. **Write** to `queries.csv` (success) or document in `README.md` Failed Queries section (after 3 attempts)

### Validation Script

```bash
# Write query to temp file and validate
cat > /tmp/query.tql << 'EOF'
<your typeql here>
EOF
python3 scripts/validate_typeql.py <database> --file /tmp/query.tql
# Returns "OK" and exit 0 on success, error message and exit 1 on failure
```

### Semantic Review Checklist (Step 5)

Before writing to CSV, verify WITHOUT looking at Cypher:
- Returns correct entity type (question asks for users → return users)
- Includes ALL conditions from question (public AND 50+ employees)
- Has correct aggregation ("top 3 by count" → needs `reduce count groupby`, `sort`, `limit`)
- Relation directions correct (supplier OF vs supplies TO)
- Numeric thresholds correct (1 million = 1000000)

### Spawning Conversion Agents

**IMPORTANT: Process queries SEQUENTIALLY, not in parallel.** Parallel writes to the same CSV file can cause race conditions.

**ALWAYS use the subagent for any TypeQL query writing/conversion:**
```
Use Task tool with subagent_type=convert-query-runner
Prompt: "Convert query <index> from the <database> database"
```

When providing hints or specific patterns to use, include them in the subagent prompt:
```
Prompt: "Convert query <index> from <database>. Hint: use reduce with max() groupby for the aggregation."
```

For re-converting queries from `failed_review.csv`:
```
/convert-query <database> <index> --source failed_review
```

## TypeDB 3.0 Query Syntax

### Key Rules

1. **Query order**: `match` → `sort` → `limit` → `fetch` (or `reduce`)
2. **Relations**: `relation_type (role: $var, role: $var);` (NOT `(role: $var) isa type`)
3. **Fetch directly**: `fetch { "prop": $entity.prop };` - no need to bind
4. **Double quotes** for strings
5. **Grouped counts**: `reduce $count = count($var) groupby $group_var;`

### Pattern Examples

```typeql
# Entity with attribute
$p isa person, has name "John";

# Relation (TypeDB 3.0 style - preferred)
follows (follower: $a, followed: $b);

# Relation with variable (when accessing relation attributes)
$rel isa follows (follower: $a, followed: $b);
$rel has timestamp $t;

# Fetch directly from entity (preferred)
fetch { "name": $p.name, "age": $p.age };

# Bind only when filtering/sorting
$p has age $a; $a > 25;
sort $a desc;
fetch { "age": $a };

# Multi-cardinality attributes
fetch { "emails": [ $p.email ] };

# Negation
not { follows (follower: $p, followed: $other); };

# Grouped aggregation
match $p isa person; acted_in (actor: $p, film: $m);
reduce $count = count($m) groupby $p;
sort $count desc;
limit 5;
```

### Advanced Features

```typeql
# Custom functions - reusable query logic
with fun follower_count($user: user) -> integer:
  match follows (followed: $user);
  return count;
match $u isa user;
let $count = follower_count($u);
fetch { "user": $u.name, "followers": $count };

# Chained reduce - HAVING equivalent (filter on aggregation)
match $tweet isa tweet; retweets (original_tweet: $tweet);
reduce $count = count groupby $tweet;
match $count > 100;  # Filter after aggregation
fetch { "tweet": $tweet.text, "retweets": $count };

# Let expressions - computed values
let $ratio = $follows / $followers;
let $difference = abs($a - $b);

# Type variables - polymorphic queries
$rel isa $t;
{ $t label mentions; } or { $t label retweets; };

# Relation role inference - omit roles to match all permutations
$rel isa interacts ($c);  # Matches $c in ANY role (character1 or character2)

# Symmetric/bidirectional matching - omit roles for both players
subsidiary_of ($o1, $o2);  # Matches ($o1 as parent, $o2 as child) OR ($o1 as child, $o2 as parent)
# Replaces: { subsidiary_of (parent: $o1, subsidiary: $o2); } or { subsidiary_of (parent: $o2, subsidiary: $o1); };

# Explicit role type checking (when needed)
$rel isa interacts ($role: $c);
{ $role sub interacts:character1; } or { $role sub interacts:character2; };
```

### Variable Scoping in Disjunctions

**IMPORTANT**: Variables inside disjunction branches are scoped and not returned. This affects counting:

```typeql
# WRONG - $rel not accessible outside disjunction, nothing to count
{ interacts (character1: $c); } or { interacts (character2: $c); };
reduce $count = count($rel) groupby $comm;  # Error: $rel undefined

# ALSO WRONG - $rel inside disjunction branches is still scoped!
{ $rel isa interacts ($c); } or { $rel isa interacts1 ($c); };
reduce $count = count($rel) groupby $comm;  # $rel still scoped to branches

# RIGHT - single relation type, bind outside disjunction
$rel isa interacts ($c);
reduce $count = count($rel) groupby $comm;  # Works

# RIGHT - multiple relation types, use TYPE VARIABLE
$rel isa $t ($c);
{ $t label interacts; } or { $t label interacts1; } or { $t label interacts2; };
reduce $count = count($rel) groupby $comm;  # Works - $rel bound outside disjunction
```

### Cypher → TypeQL Mapping

| Cypher | TypeQL 3.0 |
|--------|------------|
| `MATCH (n:Label)` | `match $n isa label;` |
| `MATCH (a)-[:REL]->(b)` | `match rel (role1: $a, role2: $b);` |
| `RETURN n.prop` | `fetch { "prop": $n.prop };` |
| `WHERE n.prop > 5` | `has prop $p; $p > 5;` |
| `WHERE n.prop CONTAINS 'x'` | `has prop $p; $p like ".*x.*";` |
| `ORDER BY n.prop DESC` | `has prop $p; sort $p desc;` |
| `LIMIT 10` | `limit 10;` |
| `COUNT(n)` | `reduce $count = count($n);` |
| `WITH n, count(m) AS c` | `reduce $c = count($m) groupby $n;` |
| `WITH a, b, count(*) AS c` | `reduce $c = count groupby $a, $b;` (tuple groupby) |
| `WITH x, count(y) WHERE c > N` | `reduce $c = count groupby $x; match $c > N;` |
| `count(DISTINCT x) GROUP BY y` | `select $x, $y; distinct; reduce $c = count groupby $y;` |
| `ORDER BY a / b` | `let $ratio = $a / $b; sort $ratio;` |

## Database Names

TypeDB databases are prefixed: `text2typeql_<database>`
- `text2typeql_twitter`
- `text2typeql_twitch`
- `text2typeql_movies`
- `text2typeql_neoflix`
- `text2typeql_recommendations`
- `text2typeql_companies`

## Semantic Review Process

After conversion, review queries to verify TypeQL matches the English question:

### Review Checklist
1. **Correct entity returned** - Does the question ask for users/tweets/movies? Does query return that?
2. **Relation direction** - "has been retweeted" (passive) vs "retweets" (active)
3. **All conditions present** - "X AND Y" must have both constraints
4. **Correct property** - "retweeted 100 times" should count retweets, not check favorites
5. **OPTIONAL MATCH** - Must use `try { }` for left-join semantics

### Key Files for Review
- **Full guidance**: `docs/semantic_review_notes.md`
- **Move helper**: `python3 scripts/review_helper.py <database> <index1> [index2...] --reason "reason"`

### Common Semantic Mismatches
| Question Pattern | Common Mistake | Correct Approach |
|------------------|----------------|------------------|
| "retweeted X times" | Using `favorites` property | Count `retweets` relation |
| "tweets that have been retweeted" | Tweet as `retweeting_tweet` | Tweet as `original_tweet` |
| "users who amplified" | Wrong direction | Check `amplifier` vs `amplified_user` roles |
| "tweets from followers" | Tweets by Me | Tweets by users who follow Me |
| OPTIONAL MATCH | Require relation | Use `try { relation; }` |

## File Structure & Workflow

### Output Files (per database)

Each `output/<database>/` folder contains:

| File | Purpose | Format |
|------|---------|--------|
| `schema.tql` | TypeQL schema definition | TypeQL |
| `neo4j_schema.json` | Original Neo4j schema | JSON |
| `README.md` | Dataset status, failed queries (with Cypher + reason), and Cypher error notes | Markdown |
| `queries.csv` | **Successfully validated** TypeQL queries | `original_index,question,cypher,typeql` |
| `failed_review.csv` | Queries that **failed semantic review** (temporary, should be empty) | `original_index,question,cypher,typeql,review_reason` |

### Where Queries Live

Queries are tracked in one of these locations:

1. **`queries.csv`** - Successfully converted, validated, and semantically reviewed queries
2. **`README.md` (Failed Queries section)** - Queries that cannot be converted due to TypeQL limitations (string functions, date arithmetic, array operations, etc.). Each entry includes the query index, original Cypher, and reason for failure.
3. **`failed_review.csv`** (temporary) - Queries pending semantic review fixes. After fixing, queries move to `queries.csv` (success) or the README (unfixable). Should be empty when review is complete.

### Query Count Verification (CRITICAL)

**Before committing any changes**, verify that the total number of queries is preserved. The count of queries in `queries.csv` + failed queries documented in the README + any entries in `failed_review.csv` MUST equal the total in the original dataset.

Each README has a verification line like: `Total: 929 + 4 = 933 ✓`

### Retry Workflow

When retrying a previously failed query:
1. Attempt conversion using `/convert-query` skill
2. Validate against TypeDB
3. If **success**: Add to `queries.csv`, update README (remove from Failed Queries, update count)
4. If **failure**: Document in README's Failed Queries section with the Cypher and reason

### Scripts

**CRITICAL: Never read entire CSV files.** Use these scripts to prevent context overflow:

```
scripts/
  get_query.py           # Get source query by database and index
  csv_read_row.py        # Read single row from CSV by original_index
  csv_append_row.py      # Append row to CSV (creates with header if needed)
  csv_move_row.py        # Move row between CSVs atomically
  review_helper.py       # Move queries during review
```

**CSV Script Usage:**
```bash
# Check if query already processed
python3 scripts/csv_read_row.py output/<db>/queries.csv <index> --exists

# Append successful conversion
python3 scripts/csv_append_row.py output/<db>/queries.csv '{"original_index": N, "question": "...", "cypher": "...", "typeql": "..."}'
```

### Other Files

```
docs/
  semantic_review_notes.md  # Full review guidance
  typeql_reference.md       # Comprehensive TypeQL 3.0 reference (read only when needed)

.claude/skills/
  convert-query.md          # Conversion skill (streamlined)
```

## Query Counts by Database

| Database | Total Queries | Converted | Failed |
|----------|--------------|-----------|--------|
| twitter | 493 | 491 | 2 |
| twitch | 561 | 553 | 8 |
| movies | 729 | 723 | 6 |
| neoflix | 915 | 910 | 5 |
| recommendations | 753 | 741 | 12 |
| companies | 933 | 929 | 4 |
| gameofthrones | 392 | 381 | 11 |
| **Total** | **4776** | **4728** | **48** |
