# Skill: convert-query

Convert a single Cypher query to TypeQL.

## Usage

```
/convert-query <database> <index>
```

## Instructions

### 1. Get Query

Run: `python3 scripts/get_query.py <database> <index>`

Returns JSON with `index`, `question`, `cypher`.

### 2. Load Schema

Read `output/<database>/schema.tql` for entity types, relations, and role names.

### 3. Convert Cypher to TypeQL

**Use TypeDB 3.0 syntax specifically.** Key differences from 2.x:
- Relations: `relation_type (role: $var)` instead of `(role: $var) isa relation_type`
- Direct attribute fetch: `$entity.attribute` in fetch clauses
- Value variables use `$` not `?`

---

# TypeQL 3.0 Comprehensive Reference

## Query Pipeline Architecture

TypeQL 3.0 uses a sequential pipeline where stages execute in strict order:

```
match <pattern>; → [sort $var;] → [offset N;] → [limit N;] → fetch { } | reduce $var = func();
```

### Pipeline Stages

| Stage | Purpose | Example |
|-------|---------|---------|
| `match` | Locate data patterns | `match $p isa person;` |
| `sort` | Order results | `sort $age desc;` |
| `offset` | Skip results | `offset 5;` |
| `limit` | Restrict count | `limit 10;` |
| `fetch` | Return JSON structure | `fetch { "name": $n };` |
| `reduce` | Aggregate values | `reduce $count = count($p);` |
| `select` | Filter output variables | `select $p, $name;` |

**CRITICAL**: Order must be `match` → `sort` → `offset` → `limit` → `fetch`/`reduce`

## Variable System

- All variables use `$` prefix: `$person`, `$name`, `$count`
- Computed values use `let` keyword for expressions
- No distinction between concept and value variables (unlike TypeQL 2.x)

## Pattern Matching

### Type Assertion
```typeql
$x isa person;
$m isa movie;
```

### Attribute Ownership
```typeql
# Exact value
$x has name "John";
$m has released 2020;

# Bind to variable (for filtering/sorting)
$x has age $a;
$m has title $t;

# Multiple attributes
$p isa person, has name $n, has age $a;
```

### Relations - TypeDB 3.0 Syntax

**Anonymous relation (PREFERRED when not accessing relation attributes):**
```typeql
# Format: relation_type (role: $player, role: $player);
friendship (friend: $x, friend: $y);
acted_in (actor: $p, film: $m);
follows (follower: $a, followed: $b);
```

**Bound relation (use when you need relation attributes):**
```typeql
# Format: $rel isa relation_type (role: $player, role: $player);
$rel isa acted_in (actor: $p, film: $m);
$rel has role_name $role;

# Or using links keyword
$rel isa acted_in, links (actor: $p, film: $m);
```

### Value Comparisons
```typeql
# Must bind attribute to variable first, then compare
$p has age $a; $a > 25;
$m has released $r; $r >= 2000; $r < 2010;
$u has name $n; $n like "^John.*";
$t has text $txt; $txt contains "graph";
```

### Comparison Operators

| Operator | Purpose | Example |
|----------|---------|---------|
| `==` | Value equality | `$a == 25;` |
| `!=` | Value inequality | `$status != "inactive";` |
| `<`, `<=`, `>`, `>=` | Ordering | `$age >= 18;` |
| `like` | Regex pattern matching | `$name like "^A.*";` |
| `contains` | Substring check | `$text contains "graph";` |
| `is` | Concept identity (same instance) | `not { $x is $y; };` |

## Logical Operators

### Conjunction (AND)
Implicit via semicolons or commas:
```typeql
# These are equivalent
$p isa person, has name $n, has age $a;
$p isa person; $p has name $n; $p has age $a;
```

### Disjunction (OR)
```typeql
# Match either condition
{ $x has status "active"; } or { $x has status "pending"; };

# Multiple alternatives
{ $m has genre "action"; } or { $m has genre "thriller"; } or { $m has genre "horror"; };

# Complex disjunction with relations
{ acted_in (actor: $p, film: $m); } or { directed (director: $p, film: $m); };
```

**When to use `or`:**
- Cypher `WHERE a OR b` conditions
- Cypher `UNION` queries
- Questions asking "X or Y"
- Multiple alternative paths to same result

### Negation (NOT)
```typeql
# Exclude pattern
not { acted_in (actor: $p, film: $m); };

# Person without any movies
$p isa person;
not { acted_in (actor: $p); };

# Movie not in specific genre
$m isa movie;
not { $m has genre "horror"; };

# Not equal (two ways)
$a != 25;
not { $a == 25; };
```

**When to use `not`:**
- Cypher `WHERE NOT exists { }`
- Cypher `WHERE NOT (condition)`
- Questions with "not", "without", "excluding", "except"

### Optionality (TRY)
```typeql
# Include result even if pattern doesn't match (LEFT JOIN semantics)
$p isa person, has name $n;
try { $p has nickname $nick; };

# Multiple optional patterns
$m isa movie, has title $t;
try { $m has budget $b; };
try { $m has revenue $r; };
```

**CRITICAL - When to use `try`:**
- Cypher `OPTIONAL MATCH` patterns
- Include entities even when optional relation doesn't exist
- Questions asking for "if available", "when present"
- Aggregations that should include zero counts

**Example - OPTIONAL MATCH conversion:**
```cypher
-- Cypher
MATCH (s:Stream)
OPTIONAL MATCH (s)-[:MODERATOR]->(m:User)
RETURN s.name, count(m) AS moderators
```
```typeql
-- TypeQL
match
  $s isa stream, has name $sn;
  try { moderation (moderated_channel: $s, moderating_user: $m); };
reduce $count = count($m) groupby $sn;
```

## Fetch Patterns

### Basic Fetch
```typeql
# Fetch bound variables
match $p isa person, has name $n, has age $a;
fetch { "name": $n, "age": $a };
```

### Direct Attribute Access (PREFERRED)
```typeql
# No need to bind - use $entity.attribute directly
match $m isa movie;
fetch { "title": $m.title, "year": $m.released };
```

### Multi-Cardinality Attributes (Array Syntax)
```typeql
# When attribute can have 0+ values, use brackets
match $p isa person;
fetch { "emails": [ $p.email ], "phones": [ $p.phone ] };
```

### Fetch All Attributes
```typeql
# Use sparingly - returns all attributes
match $p isa person;
fetch { "person": $p.* };
```

### Nested Fetch (Subqueries)
```typeql
# Single-value subquery (parentheses)
match $p isa person;
fetch {
  "name": $p.name,
  "movie_count": (match acted_in (actor: $p, film: $m); reduce $c = count($m);)
};

# Multi-value subquery (brackets)
match $p isa person;
fetch {
  "name": $p.name,
  "movies": [
    match acted_in (actor: $p, film: $m);
    fetch { "title": $m.title };
  ]
};
```

## Aggregations with Reduce

### Simple Aggregation
```typeql
match $m isa movie;
reduce $count = count($m);
```

### Grouped Aggregation
```typeql
# Count movies per actor
match
  $p isa person, has name $n;
  acted_in (actor: $p, film: $m);
reduce $count = count($m) groupby $n;
```

### Multiple Groupby Variables
```typeql
match
  $m isa movie, has genre $g;
  $m has released $year;
reduce $count = count($m) groupby $g, $year;
```

### Sorting and Limiting Aggregations
```typeql
match
  $p isa person, has name $n;
  acted_in (actor: $p, film: $m);
reduce $count = count($m) groupby $n;
sort $count desc;
limit 10;
fetch { "actor": $n, "movies": $count };
```

### Available Aggregate Functions

| Function | Purpose | Example |
|----------|---------|---------|
| `count` | Count instances | `reduce $c = count($m);` |
| `sum` | Sum numeric values | `reduce $total = sum($price);` |
| `mean` | Average value | `reduce $avg = mean($rating);` |
| `max` | Maximum value | `reduce $highest = max($score);` |
| `min` | Minimum value | `reduce $lowest = min($score);` |

## Cypher → TypeQL Mapping

| Cypher | TypeQL |
|--------|--------|
| `MATCH (n:Label)` | `match $n isa label;` |
| `MATCH (n {prop: 'val'})` | `match $n isa type, has prop "val";` |
| `MATCH (a)-[:REL]->(b)` | `match rel_type (role1: $a, role2: $b);` |
| `MATCH (a)-[r:REL]->(b)` | `match $r isa rel_type (role1: $a, role2: $b);` |
| `WHERE n.prop = 'val'` | `$n has prop "val";` |
| `WHERE n.prop > 5` | `$n has prop $p; $p > 5;` |
| `WHERE n.prop CONTAINS 'x'` | `$n has prop $p; $p contains "x";` |
| `WHERE n.prop STARTS WITH 'x'` | `$n has prop $p; $p like "^x.*";` |
| `WHERE n.prop ENDS WITH 'x'` | `$n has prop $p; $p like ".*x$";` |
| `WHERE n.prop IS NULL` | `not { $n has prop $p; };` |
| `WHERE n.prop IS NOT NULL` | `$n has prop $p;` (implicit) |
| `WHERE NOT (pattern)` | `not { pattern; };` |
| `WHERE a OR b` | `{ a; } or { b; };` |
| `OPTIONAL MATCH (pattern)` | `try { pattern; };` |
| `RETURN n.prop` | `fetch { "prop": $n.prop };` |
| `RETURN count(n)` | `reduce $count = count($n);` |
| `RETURN count(DISTINCT n)` | `reduce $count = count($n);` (TypeQL counts distinct by default) |
| `ORDER BY n.prop DESC` | `sort $p desc;` (bind prop to $p first) |
| `LIMIT 10` | `limit 10;` |
| `SKIP 5` | `offset 5;` |
| `WITH n, count(m) AS c WHERE c > 5` | Not directly supported - see Known Limitations |
| `UNION` | Use `or { } or { }` pattern |
| `COALESCE(a, b)` | Use `try { }` with default handling |

## Reserved Keywords (Never Use as Type/Role Names)

```
with, match, fetch, update, define, undefine, redefine, insert, put, delete,
end, entity, relation, attribute, role, asc, desc, struct, fun, return,
alias, sub, owns, as, plays, relates, iid, isa, links, has, is, or, not,
try, in, true, false, of, from, first, last
```

If a schema uses reserved words, rename them:
- `in` → `contained_in`, `located_in`
- `from` → `source`, `origin`

## Known Limitations (TypeDB 3.0)

These Cypher patterns cannot be directly converted:

1. **Filter on aggregation result (HAVING equivalent)**
   ```cypher
   -- Cannot convert: filtering after COUNT
   WITH u, count(m) AS movie_count
   WHERE movie_count > 5
   RETURN u
   ```
   Record in `failed.csv` with reason: "Cannot filter on reduce result in same query"

2. **Arithmetic in sort/filter**
   ```cypher
   -- Cannot convert: computed sort value
   ORDER BY (a.followers / a.following)
   ```
   Record in `failed.csv` with reason: "Arithmetic expressions not supported in sort"

3. **String/list length**
   ```cypher
   -- Cannot convert: size() function
   WHERE size(n.name) > 10
   ```
   Record in `failed.csv` with reason: "No size/length function equivalent"

---

## Semantic Conversion Guidelines

### Check Question-Query Alignment

Before finalizing, verify:

1. **Correct entity returned**: Does the question ask for users, tweets, movies? Does the query return that?

2. **Relation direction**:
   - "tweets that HAVE BEEN retweeted" (passive) → tweet is `original_tweet`
   - "tweets that RETWEET others" (active) → tweet is `retweeting_tweet`

3. **All conditions present**: If question says "X AND Y", both must be in query

4. **Correct property**: "retweeted 100 times" should check retweet count, not favorites

5. **OPTIONAL MATCH handling**: Use `try { }` to include results without matches

### Common Mistakes to Avoid

- Using `favorites` when question asks about `retweets`
- Reversing relation direction (who follows whom, who retweeted whom)
- Missing `try { }` for OPTIONAL MATCH patterns
- Returning wrong entity type
- Missing filter conditions from the question

---

### 4. Validate Against TypeDB

```python
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

driver = TypeDB.driver("localhost:1729", Credentials("admin", "password"), DriverOptions(is_tls_enabled=False))

# Database name is text2typeql_<database>
with driver.transaction(f"text2typeql_{database}", TransactionType.READ) as tx:
    result = tx.query(typeql).resolve()
    list(result.as_concept_documents()) or list(result.as_concept_rows())
```

### 5. Retry on Failure (up to 3 attempts)

Read error message carefully, fix TypeQL, retry.

Common fixes:
- Wrong order → reorder to match/sort/limit/fetch
- Role mismatch → check schema for exact role names
- Unknown type → verify entity/relation names in schema
- Syntax error → check quotes, semicolons, brackets

### 6. Write Result

**Success** → Append to `output/<database>/queries.csv`:
```
original_index,question,cypher,typeql
```

**Failure after 3 attempts** → Append to `output/<database>/failed.csv`:
```
original_index,question,cypher,error
```

Create file with header if it doesn't exist.
