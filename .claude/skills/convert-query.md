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

TypeQL 3.0 reference:

---

## TypeQL 3.0 Query Structure

TypeQL uses a pipeline architecture with stages executed in order:

```
match <pattern>; → [sort $var;] → [limit N;] → fetch { } | reduce $var = func();
```

### Pipeline Stages

| Stage | Purpose | Example |
|-------|---------|---------|
| `match` | Locate data patterns | `match $p isa person;` |
| `sort` | Order results | `sort $age desc;` |
| `limit` | Restrict count | `limit 10;` |
| `offset` | Skip results | `offset 5;` |
| `fetch` | Return JSON structure | `fetch { "name": $n };` |
| `reduce` | Aggregate values | `reduce $count = count($p);` |

**Critical**: Order must be `match` → `sort` → `limit` → `fetch`/`reduce`

### Pattern Matching

```typeql
# Type assertion
$x isa person;

# Attribute ownership
$x has name "John";
$x has age $a;

# Relations - TypeDB 3.0 syntax (PREFERRED when not accessing relation attributes)
friendship (friend: $x, friend: $y);
acted_in (actor: $p, film: $m);

# Relations - bind to variable (use when you need relation attributes)
$rel isa acted_in (actor: $p, film: $m);
$rel has role_name $role;

# Value comparisons (on bound variables)
$a > 25;
$name like "%Smith%";
```

### Logical Operators

```typeql
# Conjunction (implicit via semicolons)
$p isa person, has name $n, has age $a;

# Disjunction
{ $x has status "active"; } or { $x has status "pending"; };

# Negation
not { (actor: $p) isa acted_in; };

# Optionality
try { $p has nickname $nick; };
```

### Comparison Operators

| Operator | Purpose |
|----------|---------|
| `==`, `!=` | Value equality |
| `<`, `<=`, `>`, `>=` | Ordering |
| `like` | Pattern matching (`%` wildcard) |
| `contains` | Substring check |
| `is` | Concept identity (same instance) |

### Fetch Patterns

```typeql
# Fetch bound variables (requires: has name $n in match)
fetch { "name": $n, "age": $a };

# Fetch directly from entity using dot notation (PREFERRED - no need to bind)
fetch { "title": $m.title, "year": $m.released };

# For multi-cardinality attributes (can have 0+ values), use array syntax
fetch { "emails": [ $p.email ] };

# Fetch all attributes (use sparingly)
fetch { "person": $p.* };
```

**Tip**: Prefer `$entity.attribute` over binding with `has attribute $var` when you just need to return the value. Only bind to a variable when you need to filter/sort on it.

### Aggregations with Reduce

```typeql
# Simple count
match $m isa movie;
reduce $count = count($m);

# Grouped aggregation
match $p isa person; (actor: $p, film: $m) isa acted_in;
reduce $count = count($m) groupby $p;

# With sorting (after reduce)
reduce $count = count($m) groupby $name;
sort $count desc;
limit 5;
```

Available functions: `count`, `sum`, `mean`, `max`, `min`

### Cypher → TypeQL Mapping

| Cypher | TypeQL |
|--------|--------|
| `MATCH (n:Label)` | `match $n isa label;` |
| `MATCH (n {prop: 'val'})` | `match $n isa type, has prop "val";` |
| `MATCH (a)-[:REL]->(b)` | `match rel (role1: $a, role2: $b);` |
| `WHERE n.prop = 'val'` | `has prop "val";` or `has prop $p; $p == "val";` |
| `WHERE n.prop > 5` | `has prop $p; $p > 5;` |
| `WHERE n.prop CONTAINS 'x'` | `has prop $p; $p like "%x%";` |
| `WHERE n.prop IS NULL` | `not { $n has prop $p; };` |
| `WHERE NOT (pattern)` | `not { pattern; };` |
| `RETURN n.prop` | `fetch { "prop": $n.prop };` (direct) or `has prop $p; fetch { "prop": $p };` (if filtering) |
| `RETURN count(n)` | `reduce $count = count($n);` |
| `ORDER BY n.prop DESC` | `sort $p desc;` (bind prop to $p first) |
| `LIMIT 10` | `limit 10;` |
| `WITH n, count(m) AS c` | `reduce $c = count($m) groupby $n;` |

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
