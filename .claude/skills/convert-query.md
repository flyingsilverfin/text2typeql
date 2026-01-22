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

**Role inference (omit roles to match all permutations):**
```typeql
# Matches $c in ANY role - auto-fills all possible role combinations
$rel isa interacts ($c);

# Equivalent to: interacts (character1: $c) OR interacts (character2: $c)
# Use when you don't care which role the player has

# Symmetric/bidirectional - omit roles for BOTH players
subsidiary_of ($o1, $o2);
# Matches: (parent: $o1, subsidiary: $o2) OR (parent: $o2, subsidiary: $o1)
# Replaces verbose: { subsidiary_of (parent: $o1, subsidiary: $o2); } or { subsidiary_of (parent: $o2, subsidiary: $o1); };

# Explicit role type checking (when you need to filter by role)
$rel isa interacts ($role: $c);
{ $role sub interacts:character1; } or { $role sub interacts:character2; };
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

**CRITICAL - Variable Scoping in Disjunctions:**

Variables inside disjunction branches are **scoped** and not returned outside. This affects counting:

```typeql
# WRONG - nothing to count, $rel not accessible outside disjunction
{ interacts (character1: $c); } or { interacts (character2: $c); };
reduce $count = count($rel) groupby $comm;  # Error: $rel undefined

# ALSO WRONG - $rel inside branches is STILL scoped!
{ $rel isa interacts ($c); } or { $rel isa interacts2 ($c); };
reduce $count = count($rel) groupby $comm;  # $rel still scoped to branches!

# RIGHT - single type: bind outside disjunction
$rel isa interacts ($c);
reduce $count = count($rel) groupby $comm;  # Works

# RIGHT - multiple types: use TYPE VARIABLE with $rel outside
$rel isa $t ($c);
{ $t label interacts; } or { $t label interacts1; } or { $t label interacts2; };
reduce $count = count($rel) groupby $comm;  # Works - $rel bound outside
```

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

## Advanced TypeQL Features

### Custom Functions (`with fun`)

Define reusable query logic for complex computations:

```typeql
# Count distinct users who retweeted a tweet
with fun retweeting_users($tweet: tweet) -> integer:
  match
    retweets (original_tweet: $tweet, retweeting_tweet: $retweet);
    posts ($user, $retweet);
  select $user;
  distinct;
  return count;
match
  $me isa me, has screen_name "neo4j";
  posts ($me, $tweet);
  let $count = retweeting_users($tweet);
  $count > 5;
fetch { "tweet": $tweet.text, "retweet_count": $count };
```

```typeql
# Compute follower/following ratio using functions
with fun follower_count($user: user) -> integer:
  match follows (followed: $user);
  return count;
with fun follows_count($user: user) -> integer:
  match follows (follower: $user);
  return count;
match
  $user isa user;
  let $followers = follower_count($user);
  let $follows = follows_count($user);
  $followers > 0;
  let $ratio = $follows / $followers;
sort $ratio desc;
limit 10;
fetch { "user": $user.name, "ratio": $ratio };
```

### Select + Distinct (COUNT DISTINCT equivalent)

Use `select` and `distinct` to deduplicate before counting:

```typeql
# Count DISTINCT cities per country (Cypher: count(DISTINCT city))
match
  $c isa country;
  $city isa city;
  $o isa organization, has is_public true;
  location-contains (parent: $c, child: $city);
  located_in (organization: $o, city: $city);
select $c, $city;
distinct;
reduce $city_count = count groupby $c;
sort $city_count desc;
limit 3;
fetch { "country": $c.country_name, "num_cities": $city_count };
```

This is cleaner than chained reduces for distinct counting.

### Chained Reduce Stages (HAVING equivalent)

Use `reduce ... match ...` to filter on aggregation results:

```typeql
# Find hashtags appearing in more than 100 retweeted tweets
match
  $tweet isa tweet;
  retweets (original_tweet: $tweet, retweeting_tweet: $retweet);
reduce $count = count groupby $tweet;
match
  $count > 100;
  tags (tagged_tweet: $tweet, tag: $hashtag);
reduce $hashtag_count = count groupby $hashtag;
sort $hashtag_count desc;
limit 3;
fetch { "hashtag": $hashtag.hashtag_name, "count": $hashtag_count };
```

```typeql
# Tweets with favorites + retweet count combined
match
  $tweet isa tweet;
  retweets (original_tweet: $tweet);
reduce $retweets = count groupby $tweet;
match
  $tweet has favorites $favorites;
  let $total = $favorites + $retweets;
sort $total desc;
limit 3;
fetch { "tweet": $tweet.text, "total": $total };
```

### Let Expressions

Compute values inline with `let`:

```typeql
# Compute difference for similarity ranking
match
  $neo4j isa me, has betweenness $neo4j_betweenness;
  $user isa user, has betweenness $betweenness;
  not { $neo4j is $user; };
  let $difference = abs($betweenness - $neo4j_betweenness);
sort $difference asc;
limit 5;
fetch { "user": $user.name, "difference": $difference };
```

### Type Variables (Polymorphic Queries)

Match multiple relation types dynamically:

```typeql
# Count all interactions (mentions, retweets, replies) on a tweet
match
  $tweet isa tweet;
  $rel isa $t;
  {
    $t label mentions;
    $rel links (source_tweet: $tweet);
  } or {
    $t label retweets;
    $rel links (original_tweet: $tweet);
  } or {
    $t label reply_to;
    $rel links (original_tweet: $tweet);
  };
reduce $count = count groupby $tweet;
sort $count desc;
limit 5;
fetch { "tweet": $tweet.text, "interactions": $count };
```

### Arithmetic Expressions

Supported operations: `+`, `-`, `*`, `/`, `abs()`

```typeql
let $total = $favorites + $retweets;
let $ratio = $follows / $followers;
let $difference = abs($a - $b);
```

---

## Known Limitations (TypeDB 3.0)

These patterns require advanced features shown above:

| Cypher Pattern | TypeQL Solution |
|----------------|-----------------|
| `WITH x, count(y) WHERE count > N` | Chained reduce: `reduce ... match $count > N;` |
| `ORDER BY a / b` | Let expression: `let $ratio = $a / $b; sort $ratio;` |
| `count(DISTINCT ...)` with filter | Custom function with `distinct; return count;` |
| Multiple OPTIONAL MATCH counts | Type variables with `or` blocks |

**Still unsupported:**
- `size()` for string/list length - Record in `failed.csv`
- Date arithmetic with duration - Record in `failed.csv`
- `collect()` and array operations - Record in `failed.csv`

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

### 5. Semantic Review (REQUIRED)

**After TypeDB validation passes**, perform a semantic review comparing the **English question** to the **TypeQL query**. Do NOT look at the Cypher query during this review.

#### Semantic Review Checklist

Ask yourself these questions:

1. **Does the TypeQL return the right entity type?**
   - Question asks for "users" → query should return users, not tweets
   - Question asks for "articles" → query should return articles, not organizations

2. **Does the TypeQL include ALL conditions from the question?**
   - "public organizations with 50+ employees" → needs BOTH `is_public true` AND `nbr_employees >= 50`
   - "dissolved companies mentioned in articles" → needs `is_dissolved true` AND `mentions` relation

3. **Is the aggregation correct?**
   - "top 3 countries with most cities" → needs `reduce count groupby`, `sort desc`, `limit 3`
   - "how many users" → needs `reduce count`
   - If question asks for "top N by count of X", there MUST be a `reduce ... count ... groupby`

4. **Are relation directions correct?**
   - "suppliers OF company X" → `supplies (supplier: $s, customer: $x)` where $x is Company X
   - "companies THAT SUPPLY X" → same pattern, but return $s
   - "tweets RETWEETED BY users" → tweet is `original_tweet`
   - "tweets THAT RETWEET others" → tweet is `retweeting_tweet`

5. **Are numeric thresholds correct?**
   - "revenue over 1 million" → `$revenue > 1000000` not `$revenue > 1`
   - "more than 5 employees" → `$emp > 5`

6. **Is sorting/ordering correct?**
   - "highest revenue" → `sort $revenue desc`
   - "most similar" (smallest difference) → `sort $diff asc`

#### If Semantic Review Fails

If any check fails, the TypeQL does NOT correctly answer the question. Go back to step 3 and rewrite the query. This counts as one of your 3 retry attempts.

**Common semantic failures:**
- Missing `reduce ... groupby` for ranking/counting questions
- Wrong relation direction (who supplies whom, who follows whom)
- Missing filter conditions from the question
- Returning wrong entity type
- Wrong boolean value (`is_dissolved true` vs `false`)
- Completely unrelated query (generated from wrong understanding)

### 6. Retry on Failure (up to 3 attempts)

Retry if:
- TypeDB validation fails (syntax/type errors)
- Semantic review fails (query doesn't match question)

Read error message carefully, fix TypeQL, retry.

Common fixes:
- Wrong order → reorder to match/sort/limit/fetch
- Role mismatch → check schema for exact role names
- Unknown type → verify entity/relation names in schema
- Syntax error → check quotes, semicolons, brackets
- Semantic mismatch → reread question, rewrite query from scratch

### 7. Write Result

**Success** (validated AND passed semantic review) → Append to `output/<database>/queries.csv`:
```
original_index,question,cypher,typeql
```

**Failure after 3 attempts** → Append to `output/<database>/failed.csv`:
```
original_index,question,cypher,error
```

Create file with header if it doesn't exist.
