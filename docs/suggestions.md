# Handwritten TypeQL Suggestions for Failed Queries

These are manually crafted TypeQL queries for Cypher queries that couldn't be auto-converted due to TypeDB limitations. These use advanced TypeQL features like custom functions, chained reduce stages, and type variables.

**All queries validated against TypeDB 3.0** ✓

---

## Twitter Dataset

### Index 136

**Question:** List the tweets by 'neo4j' that have been retweeted by more than 5 different users.

**Cypher:**
```cypher
MATCH (me:Me {screen_name: 'neo4j'})-[:POSTS]->(tweet:Tweet)
WHERE count{(tweet)<-[:RETWEETS]-(:Tweet)<-[:POSTS]-(:User)} > 5
RETURN tweet.text, count{(tweet)<-[:RETWEETS]-(:Tweet)<-[:POSTS]-(:User)} AS retweets
```

**TypeQL (validated):**
```typeql
with fun retweeting_users($tweet: tweet) -> integer:
  match
    retweets (original_tweet: $tweet, retweeting_tweet: $retweet);
    posts ($user, $retweet);
  select $user;
  distinct;
  return count;
match
$u isa user, has screen_name 'neo4j';
posts ($user, $tweet);
let $retweeting_users = retweeting_users($tweet);
$retweeting_users > 5;
fetch {
  "tweet": $tweet.text
};
```

---

### Index 295

**Question:** Show all hashtags that have appeared in more than 5 tweets mentioning 'Neo4j'.

**Cypher:**
```cypher
MATCH (t:Tweet)-[:MENTIONS]->(:User {screen_name: 'neo4j'})-[:POSTS]->(tweet:Tweet)-[:TAGS]->(h:Hashtag)
WITH h, count(tweet) AS tweet_count
WHERE tweet_count > 5
RETURN h.name AS hashtag, tweet_count
ORDER BY tweet_count DESC
```

**TypeQL (validated):**
```typeql
match
  $tweet isa tweet, has text $text;
  $text like ".*Neo4j.*";
  tags (tagged_tweet: $tweet, tag: $tag);
reduce $count = count groupby $tag;
match
  $count > 5;
fetch {
  "tag": $tag.hashtag_name,
  "appearances_with_neo4j": $count
};
```

*Fix applied: Changed `contains text` to `has text`*

---

### Index 296

**Question:** List the top 5 users who have a similar betweenness to 'Neo4j'.

**Cypher:**
```cypher
MATCH (me:Me {name: 'Neo4j'}), (u:User)
WHERE u.betweenness IS NOT NULL
WITH me, u
ORDER BY abs(me.betweenness - u.betweenness)
LIMIT 5
RETURN u.name AS user, u.betweenness AS betweenness
```

**TypeQL (validated):**
```typeql
match
$neo4j isa me, has name "Neo4j", has betweenness $neo4j_betweenness;
$user isa user, has betweenness $betweenness;
not { $neo4j is $user; };
let $difference = abs($betweenness - $neo4j_betweenness);
sort $difference asc;
limit 5;
fetch {
  "user": $user.name
};
```

*Fixes applied: Changed hyphenated variable `$neo4j-betweenness` to `$neo4j_betweenness`, changed `sort desc` to `sort asc` (smallest difference = most similar)*

---

### Index 330

**Question:** Which users have the highest following to follower ratio?

**Cypher:**
```cypher
MATCH (u:User)
WHERE u.followers IS NOT NULL AND u.following IS NOT NULL
RETURN u.screen_name AS user,
       toFloat(u.following) / u.followers AS followingToFollowerRatio
ORDER BY followingToFollowerRatio DESC
LIMIT 10
```

**TypeQL (validated):**
```typeql
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
$follows > 0;
let $ratio = $follows / $followers;
sort $ratio desc;
limit 10;
fetch {
  "user": $user.name,
  "ratio": $ratio
};
```

---

### Index 49

**Question:** What are the 3 most common hashtags in tweets that have been retweeted more than 100 times?

**Notes:** The Neo4j query is wrong (uses favorites instead of retweet count)!

**Cypher:**
```cypher
MATCH (t:Tweet)-[:TAGS]->(h:Hashtag)
WHERE t.favorites > 100
RETURN h.name AS hashtag, count(*) AS count
ORDER BY count DESC
LIMIT 3
```

**TypeQL (validated):**
```typeql
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
fetch {
  "hashtag": $hashtag.hashtag_name,
  "count": $hashtag_count
};
```

---

### Index 81

**Question:** List the top 5 tweets by the total interaction (mentions, retweets, replies).

**Cypher:**
```cypher
MATCH (t:Tweet)
OPTIONAL MATCH (t)<-[:MENTIONS]-(mention)
OPTIONAL MATCH (t)<-[:RETWEETS]-(retweet)
OPTIONAL MATCH (t)<-[:REPLY_TO]-(reply)
RETURN t.text AS tweet,
       count(mention) AS mentions,
       count(retweet) AS retweets,
       count(reply) AS replies,
       (count(mention) + count(retweet) + count(reply)) AS totalInteractions
ORDER BY totalInteractions DESC
LIMIT 5
```

**TypeQL (validated):**
```typeql
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
fetch {
  "tweet": $tweet.text,
  "count": $count
};
```

*Fix applied: Added `groupby $tweet` to reduce*

---

### Index 85

**Question:** Which 3 tweets have the highest aggregation of favorites and retweets?

**Cypher:**
```cypher
MATCH (t:Tweet)
OPTIONAL MATCH (t)-[:RETWEETS]->(r:Tweet)
RETURN t, t.favorites + count(r) AS score
ORDER BY score DESC
LIMIT 3
```

**TypeQL (validated):**
```typeql
match
$tweet isa tweet;
retweets (original_tweet: $tweet);
reduce $retweets = count groupby $tweet;
match
$tweet has favorites $favorites;
let $total = $favorites + $retweets;
sort $total desc;
limit 3;
fetch {
  "tweet": $tweet.text,
  "total": $total
};
```

---

## Key TypeQL Features Used

1. **Custom functions (`with fun`)** - Define reusable query logic (indices 136, 330)
2. **Chained reduce stages** - `reduce ... match ... reduce ...` for filtering on aggregates (indices 49, 85, 295)
3. **Let expressions** - `let $var = expression` for computed values (indices 85, 296, 330)
4. **Type variables** - `$rel isa $t; $t label typename;` for polymorphic queries (index 81)
5. **Disjunction** - `{ pattern } or { pattern }` for multiple match options (index 81)
6. **Arithmetic** - `$a + $b`, `$a / $b`, `abs($a - $b)` for computed values (indices 85, 296, 330)
7. **Role inference** - `$rel isa relation ($player);` matches player in ANY role (all permutations)
8. **Tuple groupby** - `reduce $c = count groupby $a, $b;` for grouping by multiple variables

## Important Scoping Rules

**Variables inside disjunction branches are scoped and NOT returned outside:**

```typeql
# WRONG - $rel not accessible, nothing to count
{ interacts (character1: $c); } or { interacts (character2: $c); };
reduce $count = count($rel) groupby $comm;  # Error!

# ALSO WRONG - $rel inside branches is STILL scoped!
{ $rel isa interacts ($c); } or { $rel isa interacts2 ($c); };
reduce $count = count($rel) groupby $comm;  # $rel still scoped!

# RIGHT - single type: bind outside
$rel isa interacts ($c);
reduce $count = count($rel) groupby $comm;  # Works

# RIGHT - multiple types: use TYPE VARIABLE
$rel isa $t ($c);
{ $t label interacts; } or { $t label interacts1; };
reduce $count = count($rel) groupby $comm;  # Works - $rel bound outside
```

**Role inference - omit roles to match all possible role combinations:**

```typeql
# Matches $c in character1 OR character2 role (all permutations)
$rel isa interacts ($c);

# Symmetric/bidirectional - omit roles for BOTH players
subsidiary_of ($o1, $o2);
# Matches: (parent: $o1, subsidiary: $o2) OR (parent: $o2, subsidiary: $o1)
# Much simpler than: { subsidiary_of (parent: $o1, ...); } or { subsidiary_of (parent: $o2, ...); };

# Explicit role type checking when needed
$rel isa interacts ($role: $c);
{ $role sub interacts:character1; } or { $role sub interacts:character2; };
```

## Validation Status

| Index | Status | Notes |
|-------|--------|-------|
| 136 | ✓ Validated | Custom function for counting distinct users |
| 295 | ✓ Validated | Fixed `contains` to `has` |
| 296 | ✓ Validated | Fixed variable naming, sort direction |
| 330 | ✓ Validated | Custom functions for follower/following counts |
| 49 | ✓ Validated | Chained reduce for HAVING-style filter |
| 81 | ✓ Validated | Type variables with disjunction |
| 85 | ✓ Validated | Chained reduce with arithmetic |
