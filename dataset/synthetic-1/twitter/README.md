# Twitter Dataset

**Source:** `synthetic_opus_demodbs` (Neo4j text2cypher)

**Total queries in original dataset: 493**

## Current Status
- `queries.csv`: 491 successfully converted and validated queries
- 2 queries that cannot be converted (documented below in Failed Queries)

Total: 491 + 2 = 493 ✓

## Schema Extensions
- `follows` relation has `followed_at` datetime attribute (enables temporal ordering of follow relationships)

## Failed Queries (2 total)

### Query 222
**Reason:** Schema limitation — only `me` entity can play `amplifier` role in `amplifies`, but this query requires regular users as amplifiers.
```cypher
MATCH (me:Me {name: 'Neo4j'})<-[:AMPLIFIES]-(u:User)
WHERE u.following > 5000
RETURN u.name, u.screen_name, u.followers
ORDER BY u.followers DESC
LIMIT 5
```

### Query 285
**Reason:** No fuzzy string matching functions in TypeQL. The question asks for users with similar screen names (text similarity), but TypeQL lacks Levenshtein distance, soundex, or similar functions. The `similar_to` relation measures general user similarity, not screen name similarity.
```cypher
MATCH (me:Me {screen_name: 'neo4j'})-[s:SIMILAR_TO]->(u:User)
RETURN u.screen_name AS similar_user, s.score AS similarity
ORDER BY similarity DESC
LIMIT 5
```

## Original Cypher Errors

During conversion, TypeDB's strict schema enforcement caught cases where the original Cypher query did not correctly answer the English question. In each case the TypeQL was written to match the question intent rather than transliterate the Cypher.

### Wrong property: `favorites` used instead of retweet count (8 queries)

The Cypher dataset uses `t.favorites > N` when the English question asks about retweets. The `favorites` property counts likes, not retweets. TypeQL correctly counts retweet relation instances instead.

| Index | Question | Cypher error |
|-------|----------|-------------|
| 49 | "3 most common hashtags in tweets retweeted more than 100 times" | `WHERE t.favorites > 100` — checks favorites, not retweets |
| 154 | "Three tweets with highest retweet counts" | `ORDER BY t.favorites DESC` — sorts by favorites |
| 170 | "First 3 tweets retweeted more than 100 times" | `WHERE t.favorites > 100` |
| 281 | "First 3 hashtags in tweets retweeted more than 100 times" | `WHERE t.favorites > 100` |
| 336 | "Top 3 hashtags in tweets retweeted more than 50 times" | `WHERE t.favorites > 50` |
| 429 | "Tweets by neo4j retweeted more than 50 times" | `WHERE tweet.favorites > 50` |
| 468 | "Top 3 tweets from Neo4j retweeted more than 100 times" | `WHERE tweet.favorites > 100` |
| 73 | "Top 5 tweets retweeted the most times" | Counts tweets that _retweet_ (active) instead of tweets _being retweeted_ (passive) |

**How TypeDB caught it:** TypeQL has no implicit property that conflates favorites and retweets. The `retweets` relation must be explicitly matched and counted, forcing the correct semantics.

### Wrong traversal direction (5 queries)

Cypher's undirected/implicit path traversal allowed queries to return the wrong entity. TypeQL's explicit role names made the direction error visible.

| Index | Question | Cypher error |
|-------|----------|-------------|
| 212 | "Top 5 tweets with links posted by users following Neo4j" | `(u)-[:FOLLOWS]->(:Me)-[:POSTS]->(t)` — returns tweets by Neo4j, not by followers |
| 414 | "Tweets from users who follow neo4j" | Same pattern — returns Neo4j's tweets instead of followers' tweets |
| 219 | "Top 5 users that Neo4j retweeted" | Uses `AMPLIFIES` relation instead of `RETWEETS` |
| 432 | "First 3 users who amplified tweets posted by neo4j" | `(tweet)<-[:AMPLIFIES]-(user)` — AMPLIFIES is user-to-user, not user-to-tweet |
| 75 | "Top 3 users who have amplified the most tweets" | `(u)<-[:AMPLIFIES]-(me)` — finds users amplified BY Me, not users who amplify |

**How TypeDB caught it:** Every TypeQL relation requires explicit role assignments (`follower:`, `followed:`, `author:`, `content:`), making traversal direction unambiguous. Writing `posts (author: $me, content: $t)` vs `posts (author: $u, content: $t)` forces the converter to choose which entity is the author.

### Wrong sort criterion (1 query)

| Index | Question | Cypher error |
|-------|----------|-------------|
| 308 | "Top 5 most recent users followed by Neo4j" | `ORDER BY followed.followers DESC` — sorts by follower count, not recency |

**How TypeDB caught it:** The `follows` relation has an explicit `followed_at` attribute, making temporal ordering straightforward. The mismatch between "most recent" and a follower-count sort was flagged during semantic review.
