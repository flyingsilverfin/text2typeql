# Semantic Analysis: Errors in Original Neo4j Cypher Queries

> **Note:** This document catalogues 38 errors that were manually identified and verified during conversion. A subsequent [automated scan](neo_semantic_analysis/) of all 13,939 query pairs found **597 semantic errors (4.3%)** across six categories — see the [full automated analysis](neo_semantic_analysis/README.md) for the comprehensive results.

This document catalogues errors in the original Neo4j Cypher queries from the [text2cypher](https://github.com/neo4j-labs/text2cypher) benchmark that were discovered during conversion to TypeQL. In each case, the Cypher query does not correctly answer the English question it was generated for — but because Neo4j's property graph model lacks a strong type system, these errors went undetected in the original dataset.

The errors fall into two broad categories:

1. **Semantic errors** (30 in synthetic-1, 2 in synthetic-2): The Cypher is syntactically valid against the Neo4j schema but answers the wrong question — using the wrong property, reversing a relation direction, or returning the wrong entity.
2. **Schema hallucinations** (~10 across synthetic-2): The LLM that generated the Cypher referenced properties or relationships that don't exist in the Neo4j schema. Neo4j's schemaless nature means these queries parse without error but would return empty results or fail at runtime.

## Summary

| Dataset | Database | Semantic Errors | Schema Hallucinations | Total |
|---------|----------|-----------------|-----------------------|-------|
| synthetic-1 | twitter | 14 | — | 14 |
| synthetic-1 | companies | 7 | — | 7 |
| synthetic-1 | twitch | 5 | — | 5 |
| synthetic-1 | recommendations | 4 | — | 4 |
| synthetic-2 | twitter | 2 | — | 2 |
| synthetic-2 | offshoreleaks | — | 3 | 3 |
| synthetic-2 | recommendations | — | 1 | 1 |
| synthetic-2 | network | — | 1 | 1 |
| synthetic-2 | twitch | — | 1 | 1 |
| **Total** | | **32** | **6** | **38** |

Semantic review was completed for both datasets (all 22 databases, 13,939 converted queries).

## Semantic Errors (synthetic-1)

These are cases where the Cypher is valid against the Neo4j schema but does not correctly answer the English question.

### Twitter: Wrong property — `favorites` used instead of retweet count (8 queries)

The Cypher uses `t.favorites > N` when the question asks about retweets. The `favorites` property counts likes, not retweets.

| Index | Question | Cypher Error |
|-------|----------|-------------|
| 49 | "3 most common hashtags in tweets retweeted more than 100 times" | `WHERE t.favorites > 100` — checks favorites, not retweets |
| 73 | "Top 5 tweets retweeted the most times" | Counts tweets that _retweet_ (active) instead of tweets _being retweeted_ (passive) |
| 154 | "Three tweets with highest retweet counts" | `ORDER BY t.favorites DESC` — sorts by favorites |
| 170 | "First 3 tweets retweeted more than 100 times" | `WHERE t.favorites > 100` |
| 281 | "First 3 hashtags in tweets retweeted more than 100 times" | `WHERE t.favorites > 100` |
| 336 | "Top 3 hashtags in tweets retweeted more than 50 times" | `WHERE t.favorites > 50` |
| 429 | "Tweets by neo4j retweeted more than 50 times" | `WHERE tweet.favorites > 50` |
| 468 | "Top 3 tweets from Neo4j retweeted more than 100 times" | `WHERE tweet.favorites > 100` |

**How TypeDB caught it:** TypeQL has no implicit property that conflates favorites and retweets. The `retweets` relation must be explicitly matched and counted, forcing correct semantics.

### Twitter: Wrong traversal direction (5 queries)

Cypher's flexible path syntax allowed queries to return the wrong entity. TypeQL's explicit role names made the direction error visible.

| Index | Question | Cypher Error |
|-------|----------|-------------|
| 75 | "Top 3 users who have amplified the most tweets" | `(u)<-[:AMPLIFIES]-(me)` — finds users amplified BY Me, not users who amplify |
| 212 | "Top 5 tweets with links posted by users following Neo4j" | `(u)-[:FOLLOWS]->(:Me)-[:POSTS]->(t)` — returns tweets by Neo4j, not by followers |
| 219 | "Top 5 users that Neo4j retweeted" | Uses `AMPLIFIES` relation instead of `RETWEETS` |
| 414 | "Tweets from users who follow neo4j" | Same pattern as 212 — returns Neo4j's tweets instead of followers' tweets |
| 432 | "First 3 users who amplified tweets posted by neo4j" | `(tweet)<-[:AMPLIFIES]-(user)` — AMPLIFIES is user-to-user, not user-to-tweet |

**How TypeDB caught it:** Every TypeQL relation requires explicit role assignments (`follower:`, `followed:`, `author:`, `content:`), making traversal direction unambiguous.

### Twitter: Wrong sort criterion (1 query)

| Index | Question | Cypher Error |
|-------|----------|-------------|
| 308 | "Top 5 most recent users followed by Neo4j" | `ORDER BY followed.followers DESC` — sorts by follower count, not recency |

**How TypeDB caught it:** The `follows` relation has an explicit `followed_at` attribute, making temporal ordering straightforward. The mismatch between "most recent" and a follower-count sort was flagged during semantic review.

### Companies: Reversed supplier/customer direction (5 queries)

Neo4j's `HAS_SUPPLIER` relationship is ambiguous: `(A)-[:HAS_SUPPLIER]->(B)` means B is a supplier to A, but the variable naming and traversal direction in several queries reverses the semantics.

| Index | Question | Cypher Error |
|-------|----------|-------------|
| 488 | "Organizations that are suppliers to New Energy Group" | Cypher direction returns NEG's customers, not its suppliers |
| 526 | "Which organizations does New Energy Group supply?" | Cypher gets NEG's suppliers, not organizations NEG supplies to |
| 600 | "3 organizations that are suppliers to New Energy Group" | Same reversed direction |
| 647 | "Organizations that are suppliers to public companies" | Variable naming swaps supplier/customer, filter applies to wrong entity |
| 733 | "Top 3 suppliers of New Energy Group" | Same reversed direction |

**How TypeDB caught it:** The `supplies` relation has explicit `supplier` and `customer` roles. Writing `supplies (supplier: $s, customer: $o)` forces the converter to decide which entity fills which role, making direction errors immediately visible.

### Companies: Semantically invalid questions (2 queries)

| Index | Question | Cypher Error |
|-------|----------|-------------|
| 591 | "Organizations mentioned in articles authored by women" | Cypher filters for `{author: 'David Correa'}` — a man's name. The question and Cypher contradict. |
| 783 | "Organizations that have Accenture as their CEO" | Accenture is an organization, not a person. CEOs must be persons. Semantically invalid. |

**How TypeDB caught it:** Query 783 fails TypeDB's type constraints — the `ceo` role in `ceo_of` requires a `person` entity, not an `organization`. Query 591 was caught during semantic review.

### Twitch: Wrong PLAYS direction (3 queries)

The Cypher assumes `(User)-[:PLAYS]->(Game)` or `(User)-[:PLAYS]->(Stream)`, but in the schema PLAYS goes from Stream to Game. Users do not play games — streams do.

| Index | Question | Cypher Error |
|-------|----------|-------------|
| 26 | "First 5 streams played by more than 5 users" | `(s:Stream)<-[:PLAYS]-(u:User)` — PLAYS goes Stream→Game, not User→Stream |
| 227 | "Streams played by users with name containing 'doduik'" | Same incorrect direction |
| 533 | "Streams with same game played by itsbigchase and 9linda" | `(u1:User)-[:PLAYS]->(g:Game)` — users don't play games, streams do |

**How TypeDB caught it:** The `game_play` relation has explicit roles `streaming_channel` (Stream) and `played_game` (Game). A User entity cannot fill the `streaming_channel` role — the type system rejects it.

### Twitch: Wrong entity type constraints (2 queries)

| Index | Question | Cypher Error |
|-------|----------|-------------|
| 207 | "Streams with chatters who are also streams" | `WHERE exists{ (chatter:Stream) }` — checks if a User is also a Stream |
| 526 | "Streams that are VIPs in stream 'itsbigchase'" | `(s:Stream)-[:VIP]->(stream:Stream)` — VIP requires a User, not a Stream |

**How TypeDB caught it:** TypeQL role constraints enforce that only `user` entities can fill `vip_user` and only `stream` entities can fill `channel_with_vips`.

### Recommendations: Wrong counting target (1 query)

| Index | Question | Cypher Error |
|-------|----------|-------------|
| 28 | "First 5 movies rated by users from more than 10 different countries" | `count(DISTINCT u.name) AS numCountries` — counts distinct user names, not distinct countries. Variable misleadingly named `numCountries`. |

**How TypeDB caught it:** The TypeQL schema has an explicit `located_in` relation connecting users to countries. Counting a generic property like `u.name` and calling it "countries" is not possible when the type system requires matching the actual `country` entity.

### Recommendations: Hardcoded workaround (1 query)

| Index | Question | Cypher Error |
|-------|----------|-------------|
| 624 | "Movies rated by both male and female users" | Hardcodes `{name: 'Omar Huffman'}` and `{name: 'Myrtle Potter'}` instead of filtering by gender attribute. |

**How TypeDB caught it:** The TypeQL schema has an explicit `gender` attribute on User. The correct query filters by `has gender "male"` / `has gender "female"`.

### Recommendations: Non-existent attribute (2 queries)

| Index | Question | Cypher Error |
|-------|----------|-------------|
| 22 | "Top 3 movies rated by the oldest users" | `ORDER BY u.born ASC` — User entity has no `born` attribute in the schema |
| 242 | "First 3 movies rated by the youngest users" | `ORDER BY u.born DESC` — same non-existent attribute |

**How TypeDB caught it:** TypeDB type-checks every attribute access against the schema. `User` cannot have `born` unless explicitly declared.

## Semantic Errors (synthetic-2)

### Twitter: Wrong property (2 queries)

| Index | Question | Cypher Error |
|-------|----------|-------------|
| 23 | (wrong property for sorting) | Same `favorites` vs retweets pattern as synthetic-1 |
| 66 | (wrong property for filtering) | Same pattern |

These were successfully converted using TypeQL that correctly implements the English question's intent.

## Schema Hallucinations (synthetic-2)

These are cases where the LLM that generated the Cypher referenced properties or relationships that don't exist in the Neo4j schema. In a schemaless database like Neo4j, these queries parse without error but would return empty results at runtime.

| Database | Index | Question | Hallucinated Element |
|----------|-------|----------|---------------------|
| twitch | 131 | "Streams with VIP relationship for over 3 years" | `r.since` property on VIP relationship (no relation properties in schema) |
| recommendations | 733 | "Users with birth date after 1980" | `u.born` on User entity (only Person has `born`) |
| network | 623 | "Interface details" | `interface.name` attribute (does not exist in schema) |
| offshoreleaks | 41 | "Entities similar to 'Top Games Holdings Inc.'" | `(Entity)-[:similar]->(Other)` — Other doesn't play `similar` roles |
| offshoreleaks | 61 | "Entities similar to 'NINGBO SUNRISE...'" | Same hallucinated relation |
| offshoreleaks | 472 | "Entities similar to 'NINGBO SUNRISE...'" | Same hallucinated relation |

**How TypeDB caught these:** TypeDB validates every attribute access and relation role against the schema at query time. An entity cannot own an attribute or play a role unless explicitly declared in the schema definition.

## Error Categories

| Category | s1 | s2 | Total | Description |
|----------|----|----|-------|-------------|
| Wrong property | 8 | 2 | 10 | Using a different attribute than the question asks about |
| Wrong direction | 10 | — | 10 | Reversing relation traversal direction |
| Non-existent attribute | 2 | 2 | 4 | Accessing attributes not declared on the entity type |
| Schema type mismatch | 2 | 3 | 5 | Using wrong entity type in a relation role |
| Data/question contradiction | 2 | — | 2 | Cypher logic contradicts the English question |
| Hardcoded workaround | 1 | — | 1 | Substituting specific data values for schema-level filtering |
| Wrong sort criterion | 1 | — | 1 | Sorting by wrong attribute for the question's intent |
| Hallucinated relation | — | 3 | 3 | Referencing relations that don't exist in the schema |
| Hallucinated relation property | — | 1 | 1 | Referencing properties on relations that have none |
| **Total** | **26** | **11** | **37** | |

Note: The 4 recommendations/s1 non-existent attribute errors overlap with the "schema hallucination" category but are classified separately because they appear in a dataset where the schema was more ambiguous (Person vs User types).

## Why TypeDB Catches These

TypeDB's type system enforces constraints at three levels that Neo4j's property graph does not:

1. **Role-based relations.** Every relation in TypeQL requires explicit role assignments (`supplies (supplier: $x, customer: $y)`). This makes direction errors immediately visible — you must decide which entity is the supplier and which is the customer. In Cypher, `(a)-[:HAS_SUPPLIER]->(b)` leaves the semantic direction ambiguous.

2. **Schema-validated attribute access.** TypeDB type-checks every `has` clause against the schema. If `User` doesn't own `born`, the query fails at validation time. Neo4j's schemaless model allows accessing any property on any node — returning `null` silently if the property doesn't exist.

3. **Entity-role type constraints.** TypeDB enforces which entity types can play which roles. A `Stream` cannot fill a `vip_user` role — only `User` can. Neo4j has no equivalent constraint; any node can participate in any relationship type.

These constraints don't just prevent bugs in TypeQL — they surface bugs in the *source data* by forcing the converter to make explicit decisions that Cypher leaves implicit.
