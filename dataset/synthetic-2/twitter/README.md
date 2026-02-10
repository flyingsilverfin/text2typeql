# Twitter Dataset

**Source:** `synthetic_gpt4o_demodbs` (Neo4j text2cypher)

**Total valid queries: 502**

Tweets, users, follows, retweets, mentions.

## Current Status
- `queries.csv`: 500 converted queries
- 2 failed queries (semantic mismatches in source Cypher)

Total: 500 + 2 = 502 ✓

## Failed Queries

### Query 23
**Question:** List the names of the top 5 users who have the highest scores of being similar to 'Neo4j'.
```cypher
MATCH (me:Me {screen_name: 'neo4j'})-[:SIMILAR_TO]->(user:User)
RETURN user.name, user.screen_name, user.betweenness
ORDER BY user.betweenness DESC
LIMIT 5
```
**Reason:** Question asks for highest scores of being similar (similar_to relation has score attribute) but Cypher orders by user.betweenness instead of the relation score. Semantic mismatch in original Cypher.

### Query 66
**Question:** Show the first 3 hashtags tagged in tweets that have been retweeted more than 100 times.
```cypher
MATCH (t:Tweet)-[:TAGS]->(h:Hashtag)
WHERE t.favorites > 100
RETURN h.name
LIMIT 3
```
**Reason:** Question asks for tweets retweeted more than 100 times but Cypher filters on favorites > 100 instead of counting retweets relation. Semantic mismatch in original Cypher.
