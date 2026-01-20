# TODO

## Schema Naming Convention Fix

TypeDB convention is to not indicate the receiving type in role names. Roles should be short and generic.

**Current (incorrect):**
```typeql
relation contains,
  relates containing_tweet,
  relates contained_link;

relation retweets,
  relates original_tweet,
  relates retweeting_tweet;

relation reply_to,
  relates original_tweet,
  relates replying_tweet;
```

**Should be:**
```typeql
relation contains,
  relates containing,
  relates contained;

relation retweets,
  relates original,
  relates retweeting;

relation reply_to,
  relates original,
  relates replying;
```

**Affected schemas:** All schemas need review for this convention.

**Impact:** Would need to update all generated TypeQL queries to use the new role names.
