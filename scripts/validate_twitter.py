#!/usr/bin/env python3
"""
Validate and semantically review Twitter queries.
"""

import csv
import sys
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# Connection settings
DB_NAME = "text2typeql_twitter"
credentials = Credentials("admin", "password")
options = DriverOptions(is_tls_enabled=False)

# Schema reference for semantic review
SCHEMA = """
Entities: user, me (sub user), tweet, hashtag, link, source
Attributes:
  - user: betweenness, location, followers, following, profile_image_url, screen_name (@key), name, url, statuses
  - tweet: created_at, tweet_id (@key), id_str, text, favorites, import_method
  - hashtag: hashtag_name (@key)
  - link: link_url (@key)
  - source: source_name (@key)
  - similar_to relation: score

Relations:
  - follows: follower -> followed
  - posts: author -> content
  - interacts_with: interactor -> target
  - similar_to: source_user -> similar_user (owns score)
  - mentions: source_tweet -> mentioned_user
  - rt_mentions: mentioner (me only) -> mentioned_user
  - amplifies: amplifier (me only) -> amplified_user
  - using: tweet_content -> platform
  - tags: tagged_tweet -> tag
  - contains: containing_tweet -> contained_link
  - retweets: original_tweet -> retweeting_tweet
  - reply_to: original_tweet -> replying_tweet
"""


def semantic_review(question: str, cypher: str, typeql: str) -> tuple[bool, str]:
    """
    Check if TypeQL query semantically matches the question.
    Returns (is_valid, reason) tuple.

    This performs a conservative review - only flag clear semantic mismatches.
    """
    question_lower = question.lower()
    typeql_lower = typeql.lower()

    # Check for common semantic issues

    # 1. Count/aggregation checks - only when explicitly counting relationships/entities
    # Skip this check if:
    # - Question refers to stored numeric attributes like "followers", "following", "statuses", "favorites"
    # - Query fetches these stored count attributes
    stored_count_attrs = ["followers", "following", "statuses", "favorites", "favorite count", "follower count", "status count"]
    question_uses_stored_attr = any(attr in question_lower for attr in stored_count_attrs)

    # Also check if the query returns a stored count attribute
    returns_count_attr = any(attr in typeql_lower for attr in ["followers", "following", "statuses", "favorites"])

    # Only require reduce/count for actual aggregation needs (counting relationships, not stored attributes)
    count_phrases = ["how many tweets", "how many hashtags", "count of", "total number of tweets",
                     "total number of users", "total tweets", "total users"]
    # "how many users does X follow" is answered by the "following" attribute, not a count
    question_asks_count = any(phrase in question_lower for phrase in count_phrases)
    typeql_has_count = "reduce" in typeql_lower and "count" in typeql_lower

    if question_asks_count and not typeql_has_count and not question_uses_stored_attr and not returns_count_attr:
        return False, "Question asks to count entities but query doesn't use reduce count"

    # 2. Sort direction checks - be more careful about matching
    # Check specifically for "top N" with sort direction
    asks_highest = any(word in question_lower for word in ["highest", "most", "greatest", "best"])
    asks_lowest = any(word in question_lower for word in ["lowest", "least", "fewest", "smallest"])

    # Only flag if there's a clear mismatch
    if asks_highest and not asks_lowest:
        # Question asks for highest but query sorts ascending
        if "sort" in typeql_lower and "asc" in typeql_lower and "desc" not in typeql_lower:
            # Exclude false positives like "top 5 users who have exactly..."
            if "exactly" not in question_lower and "similar" not in question_lower:
                return False, "Question asks for highest/most but query sorts ascending"

    if asks_lowest and not asks_highest:
        # Question asks for lowest but query sorts descending
        if "sort" in typeql_lower and "desc" in typeql_lower and "asc" not in typeql_lower:
            return False, "Question asks for lowest/least but query sorts descending"

    # 3. Check for missing fetch clause
    if "fetch" not in typeql_lower and "reduce" not in typeql_lower:
        return False, "Query missing fetch or reduce clause"

    # 4. Check for specific attribute filtering - case insensitive
    # neo4j check - look for either quoted value or in a like pattern
    # Note: In the Twitter schema, "me" entity IS the Neo4j account, so "$me isa me" is a valid filter for Neo4j
    if "neo4j" in question_lower:
        has_neo4j_filter = (
            '"neo4j"' in typeql_lower or
            "'neo4j'" in typeql_lower or
            'neo4j' in typeql_lower or  # Covers "like" patterns with Neo4j
            'isa me' in typeql_lower    # "me" entity IS the Neo4j account
        )
        if not has_neo4j_filter:
            return False, "Question references 'neo4j' but query doesn't filter for it"

    return True, ""


def validate_query(tx, typeql: str) -> tuple[bool, str]:
    """
    Validate a TypeQL query against the database.
    Returns (is_valid, error_message) tuple.
    """
    try:
        result = tx.query(typeql).resolve()
        # Try to consume results to catch any runtime errors
        if hasattr(result, 'as_concept_documents'):
            list(result.as_concept_documents())
        elif hasattr(result, 'as_value'):
            result.as_value()
        return True, ""
    except Exception as e:
        return False, str(e)


def main():
    input_file = "/opt/text2typeql/output/twitter/queries.csv"
    output_valid = "/opt/text2typeql/output/twitter/queries.csv"
    output_failed = "/opt/text2typeql/output/twitter/failed.csv"
    output_failed_review = "/opt/text2typeql/output/twitter/failed_review.csv"

    # Read all queries
    print(f"Reading queries from {input_file}...")
    queries = []
    with open(input_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            queries.append(row)

    print(f"Loaded {len(queries)} queries")

    # Connect to TypeDB
    print(f"Connecting to TypeDB database '{DB_NAME}'...")
    driver = TypeDB.driver("localhost:1729", credentials, options)

    # Check if database exists
    db_names = [db.name for db in driver.databases.all()]
    if DB_NAME not in db_names:
        print(f"ERROR: Database '{DB_NAME}' not found. Available databases: {db_names}")
        driver.close()
        sys.exit(1)

    valid_queries = []
    failed_validation = []
    failed_review = []

    print("Validating queries...")

    with driver.transaction(DB_NAME, TransactionType.READ) as tx:
        for i, row in enumerate(queries):
            if (i + 1) % 100 == 0:
                print(f"  Processed {i + 1}/{len(queries)} queries...")

            original_index = row.get('original_index', str(i))
            question = row.get('question', '')
            cypher = row.get('cypher', '')
            typeql = row.get('typeql', '')

            # Skip empty queries
            if not typeql.strip():
                failed_validation.append({
                    'original_index': original_index,
                    'question': question,
                    'cypher': cypher,
                    'error': 'Empty TypeQL query'
                })
                continue

            # Step 1: Validate against TypeDB
            is_valid, error = validate_query(tx, typeql)

            if not is_valid:
                failed_validation.append({
                    'original_index': original_index,
                    'question': question,
                    'cypher': cypher,
                    'error': error
                })
                continue

            # Step 2: Semantic review
            is_semantic_valid, review_reason = semantic_review(question, cypher, typeql)

            if not is_semantic_valid:
                failed_review.append({
                    'original_index': original_index,
                    'question': question,
                    'cypher': cypher,
                    'typeql': typeql,
                    'review_reason': review_reason
                })
                continue

            # Query passed both checks
            valid_queries.append(row)

    driver.close()

    # Write results
    print(f"\nWriting results...")

    # Write valid queries
    if valid_queries:
        with open(output_valid, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
            writer.writeheader()
            writer.writerows(valid_queries)
        print(f"  Valid queries: {len(valid_queries)} -> {output_valid}")

    # Write validation failures
    if failed_validation:
        with open(output_failed, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'error'])
            writer.writeheader()
            writer.writerows(failed_validation)
        print(f"  Validation failures: {len(failed_validation)} -> {output_failed}")

    # Write semantic review failures
    if failed_review:
        with open(output_failed_review, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql', 'review_reason'])
            writer.writeheader()
            writer.writerows(failed_review)
        print(f"  Semantic review failures: {len(failed_review)} -> {output_failed_review}")

    # Summary
    print(f"\n{'='*50}")
    print("VALIDATION SUMMARY")
    print(f"{'='*50}")
    print(f"Total queries processed: {len(queries)}")
    print(f"Valid queries:           {len(valid_queries)}")
    print(f"Validation failures:     {len(failed_validation)}")
    print(f"Semantic review failures:{len(failed_review)}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
