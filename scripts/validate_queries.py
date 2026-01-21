#!/usr/bin/env python3
"""
Validate and semantically review TypeQL queries in the movies database.
"""

import csv
import re
import sys
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# TypeDB connection settings
CREDENTIALS = Credentials("admin", "password")
OPTIONS = DriverOptions(is_tls_enabled=False)
DATABASE = "text2typeql_movies"

# File paths
INPUT_FILE = "/opt/text2typeql/output/movies/queries.csv"
OUTPUT_VALID = "/opt/text2typeql/output/movies/queries.csv"
OUTPUT_FAILED = "/opt/text2typeql/output/movies/failed.csv"
OUTPUT_FAILED_REVIEW = "/opt/text2typeql/output/movies/failed_review.csv"

# Schema reference for semantic review
SCHEMA = """
Entities:
- person: owns name @key, born; plays actor, director, producer, writer, follower, followed, reviewer
- movie: owns title @key, votes, tagline, released; plays film (in acted_in, directed, produced, wrote, reviewed)

Relations:
- acted_in: relates actor (person), relates film (movie); owns roles
- directed: relates director (person), relates film (movie)
- produced: relates producer (person), relates film (movie)
- wrote: relates writer (person), relates film (movie)
- follows: relates follower (person), relates followed (person)
- reviewed: relates reviewer (person), relates film (movie); owns summary, rating

Attributes:
- name (string), born (integer) - for person
- title (string), votes (integer), tagline (string), released (integer) - for movie
- roles (string) - on acted_in relation
- summary (string), rating (integer) - on reviewed relation
"""


def validate_query(driver, typeql_query):
    """
    Validate a TypeQL query against TypeDB.
    Returns (is_valid, error_message)
    """
    try:
        with driver.transaction(DATABASE, TransactionType.READ) as tx:
            result = tx.query(typeql_query).resolve()
            # Try to consume some results to ensure query is valid
            if hasattr(result, 'as_concept_documents'):
                docs = list(result.as_concept_documents())
            elif hasattr(result, 'as_concept_rows'):
                rows = list(result.as_concept_rows())
        return True, None
    except Exception as e:
        return False, str(e)


def semantic_review(question, cypher, typeql):
    """
    Perform semantic review of TypeQL query against the English question.
    Returns (is_correct, review_reason)
    """
    question_lower = question.lower()
    typeql_lower = typeql.lower()

    issues = []

    # Check for correct entity types
    if "movie" in question_lower or "film" in question_lower:
        if "$m isa movie" not in typeql_lower and "isa movie" not in typeql_lower:
            # Check if it's implicitly about movies through relations
            if not any(rel in typeql_lower for rel in ["acted_in", "directed", "produced", "wrote", "reviewed"]):
                issues.append("Question asks about movies but query doesn't match movie entities")

    if "person" in question_lower or "people" in question_lower or "actor" in question_lower or "director" in question_lower:
        if "$p isa person" not in typeql_lower and "isa person" not in typeql_lower:
            # Check if person is implicitly involved
            if not any(role in typeql_lower for role in ["actor:", "director:", "producer:", "writer:", "reviewer:", "follower:", "followed:"]):
                issues.append("Question asks about persons but query doesn't match person entities")

    # Check for count/aggregation - be more specific
    count_phrases = ["how many", "count of", "total number of", "number of people", "number of movies",
                     "number of actors", "number of directors"]
    if any(phrase in question_lower for phrase in count_phrases):
        if "reduce" not in typeql_lower and "count" not in typeql_lower:
            issues.append("Question asks for count but query doesn't use reduce/count")

    # Check for sorting direction - be smarter about context
    # "oldest" means lowest birth year (ascending sort on born)
    # "highest votes", "most votes" means descending sort on votes
    # "lowest votes", "fewest votes" means ascending sort on votes
    # "top" alone doesn't imply direction - depends on what follows

    needs_desc = False
    needs_asc = False

    # Explicit descending indicators
    if any(phrase in question_lower for phrase in ["highest vote", "most vote", "highest rating",
                                                    "most recent", "newest", "youngest",
                                                    "highest number of vote", "best rated"]):
        needs_desc = True

    # Explicit ascending indicators
    if any(phrase in question_lower for phrase in ["lowest vote", "fewest vote", "lowest rating",
                                                    "oldest", "earliest", "first released",
                                                    "lowest number of vote"]):
        needs_asc = True

    # Check sort direction if we have explicit requirements
    if needs_desc and "sort" in typeql_lower:
        if "desc" not in typeql_lower:
            issues.append("Question asks for highest/most but sort is not descending")

    if needs_asc and "sort" in typeql_lower:
        if "desc" in typeql_lower:
            issues.append("Question asks for lowest/oldest but sort is descending instead of ascending")

    # Check for specific name filters (quoted names in question)
    quoted_names = re.findall(r'"([^"]+)"', question)
    for name in quoted_names:
        if name.lower() not in typeql_lower:
            issues.append(f"Question mentions '{name}' but it's not in the query")

    # Check relationship types
    if any(word in question_lower for word in ["acted", "starring", "stars in", "appeared in"]):
        if "acted_in" not in typeql_lower:
            issues.append("Question about acting but doesn't use acted_in relation")

    if "reviewed" in question_lower or ("review" in question_lower and "rating" in question_lower):
        if "reviewed" not in typeql_lower and "rating" not in typeql_lower:
            issues.append("Question about reviews/ratings but doesn't use reviewed relation or rating attribute")

    if "follow" in question_lower:
        if "follows" not in typeql_lower:
            issues.append("Question about following but doesn't use follows relation")

    # Check for limit match
    limit_match = re.search(r'\b(?:first|top)\s+(\d+)\b', question_lower)
    if limit_match:
        expected_limit = limit_match.group(1)
        if f"limit {expected_limit}" not in typeql_lower:
            issues.append(f"Question asks for top/first {expected_limit} but limit doesn't match")

    # If no issues found, consider it semantically correct
    if not issues:
        return True, None
    else:
        return False, "; ".join(issues)


def main():
    print("Connecting to TypeDB...")
    try:
        driver = TypeDB.driver("localhost:1729", CREDENTIALS, OPTIONS)
    except Exception as e:
        print(f"Failed to connect to TypeDB: {e}")
        sys.exit(1)

    # Check if database exists
    try:
        dbs = [db.name for db in driver.databases.all()]
        if DATABASE not in dbs:
            print(f"Database '{DATABASE}' not found. Available: {dbs}")
            sys.exit(1)
    except Exception as e:
        print(f"Error checking databases: {e}")
        sys.exit(1)

    print(f"Reading queries from {INPUT_FILE}...")

    # Read all queries
    queries = []
    with open(INPUT_FILE, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            queries.append(row)

    print(f"Found {len(queries)} queries to validate.")

    valid_queries = []
    failed_validation = []
    failed_review = []

    for i, row in enumerate(queries):
        if (i + 1) % 50 == 0 or i == 0:
            print(f"Processing query {i + 1}/{len(queries)}...")

        original_index = row['original_index']
        question = row['question']
        cypher = row['cypher']
        typeql = row['typeql']

        # Step 1: Validate against TypeDB
        is_valid, error = validate_query(driver, typeql)

        if not is_valid:
            failed_validation.append({
                'original_index': original_index,
                'question': question,
                'cypher': cypher,
                'error': error
            })
            continue

        # Step 2: Semantic review
        is_semantic_ok, review_reason = semantic_review(question, cypher, typeql)

        if not is_semantic_ok:
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
    with open(OUTPUT_VALID, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
        writer.writeheader()
        writer.writerows(valid_queries)
    print(f"Valid queries written to {OUTPUT_VALID}")

    # Write validation failures
    with open(OUTPUT_FAILED, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'error'])
        writer.writeheader()
        writer.writerows(failed_validation)
    print(f"Validation failures written to {OUTPUT_FAILED}")

    # Write semantic review failures
    with open(OUTPUT_FAILED_REVIEW, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql', 'review_reason'])
        writer.writeheader()
        writer.writerows(failed_review)
    print(f"Semantic review failures written to {OUTPUT_FAILED_REVIEW}")

    # Summary
    print("\n" + "="*50)
    print("VALIDATION SUMMARY")
    print("="*50)
    print(f"Total queries processed: {len(queries)}")
    print(f"Valid queries: {len(valid_queries)}")
    print(f"Validation failures: {len(failed_validation)}")
    print(f"Semantic review failures: {len(failed_review)}")
    print("="*50)


if __name__ == "__main__":
    main()
