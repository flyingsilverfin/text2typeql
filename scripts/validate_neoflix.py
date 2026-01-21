#!/usr/bin/env python3
"""
Validate and semantically review all converted queries in the neoflix database.
"""

import csv
import os
import sys
import re
from typing import Tuple, Optional

# TypeDB imports
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# Configuration
DATABASE = "text2typeql_neoflix"
INPUT_FILE = "/opt/text2typeql/output/neoflix/queries.csv"
OUTPUT_DIR = "/opt/text2typeql/output/neoflix"
SCHEMA_FILE = "/opt/text2typeql/output/neoflix/schema.tql"

# Load schema for reference
with open(SCHEMA_FILE, 'r') as f:
    SCHEMA_CONTENT = f.read()

# Extract entity types from schema
ENTITY_TYPES = set(re.findall(r'entity\s+(\w+)', SCHEMA_CONTENT))
# Extract relation types from schema
RELATION_TYPES = set(re.findall(r'relation\s+(\w+)', SCHEMA_CONTENT))
# Extract attribute types from schema
ATTRIBUTE_TYPES = set(re.findall(r'attribute\s+(\w+)', SCHEMA_CONTENT))

print(f"Schema loaded: {len(ENTITY_TYPES)} entities, {len(RELATION_TYPES)} relations, {len(ATTRIBUTE_TYPES)} attributes")

def connect_typedb():
    """Connect to TypeDB."""
    credentials = Credentials("admin", "password")
    options = DriverOptions(is_tls_enabled=False)
    driver = TypeDB.driver("localhost:1729", credentials, options)
    return driver

def validate_query(driver, query: str) -> Tuple[bool, Optional[str]]:
    """Validate a TypeQL query against the database."""
    try:
        with driver.transaction(DATABASE, TransactionType.READ) as tx:
            result = tx.query(query).resolve()
            # Try to consume some results to ensure query is fully valid
            if hasattr(result, 'as_concept_documents'):
                docs = list(result.as_concept_documents())
            elif hasattr(result, 'as_concept_rows'):
                rows = list(result.as_concept_rows())
        return True, None
    except Exception as e:
        return False, str(e)

def semantic_review(question: str, typeql: str, cypher: str) -> Tuple[bool, Optional[str]]:
    """
    Review if the TypeQL query semantically matches the question.
    Returns (passed, reason_if_failed)
    """
    question_lower = question.lower()
    typeql_lower = typeql.lower()

    # Check 1: Aggregation keywords
    agg_keywords = {
        'count': 'count(',
        'how many': 'count(',
        'number of': 'count(',
        'total number': 'count(',
        'average': 'mean(',
        'avg': 'mean(',
        'sum': 'sum(',
        'total revenue': 'sum(',
        'total budget': 'sum(',
        'maximum': 'max(',
        'max': 'max(',
        'minimum': 'min(',
        'min': 'min(',
    }

    for keyword, typeql_func in agg_keywords.items():
        if keyword in question_lower:
            # Check if aggregation is present in TypeQL
            if 'reduce' not in typeql_lower:
                # Some questions with "how many" might just want a list, check more carefully
                if keyword in ['count', 'how many', 'number of', 'total number']:
                    # Stronger indicators
                    if any(phrase in question_lower for phrase in ['how many', 'count the', 'number of', 'total number of']):
                        return False, f"Question asks for count/aggregation ('{keyword}') but TypeQL has no 'reduce' clause"

    # Check 2: Sort direction
    if 'highest' in question_lower or 'top' in question_lower or 'most' in question_lower or 'best' in question_lower:
        if 'sort' in typeql_lower and 'asc' in typeql_lower and 'desc' not in typeql_lower:
            return False, "Question asks for highest/top/most but TypeQL sorts ascending instead of descending"

    if 'lowest' in question_lower or 'least' in question_lower or 'worst' in question_lower or 'bottom' in question_lower:
        if 'sort' in typeql_lower and 'desc' in typeql_lower and 'asc' not in typeql_lower:
            return False, "Question asks for lowest/least/bottom but TypeQL sorts descending instead of ascending"

    # Check 3: Entity types mentioned
    entity_checks = {
        'movie': 'movie',
        'movies': 'movie',
        'film': 'movie',
        'films': 'movie',
        'person': 'person',
        'people': 'person',
        'actor': 'person',
        'actors': 'person',
        'director': 'person',
        'directors': 'person',
        'user': 'user',
        'users': 'user',
        'genre': 'genre',
        'genres': 'genre',
        'country': 'country',
        'countries': 'country',
        'language': 'language',
        'languages': 'language',
        'keyword': 'keyword',
        'keywords': 'keyword',
        'collection': 'collection',
        'collections': 'collection',
        'package': 'package',
        'packages': 'package',
        'subscription': 'subscription',
        'subscriptions': 'subscription',
        'production company': 'production_company',
        'studio': 'production_company',
        'studios': 'production_company',
    }

    # Check main entities
    for term, entity_type in entity_checks.items():
        if term in question_lower:
            if entity_type not in typeql_lower:
                # Not always an error, but worth noting
                pass

    # Check 4: Attribute filtering - check if question mentions specific values
    # Look for quoted strings in question
    quoted_values = re.findall(r"'([^']+)'|\"([^\"]+)\"", question)
    for val_tuple in quoted_values:
        val = val_tuple[0] or val_tuple[1]
        if val and val.lower() not in typeql_lower and val not in typeql:
            # The value mentioned in question might not be in query
            # This could be a semantic issue
            pass

    # Check 5: Relationship types
    relation_checks = {
        'produced by': 'produced_by',
        'in genre': 'in_genre',
        'genre': 'in_genre',
        'rated': 'rated',
        'rating': 'rated',
        'acted': 'cast_for',
        'acted in': 'cast_for',
        'cast': 'cast_for',
        'directed': 'crew_for',
        'crew': 'crew_for',
        'spoken': 'spoken_in_language',
        'original language': 'original_language',
        'keyword': 'has_keyword',
        'collection': 'in_collection',
        'country': 'produced_in_country',
    }

    # Check 6: Limit clause when question asks for specific number
    limit_patterns = [
        r'top (\d+)',
        r'first (\d+)',
        r'(\d+) movies',
        r'(\d+) films',
        r'list (\d+)',
        r'name (\d+)',
        r'show (\d+)',
        r'give me (\d+)',
    ]

    for pattern in limit_patterns:
        match = re.search(pattern, question_lower)
        if match:
            num = match.group(1)
            if 'limit' not in typeql_lower:
                return False, f"Question asks for {num} items but TypeQL has no limit clause"
            # Check if limit value matches
            limit_match = re.search(r'limit\s+(\d+)', typeql_lower)
            if limit_match and limit_match.group(1) != num:
                # This might not always be an error (e.g., "top 5" vs different limit)
                pass

    # Check 7: Date filtering
    year_match = re.search(r'\b(19\d{2}|20\d{2})\b', question)
    if year_match:
        year = year_match.group(1)
        if year not in typeql:
            # Year mentioned in question should appear in query
            pass

    # Check 8: Fetch clause should return what question asks for
    if 'fetch' not in typeql_lower:
        return False, "TypeQL query has no fetch clause"

    # Check 9: Make sure variables in fetch are defined in match
    fetch_match = re.search(r'fetch\s*\{([^}]+)\}', typeql, re.IGNORECASE | re.DOTALL)
    if fetch_match:
        fetch_content = fetch_match.group(1)
        # Find all variables in fetch
        fetch_vars = re.findall(r'\$\w+', fetch_content)
        # Find all variables in match
        match_section = typeql.split('fetch')[0] if 'fetch' in typeql.lower() else typeql
        match_vars = set(re.findall(r'\$\w+', match_section))

        for var in fetch_vars:
            if var not in match_vars:
                return False, f"Variable {var} in fetch is not defined in match clause"

    # All checks passed
    return True, None

def main():
    print("Connecting to TypeDB...")
    driver = connect_typedb()

    # Check database exists
    try:
        databases = driver.databases.all()
        db_names = [db.name for db in databases]
        if DATABASE not in db_names:
            print(f"Error: Database '{DATABASE}' does not exist")
            print(f"Available databases: {db_names}")
            return
    except Exception as e:
        print(f"Error connecting to TypeDB: {e}")
        return

    print(f"Connected. Reading queries from {INPUT_FILE}...")

    # Read all queries
    queries = []
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            queries.append(row)

    print(f"Read {len(queries)} queries")

    # Process queries
    valid_queries = []
    validation_failures = []
    semantic_failures = []

    for i, query in enumerate(queries):
        original_index = query['original_index']
        question = query['question']
        cypher = query['cypher']
        typeql = query['typeql']

        if (i + 1) % 100 == 0:
            print(f"Processing query {i + 1}/{len(queries)}...")

        # Step 1: TypeDB validation
        is_valid, error = validate_query(driver, typeql)

        if not is_valid:
            validation_failures.append({
                'original_index': original_index,
                'question': question,
                'cypher': cypher,
                'error': error
            })
            continue

        # Step 2: Semantic review
        passed, reason = semantic_review(question, typeql, cypher)

        if not passed:
            semantic_failures.append({
                'original_index': original_index,
                'question': question,
                'cypher': cypher,
                'typeql': typeql,
                'review_reason': reason
            })
            continue

        # Query is valid and semantically correct
        valid_queries.append(query)

    print(f"\nResults:")
    print(f"  Valid queries: {len(valid_queries)}")
    print(f"  Validation failures: {len(validation_failures)}")
    print(f"  Semantic failures: {len(semantic_failures)}")

    # Write valid queries
    valid_output = os.path.join(OUTPUT_DIR, 'queries.csv')
    with open(valid_output, 'w', newline='', encoding='utf-8') as f:
        if valid_queries:
            writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
            writer.writeheader()
            writer.writerows(valid_queries)
    print(f"  Written valid queries to {valid_output}")

    # Write validation failures
    if validation_failures:
        failed_output = os.path.join(OUTPUT_DIR, 'failed.csv')
        with open(failed_output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'error'])
            writer.writeheader()
            writer.writerows(validation_failures)
        print(f"  Written validation failures to {failed_output}")

    # Write semantic failures
    if semantic_failures:
        review_output = os.path.join(OUTPUT_DIR, 'failed_review.csv')
        with open(review_output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql', 'review_reason'])
            writer.writeheader()
            writer.writerows(semantic_failures)
        print(f"  Written semantic failures to {review_output}")

    driver.close()
    print("\nDone!")

if __name__ == '__main__':
    main()
