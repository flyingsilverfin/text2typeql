#!/usr/bin/env python3
"""
Validate and semantically review converted TypeQL queries for the twitch database.
"""

import csv
import re
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType


def load_queries(filepath):
    """Load queries from CSV file."""
    queries = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            queries.append({
                'original_index': row['original_index'],
                'question': row['question'],
                'cypher': row['cypher'],
                'typeql': row['typeql']
            })
    return queries


def validate_query(tx, typeql_query):
    """Validate a TypeQL query against the database."""
    try:
        result = tx.query(typeql_query).resolve()
        # The query is valid if resolve() succeeds
        # Determine how to consume based on query type
        has_fetch = 'fetch' in typeql_query.lower()
        has_reduce = 'reduce' in typeql_query.lower()
        has_groupby = 'groupby' in typeql_query.lower()

        if has_fetch:
            # Fetch queries return concept documents
            list(result.as_concept_documents())
        elif has_reduce:
            # In TypeDB 3.x, reduce queries (with or without groupby) return rows
            list(result.as_concept_rows())
        else:
            # Match-only queries return concept rows
            list(result.as_concept_rows())
        return True, None
    except Exception as e:
        return False, str(e)


def semantic_review(question, typeql, schema_info):
    """
    Perform semantic review of the TypeQL query against the question.
    Returns (is_valid, reason) tuple.
    """
    question_lower = question.lower()
    typeql_lower = typeql.lower()

    issues = []

    # Check for count/aggregation requirements
    # Only these specific phrases truly require count aggregation
    count_keywords = ['how many', 'total number of', 'count of']
    needs_count = any(kw in question_lower for kw in count_keywords)
    has_count = 'reduce' in typeql_lower and 'count' in typeql_lower

    if needs_count and not has_count:
        # Exclude "total view count" which is an attribute, not an aggregation request
        if 'total_view_count' not in question_lower and 'total view count' not in question_lower:
            # Also exclude "follower count" which is an attribute
            if 'follower count' not in question_lower:
                issues.append("Question asks for count but query lacks reduce/count aggregation")

    # Check for sorting direction when asking for "highest/most/top" with numeric sorting
    # BUT exclude phrases like "top N with lowest/least" where lowest modifies the sort criteria
    has_asc = re.search(r'\bsort\b.*\basc\b', typeql_lower, re.DOTALL) is not None
    has_desc = 'desc' in typeql_lower

    # "top/most/highest" typically means desc, unless combined with "lowest/least"
    asks_for_highest = any(word in question_lower for word in ['highest', 'most', 'largest', 'greatest'])
    asks_for_top = 'top' in question_lower
    asks_for_lowest = any(word in question_lower for word in ['lowest', 'least', 'fewest', 'smallest'])

    # Handle compound phrases like "top 5 with lowest"
    # First check if "least" is part of "at least" (threshold, not sort)
    at_least_pattern = re.search(r'\bat\s+least\b', question_lower)
    effective_asks_for_lowest = asks_for_lowest and not at_least_pattern

    if asks_for_top and effective_asks_for_lowest:
        # "top N with lowest X" should use ascending sort
        if 'sort' in typeql_lower and has_desc and not has_asc:
            issues.append("Question asks for top with lowest but sort is descending")
    elif asks_for_top and not effective_asks_for_lowest:
        # "top N" alone usually means highest/descending
        if 'sort' in typeql_lower and has_asc and not has_desc:
            # Only flag if not time-based (oldest first = asc is correct)
            is_time_based = any(w in question_lower for w in ['created', 'oldest', 'earliest', 'first']) and 'created_at' in typeql_lower
            # Check if question explicitly asks for something that needs asc
            asks_oldest_to_newest = 'oldest to newest' in question_lower
            # "Active the longest" means oldest, so asc is correct
            active_longest = 'active the longest' in question_lower or 'longest' in question_lower
            if not is_time_based and not asks_oldest_to_newest and not active_longest:
                issues.append("Question asks for top but sort is ascending")
    elif asks_for_highest:
        if 'sort' in typeql_lower and has_asc and not has_desc:
            issues.append("Question asks for highest/most but sort is ascending")

    # "lowest/least" alone (not combined with "top") typically means asc
    # BUT exclude "at least" which is a threshold, not a sort direction
    if asks_for_lowest and not asks_for_top:
        # Check if "least" is part of "at least" (threshold, not sort direction)
        at_least_pattern = re.search(r'\bat\s+least\b', question_lower)
        if not at_least_pattern:
            if 'sort' in typeql_lower and has_desc and not has_asc:
                issues.append("Question asks for lowest/least but sort is descending")

    # Check for first/oldest vs latest/newest when sorting by time
    if any(word in question_lower for word in ['oldest', 'earliest']):
        if 'sort' in typeql_lower and 'created_at' in typeql_lower:
            if 'desc' in typeql_lower and 'asc' not in typeql_lower:
                issues.append("Question asks for oldest/earliest but sort is descending on created_at")

    if any(word in question_lower for word in ['latest', 'newest']):
        # Exclude "oldest to newest" which is asc
        if 'oldest to newest' not in question_lower and 'most recent' not in question_lower:
            if 'sort' in typeql_lower and 'created_at' in typeql_lower:
                if 'asc' in typeql_lower and 'desc' not in typeql_lower:
                    issues.append("Question asks for latest/newest but sort is ascending on created_at")

    if 'most recent' in question_lower:
        if 'sort' in typeql_lower and 'created_at' in typeql_lower:
            if 'asc' in typeql_lower and 'desc' not in typeql_lower:
                issues.append("Question asks for most recent but sort is ascending on created_at")

    # Check for limit when asking for "top N" or specific count
    limit_match = re.search(r'\b(top|first|last)\s+(\d+)\b', question_lower)
    if limit_match:
        expected_limit = limit_match.group(2)
        limit_in_query = re.search(r'limit\s+(\d+)', typeql_lower)
        if limit_in_query:
            actual_limit = limit_in_query.group(1)
            if actual_limit != expected_limit:
                issues.append(f"Question asks for {expected_limit} items but limit is {actual_limit}")
        else:
            issues.append(f"Question asks for {expected_limit} items but no limit clause found")

    # Check for specific attribute filters with numeric thresholds
    attribute_patterns = [
        (r'more than\s+(\d[\d,]*)\s*followers?', 'followers', '>'),
        (r'over\s+(\d[\d,]*)\s*followers?', 'followers', '>'),
        (r'followers?\s*greater\s*than\s+(\d[\d,]*)', 'followers', '>'),
        (r'less than\s+(\d[\d,]*)\s*followers?', 'followers', '<'),
        (r'fewer than\s+(\d[\d,]*)\s*followers?', 'followers', '<'),
        (r'under\s+(\d[\d,]*)\s*followers?', 'followers', '<'),
        (r'view\s*count\s*(?:greater|more|over|above)\s*(?:than\s+)?(\d[\d,]*)', 'total_view_count', '>'),
        (r'view\s*count\s*(?:less|fewer|under|below)\s*(?:than\s+)?(\d[\d,]*)', 'total_view_count', '<'),
    ]

    for pattern, attr, op in attribute_patterns:
        match = re.search(pattern, question_lower)
        if match:
            if attr not in typeql_lower:
                issues.append(f"Question mentions {attr} filter but attribute not in query")

    # Check for named entity mentions - should match exactly
    name_patterns = [
        r"named?\s+['\"]([^'\"]+)['\"]",
        r"called\s+['\"]([^'\"]+)['\"]",
        r"stream\s+['\"]([^'\"]+)['\"]",
        r"name\s+['\"]([^'\"]+)['\"]",
    ]

    for pattern in name_patterns:
        match = re.search(pattern, question)  # Use original case
        if match:
            name_value = match.group(1)
            # Check if the name appears in the typeql (in quotes)
            if f'"{name_value}"' not in typeql and f"'{name_value}'" not in typeql:
                # Also check lowercase
                if f'"{name_value.lower()}"' not in typeql_lower:
                    issues.append(f"Question mentions specific name '{name_value}' but not found in query")

    # Check fetch clause has expected outputs
    if 'fetch' not in typeql_lower and 'reduce' not in typeql_lower:
        issues.append("Query lacks both fetch and reduce clauses")

    if issues:
        return False, "; ".join(issues)

    return True, None


def main():
    # Load schema info (simplified for semantic review)
    schema_info = {
        'entities': ['stream', 'game', 'language', 'user', 'team'],
        'relations': ['game_play', 'language_usage', 'moderation', 'chat_activity', 'team_membership', 'vip_status'],
        'attributes': ['name', 'created_at', 'stream_id', 'description', 'url', 'followers', 'total_view_count', 'team_id']
    }

    # Load queries
    queries = load_queries('/opt/text2typeql/output/twitch/queries.csv')
    print(f"Loaded {len(queries)} queries")

    # Connect to TypeDB
    credentials = Credentials("admin", "password")
    options = DriverOptions(is_tls_enabled=False)
    driver = TypeDB.driver("localhost:1729", credentials, options)

    valid_queries = []
    failed_validation = []
    failed_semantic = []

    try:
        with driver.transaction("text2typeql_twitch", TransactionType.READ) as tx:
            for i, query in enumerate(queries):
                if (i + 1) % 100 == 0:
                    print(f"Processing query {i + 1}/{len(queries)}...")

                typeql = query['typeql']

                # Step 1: Validate against TypeDB
                is_valid, error = validate_query(tx, typeql)

                if not is_valid:
                    failed_validation.append({
                        'original_index': query['original_index'],
                        'question': query['question'],
                        'cypher': query['cypher'],
                        'error': error
                    })
                    continue

                # Step 2: Semantic review
                is_semantic_valid, reason = semantic_review(
                    query['question'],
                    typeql,
                    schema_info
                )

                if not is_semantic_valid:
                    failed_semantic.append({
                        'original_index': query['original_index'],
                        'question': query['question'],
                        'cypher': query['cypher'],
                        'typeql': typeql,
                        'review_reason': reason
                    })
                    continue

                # Query is valid and semantically correct
                valid_queries.append(query)

    finally:
        driver.close()

    # Write results
    output_dir = '/opt/text2typeql/output/twitch'

    # Write valid queries
    with open(f'{output_dir}/queries.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
        writer.writeheader()
        writer.writerows(valid_queries)

    # Write validation failures
    with open(f'{output_dir}/failed.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'error'])
        writer.writeheader()
        writer.writerows(failed_validation)

    # Write semantic failures
    with open(f'{output_dir}/failed_review.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql', 'review_reason'])
        writer.writeheader()
        writer.writerows(failed_semantic)

    # Print summary
    print("\n" + "="*50)
    print("VALIDATION AND SEMANTIC REVIEW COMPLETE")
    print("="*50)
    print(f"Total queries processed: {len(queries)}")
    print(f"Valid queries: {len(valid_queries)}")
    print(f"Failed validation (TypeDB errors): {len(failed_validation)}")
    print(f"Failed semantic review: {len(failed_semantic)}")
    print()
    print(f"Results written to:")
    print(f"  - {output_dir}/queries.csv (valid queries)")
    print(f"  - {output_dir}/failed.csv (validation failures)")
    print(f"  - {output_dir}/failed_review.csv (semantic failures)")


if __name__ == '__main__':
    main()
