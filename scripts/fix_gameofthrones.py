#!/usr/bin/env python3
"""
Script to fix semantic failures in gameofthrones dataset.
Processes failed_review.csv, validates queries against TypeDB, and updates queries.csv.
"""

import csv
import sys
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

DATABASE = "text2typeql_gameofthrones"
FAILED_REVIEW_PATH = "/opt/text2typeql/output/gameofthrones/failed_review.csv"
QUERIES_PATH = "/opt/text2typeql/output/gameofthrones/queries.csv"

def connect_typedb():
    """Connect to TypeDB server."""
    credentials = Credentials("admin", "password")
    options = DriverOptions(is_tls_enabled=False)
    driver = TypeDB.driver("localhost:1729", credentials, options)
    return driver

def validate_query(driver, query: str) -> tuple[bool, str]:
    """Validate a TypeQL query against the database."""
    try:
        with driver.transaction(DATABASE, TransactionType.READ) as tx:
            result = tx.query(query).resolve()
            # Try to consume results to ensure query is fully valid
            if hasattr(result, 'as_concept_documents'):
                list(result.as_concept_documents())
            return True, "Valid"
    except Exception as e:
        return False, str(e)

def fix_interaction_missing(row: dict) -> str:
    """Fix queries where interaction relationship is missing."""
    question = row['question']
    cypher = row['cypher']

    # Determine which relation to use based on the cypher/question
    if 'INTERACTS1' in cypher:
        relation = 'interacts1'
    elif 'INTERACTS2' in cypher:
        relation = 'interacts2'
    elif 'INTERACTS3' in cypher:
        relation = 'interacts3'
    elif 'INTERACTS45' in cypher:
        relation = 'interacts45'
    else:
        relation = 'interacts'

    # Extract target character name from cypher
    import re
    name_match = re.search(r"\{name:\s*['\"]([^'\"]+)['\"]\}", cypher)
    if not name_match:
        name_match = re.search(r"name:\s*['\"]([^'\"]+)['\"]", cypher)

    if not name_match:
        return None

    target_name = name_match.group(1)

    # Check if this includes weight/interactions ordering
    if 'ORDER BY' in cypher and 'weight' in cypher.lower():
        # Query needs both relationship and sorting
        limit_match = re.search(r'LIMIT\s+(\d+)', cypher)
        limit = limit_match.group(1) if limit_match else "5"

        typeql = f'''match
  $c isa character, has name $cn;
  $target isa character, has name "{target_name}";
  $rel (character1: $c, character2: $target) isa {relation}, has weight $w;
sort $w desc;
limit {limit};
fetch {{ "character": $cn, "interactions": $w }};'''
    else:
        # Simple interaction query
        limit_match = re.search(r'LIMIT\s+(\d+)', cypher)
        limit = limit_match.group(1) if limit_match else "10"

        typeql = f'''match
  $c isa character, has name $cn;
  $target isa character, has name "{target_name}";
  (character1: $c, character2: $target) isa {relation};
limit {limit};
fetch {{ "c_name": $cn }};'''

    return typeql

def fix_missing_sort(row: dict) -> str:
    """Fix queries where sort by weight is missing."""
    cypher = row['cypher']
    current_typeql = row['typeql']

    # Determine which relation to use
    if 'INTERACTS1' in cypher:
        relation = 'interacts1'
    elif 'INTERACTS2' in cypher:
        relation = 'interacts2'
    elif 'INTERACTS3' in cypher:
        relation = 'interacts3'
    elif 'INTERACTS45' in cypher:
        relation = 'interacts45'
    else:
        relation = 'interacts'

    # Extract limit from cypher
    import re
    limit_match = re.search(r'LIMIT\s+(\d+)', cypher)
    limit = limit_match.group(1) if limit_match else "5"

    # Build fixed query with proper weight sorting
    typeql = f'''match
  $c isa character, has name $cn;
  $other isa character;
  $rel (character1: $c, character2: $other) isa {relation}, has weight $w;
sort $w desc;
limit {limit};
fetch {{ "character": $cn }};'''

    return typeql

def main():
    print("Connecting to TypeDB...")
    driver = connect_typedb()

    # Read failed queries
    print(f"Reading {FAILED_REVIEW_PATH}...")
    failed_queries = []
    with open(FAILED_REVIEW_PATH, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            failed_queries.append(row)

    print(f"Found {len(failed_queries)} failed queries to fix")

    # Read existing queries
    print(f"Reading {QUERIES_PATH}...")
    existing_queries = []
    with open(QUERIES_PATH, 'r') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            existing_queries.append(row)

    print(f"Found {len(existing_queries)} existing queries")

    # Process each failed query
    fixed_queries = []
    still_failed = []

    for i, row in enumerate(failed_queries):
        print(f"\n--- Processing query {i+1}/{len(failed_queries)} ---")
        print(f"Original index: {row['original_index']}")
        print(f"Question: {row['question'][:80]}...")
        print(f"Review reason: {row['review_reason']}")

        # Determine fix type
        if "doesn't use relationship" in row['review_reason']:
            print("Fix type: Add interaction relationship")
            fixed_typeql = fix_interaction_missing(row)
        elif "doesn't sort descending" in row['review_reason']:
            print("Fix type: Add sort by weight")
            fixed_typeql = fix_missing_sort(row)
        else:
            print(f"Unknown fix type for reason: {row['review_reason']}")
            still_failed.append(row)
            continue

        if fixed_typeql is None:
            print("Could not generate fix")
            still_failed.append(row)
            continue

        print(f"Fixed TypeQL:\n{fixed_typeql}")

        # Validate the fixed query
        valid, msg = validate_query(driver, fixed_typeql)

        if valid:
            print("VALIDATION: SUCCESS")
            fixed_queries.append({
                'original_index': row['original_index'],
                'question': row['question'],
                'cypher': row['cypher'],
                'typeql': fixed_typeql
            })
        else:
            print(f"VALIDATION: FAILED - {msg}")
            still_failed.append(row)

    print(f"\n\n=== SUMMARY ===")
    print(f"Total failed queries: {len(failed_queries)}")
    print(f"Successfully fixed: {len(fixed_queries)}")
    print(f"Still failed: {len(still_failed)}")

    # Add fixed queries to existing queries
    if fixed_queries:
        print(f"\nAdding {len(fixed_queries)} fixed queries to {QUERIES_PATH}...")
        existing_queries.extend(fixed_queries)

        with open(QUERIES_PATH, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(existing_queries)
        print("Queries.csv updated")

    # Update failed_review.csv with remaining failures
    print(f"\nUpdating {FAILED_REVIEW_PATH}...")
    with open(FAILED_REVIEW_PATH, 'w', newline='') as f:
        if still_failed:
            writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql', 'review_reason'])
            writer.writeheader()
            writer.writerows(still_failed)
            print(f"Wrote {len(still_failed)} remaining failures")
        else:
            # Write empty file with just header
            f.write('original_index,question,cypher,typeql,review_reason\n')
            print("Cleared failed_review.csv (no remaining failures)")

    driver.close()
    print("\nDone!")

    return len(still_failed)

if __name__ == "__main__":
    sys.exit(main())
