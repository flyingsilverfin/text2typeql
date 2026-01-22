#!/usr/bin/env python3
"""
Script to fix remaining failed queries for neoflix database.
"""

import csv
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# TypeDB connection settings
DB_NAME = "text2typeql_neoflix"
HOST = "localhost:1729"

def get_driver():
    """Get TypeDB driver connection."""
    credentials = Credentials("admin", "password")
    options = DriverOptions(is_tls_enabled=False)
    return TypeDB.driver(HOST, credentials, options)

def validate_query(driver, query):
    """Validate a TypeQL query against the database."""
    try:
        with driver.transaction(DB_NAME, TransactionType.READ) as tx:
            result = tx.query(query).resolve()
            if hasattr(result, 'as_concept_documents'):
                list(result.as_concept_documents())
            return True, None
    except Exception as e:
        return False, str(e)

def main():
    """Main function to fix remaining queries."""
    failed_path = "/opt/text2typeql/output/neoflix/failed.csv"
    queries_path = "/opt/text2typeql/output/neoflix/queries.csv"

    # Read failed queries
    failed_queries = []
    with open(failed_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            failed_queries.append(row)

    print(f"Loaded {len(failed_queries)} failed queries")

    # Connect to TypeDB
    driver = get_driver()

    # Manual fixes for specific queries
    fixes = {}

    # Query 475: Fix regex - use simpler pattern without unnecessary escapes
    # The poster path ends with a specific string
    fixes["475"] = """match
  $m isa movie, has title $title, has poster_path $path;
  $path like ".*pQFoyx7rp09CJTAb932F2g8Nlho.jpg";
limit 3;
fetch { "title": $title };"""

    # Query 594: Schema mismatch - adult doesn't play rated:rated_media
    # Update error message

    # Query 666: Schema mismatch - adult doesn't play rated:rated_media
    # Update error message

    # Query 855: Schema mismatch - adult doesn't play crew_for:film
    # Update error message

    # Process each failed query
    still_failed = []
    newly_converted = []

    for query in failed_queries:
        idx = query['original_index']
        question = query['question']
        cypher = query['cypher']
        error = query['error']

        # Check if we have a fix for this query
        if idx in fixes:
            typeql = fixes[idx]
            print(f"\nTrying fix for query {idx}: {question[:60]}...")

            is_valid, validation_error = validate_query(driver, typeql)

            if is_valid:
                print(f"  -> SUCCESS! Fixed and validated.")
                newly_converted.append({
                    'original_index': idx,
                    'question': question,
                    'cypher': cypher,
                    'typeql': typeql
                })
                continue
            else:
                print(f"  -> Fix failed: {validation_error[:100]}")
                query['error'] = f"TypeQL validation failed: {validation_error}"

        # Update error messages for schema mismatches
        if idx in ["594", "666", "742"]:
            query['error'] = "Schema mismatch: adult entity doesn't play rated:rated_media role"
        elif idx == "855":
            query['error'] = "Schema mismatch: adult entity doesn't play crew_for:film role"

        still_failed.append(query)

    driver.close()

    # Report results
    print(f"\n{'='*60}")
    print(f"RESULTS:")
    print(f"  Total failed queries: {len(failed_queries)}")
    print(f"  Successfully fixed: {len(newly_converted)}")
    print(f"  Still failed: {len(still_failed)}")
    print(f"{'='*60}")

    # Append newly converted to queries.csv
    if newly_converted:
        with open(queries_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
            for row in newly_converted:
                writer.writerow(row)
        print(f"\nAppended {len(newly_converted)} queries to {queries_path}")

    # Overwrite failed.csv with remaining failures
    with open(failed_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'error'])
        writer.writeheader()
        for row in still_failed:
            writer.writerow(row)
    print(f"Updated {failed_path} with {len(still_failed)} remaining failures")

    # Print summary of failure reasons
    if still_failed:
        print(f"\n{'='*60}")
        print("FAILURE REASONS SUMMARY:")
        reasons = {}
        for q in still_failed:
            reason = q['error'][:60] if q['error'] else "Unknown"
            reasons[reason] = reasons.get(reason, 0) + 1
        for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
            print(f"  {count}x: {reason}...")


if __name__ == "__main__":
    main()
