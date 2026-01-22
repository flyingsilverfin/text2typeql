#!/usr/bin/env python3
"""
Convert the final 4 failed Cypher queries to TypeQL for the movies database.

Fix: Use $r % 10 == 0 syntax (double equals) for modulo comparisons.
"""

import csv
from typing import Optional, Tuple
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# TypeDB connection
DATABASE = "text2typeql_movies"
FAILED_PATH = "/opt/text2typeql/output/movies/failed.csv"
OUTPUT_PATH = "/opt/text2typeql/output/movies/queries.csv"


def validate_query(driver, query: str) -> Tuple[bool, str]:
    """Validate a TypeQL query against the database."""
    try:
        with driver.transaction(DATABASE, TransactionType.READ) as tx:
            result = tx.query(query).resolve()
            if hasattr(result, 'as_concept_documents'):
                list(result.as_concept_documents())
            elif hasattr(result, 'as_value'):
                result.as_value()
        return True, ""
    except Exception as e:
        return False, str(e)


def convert_query(idx: int, question: str, cypher: str) -> Optional[str]:
    """Convert a question/cypher pair to TypeQL based on question semantics."""

    # ===== Query 385: Movies with released year divisible by 10 =====
    if idx == 385:
        return """match
  $m isa movie, has title $t, has released $r;
  $r % 10 == 0;
fetch {
  "title": $t,
  "released": $r
};"""

    # ===== Query 595: First 3 movies with released year divisible by 10 and votes over 500 =====
    if idx == 595:
        return """match
  $m isa movie, has title $t, has released $r, has votes $v;
  $r % 10 == 0;
  $v > 500;
sort $r asc;
limit 3;
fetch {
  "title": $t
};"""

    # ===== Query 610: Persons who directed a movie with released year divisible by 20 =====
    if idx == 610:
        return """match
  $p isa person, has name $n;
  (director: $p, film: $m) isa directed;
  $m has title $t, has released $r;
  $r % 20 == 0;
fetch {
  "director": $n,
  "movie": $t,
  "year": $r
};"""

    # ===== Query 700: First 3 movies with released year divisible by 10 =====
    if idx == 700:
        return """match
  $m isa movie, has title $t, has released $r;
  $r % 10 == 0;
limit 3;
fetch {
  "title": $t
};"""

    return None


def main():
    # Read failed queries
    failed_queries = []
    with open(FAILED_PATH, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            failed_queries.append(row)

    print(f"Total failed queries to process: {len(failed_queries)}")

    # Read existing successful queries
    existing_queries = []
    try:
        with open(OUTPUT_PATH, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_queries.append(row)
    except FileNotFoundError:
        pass

    print(f"Existing successful queries: {len(existing_queries)}")

    # Connect to TypeDB
    credentials = Credentials("admin", "password")
    options = DriverOptions(is_tls_enabled=False)
    driver = TypeDB.driver("localhost:1729", credentials, options)

    converted = []
    still_failed = []

    for i, query in enumerate(failed_queries):
        original_index = int(query['original_index'])
        question = query['question']
        cypher = query['cypher']
        error = query.get('error', '')

        print(f"\n[{i+1}/{len(failed_queries)}] Processing query {original_index}: {question[:60]}...")

        # Try to convert
        typeql = convert_query(original_index, question, cypher)

        if typeql is None:
            print(f"  -> No conversion rule found")
            still_failed.append(query)
            continue

        # Validate
        valid, validation_error = validate_query(driver, typeql)

        if valid:
            print(f"  -> SUCCESS!")
            converted.append({
                'original_index': original_index,
                'question': question,
                'cypher': cypher,
                'typeql': typeql
            })
        else:
            print(f"  -> Validation failed: {validation_error[:100]}")
            query['error'] = f"TypeQL validation failed: {validation_error}"
            still_failed.append(query)

    driver.close()

    # Write results
    print(f"\n\nConversion complete!")
    print(f"Converted: {len(converted)}")
    print(f"Still failed: {len(still_failed)}")

    # Write successful queries (append to existing)
    all_queries = existing_queries + converted
    with open(OUTPUT_PATH, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
        writer.writeheader()
        for row in all_queries:
            writer.writerow(row)

    # Write failed queries
    with open(FAILED_PATH, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'error'])
        writer.writeheader()
        for row in still_failed:
            writer.writerow(row)

    print(f"\nTotal successful queries now: {len(all_queries)}")
    print(f"Remaining failed queries: {len(still_failed)}")


if __name__ == "__main__":
    main()
