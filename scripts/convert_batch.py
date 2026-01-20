#!/usr/bin/env python3
"""Batch convert Cypher queries to TypeQL with validation."""

import json
import csv
import sys
import os
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# Get query from index
def get_query(database, index):
    """Get query from the dataset."""
    import subprocess
    result = subprocess.run(
        ['python3', '/opt/text2typeql/scripts/get_query.py', database, str(index)],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        return None
    return json.loads(result.stdout)

def validate_query(driver, db_name, typeql):
    """Validate a TypeQL query against the database."""
    try:
        with driver.transaction(db_name, TransactionType.READ) as tx:
            result = tx.query(typeql).resolve()
            # Try to consume results to fully validate
            try:
                for _ in result.as_concept_documents():
                    pass
            except:
                try:
                    for _ in result.as_concept_rows():
                        pass
                except:
                    pass
        return True, None
    except Exception as e:
        return False, str(e)

def append_to_csv(filepath, row):
    """Append a row to CSV file."""
    file_exists = os.path.exists(filepath)
    with open(filepath, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['original_index', 'question', 'cypher', 'typeql'])
        writer.writerow(row)

def main():
    if len(sys.argv) < 4:
        print("Usage: python convert_batch.py <database> <start_index> <typeql_query>")
        sys.exit(1)

    database = sys.argv[1]
    index = int(sys.argv[2])
    typeql = sys.argv[3]

    db_name = f"text2typeql_{database}"
    queries_csv = f"/opt/text2typeql/output/{database}/queries.csv"
    failed_csv = f"/opt/text2typeql/output/{database}/failed.csv"

    # Connect to TypeDB
    credentials = Credentials("admin", "password")
    options = DriverOptions(is_tls_enabled=False)
    driver = TypeDB.driver("localhost:1729", credentials, options)

    # Get the original query
    query_data = get_query(database, index)
    if not query_data:
        print(f"ERROR: No query found at index {index}")
        driver.close()
        sys.exit(1)

    # Validate
    valid, error = validate_query(driver, db_name, typeql)

    if valid:
        append_to_csv(queries_csv, [
            query_data['index'],
            query_data['question'],
            query_data['cypher'],
            typeql
        ])
        print(f"SUCCESS: Query {index} validated and saved")
    else:
        append_to_csv(failed_csv, [
            query_data['index'],
            query_data['question'],
            query_data['cypher'],
            f"FAILED: {error}"
        ])
        print(f"FAILED: Query {index} - {error}")

    driver.close()

if __name__ == "__main__":
    main()
