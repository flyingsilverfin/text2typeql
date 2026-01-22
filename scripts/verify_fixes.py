#!/usr/bin/env python3
"""Verify the fixed queries work against TypeDB."""

import csv
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

DATABASE = "text2typeql_neoflix"

# Indices to verify
INDICES = [128, 138, 155, 162, 168, 180, 232, 233, 248]


def main():
    # Connect to TypeDB
    credentials = Credentials("admin", "password")
    options = DriverOptions(is_tls_enabled=False)
    driver = TypeDB.driver("localhost:1729", credentials, options)

    # Read queries.csv
    queries_by_index = {}
    with open("/opt/text2typeql/output/neoflix/queries.csv", 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            idx = int(row['original_index'])
            if idx in INDICES:
                queries_by_index[idx] = {
                    'question': row['question'],
                    'typeql': row['typeql']
                }

    print(f"Found {len(queries_by_index)} queries to verify\n")

    # Validate each query
    for idx in INDICES:
        if idx not in queries_by_index:
            print(f"Index {idx}: NOT FOUND in queries.csv")
            continue

        q = queries_by_index[idx]
        print(f"Index {idx}: {q['question'][:60]}...")

        try:
            with driver.transaction(DATABASE, TransactionType.READ) as tx:
                result = tx.query(q['typeql']).resolve()
                # Count results
                count = 0
                for doc in result.as_concept_documents():
                    count += 1
                print(f"  VALID - {count} results")
        except Exception as e:
            print(f"  INVALID: {e}")

    driver.close()


if __name__ == "__main__":
    main()
