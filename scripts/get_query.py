#!/usr/bin/env python3
"""Extract the Nth query for a database from the source CSV."""

import csv
import json
import sys

def get_query(database: str, index: int) -> dict:
    """Get query at index for database (0-indexed within valid queries for that db)."""
    csv_path = "/opt/text2typeql/data/text2cypher/datasets/synthetic_opus_demodbs/text2cypher_claudeopus.csv"

    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        idx = 0
        for row in reader:
            if row['database'] != database:
                continue
            if row.get('syntax_error', '').lower() == 'true':
                continue
            if row.get('false_schema', '').lower() == 'true':
                continue

            if idx == index:
                return {
                    'index': index,
                    'question': row['question'],
                    'cypher': row['cypher']
                }
            idx += 1

    return None

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: get_query.py <database> <index>", file=sys.stderr)
        sys.exit(1)

    database = sys.argv[1]
    index = int(sys.argv[2])

    result = get_query(database, index)
    if result:
        print(json.dumps(result, indent=2))
    else:
        print(f"No query found at index {index} for {database}", file=sys.stderr)
        sys.exit(1)
