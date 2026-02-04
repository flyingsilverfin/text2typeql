#!/usr/bin/env python3
"""Validate queries that were modified by the syntax fix script.

Reads the changes JSON from apply_syntax_fixes.py and validates each
modified query against TypeDB.

Usage:
    python3 pipeline/scripts/validate_changed_queries.py /tmp/applied_changes.json
"""

import argparse
import csv
import json
import sys
from pathlib import Path

# Import validate_query function from validate_typeql.py
sys.path.insert(0, str(Path(__file__).parent))
from validate_typeql import validate_query


def get_typeql_from_csv(source: str, database: str, original_index: int) -> str | None:
    """Read the typeql field for a specific query from queries.csv."""
    csv_path = f"dataset/{source}/{database}/queries.csv"

    try:
        with open(csv_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if int(row.get('original_index', -1)) == original_index:
                    return row.get('typeql')
    except FileNotFoundError:
        return None
    return None


def main():
    parser = argparse.ArgumentParser(description='Validate modified queries against TypeDB')
    parser.add_argument('changes_file', help='Path to applied changes JSON')
    parser.add_argument('--output', help='Output JSON file for failures',
                        default='/tmp/validation_failures.json')

    args = parser.parse_args()

    # Read changes
    with open(args.changes_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    changes = data.get('changes', [])
    source = data.get('source', 'synthetic-1')

    if not changes:
        print("No changes to validate")
        return

    print(f"Validating {len(changes)} modified queries...")

    failures = []
    successes = 0

    for i, change in enumerate(changes, 1):
        database = change['database']
        original_index = change['original_index']

        # Get the updated TypeQL
        typeql = get_typeql_from_csv(source, database, original_index)

        if not typeql:
            print(f"[{i}/{len(changes)}] {database}:{original_index} - ERROR: Query not found in CSV")
            failures.append({
                'database': database,
                'original_index': original_index,
                'error': 'Query not found in CSV',
            })
            continue

        # Validate against TypeDB
        success, message = validate_query(database, typeql)

        if success:
            print(f"[{i}/{len(changes)}] {database}:{original_index} - OK")
            successes += 1
        else:
            print(f"[{i}/{len(changes)}] {database}:{original_index} - FAILED: {message}")
            failures.append({
                'database': database,
                'original_index': original_index,
                'error': message,
                'typeql': typeql,
            })

    print()

    if not failures:
        print(f"All {len(changes)} queries validated successfully.")
    else:
        # Write failures to output file
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump({
                'failures': failures,
                'total_validated': len(changes),
                'total_failed': len(failures),
            }, f, indent=2)

        print(f"FAILED: {len(failures)} of {len(changes)} queries failed validation.")
        print(f"See {args.output} for details.")
        sys.exit(1)


if __name__ == '__main__':
    main()
