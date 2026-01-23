#!/usr/bin/env python3
"""Read a single row from a CSV by original_index."""

import csv
import json
import sys

def read_row(csv_path: str, original_index: int) -> dict | None:
    """Read a single row matching original_index from CSV."""
    try:
        with open(csv_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if int(row.get('original_index', -1)) == original_index:
                    return dict(row)
    except FileNotFoundError:
        return None
    return None

def row_exists(csv_path: str, original_index: int) -> bool:
    """Check if a row with original_index exists."""
    return read_row(csv_path, original_index) is not None

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: csv_read_row.py <csv_path> <original_index> [--exists]", file=sys.stderr)
        print("  --exists: Only check if row exists (returns 'true' or 'false')", file=sys.stderr)
        sys.exit(1)

    csv_path = sys.argv[1]
    original_index = int(sys.argv[2])
    check_exists = '--exists' in sys.argv

    if check_exists:
        print('true' if row_exists(csv_path, original_index) else 'false')
    else:
        result = read_row(csv_path, original_index)
        if result:
            print(json.dumps(result, indent=2))
        else:
            print(f"No row found with original_index={original_index}", file=sys.stderr)
            sys.exit(1)
