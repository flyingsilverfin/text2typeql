#!/usr/bin/env python3
"""Append a single row to a CSV file, creating with header if needed."""

import csv
import os
import sys
import json

# Define headers for each CSV type
HEADERS = {
    'queries': ['original_index', 'question', 'cypher', 'typeql'],
    'failed': ['original_index', 'question', 'cypher', 'error'],
    'failed_review': ['original_index', 'question', 'cypher', 'typeql', 'review_reason'],
}

def get_csv_type(csv_path: str) -> str:
    """Determine CSV type from filename."""
    basename = os.path.basename(csv_path)
    if basename == 'queries.csv':
        return 'queries'
    elif basename == 'failed.csv':
        return 'failed'
    elif basename == 'failed_review.csv':
        return 'failed_review'
    else:
        return 'queries'  # Default

def append_row(csv_path: str, row_data: dict) -> bool:
    """Append a row to CSV, creating file with header if needed."""
    csv_type = get_csv_type(csv_path)
    headers = HEADERS.get(csv_type, HEADERS['queries'])

    file_exists = os.path.exists(csv_path)
    file_empty = not file_exists or os.path.getsize(csv_path) == 0

    # Ensure directory exists
    os.makedirs(os.path.dirname(csv_path) or '.', exist_ok=True)

    with open(csv_path, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction='ignore')

        if file_empty:
            writer.writeheader()

        writer.writerow(row_data)

    return True

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: csv_append_row.py <csv_path> '<json_row>'", file=sys.stderr)
        print("Example: csv_append_row.py output/movies/queries.csv '{\"original_index\": 0, \"question\": \"...\", \"cypher\": \"...\", \"typeql\": \"...\"}'", file=sys.stderr)
        sys.exit(1)

    csv_path = sys.argv[1]
    row_json = sys.argv[2]

    try:
        row_data = json.loads(row_json)
    except json.JSONDecodeError as e:
        print(f"Invalid JSON: {e}", file=sys.stderr)
        sys.exit(1)

    if append_row(csv_path, row_data):
        print(f"Row appended to {csv_path}")
    else:
        print("Failed to append row", file=sys.stderr)
        sys.exit(1)
