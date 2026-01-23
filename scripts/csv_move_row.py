#!/usr/bin/env python3
"""Move a single row from one CSV to another by original_index."""

import csv
import os
import sys
import tempfile
import shutil

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
        return 'queries'

def move_row(source_path: str, dest_path: str, original_index: int, extra_fields: dict = None) -> bool:
    """
    Move a row from source CSV to destination CSV.

    Args:
        source_path: Path to source CSV
        dest_path: Path to destination CSV
        original_index: The original_index of the row to move
        extra_fields: Additional/replacement fields for the destination row

    Returns:
        True if row was found and moved, False otherwise
    """
    if not os.path.exists(source_path):
        print(f"Source file not found: {source_path}", file=sys.stderr)
        return False

    # Read source and find the row
    found_row = None
    remaining_rows = []

    with open(source_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        source_headers = reader.fieldnames
        for row in reader:
            if int(row.get('original_index', -1)) == original_index:
                found_row = dict(row)
            else:
                remaining_rows.append(row)

    if not found_row:
        print(f"Row with original_index={original_index} not found in {source_path}", file=sys.stderr)
        return False

    # Apply extra fields if provided
    if extra_fields:
        found_row.update(extra_fields)

    # Write remaining rows back to source (atomic write)
    with tempfile.NamedTemporaryFile(mode='w', newline='', encoding='utf-8',
                                      delete=False, dir=os.path.dirname(source_path) or '.') as tmp:
        writer = csv.DictWriter(tmp, fieldnames=source_headers)
        writer.writeheader()
        writer.writerows(remaining_rows)
        tmp_path = tmp.name

    shutil.move(tmp_path, source_path)

    # Append to destination
    dest_type = get_csv_type(dest_path)
    dest_headers = HEADERS.get(dest_type, HEADERS['queries'])

    dest_exists = os.path.exists(dest_path)
    dest_empty = not dest_exists or os.path.getsize(dest_path) == 0

    os.makedirs(os.path.dirname(dest_path) or '.', exist_ok=True)

    with open(dest_path, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=dest_headers, extrasaction='ignore')
        if dest_empty:
            writer.writeheader()
        writer.writerow(found_row)

    return True

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print("Usage: csv_move_row.py <source_csv> <dest_csv> <original_index> [extra_json]", file=sys.stderr)
        print("Example: csv_move_row.py output/movies/failed.csv output/movies/queries.csv 42 '{\"typeql\": \"match ...\"}'", file=sys.stderr)
        sys.exit(1)

    source_path = sys.argv[1]
    dest_path = sys.argv[2]
    original_index = int(sys.argv[3])

    extra_fields = None
    if len(sys.argv) > 4:
        import json
        try:
            extra_fields = json.loads(sys.argv[4])
        except json.JSONDecodeError as e:
            print(f"Invalid JSON for extra fields: {e}", file=sys.stderr)
            sys.exit(1)

    if move_row(source_path, dest_path, original_index, extra_fields):
        print(f"Moved row {original_index} from {source_path} to {dest_path}")
    else:
        sys.exit(1)
