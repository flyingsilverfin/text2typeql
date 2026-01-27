#!/usr/bin/env python3
"""Read a batch of rows from a CSV file. Outputs JSON array."""

import csv
import json
import sys

def read_batch(csv_path: str, offset: int, limit: int):
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        rows = []
        for i, row in enumerate(reader):
            if i < offset:
                continue
            if i >= offset + limit:
                break
            rows.append(row)
    return rows

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print("Usage: csv_read_batch.py <csv_path> <offset> <limit>", file=sys.stderr)
        sys.exit(1)

    csv_path = sys.argv[1]
    offset = int(sys.argv[2])
    limit = int(sys.argv[3])

    rows = read_batch(csv_path, offset, limit)
    print(json.dumps(rows, indent=2))
