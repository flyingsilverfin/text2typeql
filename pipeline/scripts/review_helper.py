#!/usr/bin/env python3
"""Helper script for moving queries between CSV files during review."""

import csv
import sys

DEFAULT_SOURCE = "synthetic-1"

def move_to_failed_review(database: str, indices: list[int], reason: str = "", source: str = DEFAULT_SOURCE):
    """Move queries at given indices from queries.csv to failed_review.csv"""

    queries_path = f"/opt/text2typeql/dataset/{source}/{database}/queries.csv"
    failed_path = f"/opt/text2typeql/dataset/{source}/{database}/failed_review.csv"

    # Read all queries
    with open(queries_path, 'r') as f:
        rows = list(csv.DictReader(f))

    indices_set = set(str(i) for i in indices)

    # Separate into keep and move
    keep = []
    move = []
    for row in rows:
        if row['original_index'] in indices_set:
            row['review_reason'] = reason
            move.append(row)
        else:
            keep.append(row)

    # Write back queries.csv
    with open(queries_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
        writer.writeheader()
        writer.writerows(keep)

    # Append to failed_review.csv
    write_header = True
    try:
        with open(failed_path, 'r') as f:
            if f.readline():
                write_header = False
    except FileNotFoundError:
        pass

    with open(failed_path, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql', 'review_reason'])
        if write_header:
            writer.writeheader()
        writer.writerows(move)

    print(f"Moved {len(move)} queries to failed_review.csv")
    print(f"Remaining in queries.csv: {len(keep)}")

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: review_helper.py <database> <index1> [index2] ... [--reason 'reason'] [--source synthetic-1|synthetic-2]")
        sys.exit(1)

    database = sys.argv[1]
    reason = ""
    source = DEFAULT_SOURCE
    indices = []

    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == '--reason':
            reason = sys.argv[i + 1]
            i += 2
        elif sys.argv[i] == '--source':
            source = sys.argv[i + 1]
            i += 2
        else:
            indices.append(int(sys.argv[i]))
            i += 1

    move_to_failed_review(database, indices, reason, source)
