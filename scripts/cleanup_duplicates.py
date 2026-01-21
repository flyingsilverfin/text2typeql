#!/usr/bin/env python3
"""Clean up duplicate entries in output CSV files.

For each database:
1. Keep only the latest entry for each original_index
2. Ensure each index appears in only one file (queries.csv, failed.csv, or failed_review.csv)
3. Priority: queries.csv > failed_review.csv > failed.csv
"""

import csv
import os
from collections import OrderedDict

DATABASES = ['twitter', 'twitch', 'recommendations', 'movies', 'neoflix', 'companies', 'gameofthrones']
OUTPUT_DIR = '/opt/text2typeql/output'

def read_csv_latest(filepath):
    """Read CSV and keep only the latest entry for each original_index."""
    if not os.path.exists(filepath):
        return {}

    entries = OrderedDict()
    with open(filepath, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            idx = int(row['original_index'])
            entries[idx] = row  # Later entries overwrite earlier ones

    return entries, fieldnames

def write_csv(filepath, entries, fieldnames):
    """Write entries to CSV, sorted by original_index."""
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for idx in sorted(entries.keys()):
            writer.writerow(entries[idx])

def cleanup_database(db):
    """Clean up a single database's CSV files."""
    queries_path = f'{OUTPUT_DIR}/{db}/queries.csv'
    failed_path = f'{OUTPUT_DIR}/{db}/failed.csv'
    review_path = f'{OUTPUT_DIR}/{db}/failed_review.csv'

    # Read all files
    queries = {}
    failed = {}
    review = {}
    q_fields = ['original_index', 'question', 'cypher', 'typeql']
    f_fields = ['original_index', 'question', 'cypher', 'error']
    r_fields = ['original_index', 'question', 'cypher', 'typeql', 'review_reason']

    if os.path.exists(queries_path):
        queries, q_fields = read_csv_latest(queries_path)
    if os.path.exists(failed_path):
        failed, f_fields = read_csv_latest(failed_path)
    if os.path.exists(review_path):
        review, r_fields = read_csv_latest(review_path)

    print(f"\n{db}:")
    print(f"  Before: queries={len(queries)}, failed={len(failed)}, review={len(review)}")

    # Remove duplicates across files
    # Priority: queries > review > failed
    all_indices = set(queries.keys()) | set(failed.keys()) | set(review.keys())

    final_queries = {}
    final_failed = {}
    final_review = {}

    for idx in all_indices:
        if idx in queries:
            final_queries[idx] = queries[idx]
        elif idx in review:
            final_review[idx] = review[idx]
        elif idx in failed:
            final_failed[idx] = failed[idx]

    print(f"  After:  queries={len(final_queries)}, failed={len(final_failed)}, review={len(final_review)}")
    print(f"  Total:  {len(final_queries) + len(final_failed) + len(final_review)}")

    # Write cleaned files
    write_csv(queries_path, final_queries, q_fields)
    write_csv(failed_path, final_failed, f_fields)
    write_csv(review_path, final_review, r_fields)

    return len(final_queries), len(final_failed), len(final_review)

def main():
    print("Cleaning up duplicate entries in output CSV files...")

    totals = {'queries': 0, 'failed': 0, 'review': 0}

    for db in DATABASES:
        q, f, r = cleanup_database(db)
        totals['queries'] += q
        totals['failed'] += f
        totals['review'] += r

    print(f"\n=== Final Totals ===")
    print(f"Queries: {totals['queries']}")
    print(f"Failed: {totals['failed']}")
    print(f"Review: {totals['review']}")
    print(f"Total: {totals['queries'] + totals['failed'] + totals['review']}")

if __name__ == '__main__':
    main()
