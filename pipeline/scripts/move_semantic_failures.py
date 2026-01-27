#!/usr/bin/env python3
"""Move semantic failures to failed_review.csv with appropriate reasons."""

import csv
import json

# Load categorized issues
with open('/tmp/companies_categorized_issues.json') as f:
    categories = json.load(f)

# Map categories to reasons
reason_map = {
    'completely_wrong_query': 'TypeQL query is semantically different from the question',
    'missing_relation': 'TypeQL missing required relation for question',
    'missing_entity': 'TypeQL missing required entity type for question',
    'missing_attribute': 'TypeQL missing required attribute filter for question',
    'missing_aggregation': 'TypeQL missing required aggregation for question'
}

# Flatten to index -> reason mapping
index_reasons = {}
for cat, indices in categories.items():
    for idx in indices:
        index_reasons[str(idx)] = reason_map[cat]

# Read queries.csv
queries_path = '/opt/text2typeql/dataset/companies/queries.csv'
failed_path = '/opt/text2typeql/dataset/companies/failed_review.csv'

with open(queries_path, 'r') as f:
    rows = list(csv.DictReader(f))

# Separate into keep and move
keep = []
move = []
for row in rows:
    idx = row['original_index']
    if idx in index_reasons:
        row['review_reason'] = index_reasons[idx]
        move.append(row)
    else:
        keep.append(row)

print(f"Total queries: {len(rows)}")
print(f"Queries to keep: {len(keep)}")
print(f"Queries to move: {len(move)}")

# Write back queries.csv
with open(queries_path, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
    writer.writeheader()
    writer.writerows(keep)

# Write to failed_review.csv
with open(failed_path, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql', 'review_reason'])
    writer.writeheader()
    writer.writerows(move)

print(f"\nMoved {len(move)} queries to failed_review.csv")
print(f"Remaining in queries.csv: {len(keep)}")

# Print category breakdown
print("\n--- Category Breakdown ---")
for cat, indices in categories.items():
    print(f"{cat}: {len(indices)}")
