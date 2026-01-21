#!/usr/bin/env python3
"""Move truly unsupported queries from queries.csv to failed.csv."""

import csv

UNSUPPORTED = {
    71: "TypeQL does not support string length functions",
    95: "Complex aggregation (MAX within groupby) not supported in TypeQL",
    181: "SUM aggregation not supported in TypeQL",
    187: "MAX aggregation within groupby not supported in TypeQL",
}

queries_path = '/opt/text2typeql/output/twitch/queries.csv'
failed_path = '/opt/text2typeql/output/twitch/failed.csv'

# Read queries
with open(queries_path, 'r') as f:
    reader = csv.DictReader(f)
    queries = list(reader)

# Read failed
with open(failed_path, 'r') as f:
    reader = csv.DictReader(f)
    failed = list(reader)

# Find and move unsupported queries
new_queries = []
for row in queries:
    idx = int(row['original_index'])
    if idx in UNSUPPORTED:
        failed.append({
            'original_index': idx,
            'question': row['question'],
            'cypher': row['cypher'],
            'error': UNSUPPORTED[idx]
        })
        print(f'[{idx}] Moved to failed.csv: {UNSUPPORTED[idx]}')
    else:
        new_queries.append(row)

# Sort and write
new_queries.sort(key=lambda x: int(x['original_index']))
failed.sort(key=lambda x: int(x['original_index']))

with open(queries_path, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
    writer.writeheader()
    writer.writerows(new_queries)

with open(failed_path, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'error'])
    writer.writeheader()
    writer.writerows(failed)

print(f'\nTotal queries: {len(new_queries)}')
print(f'Total failed: {len(failed)}')
