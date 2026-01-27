#!/usr/bin/env python3
"""Extract a batch of queries for a database from the source CSV.
Usage: get_batch.py <database> <start_index> <count> [--source synthetic-1|synthetic-2]
Returns JSON array of {index, question, cypher} objects."""

import csv
import json
import sys

SOURCES = {
    "synthetic-1": {
        "csv_path": "/opt/text2typeql/pipeline/data/text2cypher/datasets/synthetic_opus_demodbs/text2cypher_claudeopus.csv",
        "exclude_column": "false_schema",
        "exclude_check": "notempty",
    },
    "synthetic-2": {
        "csv_path": "/opt/text2typeql/pipeline/data/text2cypher/datasets/synthetic_gpt4o_demodbs/text2cypher_gpt4o.csv",
        "exclude_column": "no_cypher",
        "exclude_check": "true",
    },
}

def is_excluded(row: dict, source_config: dict) -> bool:
    col = source_config["exclude_column"]
    check = source_config["exclude_check"]
    value = row.get(col, "")
    if check == "notempty":
        return bool(value and str(value).strip())
    elif check == "true":
        return str(value).strip().lower() == "true"
    return False

def get_batch(database: str, start: int, count: int, source: str = "synthetic-1") -> list:
    config = SOURCES[source]
    csv_path = config["csv_path"]
    results = []
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        idx = 0
        for row in reader:
            if row['database'] != database:
                continue
            if row.get('syntax_error', '').lower() == 'true':
                continue
            if is_excluded(row, config):
                continue
            if idx >= start and idx < start + count:
                results.append({
                    'index': idx,
                    'question': row['question'],
                    'cypher': row['cypher']
                })
            if idx >= start + count:
                break
            idx += 1
    return results

if __name__ == '__main__':
    source = "synthetic-1"
    args = []
    i = 1
    while i < len(sys.argv):
        if sys.argv[i] == '--source' and i + 1 < len(sys.argv):
            source = sys.argv[i + 1]
            i += 2
        else:
            args.append(sys.argv[i])
            i += 1

    if len(args) != 3:
        print("Usage: get_batch.py <database> <start_index> <count> [--source synthetic-1|synthetic-2]", file=sys.stderr)
        sys.exit(1)

    if source not in SOURCES:
        print(f"Unknown source '{source}'. Available: {list(SOURCES.keys())}", file=sys.stderr)
        sys.exit(1)

    database = args[0]
    start = int(args[1])
    count = int(args[2])
    results = get_batch(database, start, count, source)
    print(json.dumps(results))
