#!/usr/bin/env python3
"""Merge all per-domain queries.csv files into a single dataset/all_queries.csv."""

import csv
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent.parent
DATASET_DIR = REPO_ROOT / "dataset"

DOMAINS = [
    "twitter",
    "twitch",
    "movies",
    "neoflix",
    "recommendations",
    "companies",
    "gameofthrones",
]

OUTPUT_FIELDS = ["domain", "original_index", "question", "cypher", "typeql"]


def merge():
    output_path = DATASET_DIR / "all_queries.csv"
    total = 0

    with open(output_path, "w", newline="") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()

        for domain in DOMAINS:
            domain_csv = DATASET_DIR / domain / "queries.csv"
            if not domain_csv.exists():
                print(f"WARNING: {domain_csv} not found, skipping", file=sys.stderr)
                continue

            count = 0
            with open(domain_csv, "r") as in_f:
                reader = csv.DictReader(in_f)
                for row in reader:
                    writer.writerow({
                        "domain": domain,
                        "original_index": row["original_index"],
                        "question": row["question"],
                        "cypher": row["cypher"],
                        "typeql": row["typeql"],
                    })
                    count += 1

            print(f"{domain}: {count} queries")
            total += count

    print(f"\nTotal: {total} queries written to {output_path}")


if __name__ == "__main__":
    merge()
