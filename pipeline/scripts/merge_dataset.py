#!/usr/bin/env python3
"""Merge per-domain queries.csv files into combined datasets.

Usage:
  merge_dataset.py                        # Merge all sources into dataset/all_queries.csv
  merge_dataset.py --source synthetic-1   # Merge one source into dataset/synthetic-1/all_queries.csv
  merge_dataset.py --source synthetic-2   # Merge one source into dataset/synthetic-2/all_queries.csv
"""

import csv
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent.parent
DATASET_DIR = REPO_ROOT / "dataset"

SOURCES_DOMAINS = {
    "synthetic-1": [
        "twitter", "twitch", "movies", "neoflix",
        "recommendations", "companies", "gameofthrones",
    ],
    "synthetic-2": [
        "twitter", "twitch", "movies", "neoflix",
        "recommendations", "companies", "gameofthrones",
        "bluesky", "buzzoverflow", "fincen", "grandstack",
        "network", "northwind", "offshoreleaks", "stackoverflow2",
    ],
}

SOURCE_FIELDS = ["domain", "original_index", "question", "cypher", "typeql"]
MERGED_FIELDS = ["source", "domain", "original_index", "question", "cypher", "typeql"]


def merge_source(source: str):
    """Merge all domain queries.csv for a single source."""
    domains = SOURCES_DOMAINS.get(source)
    if not domains:
        print(f"Unknown source '{source}'. Available: {list(SOURCES_DOMAINS.keys())}", file=sys.stderr)
        sys.exit(1)

    source_dir = DATASET_DIR / source
    output_path = source_dir / "all_queries.csv"
    total = 0

    with open(output_path, "w", newline="") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=SOURCE_FIELDS)
        writer.writeheader()

        for domain in domains:
            domain_csv = source_dir / domain / "queries.csv"
            if not domain_csv.exists():
                print(f"  SKIP: {domain_csv} not found", file=sys.stderr)
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

            print(f"  {domain}: {count} queries")
            total += count

    print(f"  Total: {total} queries -> {output_path}")
    return total


def merge_all():
    """Merge all sources into a single dataset/all_queries.csv."""
    output_path = DATASET_DIR / "all_queries.csv"
    grand_total = 0

    with open(output_path, "w", newline="") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=MERGED_FIELDS)
        writer.writeheader()

        for source, domains in SOURCES_DOMAINS.items():
            source_dir = DATASET_DIR / source
            source_total = 0

            for domain in domains:
                domain_csv = source_dir / domain / "queries.csv"
                if not domain_csv.exists():
                    continue

                with open(domain_csv, "r") as in_f:
                    reader = csv.DictReader(in_f)
                    for row in reader:
                        writer.writerow({
                            "source": source,
                            "domain": domain,
                            "original_index": row["original_index"],
                            "question": row["question"],
                            "cypher": row["cypher"],
                            "typeql": row["typeql"],
                        })
                        source_total += 1

            print(f"{source}: {source_total} queries")
            grand_total += source_total

    print(f"\nTotal: {grand_total} queries -> {output_path}")
    return grand_total


if __name__ == "__main__":
    source = None
    i = 1
    while i < len(sys.argv):
        if sys.argv[i] == '--source' and i + 1 < len(sys.argv):
            source = sys.argv[i + 1]
            i += 2
        else:
            print(f"Unknown argument: {sys.argv[i]}", file=sys.stderr)
            print(__doc__, file=sys.stderr)
            sys.exit(1)

    if source:
        print(f"Merging {source}:")
        merge_source(source)
    else:
        print("Merging all sources:")
        merge_all()
