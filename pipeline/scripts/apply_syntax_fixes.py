#!/usr/bin/env python3
"""Apply validated syntax fixes to queries.csv files.

Reads a validated findings JSON file and applies the fixes atomically to CSV files.

Usage:
    python3 pipeline/scripts/apply_syntax_fixes.py /tmp/old_syntax_validated.json --dry-run
    python3 pipeline/scripts/apply_syntax_fixes.py /tmp/old_syntax_validated.json --apply
"""

import argparse
import csv
import json
import os
import shutil
import sys
import tempfile
from datetime import datetime


HEADERS = ['original_index', 'question', 'cypher', 'typeql']


def apply_fix_to_typeql(typeql: str, matched_text: str, validated_fix: str) -> str:
    """Replace the old syntax pattern with the new syntax in a TypeQL query."""
    return typeql.replace(matched_text, validated_fix)


def apply_fixes_to_database(source: str, database: str, fixes: list[dict], dry_run: bool) -> list[dict]:
    """Apply fixes to a single database's queries.csv.

    Args:
        source: Dataset source (e.g., 'synthetic-1')
        database: Database name
        fixes: List of fixes for this database
        dry_run: If True, don't actually modify files

    Returns:
        List of applied changes with database and original_index
    """
    csv_path = f"dataset/{source}/{database}/queries.csv"

    if not os.path.exists(csv_path):
        print(f"Warning: {csv_path} not found", file=sys.stderr)
        return []

    # Build a lookup from original_index to list of fixes (may have multiple per query)
    fix_lookup = {}
    for fix in fixes:
        idx = fix['original_index']
        if idx not in fix_lookup:
            fix_lookup[idx] = []
        fix_lookup[idx].append(fix)

    # Read all rows
    with open(csv_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    changes = []
    modified_rows = []

    for row in rows:
        original_index = int(row.get('original_index', -1))

        if original_index in fix_lookup:
            fixes_for_row = fix_lookup[original_index]
            old_typeql = row['typeql']
            new_typeql = old_typeql

            # Apply all fixes for this row
            for fix in fixes_for_row:
                new_typeql = apply_fix_to_typeql(
                    new_typeql,
                    fix['matched_text'],
                    fix['validated_fix']
                )

            if old_typeql != new_typeql:
                row = dict(row)  # Copy to avoid modifying original
                row['typeql'] = new_typeql
                changes.append({
                    'database': database,
                    'original_index': original_index,
                    'fixes_applied': len(fixes_for_row),
                })
                if not dry_run:
                    print(f"  Fixed {database}:{original_index} ({len(fixes_for_row)} patterns)")
                else:
                    print(f"  Would fix {database}:{original_index} ({len(fixes_for_row)} patterns)")
                    for fix in fixes_for_row:
                        print(f"    Old: {fix['matched_text']}")
                        print(f"    New: {fix['validated_fix']}")

        modified_rows.append(row)

    # Write back atomically (only if not dry run)
    if not dry_run and changes:
        dir_path = os.path.dirname(csv_path) or '.'
        with tempfile.NamedTemporaryFile(
            mode='w', newline='', encoding='utf-8',
            delete=False, dir=dir_path
        ) as tmp:
            writer = csv.DictWriter(tmp, fieldnames=HEADERS, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(modified_rows)
            tmp_path = tmp.name

        shutil.move(tmp_path, csv_path)

    return changes


def main():
    parser = argparse.ArgumentParser(description='Apply validated syntax fixes to CSV files')
    parser.add_argument('input_file', help='Path to validated findings JSON')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview changes without applying them')
    parser.add_argument('--apply', action='store_true',
                        help='Actually apply the changes')
    parser.add_argument('--output', help='Output JSON file for changes list',
                        default='/tmp/applied_changes.json')

    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        print("Error: Must specify --dry-run or --apply", file=sys.stderr)
        sys.exit(1)

    if args.dry_run and args.apply:
        print("Error: Cannot specify both --dry-run and --apply", file=sys.stderr)
        sys.exit(1)

    # Read validated findings
    with open(args.input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    validated_findings = data.get('validated_findings', [])
    source = data.get('source', 'synthetic-1')

    # Extract source from source_file path if present
    source_file = data.get('source_file', '')
    if 'synthetic-2' in source_file:
        source = 'synthetic-2'

    if not validated_findings:
        print("No validated findings to apply")
        return

    # Group fixes by database
    by_database = {}
    for finding in validated_findings:
        db = finding['database']
        if db not in by_database:
            by_database[db] = []
        by_database[db].append(finding)

    # Apply fixes
    all_changes = []
    for database, fixes in sorted(by_database.items()):
        print(f"\nProcessing {database} ({len(fixes)} fixes)...")
        changes = apply_fixes_to_database(source, database, fixes, args.dry_run)
        all_changes.extend(changes)

    # Write output
    result = {
        'applied_at': datetime.now().isoformat(),
        'source': source,
        'dry_run': args.dry_run,
        'changes': all_changes,
        'total_modified': len(all_changes),
    }

    if not args.dry_run:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2)
        print(f"\nApplied {len(all_changes)} fixes. Changes written to {args.output}")
    else:
        print(f"\nDry run: Would apply {len(all_changes)} fixes")


if __name__ == '__main__':
    main()
