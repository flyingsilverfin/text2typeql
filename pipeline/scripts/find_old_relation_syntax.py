#!/usr/bin/env python3
"""Find old-style TypeQL relation syntax in converted queries.

Detects patterns like:
  - $var (roles) isa type - old style with isa after roles
  - $var (roles); - missing relation type entirely

Usage:
    python3 pipeline/scripts/find_old_relation_syntax.py --source synthetic-1 --output /tmp/findings.json
    python3 pipeline/scripts/find_old_relation_syntax.py --source synthetic-1 --database twitter
    python3 pipeline/scripts/find_old_relation_syntax.py --source synthetic-1 --count
"""

import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime


# Pattern 1: $var (roles) isa type - old style with isa after roles
# Example: $r (follower: $a, followed: $b) isa follows
# This should be: $r isa follows (follower: $a, followed: $b)
OLD_STYLE_ISA = re.compile(
    r'\$(\w+)\s*\(([^)]+)\)\s+isa\s+(\w+)',
    re.IGNORECASE
)

# Pattern 2: $var (roles) without isa - variable with roles but no relation type
# Example: $r (follower: $a, followed: $b);
# Must have at least one role assignment (colon) to distinguish from other constructs
# Negative lookahead to avoid matching valid patterns that have isa later
MISSING_TYPE = re.compile(
    r'\$(\w+)\s*\(([^)]*\w+\s*:\s*\$\w+[^)]*)\)\s*([;,\n])',
)


DATABASES = ['twitter', 'twitch', 'movies', 'neoflix', 'recommendations', 'companies', 'gameofthrones']


def find_old_syntax_in_query(typeql: str, original_index: int, database: str) -> list[dict]:
    """Find old-style relation syntax patterns in a TypeQL query."""
    findings = []

    # Check for old-style isa pattern
    for match in OLD_STYLE_ISA.finditer(typeql):
        var_name = match.group(1)
        roles = match.group(2)
        rel_type = match.group(3)
        matched_text = match.group(0)

        # Skip if inside a string literal (crude check)
        before_match = typeql[:match.start()]
        quote_count = before_match.count('"') - before_match.count('\\"')
        if quote_count % 2 == 1:
            continue  # Inside a string literal

        # Generate fix: move isa before roles
        suggested_fix = f"${var_name} isa {rel_type} ({roles})"

        findings.append({
            'database': database,
            'original_index': original_index,
            'pattern_type': 'old_style_isa',
            'matched_text': matched_text,
            'suggested_fix': suggested_fix,
            'full_typeql': typeql,
        })

    # Check for missing type pattern (more rare, needs manual review)
    # This is trickier - we need to find $var (roles) patterns that don't have isa
    for match in MISSING_TYPE.finditer(typeql):
        var_name = match.group(1)
        roles = match.group(2)
        matched_text = f"${var_name} ({roles})"
        end_char = match.group(3)

        # Skip if this is actually followed by 'isa' (old style - already caught above)
        full_match = match.group(0)
        rest_of_query = typeql[match.end():]
        if rest_of_query.strip().startswith('isa'):
            continue

        # Skip if inside string literal
        before_match = typeql[:match.start()]
        quote_count = before_match.count('"') - before_match.count('\\"')
        if quote_count % 2 == 1:
            continue

        # Skip if this is a type variable (preceded by 'isa $var')
        # e.g., "$rel isa $t (role: $x)" - $t is a type variable, not a relation variable
        if re.search(rf'isa\s+\${var_name}\s*\(', typeql):
            continue  # This is a type variable pattern

        # Check if this variable is already defined with isa elsewhere
        # Pattern like "$r isa follows" somewhere in query
        if re.search(rf'\${var_name}\s+isa\s+\w+', typeql):
            continue  # Variable already typed elsewhere

        # This might be a legitimate pattern (role inference) or a bug
        # Mark for manual review
        findings.append({
            'database': database,
            'original_index': original_index,
            'pattern_type': 'missing_type',
            'matched_text': matched_text + end_char,
            'suggested_fix': f"${var_name} isa <RELATION_TYPE> ({roles}){end_char}",
            'full_typeql': typeql,
        })

    return findings


def scan_database(source: str, database: str) -> list[dict]:
    """Scan a single database's queries.csv for old syntax patterns."""
    csv_path = f"dataset/{source}/{database}/queries.csv"

    if not os.path.exists(csv_path):
        print(f"Warning: {csv_path} not found", file=sys.stderr)
        return []

    findings = []

    with open(csv_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            original_index = int(row.get('original_index', -1))
            typeql = row.get('typeql', '')

            row_findings = find_old_syntax_in_query(typeql, original_index, database)
            findings.extend(row_findings)

    return findings


def main():
    parser = argparse.ArgumentParser(description='Find old-style TypeQL relation syntax')
    parser.add_argument('--source', default='synthetic-1',
                        help='Source dataset (synthetic-1 or synthetic-2)')
    parser.add_argument('--database', help='Single database to scan (default: all)')
    parser.add_argument('--output', help='Output JSON file path')
    parser.add_argument('--count', action='store_true',
                        help='Only show summary counts')

    args = parser.parse_args()

    databases = [args.database] if args.database else DATABASES

    all_findings = []
    for db in databases:
        findings = scan_database(args.source, db)
        all_findings.extend(findings)
        if not args.count:
            print(f"Found {len(findings)} patterns in {db}", file=sys.stderr)

    # Categorize findings
    old_style_isa = [f for f in all_findings if f['pattern_type'] == 'old_style_isa']
    missing_type = [f for f in all_findings if f['pattern_type'] == 'missing_type']

    result = {
        'generated_at': datetime.now().isoformat(),
        'source': args.source,
        'summary': {
            'old_style_isa': len(old_style_isa),
            'missing_type': len(missing_type),
            'total': len(all_findings),
        },
        'findings': all_findings,
    }

    if args.count:
        print(f"Summary for {args.source}:")
        print(f"  old_style_isa: {len(old_style_isa)}")
        print(f"  missing_type:  {len(missing_type)}")
        print(f"  total:         {len(all_findings)}")
    elif args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2)
        print(f"Wrote {len(all_findings)} findings to {args.output}")
    else:
        # Print to stdout
        print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
