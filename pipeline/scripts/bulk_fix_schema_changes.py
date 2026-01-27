#!/usr/bin/env python3
"""Bulk fix queries for schema changes: in_country -> location-contains."""

import csv
import re

def fix_typeql(typeql: str) -> str:
    """Apply schema-related fixes to a TypeQL query."""
    fixed = typeql

    # Pattern 1: in_country (city: $x, country: $y) -> location-contains (child: $x, parent: $y)
    def replace_in_country_prefix(match):
        content = match.group(1)
        content = re.sub(r'\bcity:', 'child:', content)
        content = re.sub(r'\bcountry:', 'parent:', content)
        return f'location-contains ({content})'

    fixed = re.sub(r'\bin_country\s*\(([^)]+)\)', replace_in_country_prefix, fixed)

    # Pattern 2: (city: $x, country: $y) isa in_country -> (child: $x, parent: $y) isa location-contains
    def replace_in_country_suffix(match):
        content = match.group(1)
        content = re.sub(r'\bcity:', 'child:', content)
        content = re.sub(r'\bcountry:', 'parent:', content)
        return f'({content}) isa location-contains'

    fixed = re.sub(r'\(([^)]+)\)\s*isa\s+in_country\b', replace_in_country_suffix, fixed)

    # Pattern 3: standalone 'isa in_country' without roles
    fixed = re.sub(r'\bisa\s+in_country\b', 'isa location-contains', fixed)

    return fixed

def main():
    input_file = "dataset/companies/queries.csv"

    # Read with proper multi-line handling
    with open(input_file, 'r', newline='') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames

    print(f"Read {len(rows)} queries")

    fixed_count = 0
    for row in rows:
        original = row['typeql']
        fixed = fix_typeql(original)

        if fixed != original:
            fixed_count += 1
            row['typeql'] = fixed

    # Write back to same file with proper quoting
    with open(input_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Fixed {fixed_count} queries")
    print(f"Written back to {input_file}")

    # Verify
    with open(input_file, 'r', newline='') as f:
        verify_count = len(list(csv.DictReader(f)))
    print(f"Verified: {verify_count} queries in file")

if __name__ == "__main__":
    main()
