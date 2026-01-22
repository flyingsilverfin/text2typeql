#!/usr/bin/env python3
"""Bulk fix queries for schema changes: in_country -> location-contains."""

import csv
import re

def fix_typeql(typeql: str) -> str:
    """Apply schema-related fixes to a TypeQL query."""
    fixed = typeql

    # Fix relation name: in_country -> location-contains
    # And simultaneously fix the role names within that relation

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
    input_file = "output/companies/queries.csv"
    output_file = "output/companies/queries_fixed.csv"

    with open(input_file, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames

    fixed_count = 0
    fixed_rows = []

    for row in rows:
        original = row['typeql']
        fixed = fix_typeql(original)

        if fixed != original:
            fixed_count += 1
            print(f"Fixed index {row['original_index']}")

        row['typeql'] = fixed
        fixed_rows.append(row)

    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(fixed_rows)

    print(f"\n=== Summary ===")
    print(f"Total queries: {len(rows)}")
    print(f"Fixed: {fixed_count}")
    print(f"Output: {output_file}")

if __name__ == "__main__":
    main()
