#!/usr/bin/env python3
"""
Fix the 140 failed semantic review queries in the neoflix database.

This script reads failed_review.csv and applies fixes based on review_reason patterns.

Common issue types:
- Missing date filters (before/after specific years, date ranges)
- Missing attribute value filters (budget, revenue, runtime, rating)
- Missing relation traversals (country, genre, produced_by)
- Missing job filters (Director, Producer)
- Wrong output (count instead of names)
- Missing sort clauses
- Missing fetch clauses
"""

import pandas as pd
import re
import sys


def add_date_filter(typeql: str, cypher: str, question: str) -> str:
    """Add missing date filter based on Cypher query."""
    # Extract date comparison from Cypher
    date_patterns = [
        (r"release_date\s*>=?\s*date\('(\d{4}-\d{2}-\d{2})'\)", '>='),
        (r"release_date\s*<=?\s*date\('(\d{4}-\d{2}-\d{2})'\)", '<='),
        (r"release_date\s*>\s*date\('(\d{4}-\d{2}-\d{2})'\)", '>'),
        (r"release_date\s*<\s*date\('(\d{4}-\d{2}-\d{2})'\)", '<'),
        (r"expires_at\s*>=?\s*date\('(\d{4}-\d{2}-\d{2})'\)", '>='),
        (r"expires_at\s*<\s*date\('(\d{4}-\d{2}-\d{2})'\)", '<'),
        (r"timestamp\s*>=?\s*date\('(\d{4}-\d{2}-\d{2})'\)", '>='),
    ]

    filters_to_add = []
    attr_name = 'release_date'

    for pattern, op in date_patterns:
        match = re.search(pattern, cypher)
        if match:
            date_val = match.group(1)
            if 'expires_at' in pattern:
                attr_name = 'expires_at'
            elif 'timestamp' in pattern:
                attr_name = 'timestamp'
            filters_to_add.append((op, date_val, attr_name))

    if not filters_to_add:
        return typeql

    # Find the attribute variable in TypeQL
    var_match = re.search(rf'has {attr_name} \$(\w+)', typeql)
    if not var_match:
        return typeql

    var_name = var_match.group(1)

    # Check if filter already exists
    for op, date_val, attr in filters_to_add:
        filter_pattern = rf'\${var_name}\s*{re.escape(op)}'
        if re.search(filter_pattern, typeql):
            continue

        # Add filter after the has attribute line
        lines = typeql.split('\n')
        result = []
        filter_inserted = False

        for line in lines:
            result.append(line)
            if f'has {attr} ${var_name}' in line and not filter_inserted:
                result.append(f'  ${var_name} {op} {date_val};')
                filter_inserted = True

        typeql = '\n'.join(result)

    return typeql


def add_value_filter(typeql: str, cypher: str, attr_name: str, value: str) -> str:
    """Add missing attribute value filter."""
    # Find the attribute variable in TypeQL
    var_match = re.search(rf'has {attr_name} \$(\w+)', typeql)
    if not var_match:
        return typeql

    var_name = var_match.group(1)

    # Check if filter already exists
    if re.search(rf'\${var_name}\s*(==|=|>|<|>=|<=)\s*{re.escape(str(value))}', typeql):
        return typeql

    # Add filter after the has attribute line
    lines = typeql.split('\n')
    result = []
    filter_inserted = False

    for line in lines:
        result.append(line)
        if f'has {attr_name} ${var_name}' in line and not filter_inserted:
            result.append(f'  ${var_name} == {value};')
            filter_inserted = True

    return '\n'.join(result)


def add_job_filter(typeql: str, job: str) -> str:
    """Add job filter to crew_for relation."""
    # Check if job filter already exists
    if f'has job "{job}"' in typeql or f"has job '{job}'" in typeql:
        return typeql

    # Add job filter to crew_for relation
    typeql = re.sub(
        r'(\) isa crew_for)(;|,)',
        rf'\1, has job "{job}"\2',
        typeql
    )

    return typeql


def add_missing_relation(typeql: str, cypher: str, relation_type: str) -> str:
    """Add missing relation traversal."""
    relation_configs = {
        'IN_GENRE': {
            'name': 'in_genre',
            'roles': ('media', 'genre'),
            'entity_patterns': [
                (r'\$(\w+)\s+isa\s+(movie|video|adult)', 'media'),
                (r'\$(\w+)\s+isa\s+genre', 'genre'),
            ]
        },
        'PRODUCED_BY': {
            'name': 'produced_by',
            'roles': ('media', 'producer'),
            'entity_patterns': [
                (r'\$(\w+)\s+isa\s+(movie|video|adult)', 'media'),
                (r'\$(\w+)\s+isa\s+production_company', 'producer'),
            ]
        },
        'PRODUCED_IN_COUNTRY': {
            'name': 'produced_in_country',
            'roles': ('media', 'country'),
            'entity_patterns': [
                (r'\$(\w+)\s+isa\s+(movie|video|adult)', 'media'),
                (r'\$(\w+)\s+isa\s+country', 'country'),
            ]
        },
        'IN_COLLECTION': {
            'name': 'in_collection',
            'roles': ('media', 'collection'),
            'entity_patterns': [
                (r'\$(\w+)\s+isa\s+(movie|video|adult)', 'media'),
                (r'\$(\w+)\s+isa\s+collection', 'collection'),
            ]
        },
    }

    if relation_type not in relation_configs:
        return typeql

    config = relation_configs[relation_type]

    # Check if relation already exists
    if f"isa {config['name']}" in typeql:
        return typeql

    # Find entity variables
    vars_found = {}
    for pattern, role in config['entity_patterns']:
        match = re.search(pattern, typeql, re.IGNORECASE)
        if match:
            vars_found[role] = f'${match.group(1)}'

    if len(vars_found) != 2:
        return typeql

    # Build relation line
    role1, role2 = config['roles']
    rel_line = f"  ({role1}: {vars_found[role1]}, {role2}: {vars_found[role2]}) isa {config['name']};"

    # Insert relation before reduce/fetch/sort/limit
    lines = typeql.split('\n')
    result = []
    relation_inserted = False

    for line in lines:
        stripped = line.strip()
        if (stripped.startswith('reduce') or stripped.startswith('fetch') or
            stripped.startswith('sort') or stripped.startswith('limit')) and not relation_inserted:
            result.append(rel_line)
            relation_inserted = True
        result.append(line)

    return '\n'.join(result)


def add_country_entity_and_relation(typeql: str, country_name: str) -> str:
    """Add country entity and relation for country filter."""
    # Check if country already exists
    if 'isa country' in typeql or 'produced_in_country' in typeql:
        return typeql

    # Find movie/video/adult entity
    media_match = re.search(r'\$(\w+)\s+isa\s+(movie|video|adult)', typeql, re.IGNORECASE)
    if not media_match:
        return typeql

    media_var = f'${media_match.group(1)}'

    # Check if this is a single-line query
    if '\n' not in typeql.strip() or typeql.count('\n') < 3:
        # Single-line or semi-single-line query
        # Insert after the entity declaration
        entity_pattern = rf'(\${media_match.group(1)}\s+isa\s+{media_match.group(2)});'
        country_insert = f"$country isa country, has country_name '{country_name}'; (media: {media_var}, country: $country) isa produced_in_country;"
        typeql = re.sub(entity_pattern, rf'\1; {country_insert}', typeql)
        return typeql

    # Multi-line query
    lines = typeql.split('\n')
    result = []
    inserted = False

    for i, line in enumerate(lines):
        result.append(line)
        # Insert after the media entity line
        if media_var in line and 'isa' in line and not inserted:
            if 'country' not in typeql:
                result.append(f"  $country isa country, has country_name '{country_name}';")
                result.append(f"  (media: {media_var}, country: $country) isa produced_in_country;")
                inserted = True

    return '\n'.join(result)


def add_genre_entity_and_relation(typeql: str, genre_name: str) -> str:
    """Add genre entity and relation for genre filter."""
    # Check if genre relation already exists
    if 'isa in_genre' in typeql:
        return typeql

    # Find movie entity
    media_match = re.search(r'\$(\w+)\s+isa\s+movie', typeql, re.IGNORECASE)
    if not media_match:
        return typeql

    media_var = f'${media_match.group(1)}'

    # Check if genre entity exists
    genre_match = re.search(r'\$(\w+)\s+isa\s+genre', typeql, re.IGNORECASE)

    if genre_match:
        genre_var = f'${genre_match.group(1)}'
        # Just add the relation
        lines = typeql.split('\n')
        result = []
        relation_inserted = False

        for line in lines:
            stripped = line.strip()
            if (stripped.startswith('reduce') or stripped.startswith('fetch') or
                stripped.startswith('sort') or stripped.startswith('limit')) and not relation_inserted:
                result.append(f"  (media: {media_var}, genre: {genre_var}) isa in_genre;")
                relation_inserted = True
            result.append(line)

        return '\n'.join(result)
    else:
        # Add genre entity and relation
        lines = typeql.split('\n')
        result = []

        for i, line in enumerate(lines):
            result.append(line)
            # Insert after the media entity line
            if media_var in line and 'isa movie' in line:
                result.append(f"  $genre isa genre, has genre_name '{genre_name}';")
                result.append(f"  (media: {media_var}, genre: $genre) isa in_genre;")

        return '\n'.join(result)


def add_fetch_clause(typeql: str, cypher: str) -> str:
    """Add missing fetch clause based on Cypher RETURN."""
    if 'fetch' in typeql.lower():
        return typeql

    # Try to determine what to fetch from Cypher RETURN
    return_match = re.search(r'RETURN\s+(.+?)(?:ORDER BY|LIMIT|$)', cypher, re.IGNORECASE | re.DOTALL)
    if not return_match:
        return typeql

    return_clause = return_match.group(1).strip()

    # Find variables in TypeQL that could be fetched
    title_match = re.search(r'has title \$(\w+)', typeql)

    if title_match:
        var_name = title_match.group(1)
        typeql = typeql.rstrip().rstrip(';') + ';\n'
        typeql += f'fetch {{\n  "title": ${var_name}\n}};'

    return typeql


def add_sort_clause(typeql: str, cypher: str, attr_name: str = None, direction: str = 'desc') -> str:
    """Add missing sort clause."""
    if 'sort ' in typeql.lower():
        return typeql

    # Find the attribute variable
    if attr_name:
        var_match = re.search(rf'has {attr_name} \$(\w+)', typeql)
        if var_match:
            var_name = var_match.group(1)

            lines = typeql.split('\n')
            result = []
            sort_inserted = False

            for line in lines:
                stripped = line.strip()
                if (stripped.startswith('fetch') or stripped.startswith('limit')) and not sort_inserted:
                    result.append(f'sort ${var_name} {direction};')
                    sort_inserted = True
                result.append(line)

            return '\n'.join(result)

    return typeql


def fix_query(row: pd.Series) -> str:
    """Apply fixes based on review_reason."""
    typeql = row['typeql']
    cypher = row['cypher']
    question = row['question']
    reason = row['review_reason']

    # 1. Missing date filters
    if 'Missing date filter' in reason or 'Missing date range filter' in reason:
        typeql = add_date_filter(typeql, cypher, question)

    # 2. Missing revenue = 0 filter
    if 'revenue = 0' in reason or 'revenue IS NULL' in reason:
        typeql = add_value_filter(typeql, cypher, 'revenue', '0')

    # 3. Missing budget filter
    if 'budget = 30000000' in reason:
        typeql = add_value_filter(typeql, cypher, 'budget', '30000000')
    if 'budget = 50000000' in reason:
        typeql = add_value_filter(typeql, cypher, 'budget', '50000000')

    # 4. Missing runtime filter
    if 'runtime = 90' in reason:
        typeql = add_value_filter(typeql, cypher, 'runtime', '90')

    # 5. Missing rating filter
    if 'rating = 5.0' in reason:
        typeql = add_value_filter(typeql, cypher, 'rating', '5.0')
    if 'rating = 10' in reason:
        typeql = add_value_filter(typeql, cypher, 'rating', '10')
    if 'rating > 8.0' in reason:
        # Need special handling for > comparison
        var_match = re.search(r'has rating \$(\w+)', typeql)
        if var_match and f'${var_match.group(1)} >' not in typeql:
            var_name = var_match.group(1)
            lines = typeql.split('\n')
            result = []
            for line in lines:
                result.append(line)
                if f'has rating ${var_name}' in line:
                    result.append(f'  ${var_name} > 8.0;')
            typeql = '\n'.join(result)

    # 6. Missing job filters
    if "job = 'Director'" in reason or 'job = "Director"' in reason:
        typeql = add_job_filter(typeql, 'Director')
    if "job = 'Producer'" in reason or 'job = "Producer"' in reason:
        typeql = add_job_filter(typeql, 'Producer')

    # 7. Missing country relation
    if 'filter by country' in reason.lower() or 'Missing relation to filter by country' in reason:
        # Extract country name from reason
        country_match = re.search(r"country '([^']+)'", reason)
        if country_match:
            country_name = country_match.group(1)
            typeql = add_country_entity_and_relation(typeql, country_name)

    # 8. Missing genre relation
    if 'filter by genre' in reason.lower():
        genre_match = re.search(r"genre '([^']+)'", reason)
        if genre_match:
            genre_name = genre_match.group(1)
            typeql = add_genre_entity_and_relation(typeql, genre_name)

    # 9. Missing produced_by relation
    if 'produced_by relation' in reason.lower() or 'Missing relation between production company' in reason:
        typeql = add_missing_relation(typeql, cypher, 'PRODUCED_BY')

    # 10. Missing in_collection relation
    if 'in_collection relation' in reason.lower() or 'Missing in_collection relation' in reason:
        typeql = add_missing_relation(typeql, cypher, 'IN_COLLECTION')

    # 11. Missing fetch clause
    if 'missing fetch clause' in reason.lower():
        typeql = add_fetch_clause(typeql, cypher)

    # 12. Missing sort by timestamp
    if 'sort by timestamp' in reason.lower():
        typeql = add_sort_clause(typeql, cypher, 'timestamp', 'desc')

    # 13. Missing gender filter
    if 'gender = 1' in reason:
        typeql = add_value_filter(typeql, cypher, 'gender', '1')

    # 14. Missing character filter
    if "character = 'Woody'" in reason or "character 'Woody'" in reason:
        typeql = add_value_filter(typeql, cypher, 'character', '"Woody"')

    # 15. Missing timestamp filter
    if 'timestamp filter' in reason.lower():
        typeql = add_date_filter(typeql, cypher, question)

    # 16. Missing sort by timestamp (different patterns)
    if 'Missing sort' in reason and 'timestamp' in reason.lower():
        typeql = add_sort_clause(typeql, cypher, 'timestamp', 'desc')

    # 17. Missing budget filter from cypher
    if 'Missing budget filter' in reason:
        budget_match = re.search(r'budget\s*=\s*(\d+)', cypher)
        if budget_match:
            typeql = add_value_filter(typeql, cypher, 'budget', budget_match.group(1))

    # 18. Missing sort by timestamp (additional patterns)
    if 'Missing sort' in reason and 'timestamp' in reason.lower():
        typeql = add_sort_clause(typeql, cypher, 'timestamp', 'desc')

    # 19. Missing in_collection relation (no entity exists)
    if 'Missing in_collection relation' in reason:
        # Check if collection entity exists
        if 'isa collection' not in typeql:
            media_match = re.search(r'\$(\w+)\s+isa\s+(movie|video)', typeql, re.IGNORECASE)
            if media_match:
                media_var = f'${media_match.group(1)}'
                # Insert collection entity and relation
                if '\n' not in typeql.strip() or typeql.count('\n') < 3:
                    # Single-line
                    entity_pattern = rf'(\${media_match.group(1)}\s+isa\s+{media_match.group(2)});'
                    coll_insert = f"$collection isa collection; (media: {media_var}, collection: $collection) isa in_collection;"
                    typeql = re.sub(entity_pattern, rf'\1; {coll_insert}', typeql)
                else:
                    lines = typeql.split('\n')
                    result = []
                    inserted = False
                    for line in lines:
                        result.append(line)
                        if media_var in line and 'isa' in line and not inserted:
                            result.append(f"  $collection isa collection;")
                            result.append(f"  (media: {media_var}, collection: $collection) isa in_collection;")
                            inserted = True
                    typeql = '\n'.join(result)

    return typeql


def main():
    print("Loading failed review queries...")
    failed_df = pd.read_csv('output/neoflix/failed_review.csv')
    print(f"Found {len(failed_df)} queries to fix")

    # Apply fixes
    print("\nApplying fixes...")
    fixed_count = 0

    for idx, row in failed_df.iterrows():
        original = row['typeql']
        fixed = fix_query(row)

        if fixed != original:
            failed_df.at[idx, 'typeql'] = fixed
            fixed_count += 1

    print(f"Fixed {fixed_count} queries")

    # Save fixed queries
    failed_df.to_csv('output/neoflix/failed_review_fixed.csv', index=False)
    print("Saved fixed queries to output/neoflix/failed_review_fixed.csv")

    # Load existing queries.csv and append/update
    print("\nUpdating queries.csv...")
    queries_df = pd.read_csv('output/neoflix/queries.csv')
    print(f"Existing queries in queries.csv: {len(queries_df)}")

    # Check if queries already exist by question text
    existing_questions = set(queries_df['question'])

    # Prepare fixed queries for appending
    new_queries = []
    for _, row in failed_df.iterrows():
        if row['question'] not in existing_questions:
            new_queries.append({
                'original_index': row['original_index'],
                'question': row['question'],
                'cypher': row['cypher'],
                'typeql': row['typeql']
            })

    if new_queries:
        new_df = pd.DataFrame(new_queries)
        queries_df = pd.concat([queries_df, new_df], ignore_index=True)
        print(f"Added {len(new_queries)} new queries to queries.csv")

    # Also update any existing queries that match by question
    updated_count = 0
    fixed_by_question = {row['question']: row['typeql'] for _, row in failed_df.iterrows()}
    for idx, row in queries_df.iterrows():
        if row['question'] in fixed_by_question:
            queries_df.at[idx, 'typeql'] = fixed_by_question[row['question']]
            updated_count += 1

    queries_df.to_csv('output/neoflix/queries.csv', index=False)
    print(f"Total queries in queries.csv: {len(queries_df)}")
    print(f"Updated {updated_count} queries based on question match")

    # Show sample of changes
    print("\n=== Sample fixes ===")
    for i, (idx, row) in enumerate(failed_df.head(5).iterrows()):
        print(f"\n--- Fix {i+1} ---")
        print(f"Reason: {row['review_reason']}")
        print(f"Fixed TypeQL:\n{row['typeql'][:500]}...")


if __name__ == '__main__':
    main()
