#!/usr/bin/env python3
"""
Fix failed semantic review queries for the companies database.

This script addresses the following common issues:
1. Missing location chain (org -> city -> country)
2. Wrong revenue threshold (1 instead of actual millions/billions)
3. Missing sentiment aggregation
4. Wrong order of operations (limit before count)
5. Missing city filters (Seattle, Rome, Chicago, etc.)
6. Missing relationship connections (CEO, category, parent-child, etc.)
7. Wrong direction in supplier relationships
8. Missing date filters
9. Missing subsidiary/competitor relationships
10. Missing board member relationships
"""

import pandas as pd
import re
import csv
from typing import Optional, Tuple


def find_safe_insert_point(typeql: str) -> int:
    """
    Find a safe insertion point for new match clauses.
    Returns the index where new clauses should be inserted.
    Order of TypeQL: match ... ; [sort ...;] [reduce ...;] [limit ...;] [fetch ...;]

    We want to insert BEFORE sort/reduce/limit/fetch but AFTER all existing match clauses.
    """
    # Find the end of the match block (before any sort/reduce/limit/fetch)
    indices = []

    # Find all keywords that come after match clauses
    # Use word boundary to avoid matching 'reduce' inside words
    for keyword in ['sort ', 'sort\n', '\nsort', 'reduce ', 'reduce\n', '\nreduce',
                    'limit ', 'limit\n', '\nlimit', 'fetch ', 'fetch\n', '\nfetch', 'fetch{']:
        idx = typeql.find(keyword)
        if idx != -1:
            indices.append(idx)

    if indices:
        insert_idx = min(indices)
        # Make sure we're inserting after a semicolon and space
        # Find the last semicolon before the insert point
        last_semi = typeql.rfind(';', 0, insert_idx)
        if last_semi != -1:
            # Insert right after the last semicolon
            return last_semi + 1
        return insert_idx

    # If no keywords found, insert at the end
    return len(typeql)


def insert_clause_safely(typeql: str, clause: str) -> str:
    """
    Insert a clause at the safe insertion point.
    Handles formatting and whitespace.
    """
    insert_idx = find_safe_insert_point(typeql)

    # Ensure proper formatting
    clause = clause.strip()
    if not clause.endswith(';'):
        clause += ';'

    # Add newline if multiline query
    if '\n' in typeql:
        clause = '\n  ' + clause
    else:
        clause = ' ' + clause

    return typeql[:insert_idx] + clause + typeql[insert_idx:]


def fix_location_chain(typeql: str, cypher: str) -> str:
    """Add missing location chain: org -> city -> country"""
    # Check if there's already a proper location chain
    if '(city:' in typeql and 'country:' in typeql and 'in_country' in typeql and 'located_in' in typeql:
        return typeql

    # Find org variable
    org_match = re.search(r'(\$\w+) isa organization', typeql)
    org_var = org_match.group(1) if org_match else '$o'

    # Find country variable
    country_match = re.search(r'(\$\w+) isa country', typeql)
    country_var = country_match.group(1) if country_match else None

    # Find city variable
    city_match = re.search(r'(\$\w+) isa city', typeql)
    city_var = city_match.group(1) if city_match else None

    # Check if we have org -> city but missing city -> country
    if 'located_in' in typeql and 'in_country' not in typeql and country_var:
        # Need to add city -> country connection
        fetch_idx = typeql.find('fetch')
        reduce_idx = typeql.find('reduce')
        sort_idx = typeql.find('sort')
        limit_idx = typeql.find('limit')

        insert_idx = len(typeql)
        for idx in [fetch_idx, reduce_idx, sort_idx, limit_idx]:
            if idx != -1 and idx < insert_idx:
                insert_idx = idx

        # Find the city variable from located_in relation
        located_in_match = re.search(r'\(organization: (\$\w+), city: (\$\w+)\) isa located_in', typeql)
        if located_in_match:
            city_var = located_in_match.group(2)

        if city_var:
            typeql = typeql[:insert_idx] + f'(city: {city_var}, country: {country_var}) isa in_country; ' + typeql[insert_idx:]

    elif org_var and country_var and 'located_in' not in typeql:
        # Have org and country but missing the chain - need to add both relations
        fetch_idx = typeql.find('fetch')
        reduce_idx = typeql.find('reduce')
        sort_idx = typeql.find('sort')
        limit_idx = typeql.find('limit')

        insert_idx = len(typeql)
        for idx in [fetch_idx, reduce_idx, sort_idx, limit_idx]:
            if idx != -1 and idx < insert_idx:
                insert_idx = idx

        # Check if we need to add city variable
        if not city_var:
            # Rename country var if it conflicts with 'c'
            if country_var == '$c':
                # Replace everywhere including fetch clause
                typeql = typeql.replace('$c isa country', '$co isa country')
                typeql = typeql.replace('$c has country_name', '$co has country_name')
                typeql = typeql.replace('$c.country_name', '$co.country_name')
                typeql = typeql.replace('"country": $c', '"country": $co')
                country_var = '$co'

            city_var = '$city'
            # Use the safe insert helper
            clause = f'{city_var} isa city; (organization: {org_var}, city: {city_var}) isa located_in; (city: {city_var}, country: {country_var}) isa in_country'
            typeql = insert_clause_safely(typeql, clause)
        else:
            # City exists, add both relations
            typeql = typeql[:insert_idx] + f'(organization: {org_var}, city: {city_var}) isa located_in; (city: {city_var}, country: {country_var}) isa in_country; ' + typeql[insert_idx:]

    return typeql


def fix_revenue_threshold(typeql: str, cypher: str) -> str:
    """Fix incorrect revenue thresholds"""
    # Extract the actual threshold from Cypher
    # Patterns: 1E7, 1E8, 5e8, 1000000000, etc.
    cypher_threshold_patterns = [
        (r'revenue\s*[<>]=?\s*(\d+\.?\d*)[Ee](\d+)', lambda m: float(m.group(1)) * (10 ** int(m.group(2)))),
        (r'revenue\s*[<>]=?\s*(\d{6,})', lambda m: float(m.group(1))),
    ]

    actual_threshold = None
    for pattern, extractor in cypher_threshold_patterns:
        match = re.search(pattern, cypher)
        if match:
            actual_threshold = extractor(match)
            break

    if actual_threshold is None:
        return typeql

    # Find the wrong threshold in TypeQL
    # Patterns like: $o_revenue > 1; or $o_revenue < 5;
    typeql_threshold_pattern = r'(\$\w*revenue\w*)\s*([<>]=?)\s*(\d+\.?\d*)\s*;'
    match = re.search(typeql_threshold_pattern, typeql)

    if match:
        var_name = match.group(1)
        operator = match.group(2)
        old_value = match.group(3)

        new_value = f"{int(actual_threshold)}"

        old_expr = f"{var_name} {operator} {old_value};"
        new_expr = f"{var_name} {operator} {new_value};"
        typeql = typeql.replace(old_expr, new_expr)

    return typeql


def fix_sentiment_aggregation(typeql: str, cypher: str) -> str:
    """Add proper sentiment aggregation and sorting"""
    # Check if this is a sentiment aggregation query
    if 'avg(a.sentiment)' not in cypher.lower() and 'avgsentiment' not in cypher.lower():
        return typeql

    # Check if TypeQL is missing the aggregation
    if 'reduce' in typeql and 'mean' in typeql:
        return typeql  # Already has aggregation

    # Determine sort order
    sort_order = 'desc' if 'DESC' in cypher else 'asc'

    # Find limit value
    limit_match = re.search(r'LIMIT\s+(\d+)', cypher)
    limit_val = limit_match.group(1) if limit_match else '10'

    new_typeql = f"""match $o isa organization, has name $n; $a isa article, has sentiment $s; (article: $a, organization: $o) isa mentions; reduce $avg_sentiment = mean($s) groupby $o; match $o has name $n; sort $avg_sentiment {sort_order}; limit {limit_val}; fetch {{ "organization": $n, "avgSentiment": $avg_sentiment }};"""

    return new_typeql


def fix_order_of_operations(typeql: str, cypher: str) -> str:
    """Fix queries where limit comes before count - need to count, sort, then limit"""
    # Check if this is a count with order and limit query
    if 'count(' not in cypher.lower():
        return typeql

    # Check if TypeQL has limit before reduce
    limit_before_reduce = re.search(r'limit\s+\d+\s*;\s*reduce', typeql)
    if not limit_before_reduce:
        return typeql

    # This needs to be restructured: count per group, sort, then limit
    # Determine what we're counting and grouping by
    limit_match = re.search(r'LIMIT\s+(\d+)', cypher)
    limit_val = limit_match.group(1) if limit_match else '5'

    # Case 1: Count articles per organization
    if 'count(a)' in cypher.lower() and 'Organization' in cypher:
        return f"""match $o isa organization, has name $n; $a isa article; (article: $a, organization: $o) isa mentions; reduce $mentions = count($a) groupby $o; match $o has name $n; sort $mentions desc; limit {limit_val}; fetch {{ "organization": $n, "mentions": $mentions }};"""

    # Case 2: Count organizations per category
    if 'count(o)' in cypher.lower() and 'IndustryCategory' in cypher:
        return f"""match $ic isa industry_category, has industry_category_name $name; $o isa organization; (organization: $o, category: $ic) isa in_category; reduce $numOrganizations = count($o) groupby $ic; match $ic has industry_category_name $name; sort $numOrganizations desc; limit {limit_val}; fetch {{ "industry": $name, "numOrganizations": $numOrganizations }};"""

    return typeql


def fix_city_filter(typeql: str, cypher: str, city_name: str) -> str:
    """Add missing city filter"""
    # Check if city is already properly filtered
    if f'has city_name "{city_name}"' in typeql:
        return typeql

    # Need to add city filter and location relation
    # Find where organization is matched
    org_match = re.search(r'\$o isa organization[^;]*;', typeql)
    if not org_match:
        return typeql

    # Check if we already have city variable
    if '$c isa city' in typeql or '$city isa city' in typeql:
        # Just add the name filter
        city_var = '$c' if '$c isa city' in typeql else '$city'
        # Find where city is defined and add name filter
        city_def = re.search(rf'{re.escape(city_var)} isa city[^;]*;', typeql)
        if city_def:
            old_def = city_def.group(0)
            new_def = old_def.replace(';', f', has city_name "{city_name}";')
            typeql = typeql.replace(old_def, new_def)

        # Also add location relationship if missing
        if 'located_in' not in typeql:
            fetch_idx = typeql.find('fetch')
            if fetch_idx != -1:
                typeql = typeql[:fetch_idx] + f'(organization: $o, city: {city_var}) isa located_in; ' + typeql[fetch_idx:]
    else:
        # Need to add city variable, filter, and location relation
        insert_pos = org_match.end()
        city_clause = f' $city isa city, has city_name "{city_name}"; (organization: $o, city: $city) isa located_in;'
        typeql = typeql[:insert_pos] + city_clause + typeql[insert_pos:]

    return typeql


def fix_parent_child_relationship(typeql: str, cypher: str) -> str:
    """Add missing parent_of relationship"""
    # Check if we need parent-child connection
    if 'HAS_CHILD' not in cypher and 'HAS_PARENT' not in cypher:
        return typeql

    # Check if already has the relationship
    if 'parent_of' in typeql:
        return typeql

    # Find all person variables
    person_matches = re.findall(r'(\$\w+) isa person', typeql)

    if len(person_matches) >= 2:
        # Determine which is parent and which is child based on Cypher direction
        if '-[:HAS_CHILD]->' in cypher or '-[:HAS_CHILD*' in cypher:
            # p -> child, so first person is parent
            parent_var = person_matches[0]
            child_var = person_matches[1]
        elif '-[:HAS_PARENT]->' in cypher or '-[:HAS_PARENT*' in cypher:
            # p -> parent, so first person is child
            child_var = person_matches[0]
            parent_var = person_matches[1]
        else:
            # Default: first is parent
            parent_var = person_matches[0]
            child_var = person_matches[1]

        fetch_idx = typeql.find('fetch')
        limit_idx = typeql.find('limit')
        insert_idx = min(fetch_idx if fetch_idx != -1 else len(typeql),
                        limit_idx if limit_idx != -1 else len(typeql))

        relationship = f'(parent: {parent_var}, child: {child_var}) isa parent_of; '
        typeql = typeql[:insert_idx] + relationship + typeql[insert_idx:]

    elif len(person_matches) == 1:
        # Only one person found, might need to handle special cases
        person_var = person_matches[0]
        # Check for child/parent variable names
        if '$child' in typeql:
            fetch_idx = typeql.find('fetch')
            limit_idx = typeql.find('limit')
            insert_idx = min(fetch_idx if fetch_idx != -1 else len(typeql),
                            limit_idx if limit_idx != -1 else len(typeql))
            relationship = f'(parent: {person_var}, child: $child) isa parent_of; '
            typeql = typeql[:insert_idx] + relationship + typeql[insert_idx:]
        elif '$parent' in typeql:
            fetch_idx = typeql.find('fetch')
            limit_idx = typeql.find('limit')
            insert_idx = min(fetch_idx if fetch_idx != -1 else len(typeql),
                            limit_idx if limit_idx != -1 else len(typeql))
            relationship = f'(parent: $parent, child: {person_var}) isa parent_of; '
            typeql = typeql[:insert_idx] + relationship + typeql[insert_idx:]

    return typeql


def fix_ceo_relationship(typeql: str, cypher: str) -> str:
    """Add missing ceo_of relationship"""
    # Check if Cypher has CEO relationship
    if 'HAS_CEO' not in cypher:
        return typeql

    # Check if TypeQL already has it
    if 'ceo_of' in typeql:
        return typeql

    # Find org and person variables
    org_pattern = re.search(r'(\$\w+) isa organization', typeql)
    person_pattern = re.search(r'(\$(?:ceo|p|person)\w*) isa person', typeql)

    if org_pattern and person_pattern:
        org_var = org_pattern.group(1)
        person_var = person_pattern.group(1)

        # Add ceo_of relationship
        fetch_idx = typeql.find('fetch')
        limit_idx = typeql.find('limit')
        reduce_idx = typeql.find('reduce')

        insert_idx = len(typeql)
        for idx in [fetch_idx, limit_idx, reduce_idx]:
            if idx != -1 and idx < insert_idx:
                insert_idx = idx

        if insert_idx > 0:
            relationship = f'(organization: {org_var}, ceo: {person_var}) isa ceo_of; '
            typeql = typeql[:insert_idx] + relationship + typeql[insert_idx:]

    return typeql


def fix_org_category_relationship(typeql: str, cypher: str) -> str:
    """Add missing in_category relationship"""
    # Check if Cypher has category relationship
    if 'HAS_CATEGORY' not in cypher:
        return typeql

    # Check if TypeQL already has it
    if 'in_category' in typeql:
        return typeql

    # Find org and category variables
    org_pattern = re.search(r'(\$\w+) isa organization', typeql)
    cat_pattern = re.search(r'(\$\w+) isa industry_category', typeql)

    if org_pattern and cat_pattern:
        org_var = org_pattern.group(1)
        cat_var = cat_pattern.group(1)
        relationship = f'(organization: {org_var}, category: {cat_var}) isa in_category'
        typeql = insert_clause_safely(typeql, relationship)
    elif org_pattern:
        # Need to add category variable
        org_var = org_pattern.group(1)
        # Check if there's a category name in Cypher
        cat_name_match = re.search(r"IndustryCategory\s*\{name:\s*['\"]([^'\"]+)['\"]\}", cypher, re.IGNORECASE)
        if cat_name_match:
            cat_name = cat_name_match.group(1)
            clause = f'$ic isa industry_category, has industry_category_name "{cat_name}"; (organization: {org_var}, category: $ic) isa in_category'
        else:
            clause = f'$ic isa industry_category; (organization: {org_var}, category: $ic) isa in_category'
        typeql = insert_clause_safely(typeql, clause)

    return typeql


def fix_supplier_direction(typeql: str, cypher: str, question: str) -> str:
    """Fix reversed supplier/customer roles"""
    # Check if this is a supplier query
    if 'supplies' not in typeql:
        return typeql

    question_lower = question.lower()

    org_name_match = re.search(r"'([^']+)'", cypher)
    if not org_name_match:
        # Check for general supplier-to-public pattern
        if 'supplier' in question_lower and 'to public' in question_lower:
            if '(customer: $supplier, supplier: $company)' in typeql:
                typeql = typeql.replace(
                    '(customer: $supplier, supplier: $company)',
                    '(supplier: $supplier, customer: $company)'
                )
        return typeql

    org_name = org_name_match.group(1)

    # Check Cypher direction
    if f"-[:HAS_SUPPLIER]->" in cypher:
        # org->supplier means org IS SUPPLIED BY supplier
        # So org is customer, other is supplier
        # If TypeQL has org as supplier, it's wrong
        if f'has name "{org_name}"' in typeql:
            # Check if $o (the named org) is incorrectly set as supplier
            if '(customer: $o, supplier:' in typeql:
                # This seems correct - org is customer
                pass
            elif '(supplier: $o, customer:' in typeql or '(customer:' in typeql and ', supplier: $o)' in typeql:
                # This is wrong - need to flip
                typeql = re.sub(
                    r'\(supplier: \$o, customer: (\$\w+)\)',
                    r'(customer: $o, supplier: \1)',
                    typeql
                )
                typeql = re.sub(
                    r'\(customer: (\$\w+), supplier: \$o\)',
                    r'(supplier: \1, customer: $o)',
                    typeql
                )

    elif f"<-[:HAS_SUPPLIER]-" in cypher:
        # supplier<-org means supplier IS SUPPLYING TO org
        # So org is customer (being supplied to), supplier provides
        if f'has name "{org_name}"' in typeql:
            # The named org should be customer
            if '(customer: $supplier, supplier: $o)' in typeql:
                # Wrong - flip it
                typeql = typeql.replace(
                    '(customer: $supplier, supplier: $o)',
                    '(supplier: $supplier, customer: $o)'
                )

    # Handle "Which organizations does X supply?"
    if 'does' in question_lower and 'supply' in question_lower and org_name:
        # X supplies to others, so X is supplier, others are customers
        if f'has name "{org_name}"' in typeql:
            if '(customer: $o, supplier:' in typeql:
                # Wrong - named org should be supplier
                typeql = re.sub(
                    r'\(customer: \$o, supplier: (\$\w+)\)',
                    r'(supplier: $o, customer: \1)',
                    typeql
                )

    return typeql


def fix_date_filter(typeql: str, cypher: str) -> str:
    """Add missing date filters"""
    # Extract date from Cypher - multiple patterns
    date_patterns = [
        (r"date\s*>\s*datetime\(['\"](\d{4}-\d{2}-\d{2})['\"]", '>'),
        (r"date\s*<\s*datetime\(['\"](\d{4}-\d{2}-\d{2})['\"]", '<'),
        (r"date\s*>=\s*datetime\(['\"](\d{4}-\d{2}-\d{2})['\"]", '>='),
        (r"date\s*<=\s*datetime\(['\"](\d{4}-\d{2}-\d{2})['\"]", '<='),
        (r"date\s*>\s*date\(['\"](\d{4}-\d{2}-\d{2})['\"]", '>'),
        (r"date\s*<\s*date\(['\"](\d{4}-\d{2}-\d{2})['\"]", '<'),
        (r"a\.date\s*>\s*datetime\(['\"](\d{4}-\d{2}-\d{2})['\"]", '>'),
        (r"a\.date\s*<\s*datetime\(['\"](\d{4}-\d{2}-\d{2})['\"]", '<'),
        (r"a\.date\s*>\s*date\(['\"](\d{4}-\d{2}-\d{2})['\"]", '>'),
        (r"a\.date\s*<\s*date\(['\"](\d{4}-\d{2}-\d{2})['\"]", '<'),
    ]

    date_value = None
    operator = None
    for pattern, op in date_patterns:
        match = re.search(pattern, cypher, re.IGNORECASE)
        if match:
            date_value = match.group(1)
            operator = op
            break

    if not date_value:
        return typeql

    # Check if TypeQL already has date filter with same operator
    if f'{operator} {date_value}' in typeql:
        return typeql

    # Add date filter
    # First make sure we have date variable
    if '$a has date $a_date' not in typeql and 'has date $a_date' not in typeql:
        # Add date attribute
        article_match = re.search(r'\$a isa article[^;]*;', typeql)
        if article_match:
            old = article_match.group(0)
            if 'has date' not in old:
                new = old.replace(';', ', has date $a_date;')
                typeql = typeql.replace(old, new)

    # Find date variable name
    date_var_match = re.search(r'has date (\$\w+)', typeql)
    date_var = date_var_match.group(1) if date_var_match else '$a_date'

    # Add the comparison
    fetch_idx = typeql.find('fetch')
    sort_idx = typeql.find('sort')
    limit_idx = typeql.find('limit')

    insert_idx = len(typeql)
    for idx in [fetch_idx, sort_idx, limit_idx]:
        if idx != -1 and idx < insert_idx:
            insert_idx = idx

    date_filter = f'{date_var} {operator} {date_value}T00:00:00; '
    typeql = typeql[:insert_idx] + date_filter + typeql[insert_idx:]

    return typeql


def fix_not_dissolved_filter(typeql: str, cypher: str) -> str:
    """Fix is_dissolved filter to check for false, not just existence"""
    # Check if Cypher filters for not dissolved
    if 'NOT o.isDissolved' not in cypher and 'isDissolved = false' not in cypher.lower():
        return typeql

    # Check if TypeQL has the wrong check (existence instead of value)
    if 'has is_dissolved $' in typeql and 'has is_dissolved false' not in typeql:
        # Replace existence check with value check
        typeql = re.sub(
            r'has is_dissolved \$\w+',
            'has is_dissolved false',
            typeql
        )

    return typeql


def fix_subsidiary_relationship(typeql: str, cypher: str) -> str:
    """Add missing subsidiary_of relationship"""
    if 'HAS_SUBSIDIARY' not in cypher:
        return typeql

    if 'subsidiary_of' in typeql:
        return typeql

    # Find the org variables
    org_matches = re.findall(r'(\$\w+) isa organization', typeql)

    # Different patterns in Cypher
    if '-[:HAS_SUBSIDIARY]->' in cypher or '-[:HAS_SUBSIDIARY*' in cypher:
        # First org is parent, second is subsidiary
        if len(org_matches) >= 2:
            parent_var = org_matches[0]
            subsidiary_var = org_matches[1]

            fetch_idx = typeql.find('fetch')
            limit_idx = typeql.find('limit')

            insert_idx = len(typeql)
            for idx in [fetch_idx, limit_idx]:
                if idx != -1 and idx < insert_idx:
                    insert_idx = idx

            if insert_idx > 0:
                relationship = f'(parent: {parent_var}, subsidiary: {subsidiary_var}) isa subsidiary_of; '
                typeql = typeql[:insert_idx] + relationship + typeql[insert_idx:]
        elif len(org_matches) == 1:
            # Only one org, need to add subsidiary org variable
            org_var = org_matches[0]
            org_match = re.search(rf'{re.escape(org_var)} isa organization[^;]*;', typeql)
            if org_match:
                insert_pos = org_match.end()
                typeql = (typeql[:insert_pos] +
                         f' $subsidiary isa organization; (parent: {org_var}, subsidiary: $subsidiary) isa subsidiary_of;' +
                         typeql[insert_pos:])

    elif '<-[:HAS_SUBSIDIARY]-' in cypher or '<-[:HAS_SUBSIDIARY*' in cypher:
        # First org is subsidiary, second is parent
        if len(org_matches) >= 2:
            subsidiary_var = org_matches[0]
            parent_var = org_matches[1]

            fetch_idx = typeql.find('fetch')
            limit_idx = typeql.find('limit')

            insert_idx = len(typeql)
            for idx in [fetch_idx, limit_idx]:
                if idx != -1 and idx < insert_idx:
                    insert_idx = idx

            if insert_idx > 0:
                relationship = f'(parent: {parent_var}, subsidiary: {subsidiary_var}) isa subsidiary_of; '
                typeql = typeql[:insert_idx] + relationship + typeql[insert_idx:]

    return typeql


def fix_competitor_relationship(typeql: str, cypher: str) -> str:
    """Add missing competes_with relationship"""
    if 'HAS_COMPETITOR' not in cypher:
        return typeql

    if 'competes_with' in typeql:
        return typeql

    # Find org variables
    org_matches = re.findall(r'(\$\w+) isa organization', typeql)
    if len(org_matches) >= 2:
        org1_var = org_matches[0]
        org2_var = org_matches[1]

        fetch_idx = typeql.find('fetch')
        limit_idx = typeql.find('limit')

        insert_idx = len(typeql)
        for idx in [fetch_idx, limit_idx]:
            if idx != -1 and idx < insert_idx:
                insert_idx = idx

        if insert_idx > 0:
            relationship = f'(competitor: {org1_var}, competitor: {org2_var}) isa competes_with; '
            typeql = typeql[:insert_idx] + relationship + typeql[insert_idx:]

    return typeql


def fix_board_member_relationship(typeql: str, cypher: str) -> str:
    """Add missing board_member_of relationship"""
    if 'HAS_BOARD_MEMBER' not in cypher:
        return typeql

    if 'board_member_of' in typeql:
        return typeql

    # Find org and person variables
    org_pattern = re.search(r'(\$\w+) isa organization', typeql)
    person_pattern = re.search(r'(\$\w+) isa person', typeql)

    if org_pattern and person_pattern:
        org_var = org_pattern.group(1)
        person_var = person_pattern.group(1)

        fetch_idx = typeql.find('fetch')
        limit_idx = typeql.find('limit')

        insert_idx = len(typeql)
        for idx in [fetch_idx, limit_idx]:
            if idx != -1 and idx < insert_idx:
                insert_idx = idx

        if insert_idx > 0:
            relationship = f'(organization: {org_var}, member: {person_var}) isa board_member_of; '
            typeql = typeql[:insert_idx] + relationship + typeql[insert_idx:]

    return typeql


def fix_investor_relationship(typeql: str, cypher: str) -> str:
    """Add missing invested_in relationship"""
    if 'HAS_INVESTOR' not in cypher:
        return typeql

    if 'invested_in' in typeql:
        return typeql

    # Find org and person/org variables
    org_pattern = re.search(r'(\$\w+) isa organization', typeql)
    person_pattern = re.search(r'(\$\w+) isa person', typeql)

    if org_pattern:
        org_var = org_pattern.group(1)
        investor_var = person_pattern.group(1) if person_pattern else None

        # Check for second org as investor
        org_matches = re.findall(r'(\$\w+) isa organization', typeql)
        if len(org_matches) >= 2 and not investor_var:
            investor_var = org_matches[1]

        if investor_var:
            fetch_idx = typeql.find('fetch')
            if fetch_idx != -1:
                relationship = f'(organization: {org_var}, investor: {investor_var}) isa invested_in; '
                typeql = typeql[:fetch_idx] + relationship + typeql[fetch_idx:]

    return typeql


def fix_org_city_relationship(typeql: str, cypher: str) -> str:
    """Add missing located_in relationship between org and city"""
    if 'IN_CITY' not in cypher:
        return typeql

    if 'located_in' in typeql:
        return typeql

    # Find org and city variables
    org_pattern = re.search(r'(\$\w+) isa organization', typeql)
    city_pattern = re.search(r'(\$\w+) isa city', typeql)

    if org_pattern and city_pattern:
        org_var = org_pattern.group(1)
        city_var = city_pattern.group(1)
        relationship = f'(organization: {org_var}, city: {city_var}) isa located_in'
        typeql = insert_clause_safely(typeql, relationship)
    elif org_pattern:
        # Need to add city variable too
        org_var = org_pattern.group(1)
        clause = f'$city isa city; (organization: {org_var}, city: $city) isa located_in'
        typeql = insert_clause_safely(typeql, clause)

    return typeql


def fix_supplier_relationship(typeql: str, cypher: str) -> str:
    """Add missing supplies relationship"""
    if 'HAS_SUPPLIER' not in cypher:
        return typeql

    if 'supplies' in typeql:
        return typeql

    # Find org variables
    org_matches = re.findall(r'(\$\w+) isa organization', typeql)
    if len(org_matches) >= 2:
        org1_var = org_matches[0]
        org2_var = org_matches[1]

        # Determine direction from Cypher
        if '-[:HAS_SUPPLIER]->' in cypher:
            # org1 is customer, org2 is supplier
            relationship = f'(customer: {org1_var}, supplier: {org2_var}) isa supplies'
        else:
            relationship = f'(supplier: {org1_var}, customer: {org2_var}) isa supplies'
        typeql = insert_clause_safely(typeql, relationship)
    elif len(org_matches) == 1:
        # Need to add second org variable
        org_var = org_matches[0]
        clause = f'$supplier isa organization; (customer: {org_var}, supplier: $supplier) isa supplies'
        typeql = insert_clause_safely(typeql, clause)

    return typeql


def extract_city_name_from_cypher(cypher: str) -> Optional[str]:
    """Extract city name from Cypher query."""
    match = re.search(r"City\s*\{name:\s*['\"]([^'\"]+)['\"]\}", cypher, re.IGNORECASE)
    return match.group(1) if match else None


def extract_person_name_from_cypher(cypher: str) -> Optional[str]:
    """Extract person name from Cypher query."""
    match = re.search(r"Person\s*\{name:\s*['\"]([^'\"]+)['\"]\}", cypher, re.IGNORECASE)
    return match.group(1) if match else None


def extract_org_name_from_cypher(cypher: str) -> Optional[str]:
    """Extract organization name from Cypher query."""
    match = re.search(r"Organization\s*\{name:\s*['\"]([^'\"]+)['\"]\}", cypher, re.IGNORECASE)
    return match.group(1) if match else None


def fix_missing_person_filter(typeql: str, cypher: str) -> str:
    """Add missing person name filter."""
    person_name = extract_person_name_from_cypher(cypher)
    if not person_name:
        return typeql

    # Check if already filtered
    if f'has name "{person_name}"' in typeql:
        return typeql

    # Find person variable
    person_match = re.search(r'(\$\w+) isa person', typeql)
    if not person_match:
        return typeql

    person_var = person_match.group(1)

    # Add the name filter
    old_pattern = re.search(rf'{re.escape(person_var)} isa person[^;]*;', typeql)
    if old_pattern:
        old_def = old_pattern.group(0)
        if 'has name' not in old_def:
            new_def = old_def.replace(';', f', has name "{person_name}";')
            typeql = typeql.replace(old_def, new_def)

    return typeql


def fix_missing_org_filter(typeql: str, cypher: str) -> str:
    """Add missing organization name filter."""
    org_name = extract_org_name_from_cypher(cypher)
    if not org_name:
        return typeql

    # Check if already filtered
    if f'has name "{org_name}"' in typeql:
        return typeql

    # Find org variable
    org_match = re.search(r'(\$\w+) isa organization', typeql)
    if not org_match:
        return typeql

    org_var = org_match.group(1)

    # Add the name filter
    old_pattern = re.search(rf'{re.escape(org_var)} isa organization[^;]*;', typeql)
    if old_pattern:
        old_def = old_pattern.group(0)
        if 'has name' not in old_def:
            new_def = old_def.replace(';', f', has name "{org_name}";')
            typeql = typeql.replace(old_def, new_def)

    return typeql


def fix_mentions_relationship(typeql: str, cypher: str) -> str:
    """Add missing mentions relationship between article and organization."""
    if 'MENTIONS' not in cypher:
        return typeql

    if 'mentions' in typeql:
        return typeql

    # Find article and org variables
    article_match = re.search(r'(\$\w+) isa article', typeql)
    org_match = re.search(r'(\$\w+) isa organization', typeql)

    if article_match and org_match:
        article_var = article_match.group(1)
        org_var = org_match.group(1)

        # Find insertion point
        fetch_idx = typeql.find('fetch')
        limit_idx = typeql.find('limit')
        reduce_idx = typeql.find('reduce')
        sort_idx = typeql.find('sort')

        insert_idx = len(typeql)
        for idx in [fetch_idx, limit_idx, reduce_idx, sort_idx]:
            if idx != -1 and idx < insert_idx:
                insert_idx = idx

        if insert_idx > 0:
            relationship = f'(article: {article_var}, organization: {org_var}) isa mentions; '
            typeql = typeql[:insert_idx] + relationship + typeql[insert_idx:]

    return typeql


def fix_in_country_relationship(typeql: str, cypher: str) -> str:
    """Add missing in_country relationship between city and country."""
    if 'IN_COUNTRY' not in cypher:
        return typeql

    if 'in_country' in typeql:
        return typeql

    # Find city and country variables
    city_match = re.search(r'(\$\w+) isa city', typeql)
    country_match = re.search(r'(\$\w+) isa country', typeql)

    if city_match and country_match:
        city_var = city_match.group(1)
        country_var = country_match.group(1)

        # Find insertion point
        fetch_idx = typeql.find('fetch')
        limit_idx = typeql.find('limit')

        insert_idx = len(typeql)
        for idx in [fetch_idx, limit_idx]:
            if idx != -1 and idx < insert_idx:
                insert_idx = idx

        if insert_idx > 0:
            relationship = f'(city: {city_var}, country: {country_var}) isa in_country; '
            typeql = typeql[:insert_idx] + relationship + typeql[insert_idx:]

    return typeql


def fix_exists_ceo_check(typeql: str, cypher: str) -> str:
    """Add missing CEO check for EXISTS {(o)-[:HAS_CEO]->...} patterns."""
    # Check for EXISTS CEO pattern in Cypher
    if 'EXISTS' not in cypher:
        return typeql
    if 'HAS_CEO' not in cypher:
        return typeql
    if 'ceo_of' in typeql:
        return typeql

    # Find org variable
    org_match = re.search(r'(\$\w+) isa organization', typeql)
    if not org_match:
        return typeql

    org_var = org_match.group(1)

    # Find insertion point
    fetch_idx = typeql.find('fetch')
    limit_idx = typeql.find('limit')
    reduce_idx = typeql.find('reduce')

    insert_idx = len(typeql)
    for idx in [fetch_idx, limit_idx, reduce_idx]:
        if idx != -1 and idx < insert_idx:
            insert_idx = idx

    if insert_idx > 0:
        # Check if a specific person name is mentioned
        person_name_match = re.search(r"Person\s*\{name:\s*['\"]([^'\"]+)['\"]\}", cypher, re.IGNORECASE)
        if person_name_match:
            person_name = person_name_match.group(1)
            relationship = f'$ceo isa person, has name "{person_name}"; (organization: {org_var}, ceo: $ceo) isa ceo_of; '
        else:
            relationship = f'$ceo isa person; (organization: {org_var}, ceo: $ceo) isa ceo_of; '
        typeql = typeql[:insert_idx] + relationship + typeql[insert_idx:]

    return typeql


def fix_exists_investor_check(typeql: str, cypher: str) -> str:
    """Add missing investor check for EXISTS patterns."""
    if 'EXISTS' not in cypher:
        return typeql
    if 'HAS_INVESTOR' not in cypher:
        return typeql
    if 'invested_in' in typeql:
        return typeql

    # Find person variable
    person_match = re.search(r'(\$\w+) isa person', typeql)
    if not person_match:
        return typeql

    person_var = person_match.group(1)

    # Find insertion point
    fetch_idx = typeql.find('fetch')
    limit_idx = typeql.find('limit')

    insert_idx = len(typeql)
    for idx in [fetch_idx, limit_idx]:
        if idx != -1 and idx < insert_idx:
            insert_idx = idx

    if insert_idx > 0:
        relationship = f'$invested_org isa organization; (organization: $invested_org, investor: {person_var}) isa invested_in; '
        typeql = typeql[:insert_idx] + relationship + typeql[insert_idx:]

    return typeql


def fix_exists_parent_child_check(typeql: str, cypher: str) -> str:
    """Add missing parent/child check for EXISTS patterns."""
    if 'EXISTS' not in cypher:
        return typeql
    if 'HAS_PARENT' not in cypher and 'HAS_CHILD' not in cypher:
        return typeql
    if 'parent_of' in typeql:
        return typeql

    # Find person variable
    person_match = re.search(r'(\$\w+) isa person', typeql)
    if not person_match:
        return typeql

    person_var = person_match.group(1)

    # Find insertion point
    fetch_idx = typeql.find('fetch')
    limit_idx = typeql.find('limit')

    insert_idx = len(typeql)
    for idx in [fetch_idx, limit_idx]:
        if idx != -1 and idx < insert_idx:
            insert_idx = idx

    if insert_idx > 0:
        if 'HAS_PARENT' in cypher and 'HAS_CHILD' in cypher:
            # Person must have both parent and child
            relationship = f'$parent isa person; $child isa person; (parent: $parent, child: {person_var}) isa parent_of; (parent: {person_var}, child: $child) isa parent_of; '
        elif 'HAS_PARENT' in cypher:
            relationship = f'$parent isa person; (parent: $parent, child: {person_var}) isa parent_of; '
        else:
            relationship = f'$child isa person; (parent: {person_var}, child: $child) isa parent_of; '
        typeql = typeql[:insert_idx] + relationship + typeql[insert_idx:]

    return typeql


def fix_revenue_comparison(typeql: str, cypher: str) -> str:
    """Fix revenue comparison between organizations."""
    # Check if this is a revenue comparison query
    if 'revenue >' not in cypher.lower() and 'revenue <' not in cypher.lower():
        return typeql
    if '.revenue >' not in cypher.lower() and '.revenue <' not in cypher.lower():
        return typeql

    # Find the comparison in Cypher (e.g., o.revenue > sub.revenue)
    match = re.search(r'(\w+)\.revenue\s*([<>]=?)\s*(\w+)\.revenue', cypher, re.IGNORECASE)
    if not match:
        return typeql

    # Find org variables in TypeQL
    org_matches = re.findall(r'(\$\w+) isa organization', typeql)
    if len(org_matches) < 2:
        return typeql

    # Check if there's already a revenue comparison
    if 'revenue $' in typeql and '>' in typeql:
        return typeql

    org1_var = org_matches[0]
    org2_var = org_matches[1]

    # Find insertion point
    fetch_idx = typeql.find('fetch')
    limit_idx = typeql.find('limit')

    insert_idx = len(typeql)
    for idx in [fetch_idx, limit_idx]:
        if idx != -1 and idx < insert_idx:
            insert_idx = idx

    if insert_idx > 0:
        operator = match.group(2)
        comparison = f'{org1_var} has revenue $rev1; {org2_var} has revenue $rev2; $rev1 {operator} $rev2; '
        typeql = typeql[:insert_idx] + comparison + typeql[insert_idx:]

    return typeql


def fix_location_chain_with_country_filter(typeql: str, cypher: str) -> str:
    """Add complete location chain (org->city->country) with country filter."""
    # Check if there's a country filter in Cypher
    country_match = re.search(r"Country\s*\{name:\s*['\"]([^'\"]+)['\"]\}", cypher, re.IGNORECASE)
    if not country_match:
        # Also check for Country variable with name check
        country_match = re.search(r"Country[^}]*WHERE[^}]*name\s*=\s*['\"]([^'\"]+)['\"]", cypher, re.IGNORECASE)

    if not country_match:
        return typeql

    country_name = country_match.group(1)

    # Check if already has complete location chain with country filter
    if 'in_country' in typeql and f'country_name "{country_name}"' in typeql:
        return typeql

    # Find org variable
    org_match = re.search(r'(\$\w+) isa organization', typeql)
    if not org_match:
        return typeql

    org_var = org_match.group(1)

    # Find insertion point
    fetch_idx = typeql.find('fetch')
    limit_idx = typeql.find('limit')

    insert_idx = len(typeql)
    for idx in [fetch_idx, limit_idx]:
        if idx != -1 and idx < insert_idx:
            insert_idx = idx

    if insert_idx > 0:
        # Check what's already there
        if 'located_in' not in typeql:
            chain = f'$city isa city; $country isa country, has country_name "{country_name}"; (organization: {org_var}, city: $city) isa located_in; (city: $city, country: $country) isa in_country; '
        elif 'in_country' not in typeql:
            city_match = re.search(r'(\$\w+) isa city', typeql)
            city_var = city_match.group(1) if city_match else '$city'
            chain = f'$country isa country, has country_name "{country_name}"; (city: {city_var}, country: $country) isa in_country; '
        elif f'country_name "{country_name}"' not in typeql:
            # Just need to add the country filter
            country_match = re.search(r'(\$\w+) isa country', typeql)
            if country_match:
                country_var = country_match.group(1)
                old_def = re.search(rf'{re.escape(country_var)} isa country[^;]*;', typeql)
                if old_def and 'has country_name' not in old_def.group(0):
                    old_str = old_def.group(0)
                    new_str = old_str.replace(';', f', has country_name "{country_name}";')
                    typeql = typeql.replace(old_str, new_str)
                return typeql
            chain = ''
        else:
            chain = ''

        if chain:
            typeql = typeql[:insert_idx] + chain + typeql[insert_idx:]

    return typeql


def detect_and_fix_generic_issues(typeql: str, cypher: str, question: str) -> str:
    """Detect and fix common issues by comparing Cypher and TypeQL patterns."""
    cypher_upper = cypher.upper()

    # 1. Fix missing located_in relationship
    if 'IN_CITY' in cypher_upper and 'located_in' not in typeql:
        typeql = fix_org_city_relationship(typeql, cypher)

    # 2. Fix missing in_country relationship
    if 'IN_COUNTRY' in cypher_upper and 'in_country' not in typeql:
        typeql = fix_in_country_relationship(typeql, cypher)

    # 3. Fix missing in_category relationship
    if ('HAS_CATEGORY' in cypher_upper or 'HAS_INDUSTRY' in cypher_upper) and 'in_category' not in typeql:
        typeql = fix_org_category_relationship(typeql, cypher)

    # 4. Fix missing subsidiary_of relationship
    if 'HAS_SUBSIDIARY' in cypher_upper and 'subsidiary_of' not in typeql:
        typeql = fix_subsidiary_relationship(typeql, cypher)

    # 5. Fix missing ceo_of relationship
    if 'HAS_CEO' in cypher_upper and 'ceo_of' not in typeql:
        typeql = fix_ceo_relationship(typeql, cypher)

    # 6. Fix missing mentions relationship
    if 'MENTIONS' in cypher_upper and 'mentions' not in typeql:
        typeql = fix_mentions_relationship(typeql, cypher)

    # 7. Fix missing invested_in relationship
    if 'HAS_INVESTOR' in cypher_upper and 'invested_in' not in typeql:
        typeql = fix_investor_relationship(typeql, cypher)

    # 8. Fix missing board_member_of relationship
    if 'HAS_BOARD_MEMBER' in cypher_upper and 'board_member_of' not in typeql:
        typeql = fix_board_member_relationship(typeql, cypher)

    # 9. Fix missing competes_with relationship
    if ('HAS_COMPETITOR' in cypher_upper or 'COMPETES_WITH' in cypher_upper) and 'competes_with' not in typeql:
        typeql = fix_competitor_relationship(typeql, cypher)

    # 10. Fix missing supplies relationship
    if ('HAS_SUPPLIER' in cypher_upper or 'SUPPLIES' in cypher_upper) and 'supplies' not in typeql:
        typeql = fix_supplier_relationship(typeql, cypher)

    # 11. Fix missing parent_of relationship (person family)
    if ('HAS_CHILD' in cypher_upper or 'HAS_PARENT' in cypher_upper) and 'parent_of' not in typeql:
        typeql = fix_parent_child_relationship(typeql, cypher)

    # 12. Fix revenue thresholds (scientific notation)
    if re.search(r'>\s*\d+\.?\d*[Ee]\d+', cypher) or re.search(r'>\s*\d{7,}', cypher):
        typeql = fix_revenue_threshold(typeql, cypher)

    # 13. Fix NOT dissolved check
    if 'NOT O.ISDISSOLVED' in cypher_upper or 'NOT ORG.ISDISSOLVED' in cypher_upper:
        typeql = fix_not_dissolved_filter(typeql, cypher)

    # 14. Fix missing city name filter
    city_name = extract_city_name_from_cypher(cypher)
    if city_name and f'has city_name "{city_name}"' not in typeql:
        typeql = fix_city_filter(typeql, cypher, city_name)

    # 15. Fix missing person name filter
    person_name = extract_person_name_from_cypher(cypher)
    if person_name and f'has name "{person_name}"' not in typeql:
        typeql = fix_missing_person_filter(typeql, cypher)

    # 16. Fix missing organization name filter
    org_name = extract_org_name_from_cypher(cypher)
    if org_name and f'has name "{org_name}"' not in typeql:
        typeql = fix_missing_org_filter(typeql, cypher)

    # 17. Fix date filters
    if 'date(' in cypher.lower():
        typeql = fix_date_filter(typeql, cypher)

    # 18. Fix location chain if needed (org -> city -> country)
    if 'IN_CITY' in cypher_upper and 'IN_COUNTRY' in cypher_upper:
        typeql = fix_location_chain(typeql, cypher)

    # 19. Fix EXISTS patterns for CEO checks
    if 'EXISTS' in cypher and 'HAS_CEO' in cypher_upper and 'ceo_of' not in typeql:
        typeql = fix_exists_ceo_check(typeql, cypher)

    # 20. Fix EXISTS patterns for investor checks
    if 'EXISTS' in cypher and 'HAS_INVESTOR' in cypher_upper and 'invested_in' not in typeql:
        typeql = fix_exists_investor_check(typeql, cypher)

    # 21. Fix EXISTS patterns for parent/child checks
    if 'EXISTS' in cypher and ('HAS_PARENT' in cypher_upper or 'HAS_CHILD' in cypher_upper):
        typeql = fix_exists_parent_child_check(typeql, cypher)

    # 22. Fix revenue comparison between organizations
    if '.revenue >' in cypher.lower() or '.revenue <' in cypher.lower():
        typeql = fix_revenue_comparison(typeql, cypher)

    # 23. Fix complete location chain with country filter
    if 'IN_CITY' in cypher_upper and 'IN_COUNTRY' in cypher_upper:
        typeql = fix_location_chain_with_country_filter(typeql, cypher)

    return typeql


def fix_query(row: pd.Series) -> str:
    """Apply appropriate fixes based on the review reason"""
    typeql = row['typeql']
    cypher = row['cypher']
    question = row['question']
    reason = row['review_reason']

    # Handle generic "Failed semantic review (recovered from git)" cases
    if 'Failed semantic review' in reason or 'recovered from git' in reason:
        typeql = detect_and_fix_generic_issues(typeql, cypher, question)
        return typeql

    # Apply fixes based on the review reason

    # Revenue threshold fixes
    if 'Wrong revenue threshold' in reason:
        typeql = fix_revenue_threshold(typeql, cypher)

    # Location chain fixes
    if 'Missing location chain' in reason or 'does not connect org to city to country' in reason:
        typeql = fix_location_chain(typeql, cypher)

    # Sentiment aggregation fixes
    if 'sentiment aggregation' in reason.lower():
        typeql = fix_sentiment_aggregation(typeql, cypher)

    # Order of operations fixes
    if 'Wrong order of operations' in reason:
        typeql = fix_order_of_operations(typeql, cypher)

    # City filter fixes
    city_patterns = [
        ('Seattle', 'Seattle'),
        ('Rome', 'Rome'),
        ('Chicago', 'Chicago'),
        ('NYC', 'New York'),
        ('New York', 'New York'),
        ('Chattanooga', 'Chattanooga'),
    ]
    for pattern, city_name in city_patterns:
        if pattern in reason and f'has city_name "{city_name}"' not in typeql:
            typeql = fix_city_filter(typeql, cypher, city_name)

    # Parent-child relationship fixes
    if 'parent-child relationship' in reason.lower() or 'parent relationship' in reason.lower():
        typeql = fix_parent_child_relationship(typeql, cypher)

    # CEO relationship fixes
    if 'Missing CEO relationship' in reason or 'does not connect org to CEO' in reason.lower() or 'does not verify person is CEO' in reason.lower():
        typeql = fix_ceo_relationship(typeql, cypher)

    # Org-category relationship fixes
    if 'org-category relationship' in reason.lower() or 'does not connect organization to' in reason.lower():
        typeql = fix_org_category_relationship(typeql, cypher)

    # Supplier direction fixes
    if 'Wrong direction' in reason:
        typeql = fix_supplier_direction(typeql, cypher, question)

    # Date filter fixes
    if 'date filter' in reason.lower():
        typeql = fix_date_filter(typeql, cypher)

    # Not dissolved filter fixes
    if 'not dissolved' in reason.lower():
        typeql = fix_not_dissolved_filter(typeql, cypher)

    # Subsidiary relationship fixes
    if 'subsidiary relationship' in reason.lower() or 'Missing subsidiary' in reason:
        typeql = fix_subsidiary_relationship(typeql, cypher)

    # Competitor relationship fixes
    if 'competitor' in reason.lower() and 'Missing' in reason:
        typeql = fix_competitor_relationship(typeql, cypher)

    # Board member relationship fixes
    if 'board member' in reason.lower() and ('Missing' in reason or 'does not' in reason.lower()):
        typeql = fix_board_member_relationship(typeql, cypher)

    # Investor relationship fixes
    if 'investor' in reason.lower() and 'Missing' in reason:
        typeql = fix_investor_relationship(typeql, cypher)

    # Org-city relationship fixes
    if 'org-city relationship' in reason.lower() or ('does not connect org to city' in reason.lower() and 'country' not in reason.lower()):
        typeql = fix_org_city_relationship(typeql, cypher)

    # Supplier relationship (not direction) fixes
    if 'supplier relationship' in reason.lower() and 'Missing' in reason and 'Wrong direction' not in reason:
        typeql = fix_supplier_relationship(typeql, cypher)

    # Mentions relationship fixes
    if 'mention' in reason.lower() and ('Missing' in reason or 'does not' in reason.lower()):
        typeql = fix_mentions_relationship(typeql, cypher)

    # In-country relationship fixes
    if 'city-country' in reason.lower() or 'in_country' in reason.lower():
        typeql = fix_in_country_relationship(typeql, cypher)

    # Also try generic fixes for any remaining issues
    typeql = detect_and_fix_generic_issues(typeql, cypher, question)

    return typeql


def main():
    # Read the failed review file
    failed_df = pd.read_csv('output/companies/failed_review.csv')
    print(f"Loaded {len(failed_df)} failed queries")

    # Apply fixes
    fixed_queries = []
    for idx, row in failed_df.iterrows():
        original_typeql = row['typeql']
        fixed_typeql = fix_query(row)

        fixed_queries.append({
            'original_index': row['original_index'],
            'question': row['question'],
            'cypher': row['cypher'],
            'original_typeql': original_typeql,
            'fixed_typeql': fixed_typeql,
            'review_reason': row['review_reason'],
            'changed': original_typeql != fixed_typeql
        })

    fixed_df = pd.DataFrame(fixed_queries)

    # Report statistics
    changed_count = fixed_df['changed'].sum()
    print(f"Fixed {changed_count} of {len(fixed_df)} queries")

    # Group by review reason to see fix coverage
    print("\nFix coverage by reason:")
    reason_stats = fixed_df.groupby('review_reason')['changed'].agg(['sum', 'count'])
    reason_stats.columns = ['fixed', 'total']
    reason_stats['pct'] = (reason_stats['fixed'] / reason_stats['total'] * 100).round(1)
    for reason, row in reason_stats.iterrows():
        if row['fixed'] > 0:
            print(f"  [OK] {reason[:70]}... ({int(row['fixed'])}/{int(row['total'])})")
        else:
            print(f"  [--] {reason[:70]}... ({int(row['fixed'])}/{int(row['total'])})")

    # Save fixed queries
    output_path = 'output/companies/fixed_queries.csv'
    fixed_df.to_csv(output_path, index=False)
    print(f"\nSaved fixed queries to {output_path}")

    # Also update the main queries.csv
    # Read original queries file
    queries_df = pd.read_csv('output/companies/queries.csv')

    # Create a mapping from original_index to fixed_typeql
    fix_map = dict(zip(fixed_df['original_index'], fixed_df['fixed_typeql']))

    # Update queries
    updated_count = 0
    for idx, row in queries_df.iterrows():
        if idx in fix_map:
            queries_df.at[idx, 'typeql'] = fix_map[idx]
            updated_count += 1

    # Save updated queries
    queries_df.to_csv('output/companies/queries.csv', index=False)
    print(f"Updated {updated_count} queries in queries.csv")

    # Show some examples of fixes
    print("\n=== Sample Fixed Queries ===")
    samples = fixed_df[fixed_df['changed']].head(5)
    for _, row in samples.iterrows():
        print(f"\nQuestion: {row['question'][:80]}...")
        print(f"Reason: {row['review_reason'][:60]}...")
        print(f"Original: {row['original_typeql'][:100]}...")
        print(f"Fixed: {row['fixed_typeql'][:100]}...")


if __name__ == '__main__':
    main()
