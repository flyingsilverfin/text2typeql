#!/usr/bin/env python3
"""Convert Game of Thrones Cypher queries to TypeQL."""

import csv
import json
import re
import sys
from pathlib import Path

# TypeDB imports
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# Attribute name mapping from Cypher (camelCase) to TypeQL (snake_case)
ATTR_MAP = {
    'book1betweennesscentrality': 'book1_betweenness_centrality',
    'book1pagerank': 'book1_page_rank',
    'book45pagerank': 'book45_page_rank',
    'fastrf_embedding': 'fastrf_embedding',
    'centrality': 'centrality',
    'pagerank': 'pagerank',
    'degree': 'degree',
    'community': 'community',
    'louvain': 'louvain',
    'name': 'name',
    'weight': 'weight',
    'book': 'book',
}

# Relation mapping
REL_MAP = {
    'interacts': 'interacts',
    'interacts1': 'interacts1',
    'interacts2': 'interacts2',
    'interacts3': 'interacts3',
    'interacts45': 'interacts45',
}


def get_query(database: str, index: int) -> dict:
    """Get query at index for database."""
    csv_path = "/opt/text2typeql/data/text2cypher/datasets/synthetic_opus_demodbs/text2cypher_claudeopus.csv"

    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        idx = 0
        for row in reader:
            if row['database'] != database:
                continue
            if row.get('syntax_error', '').lower() == 'true':
                continue
            if row.get('false_schema', '').lower() == 'true':
                continue

            if idx == index:
                return {
                    'index': index,
                    'question': row['question'],
                    'cypher': row['cypher']
                }
            idx += 1

    return None


def map_attr(attr: str) -> str:
    """Map Cypher attribute name to TypeQL."""
    attr_lower = attr.lower()
    return ATTR_MAP.get(attr_lower, attr_lower)


def convert_cypher_to_typeql(cypher: str) -> str:
    """Convert a Cypher query to TypeQL."""
    cypher = cypher.strip()
    cypher_joined = ' '.join(cypher.split('\n'))
    cypher_joined = re.sub(r'\s+', ' ', cypher_joined).strip()

    # Skip unsupported patterns
    unsupported_patterns = [
        r'any\s*\(',  # any() function
        r'\[\s*\d+\s*\]',  # array indexing
        r'DISTINCT',  # DISTINCT keyword
        r'WITH\s+',  # WITH clause
        r'COLLECT\s*\(',  # collect function
        r'SIZE\s*\(',  # size function
        r'EXISTS\s*\{',  # EXISTS pattern
        r'OPTIONAL\s+MATCH',  # OPTIONAL MATCH
        r'STARTS\s+WITH',  # STARTS WITH string function
        r'ENDS\s+WITH',  # ENDS WITH string function
        r'IS\s+NOT\s+NULL',  # IS NOT NULL
        r'count\s*\(\s*\*\s*\)',  # count(*) - requires special handling
        r'\|',  # Union relation types like INTERACTS|INTERACTS1
        r'\*\d*\.\.', # Variable length paths like *0..
    ]

    for pattern in unsupported_patterns:
        if re.search(pattern, cypher_joined, re.IGNORECASE):
            return None

    # Extract clauses
    match_clause = ''
    where_clause = ''
    return_clause = ''
    order_clause = ''
    limit_clause = ''

    match_match = re.search(r'MATCH\s+(.+?)(?=\s+WHERE|\s+RETURN|\s+ORDER|\s+LIMIT|$)', cypher_joined, re.IGNORECASE)
    if match_match:
        match_clause = match_match.group(1).strip()

    where_match = re.search(r'WHERE\s+(.+?)(?=\s+RETURN|\s+ORDER|\s+LIMIT|$)', cypher_joined, re.IGNORECASE)
    if where_match:
        where_clause = where_match.group(1).strip()

    return_match = re.search(r'RETURN\s+(.+?)(?=\s+ORDER|\s+LIMIT|$)', cypher_joined, re.IGNORECASE)
    if return_match:
        return_clause = return_match.group(1).strip()

    order_match = re.search(r'ORDER\s+BY\s+(.+?)(?=\s+LIMIT|$)', cypher_joined, re.IGNORECASE)
    if order_match:
        order_clause = order_match.group(1).strip()

    limit_match = re.search(r'LIMIT\s+(\d+)', cypher_joined, re.IGNORECASE)
    if limit_match:
        limit_clause = limit_match.group(1).strip()

    # State tracking
    typeql_parts = []
    char_var1 = None
    char_var2 = None
    rel_var = None  # Original Cypher variable name for relation
    rel_type = None
    is_relation_query = False

    # Track relation attributes we need to extract
    rel_attr_vars = {}  # Map attr name -> TypeQL variable

    # Track entity attributes we need
    entity_attr_vars = {}  # Map (var, attr) -> TypeQL variable

    # Check for relation pattern with variable: (c1:Char)-[r:REL]-(c2:Char)
    rel_pattern = re.search(
        r'\((\w+):(\w+)\)\s*-\[(\w+):(\w+)(?:\s*\{([^}]+)\})?\]-[>]?\s*\((\w+):(\w+)\)',
        match_clause, re.IGNORECASE
    )

    # Check for relation pattern without variable: (c1:Char)-[:REL]-(c2:Char)
    rel_pattern_no_var = None
    if not rel_pattern:
        rel_pattern_no_var = re.search(
            r'\((\w+):(\w+)\)\s*-\[:(\w+)(?:\s*\{([^}]+)\})?\]-[>]?\s*\((\w+):(\w+)\)',
            match_clause, re.IGNORECASE
        )

    inline_rel_conditions = []

    if rel_pattern:
        is_relation_query = True
        char_var1, type1, rel_var, rel_type_raw, props_str, char_var2, type2 = rel_pattern.groups()
        rel_type = rel_type_raw.lower()

        if rel_type not in REL_MAP:
            return None

        typeql_parts.append(f"${char_var1} isa character;")
        typeql_parts.append(f"${char_var2} isa character;")

        # Parse inline relation properties like {weight: 150}
        if props_str:
            for prop_match in re.finditer(r'(\w+)\s*:\s*(\d+(?:\.\d+)?|"[^"]*"|\'[^\']*\')', props_str):
                prop_name = prop_match.group(1).lower()
                prop_val = prop_match.group(2)
                typeql_attr = map_attr(prop_name)
                attr_var = f"$rel_{typeql_attr}"
                rel_attr_vars[typeql_attr] = attr_var
                inline_rel_conditions.append((attr_var, '==', prop_val))

    elif rel_pattern_no_var:
        is_relation_query = True
        char_var1, type1, rel_type_raw, props_str, char_var2, type2 = rel_pattern_no_var.groups()
        rel_var = None  # No relation variable
        rel_type = rel_type_raw.lower()

        if rel_type not in REL_MAP:
            return None

        typeql_parts.append(f"${char_var1} isa character;")
        typeql_parts.append(f"${char_var2} isa character;")

        # Parse inline relation properties
        if props_str:
            for prop_match in re.finditer(r'(\w+)\s*:\s*(\d+(?:\.\d+)?|"[^"]*"|\'[^\']*\')', props_str):
                prop_name = prop_match.group(1).lower()
                prop_val = prop_match.group(2)
                typeql_attr = map_attr(prop_name)
                attr_var = f"$rel_{typeql_attr}"
                rel_attr_vars[typeql_attr] = attr_var
                inline_rel_conditions.append((attr_var, '==', prop_val))

    else:
        # Simple entity pattern
        entity_pattern = re.search(r'\((\w+):(\w+)(?:\s*\{([^}]+)\})?\)', match_clause, re.IGNORECASE)
        if entity_pattern:
            var = entity_pattern.group(1)
            etype = entity_pattern.group(2).lower()
            props = entity_pattern.group(3)

            if etype == 'character':
                if props:
                    prop_parts = []
                    for prop_match in re.finditer(r'(\w+)\s*:\s*(\d+(?:\.\d+)?|"[^"]*"|\'[^\']*\')', props):
                        prop_name = prop_match.group(1)
                        prop_val = prop_match.group(2)
                        typeql_attr = map_attr(prop_name)
                        prop_parts.append(f"has {typeql_attr} {prop_val}")
                    if prop_parts:
                        typeql_parts.append(f"${var} isa character, {', '.join(prop_parts)};")
                    else:
                        typeql_parts.append(f"${var} isa character;")
                else:
                    typeql_parts.append(f"${var} isa character;")
                char_var1 = var

    # Parse WHERE clause
    where_conditions = []  # List of (var, op, val) for non-relation attributes
    rel_conditions = []    # List of (attr_var, op, val) for relation attributes
    cross_var_conditions = []  # List of (var1, var2) for cross-variable comparisons

    if where_clause:
        conditions = re.split(r'\s+AND\s+', where_clause, flags=re.IGNORECASE)
        for cond in conditions:
            cond = cond.strip()

            # Check for cross-variable comparison: c1.attr = c2.attr
            # Make sure both sides start with a letter (not a digit) to avoid matching floats
            cross_match = re.match(r'([a-zA-Z_]\w*)\.(\w+)\s*(=|==)\s*([a-zA-Z_]\w*)\.(\w+)', cond)
            if cross_match:
                var1 = cross_match.group(1)
                attr1 = cross_match.group(2)
                var2 = cross_match.group(4)
                attr2 = cross_match.group(5)

                typeql_attr1 = map_attr(attr1)
                typeql_attr2 = map_attr(attr2)

                attr_var1 = f"${var1}_{typeql_attr1}"
                attr_var2 = f"${var2}_{typeql_attr2}"

                entity_attr_vars[(var1, typeql_attr1)] = attr_var1
                entity_attr_vars[(var2, typeql_attr2)] = attr_var2
                cross_var_conditions.append((attr_var1, attr_var2))
                continue

            cond_match = re.match(r'(\w+)\.(\w+)\s*(>=|<=|<>|!=|>|<|=)\s*(.+)', cond)
            if cond_match:
                var = cond_match.group(1)
                prop = cond_match.group(2)
                op = cond_match.group(3)
                val = cond_match.group(4).strip()
                typeql_attr = map_attr(prop)

                # Convert operator
                if op == '=':
                    op = '=='
                elif op in ('<>', '!='):
                    op = '!='

                if is_relation_query and var == rel_var:
                    # Relation attribute
                    attr_var = f"$rel_{typeql_attr}"
                    rel_attr_vars[typeql_attr] = attr_var
                    rel_conditions.append((attr_var, op, val))
                else:
                    # Entity attribute
                    attr_var = f"${var}_{typeql_attr}"
                    entity_attr_vars[(var, typeql_attr)] = attr_var
                    where_conditions.append((attr_var, op, val))

    # Add inline relation conditions
    if is_relation_query and inline_rel_conditions:
        rel_conditions.extend(inline_rel_conditions)

    # Parse RETURN clause to determine fetch items and needed attributes
    fetch_items = {}

    if return_clause:
        # Handle COUNT
        if 'count(' in return_clause.lower():
            count_match = re.search(r'count\((\w+|\*)\)', return_clause, re.IGNORECASE)
            if count_match:
                count_var = count_match.group(1)
                if count_var == '*':
                    count_var = char_var1 if char_var1 else 'c'
                # Build count query differently
                return build_count_query_v2(
                    typeql_parts, is_relation_query, rel_type, char_var1, char_var2,
                    rel_attr_vars, rel_conditions, entity_attr_vars, where_conditions,
                    count_var
                )

        return_items = [item.strip() for item in return_clause.split(',')]

        for item in return_items:
            as_match = re.match(r'(.+?)\s+AS\s+(\w+)', item, re.IGNORECASE)
            if as_match:
                expr = as_match.group(1).strip()
                alias = as_match.group(2).strip()
            else:
                expr = item
                alias = None

            prop_match = re.match(r'(\w+)\.(\w+)', expr)
            if prop_match:
                var = prop_match.group(1)
                prop = prop_match.group(2)
                typeql_attr = map_attr(prop)

                if is_relation_query and var == rel_var:
                    # Relation attribute in fetch - need to capture it
                    attr_var = f"$rel_{typeql_attr}"
                    rel_attr_vars[typeql_attr] = attr_var
                    fetch_key = alias or f"weight"
                    fetch_items[fetch_key] = attr_var
                else:
                    fetch_key = alias or f"{var}_{typeql_attr}"
                    fetch_items[fetch_key] = f"${var}.{typeql_attr}"
            elif re.match(r'^\w+$', expr):
                var = expr
                if is_relation_query and var == rel_var:
                    # Return relation - need to get weight
                    attr_var = f"$rel_weight"
                    rel_attr_vars['weight'] = attr_var
                    fetch_items['rel_weight'] = attr_var
                else:
                    # Entity - return name by default
                    fetch_items[f"{var}_name"] = f"${var}.name"

    # Parse ORDER BY
    sort_var = None
    sort_direction = 'desc'

    if order_clause:
        # Handle multiple ORDER BY columns - only use the first one
        # Split by comma first to get individual sort expressions
        order_columns = [col.strip() for col in order_clause.split(',')]
        first_col = order_columns[0]

        order_parts = first_col.split()
        order_expr = order_parts[0]
        if len(order_parts) > 1:
            sort_direction = order_parts[1].lower()

        order_prop_match = re.match(r'(\w+)\.(\w+)', order_expr)
        if order_prop_match:
            var = order_prop_match.group(1)
            prop = order_prop_match.group(2)
            typeql_attr = map_attr(prop)

            if is_relation_query and var == rel_var:
                attr_var = f"$rel_{typeql_attr}"
                rel_attr_vars[typeql_attr] = attr_var
                sort_var = attr_var
            else:
                attr_var = f"${var}_{typeql_attr}"
                entity_attr_vars[(var, typeql_attr)] = attr_var
                sort_var = attr_var
        elif re.match(r'^\w+$', order_expr):
            # It's an alias
            alias = order_expr.lower()
            for key, val in fetch_items.items():
                if key.lower() == alias:
                    if val.startswith('$') and '.' in val:
                        # It's like $c.pagerank
                        val_match = re.match(r'\$(\w+)\.(\w+)', val)
                        if val_match:
                            base_var = val_match.group(1)
                            attr = val_match.group(2)
                            attr_var = f"${base_var}_{attr}"
                            entity_attr_vars[(base_var, attr)] = attr_var
                            sort_var = attr_var
                    elif val.startswith('$'):
                        # Direct variable reference
                        sort_var = val
                    break

    # Build final TypeQL query
    typeql = "match\n"

    # Add entity patterns
    for part in typeql_parts:
        typeql += f"  {part}\n"

    # Add entity attribute has clauses
    for (var, attr), attr_var in entity_attr_vars.items():
        typeql += f"  ${var} has {attr} {attr_var};\n"

    # Add relation pattern with its attributes
    if is_relation_query:
        typeql_rel = REL_MAP[rel_type]
        if rel_attr_vars:
            # Build relation with has clauses inline
            has_parts = [f"has {attr} {var}" for attr, var in rel_attr_vars.items()]
            typeql += f"  (character1: ${char_var1}, character2: ${char_var2}) isa {typeql_rel}, {', '.join(has_parts)};\n"
        else:
            typeql += f"  (character1: ${char_var1}, character2: ${char_var2}) isa {typeql_rel};\n"

    # Add entity attribute conditions
    for attr_var, op, val in where_conditions:
        typeql += f"  {attr_var} {op} {val};\n"

    # Add relation attribute conditions
    for attr_var, op, val in rel_conditions:
        typeql += f"  {attr_var} {op} {val};\n"

    # Add cross-variable comparison conditions
    for attr_var1, attr_var2 in cross_var_conditions:
        typeql += f"  {attr_var1} == {attr_var2};\n"

    # Add sort
    if sort_var:
        typeql += f"sort {sort_var} {sort_direction};\n"

    # Add limit
    if limit_clause:
        typeql += f"limit {limit_clause};\n"

    # Add fetch
    if fetch_items:
        fetch_parts = [f'"{key}": {val}' for key, val in fetch_items.items()]
        typeql += "fetch { " + ", ".join(fetch_parts) + " };"
    else:
        var = char_var1 if char_var1 else 'c'
        typeql += f'fetch {{ "name": ${var}.name }};'

    return typeql


def build_count_query_v2(typeql_parts, is_relation_query, rel_type, char_var1, char_var2,
                         rel_attr_vars, rel_conditions, entity_attr_vars, where_conditions, count_var):
    """Build a count query with proper relation handling."""
    typeql = "match\n"

    for part in typeql_parts:
        typeql += f"  {part}\n"

    for (var, attr), attr_var in entity_attr_vars.items():
        typeql += f"  ${var} has {attr} {attr_var};\n"

    if is_relation_query:
        typeql_rel = REL_MAP[rel_type]
        if rel_attr_vars:
            has_parts = [f"has {attr} {var}" for attr, var in rel_attr_vars.items()]
            typeql += f"  (character1: ${char_var1}, character2: ${char_var2}) isa {typeql_rel}, {', '.join(has_parts)};\n"
        else:
            typeql += f"  (character1: ${char_var1}, character2: ${char_var2}) isa {typeql_rel};\n"

    for attr_var, op, val in where_conditions:
        typeql += f"  {attr_var} {op} {val};\n"

    for attr_var, op, val in rel_conditions:
        typeql += f"  {attr_var} {op} {val};\n"

    typeql += f"reduce $count = count(${count_var});"
    return typeql


def validate_query(driver, database: str, typeql: str) -> tuple:
    """Validate a TypeQL query against the database."""
    try:
        with driver.transaction(database, TransactionType.READ) as tx:
            result = tx.query(typeql).resolve()
            if hasattr(result, 'as_concept_documents'):
                list(result.as_concept_documents())
            elif hasattr(result, 'as_value'):
                result.as_value()
        return (True, None)
    except Exception as e:
        return (False, str(e))


def main():
    """Main conversion loop."""
    database = 'gameofthrones'
    typedb_name = 'text2typeql_gameofthrones'
    output_dir = Path('/opt/text2typeql/output/gameofthrones')

    credentials = Credentials('admin', 'password')
    options = DriverOptions(is_tls_enabled=False)
    driver = TypeDB.driver('localhost:1729', credentials, options)

    queries_file = output_dir / 'queries.csv'
    failed_file = output_dir / 'failed.csv'

    successful = []
    failed = []

    total = 392
    for i in range(total):
        query_data = get_query(database, i)
        if not query_data:
            print(f"[{i}] No query found")
            continue

        question = query_data['question']
        cypher = query_data['cypher']

        try:
            typeql = convert_cypher_to_typeql(cypher)

            if typeql is None:
                failed.append({
                    'original_index': i,
                    'question': question,
                    'cypher': cypher,
                    'error': 'Unsupported Cypher feature'
                })
                print(f"[{i}] SKIP: Unsupported feature")
                continue

            success, error = validate_query(driver, typedb_name, typeql)

            if success:
                successful.append({
                    'original_index': i,
                    'question': question,
                    'cypher': cypher,
                    'typeql': typeql
                })
                print(f"[{i}] OK")
            else:
                failed.append({
                    'original_index': i,
                    'question': question,
                    'cypher': cypher,
                    'error': error
                })
                print(f"[{i}] FAIL: {error[:80]}")

        except Exception as e:
            failed.append({
                'original_index': i,
                'question': question,
                'cypher': cypher,
                'error': str(e)
            })
            print(f"[{i}] ERROR: {str(e)[:80]}")

    driver.close()

    if successful:
        with open(queries_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
            writer.writeheader()
            writer.writerows(successful)

    if failed:
        with open(failed_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'error'])
            writer.writeheader()
            writer.writerows(failed)

    print(f"\nResults: {len(successful)} successful, {len(failed)} failed")
    print(f"Queries written to: {queries_file}")
    print(f"Failed written to: {failed_file}")


if __name__ == '__main__':
    main()
