#!/usr/bin/env python3
"""Convert neoflix Cypher queries to TypeQL and validate against TypeDB."""

import csv
import json
import re
import sys
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# Get query helper
def get_query(database: str, index: int) -> dict:
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

# Entity and attribute mapping
ENTITY_MAP = {
    'Movie': 'movie',
    'Video': 'video',
    'Adult': 'adult',
    'Person': 'person',
    'Genre': 'genre',
    'Language': 'language',
    'Country': 'country',
    'ProductionCompany': 'production_company',
    'Collection': 'collection',
    'User': 'user',
    'Keyword': 'keyword',
    'Package': 'package',
    'Subscription': 'subscription',
}

NAME_ATTRS = {
    'movie': 'title',
    'video': 'title',
    'adult': 'title',
    'person': 'person_name',
    'genre': 'genre_name',
    'language': 'language_name',
    'country': 'country_name',
    'production_company': 'production_company_name',
    'collection': 'collection_name',
    'keyword': 'keyword_name',
    'package': 'package_name'
}

ID_ATTRS = {
    'movie': 'movie_id',
    'video': 'movie_id',
    'adult': 'movie_id',
    'person': 'person_id',
    'genre': 'genre_id',
    'language': 'language_id',
    'country': 'country_id',
    'production_company': 'production_company_id',
    'collection': 'collection_id',
    'user': 'user_id',
    'keyword': 'keyword_id',
    'package': 'package_id',
    'subscription': 'subscription_id'
}

# Relation mapping: Cypher relation -> (typeql_relation, role1, role2)
# role1 is the "from" role, role2 is the "to" role
REL_MAP = {
    'PRODUCED_BY': ('produced_by', 'media', 'producer'),
    'IN_GENRE': ('in_genre', 'media', 'genre'),
    'IN_COLLECTION': ('in_collection', 'media', 'collection'),
    'ORIGINAL_LANGUAGE': ('original_language', 'media', 'language'),
    'SPOKEN_IN_LANGUAGE': ('spoken_in_language', 'media', 'language'),
    'PRODUCED_IN_COUNTRY': ('produced_in_country', 'media', 'country'),
    'HAS_KEYWORD': ('has_keyword', 'media', 'keyword'),
    'ACTED_IN': ('cast_for', 'actor', 'film'),
    'CAST_FOR': ('cast_for', 'actor', 'film'),
    'DIRECTED': ('crew_for', 'crew_member', 'film'),
    'CREW_FOR': ('crew_for', 'crew_member', 'film'),
    'RATED': ('rated', 'reviewer', 'rated_media'),
    'PROVIDES_ACCESS_TO': ('provides_access_to', 'package', 'genre'),
    'FOR_PACKAGE': ('for_package', 'subscription', 'package'),
}

def map_entity(label):
    return ENTITY_MAP.get(label, label.lower())

def map_attr(attr, entity_type):
    if attr == 'name':
        return NAME_ATTRS.get(entity_type, attr)
    if attr == 'id':
        return ID_ATTRS.get(entity_type, attr)
    # CamelCase to snake_case conversions
    camel_to_snake = {
        'expiresAt': 'expires_at',
        'releaseDate': 'release_date',
        'originalTitle': 'original_title',
        'originalLanguage': 'original_language',
        'voteCount': 'vote_count',
        'averageVote': 'average_vote',
        'posterPath': 'poster_path',
        'backdropPath': 'backdrop_path',
        'profilePath': 'profile_path',
        'imdbId': 'imdb_id',
        'movieId': 'movie_id',
        'personId': 'person_id',
        'genreId': 'genre_id',
        'languageId': 'language_id',
        'countryId': 'country_id',
        'collectionId': 'collection_id',
        'keywordId': 'keyword_id',
        'userId': 'user_id',
        'packageId': 'package_id',
        'subscriptionId': 'subscription_id',
        'creditId': 'credit_id',
        'castOrder': 'cast_order',
        'castId': 'cast_id',
        'productionCompanyId': 'production_company_id',
    }
    if attr in camel_to_snake:
        return camel_to_snake[attr]
    # Map entity-specific attributes
    if entity_type == 'package':
        if attr == 'price':
            return 'package_price'
        if attr == 'duration':
            return 'package_duration'
    return attr

def convert_cypher_to_typeql(cypher: str, question: str) -> str:
    """Convert a Cypher query to TypeQL."""
    cypher = cypher.strip()
    cypher_upper = cypher.upper()

    # Parse all components
    where_match = re.search(r'\bWHERE\s+(.*?)(?=\bRETURN\b|\bORDER\b|\bWITH\b|$)', cypher, re.IGNORECASE | re.DOTALL)
    where_clause = where_match.group(1).strip() if where_match else ""

    return_match = re.search(r'\bRETURN\s+(.*?)(?=\bORDER\b|\bLIMIT\b|$)', cypher, re.IGNORECASE | re.DOTALL)
    return_clause = return_match.group(1).strip() if return_match else ""

    order_match = re.search(r'\bORDER\s+BY\s+(.*?)(?=\bLIMIT\b|$)', cypher, re.IGNORECASE | re.DOTALL)
    order_clause = order_match.group(1).strip() if order_match else ""

    limit_match = re.search(r'\bLIMIT\s+(\d+)', cypher, re.IGNORECASE)
    limit_value = limit_match.group(1) if limit_match else ""

    with_match = re.search(r'\bWITH\s+(.*?)(?=\bRETURN\b|\bORDER\b|\bWHERE\b|\bMATCH\b|$)', cypher, re.IGNORECASE | re.DOTALL)
    with_clause = with_match.group(1).strip() if with_match else ""

    # Detect features
    has_count = 'COUNT(' in cypher_upper
    has_sum = 'SUM(' in cypher_upper
    has_avg = 'AVG(' in cypher_upper
    has_max = 'MAX(' in cypher_upper
    has_min = 'MIN(' in cypher_upper
    has_distinct = 'DISTINCT' in cypher_upper
    has_with_agg = with_clause and (has_count or has_sum or has_avg or has_max or has_min)
    has_collect = 'COLLECT(' in cypher_upper

    # Parse nodes
    node_pattern = r'\((\w+):(\w+)(?:\s*\{([^}]*)\})?\)'
    nodes = re.findall(node_pattern, cypher)

    entity_vars = {}  # var -> entity_type
    node_props = {}   # var -> {prop: value}

    for var, label, props in nodes:
        entity_type = map_entity(label)
        entity_vars[var] = entity_type
        if props:
            prop_dict = {}
            prop_matches = re.findall(r'(\w+):\s*([^,}]+)', props)
            for pname, pval in prop_matches:
                prop_dict[pname] = pval.strip()
            node_props[var] = prop_dict

    # Parse relations - more flexible pattern
    rel_pattern_fwd = r'\((\w+)(?::\w+)?(?:\s*\{[^}]*\})?\)\s*-\s*\[(\w*):?(\w+)(?:\s*\{[^}]*\})?\]\s*->\s*\((\w+)(?::\w+)?(?:\s*\{[^}]*\})?\)'
    rel_pattern_bwd = r'\((\w+)(?::\w+)?(?:\s*\{[^}]*\})?\)\s*<-\s*\[(\w*):?(\w+)(?:\s*\{[^}]*\})?\]\s*-\s*\((\w+)(?::\w+)?(?:\s*\{[^}]*\})?\)'

    fwd_rels = re.findall(rel_pattern_fwd, cypher)
    bwd_rels = re.findall(rel_pattern_bwd, cypher)

    relations = []
    rel_counter = 0
    for var1, rel_var, rel_type, var2 in fwd_rels:
        if not rel_var:
            rel_var = f'rel{rel_counter}'
            rel_counter += 1
        relations.append((var1, rel_var, rel_type.upper(), var2, 'fwd'))
    for var1, rel_var, rel_type, var2 in bwd_rels:
        if not rel_var:
            rel_var = f'rel{rel_counter}'
            rel_counter += 1
        relations.append((var1, rel_var, rel_type.upper(), var2, 'bwd'))

    # Build match clauses
    match_lines = []
    attr_vars = {}  # (var, attr) -> $var_name

    # Add entity definitions
    for var, entity_type in entity_vars.items():
        parts = [f"${var} isa {entity_type}"]
        if var in node_props:
            for pname, pval in node_props[var].items():
                attr_name = map_attr(pname, entity_type)
                parts.append(f"has {attr_name} {pval}")
        match_lines.append(", ".join(parts) + ";")

    # Add relations
    rel_attr_vars = {}  # (rel_var, attr) -> $var_name
    for var1, rel_var, rel_type, var2, direction in relations:
        if rel_type in REL_MAP:
            rel_name, role1, role2 = REL_MAP[rel_type]
            # Determine which variable plays which role based on entity types
            entity1 = entity_vars.get(var1, 'unknown')
            entity2 = entity_vars.get(var2, 'unknown')

            # Map entity types to role preferences
            role_for_entity = {
                # For produced_by: media, producer
                ('movie', 'produced_by'): 'media',
                ('video', 'produced_by'): 'media',
                ('adult', 'produced_by'): 'media',
                ('production_company', 'produced_by'): 'producer',
                # For in_genre: media, genre
                ('movie', 'in_genre'): 'media',
                ('video', 'in_genre'): 'media',
                ('adult', 'in_genre'): 'media',
                ('genre', 'in_genre'): 'genre',
                # For in_collection: media, collection
                ('movie', 'in_collection'): 'media',
                ('video', 'in_collection'): 'media',
                ('collection', 'in_collection'): 'collection',
                # For original_language: media, language
                ('movie', 'original_language'): 'media',
                ('video', 'original_language'): 'media',
                ('adult', 'original_language'): 'media',
                ('language', 'original_language'): 'language',
                # For spoken_in_language: media, language
                ('movie', 'spoken_in_language'): 'media',
                ('video', 'spoken_in_language'): 'media',
                ('adult', 'spoken_in_language'): 'media',
                ('language', 'spoken_in_language'): 'language',
                # For produced_in_country: media, country
                ('movie', 'produced_in_country'): 'media',
                ('video', 'produced_in_country'): 'media',
                ('adult', 'produced_in_country'): 'media',
                ('country', 'produced_in_country'): 'country',
                # For has_keyword: media, keyword
                ('movie', 'has_keyword'): 'media',
                ('video', 'has_keyword'): 'media',
                ('adult', 'has_keyword'): 'media',
                ('keyword', 'has_keyword'): 'keyword',
                # For cast_for: actor, film
                ('person', 'cast_for'): 'actor',
                ('movie', 'cast_for'): 'film',
                ('video', 'cast_for'): 'film',
                # For crew_for: crew_member, film
                ('person', 'crew_for'): 'crew_member',
                ('movie', 'crew_for'): 'film',
                ('video', 'crew_for'): 'film',
                # For rated: reviewer, rated_media
                ('user', 'rated'): 'reviewer',
                ('movie', 'rated'): 'rated_media',
                ('video', 'rated'): 'rated_media',
                # For provides_access_to: package, genre
                ('package', 'provides_access_to'): 'package',
                ('genre', 'provides_access_to'): 'genre',
                # For for_package: subscription, package
                ('subscription', 'for_package'): 'subscription',
                ('package', 'for_package'): 'package',
            }

            role_var1 = role_for_entity.get((entity1, rel_name), role1)
            role_var2 = role_for_entity.get((entity2, rel_name), role2)

            match_lines.append(f"${rel_var} ({role_var1}: ${var1}, {role_var2}: ${var2}) isa {rel_name};")

    # Collect all attributes needed for WHERE, RETURN, ORDER BY
    all_refs = where_clause + " " + return_clause + " " + order_clause + " " + with_clause

    for var, entity_type in entity_vars.items():
        # Find var.attr patterns
        attr_pattern = rf'{var}\.(\w+)'
        for match in re.finditer(attr_pattern, all_refs):
            attr = match.group(1)
            attr_name = map_attr(attr, entity_type)
            var_name = f"${attr_name}" if len(entity_vars) == 1 else f"${attr_name}_{var}"
            if (var, attr_name) not in attr_vars:
                match_lines.append(f"${var} has {attr_name} {var_name};")
                attr_vars[(var, attr_name)] = var_name

    # Check for relation attributes (r.rating, r.character, etc) in all clauses
    rel_vars = [r[1] for r in relations]
    rel_attr_pattern = r'(\w+)\.(\w+)'
    all_clauses = return_clause + " " + with_clause + " " + where_clause
    for match in re.finditer(rel_attr_pattern, all_clauses):
        var, attr = match.groups()
        if var not in entity_vars and var in rel_vars:
            # This is a relation variable
            var_name = f"${attr}"
            if (var, attr) not in rel_attr_vars:
                # Find the relation line and add the has clause
                for i, line in enumerate(match_lines):
                    if line.startswith(f"${var} "):
                        base = line.rstrip(';')
                        match_lines[i] = f"{base}, has {attr} {var_name};"
                        rel_attr_vars[(var, attr)] = var_name
                        break

    # Build filter conditions from WHERE
    filter_lines = []
    if where_clause:
        # Numeric comparisons
        for var, entity_type in entity_vars.items():
            pattern = rf'{var}\.(\w+)\s*(>|<|>=|<=|<>)\s*(\d+(?:\.\d+)?)'
            for match in re.finditer(pattern, where_clause):
                attr, op, val = match.groups()
                attr_name = map_attr(attr, entity_type)
                var_name = attr_vars.get((var, attr_name), f"${attr_name}")
                if op == '<>':
                    op = '!='
                filter_lines.append(f"{var_name} {op} {val};")

        # String equality
        for var, entity_type in entity_vars.items():
            pattern = rf"{var}\.(\w+)\s*=\s*'([^']+)'"
            for match in re.finditer(pattern, where_clause):
                attr, val = match.groups()
                attr_name = map_attr(attr, entity_type)
                var_name = attr_vars.get((var, attr_name), f"${attr_name}")
                filter_lines.append(f'{var_name} == "{val}";')

        # STARTS WITH -> like
        for var, entity_type in entity_vars.items():
            pattern = rf"{var}\.(\w+)\s+STARTS\s+WITH\s+'([^']+)'"
            for match in re.finditer(pattern, where_clause, re.IGNORECASE):
                attr, prefix = match.groups()
                attr_name = map_attr(attr, entity_type)
                var_name = attr_vars.get((var, attr_name), f"${attr_name}")
                filter_lines.append(f'{var_name} like "{prefix}.*";')

        # CONTAINS -> like
        for var, entity_type in entity_vars.items():
            pattern = rf"{var}\.(\w+)\s+CONTAINS\s+'([^']+)'"
            for match in re.finditer(pattern, where_clause, re.IGNORECASE):
                attr, substring = match.groups()
                attr_name = map_attr(attr, entity_type)
                var_name = attr_vars.get((var, attr_name), f"${attr_name}")
                filter_lines.append(f'{var_name} like ".*{substring}.*";')

        # ENDS WITH -> like
        for var, entity_type in entity_vars.items():
            pattern = rf"{var}\.(\w+)\s+ENDS\s+WITH\s+'([^']+)'"
            for match in re.finditer(pattern, where_clause, re.IGNORECASE):
                attr, suffix = match.groups()
                attr_name = map_attr(attr, entity_type)
                var_name = attr_vars.get((var, attr_name), f"${attr_name}")
                filter_lines.append(f'{var_name} like ".*{suffix}";')

    # Build query
    lines = ["match"]
    for ml in match_lines:
        lines.append("  " + ml)
    for fl in filter_lines:
        lines.append("  " + fl)

    # Handle WITH aggregation (two-stage queries)
    if has_with_agg:
        return build_agg_query(lines, with_clause, return_clause, order_clause, limit_value, entity_vars, attr_vars, rel_attr_vars, rel_vars)

    # Simple aggregations in RETURN
    if has_count and not has_with_agg:
        count_match = re.search(r'COUNT\((?:DISTINCT\s+)?(\*|\w+)\)', return_clause, re.IGNORECASE)
        if count_match:
            count_target = count_match.group(1)
            if count_target == '*':
                # Count first entity
                first_var = list(entity_vars.keys())[0]
                lines.append(f"reduce $count = count(${first_var});")
            else:
                lines.append(f"reduce $count = count(${count_target});")
            return "\n".join(lines)

    if has_sum and not has_with_agg:
        sum_match = re.search(r'SUM\((\w+)\.(\w+)\)', return_clause, re.IGNORECASE)
        if sum_match:
            var, attr = sum_match.groups()
            entity_type = entity_vars.get(var, 'movie')
            attr_name = map_attr(attr, entity_type)
            var_name = attr_vars.get((var, attr_name), f"${attr_name}")
            lines.append(f"reduce $sum = sum({var_name});")
            return "\n".join(lines)

    if has_avg and not has_with_agg:
        avg_match = re.search(r'AVG\((\w+)\.(\w+)\)', return_clause, re.IGNORECASE)
        if avg_match:
            var, attr = avg_match.groups()
            entity_type = entity_vars.get(var, 'movie')
            attr_name = map_attr(attr, entity_type)
            var_name = attr_vars.get((var, attr_name), f"${attr_name}")
            lines.append(f"reduce $avg = mean({var_name});")
            return "\n".join(lines)

    if has_max and not has_with_agg:
        max_match = re.search(r'MAX\((\w+)\.(\w+)\)', return_clause, re.IGNORECASE)
        if max_match:
            var, attr = max_match.groups()
            entity_type = entity_vars.get(var, 'movie')
            attr_name = map_attr(attr, entity_type)
            var_name = attr_vars.get((var, attr_name), f"${attr_name}")
            lines.append(f"reduce $max = max({var_name});")
            return "\n".join(lines)

    if has_min and not has_with_agg:
        min_match = re.search(r'MIN\((\w+)\.(\w+)\)', return_clause, re.IGNORECASE)
        if min_match:
            var, attr = min_match.groups()
            entity_type = entity_vars.get(var, 'movie')
            attr_name = map_attr(attr, entity_type)
            var_name = attr_vars.get((var, attr_name), f"${attr_name}")
            lines.append(f"reduce $min = min({var_name});")
            return "\n".join(lines)

    # Sort
    if order_clause:
        sort_items = []
        pattern = r'(\w+)\.(\w+)(?:\s+(ASC|DESC))?'
        for match in re.finditer(pattern, order_clause, re.IGNORECASE):
            var, attr, direction = match.groups()
            direction = (direction or 'asc').lower()
            if var in entity_vars:
                entity_type = entity_vars[var]
                attr_name = map_attr(attr, entity_type)
                var_name = attr_vars.get((var, attr_name), f"${attr_name}")
                sort_items.append(f"{var_name} {direction}")

        if sort_items:
            lines.append("sort " + ", ".join(sort_items) + ";")

    # Limit
    if limit_value:
        lines.append(f"limit {limit_value};")

    # Fetch
    fetch_parts = build_fetch(return_clause, entity_vars, attr_vars, rel_attr_vars, has_distinct)
    if fetch_parts:
        lines.append("fetch {")
        lines.append("  " + ", ".join(fetch_parts))
        lines.append("};")

    return "\n".join(lines)

def build_agg_query(lines, with_clause, return_clause, order_clause, limit_value, entity_vars, attr_vars, rel_attr_vars=None, rel_vars=None):
    """Build aggregation queries with groupby."""
    rel_attr_vars = rel_attr_vars or {}
    rel_vars = rel_vars or []

    # Find groupby variable
    groupby_match = re.search(r'WITH\s+(\w+)\s*,', with_clause, re.IGNORECASE)
    groupby_var = groupby_match.group(1) if groupby_match else None

    # Find aggregation type
    count_match = re.search(r'COUNT\((?:DISTINCT\s+)?(\*|\w+)\)', with_clause, re.IGNORECASE)
    sum_match = re.search(r'SUM\((\w+)\.(\w+)\)', with_clause, re.IGNORECASE)
    avg_match = re.search(r'AVG\((\w+)\.(\w+)\)', with_clause, re.IGNORECASE)

    agg_var = None
    if count_match:
        count_target = count_match.group(1)
        if count_target == '*':
            first_var = list(entity_vars.keys())[0]
            target = f"${first_var}"
        else:
            target = f"${count_target}"
        if groupby_var:
            lines.append(f"reduce $count = count({target}) groupby ${groupby_var};")
        else:
            lines.append(f"reduce $count = count({target});")
        agg_var = '$count'

    elif sum_match:
        var, attr = sum_match.groups()
        # Check if it's a relation attribute or entity attribute
        if var in rel_vars:
            var_name = rel_attr_vars.get((var, attr), f"${attr}")
        else:
            entity_type = entity_vars.get(var, 'movie')
            attr_name = map_attr(attr, entity_type)
            var_name = attr_vars.get((var, attr_name), f"${attr_name}")
        if groupby_var:
            lines.append(f"reduce $total = sum({var_name}) groupby ${groupby_var};")
        else:
            lines.append(f"reduce $total = sum({var_name});")
        agg_var = '$total'

    elif avg_match:
        var, attr = avg_match.groups()
        # Check if it's a relation attribute or entity attribute
        if var in rel_vars:
            var_name = rel_attr_vars.get((var, attr), f"${attr}")
        else:
            entity_type = entity_vars.get(var, 'movie')
            attr_name = map_attr(attr, entity_type)
            var_name = attr_vars.get((var, attr_name), f"${attr_name}")
        if groupby_var:
            lines.append(f"reduce $avg = mean({var_name}) groupby ${groupby_var};")
        else:
            lines.append(f"reduce $avg = mean({var_name});")
        agg_var = '$avg'

    # Check for HAVING-like filter after WITH
    # Pattern: WITH ... WHERE count > N
    having_match = re.search(r'WITH.*?WHERE\s+(\w+)\s*(>|<|>=|<=|=)\s*(\d+)', with_clause + " " + return_clause, re.IGNORECASE | re.DOTALL)
    if having_match:
        alias, op, val = having_match.groups()
        if agg_var:
            lines.append(f"match {agg_var} {op} {val};")

    # Sort
    if order_clause:
        if 'count' in order_clause.lower() or 'COUNT' in order_clause:
            direction = 'desc' if 'DESC' in order_clause.upper() else 'asc'
            lines.append(f"sort $count {direction};")
        elif 'total' in order_clause.lower():
            direction = 'desc' if 'DESC' in order_clause.upper() else 'asc'
            lines.append(f"sort $total {direction};")
        elif 'avg' in order_clause.lower():
            direction = 'desc' if 'DESC' in order_clause.upper() else 'asc'
            lines.append(f"sort $avg {direction};")
        else:
            # Try to parse normal order by
            pattern = r'(\w+)\.(\w+)(?:\s+(ASC|DESC))?'
            for match in re.finditer(pattern, order_clause, re.IGNORECASE):
                var, attr, direction = match.groups()
                direction = (direction or 'asc').lower()
                if var in entity_vars:
                    entity_type = entity_vars[var]
                    attr_name = map_attr(attr, entity_type)
                    var_name = attr_vars.get((var, attr_name), f"${attr_name}")
                    lines.append(f"sort {var_name} {direction};")
                    break

    # Limit
    if limit_value:
        lines.append(f"limit {limit_value};")

    # Fetch
    fetch_parts = []
    if groupby_var and groupby_var in entity_vars:
        entity_type = entity_vars[groupby_var]
        for (v, attr), var_name in attr_vars.items():
            if v == groupby_var:
                fetch_parts.append(f'"{attr}": {var_name}')

    if agg_var:
        agg_name = agg_var.lstrip('$')
        fetch_parts.append(f'"{agg_name}": {agg_var}')

    if fetch_parts:
        lines.append("fetch {")
        lines.append("  " + ", ".join(fetch_parts))
        lines.append("};")

    return "\n".join(lines)

def build_fetch(return_clause, entity_vars, attr_vars, rel_attr_vars, has_distinct):
    """Build fetch clause parts."""
    parts = []

    # Extract explicit aliases: expr AS alias
    alias_pattern = r'(\w+(?:\.\w+)?)\s+AS\s+(\w+)'
    aliases = {}
    for match in re.finditer(alias_pattern, return_clause, re.IGNORECASE):
        expr, alias = match.groups()
        aliases[expr] = alias

    # Process var.attr patterns
    for var, entity_type in entity_vars.items():
        pattern = rf'{var}\.(\w+)'
        for match in re.finditer(pattern, return_clause):
            attr = match.group(1)
            full_expr = f"{var}.{attr}"
            attr_name = map_attr(attr, entity_type)
            var_name = attr_vars.get((var, attr_name))
            if var_name:
                key = aliases.get(full_expr, attr)
                if f'"{key}":' not in str(parts):
                    parts.append(f'"{key}": {var_name}')

    # Process relation attributes
    for (rel_var, attr), var_name in rel_attr_vars.items():
        full_expr = f"{rel_var}.{attr}"
        key = aliases.get(full_expr, attr)
        if f'"{key}":' not in str(parts):
            parts.append(f'"{key}": {var_name}')

    # Handle DISTINCT var - return the entity's key attributes
    if has_distinct:
        distinct_match = re.search(r'DISTINCT\s+(\w+)(?:\.(\w+))?', return_clause, re.IGNORECASE)
        if distinct_match:
            var = distinct_match.group(1)
            attr = distinct_match.group(2)
            if var in entity_vars and attr:
                entity_type = entity_vars[var]
                attr_name = map_attr(attr, entity_type)
                var_name = attr_vars.get((var, attr_name))
                if var_name and f'"{attr}":' not in str(parts):
                    parts.append(f'"{attr}": {var_name}')

    # Handle plain entity return (RETURN m)
    if not parts:
        for var in entity_vars:
            if re.search(rf'\bRETURN\s+(?:DISTINCT\s+)?{var}\b(?!\s*\.)', return_clause, re.IGNORECASE):
                for (v, attr), var_name in attr_vars.items():
                    if v == var:
                        parts.append(f'"{attr}": {var_name}')

    return parts

def validate_query(driver, database: str, typeql: str) -> tuple:
    """Validate a TypeQL query against the database."""
    if typeql.startswith('#'):
        return False, "Not converted"

    try:
        with driver.transaction(database, TransactionType.READ) as tx:
            result = tx.query(typeql).resolve()
            # Try to consume the result
            if 'fetch' in typeql:
                docs = list(result.as_concept_documents())
            elif 'reduce' in typeql:
                rows = list(result.as_concept_rows())
            else:
                rows = list(result.as_concept_rows())
        return True, None
    except Exception as e:
        return False, str(e)

def main():
    # Connect to TypeDB
    credentials = Credentials('admin', 'password')
    options = DriverOptions(is_tls_enabled=False)
    driver = TypeDB.driver('localhost:1729', credentials, options)

    database = 'text2typeql_neoflix'

    # Output files
    queries_csv = '/opt/text2typeql/output/neoflix/queries.csv'
    failed_csv = '/opt/text2typeql/output/neoflix/failed.csv'

    # Initialize CSV files
    with open(queries_csv, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['original_index', 'question', 'cypher', 'typeql'])

    with open(failed_csv, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['original_index', 'question', 'cypher', 'error'])

    success_count = 0
    fail_count = 0

    # Process all queries
    index = 0
    while True:
        query_data = get_query('neoflix', index)
        if not query_data:
            break

        question = query_data['question']
        cypher = query_data['cypher']

        # Convert
        try:
            typeql = convert_cypher_to_typeql(cypher, question)
        except Exception as e:
            typeql = f"# Conversion error: {e}"

        # Validate
        is_valid, error = validate_query(driver, database, typeql)

        if is_valid:
            with open(queries_csv, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([index, question, cypher, typeql])
            success_count += 1
            print(f"[{index}] SUCCESS")
        else:
            with open(failed_csv, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([index, question, cypher, error])
            fail_count += 1
            print(f"[{index}] FAILED: {error[:80]}...")

        index += 1

        if index % 50 == 0:
            print(f"Progress: {index} processed, {success_count} success, {fail_count} failed")

    print(f"\nFinal: {success_count} success, {fail_count} failed out of {index} total")
    driver.close()

if __name__ == '__main__':
    main()
