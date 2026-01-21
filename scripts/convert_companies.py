#!/usr/bin/env python3
"""Convert Cypher queries to TypeQL for companies database."""

import csv
import json
import re
import sys
from typing import Optional, Tuple, List, Dict, Set
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# Cypher to TypeQL relation/role mappings for companies schema
# Format: relationship -> (relation_name, source_role, target_role)
# For -[:REL]->: source plays source_role, target plays target_role
RELATION_MAPPINGS = {
    # HAS_CEO: (org)-[:HAS_CEO]->(person) means org's CEO is person
    # So org plays organization role, person plays ceo role
    # Note: Some Cypher queries use reversed pattern which is semantically inconsistent
    'HAS_CEO': ('ceo_of', 'organization', 'ceo'),
    'IN_CITY': ('located_in', 'organization', 'city'),
    'IN_COUNTRY': ('in_country', 'city', 'country'),
    'HAS_CATEGORY': ('in_category', 'organization', 'category'),
    'HAS_SUBSIDIARY': ('subsidiary_of', 'parent', 'subsidiary'),
    'SUBSIDIARY_OF': ('subsidiary_of', 'subsidiary', 'parent'),
    'HAS_SUPPLIER': ('supplies', 'customer', 'supplier'),
    'SUPPLIES': ('supplies', 'supplier', 'customer'),
    # HAS_INVESTOR: (X)-[:HAS_INVESTOR]->(Y) means X invests in Y
    # invested_in relation: organization (being invested in), investor (doing the investing)
    # So X is investor, Y is organization
    # Note: Schema only allows person as investor, but Cypher queries often have org-to-org investments
    'HAS_INVESTOR': ('invested_in', 'investor', 'organization'),
    'HAS_BOARD_MEMBER': ('board_member_of', 'organization', 'member'),
    'HAS_COMPETITOR': ('competes_with', 'competitor', 'competitor'),
    'HAS_CHUNK': ('has_chunk', 'article', 'chunk'),
    'MENTIONS': ('mentions', 'article', 'organization'),
    'PARENT_OF': ('parent_of', 'parent', 'child'),
    'HAS_PARENT': ('parent_of', 'child', 'parent'),
}

# Cypher property to TypeQL attribute mappings
PROPERTY_MAPPINGS = {
    'nbrEmployees': 'nbr_employees',
    'isPublic': 'is_public',
    'isDissolved': 'is_dissolved',
    'siteName': 'site_name',
}

# Entity-specific attribute names (entity_type -> cypher_prop -> typeql_attr)
ENTITY_ATTRIBUTES = {
    'city': {
        'name': 'city_name',
        'summary': 'city_summary',
        'id': 'city_id',
    },
    'country': {
        'name': 'country_name',
        'summary': 'country_summary',
        'id': 'country_id',
    },
    'article': {
        'summary': 'article_summary',
        'id': 'article_id',
    },
    'industry_category': {
        'name': 'industry_category_name',
        'id': 'industry_category_id',
    },
    'organization': {
        'id': 'organization_id',
    },
    'person': {
        'id': 'person_id',
    },
    'fewshot': {
        'id': 'fewshot_id',
    },
}

# Cypher node labels to TypeQL entity types
ENTITY_MAPPINGS = {
    'Organization': 'organization',
    'Person': 'person',
    'City': 'city',
    'Country': 'country',
    'Article': 'article',
    'Chunk': 'chunk',
    'IndustryCategory': 'industry_category',
    'Fewshot': 'fewshot',
}

def convert_property_name(prop: str, entity_type: str = None) -> str:
    """Convert Cypher property name to TypeQL attribute name."""
    # First check entity-specific mappings
    if entity_type and entity_type in ENTITY_ATTRIBUTES:
        if prop in ENTITY_ATTRIBUTES[entity_type]:
            return ENTITY_ATTRIBUTES[entity_type][prop]

    # Then general mappings
    return PROPERTY_MAPPINGS.get(prop, prop.lower())

class CypherConverter:
    def __init__(self):
        self.variables: Dict[str, str] = {}  # var -> entity type
        self.attr_vars: Dict[str, str] = {}  # attr_var -> declaration pattern
        self.match_patterns: List[str] = []
        self.declared_attr_vars: Set[str] = set()
        self.agg_vars: Dict[str, str] = {}  # aggregation alias -> aggregation expression

    def reset(self):
        self.variables = {}
        self.attr_vars = {}
        self.match_patterns = []
        self.declared_attr_vars = set()
        self.agg_vars = {}

    def get_attr_name(self, var: str, prop: str) -> str:
        """Get proper attribute name for var.prop."""
        entity_type = self.variables.get(var)
        return convert_property_name(prop, entity_type)

    def ensure_attr_var(self, var: str, prop: str) -> str:
        """Ensure attribute variable is declared, return the var name."""
        attr_name = self.get_attr_name(var, prop)
        attr_var = f'${var}_{attr_name}'

        if attr_var not in self.declared_attr_vars:
            self.match_patterns.append(f'${var} has {attr_name} {attr_var};')
            self.declared_attr_vars.add(attr_var)

        return attr_var

    def convert(self, cypher: str, question: str) -> Tuple[Optional[str], Optional[str]]:
        """Convert a Cypher query to TypeQL."""
        self.reset()

        try:
            cypher = cypher.strip()

            # Parse Cypher query components
            match_parts = []
            where_parts = []
            with_parts = []
            return_parts = []
            order_by = None
            limit = None
            with_where = None  # WHERE after WITH (HAVING-like)

            lines = cypher.split('\n')
            current_section = None
            current_buffer = []
            after_with = False

            for line in lines:
                line = line.strip()
                if not line:
                    continue

                upper = line.upper()

                if upper.startswith('MATCH'):
                    if current_section and current_buffer:
                        if current_section == 'MATCH':
                            match_parts.append(' '.join(current_buffer))
                        elif current_section == 'WHERE':
                            if after_with:
                                with_where = ' '.join(current_buffer)
                            else:
                                where_parts.append(' '.join(current_buffer))
                        elif current_section == 'WITH':
                            with_parts.append(' '.join(current_buffer))
                    current_section = 'MATCH'
                    current_buffer = [line[5:].strip()]
                elif upper.startswith('WHERE'):
                    if current_section and current_buffer:
                        if current_section == 'MATCH':
                            match_parts.append(' '.join(current_buffer))
                        elif current_section == 'WITH':
                            with_parts.append(' '.join(current_buffer))
                    current_section = 'WHERE'
                    current_buffer = [line[5:].strip()]
                elif upper.startswith('WITH'):
                    if current_section and current_buffer:
                        if current_section == 'MATCH':
                            match_parts.append(' '.join(current_buffer))
                        elif current_section == 'WHERE':
                            where_parts.append(' '.join(current_buffer))
                    current_section = 'WITH'
                    current_buffer = [line[4:].strip()]
                    after_with = True
                elif upper.startswith('RETURN'):
                    if current_section and current_buffer:
                        if current_section == 'MATCH':
                            match_parts.append(' '.join(current_buffer))
                        elif current_section == 'WHERE':
                            if after_with:
                                with_where = ' '.join(current_buffer)
                            else:
                                where_parts.append(' '.join(current_buffer))
                        elif current_section == 'WITH':
                            with_parts.append(' '.join(current_buffer))
                    return_parts.append(line[6:].strip())
                    current_section = 'RETURN'
                    current_buffer = []
                elif upper.startswith('ORDER BY'):
                    order_by = line[8:].strip()
                elif upper.startswith('LIMIT'):
                    limit = line[5:].strip()
                else:
                    if current_section:
                        current_buffer.append(line)

            # Finalize remaining buffer
            if current_section and current_buffer:
                if current_section == 'MATCH':
                    match_parts.append(' '.join(current_buffer))
                elif current_section == 'WHERE':
                    if after_with:
                        with_where = ' '.join(current_buffer)
                    else:
                        where_parts.append(' '.join(current_buffer))
                elif current_section == 'WITH':
                    with_parts.append(' '.join(current_buffer))

            # Process MATCH patterns
            for match_str in match_parts:
                self.parse_match_patterns(match_str)

            # Process WHERE conditions (before WITH)
            for where_str in where_parts:
                self.parse_where_conditions(where_str)

            # Check if we have WITH aggregation
            has_with_agg = False
            groupby_var = None
            agg_alias = None
            agg_target = None
            agg_func = None

            for with_str in with_parts:
                # Pattern: var, count(other) as alias
                agg_match = re.search(r'(\w+),\s*count\s*\(\s*(\w+)\s*\)\s*as\s+(\w+)', with_str, re.IGNORECASE)
                if agg_match:
                    has_with_agg = True
                    groupby_var = agg_match.group(1)
                    agg_target = agg_match.group(2)
                    agg_alias = agg_match.group(3)
                    agg_func = 'count'
                    self.agg_vars[agg_alias] = f'count(${agg_target})'

            # Process ORDER BY - check if it uses aggregation alias
            sort_clause = None
            sort_uses_agg = False
            sort_direction = 'asc'

            if order_by:
                # Check if ORDER BY uses an aggregation alias
                order_match = re.match(r'(\w+)(?:\s+(DESC|ASC))?', order_by, re.IGNORECASE)
                if order_match:
                    order_var = order_match.group(1)
                    direction = order_match.group(2)
                    if direction and direction.upper() == 'DESC':
                        sort_direction = 'desc'

                    if order_var in self.agg_vars:
                        sort_uses_agg = True
                        sort_clause = f'${order_var} {sort_direction}'
                    else:
                        sort_clause = self.parse_order_by(order_by)

            # Process RETURN to build fetch
            return_str = ' '.join(return_parts)

            # Build TypeQL query
            query_parts = ['match']
            query_parts.append('  ' + '\n  '.join(self.match_patterns))

            if has_with_agg:
                # Build aggregation query with HAVING pattern
                # reduce $alias = count($target) groupby $groupby;
                reduce_clause = f'reduce ${agg_alias} = {agg_func}(${agg_target}) groupby ${groupby_var}'
                query_parts.append(f'{reduce_clause};')

                # Add HAVING-like condition if present
                if with_where:
                    having_cond = self.parse_having_condition(with_where)
                    if having_cond:
                        query_parts.append(f'match {having_cond};')

                if sort_clause:
                    query_parts.append(f'sort {sort_clause};')

                if limit:
                    query_parts.append(f'limit {limit};')

                # Build fetch for aggregation results
                fetch_items = self.parse_return_with_agg(return_str, groupby_var, agg_alias)
                if fetch_items:
                    fetch_str = ', '.join(fetch_items)
                    query_parts.append(f'fetch {{ {fetch_str} }};')
                else:
                    return None, "Empty fetch clause for aggregation"
            else:
                # Regular query without WITH aggregation
                if sort_clause:
                    query_parts.append(f'sort {sort_clause};')

                if limit:
                    query_parts.append(f'limit {limit};')

                fetch_items, is_aggregation, agg_clause = self.parse_return(return_str)

                if is_aggregation:
                    query_parts.append(f'reduce {agg_clause};')
                else:
                    if fetch_items:
                        fetch_str = ', '.join(fetch_items)
                        query_parts.append(f'fetch {{ {fetch_str} }};')
                    else:
                        return None, "Empty fetch clause"

            typeql = '\n'.join(query_parts)
            return typeql, None

        except Exception as e:
            import traceback
            return None, f"{str(e)}: {traceback.format_exc()}"

    def parse_having_condition(self, where_str: str) -> Optional[str]:
        """Parse HAVING-like condition (WHERE after WITH)."""
        # Pattern: alias > N
        match = re.match(r'(\w+)\s*(>|<|>=|<=|=|<>)\s*(\d+)', where_str)
        if match:
            alias = match.group(1)
            op = match.group(2)
            value = match.group(3)

            if op == '=':
                op = '=='
            elif op == '<>':
                op = '!='

            return f'${alias} {op} {value}'

        return None

    def parse_return_with_agg(self, return_str: str, groupby_var: str, agg_alias: str) -> List[str]:
        """Parse RETURN clause for aggregation query."""
        fetch_items = []

        # Parse return items
        items = self.split_return_items(return_str)

        for item in items:
            item = item.strip()
            if not item:
                continue

            # Pattern: var.prop AS alias
            alias_match = re.match(r'(\w+)\.(\w+)\s+[aA][sS]\s+(\w+)', item)
            if alias_match:
                var = alias_match.group(1)
                prop = alias_match.group(2)
                alias = alias_match.group(3)
                attr_name = self.get_attr_name(var, prop)
                fetch_items.append(f'"{alias}": ${var}.{attr_name}')
                continue

            # Pattern: agg_alias AS alias
            agg_alias_match = re.match(r'(\w+)\s+[aA][sS]\s+(\w+)', item)
            if agg_alias_match:
                var = agg_alias_match.group(1)
                alias = agg_alias_match.group(2)
                if var in self.agg_vars or var == agg_alias:
                    fetch_items.append(f'"{alias}": ${var}')
                else:
                    fetch_items.append(f'"{alias}": ${var}')
                continue

            # Pattern: just agg_alias
            if item in self.agg_vars or item == agg_alias:
                fetch_items.append(f'"{item}": ${item}')
                continue

            # Pattern: var.prop (no alias)
            prop_match = re.match(r'(\w+)\.(\w+)$', item)
            if prop_match:
                var = prop_match.group(1)
                prop = prop_match.group(2)
                attr_name = self.get_attr_name(var, prop)
                fetch_items.append(f'"{attr_name}": ${var}.{attr_name}')
                continue

        return fetch_items

    def parse_match_patterns(self, match_str: str):
        """Parse MATCH patterns and add to self.match_patterns."""
        # Find node patterns: (var:Label {props}) or (var:Label) or (var)
        node_pattern = r'\((\w+)(?::(\w+))?(?:\s*\{([^}]+)\})?\)'

        # Parse nodes first to establish variables
        for match in re.finditer(node_pattern, match_str):
            var = match.group(1)
            label = match.group(2)
            props = match.group(3)

            if label:
                entity_type = ENTITY_MAPPINGS.get(label, label.lower())
                self.variables[var] = entity_type
                pattern = f'${var} isa {entity_type}'

                if props:
                    prop_patterns = self.parse_inline_props(props, var, entity_type)
                    if prop_patterns:
                        pattern += ', ' + ', '.join(prop_patterns)

                pattern += ';'
                self.match_patterns.append(pattern)

        # Find and process relationships
        # Pattern: (var1)-[:REL]->(var2) or (var1)<-[:REL]-(var2)
        rel_pattern = r'\((\w+)[^)]*\)\s*(<)?-\[:(\w+)\]-(>)?\s*\((\w+)[^)]*\)'

        for match in re.finditer(rel_pattern, match_str):
            var1 = match.group(1)
            left_arrow = match.group(2)
            rel_type = match.group(3)
            right_arrow = match.group(4)
            var2 = match.group(5)

            if rel_type in RELATION_MAPPINGS:
                rel_name, role1, role2 = RELATION_MAPPINGS[rel_type]

                # Handle symmetric relations (like competes_with)
                if role1 == role2:
                    self.match_patterns.append(f'({role1}: ${var1}, {role2}: ${var2}) isa {rel_name};')
                elif left_arrow:  # <-[:REL]-
                    # var1 is target (role2), var2 is source (role1)
                    self.match_patterns.append(f'({role1}: ${var2}, {role2}: ${var1}) isa {rel_name};')
                else:  # -[:REL]->
                    # var1 is source (role1), var2 is target (role2)
                    self.match_patterns.append(f'({role1}: ${var1}, {role2}: ${var2}) isa {rel_name};')

    def parse_inline_props(self, props_str: str, var: str, entity_type: str) -> list:
        """Parse inline property constraints like {name: 'Accenture'}."""
        patterns = []

        # Match property: 'value' pairs
        for match in re.finditer(r"(\w+):\s*'([^']*)'", props_str):
            prop = match.group(1)
            attr_name = convert_property_name(prop, entity_type)
            value = match.group(2)
            patterns.append(f'has {attr_name} "{value}"')

        # Match property: number pairs
        for match in re.finditer(r'(\w+):\s*(\d+(?:\.\d+)?)\b', props_str):
            prop = match.group(1)
            attr_name = convert_property_name(prop, entity_type)
            value = match.group(2)
            patterns.append(f'has {attr_name} {value}')

        # Match property: boolean pairs
        for match in re.finditer(r'(\w+):\s*(true|false)\b', props_str, re.IGNORECASE):
            prop = match.group(1)
            attr_name = convert_property_name(prop, entity_type)
            value = match.group(2).lower()
            patterns.append(f'has {attr_name} {value}')

        return patterns

    def parse_where_conditions(self, where_str: str):
        """Parse WHERE conditions and add to self.match_patterns."""
        # Handle AND conditions
        conditions = re.split(r'\s+AND\s+', where_str, flags=re.IGNORECASE)

        for cond in conditions:
            cond = cond.strip()
            if not cond:
                continue

            # Pattern: var.prop > value (numeric comparison)
            comp_match = re.match(r'(\w+)\.(\w+)\s*(>|<|>=|<=|=|<>)\s*(\d+(?:\.\d+)?)', cond)
            if comp_match:
                var = comp_match.group(1)
                prop = comp_match.group(2)
                op = comp_match.group(3)
                value = comp_match.group(4)

                if op == '=':
                    op = '=='
                elif op == '<>':
                    op = '!='

                attr_var = self.ensure_attr_var(var, prop)
                self.match_patterns.append(f'{attr_var} {op} {value};')
                continue

            # Pattern: var.prop = 'string'
            str_match = re.match(r"(\w+)\.(\w+)\s*=\s*'([^']*)'", cond)
            if str_match:
                var = str_match.group(1)
                prop = str_match.group(2)
                value = str_match.group(3)
                attr_name = self.get_attr_name(var, prop)
                self.match_patterns.append(f'${var} has {attr_name} "{value}";')
                continue

            # Pattern: var.prop = true/false
            bool_match = re.match(r'(\w+)\.(\w+)\s*=\s*(true|false)', cond, re.IGNORECASE)
            if bool_match:
                var = bool_match.group(1)
                prop = bool_match.group(2)
                value = bool_match.group(3).lower()
                attr_name = self.get_attr_name(var, prop)
                self.match_patterns.append(f'${var} has {attr_name} {value};')
                continue

            # Pattern: var.prop IS NOT NULL
            not_null_match = re.match(r'(\w+)\.(\w+)\s+IS\s+NOT\s+NULL', cond, re.IGNORECASE)
            if not_null_match:
                var = not_null_match.group(1)
                prop = not_null_match.group(2)
                self.ensure_attr_var(var, prop)
                continue

            # Pattern: var.prop CONTAINS 'string'
            contains_match = re.match(r"(\w+)\.(\w+)\s+CONTAINS\s+'([^']*)'", cond, re.IGNORECASE)
            if contains_match:
                var = contains_match.group(1)
                prop = contains_match.group(2)
                value = contains_match.group(3)
                attr_var = self.ensure_attr_var(var, prop)
                self.match_patterns.append(f'{attr_var} contains "{value}";')
                continue

            # Pattern: var.prop STARTS WITH 'string'
            starts_match = re.match(r"(\w+)\.(\w+)\s+STARTS\s+WITH\s+'([^']*)'", cond, re.IGNORECASE)
            if starts_match:
                var = starts_match.group(1)
                prop = starts_match.group(2)
                value = starts_match.group(3)
                attr_var = self.ensure_attr_var(var, prop)
                self.match_patterns.append(f'{attr_var} like "^{value}.*";')
                continue

    def parse_return(self, return_str: str) -> tuple:
        """Parse RETURN clause to build fetch items.

        Returns (fetch_items, is_aggregation, aggregation_clause)
        """
        fetch_items = []
        is_aggregation = False
        agg_clause = None

        # Check for count aggregation
        count_match = re.search(r'count\s*\(\s*(?:DISTINCT\s+)?(\w+|\*)\s*\)\s*(?:AS\s+(\w+))?', return_str, re.IGNORECASE)
        if count_match:
            is_aggregation = True
            target = count_match.group(1)
            alias = count_match.group(2) or 'count'

            if target == '*':
                # Count first available variable
                for var in self.variables:
                    agg_clause = f'${alias} = count(${var})'
                    break
            else:
                agg_clause = f'${alias} = count(${target})'
            return fetch_items, is_aggregation, agg_clause

        # Remove DISTINCT keyword (TypeQL handles this differently)
        return_str = re.sub(r'\bDISTINCT\s+', '', return_str, flags=re.IGNORECASE)

        # Parse return items
        items = self.split_return_items(return_str)

        for item in items:
            item = item.strip()
            if not item:
                continue

            # Pattern: var.prop AS alias
            alias_match = re.match(r'(\w+)\.(\w+)\s+AS\s+(\w+)', item, re.IGNORECASE)
            if alias_match:
                var = alias_match.group(1)
                prop = alias_match.group(2)
                alias = alias_match.group(3)
                attr_name = self.get_attr_name(var, prop)
                fetch_items.append(f'"{alias}": ${var}.{attr_name}')
                continue

            # Pattern: var.prop (no alias)
            prop_match = re.match(r'(\w+)\.(\w+)$', item)
            if prop_match:
                var = prop_match.group(1)
                prop = prop_match.group(2)
                attr_name = self.get_attr_name(var, prop)
                fetch_items.append(f'"{attr_name}": ${var}.{attr_name}')
                continue

            # Pattern: var AS alias
            var_alias_match = re.match(r'(\w+)\s+AS\s+(\w+)', item, re.IGNORECASE)
            if var_alias_match:
                var = var_alias_match.group(1)
                alias = var_alias_match.group(2)
                fetch_items.append(f'"{alias}": ${var}')
                continue

            # Pattern: just variable name
            if re.match(r'^\w+$', item):
                var = item
                if var in self.variables:
                    fetch_items.append(f'"{var}": ${var}')

        return fetch_items, is_aggregation, agg_clause

    def split_return_items(self, return_str: str) -> list:
        """Split RETURN items by comma, respecting parentheses."""
        items = []
        depth = 0
        current = ''

        for char in return_str:
            if char == '(':
                depth += 1
                current += char
            elif char == ')':
                depth -= 1
                current += char
            elif char == ',' and depth == 0:
                items.append(current.strip())
                current = ''
            else:
                current += char

        if current.strip():
            items.append(current.strip())

        return items

    def parse_order_by(self, order_str: str) -> str:
        """Parse ORDER BY clause and ensure attr var is declared."""
        # Pattern: var.prop DESC/ASC
        match = re.match(r'(\w+)\.(\w+)(?:\s+(DESC|ASC))?', order_str, re.IGNORECASE)
        if match:
            var = match.group(1)
            prop = match.group(2)
            direction = match.group(3)

            attr_var = self.ensure_attr_var(var, prop)
            if direction and direction.upper() == 'DESC':
                return f'{attr_var} desc'
            return f'{attr_var} asc'

        return None


def validate_typeql(typeql: str, driver) -> Tuple[bool, Optional[str]]:
    """Validate TypeQL query against TypeDB."""
    try:
        with driver.transaction('text2typeql_companies', TransactionType.READ) as tx:
            result = tx.query(typeql).resolve()
            return True, None
    except Exception as e:
        return False, str(e)


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


def main():
    """Convert all companies queries."""
    # Connect to TypeDB
    credentials = Credentials('admin', 'password')
    options = DriverOptions(is_tls_enabled=False)
    driver = TypeDB.driver('localhost:1729', credentials, options)

    queries_file = '/opt/text2typeql/output/companies/queries.csv'
    failed_file = '/opt/text2typeql/output/companies/failed.csv'

    # Initialize CSV files
    with open(queries_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['original_index', 'question', 'cypher', 'typeql'])

    with open(failed_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['original_index', 'question', 'cypher', 'error'])

    converter = CypherConverter()
    total = 933
    success_count = 0
    fail_count = 0

    for i in range(total):
        query_data = get_query('companies', i)
        if not query_data:
            print(f"[{i}] Query not found")
            continue

        question = query_data['question']
        cypher = query_data['cypher']

        typeql, convert_error = converter.convert(cypher, question)

        if convert_error:
            with open(failed_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([i, question, cypher, f"Conversion error: {convert_error}"])
            fail_count += 1
            print(f"[{i}] FAIL (conversion): {convert_error[:50]}")
            continue

        # Validate against TypeDB
        is_valid, validate_error = validate_typeql(typeql, driver)

        if is_valid:
            with open(queries_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([i, question, cypher, typeql])
            success_count += 1
            print(f"[{i}] OK")
        else:
            with open(failed_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([i, question, cypher, f"Validation error: {validate_error}"])
            fail_count += 1
            print(f"[{i}] FAIL (validation): {validate_error[:80] if validate_error else 'Unknown'}")

    driver.close()

    print(f"\n=== Summary ===")
    print(f"Total: {total}")
    print(f"Success: {success_count}")
    print(f"Failed: {fail_count}")
    print(f"Queries written to: {queries_file}")
    print(f"Failed queries written to: {failed_file}")


if __name__ == '__main__':
    main()
