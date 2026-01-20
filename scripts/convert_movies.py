#!/usr/bin/env python3
"""Convert all movies Cypher queries to TypeQL with validation."""

import csv
import json
import re
import sys
from typing import Optional, Tuple, List, Dict, Set
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# Database configuration
DATABASE = "text2typeql_movies"
CSV_PATH = "/opt/text2typeql/data/text2cypher/datasets/synthetic_opus_demodbs/text2cypher_claudeopus.csv"
OUTPUT_PATH = "/opt/text2typeql/output/movies/queries.csv"
FAILED_PATH = "/opt/text2typeql/output/movies/failed.csv"

class CypherToTypeQLConverter:
    """Converts Cypher queries to TypeQL for the movies schema."""

    def __init__(self):
        self.relation_map = {
            'ACTED_IN': ('acted_in', 'actor', 'film'),
            'DIRECTED': ('directed', 'director', 'film'),
            'PRODUCED': ('produced', 'producer', 'film'),
            'WROTE': ('wrote', 'writer', 'film'),
            'FOLLOWS': ('follows', 'follower', 'followed'),
            'REVIEWED': ('reviewed', 'reviewer', 'film'),
        }

        self.entity_map = {
            'Person': 'person',
            'Movie': 'movie',
        }

        self.entity_attrs = {
            'person': ['name', 'born'],
            'movie': ['title', 'votes', 'tagline', 'released'],
        }

        self.relation_attrs = {
            'acted_in': ['roles'],
            'reviewed': ['summary', 'rating'],
        }

        # State tracking
        self.var_types: Dict[str, str] = {}  # var -> entity type
        self.rel_vars: Dict[str, str] = {}   # rel_var -> relation type
        self.declared_attrs: Set[str] = set()
        self.rel_attr_refs: Dict[str, Set[str]] = {}  # rel_var -> set of attrs to declare

    def convert(self, cypher: str) -> str:
        """Convert a Cypher query to TypeQL."""
        # Reset state
        self.var_types = {}
        self.rel_vars = {}
        self.declared_attrs = set()
        self.rel_attr_refs = {}

        cypher = cypher.strip()

        # Check for unsupported patterns
        if re.search(r'\[(\w+)?\]\s*->', cypher) and not re.search(r'\[\w*:\w+\]', cypher):
            raise ValueError("Generic relationship patterns without type are not supported")

        if 'type(r)' in cypher.lower() or 'type(' in cypher.lower():
            raise ValueError("type() function is not supported")

        if 'id(' in cypher.lower():
            raise ValueError("id() function is not supported")

        if 'size(' in cypher.lower():
            raise ValueError("size() function is not supported")

        # First pass: scan for all relation attribute references
        self._scan_rel_attr_refs(cypher)

        # Parse the Cypher query
        match_clause = self._extract_match(cypher)
        where_clause = self._extract_where(cypher)
        with_clause = self._extract_with(cypher)
        return_clause = self._extract_return(cypher)
        order_clause = self._extract_order(cypher)
        limit_clause = self._extract_limit(cypher)

        has_count = return_clause and 'count(' in return_clause.lower()
        has_with_count = with_clause and 'count(' in with_clause.lower()
        has_collect = return_clause and 'collect(' in return_clause.lower()
        has_sum = return_clause and 'sum(' in return_clause.lower()
        has_avg = return_clause and 'avg(' in return_clause.lower()
        has_min = return_clause and 'min(' in return_clause.lower()
        has_max = return_clause and 'max(' in return_clause.lower()

        # Handle WITH ... ORDER BY ... RETURN pattern (without WHERE)
        if has_with_count and not where_clause:
            return self._convert_with_count_order(cypher, match_clause, with_clause, return_clause, order_clause, limit_clause)

        # Handle WITH ... WHERE pattern (HAVING equivalent)
        if has_with_count and where_clause:
            return self._convert_with_count_where(cypher, match_clause, with_clause, where_clause, return_clause, order_clause, limit_clause)

        # Parse MATCH patterns first
        match_result = self._parse_match_patterns(match_clause) if match_clause else {'patterns': [], 'variables': {}}
        match_patterns = match_result['patterns']

        # Parse WHERE conditions
        if where_clause:
            where_patterns = self._parse_where(where_clause)
            match_patterns.extend(where_patterns)

        # Handle aggregations
        if has_count:
            return self._convert_count_query(cypher, match_patterns, return_clause, order_clause, limit_clause)

        if has_sum or has_avg or has_min or has_max:
            return self._convert_aggregate_query(cypher, match_patterns, return_clause, order_clause, limit_clause)

        if has_collect:
            return self._convert_collect_query(cypher, match_patterns, return_clause, order_clause, limit_clause)

        # Parse RETURN clause
        fetch_items = []
        if return_clause:
            fetch_items, extra_patterns = self._parse_return(return_clause, match_patterns)
            match_patterns.extend(extra_patterns)

        # Parse ORDER BY
        sort_clause = ""
        if order_clause:
            sort_var, sort_dir, extra_patterns = self._parse_order(order_clause, match_patterns)
            match_patterns.extend(extra_patterns)
            if sort_var:
                sort_clause = f"sort ${sort_var} {sort_dir};\n"

        # Parse LIMIT
        limit_num = None
        if limit_clause:
            limit_match = re.search(r'LIMIT\s+(\d+)', limit_clause, re.IGNORECASE)
            if limit_match:
                limit_num = limit_match.group(1)

        # Build the query
        typeql = "match\n"
        seen = set()
        unique_patterns = []
        for pattern in match_patterns:
            if pattern not in seen:
                seen.add(pattern)
                unique_patterns.append(pattern)

        for pattern in unique_patterns:
            typeql += f"  {pattern};\n"

        if sort_clause:
            typeql += sort_clause

        if limit_num:
            typeql += f"limit {limit_num};\n"

        if fetch_items:
            typeql += "fetch {\n"
            fetch_strs = []
            for item in fetch_items:
                key = item['key']
                val = item['value']
                fetch_strs.append(f'  "{key}": {val}')
            typeql += ",\n".join(fetch_strs)
            typeql += "\n};"
        else:
            typeql += "fetch { };"

        return typeql

    def _scan_rel_attr_refs(self, cypher: str):
        """Scan for relation attribute references (e.g., r.rating)."""
        # Find all var.attr references
        refs = re.findall(r'(\w+)\.(\w+)', cypher)

        # We'll identify which are relation vars after parsing MATCH
        # For now, just record all
        for var, attr in refs:
            # Store as potential relation attr ref
            if var not in self.rel_attr_refs:
                self.rel_attr_refs[var] = set()
            self.rel_attr_refs[var].add(attr)

    def _extract_match(self, cypher: str) -> Optional[str]:
        match = re.search(r'MATCH\s+(.+?)(?=\s*(?:WHERE|WITH|RETURN|ORDER|LIMIT|$))', cypher, re.IGNORECASE | re.DOTALL)
        return match.group(1).strip() if match else None

    def _extract_where(self, cypher: str) -> Optional[str]:
        match = re.search(r'WHERE\s+(.+?)(?=\s*(?:WITH|RETURN|ORDER|LIMIT|$))', cypher, re.IGNORECASE | re.DOTALL)
        return match.group(1).strip() if match else None

    def _extract_with(self, cypher: str) -> Optional[str]:
        match = re.search(r'WITH\s+(.+?)(?=\s*(?:WHERE|ORDER|RETURN|LIMIT|$))', cypher, re.IGNORECASE | re.DOTALL)
        return match.group(1).strip() if match else None

    def _extract_return(self, cypher: str) -> Optional[str]:
        match = re.search(r'RETURN\s+(.+?)(?=\s*(?:ORDER|LIMIT|$))', cypher, re.IGNORECASE | re.DOTALL)
        return match.group(1).strip() if match else None

    def _extract_order(self, cypher: str) -> Optional[str]:
        match = re.search(r'ORDER\s+BY\s+(.+?)(?=\s*(?:LIMIT|$))', cypher, re.IGNORECASE | re.DOTALL)
        return match.group(1).strip() if match else None

    def _extract_limit(self, cypher: str) -> Optional[str]:
        match = re.search(r'(LIMIT\s+\d+)', cypher, re.IGNORECASE)
        return match.group(1) if match else None

    def _parse_match_patterns(self, match_clause: str) -> dict:
        """Parse MATCH patterns into TypeQL patterns."""
        patterns = []
        pattern_parts = self._split_match_patterns(match_clause)

        for part in pattern_parts:
            part = part.strip()
            if not part:
                continue

            rel_match = re.search(r'\((\w+)(?::(\w+))?(?:\s*\{([^}]+)\})?\)\s*(<)?-\s*\[(\w*):?(\w*)?\]\s*-\s*(>)?\s*\((\w+)(?::(\w+))?(?:\s*\{([^}]+)\})?\)', part)

            if rel_match:
                var1, type1, props1, left_arrow, rel_var, rel_type, right_arrow, var2, type2, props2 = rel_match.groups()

                is_left = left_arrow == '<'
                is_right = right_arrow == '>'

                tql_type1 = self.entity_map.get(type1) if type1 else self.var_types.get(var1)
                tql_type2 = self.entity_map.get(type2) if type2 else self.var_types.get(var2)

                # Add entity patterns
                if var1 not in self.var_types:
                    if tql_type1:
                        pattern = f"${var1} isa {tql_type1}"
                        if props1:
                            prop_patterns = self._parse_props(props1, var1)
                            pattern += ", " + ", ".join(prop_patterns)
                        patterns.append(pattern)
                        self.var_types[var1] = tql_type1
                elif props1:
                    prop_patterns = self._parse_props(props1, var1)
                    for pp in prop_patterns:
                        patterns.append(f"${var1} {pp}")

                if var2 not in self.var_types:
                    if tql_type2:
                        pattern = f"${var2} isa {tql_type2}"
                        if props2:
                            prop_patterns = self._parse_props(props2, var2)
                            pattern += ", " + ", ".join(prop_patterns)
                        patterns.append(pattern)
                        self.var_types[var2] = tql_type2
                elif props2:
                    prop_patterns = self._parse_props(props2, var2)
                    for pp in prop_patterns:
                        patterns.append(f"${var2} {pp}")

                # Add relation pattern
                if rel_type:
                    rel_info = self.relation_map.get(rel_type)
                    if rel_info:
                        tql_rel, role1, role2 = rel_info

                        # Determine role assignment based on direction
                        if is_left and not is_right:
                            src_var, tgt_var = var2, var1
                        else:
                            src_var, tgt_var = var1, var2

                        # Build relation pattern
                        if rel_var:
                            self.rel_vars[rel_var] = tql_rel
                            # Check if we need to declare relation attributes inline
                            rel_attrs_needed = self.rel_attr_refs.get(rel_var, set())
                            valid_attrs = set(self.relation_attrs.get(tql_rel, []))
                            attrs_to_declare = rel_attrs_needed & valid_attrs

                            rel_pattern = f"${rel_var} ({role1}: ${src_var}, {role2}: ${tgt_var}) isa {tql_rel}"
                            for attr in attrs_to_declare:
                                attr_var = f"{attr}{rel_var}"
                                rel_pattern += f", has {attr} ${attr_var}"
                                self.declared_attrs.add(attr_var)
                            patterns.append(rel_pattern)
                        else:
                            patterns.append(f"({role1}: ${src_var}, {role2}: ${tgt_var}) isa {tql_rel}")
            else:
                # Standalone node pattern
                node_match = re.search(r'\((\w+)(?::(\w+))?(?:\s*\{([^}]+)\})?\)', part)
                if node_match:
                    var, node_type, props = node_match.groups()
                    if var not in self.var_types:
                        tql_type = self.entity_map.get(node_type) if node_type else None
                        if tql_type:
                            pattern = f"${var} isa {tql_type}"
                            if props:
                                prop_patterns = self._parse_props(props, var)
                                pattern += ", " + ", ".join(prop_patterns)
                            patterns.append(pattern)
                            self.var_types[var] = tql_type

        return {'patterns': patterns, 'variables': self.var_types}

    def _split_match_patterns(self, match_clause: str) -> List[str]:
        parts = []
        current = ""
        depth = 0

        for char in match_clause:
            if char in '([':
                depth += 1
            elif char in ')]':
                depth -= 1
            elif char == ',' and depth == 0:
                parts.append(current.strip())
                current = ""
                continue
            current += char

        if current.strip():
            parts.append(current.strip())

        return parts

    def _parse_props(self, props_str: str, var: str) -> List[str]:
        patterns = []
        prop_matches = re.finditer(r"(\w+)\s*:\s*(?:'([^']*)'|\"([^\"]*)\"|(\d+))", props_str)
        for m in prop_matches:
            prop_name = m.group(1)
            value = m.group(2) or m.group(3) or m.group(4)
            if m.group(2) or m.group(3):
                patterns.append(f'has {prop_name} "{value}"')
            else:
                patterns.append(f'has {prop_name} {value}')
        return patterns

    def _parse_where(self, where_clause: str) -> List[str]:
        """Parse WHERE clause into TypeQL patterns."""
        patterns = []

        # Handle exists patterns (positive)
        exists_matches = list(re.finditer(r'(?<!NOT\s)exists\s*\{\s*\((\w+)\)\s*-\s*\[:(\w+)\]\s*->\s*\(:(\w+)\)\s*\}', where_clause, re.IGNORECASE))
        for m in exists_matches:
            var, rel_type, target_type = m.groups()
            rel_info = self.relation_map.get(rel_type)
            if rel_info:
                tql_rel, role1, role2 = rel_info
                patterns.append(f"({role1}: ${var}) isa {tql_rel}")

        # Handle NOT exists patterns
        not_exists_matches = list(re.finditer(r'NOT\s+exists\s*\{\s*\((\w+)\)\s*-\s*\[:(\w+)\]\s*->\s*\(:(\w+)\)\s*\}', where_clause, re.IGNORECASE))
        for m in not_exists_matches:
            var, rel_type, target_type = m.groups()
            rel_info = self.relation_map.get(rel_type)
            if rel_info:
                tql_rel, role1, role2 = rel_info
                patterns.append(f"not {{ ({role1}: ${var}) isa {tql_rel} }}")

        # Handle NOT (var)-[:REL]->() pattern
        not_rel_match = re.search(r'NOT\s*\((\w+)\)\s*-\s*\[:(\w+)\]\s*->\s*\(\)', where_clause, re.IGNORECASE)
        if not_rel_match:
            var, rel_type = not_rel_match.groups()
            rel_info = self.relation_map.get(rel_type)
            if rel_info:
                tql_rel, role1, role2 = rel_info
                patterns.append(f"not {{ ({role1}: ${var}) isa {tql_rel} }}")

        # Handle IS NOT NULL
        is_not_null = re.findall(r'(\w+)\.(\w+)\s+IS\s+NOT\s+NULL', where_clause, re.IGNORECASE)
        for var, prop in is_not_null:
            attr_var = f"{prop}{var}"
            if var not in self.rel_vars and attr_var not in self.declared_attrs:
                patterns.append(f"${var} has {prop} ${attr_var}")
                self.declared_attrs.add(attr_var)

        # Handle IS NULL
        is_null = re.findall(r'(\w+)\.(\w+)\s+IS\s+NULL', where_clause, re.IGNORECASE)
        for var, prop in is_null:
            patterns.append(f"not {{ ${var} has {prop} $null{prop}{var} }}")

        # Parse comparison conditions
        conditions = re.split(r'\s+AND\s+', where_clause, flags=re.IGNORECASE)

        for cond in conditions:
            cond = cond.strip()

            if 'IS NOT NULL' in cond.upper() or 'IS NULL' in cond.upper():
                continue
            if 'exists' in cond.lower():
                continue

            # Handle modulo: var.prop % n = m
            mod_match = re.search(r'(\w+)\.(\w+)\s*%\s*(\d+)\s*=\s*(\d+)', cond)
            if mod_match:
                var, prop, divisor, result = mod_match.groups()
                attr_var = f"{prop}{var}"
                # Skip if it's a relation attribute - already declared
                if var not in self.rel_vars and attr_var not in self.declared_attrs:
                    patterns.append(f"${var} has {prop} ${attr_var}")
                    self.declared_attrs.add(attr_var)
                patterns.append(f"${attr_var} mod {divisor} = {result}")
                continue

            # Handle property comparisons: var.prop op value
            comp_match = re.search(r'(\w+)\.(\w+)\s*(=|<>|!=|>=|<=|>|<)\s*(?:\'([^\']*)\'|"([^"]*)"|(-?\d+\.?\d*))', cond)
            if comp_match:
                var, prop, op, str_val1, str_val2, num_val = comp_match.groups()
                value = str_val1 or str_val2 or num_val

                attr_var = f"{prop}{var}"

                # Check if this is a relation attribute
                is_rel_attr = var in self.rel_vars

                if op == '=':
                    if is_rel_attr:
                        # Relation attr already declared, just add constraint
                        if str_val1 or str_val2:
                            patterns.append(f'${attr_var} = "{value}"')
                        else:
                            patterns.append(f'${attr_var} = {value}')
                    else:
                        if str_val1 or str_val2:
                            patterns.append(f'${var} has {prop} "{value}"')
                        else:
                            patterns.append(f'${var} has {prop} {value}')
                elif op in ('<>', '!='):
                    if not is_rel_attr and attr_var not in self.declared_attrs:
                        patterns.append(f'${var} has {prop} ${attr_var}')
                        self.declared_attrs.add(attr_var)
                    if str_val1 or str_val2:
                        patterns.append(f'${attr_var} != "{value}"')
                    else:
                        patterns.append(f'${attr_var} != {value}')
                else:
                    if not is_rel_attr and attr_var not in self.declared_attrs:
                        patterns.append(f'${var} has {prop} ${attr_var}')
                        self.declared_attrs.add(attr_var)
                    if str_val1 or str_val2:
                        patterns.append(f'${attr_var} {op} "{value}"')
                    else:
                        patterns.append(f'${attr_var} {op} {value}')
                continue

            # Handle CONTAINS
            contains_match = re.search(r'(\w+)\.(\w+)\s+CONTAINS\s+[\'"]([^\'"]+)[\'"]', cond, re.IGNORECASE)
            if contains_match:
                var, prop, value = contains_match.groups()
                attr_var = f"{prop}{var}"
                if var not in self.rel_vars and attr_var not in self.declared_attrs:
                    patterns.append(f'${var} has {prop} ${attr_var}')
                    self.declared_attrs.add(attr_var)
                patterns.append(f'${attr_var} contains "{value}"')
                continue

            # Handle STARTS WITH
            starts_match = re.search(r'(\w+)\.(\w+)\s+STARTS\s+WITH\s+[\'"]([^\'"]+)[\'"]', cond, re.IGNORECASE)
            if starts_match:
                var, prop, value = starts_match.groups()
                attr_var = f"{prop}{var}"
                if var not in self.rel_vars and attr_var not in self.declared_attrs:
                    patterns.append(f'${var} has {prop} ${attr_var}')
                    self.declared_attrs.add(attr_var)
                patterns.append(f'${attr_var} like "{value}.*"')
                continue

            # Handle ENDS WITH
            ends_match = re.search(r'(\w+)\.(\w+)\s+ENDS\s+WITH\s+[\'"]([^\'"]+)[\'"]', cond, re.IGNORECASE)
            if ends_match:
                var, prop, value = ends_match.groups()
                attr_var = f"{prop}{var}"
                if var not in self.rel_vars and attr_var not in self.declared_attrs:
                    patterns.append(f'${var} has {prop} ${attr_var}')
                    self.declared_attrs.add(attr_var)
                patterns.append(f'${attr_var} like ".*{value}"')
                continue

            # Handle IN clause
            in_match = re.search(r'(\w+)\.(\w+)\s+IN\s+\[([^\]]+)\]', cond, re.IGNORECASE)
            if in_match:
                var, prop, values_str = in_match.groups()
                values = re.findall(r"'([^']*)'|\"([^\"]*)\"|(\d+)", values_str)
                value_list = [v[0] or v[1] or v[2] for v in values]
                attr_var = f"{prop}{var}"
                if var not in self.rel_vars and attr_var not in self.declared_attrs:
                    patterns.append(f'${var} has {prop} ${attr_var}')
                    self.declared_attrs.add(attr_var)
                or_conditions = []
                for v in value_list:
                    if v.isdigit():
                        or_conditions.append(f'{{ ${attr_var} = {v}; }}')
                    else:
                        or_conditions.append(f'{{ ${attr_var} = "{v}"; }}')
                if or_conditions:
                    patterns.append(' or '.join(or_conditions))
                continue

        return patterns

    def _parse_return(self, return_clause: str, existing_patterns: List[str]) -> Tuple[List[dict], List[str]]:
        """Parse RETURN clause into fetch items and extra patterns."""
        items = []
        extra_patterns = []

        return_clause = re.sub(r'^DISTINCT\s+', '', return_clause, flags=re.IGNORECASE)

        parts = self._split_return_parts(return_clause)

        for part in parts:
            part = part.strip()

            alias_match = re.search(r'(.+?)\s+AS\s+(\w+)', part, re.IGNORECASE)
            if alias_match:
                expr, alias = alias_match.groups()
                expr = expr.strip()

                if 'count(' in expr.lower():
                    continue

                prop_match = re.match(r'(\w+)\.(\w+)', expr)
                if prop_match:
                    var, prop = prop_match.groups()
                    attr_var = f"{prop}{var}"

                    # Check if it's a relation attribute (already declared in relation pattern)
                    if var in self.rel_vars:
                        items.append({'key': alias, 'value': f'${attr_var}'})
                    else:
                        if attr_var not in self.declared_attrs:
                            extra_patterns.append(f"${var} has {prop} ${attr_var}")
                            self.declared_attrs.add(attr_var)
                        items.append({'key': alias, 'value': f'${attr_var}'})
                else:
                    items.append({'key': alias, 'value': f'${expr}'})
            else:
                prop_match = re.match(r'(\w+)\.(\w+)', part)
                if prop_match:
                    var, prop = prop_match.groups()
                    attr_var = f"{prop}{var}"

                    if var in self.rel_vars:
                        items.append({'key': prop, 'value': f'${attr_var}'})
                    else:
                        if attr_var not in self.declared_attrs:
                            extra_patterns.append(f"${var} has {prop} ${attr_var}")
                            self.declared_attrs.add(attr_var)
                        items.append({'key': prop, 'value': f'${attr_var}'})
                elif re.match(r'^\w+$', part):
                    var = part
                    var_type = self.var_types.get(var)
                    if var_type and var_type in self.entity_attrs:
                        for attr in self.entity_attrs[var_type]:
                            attr_var = f"{attr}{var}"
                            if attr_var not in self.declared_attrs:
                                extra_patterns.append(f"${var} has {attr} ${attr_var}")
                                self.declared_attrs.add(attr_var)
                            items.append({'key': attr, 'value': f'${attr_var}'})
                    else:
                        items.append({'key': var, 'value': f'${var}'})

        return items, extra_patterns

    def _split_return_parts(self, return_clause: str) -> List[str]:
        parts = []
        current = ""
        depth = 0

        for char in return_clause:
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
            elif char == ',' and depth == 0:
                parts.append(current)
                current = ""
                continue
            current += char

        if current:
            parts.append(current)

        return parts

    def _parse_order(self, order_clause: str, existing_patterns: List[str]) -> Tuple[Optional[str], str, List[str]]:
        """Parse ORDER BY clause."""
        extra_patterns = []

        match = re.search(r'(\w+)\.(\w+)\s*(DESC|ASC)?', order_clause, re.IGNORECASE)
        if match:
            var, prop, direction = match.groups()
            sort_dir = 'desc' if direction and direction.upper() == 'DESC' else 'asc'
            attr_var = f"{prop}{var}"

            # Don't add if it's a relation attr (already declared)
            if var not in self.rel_vars and attr_var not in self.declared_attrs:
                extra_patterns.append(f"${var} has {prop} ${attr_var}")
                self.declared_attrs.add(attr_var)

            return attr_var, sort_dir, extra_patterns

        alias_match = re.search(r'(\w+)\s*(DESC|ASC)?', order_clause, re.IGNORECASE)
        if alias_match:
            alias, direction = alias_match.groups()
            sort_dir = 'desc' if direction and direction.upper() == 'DESC' else 'asc'
            return alias, sort_dir, extra_patterns

        return None, 'asc', extra_patterns

    def _convert_count_query(self, cypher: str, match_patterns: List[str], return_clause: str, order_clause: str, limit_clause: str) -> str:
        """Convert COUNT aggregation query."""
        count_match = re.search(r'count\((?:DISTINCT\s+)?(\w+|\*)?\)', return_clause, re.IGNORECASE)
        if not count_match:
            return self._simple_count(match_patterns)

        count_var = count_match.group(1) if count_match.group(1) else None
        is_distinct = 'DISTINCT' in return_clause.upper()

        group_match = re.search(r'(\w+)\.(\w+)\s*(?:AS\s+(\w+))?\s*,\s*count', return_clause, re.IGNORECASE)

        if group_match:
            group_var, group_prop, group_alias = group_match.groups()
            group_alias = group_alias or group_prop

            alias_match = re.search(r'count\([^)]+\)\s+AS\s+(\w+)', return_clause, re.IGNORECASE)
            count_alias = alias_match.group(1) if alias_match else 'count'

            typeql = "match\n"

            seen = set()
            for pattern in match_patterns:
                if pattern not in seen:
                    seen.add(pattern)
                    typeql += f"  {pattern};\n"

            group_attr_var = f"{group_prop}{group_var}"
            if group_attr_var not in self.declared_attrs:
                typeql += f"  ${group_var} has {group_prop} ${group_attr_var};\n"

            if count_var and count_var != '*':
                typeql += f"reduce ${count_alias} = count(${count_var}) groupby ${group_attr_var};\n"
            else:
                first_var = None
                for p in match_patterns:
                    var_match = re.search(r'\$(\w+)\s+isa', p)
                    if var_match:
                        first_var = var_match.group(1)
                        break
                if first_var:
                    typeql += f"reduce ${count_alias} = count(${first_var}) groupby ${group_attr_var};\n"
                else:
                    typeql += f"reduce ${count_alias} = count groupby ${group_attr_var};\n"

            if order_clause:
                sort_match = re.search(r'(\w+)\s*(DESC|ASC)?', order_clause, re.IGNORECASE)
                if sort_match:
                    sort_var, sort_dir = sort_match.groups()
                    sort_dir = 'desc' if sort_dir and sort_dir.upper() == 'DESC' else 'asc'
                    typeql += f"sort ${sort_var} {sort_dir};\n"

            if limit_clause:
                limit_match = re.search(r'LIMIT\s+(\d+)', limit_clause, re.IGNORECASE)
                if limit_match:
                    typeql += f"limit {limit_match.group(1)};\n"

            typeql += f'fetch {{\n  "{group_alias}": ${group_attr_var},\n  "{count_alias}": ${count_alias}\n}};'
            return typeql

        return self._simple_count(match_patterns, count_var)

    def _simple_count(self, match_patterns: List[str], count_var: str = None) -> str:
        """Generate simple count query."""
        typeql = "match\n"

        seen = set()
        for pattern in match_patterns:
            if pattern not in seen:
                seen.add(pattern)
                typeql += f"  {pattern};\n"

        if count_var and count_var != '*':
            typeql += f"reduce $count = count(${count_var});"
        else:
            first_var = None
            for p in match_patterns:
                var_match = re.search(r'\$(\w+)\s+isa', p)
                if var_match:
                    first_var = var_match.group(1)
                    break
            if first_var:
                typeql += f"reduce $count = count(${first_var});"
            else:
                typeql += "reduce $count = count;"

        return typeql

    def _convert_with_count_order(self, cypher: str, match_clause: str, with_clause: str, return_clause: str, order_clause: str, limit_clause: str) -> str:
        """Convert WITH count(...) ORDER BY pattern (without WHERE)."""
        with_match = re.search(r'(\w+),\s*count\((\w+|\*)\)\s+AS\s+(\w+)', with_clause, re.IGNORECASE)
        if not with_match:
            with_match = re.search(r'count\((\w+|\*)\)\s+AS\s+(\w+)', with_clause, re.IGNORECASE)
            if with_match:
                count_var, count_alias = with_match.groups()
                group_var = None
            else:
                raise ValueError(f"Cannot parse WITH clause: {with_clause}")
        else:
            group_var, count_var, count_alias = with_match.groups()

        return_items = []
        if return_clause:
            parts = self._split_return_parts(return_clause)
            for part in parts:
                part = part.strip()
                alias_match = re.search(r'(.+?)\s+AS\s+(\w+)', part, re.IGNORECASE)
                if alias_match:
                    expr, alias = alias_match.groups()
                    prop_match = re.match(r'(\w+)\.(\w+)', expr.strip())
                    if prop_match:
                        return_items.append((prop_match.group(1), prop_match.group(2), alias))
                    elif expr.strip() == count_alias:
                        return_items.append((None, count_alias, alias))
                else:
                    prop_match = re.match(r'(\w+)\.(\w+)', part)
                    if prop_match:
                        return_items.append((prop_match.group(1), prop_match.group(2), prop_match.group(2)))
                    elif part == count_alias:
                        return_items.append((None, count_alias, count_alias))

        patterns = self._parse_match_patterns(match_clause)

        typeql = "match\n"
        for pattern in patterns['patterns']:
            typeql += f"  {pattern};\n"

        group_attr = None
        for var, prop, alias in return_items:
            if var == group_var:
                group_attr = prop
                break

        if group_attr and group_var:
            group_attr_var = f"{group_attr}{group_var}"
            typeql += f"  ${group_var} has {group_attr} ${group_attr_var};\n"

        if count_var and count_var != '*':
            if group_attr:
                typeql += f"reduce ${count_alias} = count(${count_var}) groupby ${group_attr_var};\n"
            else:
                typeql += f"reduce ${count_alias} = count(${count_var});\n"
        else:
            if group_attr:
                first_var = None
                for p in patterns['patterns']:
                    var_match = re.search(r'\$(\w+)\s+isa', p)
                    if var_match and var_match.group(1) != group_var:
                        first_var = var_match.group(1)
                        break
                if first_var:
                    typeql += f"reduce ${count_alias} = count(${first_var}) groupby ${group_attr_var};\n"
                else:
                    typeql += f"reduce ${count_alias} = count groupby ${group_attr_var};\n"

        if order_clause:
            sort_match = re.search(r'(\w+)\s*(DESC|ASC)?', order_clause, re.IGNORECASE)
            if sort_match:
                sort_var, sort_dir = sort_match.groups()
                sort_dir = 'desc' if sort_dir and sort_dir.upper() == 'DESC' else 'asc'
                typeql += f"sort ${sort_var} {sort_dir};\n"

        if limit_clause:
            limit_match = re.search(r'LIMIT\s+(\d+)', limit_clause, re.IGNORECASE)
            if limit_match:
                typeql += f"limit {limit_match.group(1)};\n"

        fetch_parts = []
        for var, prop, alias in return_items:
            if var is None:
                fetch_parts.append(f'  "{alias}": ${prop}')
            else:
                fetch_parts.append(f'  "{alias}": ${prop}{var}')

        if fetch_parts:
            typeql += "fetch {\n" + ",\n".join(fetch_parts) + "\n};"
        else:
            typeql += "fetch { };"

        return typeql

    def _convert_with_count_where(self, cypher: str, match_clause: str, with_clause: str, where_clause: str, return_clause: str, order_clause: str, limit_clause: str) -> str:
        """Convert WITH count(...) WHERE pattern (HAVING equivalent)."""
        with_match = re.search(r'(\w+),\s*count\((\w+|\*)\)\s+AS\s+(\w+)', with_clause, re.IGNORECASE)
        if not with_match:
            with_match = re.search(r'count\((\w+|\*)\)\s+AS\s+(\w+)', with_clause, re.IGNORECASE)
            if with_match:
                count_var, count_alias = with_match.groups()
                group_var = None
            else:
                raise ValueError(f"Cannot parse WITH clause: {with_clause}")
        else:
            group_var, count_var, count_alias = with_match.groups()

        where_match = re.search(rf'{count_alias}\s*(>|>=|<|<=|=|<>|!=)\s*(\d+)', where_clause, re.IGNORECASE)
        if not where_match:
            raise ValueError(f"Cannot parse WHERE clause for HAVING: {where_clause}")

        op, threshold = where_match.groups()

        return_items = []
        if return_clause:
            parts = self._split_return_parts(return_clause)
            for part in parts:
                part = part.strip()
                alias_match = re.search(r'(.+?)\s+AS\s+(\w+)', part, re.IGNORECASE)
                if alias_match:
                    expr, alias = alias_match.groups()
                    prop_match = re.match(r'(\w+)\.(\w+)', expr.strip())
                    if prop_match:
                        return_items.append((prop_match.group(1), prop_match.group(2), alias))
                    elif expr.strip() == count_alias:
                        return_items.append((None, count_alias, alias))
                else:
                    prop_match = re.match(r'(\w+)\.(\w+)', part)
                    if prop_match:
                        return_items.append((prop_match.group(1), prop_match.group(2), prop_match.group(2)))
                    elif part == count_alias:
                        return_items.append((None, count_alias, count_alias))

        patterns = self._parse_match_patterns(match_clause)

        typeql = "match\n"
        for pattern in patterns['patterns']:
            typeql += f"  {pattern};\n"

        group_attr = None
        for var, prop, alias in return_items:
            if var == group_var:
                group_attr = prop
                break

        if group_attr and group_var:
            group_attr_var = f"{group_attr}{group_var}"
            typeql += f"  ${group_var} has {group_attr} ${group_attr_var};\n"

        if count_var and count_var != '*':
            if group_attr:
                typeql += f"reduce ${count_alias} = count(${count_var}) groupby ${group_attr_var};\n"
            else:
                typeql += f"reduce ${count_alias} = count(${count_var});\n"
        else:
            if group_attr:
                first_var = None
                for p in patterns['patterns']:
                    var_match = re.search(r'\$(\w+)\s+isa', p)
                    if var_match and var_match.group(1) != group_var:
                        first_var = var_match.group(1)
                        break
                if first_var:
                    typeql += f"reduce ${count_alias} = count(${first_var}) groupby ${group_attr_var};\n"
                else:
                    typeql += f"reduce ${count_alias} = count groupby ${group_attr_var};\n"

        typeql += f"match ${count_alias} {op} {threshold};\n"

        if order_clause:
            sort_match = re.search(r'(\w+)\s*(DESC|ASC)?', order_clause, re.IGNORECASE)
            if sort_match:
                sort_var, sort_dir = sort_match.groups()
                sort_dir = 'desc' if sort_dir and sort_dir.upper() == 'DESC' else 'asc'
                typeql += f"sort ${sort_var} {sort_dir};\n"

        if limit_clause:
            limit_match = re.search(r'LIMIT\s+(\d+)', limit_clause, re.IGNORECASE)
            if limit_match:
                typeql += f"limit {limit_match.group(1)};\n"

        fetch_parts = []
        for var, prop, alias in return_items:
            if var is None:
                fetch_parts.append(f'  "{alias}": ${prop}')
            else:
                fetch_parts.append(f'  "{alias}": ${prop}{var}')

        if fetch_parts:
            typeql += "fetch {\n" + ",\n".join(fetch_parts) + "\n};"
        else:
            typeql += "fetch { };"

        return typeql

    def _convert_aggregate_query(self, cypher: str, match_patterns: List[str], return_clause: str, order_clause: str, limit_clause: str) -> str:
        """Convert SUM/AVG/MIN/MAX aggregation queries."""
        agg_match = re.search(r'(sum|avg|min|max)\s*\(\s*(\w+)\.(\w+)\s*\)', return_clause, re.IGNORECASE)
        if not agg_match:
            raise ValueError(f"Cannot parse aggregation in: {return_clause}")

        agg_func = agg_match.group(1).lower()
        agg_var = agg_match.group(2)
        agg_prop = agg_match.group(3)

        alias_match = re.search(rf'{agg_func}\s*\([^)]+\)\s+AS\s+(\w+)', return_clause, re.IGNORECASE)
        agg_alias = alias_match.group(1) if alias_match else agg_func

        typeql = "match\n"

        seen = set()
        for pattern in match_patterns:
            if pattern not in seen:
                seen.add(pattern)
                typeql += f"  {pattern};\n"

        agg_attr_var = f"{agg_prop}{agg_var}"
        # Check if it's a relation attr that's already been declared
        if agg_attr_var not in self.declared_attrs:
            typeql += f"  ${agg_var} has {agg_prop} ${agg_attr_var};\n"

        typeql += f"reduce ${agg_alias} = {agg_func}(${agg_attr_var});"

        return typeql

    def _convert_collect_query(self, cypher: str, match_patterns: List[str], return_clause: str, order_clause: str, limit_clause: str) -> str:
        """Convert COLLECT aggregation query."""
        typeql = "match\n"

        seen = set()
        for pattern in match_patterns:
            if pattern not in seen:
                seen.add(pattern)
                typeql += f"  {pattern};\n"

        collect_match = re.search(r'collect\(([^)]+)\)', return_clause, re.IGNORECASE)
        group_match = re.search(r'(\w+)\.(\w+)\s*(?:AS\s+(\w+))?\s*,\s*collect', return_clause, re.IGNORECASE)

        if collect_match and group_match:
            collect_expr = collect_match.group(1).strip()
            group_var, group_prop, group_alias = group_match.groups()
            group_alias = group_alias or group_prop

            collect_prop_match = re.match(r'(\w+)\.(\w+)', collect_expr)
            if collect_prop_match:
                coll_var, coll_prop = collect_prop_match.groups()

                group_attr_var = f"{group_prop}{group_var}"
                coll_attr_var = f"{coll_prop}{coll_var}"

                if group_attr_var not in self.declared_attrs:
                    typeql += f"  ${group_var} has {group_prop} ${group_attr_var};\n"
                if coll_attr_var not in self.declared_attrs:
                    typeql += f"  ${coll_var} has {coll_prop} ${coll_attr_var};\n"

                collect_alias_match = re.search(r'collect\([^)]+\)\s+AS\s+(\w+)', return_clause, re.IGNORECASE)
                collect_alias = collect_alias_match.group(1) if collect_alias_match else 'collected'

                if order_clause:
                    sort_match = re.search(r'(\w+)\.(\w+)\s*(DESC|ASC)?', order_clause, re.IGNORECASE)
                    if sort_match:
                        s_var, s_prop, s_dir = sort_match.groups()
                        s_dir = 'desc' if s_dir and s_dir.upper() == 'DESC' else 'asc'
                        typeql += f"sort ${s_prop}{s_var} {s_dir};\n"

                if limit_clause:
                    limit_match = re.search(r'LIMIT\s+(\d+)', limit_clause, re.IGNORECASE)
                    if limit_match:
                        typeql += f"limit {limit_match.group(1)};\n"

                typeql += f'fetch {{\n  "{group_alias}": ${group_attr_var},\n  "{collect_alias}": ${coll_attr_var}\n}};'
                return typeql

        typeql += "fetch { };"
        return typeql


def get_all_queries(database: str) -> List[dict]:
    """Get all valid queries for a database."""
    queries = []
    with open(CSV_PATH, 'r') as f:
        reader = csv.DictReader(f)
        idx = 0
        for row in reader:
            if row['database'] != database:
                continue
            if row.get('syntax_error', '').lower() == 'true':
                continue
            if row.get('false_schema', '').lower() == 'true':
                continue

            queries.append({
                'index': idx,
                'question': row['question'],
                'cypher': row['cypher']
            })
            idx += 1

    return queries


def validate_query(driver, database: str, typeql: str) -> Tuple[bool, str]:
    """Validate a TypeQL query against the database."""
    try:
        with driver.transaction(database, TransactionType.READ) as tx:
            result = tx.query(typeql).resolve()
            if hasattr(result, 'as_concept_documents'):
                list(result.as_concept_documents())
            elif hasattr(result, 'as_concept_rows'):
                list(result.as_concept_rows())
        return True, ""
    except Exception as e:
        return False, str(e)


def main():
    """Main conversion process."""
    credentials = Credentials('admin', 'password')
    options = DriverOptions(is_tls_enabled=False)
    driver = TypeDB.driver('localhost:1729', credentials, options)

    converter = CypherToTypeQLConverter()

    queries = get_all_queries('movies')
    print(f"Found {len(queries)} queries to convert")

    successful = []
    failed = []

    for i, query in enumerate(queries):
        idx = query['index']
        question = query['question']
        cypher = query['cypher']

        try:
            typeql = converter.convert(cypher)

            valid, error = validate_query(driver, DATABASE, typeql)

            if valid:
                successful.append({
                    'original_index': idx,
                    'question': question,
                    'cypher': cypher,
                    'typeql': typeql
                })
            else:
                failed.append({
                    'original_index': idx,
                    'question': question,
                    'cypher': cypher,
                    'error': error
                })
        except Exception as e:
            failed.append({
                'original_index': idx,
                'question': question,
                'cypher': cypher,
                'error': str(e)
            })

        if (i + 1) % 100 == 0:
            print(f"Progress: {i + 1}/{len(queries)} - {len(successful)} successful, {len(failed)} failed")

    with open(OUTPUT_PATH, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
        writer.writeheader()
        writer.writerows(successful)

    with open(FAILED_PATH, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'error'])
        writer.writeheader()
        writer.writerows(failed)

    print(f"\nConversion complete!")
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")
    print(f"Output written to {OUTPUT_PATH}")
    print(f"Failed queries written to {FAILED_PATH}")

    driver.close()


if __name__ == '__main__':
    main()
