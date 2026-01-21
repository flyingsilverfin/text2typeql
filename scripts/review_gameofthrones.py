#!/usr/bin/env python3
"""
Comprehensive semantic review of Game of Thrones queries.
Checks for issues like:
- Wrong sort direction for "lowest" queries (should be asc, not desc)
- Missing target character in INTERACTS queries
- Missing community/louvain IN filters
- Missing aggregations (sum, count)
- Missing sort when ORDER BY present
- Missing limit when LIMIT present
"""

import csv
import re
from dataclasses import dataclass, field
from typing import Optional, List
from pathlib import Path


@dataclass
class SemanticIssue:
    """Represents a semantic issue found in a query."""
    issue_type: str
    description: str
    severity: str = "error"  # error, warning


@dataclass
class QueryReview:
    """Result of reviewing a single query."""
    original_index: int
    question: str
    cypher: str
    typeql: str
    issues: list = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return any(i.severity == "error" for i in self.issues)

    @property
    def is_valid(self) -> bool:
        return len(self.issues) == 0


def check_sort_direction(question: str, cypher: str, typeql: str) -> Optional[SemanticIssue]:
    """
    Check if sort direction matches between Cypher and TypeQL.

    Compares the ORDER BY direction in Cypher with the sort direction in TypeQL.
    """
    cypher_upper = cypher.upper()
    typeql_lower = typeql.lower()

    # Check actual sort direction in TypeQL
    has_sort_desc = 'sort ' in typeql_lower and ' desc' in typeql_lower
    has_sort_asc = 'sort ' in typeql_lower and ' asc' in typeql_lower
    has_sort = 'sort ' in typeql_lower

    # Get Cypher sort direction
    has_order_by = 'ORDER BY' in cypher_upper
    if not has_order_by:
        return None  # No ordering in Cypher, nothing to check

    cypher_has_desc = 'ORDER BY' in cypher_upper and 'DESC' in cypher_upper
    # Cypher defaults to ASC if no direction specified
    cypher_has_asc = 'ORDER BY' in cypher_upper and ('ASC' in cypher_upper or 'DESC' not in cypher_upper)

    # Check if TypeQL has sort when Cypher has ORDER BY
    if has_order_by and not has_sort:
        return SemanticIssue(
            issue_type="missing_sort",
            description="Cypher has ORDER BY but TypeQL lacks sort clause",
            severity="error"
        )

    # Check for direction mismatches
    if cypher_has_desc and has_sort_asc:
        return SemanticIssue(
            issue_type="wrong_sort_direction",
            description="Cypher uses DESC sort but TypeQL uses ASC",
            severity="error"
        )

    if cypher_has_asc and has_sort_desc:
        return SemanticIssue(
            issue_type="wrong_sort_direction",
            description="Cypher uses ASC sort but TypeQL uses DESC",
            severity="error"
        )

    return None


def check_missing_target_character(question: str, cypher: str, typeql: str) -> Optional[SemanticIssue]:
    """Check if a specific character mentioned in Cypher filter is missing from the TypeQL query."""
    typeql_lower = typeql.lower()

    # Check for quoted character names in Cypher that should appear in TypeQL
    # Match both single and double quotes
    cypher_char_pattern = r'["\']([A-Z][a-zA-Z]+(?:-[A-Z][a-zA-Z]+)?)["\']'
    cypher_chars = re.findall(cypher_char_pattern, cypher)

    # Common non-character strings to skip
    skip_words = {'character', 'name', 'weight', 'book', 'pagerank', 'centrality',
                  'degree', 'louvain', 'community', 'interacts', 'string', 'integer',
                  'double', 'boolean', 'datetime', 'asc', 'desc', 'limit', 'order',
                  'match', 'return', 'where', 'and', 'or', 'not', 'in', 'true', 'false'}

    for char in cypher_chars:
        char_lower = char.lower()
        if char_lower in skip_words:
            continue

        # Check if this character name appears in the TypeQL (case-insensitive)
        if char_lower not in typeql_lower and f'"{char}"' not in typeql and f"'{char}'" not in typeql:
            # Check various case variations
            found = False
            for variant in [char, char.lower(), char.upper(), char.title()]:
                if f'"{variant}"' in typeql or f"'{variant}'" in typeql:
                    found = True
                    break
            if not found:
                return SemanticIssue(
                    issue_type="missing_target_character",
                    description=f"Cypher filters for character '{char}' but this is missing from TypeQL query",
                    severity="error"
                )

    return None


def check_missing_aggregation(question: str, cypher: str, typeql: str) -> Optional[SemanticIssue]:
    """Check if aggregations in Cypher are present in the TypeQL."""
    cypher_upper = cypher.upper()
    typeql_lower = typeql.lower()

    # Check for SUM in Cypher
    if 'SUM(' in cypher_upper:
        if 'reduce' not in typeql_lower or 'sum' not in typeql_lower:
            return SemanticIssue(
                issue_type="missing_aggregation",
                description="Cypher has SUM() but TypeQL lacks 'reduce ... sum' aggregation",
                severity="error"
            )

    # Check for COUNT in Cypher
    if 'COUNT(' in cypher_upper:
        if 'reduce' not in typeql_lower or 'count' not in typeql_lower:
            return SemanticIssue(
                issue_type="missing_aggregation",
                description="Cypher has COUNT() but TypeQL lacks 'reduce ... count' aggregation",
                severity="error"
            )

    # Check for AVG in Cypher
    if 'AVG(' in cypher_upper:
        if 'reduce' not in typeql_lower:
            return SemanticIssue(
                issue_type="missing_aggregation",
                description="Cypher has AVG() but TypeQL lacks aggregation",
                severity="error"
            )

    # Check for MIN in Cypher
    if 'MIN(' in cypher_upper:
        if 'reduce' not in typeql_lower or 'min' not in typeql_lower:
            return SemanticIssue(
                issue_type="missing_aggregation",
                description="Cypher has MIN() but TypeQL lacks 'reduce ... min' aggregation",
                severity="error"
            )

    # Check for MAX in Cypher
    if 'MAX(' in cypher_upper:
        if 'reduce' not in typeql_lower or 'max' not in typeql_lower:
            return SemanticIssue(
                issue_type="missing_aggregation",
                description="Cypher has MAX() but TypeQL lacks 'reduce ... max' aggregation",
                severity="error"
            )

    return None


def check_community_louvain_filter(question: str, cypher: str, typeql: str) -> Optional[SemanticIssue]:
    """Check if community/louvain IN filters are correctly translated."""
    # Check for IN clauses in Cypher with multiple values
    in_pattern = r'(\w+)\s+IN\s*\[([^\]]+)\]'
    matches = re.findall(in_pattern, cypher, re.IGNORECASE)

    for field, values in matches:
        field_lower = field.lower()
        values_list = [v.strip() for v in values.split(',')]
        num_values = len(values_list)

        # Count how many values appear in TypeQL
        values_in_typeql = sum(1 for v in values_list if v.strip() in typeql)

        if values_in_typeql < num_values:
            return SemanticIssue(
                issue_type="missing_in_filter",
                description=f"Cypher has IN clause with {num_values} values for {field}, but TypeQL only includes {values_in_typeql}",
                severity="error"
            )

    return None


def check_relation_type(question: str, cypher: str, typeql: str) -> Optional[SemanticIssue]:
    """Check if the correct relation type is used in TypeQL."""
    # Extract relation types from Cypher
    relation_pattern = r'\[:?(INTERACTS\d*|INTERACTS45)\]'
    cypher_relations = re.findall(relation_pattern, cypher, re.IGNORECASE)

    typeql_lower = typeql.lower()

    for rel in cypher_relations:
        rel_lower = rel.lower()
        if rel_lower not in typeql_lower:
            return SemanticIssue(
                issue_type="missing_relation",
                description=f"Cypher uses relation '{rel}' but it's missing from TypeQL",
                severity="error"
            )

    return None


def check_attribute_filter_values(question: str, cypher: str, typeql: str) -> Optional[SemanticIssue]:
    """Check if attribute filter values in Cypher appear in TypeQL."""
    # Extract comparisons from Cypher WHERE clauses
    # Pattern matches things like: c.community = 579 or c.pagerank > 0.2
    comp_pattern = r'\.(\w+)\s*([>=<]+|=)\s*([\d.]+)'
    cypher_comps = re.findall(comp_pattern, cypher)

    for attr, op, value in cypher_comps:
        # Check if the same value appears in TypeQL
        if value not in typeql:
            return SemanticIssue(
                issue_type="missing_filter_value",
                description=f"Cypher filters '{attr}' with value {value} but this value is missing from TypeQL",
                severity="error"
            )

    return None


def check_limit_consistency(question: str, cypher: str, typeql: str) -> Optional[SemanticIssue]:
    """Check if LIMIT clause is correctly translated."""
    cypher_upper = cypher.upper()
    typeql_lower = typeql.lower()

    # Extract LIMIT value from Cypher
    limit_match = re.search(r'LIMIT\s+(\d+)', cypher_upper)
    if limit_match:
        limit_value = limit_match.group(1)
        if 'limit' not in typeql_lower:
            return SemanticIssue(
                issue_type="missing_limit",
                description=f"Cypher has LIMIT {limit_value} but TypeQL lacks limit clause",
                severity="error"
            )
        elif f'limit {limit_value}' not in typeql_lower:
            return SemanticIssue(
                issue_type="wrong_limit_value",
                description=f"Cypher has LIMIT {limit_value} but TypeQL has different limit value",
                severity="warning"
            )

    return None


def check_negation(question: str, cypher: str, typeql: str) -> Optional[SemanticIssue]:
    """Check if negation in Cypher is present in TypeQL."""
    cypher_upper = cypher.upper()
    typeql_lower = typeql.lower()

    # Check for NOT EXISTS or NOT patterns
    if ' NOT ' in cypher_upper or 'NOT EXISTS' in cypher_upper:
        if 'not {' not in typeql_lower and 'not{' not in typeql_lower:
            return SemanticIssue(
                issue_type="missing_negation",
                description="Cypher has NOT clause but TypeQL lacks 'not' block",
                severity="error"
            )

    # Check for != or <>
    if '!=' in cypher or '<>' in cypher:
        # TypeQL uses != for not equal
        if '!=' not in typeql and 'not {' not in typeql_lower:
            return SemanticIssue(
                issue_type="missing_inequality",
                description="Cypher has inequality (!= or <>) but TypeQL may be missing it",
                severity="warning"
            )

    return None


def check_optional_match(question: str, cypher: str, typeql: str) -> Optional[SemanticIssue]:
    """Check if OPTIONAL MATCH is handled."""
    if 'OPTIONAL MATCH' in cypher.upper():
        return SemanticIssue(
            issue_type="optional_match_warning",
            description="Cypher uses OPTIONAL MATCH which may need special handling in TypeQL",
            severity="warning"
        )
    return None


def check_collect_handling(question: str, cypher: str, typeql: str) -> Optional[SemanticIssue]:
    """Check if COLLECT is properly translated."""
    if 'COLLECT(' in cypher.upper():
        return SemanticIssue(
            issue_type="collect_warning",
            description="Cypher uses COLLECT() which may need special handling in TypeQL",
            severity="warning"
        )
    return None


def check_weight_attribute(question: str, cypher: str, typeql: str) -> Optional[SemanticIssue]:
    """Check if weight attribute is properly accessed from the relation."""
    if '.weight' in cypher.lower() or 'i.weight' in cypher.lower():
        # If Cypher uses weight from a relation, TypeQL should have 'has weight' or similar
        if 'weight' not in typeql.lower():
            return SemanticIssue(
                issue_type="missing_weight_attribute",
                description="Cypher uses weight attribute but it's missing from TypeQL",
                severity="error"
            )
    return None


def review_query(row: dict) -> QueryReview:
    """Review a single query for semantic issues."""
    review = QueryReview(
        original_index=int(row['original_index']),
        question=row['question'],
        cypher=row['cypher'],
        typeql=row['typeql']
    )

    # Run all checks
    checks = [
        check_sort_direction,
        check_missing_target_character,
        check_missing_aggregation,
        check_community_louvain_filter,
        check_relation_type,
        check_attribute_filter_values,
        check_limit_consistency,
        check_negation,
        check_optional_match,
        check_collect_handling,
        check_weight_attribute,
    ]

    for check in checks:
        issue = check(review.question, review.cypher, review.typeql)
        if issue:
            review.issues.append(issue)

    return review


def main():
    input_path = Path('/opt/text2typeql/output/gameofthrones/queries.csv')
    output_failed_path = Path('/opt/text2typeql/output/gameofthrones/failed_review.csv')

    print(f"Reading queries from {input_path}...")

    # Read all queries
    with open(input_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"Total queries: {len(rows)}")

    # Review all queries
    reviews = []
    for row in rows:
        review = review_query(row)
        reviews.append(review)

    # Separate valid and failed
    valid_reviews = [r for r in reviews if not r.has_errors]
    failed_reviews = [r for r in reviews if r.has_errors]

    print(f"\nReview Results:")
    print(f"  Valid queries: {len(valid_reviews)}")
    print(f"  Failed queries: {len(failed_reviews)}")

    # Count issues by type
    issue_counts = {}
    all_issues_counts = {}
    for review in reviews:
        for issue in review.issues:
            key = issue.issue_type
            if key not in all_issues_counts:
                all_issues_counts[key] = 0
            all_issues_counts[key] += 1
            if issue.severity == "error":
                if key not in issue_counts:
                    issue_counts[key] = 0
                issue_counts[key] += 1

    if issue_counts:
        print(f"\nError breakdown:")
        for issue_type, count in sorted(issue_counts.items(), key=lambda x: -x[1]):
            print(f"  {issue_type}: {count}")

    # Write failed queries to failed_review.csv
    if failed_reviews:
        print(f"\nWriting {len(failed_reviews)} failed queries to {output_failed_path}...")
        with open(output_failed_path, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['original_index', 'question', 'cypher', 'typeql', 'issues']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for review in failed_reviews:
                issues_str = '; '.join([f"{i.issue_type}: {i.description}" for i in review.issues if i.severity == "error"])
                writer.writerow({
                    'original_index': review.original_index,
                    'question': review.question,
                    'cypher': review.cypher,
                    'typeql': review.typeql,
                    'issues': issues_str
                })
        print(f"Done!")

    # Update the original queries.csv with only valid queries
    print(f"\nUpdating {input_path} with {len(valid_reviews)} valid queries...")
    with open(input_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['original_index', 'question', 'cypher', 'typeql']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for review in valid_reviews:
            writer.writerow({
                'original_index': review.original_index,
                'question': review.question,
                'cypher': review.cypher,
                'typeql': review.typeql
            })
    print(f"Done!")

    # Show examples of failed queries
    if failed_reviews:
        print(f"\n=== Failed Queries ===")
        for review in failed_reviews:
            print(f"\n[Index {review.original_index}]")
            print(f"Question: {review.question}")
            print(f"Issues:")
            for issue in review.issues:
                if issue.severity == "error":
                    print(f"  - [{issue.issue_type}] {issue.description}")
    else:
        print("\n=== All queries passed semantic review! ===")


if __name__ == '__main__':
    main()
