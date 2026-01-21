#!/usr/bin/env python3
"""
Semantic Review Script for Companies Database Queries

Reviews all queries in the companies database to verify the TypeQL
semantically answers the English question correctly.

Issues checked:
1. Revenue thresholds (should be millions/billions not single digits)
2. Missing location chain (org -> city -> country)
3. Missing relationship constraints (subsidiary_of, supplies, ceo_of, etc.)
4. Wrong supplier/customer direction
5. Missing sentiment aggregation
6. Missing city/country name filters
7. Missing date filters
8. Missing industry category connections
"""

import csv
import re
import sys
from pathlib import Path
from typing import Tuple, List, Optional

# Threshold mappings - how values should be converted
REVENUE_THRESHOLDS = {
    # Scientific notation from Cypher -> expected TypeQL value
    "1E8": 100000000,      # 100 million
    "1e8": 100000000,
    "5E7": 50000000,       # 50 million
    "5e7": 50000000,
    "1E9": 1000000000,     # 1 billion
    "1e9": 1000000000,
    "1E7": 10000000,       # 10 million
    "1e7": 10000000,
    "1E6": 1000000,        # 1 million
    "1e6": 1000000,
    "100 million": 100000000,
    "50 million": 50000000,
    "1 billion": 1000000000,
    "10 million": 10000000,
    "$100 million": 100000000,
    "$50 million": 50000000,
    "$1 billion": 1000000000,
    "$10 million": 10000000,
}


class SemanticIssue:
    """Represents a semantic issue found in a query."""
    def __init__(self, issue_type: str, description: str, severity: str = "error"):
        self.issue_type = issue_type
        self.description = description
        self.severity = severity  # error, warning

    def __str__(self):
        return f"[{self.severity.upper()}] {self.issue_type}: {self.description}"


def check_revenue_threshold(question: str, cypher: str, typeql: str) -> List[SemanticIssue]:
    """Check if revenue thresholds are correctly converted."""
    issues = []

    # Look for revenue mentions in question
    revenue_patterns = [
        (r'\$(\d+)\s*million', lambda m: int(m.group(1)) * 1000000),
        (r'\$(\d+)\s*billion', lambda m: int(m.group(1)) * 1000000000),
        (r'(\d+)\s*million', lambda m: int(m.group(1)) * 1000000),
        (r'(\d+)\s*billion', lambda m: int(m.group(1)) * 1000000000),
        (r'revenue.*?greater than.*?(\d+[eE]\d+)', lambda m: float(m.group(1))),
        (r'revenue.*?more than.*?(\d+[eE]\d+)', lambda m: float(m.group(1))),
        (r'revenue.*?over.*?(\d+[eE]\d+)', lambda m: float(m.group(1))),
    ]

    # Check Cypher for scientific notation
    cypher_revenue = re.search(r'revenue\s*[><=]+\s*(\d+[eE]\d+)', cypher, re.IGNORECASE)

    if cypher_revenue:
        expected_value = float(cypher_revenue.group(1))

        # Check TypeQL for the revenue comparison
        typeql_revenue = re.search(r'\$\w*revenue\s*[><=]+\s*([\d.]+)', typeql)

        if typeql_revenue:
            actual_value = float(typeql_revenue.group(1))

            # If the TypeQL value is way too small (single digit vs millions)
            if expected_value > 1000 and actual_value < 100:
                issues.append(SemanticIssue(
                    "REVENUE_THRESHOLD",
                    f"Revenue threshold wrong: expected ~{expected_value:,.0f}, got {actual_value}"
                ))

    # Also check question for dollar amounts
    for pattern, extractor in revenue_patterns[:4]:  # First 4 are from question
        match = re.search(pattern, question, re.IGNORECASE)
        if match:
            expected_value = extractor(match)
            typeql_revenue = re.search(r'\$\w*revenue\s*[><=]+\s*([\d.]+)', typeql)
            if typeql_revenue:
                actual_value = float(typeql_revenue.group(1))
                if expected_value > 1000 and actual_value < 100:
                    issues.append(SemanticIssue(
                        "REVENUE_THRESHOLD",
                        f"Revenue threshold from question wrong: expected ~{expected_value:,.0f}, got {actual_value}"
                    ))
            break

    return issues


def check_location_chain(question: str, cypher: str, typeql: str) -> List[SemanticIssue]:
    """Check if location chains (org -> city -> country) are complete."""
    issues = []

    # Check if question mentions country
    country_mentioned = any(keyword in question.lower() for keyword in [
        'country', 'united states', 'america', 'germany', 'france', 'uk', 'japan',
        'china', 'canada', 'australia', 'india', 'brazil', 'italy', 'spain'
    ])

    # Check if Cypher has the full chain
    cypher_has_city_country = 'IN_CITY' in cypher and 'IN_COUNTRY' in cypher

    if cypher_has_city_country:
        # TypeQL should have both located_in and in_country relations
        has_located_in = 'located_in' in typeql
        has_in_country = 'in_country' in typeql

        if not has_located_in or not has_in_country:
            issues.append(SemanticIssue(
                "MISSING_LOCATION_CHAIN",
                f"Missing location chain: located_in={has_located_in}, in_country={has_in_country}"
            ))

    # Check if country name filter is missing when mentioned in Cypher
    country_filter_cypher = re.search(r"Country\s*\{?\s*name:\s*['\"]([^'\"]+)['\"]", cypher)
    if country_filter_cypher:
        country_name = country_filter_cypher.group(1)
        if country_name.lower() not in typeql.lower() and 'country_name' not in typeql:
            issues.append(SemanticIssue(
                "MISSING_COUNTRY_FILTER",
                f"Missing country filter for '{country_name}'"
            ))

    return issues


def check_city_filter(question: str, cypher: str, typeql: str) -> List[SemanticIssue]:
    """Check if city name filters are correctly applied."""
    issues = []

    # Look for city name in Cypher
    city_filter_cypher = re.search(r"City\s*\{?\s*name:\s*['\"]([^'\"]+)['\"]", cypher)

    if city_filter_cypher:
        city_name = city_filter_cypher.group(1)

        # Check if TypeQL has the city filter
        has_city_relation = 'located_in' in typeql
        has_city_filter = city_name.lower() in typeql.lower() or f'city_name "{city_name}"' in typeql

        if not has_city_relation:
            issues.append(SemanticIssue(
                "MISSING_CITY_RELATION",
                f"Missing located_in relation for city filter"
            ))

        if not has_city_filter:
            issues.append(SemanticIssue(
                "MISSING_CITY_FILTER",
                f"Missing city name filter for '{city_name}'"
            ))

    return issues


def check_subsidiary_relation(question: str, cypher: str, typeql: str) -> List[SemanticIssue]:
    """Check if subsidiary relationships are correctly represented."""
    issues = []

    # Check if question/cypher mentions subsidiaries
    mentions_subsidiary = 'subsidiar' in question.lower() or 'HAS_SUBSIDIARY' in cypher

    if mentions_subsidiary:
        has_subsidiary_relation = 'subsidiary_of' in typeql
        if not has_subsidiary_relation:
            issues.append(SemanticIssue(
                "MISSING_SUBSIDIARY_RELATION",
                "Question mentions subsidiaries but TypeQL lacks subsidiary_of relation"
            ))

    return issues


def check_supplier_direction(question: str, cypher: str, typeql: str) -> List[SemanticIssue]:
    """Check if supplier/customer direction is correct."""
    issues = []

    # Check if question/cypher mentions suppliers
    if 'supplier' in question.lower() or 'HAS_SUPPLIER' in cypher:
        has_supplies_relation = 'supplies' in typeql
        if not has_supplies_relation:
            issues.append(SemanticIssue(
                "MISSING_SUPPLIES_RELATION",
                "Question mentions suppliers but TypeQL lacks supplies relation"
            ))
        else:
            # Check direction - HAS_SUPPLIER means the org has a supplier (is a customer)
            # (o)-[:HAS_SUPPLIER]->(supplier) means o is customer, supplier is supplier
            if 'HAS_SUPPLIER' in cypher:
                # The organization doing the MATCH is the customer
                # In TypeQL: (customer: $o, supplier: $s) isa supplies
                if 'customer:' not in typeql and 'supplier:' not in typeql:
                    issues.append(SemanticIssue(
                        "SUPPLIER_DIRECTION_UNCLEAR",
                        "Supplier relation exists but roles not clearly specified",
                        severity="warning"
                    ))

    return issues


def check_investor_relation(question: str, cypher: str, typeql: str) -> List[SemanticIssue]:
    """Check if investor relationships are correctly represented."""
    issues = []

    if 'investor' in question.lower() or 'HAS_INVESTOR' in cypher:
        has_invested_relation = 'invested_in' in typeql
        if not has_invested_relation:
            issues.append(SemanticIssue(
                "MISSING_INVESTOR_RELATION",
                "Question mentions investors but TypeQL lacks invested_in relation"
            ))

    return issues


def check_board_member_relation(question: str, cypher: str, typeql: str) -> List[SemanticIssue]:
    """Check if board member relationships are correctly represented."""
    issues = []

    if 'board member' in question.lower() or 'HAS_BOARD_MEMBER' in cypher:
        has_board_relation = 'board_member_of' in typeql
        if not has_board_relation:
            issues.append(SemanticIssue(
                "MISSING_BOARD_MEMBER_RELATION",
                "Question mentions board members but TypeQL lacks board_member_of relation"
            ))

    return issues


def check_ceo_relation(question: str, cypher: str, typeql: str) -> List[SemanticIssue]:
    """Check if CEO relationships are correctly represented."""
    issues = []

    if 'ceo' in question.lower() or 'HAS_CEO' in cypher:
        has_ceo_relation = 'ceo_of' in typeql
        if not has_ceo_relation:
            issues.append(SemanticIssue(
                "MISSING_CEO_RELATION",
                "Question mentions CEO but TypeQL lacks ceo_of relation"
            ))

    return issues


def check_competitor_relation(question: str, cypher: str, typeql: str) -> List[SemanticIssue]:
    """Check if competitor relationships are correctly represented."""
    issues = []

    if 'competitor' in question.lower() or 'HAS_COMPETITOR' in cypher:
        has_competitor_relation = 'competes_with' in typeql
        if not has_competitor_relation:
            issues.append(SemanticIssue(
                "MISSING_COMPETITOR_RELATION",
                "Question mentions competitors but TypeQL lacks competes_with relation"
            ))

    return issues


def check_industry_category_relation(question: str, cypher: str, typeql: str) -> List[SemanticIssue]:
    """Check if industry category relationships are correctly represented."""
    issues = []

    if 'industry' in question.lower() or 'HAS_CATEGORY' in cypher or 'IndustryCategory' in cypher:
        has_category_relation = 'in_category' in typeql
        if not has_category_relation:
            issues.append(SemanticIssue(
                "MISSING_CATEGORY_RELATION",
                "Question mentions industry but TypeQL lacks in_category relation"
            ))

    return issues


def check_date_filter(question: str, cypher: str, typeql: str) -> List[SemanticIssue]:
    """Check if date filters are correctly applied."""
    issues = []

    # Check for year mentions
    year_match = re.search(r'\b(20\d{2}|19\d{2})\b', question)
    cypher_date = re.search(r'date\s*[><=]+', cypher, re.IGNORECASE) or 'date(' in cypher

    if cypher_date and year_match:
        year = year_match.group(1)
        # Check if TypeQL has date filter
        has_date_var = '$a_date' in typeql or '$article_date' in typeql or 'has date' in typeql
        has_date_filter = year in typeql or 'date' in typeql.lower()

        if not has_date_filter:
            issues.append(SemanticIssue(
                "MISSING_DATE_FILTER",
                f"Question mentions year {year} but TypeQL lacks date filter"
            ))

    return issues


def check_mentions_relation(question: str, cypher: str, typeql: str) -> List[SemanticIssue]:
    """Check if article mentions relationships are correctly represented."""
    issues = []

    if 'MENTIONS' in cypher:
        has_mentions_relation = 'mentions' in typeql
        if not has_mentions_relation:
            issues.append(SemanticIssue(
                "MISSING_MENTIONS_RELATION",
                "Cypher has MENTIONS relation but TypeQL lacks mentions relation"
            ))

    return issues


def check_sentiment_filter(question: str, cypher: str, typeql: str) -> List[SemanticIssue]:
    """Check if sentiment filters are correctly applied."""
    issues = []

    # Check for sentiment mentions
    if 'sentiment' in question.lower() or 'sentiment' in cypher.lower():
        # Check for specific sentiment thresholds
        cypher_sentiment = re.search(r'sentiment\s*[><=]+\s*([\d.-]+)', cypher, re.IGNORECASE)

        if cypher_sentiment:
            expected_value = float(cypher_sentiment.group(1))
            typeql_sentiment = re.search(r'\$\w*sentiment\s*[><=]+\s*([\d.-]+)', typeql)

            if typeql_sentiment:
                actual_value = float(typeql_sentiment.group(1))
                if abs(expected_value - actual_value) > 0.001:
                    issues.append(SemanticIssue(
                        "SENTIMENT_THRESHOLD",
                        f"Sentiment threshold mismatch: expected {expected_value}, got {actual_value}"
                    ))
            elif 'sentiment' not in typeql:
                issues.append(SemanticIssue(
                    "MISSING_SENTIMENT_FILTER",
                    "Cypher has sentiment filter but TypeQL lacks it"
                ))

    return issues


def check_person_name_filter(question: str, cypher: str, typeql: str) -> List[SemanticIssue]:
    """Check if person name filters are correctly applied."""
    issues = []

    # Look for person name in Cypher
    person_filter = re.search(r"Person\s*\{?\s*name:\s*['\"]([^'\"]+)['\"]", cypher)

    if person_filter:
        person_name = person_filter.group(1)
        if person_name not in typeql:
            issues.append(SemanticIssue(
                "MISSING_PERSON_FILTER",
                f"Missing person name filter for '{person_name}'"
            ))

    return issues


def check_organization_name_filter(question: str, cypher: str, typeql: str) -> List[SemanticIssue]:
    """Check if organization name filters are correctly applied."""
    issues = []

    # Look for org name in Cypher
    org_filter = re.search(r"Organization\s*\{?\s*name:\s*['\"]([^'\"]+)['\"]", cypher)

    if org_filter:
        org_name = org_filter.group(1)
        if org_name not in typeql:
            issues.append(SemanticIssue(
                "MISSING_ORG_FILTER",
                f"Missing organization name filter for '{org_name}'"
            ))

    return issues


def check_category_name_filter(question: str, cypher: str, typeql: str) -> List[SemanticIssue]:
    """Check if industry category name filters are correctly applied."""
    issues = []

    # Look for category name in Cypher
    cat_filter = re.search(r"IndustryCategory\s*\{?\s*name:\s*['\"]([^'\"]+)['\"]", cypher)

    if cat_filter:
        cat_name = cat_filter.group(1)
        has_category_relation = 'in_category' in typeql
        has_category_filter = cat_name.lower() in typeql.lower()

        if not has_category_relation:
            issues.append(SemanticIssue(
                "MISSING_CATEGORY_RELATION",
                f"Missing in_category relation for category filter"
            ))

        if not has_category_filter:
            issues.append(SemanticIssue(
                "MISSING_CATEGORY_FILTER",
                f"Missing category name filter for '{cat_name}'"
            ))

    return issues


def check_is_dissolved_filter(question: str, cypher: str, typeql: str) -> List[SemanticIssue]:
    """Check if is_dissolved filters are correctly applied."""
    issues = []

    # Check for NOT dissolved in Cypher
    not_dissolved = 'NOT o.isDissolved' in cypher or 'NOT isDissolved' in cypher

    if not_dissolved:
        # TypeQL should have is_dissolved false or NOT pattern
        has_not_dissolved = 'is_dissolved false' in typeql or 'not {' in typeql.lower()
        if not has_not_dissolved:
            issues.append(SemanticIssue(
                "MISSING_NOT_DISSOLVED_FILTER",
                "Cypher filters for NOT dissolved but TypeQL lacks equivalent filter"
            ))

    return issues


def check_parent_child_relation(question: str, cypher: str, typeql: str) -> List[SemanticIssue]:
    """Check if parent/child relationships (for person) are correctly represented."""
    issues = []

    if 'child' in question.lower() or 'parent' in question.lower() or 'HAS_CHILD' in cypher:
        has_parent_of_relation = 'parent_of' in typeql
        if not has_parent_of_relation:
            issues.append(SemanticIssue(
                "MISSING_PARENT_RELATION",
                "Question mentions parent/child but TypeQL lacks parent_of relation"
            ))

    return issues


def review_query(original_index: int, question: str, cypher: str, typeql: str) -> Tuple[bool, List[SemanticIssue]]:
    """
    Review a single query for semantic correctness.
    Returns (is_valid, list_of_issues)
    """
    all_issues = []

    # Run all checks
    all_issues.extend(check_revenue_threshold(question, cypher, typeql))
    all_issues.extend(check_location_chain(question, cypher, typeql))
    all_issues.extend(check_city_filter(question, cypher, typeql))
    all_issues.extend(check_subsidiary_relation(question, cypher, typeql))
    all_issues.extend(check_supplier_direction(question, cypher, typeql))
    all_issues.extend(check_investor_relation(question, cypher, typeql))
    all_issues.extend(check_board_member_relation(question, cypher, typeql))
    all_issues.extend(check_ceo_relation(question, cypher, typeql))
    all_issues.extend(check_competitor_relation(question, cypher, typeql))
    all_issues.extend(check_industry_category_relation(question, cypher, typeql))
    all_issues.extend(check_date_filter(question, cypher, typeql))
    all_issues.extend(check_mentions_relation(question, cypher, typeql))
    all_issues.extend(check_sentiment_filter(question, cypher, typeql))
    all_issues.extend(check_person_name_filter(question, cypher, typeql))
    all_issues.extend(check_organization_name_filter(question, cypher, typeql))
    all_issues.extend(check_category_name_filter(question, cypher, typeql))
    all_issues.extend(check_is_dissolved_filter(question, cypher, typeql))
    all_issues.extend(check_parent_child_relation(question, cypher, typeql))

    # Filter for errors only (not warnings)
    errors = [i for i in all_issues if i.severity == "error"]

    return len(errors) == 0, all_issues


def main():
    input_file = Path("/opt/text2typeql/output/companies/queries.csv")
    passed_file = Path("/opt/text2typeql/output/companies/queries_reviewed.csv")
    failed_file = Path("/opt/text2typeql/output/companies/failed_review.csv")

    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        sys.exit(1)

    # Read all queries
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        queries = list(reader)

    print(f"Loaded {len(queries)} queries from {input_file}")

    passed_queries = []
    failed_queries = []
    issue_counts = {}

    for query in queries:
        original_index = query.get('original_index', 'N/A')
        question = query.get('question', '')
        cypher = query.get('cypher', '')
        typeql = query.get('typeql', '')

        is_valid, issues = review_query(original_index, question, cypher, typeql)

        # Count issues
        for issue in issues:
            if issue.issue_type not in issue_counts:
                issue_counts[issue.issue_type] = 0
            issue_counts[issue.issue_type] += 1

        if is_valid:
            passed_queries.append(query)
        else:
            # Add issue information to the query
            error_issues = [i for i in issues if i.severity == "error"]
            query['semantic_issues'] = '; '.join([str(i) for i in error_issues])
            failed_queries.append(query)

    # Write passed queries
    if passed_queries:
        fieldnames = ['original_index', 'question', 'cypher', 'typeql']
        with open(passed_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(passed_queries)
        print(f"Wrote {len(passed_queries)} passed queries to {passed_file}")

    # Write failed queries
    if failed_queries:
        fieldnames = ['original_index', 'question', 'cypher', 'typeql', 'semantic_issues']
        with open(failed_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(failed_queries)
        print(f"Wrote {len(failed_queries)} failed queries to {failed_file}")

    # Print summary
    print("\n" + "="*60)
    print("SEMANTIC REVIEW SUMMARY")
    print("="*60)
    print(f"Total queries reviewed: {len(queries)}")
    print(f"Passed: {len(passed_queries)} ({100*len(passed_queries)/len(queries):.1f}%)")
    print(f"Failed: {len(failed_queries)} ({100*len(failed_queries)/len(queries):.1f}%)")
    print("\nIssue breakdown:")
    for issue_type, count in sorted(issue_counts.items(), key=lambda x: -x[1]):
        print(f"  {issue_type}: {count}")

    # Print some examples of failed queries
    print("\n" + "="*60)
    print("SAMPLE FAILED QUERIES")
    print("="*60)
    for i, query in enumerate(failed_queries[:10]):
        print(f"\n--- Query #{query.get('original_index', 'N/A')} ---")
        print(f"Question: {query['question'][:100]}...")
        print(f"Issues: {query.get('semantic_issues', 'N/A')}")


if __name__ == "__main__":
    main()
