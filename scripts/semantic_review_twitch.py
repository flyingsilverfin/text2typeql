#!/usr/bin/env python3
"""
Semantic Review Script for Twitch Database Queries
Reviews TypeQL conversions for semantic correctness against English questions.
"""

import csv
import re
from dataclasses import dataclass
from typing import List, Tuple, Optional
import os

@dataclass
class QueryIssue:
    original_index: int
    question: str
    cypher: str
    typeql: str
    review_reason: str

def load_queries(filepath: str) -> List[dict]:
    """Load queries from CSV file."""
    queries = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            queries.append(row)
    return queries

def check_sort_direction(question: str, typeql: str, cypher: str) -> Optional[str]:
    """Check if sort direction matches question semantics."""
    question_lower = question.lower()
    cypher_lower = cypher.lower()

    # Skip if no sorting in original Cypher
    if 'order by' not in cypher_lower:
        return None

    # Keywords indicating descending order (highest, most, top)
    desc_keywords = ['highest', 'most', 'top', 'largest', 'maximum', 'max', 'greatest', 'best', 'newest']
    # Keywords indicating ascending order (lowest, least, oldest, first, minimum)
    asc_keywords = ['lowest', 'least', 'fewest', 'minimum', 'min', 'smallest', 'oldest', 'earliest']

    has_desc_keyword = any(kw in question_lower for kw in desc_keywords)
    has_asc_keyword = any(kw in question_lower for kw in asc_keywords)

    has_sort_desc = 'sort' in typeql and 'desc' in typeql
    has_sort_asc = 'sort' in typeql and 'asc' in typeql and 'desc' not in typeql

    # Also check the Cypher for expected direction
    cypher_desc = 'desc' in cypher_lower
    cypher_asc = 'asc' in cypher_lower or ('order by' in cypher_lower and 'desc' not in cypher_lower)

    # Check for mismatch between Cypher and TypeQL sort directions
    if cypher_desc and has_sort_asc:
        return f"Sort direction mismatch: Cypher uses DESC but TypeQL uses ASC"
    if cypher_asc and has_sort_desc:
        # Exception: some queries like "oldest" in Cypher might not have explicit ASC
        if not has_desc_keyword:
            return f"Sort direction mismatch: Cypher uses ASC (or default) but TypeQL uses DESC"

    # Check semantic meaning
    if has_asc_keyword and has_sort_desc and not has_desc_keyword:
        return f"Sort direction issue: question asks for '{[kw for kw in asc_keywords if kw in question_lower]}' but TypeQL sorts descending"

    if has_desc_keyword and has_sort_asc:
        return f"Sort direction issue: question asks for '{[kw for kw in desc_keywords if kw in question_lower]}' but TypeQL sorts ascending"

    return None

def check_limit_presence(question: str, typeql: str, cypher: str) -> Optional[str]:
    """Check if LIMIT is present when question asks for 'top N' or 'first N'."""
    cypher_lower = cypher.lower()

    # Check if Cypher has a LIMIT
    cypher_limit_match = re.search(r'limit\s+(\d+)', cypher_lower)
    if not cypher_limit_match:
        return None  # No limit expected

    expected_limit = int(cypher_limit_match.group(1))

    # Check TypeQL
    typeql_limit_match = re.search(r'limit\s+(\d+)', typeql.lower())
    if not typeql_limit_match:
        return f"Missing LIMIT: Cypher has LIMIT {expected_limit} but TypeQL has no limit"

    actual_limit = int(typeql_limit_match.group(1))
    if actual_limit != expected_limit:
        return f"Limit mismatch: Cypher LIMIT {expected_limit} but TypeQL limit {actual_limit}"

    return None

def check_aggregation(question: str, typeql: str, cypher: str) -> Optional[str]:
    """Check if aggregation is present when question/Cypher has count/sum/avg."""
    cypher_lower = cypher.lower()

    # Count keywords in Cypher
    if 'count(' in cypher_lower:
        if 'reduce' not in typeql.lower() and 'count' not in typeql.lower():
            return "Missing COUNT aggregation: Cypher uses count() but TypeQL has no reduce/count"

    # Sum in Cypher
    if 'sum(' in cypher_lower:
        if 'sum' not in typeql.lower():
            return "Missing SUM aggregation: Cypher uses sum() but TypeQL has no sum"

    # Max in Cypher
    if 'max(' in cypher_lower:
        if 'max' not in typeql.lower():
            return "Missing MAX aggregation: Cypher uses max() but TypeQL has no max"

    # Min in Cypher
    if 'min(' in cypher_lower:
        if 'min' not in typeql.lower():
            return "Missing MIN aggregation: Cypher uses min() but TypeQL has no min"

    return None

def check_subquery_semantics(question: str, typeql: str, cypher: str) -> Optional[str]:
    """Check for complex subquery semantics issues."""
    question_lower = question.lower()
    cypher_lower = cypher.lower()

    # Pattern: "for the stream with the highest X" or "in the stream with the most Y"
    patterns = [
        r'for the stream with the (highest|most|lowest|least)',
        r'in the stream with the (highest|most|lowest|least)',
        r'of the stream with the (highest|most|lowest|least)',
        r'the stream with the (highest|most|lowest|least)',
    ]

    for pattern in patterns:
        if re.search(pattern, question_lower):
            # Check if Cypher uses WITH clause and LIMIT 1 to isolate the single stream
            if 'with s' in cypher_lower or 'with t' in cypher_lower:
                if 'limit 1' in cypher_lower:
                    typeql_limits = re.findall(r'limit\s+(\d+)', typeql.lower())
                    if typeql_limits:
                        cypher_limits = re.findall(r'limit\s+(\d+)', cypher_lower)
                        if len(cypher_limits) > 1:
                            if len(typeql_limits) < 2:
                                return "Subquery semantics: Cypher has multiple LIMIT clauses (subquery pattern) but TypeQL has single limit"
                        if 'moderator' in question_lower or 'vip' in question_lower or 'chatter' in question_lower:
                            return "Subquery semantics issue: should find single highest/most stream first, then get its related items"
    return None

def check_specific_issues(query: dict) -> Optional[str]:
    """Check for specific known issues in certain queries."""
    question = query.get('question', '')
    typeql = query.get('typeql', '')
    cypher = query.get('cypher', '')
    question_lower = question.lower()
    cypher_lower = cypher.lower()

    issues = []

    # String length checking
    if 'longer than' in question_lower and 'character' in question_lower:
        issues.append("TypeQL cannot easily check string length - may not correctly filter by description length")

    # "more than one" filtering after groupby
    if 'more than one' in question_lower or 'more than 1' in question_lower:
        if 'groupby' in typeql.lower():
            if '> 1' not in typeql and '>= 2' not in typeql:
                issues.append("Missing filter '> 1' after groupby for 'more than one' condition")

    # "more than N" where N > 1
    more_than_match = re.search(r'more than (\d+)', question_lower)
    if more_than_match:
        n = int(more_than_match.group(1))
        if n > 1:  # Only check for N > 1
            expected_condition = f'> {n}'
            if expected_condition not in typeql and f'>= {n+1}' not in typeql:
                if 'groupby' in typeql.lower() or 'where' in cypher_lower:
                    issues.append(f"May be missing filter condition '> {n}'")

    # "at least N" conditions
    at_least_match = re.search(r'at least (\d+)', question_lower)
    if at_least_match:
        n = int(at_least_match.group(1))
        if n > 1:  # Only check for N > 1
            expected_condition = f'>= {n}'
            if expected_condition not in typeql and f'> {n-1}' not in typeql:
                if 'groupby' in typeql.lower() or 'where' in cypher_lower:
                    issues.append(f"May be missing filter condition '>= {n}'")

    return "; ".join(issues) if issues else None

def review_query(query: dict) -> Optional[QueryIssue]:
    """Review a single query for semantic issues."""
    question = query.get('question', '')
    cypher = query.get('cypher', '')
    typeql = query.get('typeql', '')
    original_index = query.get('original_index', '')

    issues = []

    # Run all checks
    sort_issue = check_sort_direction(question, typeql, cypher)
    if sort_issue:
        issues.append(sort_issue)

    limit_issue = check_limit_presence(question, typeql, cypher)
    if limit_issue:
        issues.append(limit_issue)

    agg_issue = check_aggregation(question, typeql, cypher)
    if agg_issue:
        issues.append(agg_issue)

    subquery_issue = check_subquery_semantics(question, typeql, cypher)
    if subquery_issue:
        issues.append(subquery_issue)

    specific_issue = check_specific_issues(query)
    if specific_issue:
        issues.append(specific_issue)

    if issues:
        return QueryIssue(
            original_index=original_index,
            question=question,
            cypher=cypher,
            typeql=typeql,
            review_reason="; ".join(issues)
        )

    return None

def write_failed_queries(issues: List[QueryIssue], output_path: str):
    """Write failed queries to CSV file."""
    with open(output_path, 'w', encoding='utf-8', newline='') as f:
        fieldnames = ['original_index', 'question', 'cypher', 'typeql', 'review_reason']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for issue in issues:
            writer.writerow({
                'original_index': issue.original_index,
                'question': issue.question,
                'cypher': issue.cypher,
                'typeql': issue.typeql,
                'review_reason': issue.review_reason
            })

def write_passing_queries(queries: List[dict], failed_indices: set, output_path: str):
    """Write passing queries to CSV file."""
    with open(output_path, 'w', encoding='utf-8', newline='') as f:
        fieldnames = ['original_index', 'question', 'cypher', 'typeql']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for query in queries:
            if query.get('original_index') not in failed_indices:
                writer.writerow({
                    'original_index': query.get('original_index', ''),
                    'question': query.get('question', ''),
                    'cypher': query.get('cypher', ''),
                    'typeql': query.get('typeql', '')
                })

def main():
    input_path = '/opt/text2typeql/output/twitch/queries.csv'
    failed_path = '/opt/text2typeql/output/twitch/failed_review.csv'

    print("="*80)
    print("TWITCH DATABASE SEMANTIC REVIEW")
    print("="*80)

    print("\nLoading queries...")
    queries = load_queries(input_path)
    print(f"Loaded {len(queries)} queries")

    print("\nReviewing queries for semantic issues...")
    issues = []
    failed_indices = set()

    for query in queries:
        issue = review_query(query)
        if issue:
            issues.append(issue)
            failed_indices.add(issue.original_index)

    print(f"\nFound {len(issues)} queries with semantic issues")

    if issues:
        print(f"\nWriting failed queries to {failed_path}")
        write_failed_queries(issues, failed_path)

    # Summary
    print("\n" + "="*80)
    print("SEMANTIC REVIEW SUMMARY")
    print("="*80)
    print(f"Total queries reviewed: {len(queries)}")
    print(f"Queries with issues:    {len(issues)}")
    print(f"Queries passing:        {len(queries) - len(issues)}")
    print(f"Pass rate:              {(len(queries) - len(issues)) / len(queries) * 100:.1f}%")

    # Categorize issues
    issue_categories = {}
    for issue in issues:
        reasons = issue.review_reason.split("; ")
        for reason in reasons:
            category = reason.split(":")[0] if ":" in reason else reason[:50]
            issue_categories[category] = issue_categories.get(category, 0) + 1

    print("\n" + "-"*80)
    print("ISSUE BREAKDOWN BY CATEGORY")
    print("-"*80)
    for category, count in sorted(issue_categories.items(), key=lambda x: -x[1]):
        print(f"  [{count:3d}] {category}")

    # Group issues by type for detailed report
    print("\n" + "="*80)
    print("DETAILED ISSUE LIST")
    print("="*80)

    # Group by issue type
    issue_groups = {}
    for issue in issues:
        primary_reason = issue.review_reason.split(";")[0].strip()
        category = primary_reason.split(":")[0] if ":" in primary_reason else primary_reason[:50]
        if category not in issue_groups:
            issue_groups[category] = []
        issue_groups[category].append(issue)

    for category, group_issues in sorted(issue_groups.items(), key=lambda x: -len(x[1])):
        print(f"\n### {category} ({len(group_issues)} queries) ###")
        print("-"*60)
        for issue in group_issues[:5]:  # Show first 5 examples
            print(f"  Query {issue.original_index}: {issue.question[:70]}...")
        if len(group_issues) > 5:
            print(f"  ... and {len(group_issues) - 5} more")

    # Print recommendations
    print("\n" + "="*80)
    print("RECOMMENDATIONS")
    print("="*80)

    recommendations = {
        "Subquery semantics": """
    These queries require a two-step process in TypeQL:
    1. First find THE single stream with highest/most/lowest property
    2. Then query related items (moderators, VIPs, chatters) for that stream
    TypeQL doesn't support subqueries in the same way as Cypher's WITH clause.
    Consider using a TypeQL rule or breaking into two queries.""",

        "Missing filter '> 1' after groupby": """
    These queries need a post-aggregation filter. In TypeQL 3.x, you may need to:
    1. Use reduce with groupby
    2. Then filter the grouped results
    Current TypeQL may not support filtering after reduce in a single query.""",

        "Sort direction": """
    These queries have confusing semantics like 'top oldest' or 'top lowest'.
    Review each case to determine if the sort direction in TypeQL matches
    the intended semantics of the English question.""",

        "Missing aggregation": """
    These queries need COUNT/SUM/MAX/MIN aggregations that are present in Cypher
    but missing in the TypeQL translation. Add reduce clauses as needed.""",

        "Limit mismatch": """
    The LIMIT values between Cypher and TypeQL don't match. This often happens
    with subquery patterns where Cypher uses multiple LIMITs."""
    }

    for category, recommendation in recommendations.items():
        if any(category.lower() in cat.lower() for cat in issue_categories.keys()):
            print(f"\n[{category}]")
            print(recommendation)

    print("\n" + "="*80)
    print(f"Review complete. Failed queries written to: {failed_path}")
    print("="*80)

if __name__ == "__main__":
    main()
