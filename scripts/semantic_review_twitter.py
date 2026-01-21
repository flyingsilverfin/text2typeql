#!/usr/bin/env python3
"""
Semantic Review Script for Twitter TypeQL Queries

This script analyzes each query in the twitter queries.csv file to verify
that the TypeQL query semantically answers the English question correctly.

Common issues checked:
1. Missing filters or conditions from the question
2. Wrong sort direction (ascending vs descending for "lowest/highest")
3. Missing aggregations (count, sum, avg)
4. Missing relationship constraints
5. Wrong attributes being returned
6. Missing limit when question asks for "top N"
7. Wrong limit value
8. Missing date/time constraints
9. Wrong entity type (user vs me)
"""

import csv
import re
import sys
from dataclasses import dataclass, field
from typing import Optional, List, Tuple


@dataclass
class SemanticIssue:
    """Represents a semantic issue found in a query"""
    issue_type: str
    description: str
    severity: str = "error"  # "error" or "warning"


@dataclass
class QueryReview:
    """Result of reviewing a single query"""
    original_index: int
    question: str
    cypher: str
    typeql: str
    issues: List[SemanticIssue] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len([i for i in self.issues if i.severity == "error"]) == 0

    @property
    def review_reason(self) -> str:
        if not self.issues:
            return ""
        return "; ".join([f"{i.issue_type}: {i.description}" for i in self.issues])


def extract_limit_from_question(question: str) -> Optional[int]:
    """Extract expected limit from question text.

    Only extracts limits when question explicitly asks for "top N" or "first N".
    Words like "highest" alone don't imply a limit.
    """
    question_lower = question.lower()

    # Handle word numbers
    word_to_num = {'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
                   'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10}

    # Only explicit limit patterns - "top N" or "first N"
    # Not "highest" or "most" alone as those don't imply limit
    patterns = [
        (r'\btop\s+(\d+)\b', True),
        (r'\bfirst\s+(\d+)\b', True),
        (r'\btop\s+(one|two|three|four|five|six|seven|eight|nine|ten)\b', True),
        (r'\bfirst\s+(one|two|three|four|five|six|seven|eight|nine|ten)\b', True),
        (r'\bwhich\s+(one|two|three|four|five|six|seven|eight|nine|ten)\b', True),
    ]

    for pattern, _ in patterns:
        match = re.search(pattern, question_lower)
        if match:
            captured = match.group(1)
            if captured in word_to_num:
                return word_to_num[captured]
            try:
                return int(captured)
            except ValueError:
                pass

    return None


def extract_limit_from_typeql(typeql: str) -> Optional[int]:
    """Extract limit from TypeQL query"""
    match = re.search(r'limit\s+(\d+)', typeql.lower())
    if match:
        return int(match.group(1))
    return None


def check_sort_direction(question: str, typeql: str) -> List[SemanticIssue]:
    """Check if sort direction matches the question intent"""
    issues = []
    question_lower = question.lower()

    # Recent means descending by date
    recent_keywords = ['most recent', 'latest', 'newest']
    # Oldest means ascending by date
    oldest_keywords = ['oldest', 'earliest', 'first posted']

    has_sort = 'sort' in typeql.lower()

    # Check for recent/newest (should be desc)
    for kw in recent_keywords:
        if kw in question_lower:
            if has_sort and 'desc' not in typeql.lower():
                issues.append(SemanticIssue(
                    "wrong_sort_direction",
                    f"Question asks for '{kw}' but TypeQL doesn't sort descending"
                ))
            elif not has_sort and 'created_at' in typeql.lower():
                issues.append(SemanticIssue(
                    "missing_sort",
                    f"Question asks for '{kw}' but TypeQL has no sort clause"
                ))
            break

    # Check for oldest/earliest (should be asc)
    for kw in oldest_keywords:
        if kw in question_lower:
            if has_sort and 'asc' not in typeql.lower():
                issues.append(SemanticIssue(
                    "wrong_sort_direction",
                    f"Question asks for '{kw}' but TypeQL doesn't sort ascending"
                ))
            break

    # Skip sort direction check for phrases like "top 5 with lowest" which correctly sorts ascending
    # These are NOT errors - "top N with lowest X" means first N when sorted by X ascending
    has_lowest_phrase = any(kw in question_lower for kw in ['lowest', 'least', 'fewest', 'smallest'])
    has_highest_phrase = any(kw in question_lower for kw in ['highest', 'most', 'greatest', 'largest', 'maximum'])
    has_similar_phrase = 'similar' in question_lower  # "similar to" queries sort by closeness (asc)
    has_exactly_phrase = 'exactly' in question_lower  # "exactly N followers" doesn't imply sort direction

    # Only flag if there's a clear mismatch without modifier phrases
    if has_highest_phrase and not has_lowest_phrase and not has_similar_phrase and not has_exactly_phrase:
        if has_sort and 'asc' in typeql.lower() and 'desc' not in typeql.lower():
            # Exception: if question contains "top N ... lowest" pattern, ascending is correct
            if not re.search(r'top\s+\d+.*?lowest', question_lower):
                issues.append(SemanticIssue(
                    "wrong_sort_direction",
                    f"Question asks for highest/most but TypeQL sorts ascending instead of descending"
                ))

    # Only flag ascending issues if there's a clear "lowest" intent without "top N with lowest"
    if has_lowest_phrase and not has_highest_phrase:
        if has_sort and 'desc' in typeql.lower() and 'asc' not in typeql.lower():
            issues.append(SemanticIssue(
                "wrong_sort_direction",
                f"Question asks for lowest/least but TypeQL sorts descending instead of ascending"
            ))

    return issues


def check_aggregation(question: str, typeql: str) -> List[SemanticIssue]:
    """Check if required aggregations are present"""
    issues = []
    question_lower = question.lower()

    # Check for count requirements
    count_keywords = ['how many', 'count of', 'number of', 'total number']
    needs_count = any(kw in question_lower for kw in count_keywords)

    has_reduce = 'reduce' in typeql.lower()
    has_count = 'count(' in typeql.lower()

    if needs_count and not has_count:
        # Check if it's just returning a stored count attribute
        if not any(attr in typeql.lower() for attr in ['followers', 'following', 'statuses', 'favorites']):
            issues.append(SemanticIssue(
                "missing_aggregation",
                "Question asks 'how many' but TypeQL has no count aggregation"
            ))

    return issues


def check_limit_consistency(question: str, typeql: str) -> List[SemanticIssue]:
    """Check if limit matches the question"""
    issues = []

    expected_limit = extract_limit_from_question(question)
    actual_limit = extract_limit_from_typeql(typeql)

    if expected_limit is not None:
        if actual_limit is None:
            # Only flag if the question clearly asks for a limited set
            question_lower = question.lower()
            if any(kw in question_lower for kw in ['top ', 'first ', 'limit ']):
                issues.append(SemanticIssue(
                    "missing_limit",
                    f"Question asks for top/first {expected_limit} but TypeQL has no limit"
                ))
        elif actual_limit != expected_limit:
            issues.append(SemanticIssue(
                "wrong_limit",
                f"Question asks for {expected_limit} items but TypeQL limits to {actual_limit}"
            ))

    return issues


def check_filter_conditions(question: str, typeql: str) -> List[SemanticIssue]:
    """Check if filter conditions from question are present in TypeQL"""
    issues = []
    question_lower = question.lower()
    typeql_lower = typeql.lower()

    # Check for numeric comparisons
    comparison_patterns = [
        (r'more than\s+(\d+)', '>', 'greater than'),
        (r'greater than\s+(\d+)', '>', 'greater than'),
        (r'above\s+(\d+)', '>', 'above'),
        (r'over\s+(\d+)', '>', 'over'),
        (r'less than\s+(\d+)', '<', 'less than'),
        (r'below\s+(\d+)', '<', 'below'),
        (r'under\s+(\d+)', '<', 'under'),
        (r'at least\s+(\d+)', '>=', 'at least'),
        (r'at most\s+(\d+)', '<=', 'at most'),
        (r'between\s+(\d+)\s+and\s+(\d+)', 'between', 'between'),
    ]

    for pattern, operator, desc in comparison_patterns:
        match = re.search(pattern, question_lower)
        if match:
            value = match.group(1)
            if operator == 'between':
                value2 = match.group(2)
                if value not in typeql and value2 not in typeql:
                    issues.append(SemanticIssue(
                        "missing_filter",
                        f"Question mentions '{desc} {value} and {value2}' but values not in TypeQL",
                        severity="warning"
                    ))
            else:
                # Check if the comparison value appears in TypeQL
                if value not in typeql:
                    issues.append(SemanticIssue(
                        "missing_filter",
                        f"Question mentions '{desc} {value}' but value not in TypeQL",
                        severity="warning"
                    ))

    # Check for date constraints
    date_patterns = [
        (r'in\s+(\d{4})', 'year'),
        (r'before\s+(\d{4})', 'before year'),
        (r'after\s+(\d{4})', 'after year'),
        (r'january|february|march|april|may|june|july|august|september|october|november|december', 'month'),
    ]

    for pattern, desc in date_patterns:
        match = re.search(pattern, question_lower)
        if match:
            if 'created_at' not in typeql_lower and 'datetime' not in typeql_lower:
                # Check if date filtering is present
                if not any(d in typeql for d in ['2020', '2021', '2019', '2022']):
                    issues.append(SemanticIssue(
                        "missing_date_filter",
                        f"Question mentions {desc} but no date constraint in TypeQL",
                        severity="warning"
                    ))

    return issues


def check_relationship_constraints(question: str, cypher: str, typeql: str) -> List[SemanticIssue]:
    """Check if relationship constraints are properly translated"""
    issues = []

    # First check for malformed Cypher patterns that we should skip
    # Pattern like (t:Tweet)-[:MENTIONS]->(User)-[:POSTS]->(t) is cyclic/malformed
    # POSTS goes from User to Tweet, not User to User
    cypher_upper = cypher.upper()

    # Check for cyclic patterns where same variable appears twice with POSTS in between
    # These are Cypher bugs, not TypeQL errors
    if re.search(r'\((\w+):TWEET\).*\[:MENTIONS\].*\[:POSTS\].*\(\1\)', cypher_upper, re.IGNORECASE):
        return []  # Skip check - Cypher is malformed

    # Check for pattern where MENTIONS followed by POSTS going wrong direction
    if re.search(r'\[:MENTIONS\].*\(:USER.*\)-\[:POSTS\]->', cypher_upper, re.IGNORECASE):
        issues.append(SemanticIssue(
            "cypher_structural_issue",
            "Cypher has questionable pattern (User)-[:POSTS]-> after MENTIONS - TypeQL may use different semantics",
            severity="warning"
        ))
        return issues  # Don't flag as error, return as warning

    # Map Cypher relationships to TypeQL
    # Note: CONTAINS as a string operator (WHERE x.text CONTAINS 'word') is different from
    # [:CONTAINS] as a relationship between tweet and link entities
    rel_mapping = {
        'FOLLOWS': 'follows',
        'MENTIONS': 'mentions',
        'RETWEETS': 'retweets',
        'REPLY_TO': 'reply_to',
        'TAGS': 'tags',
        'USING': 'using',
        'INTERACTS_WITH': 'interacts_with',
        'SIMILAR_TO': 'similar_to',
        'RT_MENTIONS': 'rt_mentions',
    }

    typeql_lower = typeql.lower()

    for cypher_rel, typeql_rel in rel_mapping.items():
        # Check for relationship pattern [:RELATIONSHIP]
        rel_pattern = f'[:{cypher_rel}]'
        if rel_pattern in cypher_upper or f'[:{ cypher_rel}]' in cypher_upper:
            if typeql_rel not in typeql_lower:
                issues.append(SemanticIssue(
                    "missing_relationship",
                    f"Cypher has {cypher_rel} relationship but TypeQL missing {typeql_rel}"
                ))

    # Special handling for AMPLIFIES - it can be semantically equivalent to retweets in some contexts
    if '[:AMPLIFIES]' in cypher_upper:
        if 'amplifies' not in typeql_lower:
            if 'retweets' in typeql_lower:
                issues.append(SemanticIssue(
                    "semantic_substitution",
                    "Cypher uses AMPLIFIES but TypeQL uses retweets - may be semantically appropriate",
                    severity="warning"
                ))
            else:
                issues.append(SemanticIssue(
                    "missing_relationship",
                    f"Cypher has AMPLIFIES relationship but TypeQL missing amplifies or retweets"
                ))

    # Check for POSTS relationship - only flag if it's a clear structural pattern
    if '[:POSTS]' in cypher_upper:
        if 'posts' not in typeql_lower:
            # Only flag if the pattern makes sense (not part of malformed cyclic pattern)
            if not re.search(r'\[:MENTIONS\].*\[:POSTS\]', cypher_upper):
                issues.append(SemanticIssue(
                    "missing_relationship",
                    f"Cypher has POSTS relationship but TypeQL missing posts"
                ))

    # Special check for CONTAINS relationship (tweet contains link)
    # Only flag if the Cypher explicitly uses [:CONTAINS] pattern (relationship), not 'CONTAINS' operator
    if '[:CONTAINS]' in cypher_upper:
        if 'contains' not in typeql_lower:
            issues.append(SemanticIssue(
                "missing_relationship",
                f"Cypher has CONTAINS relationship but TypeQL missing contains"
            ))

    return issues


def check_return_attributes(question: str, cypher: str, typeql: str) -> List[SemanticIssue]:
    """Check if returned attributes match the question"""
    issues = []
    question_lower = question.lower()

    # Check for specific attribute requirements
    attr_keywords = {
        'screen name': ['screen_name'],
        'name': ['name'],
        'location': ['location'],
        'profile image': ['profile_image_url'],
        'url': ['url', 'link_url'],
        'followers': ['followers'],
        'following': ['following'],
        'betweenness': ['betweenness'],
        'statuses': ['statuses'],
        'text': ['text'],
        'favorites': ['favorites'],
    }

    for keyword, attrs in attr_keywords.items():
        if keyword in question_lower:
            # Check if any expected attribute is in the fetch clause
            fetch_match = re.search(r'fetch\s*\{([^}]+)\}', typeql, re.IGNORECASE | re.DOTALL)
            if fetch_match:
                fetch_content = fetch_match.group(1).lower()
                if not any(attr in fetch_content for attr in attrs):
                    # Allow if it's a count question
                    if 'count' not in question_lower and 'how many' not in question_lower:
                        issues.append(SemanticIssue(
                            "wrong_return_attribute",
                            f"Question asks for '{keyword}' but not in fetch clause",
                            severity="warning"
                        ))

    return issues


def check_entity_type(question: str, typeql: str) -> List[SemanticIssue]:
    """Check if correct entity types are used"""
    issues = []
    question_lower = question.lower()
    typeql_lower = typeql.lower()

    # 'neo4j' or 'Neo4j' usually refers to the 'me' entity
    if "'neo4j'" in question_lower or '"neo4j"' in question_lower or "user 'neo4j'" in question_lower:
        # Should use 'me' entity for neo4j user
        if '$me isa me' not in typeql_lower and 'isa me' not in typeql_lower:
            # Check if it's using 'user' instead
            if 'screen_name "neo4j"' in typeql_lower or "screen_name 'neo4j'" in typeql_lower:
                if '$u isa user' in typeql_lower or '$me isa user' in typeql_lower:
                    issues.append(SemanticIssue(
                        "wrong_entity_type",
                        "Question refers to 'neo4j' which should use 'me' entity, not 'user'",
                        severity="warning"
                    ))

    return issues


def check_negation(question: str, cypher: str, typeql: str) -> List[SemanticIssue]:
    """Check if negation is properly handled"""
    issues = []
    question_lower = question.lower()
    cypher_lower = cypher.lower()
    typeql_lower = typeql.lower()

    # Negation keywords
    neg_keywords = ['not ', 'never', 'no ', 'without', 'except', 'excluding', 'outside']

    has_negation_in_question = any(kw in question_lower for kw in neg_keywords)
    has_negation_in_cypher = 'not exists' in cypher_lower or 'not {' in cypher_lower or '<>' in cypher or '!=' in cypher
    has_negation_in_typeql = 'not {' in typeql_lower or 'not{' in typeql_lower

    if has_negation_in_cypher and not has_negation_in_typeql:
        # Check if it's a simple inequality that doesn't need 'not {'
        if '<>' in cypher or '!=' in cypher:
            pass  # These can be handled differently
        else:
            issues.append(SemanticIssue(
                "missing_negation",
                "Cypher has negation but TypeQL may be missing 'not' clause",
                severity="warning"
            ))

    return issues


def review_query(row: dict) -> QueryReview:
    """Perform semantic review on a single query"""
    original_index = int(row['original_index'])
    question = row['question']
    cypher = row['cypher']
    typeql = row['typeql']

    review = QueryReview(
        original_index=original_index,
        question=question,
        cypher=cypher,
        typeql=typeql
    )

    # Run all checks
    review.issues.extend(check_sort_direction(question, typeql))
    review.issues.extend(check_aggregation(question, typeql))
    review.issues.extend(check_limit_consistency(question, typeql))
    review.issues.extend(check_filter_conditions(question, typeql))
    review.issues.extend(check_relationship_constraints(question, cypher, typeql))
    review.issues.extend(check_return_attributes(question, cypher, typeql))
    review.issues.extend(check_entity_type(question, typeql))
    review.issues.extend(check_negation(question, cypher, typeql))

    return review


def check_question_keyword_alignment(question: str, cypher: str, typeql: str) -> List[SemanticIssue]:
    """Check if key terms in the question are reflected in the TypeQL"""
    issues = []
    question_lower = question.lower()
    typeql_lower = typeql.lower()

    # Check for "amplify/amplified" in question
    if 'amplif' in question_lower:
        if 'amplifies' not in typeql_lower:
            # Could be translated to retweets which is semantically similar
            if 'retweets' in typeql_lower:
                issues.append(SemanticIssue(
                    "semantic_interpretation",
                    "Question mentions 'amplified' but TypeQL uses 'retweets' - may be semantically equivalent",
                    severity="warning"
                ))
            else:
                issues.append(SemanticIssue(
                    "missing_semantic_term",
                    "Question mentions 'amplified' but TypeQL has no amplifies/retweets relationship",
                    severity="warning"
                ))

    # Check for "retweet" in question
    if 'retweet' in question_lower:
        if 'retweets' not in typeql_lower and 'amplifies' not in typeql_lower:
            issues.append(SemanticIssue(
                "missing_semantic_term",
                "Question mentions 'retweet' but TypeQL has no retweets relationship",
                severity="warning"
            ))

    return issues


def main():
    input_file = '/opt/text2typeql/output/twitter/queries.csv'
    passed_file = '/opt/text2typeql/output/twitter/queries_reviewed.csv'
    failed_file = '/opt/text2typeql/output/twitter/failed_review.csv'

    passed_queries = []
    failed_queries = []

    total = 0
    error_count = 0
    warning_count = 0

    print("=" * 80)
    print("SEMANTIC REVIEW OF TWITTER TYPEQL QUERIES")
    print("=" * 80)
    print()

    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            total += 1
            review = review_query(row)

            errors = [i for i in review.issues if i.severity == "error"]
            warnings = [i for i in review.issues if i.severity == "warning"]

            if errors:
                error_count += 1
                failed_queries.append(review)
                print(f"[FAIL] Query {review.original_index}: {review.question[:60]}...")
                for issue in errors:
                    print(f"       ERROR: {issue.issue_type}: {issue.description}")
                for issue in warnings:
                    print(f"       WARNING: {issue.issue_type}: {issue.description}")
                print()
            elif warnings:
                warning_count += 1
                # Keep queries with only warnings in passed
                passed_queries.append(review)
                print(f"[WARN] Query {review.original_index}: {review.question[:60]}...")
                for issue in warnings:
                    print(f"       WARNING: {issue.issue_type}: {issue.description}")
                print()
            else:
                passed_queries.append(review)

    # Write passed queries
    with open(passed_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['original_index', 'question', 'cypher', 'typeql'])
        for review in passed_queries:
            writer.writerow([review.original_index, review.question, review.cypher, review.typeql])

    # Write failed queries
    with open(failed_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['original_index', 'question', 'cypher', 'typeql', 'review_reason'])
        for review in failed_queries:
            writer.writerow([review.original_index, review.question, review.cypher, review.typeql, review.review_reason])

    # Print summary
    print()
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total queries reviewed: {total}")
    print(f"Passed (no errors): {len(passed_queries)} ({100*len(passed_queries)/total:.1f}%)")
    print(f"Failed (with errors): {len(failed_queries)} ({100*len(failed_queries)/total:.1f}%)")
    print(f"Warnings only: {warning_count}")
    print()
    print(f"Passed queries written to: {passed_file}")
    print(f"Failed queries written to: {failed_file}")
    print()

    # Write queries with warnings to a separate file for reference
    warnings_file = '/opt/text2typeql/output/twitter/warnings_review.csv'
    warning_queries = [r for r in passed_queries if r.issues]
    with open(warnings_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['original_index', 'question', 'cypher', 'typeql', 'warnings'])
        for review in warning_queries:
            writer.writerow([review.original_index, review.question, review.cypher, review.typeql, review.review_reason])

    print(f"Queries with warnings written to: {warnings_file}")
    print()

    # Categorize failures
    if failed_queries:
        print("=" * 80)
        print("FAILURE CATEGORIES")
        print("=" * 80)

        categories = {}
        for review in failed_queries:
            for issue in review.issues:
                if issue.severity == "error":
                    cat = issue.issue_type
                    if cat not in categories:
                        categories[cat] = []
                    categories[cat].append(review.original_index)

        for cat, indices in sorted(categories.items(), key=lambda x: -len(x[1])):
            print(f"{cat}: {len(indices)} queries")
            if len(indices) <= 10:
                print(f"  Query indices: {indices}")
            else:
                print(f"  Query indices (first 10): {indices[:10]}...")
        print()

    # Categorize warnings
    print("=" * 80)
    print("WARNING CATEGORIES")
    print("=" * 80)

    warning_categories = {}
    for review in passed_queries:
        for issue in review.issues:
            if issue.severity == "warning":
                cat = issue.issue_type
                if cat not in warning_categories:
                    warning_categories[cat] = []
                warning_categories[cat].append(review.original_index)

    for cat, indices in sorted(warning_categories.items(), key=lambda x: -len(x[1])):
        print(f"{cat}: {len(indices)} queries")
    print()


if __name__ == '__main__':
    main()
