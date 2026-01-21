#!/usr/bin/env python3
"""
Semantic Review Script for Neoflix Database Queries

This script reviews all queries in queries.csv to verify that the TypeQL
semantically answers the English question correctly. It identifies issues like:
- Missing job filters for crew (Director, Producer, etc.)
- Missing country/language relation filters
- Missing date range filters
- Wrong revenue/budget thresholds
- Missing ratio calculations
- Incorrect relation traversals
- Missing constraints from question

Output: failed_review.csv with queries that fail semantic review
"""

import csv
import re
import os
from dataclasses import dataclass
from typing import List, Tuple, Optional

@dataclass
class Query:
    original_index: str
    question: str
    cypher: str
    typeql: str
    row_num: int

@dataclass
class ReviewResult:
    query: Query
    passed: bool
    reasons: List[str]


def load_queries(filepath: str) -> List[Query]:
    """Load queries from CSV file."""
    queries = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is row 1)
            queries.append(Query(
                original_index=row['original_index'],
                question=row['question'],
                cypher=row['cypher'],
                typeql=row['typeql'],
                row_num=row_num
            ))
    return queries


def check_job_filter_missing(query: Query) -> Optional[str]:
    """
    Check if a job-specific role is mentioned in the question but missing in TypeQL.
    Returns reason string if issue found, None otherwise.
    """
    question_lower = query.question.lower()
    typeql_lower = query.typeql.lower()
    cypher_lower = query.cypher.lower()

    # Map of question keywords to expected job values
    job_keywords = {
        'director': 'Director',
        'directed': 'Director',
        'produce': 'Producer',
        'produced by': None,  # This can mean production company, not person as producer
        'produced a movie': 'Producer',
        'producer': 'Producer',
        'composer': 'Composer',
        'music by': 'Original Music Composer',
        'music': None,  # Too general, need to check context
        'writer': 'Writer',
        'screenplay': 'Screenplay',
        'cinematographer': 'Cinematographer',
        'editor': 'Editor',
    }

    issues = []

    # Check if Cypher has job filter but TypeQL doesn't
    cypher_job_match = re.search(r"job:\s*'([^']+)'", query.cypher, re.IGNORECASE)
    if cypher_job_match:
        expected_job = cypher_job_match.group(1)
        # Check if TypeQL has this job filter
        if f'has job "{expected_job}"' not in query.typeql and f"has job '{expected_job}'" not in query.typeql:
            issues.append(f"Missing job filter '{expected_job}' - Cypher has job filter but TypeQL doesn't")

    # Check for specific director/producer mentions
    if 'director' in question_lower or 'directed' in question_lower:
        # Check if it's about a person directing (not just director role name)
        if 'crew_for' in typeql_lower and 'job' not in typeql_lower:
            # Cypher should have Director job
            if 'director' in cypher_lower:
                issues.append("Question mentions 'director/directed' but TypeQL crew_for missing job='Director' filter")

    if 'producer' in question_lower and 'person' in question_lower:
        if 'crew_for' in typeql_lower and 'job' not in typeql_lower:
            if 'producer' in cypher_lower:
                issues.append("Question mentions 'producer' (person) but TypeQL crew_for missing job='Producer' filter")

    # Special check for "both acted in and produced"
    if ('acted' in question_lower and 'produced' in question_lower) or \
       ('act' in question_lower and 'produce' in question_lower):
        if 'crew_for' in typeql_lower and 'job' not in typeql_lower:
            # This might mean produced as Producer role
            if 'producer' in question_lower or 'produced a movie' in question_lower:
                issues.append("Question implies 'produced' as Producer role but TypeQL crew_for missing job filter")

    # Check for music/composer
    if 'music by' in question_lower or 'composer' in question_lower:
        if 'crew_for' in typeql_lower and 'job' not in typeql_lower:
            issues.append("Question mentions 'music/composer' but TypeQL crew_for missing job filter")

    return '; '.join(issues) if issues else None


def check_country_filter_missing(query: Query) -> Optional[str]:
    """Check if country is mentioned but not properly filtered."""
    question_lower = query.question.lower()
    typeql_lower = query.typeql.lower()

    issues = []

    # Check for country mentions in question
    country_patterns = [
        r"united states|usa|america",
        r"france|french",
        r"germany|german",
        r"uk|united kingdom|british",
        r"japan|japanese",
        r"china|chinese",
        r"india|indian",
    ]

    for pattern in country_patterns:
        if re.search(pattern, question_lower):
            if 'produced_in_country' not in typeql_lower and 'country' in query.cypher.lower():
                issues.append(f"Question mentions country but TypeQL missing produced_in_country relation")
                break

    # Check for "not in country X" patterns
    if 'other than' in question_lower or 'not in' in question_lower or 'except' in question_lower:
        if 'country' in question_lower:
            if 'not {' not in typeql_lower and 'not{' not in typeql_lower:
                if '<>' in query.cypher or 'NOT' in query.cypher.upper():
                    issues.append("Question has 'other than/not in' for country but TypeQL may be missing negation")

    return '; '.join(issues) if issues else None


def check_language_filter_missing(query: Query) -> Optional[str]:
    """Check if language is mentioned but not properly filtered."""
    question_lower = query.question.lower()
    typeql_lower = query.typeql.lower()

    issues = []

    # Check for original vs spoken language
    if 'original language' in question_lower:
        if 'original_language' not in typeql_lower:
            if 'original_language' in query.cypher.lower() or 'ORIGINAL_LANGUAGE' in query.cypher:
                issues.append("Question mentions 'original language' but TypeQL missing original_language relation")

    if 'spoken' in question_lower and 'language' in question_lower:
        if 'spoken_in_language' not in typeql_lower:
            if 'SPOKEN_IN_LANGUAGE' in query.cypher or 'spoken_in_language' in query.cypher.lower():
                issues.append("Question mentions 'spoken language' but TypeQL missing spoken_in_language relation")

    # Check for specific language that appears in question but not filtered
    languages = ['english', 'french', 'spanish', 'german', 'japanese', 'chinese', 'korean', 'kiswahili']
    for lang in languages:
        if lang in question_lower:
            if lang not in typeql_lower and f'"{lang}"' not in typeql_lower.lower():
                if lang in query.cypher.lower():
                    issues.append(f"Question mentions '{lang}' but TypeQL missing language filter")

    return '; '.join(issues) if issues else None


def check_date_filter_missing(query: Query) -> Optional[str]:
    """Check if date ranges are mentioned but not properly filtered."""
    question_lower = query.question.lower()
    typeql_lower = query.typeql.lower()
    cypher_lower = query.cypher.lower()

    issues = []

    # Check for year mentions
    year_match = re.search(r'\b(19\d{2}|20\d{2})\b', query.question)
    if year_match:
        year = year_match.group(1)
        # Check if TypeQL has date filter
        if 'release_date' not in typeql_lower:
            if 'release_date' in cypher_lower:
                issues.append(f"Question mentions year {year} but TypeQL missing release_date filter")
        else:
            # Check if the specific year is used
            if year not in query.typeql:
                # Check if it should be present
                if year in query.cypher:
                    issues.append(f"Year {year} in Cypher but not in TypeQL date filter")

    # Check for decade mentions
    decade_patterns = {
        '1990s': ('1990-01-01', '2000-01-01'),
        '1980s': ('1980-01-01', '1990-01-01'),
        '2000s': ('2000-01-01', '2010-01-01'),
        '2010s': ('2010-01-01', '2020-01-01'),
    }

    for decade, (start, end) in decade_patterns.items():
        if decade in question_lower:
            # Check for range filters
            if start not in query.typeql or end not in query.typeql:
                if start in query.cypher or decade in cypher_lower:
                    issues.append(f"Question mentions {decade} but TypeQL missing proper date range filter")

    # Check for "before/after year X"
    before_match = re.search(r'before\s+(?:the\s+year\s+)?(\d{4})', question_lower)
    after_match = re.search(r'after\s+(?:the\s+year\s+)?(\d{4})', question_lower)

    if before_match:
        year = before_match.group(1)
        if f'< {year}' not in query.typeql and f'<{year}' not in query.typeql and year not in query.typeql:
            if year in query.cypher:
                issues.append(f"Question says 'before {year}' but TypeQL missing proper date filter")

    if after_match:
        year = after_match.group(1)
        if f'> {year}' not in query.typeql and f'>{year}' not in query.typeql and year not in query.typeql:
            if year in query.cypher:
                issues.append(f"Question says 'after {year}' but TypeQL missing proper date filter")

    return '; '.join(issues) if issues else None


def check_threshold_values(query: Query) -> Optional[str]:
    """Check if numerical thresholds are correctly translated."""
    issues = []

    # Extract numbers from question
    question_numbers = re.findall(r'(\d+(?:\.\d+)?)\s*(?:million|m\b)', query.question.lower())

    for num in question_numbers:
        # Convert million to actual number
        expected_value = float(num) * 1000000
        expected_str = str(int(expected_value))

        # Check if this value appears in TypeQL
        if expected_str not in query.typeql:
            if expected_str in query.cypher:
                issues.append(f"Expected threshold {expected_str} from question ({num} million) not in TypeQL")

    # Check for specific amounts without 'million'
    dollar_amounts = re.findall(r'\$(\d+(?:,\d{3})*)', query.question)
    for amount in dollar_amounts:
        clean_amount = amount.replace(',', '')
        if clean_amount not in query.typeql:
            if clean_amount in query.cypher:
                issues.append(f"Expected dollar amount {clean_amount} from question not in TypeQL")

    # Check for percentage/score thresholds
    score_patterns = re.findall(r'(?:above|over|higher than|greater than|below|under|less than)\s+(\d+(?:\.\d+)?)', query.question.lower())
    for score in score_patterns:
        if score not in query.typeql:
            if score in query.cypher:
                issues.append(f"Expected threshold {score} from question not in TypeQL")

    return '; '.join(issues) if issues else None


def check_ratio_calculations(query: Query) -> Optional[str]:
    """Check if ratio calculations are properly implemented."""
    question_lower = query.question.lower()

    issues = []

    # Check for ratio mentions
    ratio_patterns = [
        'budget to revenue ratio',
        'revenue to budget ratio',
        'budget-efficient',
        'efficiency',
        'profit margin',
        'return on investment',
        'roi',
    ]

    for pattern in ratio_patterns:
        if pattern in question_lower:
            # Check if TypeQL has let statement for ratio
            if 'let' not in query.typeql.lower() and '/' not in query.typeql:
                if '/' in query.cypher or 'ratio' in query.cypher.lower():
                    issues.append(f"Question mentions '{pattern}' but TypeQL missing ratio calculation")

    # Check for discrepancy/profit calculations
    if 'discrepancy' in question_lower or 'profit' in question_lower:
        if 'let' not in query.typeql.lower() and '-' not in query.typeql:
            if '-' in query.cypher:
                issues.append("Question mentions 'discrepancy/profit' but TypeQL missing subtraction calculation")

    return '; '.join(issues) if issues else None


def check_aggregation_mismatch(query: Query) -> Optional[str]:
    """Check if aggregation is properly implemented."""
    question_lower = query.question.lower()
    typeql_lower = query.typeql.lower()
    cypher_upper = query.cypher.upper()

    issues = []

    # Check for count aggregation
    if 'how many' in question_lower or 'count' in question_lower or 'number of' in question_lower:
        if 'COUNT' in cypher_upper:
            if 'reduce' not in typeql_lower and 'count' not in typeql_lower:
                issues.append("Question asks for count but TypeQL missing reduce/count aggregation")

    # Check for average
    if 'average' in question_lower or 'avg' in question_lower:
        if 'AVG' in cypher_upper or 'avg(' in query.cypher.lower():
            if 'mean' not in typeql_lower and 'avg' not in typeql_lower:
                issues.append("Question asks for average but TypeQL missing mean aggregation")

    # Check for sum
    if 'total' in question_lower or 'sum' in question_lower:
        if 'SUM' in cypher_upper:
            if 'sum' not in typeql_lower:
                issues.append("Question asks for sum/total but TypeQL missing sum aggregation")

    # Check for max/min
    if 'highest' in question_lower or 'maximum' in question_lower or 'most' in question_lower:
        # These are usually handled by ORDER BY DESC LIMIT, check sorting
        if 'ORDER BY' in cypher_upper and 'DESC' in cypher_upper:
            if 'sort' not in typeql_lower or 'desc' not in typeql_lower:
                # Could be missing sort
                pass  # Many cases handle this differently

    if 'lowest' in question_lower or 'minimum' in question_lower or 'least' in question_lower:
        if 'ORDER BY' in cypher_upper and 'ASC' not in cypher_upper and 'DESC' not in cypher_upper:
            if 'sort' not in typeql_lower or 'asc' not in typeql_lower:
                pass  # Many cases handle this differently

    return '; '.join(issues) if issues else None


def check_sorting_mismatch(query: Query) -> Optional[str]:
    """Check if sorting is properly implemented."""
    question_lower = query.question.lower()
    typeql_lower = query.typeql.lower()
    cypher_upper = query.cypher.upper()

    issues = []

    # Check for ORDER BY in Cypher but missing sort in TypeQL
    if 'ORDER BY' in cypher_upper:
        if 'sort' not in typeql_lower:
            # Check if aggregation - some aggregations don't need sort in TypeQL
            if 'reduce' not in typeql_lower:
                issues.append("Cypher has ORDER BY but TypeQL missing sort clause")

    # Check ascending vs descending
    if 'ORDER BY' in cypher_upper and 'DESC' in cypher_upper:
        if 'sort' in typeql_lower and 'desc' not in typeql_lower:
            issues.append("Cypher sorts DESC but TypeQL sort missing 'desc'")

    if 'ORDER BY' in cypher_upper and 'ASC' in cypher_upper:
        if 'sort' in typeql_lower and 'asc' not in typeql_lower and 'desc' in typeql_lower:
            issues.append("Cypher sorts ASC but TypeQL sorts 'desc'")

    return '; '.join(issues) if issues else None


def check_limit_mismatch(query: Query) -> Optional[str]:
    """Check if LIMIT is properly translated."""
    issues = []

    # Extract limit from Cypher
    cypher_limit = re.search(r'LIMIT\s+(\d+)', query.cypher, re.IGNORECASE)
    typeql_limit = re.search(r'limit\s+(\d+)', query.typeql, re.IGNORECASE)

    if cypher_limit:
        expected_limit = cypher_limit.group(1)
        if typeql_limit:
            actual_limit = typeql_limit.group(1)
            if expected_limit != actual_limit:
                issues.append(f"Limit mismatch: Cypher has {expected_limit}, TypeQL has {actual_limit}")
        else:
            issues.append(f"Cypher has LIMIT {expected_limit} but TypeQL missing limit clause")

    # Also check if question mentions specific number
    number_patterns = [
        r'top\s+(\d+)',
        r'first\s+(\d+)',
        r'(\d+)\s+most',
        r'(\d+)\s+least',
    ]

    for pattern in number_patterns:
        match = re.search(pattern, query.question.lower())
        if match:
            expected = match.group(1)
            if typeql_limit:
                if typeql_limit.group(1) != expected:
                    issues.append(f"Question asks for {expected} results but TypeQL has limit {typeql_limit.group(1)}")
            break

    return '; '.join(issues) if issues else None


def check_entity_type_mismatch(query: Query) -> Optional[str]:
    """Check if correct entity types are used."""
    question_lower = query.question.lower()
    typeql_lower = query.typeql.lower()

    issues = []

    # Check for video vs movie confusion
    if 'video' in question_lower and 'video' not in typeql_lower:
        if 'movie' in typeql_lower and ':Video' in query.cypher:
            issues.append("Question asks about 'video' but TypeQL uses 'movie'")

    if 'adult film' in question_lower or 'adult' in question_lower:
        if 'adult' not in typeql_lower:
            if ':Adult' in query.cypher:
                issues.append("Question asks about 'adult' content but TypeQL uses wrong entity type")

    return '; '.join(issues) if issues else None


def check_relation_traversal(query: Query) -> Optional[str]:
    """Check if relation traversals match between Cypher and TypeQL."""
    issues = []

    # Map Cypher relations to TypeQL
    relation_map = {
        ':CAST_FOR': 'cast_for',
        ':CREW_FOR': 'crew_for',
        ':IN_GENRE': 'in_genre',
        ':PRODUCED_BY': 'produced_by',
        ':PRODUCED_IN_COUNTRY': 'produced_in_country',
        ':ORIGINAL_LANGUAGE': 'original_language',
        ':SPOKEN_IN_LANGUAGE': 'spoken_in_language',
        ':IN_COLLECTION': 'in_collection',
        ':HAS_KEYWORD': 'has_keyword',
        ':RATED': 'rated',
        ':PROVIDES_ACCESS_TO': 'provides_access_to',
        ':FOR_PACKAGE': 'for_package',
    }

    for cypher_rel, typeql_rel in relation_map.items():
        if cypher_rel in query.cypher or cypher_rel.lower() in query.cypher.lower():
            if typeql_rel not in query.typeql.lower():
                issues.append(f"Cypher uses {cypher_rel} but TypeQL missing {typeql_rel} relation")

    return '; '.join(issues) if issues else None


def check_negation_patterns(query: Query) -> Optional[str]:
    """Check if negation is properly implemented."""
    question_lower = query.question.lower()
    typeql_lower = query.typeql.lower()
    cypher_upper = query.cypher.upper()

    issues = []

    # Check for negation indicators
    negation_words = ['not', 'never', 'no', 'without', 'except', 'other than', 'exclude']

    has_negation = any(word in question_lower for word in negation_words)

    if has_negation:
        # Check if Cypher has negation
        cypher_negation = 'NOT' in cypher_upper or '<>' in query.cypher or '!=' in query.cypher
        typeql_negation = 'not {' in typeql_lower or 'not{' in typeql_lower

        if cypher_negation and not typeql_negation:
            issues.append("Question has negation and Cypher has NOT/<> but TypeQL missing 'not {}' block")

    return '; '.join(issues) if issues else None


def check_cast_order(query: Query) -> Optional[str]:
    """Check if cast order filtering is correct."""
    question_lower = query.question.lower()
    typeql_lower = query.typeql.lower()

    issues = []

    # Check for first/lead actor mentions
    if 'first' in question_lower and ('actor' in question_lower or 'cast' in question_lower):
        if 'cast_order' not in typeql_lower:
            if 'order' in query.cypher.lower() or 'r.order' in query.cypher.lower():
                issues.append("Question mentions first actor/cast but TypeQL missing cast_order filter")

    if 'lead' in question_lower or 'main' in question_lower:
        if 'cast_order' not in typeql_lower and 'cast_order' in query.cypher.lower():
            issues.append("Question mentions lead/main role but TypeQL missing cast_order filter")

    return '; '.join(issues) if issues else None


def check_distinct_missing(query: Query) -> Optional[str]:
    """Check if DISTINCT is needed but missing."""
    issues = []

    if 'DISTINCT' in query.cypher.upper():
        # TypeQL typically handles uniqueness differently
        # But we should note if explicit distinct was in Cypher
        pass  # TypeQL may handle this differently

    return '; '.join(issues) if issues else None


def check_null_handling(query: Query) -> Optional[str]:
    """Check if NULL handling is correct."""
    question_lower = query.question.lower()
    cypher_upper = query.cypher.upper()

    issues = []

    # Check for "no revenue reported" type questions
    if 'no revenue' in question_lower or 'no budget' in question_lower:
        if 'IS NULL' in cypher_upper:
            # TypeQL handles NULL differently - absence of attribute
            # Check if query fetches a value that should be null
            if 'has revenue' in query.typeql.lower() or 'has budget' in query.typeql.lower():
                issues.append("Question asks for NULL values but TypeQL has attribute which implies non-NULL")

    return '; '.join(issues) if issues else None


def check_character_name(query: Query) -> Optional[str]:
    """Check if character name filtering is correct."""
    question_lower = query.question.lower()
    typeql_lower = query.typeql.lower()

    issues = []

    # Check for character mentions
    char_match = re.search(r"character\s+(?:named\s+)?['\"]?([^'\"]+)['\"]?", question_lower)
    if char_match or 'character' in question_lower:
        if 'character' not in typeql_lower:
            if 'character' in query.cypher.lower():
                issues.append("Question mentions character but TypeQL missing character attribute filter")

    return '; '.join(issues) if issues else None


def review_query(query: Query) -> ReviewResult:
    """Run all semantic checks on a single query."""
    reasons = []

    # Run all checks
    checks = [
        check_job_filter_missing,
        check_country_filter_missing,
        check_language_filter_missing,
        check_date_filter_missing,
        check_threshold_values,
        check_ratio_calculations,
        check_aggregation_mismatch,
        check_sorting_mismatch,
        check_limit_mismatch,
        check_entity_type_mismatch,
        check_relation_traversal,
        check_negation_patterns,
        check_cast_order,
        check_null_handling,
        check_character_name,
    ]

    for check in checks:
        result = check(query)
        if result:
            reasons.append(result)

    return ReviewResult(
        query=query,
        passed=len(reasons) == 0,
        reasons=reasons
    )


def save_failed_reviews(results: List[ReviewResult], output_path: str):
    """Save failed reviews to CSV file."""
    failed = [r for r in results if not r.passed]

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['original_index', 'question', 'cypher', 'typeql', 'review_reason'])

        for result in failed:
            writer.writerow([
                result.query.original_index,
                result.query.question,
                result.query.cypher,
                result.query.typeql,
                ' | '.join(result.reasons)
            ])

    return len(failed)


def save_passed_reviews(results: List[ReviewResult], queries_path: str, output_path: str):
    """Save passed reviews (remaining queries) to the original CSV format."""
    passed = [r for r in results if r.passed]

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['original_index', 'question', 'cypher', 'typeql'])

        for result in passed:
            writer.writerow([
                result.query.original_index,
                result.query.question,
                result.query.cypher,
                result.query.typeql
            ])

    return len(passed)


def main():
    # Paths
    base_dir = '/opt/text2typeql/output/neoflix'
    queries_path = os.path.join(base_dir, 'queries.csv')
    failed_path = os.path.join(base_dir, 'failed_review.csv')

    print(f"Loading queries from {queries_path}...")
    queries = load_queries(queries_path)
    print(f"Loaded {len(queries)} queries")

    print("\nRunning semantic review...")
    results = []
    for i, query in enumerate(queries):
        if (i + 1) % 100 == 0:
            print(f"  Reviewed {i + 1}/{len(queries)} queries...")
        result = review_query(query)
        results.append(result)

    # Count results
    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed)

    print(f"\nReview complete!")
    print(f"  Passed: {passed}")
    print(f"  Failed: {failed}")

    # Save failed reviews
    print(f"\nSaving failed reviews to {failed_path}...")
    save_failed_reviews(results, failed_path)

    # Print summary of issues
    print("\n" + "="*60)
    print("ISSUE SUMMARY")
    print("="*60)

    issue_counts = {}
    for result in results:
        if not result.passed:
            for reason in result.reasons:
                # Extract main issue type
                if 'job' in reason.lower() or 'director' in reason.lower() or 'producer' in reason.lower():
                    issue_type = "Missing job filter (Director/Producer/etc.)"
                elif 'country' in reason.lower():
                    issue_type = "Country filter issue"
                elif 'language' in reason.lower():
                    issue_type = "Language filter issue"
                elif 'date' in reason.lower() or 'year' in reason.lower():
                    issue_type = "Date/year filter issue"
                elif 'threshold' in reason.lower() or 'million' in reason.lower():
                    issue_type = "Threshold value issue"
                elif 'ratio' in reason.lower() or 'calculation' in reason.lower():
                    issue_type = "Ratio/calculation issue"
                elif 'aggregation' in reason.lower() or 'count' in reason.lower() or 'sum' in reason.lower():
                    issue_type = "Aggregation issue"
                elif 'sort' in reason.lower():
                    issue_type = "Sorting issue"
                elif 'limit' in reason.lower():
                    issue_type = "Limit mismatch"
                elif 'entity' in reason.lower() or 'type' in reason.lower():
                    issue_type = "Entity type mismatch"
                elif 'relation' in reason.lower():
                    issue_type = "Relation traversal issue"
                elif 'negation' in reason.lower() or 'not' in reason.lower():
                    issue_type = "Negation pattern issue"
                elif 'cast_order' in reason.lower():
                    issue_type = "Cast order issue"
                elif 'null' in reason.lower():
                    issue_type = "NULL handling issue"
                elif 'character' in reason.lower():
                    issue_type = "Character filter issue"
                else:
                    issue_type = "Other"

                issue_counts[issue_type] = issue_counts.get(issue_type, 0) + 1

    for issue_type, count in sorted(issue_counts.items(), key=lambda x: -x[1]):
        print(f"  {issue_type}: {count}")

    print("\n" + "="*60)
    print(f"Detailed failed reviews written to: {failed_path}")
    print("="*60)

    # Print first few failed examples
    failed_results = [r for r in results if not r.passed]
    if failed_results:
        print("\n" + "="*60)
        print("SAMPLE FAILED QUERIES (first 5)")
        print("="*60)
        for result in failed_results[:5]:
            print(f"\n--- Query {result.query.original_index} ---")
            print(f"Question: {result.query.question[:100]}...")
            print(f"Issues: {'; '.join(result.reasons)}")


if __name__ == '__main__':
    main()
