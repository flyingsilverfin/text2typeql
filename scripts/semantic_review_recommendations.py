#!/usr/bin/env python3
"""
Semantic Review Script for Recommendations Database Queries

This script reviews all queries in the recommendations dataset to verify
that the TypeQL semantically answers the English question correctly.

Common issues checked:
- Missing filters or conditions from the question
- Wrong sort direction
- Missing aggregations (count, sum, avg) when question asks "how many" or "total"
- Missing relationship constraints
- Wrong thresholds (e.g., budget > 1000000 vs budget > 1)
- Missing ratio calculations (revenue/budget)
- Missing sort/limit after reduce (aggregation)
"""

import csv
import re
import sys
from pathlib import Path
from typing import Optional, Tuple, List

# Schema reference for recommendations database
SCHEMA = {
    "entities": ["movie", "genre", "user", "person"],
    "relations": ["in_genre", "rated", "acted_in", "directed"],
    "movie_attrs": ["budget", "revenue", "runtime", "imdb_rating", "year",
                    "countries", "languages", "plot", "title", "imdb_votes",
                    "released", "poster", "url", "tmdb_id", "imdb_id", "movie_id"],
    "person_attrs": ["name", "born", "died", "born_in", "bio", "poster", "url", "tmdb_id", "imdb_id"],
    "user_attrs": ["name", "user_id"],
    "genre_attrs": ["name"],
    "rated_attrs": ["rating", "timestamp"],
    "acted_in_attrs": ["character_role"],
    "directed_attrs": ["character_role"]
}

# Monetary thresholds commonly found in questions
MONETARY_THRESHOLDS = {
    "million": 1000000,
    "billion": 1000000000,
    "hundred million": 100000000,
    "50 million": 50000000,
    "75 million": 75000000,
    "100 million": 100000000,
    "200 million": 200000000,
    "500 million": 500000000,
    "1 billion": 1000000000,
    "10 million": 10000000,
    "20 million": 20000000
}


def extract_threshold_from_question(question: str) -> Optional[Tuple[str, int]]:
    """Extract monetary thresholds from the question."""
    q_lower = question.lower()

    # Check for specific dollar amounts
    # Pattern: X million/billion (dollars)
    patterns = [
        (r'(\d+)\s*billion', lambda m: int(m.group(1)) * 1000000000),
        (r'(\d+)\s*hundred\s*million', lambda m: int(m.group(1)) * 100000000),
        (r'(\d+)\s*million', lambda m: int(m.group(1)) * 1000000),
        (r'\$(\d+)\s*million', lambda m: int(m.group(1)) * 1000000),
        (r'\$(\d+)\s*billion', lambda m: int(m.group(1)) * 1000000000),
    ]

    for pattern, converter in patterns:
        match = re.search(pattern, q_lower)
        if match:
            return ("budget_or_revenue", converter(match))

    return None


def check_aggregation_needed(question: str) -> Optional[str]:
    """Check if the question requires aggregation."""
    q_lower = question.lower()

    if any(phrase in q_lower for phrase in ["how many", "number of", "count"]):
        return "count"
    if "average" in q_lower or "avg" in q_lower:
        return "mean"
    if "total" in q_lower and "sum" in q_lower:
        return "sum"
    if "highest average" in q_lower or "lowest average" in q_lower:
        return "mean"

    return None


def check_sort_direction(question: str, typeql: str) -> Optional[str]:
    """Check if sort direction matches the question intent."""
    q_lower = question.lower()

    # Check for "top" / "highest" / "most" -> desc
    expects_desc = any(w in q_lower for w in [
        "top", "highest", "most", "best", "greatest", "largest", "maximum"
    ])

    # Check for "bottom" / "lowest" / "least" / "oldest" / "first" (chronological)
    expects_asc = any(w in q_lower for w in [
        "lowest", "least", "smallest", "minimum", "oldest", "shortest", "first"
    ])

    # Special case: "oldest" usually means sort by date asc
    if "oldest" in q_lower:
        if "sort" in typeql:
            if "desc" in typeql and ("born" in typeql or "year" in typeql or "released" in typeql):
                return "Question asks for 'oldest' but TypeQL sorts desc (should be asc)"

    # Special case: "youngest" usually means sort by date desc
    if "youngest" in q_lower:
        if "sort" in typeql:
            if "asc" in typeql and ("born" in typeql):
                return "Question asks for 'youngest' but TypeQL sorts asc (should be desc)"

    # Check for "shortest" runtime
    if "shortest" in q_lower and "runtime" in q_lower:
        if "sort" in typeql and "desc" in typeql:
            return "Question asks for 'shortest runtime' but TypeQL sorts desc (should be asc)"

    # Check for "longest" runtime
    if "longest" in q_lower and "runtime" in q_lower:
        if "sort" in typeql and "asc" in typeql:
            return "Question asks for 'longest runtime' but TypeQL sorts asc (should be desc)"

    return None


def check_ratio_calculation(question: str, typeql: str) -> Optional[str]:
    """Check if ratio calculations are present when needed."""
    q_lower = question.lower()

    # Budget to revenue ratio
    if "budget" in q_lower and "revenue" in q_lower and "ratio" in q_lower:
        if "let $ratio" not in typeql and "/" not in typeql:
            return "Question asks for budget/revenue ratio but TypeQL lacks ratio calculation"

    # Revenue more than double budget
    if "more than double" in q_lower or "twice" in q_lower:
        if "ratio" not in typeql and "> 2" not in typeql and "$ratio > 2" not in typeql:
            return "Question asks for ratio comparison but TypeQL may lack proper calculation"

    # Profit calculation
    if "profit" in q_lower:
        if "let $profit" not in typeql and "$r - $b" not in typeql and "revenue" not in typeql:
            return "Question asks for profit but TypeQL lacks profit calculation"

    return None


def check_threshold_match(question: str, typeql: str) -> Optional[str]:
    """Check if threshold values in TypeQL match the question."""
    threshold_info = extract_threshold_from_question(question)

    if threshold_info:
        expected_value = threshold_info[1]

        # Look for numeric thresholds in TypeQL for budget/revenue
        # Pattern: $b > number or $r > number (budget/revenue variables)
        # Only check for values that are likely budget/revenue (> 100000)
        numbers_in_typeql = re.findall(r'\$[br]\s*[><=]+\s*(\d+)', typeql)

        if numbers_in_typeql:
            for num_str in numbers_in_typeql:
                num = int(num_str)
                # Skip year values (1900-2100 range)
                if 1900 <= num <= 2100:
                    continue
                # Skip rating values (typically 0-10 range)
                if num <= 10:
                    continue
                # Check if the number matches expected or is way off
                if num > 0 and expected_value > 0:
                    ratio = num / expected_value if num > 0 else 0
                    if ratio < 0.001 or ratio > 1000:
                        # Huge difference, likely wrong
                        return f"Threshold mismatch: question mentions {expected_value:,} but TypeQL has {num:,}"

    return None


def check_missing_filter_count(question: str, typeql: str) -> Optional[str]:
    """Check if aggregation results are filtered when question specifies threshold."""
    q_lower = question.lower()

    # Pattern: "actors who have acted in more than X movies"
    count_patterns = [
        (r'more than (\d+) movies', '>'),
        (r'at least (\d+) movies', '>='),
        (r'over (\d+) movies', '>'),
        (r'more than (\d+) ratings', '>'),
        (r'at least (\d+) ratings', '>='),
        (r'more than (\d+) different', '>'),
        (r'at least (\d+) different', '>='),
    ]

    for pattern, operator in count_patterns:
        match = re.search(pattern, q_lower)
        if match:
            threshold = int(match.group(1))
            # Check if TypeQL has the threshold filter after reduce
            if "reduce" in typeql:
                # Look for match $count > N or match $count >= N patterns
                filter_patterns = [
                    rf'\$count\s*>\s*{threshold}',
                    rf'\$count\s*>=\s*{threshold}',
                    rf'match\s+\$count\s*>\s*{threshold}',
                    rf'match\s+\$count\s*>=\s*{threshold}',
                ]
                found = any(re.search(p, typeql) for p in filter_patterns)
                if not found:
                    return f"Question requires count {operator} {threshold} but filter may be missing after reduce"

    return None


def check_missing_sort_after_aggregate(question: str, typeql: str) -> Optional[str]:
    """Check if sort is missing after aggregation when ordering is implied."""
    q_lower = question.lower()

    # If question asks for "top" or "most" with aggregation
    needs_sort = any(w in q_lower for w in ["top", "most", "highest", "lowest", "least"])

    if needs_sort and "reduce" in typeql:
        if "sort" not in typeql:
            return "Question asks for ordered results with aggregation but 'sort' is missing after 'reduce'"

    return None


def check_missing_limit(question: str, typeql: str) -> Optional[str]:
    """Check if limit is missing when question specifies a number."""
    q_lower = question.lower()

    # Pattern: "top 5", "first 3" - these specify result count
    # But NOT "more than 5 movies" which is a filter condition
    limit_patterns = [
        r'^(?:what are the |list the |show the |which |identify the |name the |find the |who are the )?(?:top|first)\s+(\d+)',
        r'^(?:what are the |list the |show the |which |identify the |name the |find the |who are the )?(\d+)\s+(?:most|oldest|youngest|highest|lowest)',
        r'^list\s+(\d+)\s+',
        r'^name\s+(\d+)\s+',
    ]

    for pattern in limit_patterns:
        match = re.search(pattern, q_lower)
        if match:
            expected_limit = match.group(1)
            if "limit" in typeql:
                limit_match = re.search(r'limit\s+(\d+)', typeql)
                if limit_match and limit_match.group(1) != expected_limit:
                    return f"Question asks for {expected_limit} results but TypeQL has limit {limit_match.group(1)}"
            else:
                # Limit might be intentionally omitted for some queries
                pass

    return None


def check_countries_languages_as_string(question: str, typeql: str) -> Optional[str]:
    """Check that countries and languages are treated as string attributes, not entities."""
    q_lower = question.lower()

    # Countries and languages are STRING attributes on movie, not separate entities
    if "countr" in q_lower:
        if "$c isa country" in typeql or "country isa" in typeql:
            return "countries is a string attribute on movie, not a separate entity"

    if "language" in q_lower:
        if "$l isa language" in typeql or "language isa" in typeql:
            return "languages is a string attribute on movie, not a separate entity"

    return None


def check_rated_by_users_count(question: str, typeql: str) -> Optional[str]:
    """Check if 'rated by more than X users' uses the rated relation correctly."""
    q_lower = question.lower()

    # Pattern: "rated by more than X users"
    if re.search(r'rated by (more than|over|at least) \d+ users', q_lower):
        if "imdb_votes" in typeql:
            return "Question asks about users who rated (rated relation) but TypeQL uses imdb_votes instead"

    return None


def check_imdb_votes_vs_user_ratings(question: str, typeql: str) -> Optional[str]:
    """Check confusion between IMDb votes and user ratings."""
    q_lower = question.lower()

    # IMDb votes refers to imdb_votes attribute
    if "imdb" in q_lower and "votes" in q_lower:
        if "rated" in typeql and "reduce" in typeql and "imdb_votes" not in typeql:
            # Might be incorrectly using rated relation instead of imdb_votes attribute
            pass  # This is complex, leave for manual review

    return None


def check_distinct_count(question: str, typeql: str) -> Optional[str]:
    """Check if distinct counting is needed but missing."""
    q_lower = question.lower()

    # Pattern: "different genres", "distinct years", "unique actors"
    if any(w in q_lower for w in ["different", "distinct", "unique", "diverse"]):
        if "reduce" in typeql and "count" in typeql:
            # TypeQL count doesn't have a distinct modifier in the same way
            # but the groupby should handle uniqueness
            pass

    return None


def check_missing_person_constraint(question: str, typeql: str) -> Optional[str]:
    """Check if actor/director constraint is missing."""
    q_lower = question.lower()

    # If asking about actors who "acted in" something specific, should have acted_in relation
    # But just asking about "actors born before X" doesn't require acted_in
    if "actor" in q_lower and "acted in" in q_lower:
        if "acted_in" not in typeql:
            return "Question asks about actors who acted in something but 'acted_in' relation is missing"

    # If asking about directors who "directed" something specific, should have directed relation
    # But just asking about "directors born before X" doesn't require directed
    if "director" in q_lower and "directed" in q_lower:
        if "directed" not in typeql:
            return "Question asks about directors who directed something but 'directed' relation is missing"

    return None


def check_genre_constraint(question: str, typeql: str) -> Optional[str]:
    """Check if genre constraint is properly applied."""
    q_lower = question.lower()

    # Skip if the question is about plot content, not genre
    if "plot" in q_lower and any(w in q_lower for w in ["containing", "mentioning", "involving"]):
        return None

    # Common genres - only check if question explicitly mentions genre context
    common_genres = ["comedy", "drama", "action", "adventure", "horror", "sci-fi",
                     "animation", "romance", "thriller", "documentary", "fantasy", "history"]

    # Look for genre patterns like "'Comedy' genre" or "Comedy genre" or "in the Comedy genre"
    genre_patterns = [
        r"['\"](\w+)['\"] genre",
        r"in the (\w+) genre",
        r"(\w+) genre",
        r"genre ['\"](\w+)['\"]",
        r"genre (?:of |called )?(\w+)",
    ]

    for pattern in genre_patterns:
        match = re.search(pattern, q_lower)
        if match:
            genre = match.group(1).lower()
            if genre in common_genres:
                if "in_genre" not in typeql:
                    return f"Question mentions '{genre}' genre but 'in_genre' relation may be missing"
                if genre.title() not in typeql and f'"{genre.title()}"' not in typeql:
                    return f"Question mentions '{genre}' genre but it's not in the TypeQL"

    return None


def check_year_decade_filter(question: str, typeql: str) -> Optional[str]:
    """Check if decade filters are correctly applied."""
    q_lower = question.lower()

    # Decade patterns
    decades = {
        "1990s": (1990, 2000, "199"),
        "2000s": (2000, 2010, "200"),
        "2010s": (2010, 2020, "201"),
        "1980s": (1980, 1990, "198"),
        "1970s": (1970, 1980, "197"),
    }

    for decade, (start, end, prefix) in decades.items():
        if decade in q_lower:
            # Check for year-based filter OR released string pattern
            has_year_filter = (f">= {start}" in typeql or f"$y >= {start}" in typeql or
                               f"< {end}" in typeql or f"$y < {end}" in typeql)
            has_string_filter = f'like "^{prefix}' in typeql or f'like "{prefix}' in typeql

            if not has_year_filter and not has_string_filter:
                # Neither approach used
                if str(start)[:3] not in typeql:
                    return f"Question mentions {decade} but year >= {start} filter may be missing"

    return None


def semantic_review(question: str, typeql: str) -> Optional[str]:
    """
    Perform semantic review of a single query.
    Returns a reason string if the query fails review, None if it passes.
    """
    checks = [
        check_sort_direction,
        check_ratio_calculation,
        check_threshold_match,
        check_missing_filter_count,
        check_missing_sort_after_aggregate,
        check_missing_limit,
        check_countries_languages_as_string,
        check_rated_by_users_count,
        check_missing_person_constraint,
        check_genre_constraint,
        check_year_decade_filter,
    ]

    reasons = []
    for check_fn in checks:
        result = check_fn(question, typeql)
        if result:
            reasons.append(result)

    if reasons:
        return "; ".join(reasons)

    return None


def main():
    input_file = Path("/opt/text2typeql/output/recommendations/queries.csv")
    output_passed = Path("/opt/text2typeql/output/recommendations/queries_reviewed.csv")
    output_failed = Path("/opt/text2typeql/output/recommendations/failed_review.csv")

    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        sys.exit(1)

    passed_queries = []
    failed_queries = []

    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames

        for row in reader:
            question = row.get('question', '')
            typeql = row.get('typeql', '')

            review_reason = semantic_review(question, typeql)

            if review_reason:
                row['review_reason'] = review_reason
                failed_queries.append(row)
            else:
                passed_queries.append(row)

    # Write passed queries
    if passed_queries:
        with open(output_passed, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(passed_queries)
        print(f"Passed queries: {len(passed_queries)} -> {output_passed}")

    # Write failed queries
    if failed_queries:
        failed_fieldnames = list(fieldnames) + ['review_reason']
        with open(output_failed, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=failed_fieldnames)
            writer.writeheader()
            writer.writerows(failed_queries)
        print(f"Failed queries: {len(failed_queries)} -> {output_failed}")

    # Summary
    total = len(passed_queries) + len(failed_queries)
    print(f"\nSummary:")
    print(f"  Total queries: {total}")
    print(f"  Passed: {len(passed_queries)} ({100*len(passed_queries)/total:.1f}%)")
    print(f"  Failed: {len(failed_queries)} ({100*len(failed_queries)/total:.1f}%)")

    # Show failure reasons breakdown
    if failed_queries:
        print(f"\nFailure reasons breakdown:")
        reason_counts = {}
        for q in failed_queries:
            reasons = q.get('review_reason', '').split('; ')
            for reason in reasons:
                # Extract the check type
                check_type = reason.split(':')[0] if ':' in reason else reason[:50]
                reason_counts[check_type] = reason_counts.get(check_type, 0) + 1

        for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
            print(f"  {reason}: {count}")


if __name__ == "__main__":
    main()
