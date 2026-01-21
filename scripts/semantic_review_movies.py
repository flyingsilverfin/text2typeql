#!/usr/bin/env python3
"""
Comprehensive semantic review of ALL queries in the movies database.

Checks for:
- Missing relation constraints (acted_in, directed, produced, wrote, reviewed, follows)
- Wrong sort direction for "lowest/highest"
- Missing aggregations
- Missing filters
- Questions asking about multiple roles (acted AND directed)
- Missing FOLLOWS relationship for social queries
- Missing REVIEWED relationship for review queries
"""

import csv
import re
from pathlib import Path
from typing import List, Tuple, Optional


def load_queries(filepath: Path) -> List[dict]:
    """Load queries from CSV file."""
    queries = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            queries.append(row)
    return queries


def extract_keywords_from_question(question: str) -> dict:
    """Extract semantic keywords from the question."""
    q_lower = question.lower()

    return {
        # Relation keywords
        'acted': any(w in q_lower for w in ['acted', 'actor', 'actors', 'acting', 'starred', 'star in', 'cast']),
        'directed': any(w in q_lower for w in ['directed', 'director', 'directors', 'direct ']),
        'produced': any(w in q_lower for w in ['produced', 'producer', 'producers', 'production']),
        'wrote': any(w in q_lower for w in ['wrote', 'written', 'writer', 'writers', 'screenplay', 'script']),
        'reviewed': any(w in q_lower for w in ['reviewed', 'reviewer', 'reviewers', 'review ', 'reviews']),
        'follows': any(w in q_lower for w in ['follow', 'follows', 'following', 'follower', 'followers', 'mutual']),

        # Sort direction keywords - be more specific
        'lowest': any(w in q_lower for w in ['lowest', 'least', 'minimum', 'fewest', 'smallest', 'bottom', 'worst']),
        'highest': any(w in q_lower for w in ['highest', 'greatest', 'largest', 'best']),
        'most': 'most' in q_lower and not ('most recent' in q_lower),  # "most" is ambiguous
        'oldest': 'oldest' in q_lower or 'earliest' in q_lower,
        'youngest': 'youngest' in q_lower,
        'most_recent': 'most recent' in q_lower or 'latest' in q_lower,

        # Filter keywords
        'tagline_contains': 'tagline' in q_lower and ('contains' in q_lower or 'with' in q_lower or 'include' in q_lower),
        'name_starts': 'name' in q_lower and ('start' in q_lower or 'begin' in q_lower),
        'born_before': 'born' in q_lower and 'before' in q_lower,
        'born_after': 'born' in q_lower and 'after' in q_lower,
        'released_before': 'released' in q_lower and 'before' in q_lower,
        'released_after': 'released' in q_lower and 'after' in q_lower,

        # Aggregation keywords - be more specific
        'count_result': any(phrase in q_lower for phrase in ['how many', 'count of', 'total number of']),

        # Multiple role keywords
        'both_acted_directed': ('acted' in q_lower or 'actor' in q_lower) and ('directed' in q_lower or 'director' in q_lower) and ('and' in q_lower or 'both' in q_lower or 'also' in q_lower),
        'both_produced_directed': ('produced' in q_lower or 'producer' in q_lower) and ('directed' in q_lower or 'director' in q_lower) and ('and' in q_lower or 'both' in q_lower),
        'both_wrote_directed': ('wrote' in q_lower or 'writer' in q_lower or 'written' in q_lower) and ('directed' in q_lower or 'director' in q_lower) and ('and' in q_lower or 'both' in q_lower),
        'both_acted_wrote': ('acted' in q_lower or 'actor' in q_lower) and ('wrote' in q_lower or 'writer' in q_lower or 'written' in q_lower) and ('and' in q_lower or 'both' in q_lower),
        'both_acted_produced': ('acted' in q_lower or 'actor' in q_lower) and ('produced' in q_lower or 'producer' in q_lower) and ('and' in q_lower or 'both' in q_lower),
        'both_reviewed_acted': ('reviewed' in q_lower or 'reviewer' in q_lower) and ('acted' in q_lower or 'actor' in q_lower) and ('and' in q_lower or 'both' in q_lower or 'also' in q_lower),
        'both_reviewed_directed': ('reviewed' in q_lower or 'reviewer' in q_lower) and ('directed' in q_lower or 'director' in q_lower) and ('and' in q_lower or 'both' in q_lower or 'also' in q_lower),
        'both_reviewed_produced': ('reviewed' in q_lower or 'reviewer' in q_lower) and ('produced' in q_lower or 'producer' in q_lower) and ('and' in q_lower or 'both' in q_lower or 'also' in q_lower),
        'both_reviewed_wrote': ('reviewed' in q_lower or 'reviewer' in q_lower) and ('wrote' in q_lower or 'writer' in q_lower or 'written' in q_lower) and ('and' in q_lower or 'both' in q_lower),

        # Mutual follows
        'mutual_follow': 'mutual' in q_lower and 'follow' in q_lower,

        # Role (character) specific queries
        'specific_role': any(w in q_lower for w in ['role', 'roles', 'character', 'characters', 'played as', 'playing the role']),

        # Negation - be more specific
        'explicit_negation': any(w in q_lower for w in ['not ', 'never ', 'haven\'t', 'hasn\'t', 'don\'t', 'doesn\'t', 'without', 'no one', 'nobody']),
    }


def check_typeql_relations(typeql: str) -> dict:
    """Check which relations are present in the TypeQL query."""
    return {
        'has_acted_in': bool(re.search(r'isa\s+acted_in', typeql)),
        'has_directed': bool(re.search(r'isa\s+directed', typeql)),
        'has_produced': bool(re.search(r'isa\s+produced', typeql)),
        'has_wrote': bool(re.search(r'isa\s+wrote', typeql)),
        'has_reviewed': bool(re.search(r'isa\s+reviewed', typeql)),
        'has_follows': bool(re.search(r'isa\s+follows', typeql)),
        'has_sort_asc': bool(re.search(r'sort\s+\$\w+\s+asc', typeql)),
        'has_sort_desc': bool(re.search(r'sort\s+\$\w+\s+desc', typeql)),
        'has_reduce': 'reduce' in typeql.lower(),
        'has_count': 'count(' in typeql.lower(),
        'has_not': bool(re.search(r'\bnot\s*\{', typeql)),
        'has_contains': 'contains' in typeql.lower(),
        'has_like': 'like' in typeql.lower(),
    }


def check_cypher_relations(cypher: str) -> dict:
    """Check which relations are present in the original Cypher query."""
    cypher_upper = cypher.upper()
    return {
        'cypher_acted_in': ':ACTED_IN' in cypher_upper,
        'cypher_directed': ':DIRECTED' in cypher_upper,
        'cypher_produced': ':PRODUCED' in cypher_upper,
        'cypher_wrote': ':WROTE' in cypher_upper,
        'cypher_reviewed': ':REVIEWED' in cypher_upper,
        'cypher_follows': ':FOLLOWS' in cypher_upper,
        # Check for actual NOT EXISTS or NOT pattern (not IS NOT NULL)
        'cypher_not_pattern': bool(re.search(r'\bNOT\s*\(', cypher_upper)) or 'NOT EXISTS' in cypher_upper or bool(re.search(r'WHERE\s+NOT\s+', cypher_upper)),
    }


def semantic_review(question: str, cypher: str, typeql: str) -> Tuple[bool, Optional[str]]:
    """
    Perform semantic review of a query.
    Returns (is_valid, reason) tuple.
    """
    keywords = extract_keywords_from_question(question)
    typeql_rels = check_typeql_relations(typeql)
    cypher_rels = check_cypher_relations(cypher)

    q_lower = question.lower()

    # === RELATION CHECKS ===

    # Check 1: If Cypher has ACTED_IN but TypeQL doesn't
    if cypher_rels['cypher_acted_in'] and not typeql_rels['has_acted_in']:
        return False, "Missing acted_in relation - Cypher has ACTED_IN but TypeQL doesn't"

    # Check 2: If Cypher has DIRECTED but TypeQL doesn't
    if cypher_rels['cypher_directed'] and not typeql_rels['has_directed']:
        return False, "Missing directed relation - Cypher has DIRECTED but TypeQL doesn't"

    # Check 3: If Cypher has PRODUCED but TypeQL doesn't
    if cypher_rels['cypher_produced'] and not typeql_rels['has_produced']:
        return False, "Missing produced relation - Cypher has PRODUCED but TypeQL doesn't"

    # Check 4: If Cypher has WROTE but TypeQL doesn't
    if cypher_rels['cypher_wrote'] and not typeql_rels['has_wrote']:
        return False, "Missing wrote relation - Cypher has WROTE but TypeQL doesn't"

    # Check 5: If Cypher has REVIEWED but TypeQL doesn't
    if cypher_rels['cypher_reviewed'] and not typeql_rels['has_reviewed']:
        return False, "Missing reviewed relation - Cypher has REVIEWED but TypeQL doesn't"

    # Check 6: If Cypher has FOLLOWS but TypeQL doesn't
    if cypher_rels['cypher_follows'] and not typeql_rels['has_follows']:
        return False, "Missing follows relation - Cypher has FOLLOWS but TypeQL doesn't"

    # === MULTIPLE ROLE CHECKS ===

    # Check: Both acted AND directed
    if keywords['both_acted_directed']:
        if not (typeql_rels['has_acted_in'] and typeql_rels['has_directed']):
            return False, "Question asks about both acting AND directing but TypeQL missing one or both relations"

    # Check: Both produced AND directed
    if keywords['both_produced_directed']:
        if not (typeql_rels['has_produced'] and typeql_rels['has_directed']):
            return False, "Question asks about both producing AND directing but TypeQL missing one or both relations"

    # Check: Both wrote AND directed
    if keywords['both_wrote_directed']:
        if not (typeql_rels['has_wrote'] and typeql_rels['has_directed']):
            return False, "Question asks about both writing AND directing but TypeQL missing one or both relations"

    # Check: Both acted AND wrote
    if keywords['both_acted_wrote']:
        if not (typeql_rels['has_acted_in'] and typeql_rels['has_wrote']):
            return False, "Question asks about both acting AND writing but TypeQL missing one or both relations"

    # Check: Both acted AND produced
    if keywords['both_acted_produced']:
        if not (typeql_rels['has_acted_in'] and typeql_rels['has_produced']):
            return False, "Question asks about both acting AND producing but TypeQL missing one or both relations"

    # Check: Both reviewed AND acted
    if keywords['both_reviewed_acted']:
        if not (typeql_rels['has_reviewed'] and typeql_rels['has_acted_in']):
            return False, "Question asks about both reviewing AND acting but TypeQL missing one or both relations"

    # Check: Both reviewed AND directed
    if keywords['both_reviewed_directed']:
        if not (typeql_rels['has_reviewed'] and typeql_rels['has_directed']):
            return False, "Question asks about both reviewing AND directing but TypeQL missing one or both relations"

    # Check: Both reviewed AND produced
    if keywords['both_reviewed_produced']:
        if not (typeql_rels['has_reviewed'] and typeql_rels['has_produced']):
            return False, "Question asks about both reviewing AND producing but TypeQL missing one or both relations"

    # Check: Both reviewed AND wrote
    if keywords['both_reviewed_wrote']:
        if not (typeql_rels['has_reviewed'] and typeql_rels['has_wrote']):
            return False, "Question asks about both reviewing AND writing but TypeQL missing one or both relations"

    # === SORT DIRECTION CHECKS ===

    # Check: lowest should have asc sort (only when we're sorting by the metric in question)
    if keywords['lowest'] and typeql_rels['has_sort_desc'] and not typeql_rels['has_sort_asc']:
        # Only flag if not combined with "most" or "highest"
        if not keywords['highest'] and not keywords['most']:
            return False, "Question asks for 'lowest/least' but TypeQL sorts DESC instead of ASC"

    # Check: oldest born should have asc sort (smaller year = older)
    if keywords['oldest'] and 'born' in q_lower:
        if re.search(r'sort\s+\$\w*born\w*\s+desc', typeql):
            return False, "Question asks for 'oldest' people but TypeQL sorts born DESC instead of ASC (older = lower birth year)"

    # Check: youngest born should have desc sort (larger year = younger)
    if keywords['youngest'] and 'born' in q_lower:
        if re.search(r'sort\s+\$\w*born\w*\s+asc', typeql):
            return False, "Question asks for 'youngest' people but TypeQL sorts born ASC instead of DESC (younger = higher birth year)"

    # Check: most recent should sort desc
    if keywords['most_recent']:
        if typeql_rels['has_sort_asc'] and not typeql_rels['has_sort_desc']:
            # Only flag if sorting by a date/year field
            if re.search(r'sort\s+\$\w*(released|year|date)\w*\s+asc', typeql):
                return False, "Question asks for 'most recent/latest' but TypeQL sorts ASC instead of DESC"

    # === AGGREGATION CHECKS ===

    # Check: explicit count questions should have reduce/count
    if keywords['count_result']:
        if not typeql_rels['has_reduce'] and not typeql_rels['has_count']:
            return False, "Question asks for count/number but TypeQL doesn't use reduce/count"

    # === MUTUAL FOLLOWS CHECK ===

    if keywords['mutual_follow']:
        # Should have two follows relations
        follows_count = len(re.findall(r'isa\s+follows', typeql))
        if follows_count < 2:
            return False, "Question asks about mutual follows but TypeQL only has one follows relation (needs two for mutual)"

    # === NEGATION CHECK ===

    # Only check for actual NOT patterns, not IS NOT NULL
    if cypher_rels['cypher_not_pattern'] and not typeql_rels['has_not']:
        return False, "Cypher uses NOT/NOT EXISTS pattern but TypeQL doesn't have negation"

    # === STRING FILTER CHECKS ===

    # Check: contains in cypher but not in typeql
    if 'CONTAINS' in cypher.upper() and not typeql_rels['has_contains']:
        return False, "Cypher uses CONTAINS but TypeQL doesn't use contains"

    # Check: STARTS WITH / ENDS WITH
    if 'STARTS WITH' in cypher.upper() and not typeql_rels['has_like']:
        return False, "Cypher uses STARTS WITH but TypeQL doesn't use like pattern"

    if 'ENDS WITH' in cypher.upper() and not typeql_rels['has_like']:
        return False, "Cypher uses ENDS WITH but TypeQL doesn't use like pattern"

    # === ADDITIONAL SEMANTIC CHECKS ===

    # Check: Question asks about directors but query only has acted_in
    if keywords['directed'] and not keywords['acted']:
        if typeql_rels['has_acted_in'] and not typeql_rels['has_directed']:
            return False, "Question asks about directors but TypeQL only has acted_in relation"

    # Check: Question asks about actors but query only has directed
    if keywords['acted'] and not keywords['directed']:
        if typeql_rels['has_directed'] and not typeql_rels['has_acted_in']:
            return False, "Question asks about actors but TypeQL only has directed relation"

    # Check: Question asks about producers but query has wrong relation
    if keywords['produced'] and not keywords['directed'] and not keywords['acted']:
        if not typeql_rels['has_produced'] and (typeql_rels['has_acted_in'] or typeql_rels['has_directed']):
            return False, "Question asks about producers but TypeQL has wrong relation type"

    # Check: Question asks about writers but query has wrong relation
    if keywords['wrote'] and not keywords['directed'] and not keywords['acted']:
        if not typeql_rels['has_wrote'] and (typeql_rels['has_acted_in'] or typeql_rels['has_directed']):
            return False, "Question asks about writers but TypeQL has wrong relation type"

    # All checks passed
    return True, None


def main():
    """Main function to run semantic review on all queries."""

    base_path = Path("/opt/text2typeql/output/movies")
    queries_path = base_path / "queries.csv"
    failed_review_path = base_path / "failed_review.csv"

    # First, restore original queries.csv if needed (check if we have backup)
    backup_path = base_path / "queries_original.csv"
    if not backup_path.exists():
        # Create backup of original
        import shutil
        # We need to reload from the original since we may have already run this
        print("Note: No backup found, using current queries.csv")

    print(f"Loading queries from {queries_path}...")
    queries = load_queries(queries_path)
    print(f"Loaded {len(queries)} queries")

    passed = []
    failed = []

    for query in queries:
        original_index = query['original_index']
        question = query['question']
        cypher = query['cypher']
        typeql = query['typeql']

        is_valid, reason = semantic_review(question, cypher, typeql)

        if is_valid:
            passed.append(query)
        else:
            failed.append({
                'original_index': original_index,
                'question': question,
                'cypher': cypher,
                'typeql': typeql,
                'review_reason': reason
            })

    print(f"\n=== SEMANTIC REVIEW RESULTS ===")
    print(f"Passed: {len(passed)}")
    print(f"Failed: {len(failed)}")

    # Write passed queries back to queries.csv
    print(f"\nWriting {len(passed)} passed queries to {queries_path}...")
    with open(queries_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
        writer.writeheader()
        for query in passed:
            writer.writerow(query)

    # Write failed queries to failed_review.csv
    print(f"Writing {len(failed)} failed queries to {failed_review_path}...")
    with open(failed_review_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql', 'review_reason'])
        writer.writeheader()
        for query in failed:
            writer.writerow(query)

    # Print failure breakdown
    print(f"\n=== FAILURE BREAKDOWN ===")
    failure_reasons = {}
    for query in failed:
        reason = query['review_reason']
        # Extract the main reason type
        reason_type = reason.split(' - ')[0] if ' - ' in reason else reason.split(' but ')[0]
        failure_reasons[reason_type] = failure_reasons.get(reason_type, 0) + 1

    for reason, count in sorted(failure_reasons.items(), key=lambda x: -x[1]):
        print(f"  {reason}: {count}")

    # Print some example failures
    print(f"\n=== SAMPLE FAILURES ===")
    for query in failed[:10]:
        print(f"\n--- Query {query['original_index']} ---")
        print(f"Question: {query['question'][:100]}...")
        print(f"Reason: {query['review_reason']}")


if __name__ == "__main__":
    main()
