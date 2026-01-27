#!/usr/bin/env python3
"""
Final comprehensive semantic check for companies queries.
Focus on queries where TypeQL doesn't match question intent.
"""

import csv
import re
import json

def check_semantic_match(idx, question, cypher, typeql):
    """
    Check if TypeQL correctly implements the question.
    Return (is_correct, issue_description) or (True, None) if correct.
    """
    q_lower = question.lower()
    t_lower = typeql.lower()
    c_lower = cypher.lower()

    # === SEVERE MISMATCH CHECKS ===

    # 1. TypeQL references wrong person
    if 'julie spellman sweet' in t_lower:
        if 'julie spellman sweet' not in q_lower:
            return False, "TypeQL references 'Julie Spellman Sweet' but question doesn't mention this person - completely wrong query"

    # 2. TypeQL uses completely wrong entity/relation
    # Check if key entities from question are missing

    # Supplier checks
    if 'supplier' in q_lower and 'has_supplier' in c_lower:
        if 'supplies' not in t_lower:
            return False, "Question asks about suppliers but TypeQL lacks supplies relation"

    # Board member checks
    if 'board member' in q_lower and 'has_board_member' in c_lower:
        if 'board_member_of' not in t_lower:
            return False, "Question asks about board members but TypeQL lacks board_member_of relation"

    # Investor checks
    if 'investor' in q_lower and ('has_investor' in c_lower or 'invested' in c_lower):
        if 'invested_in' not in t_lower:
            return False, "Question asks about investors but TypeQL lacks invested_in relation"

    # CEO checks
    if 'ceo' in q_lower and 'has_ceo' in c_lower:
        if 'ceo_of' not in t_lower:
            return False, "Question asks about CEOs but TypeQL lacks ceo_of relation"

    # Parent/child (person) checks
    if ('child' in q_lower or 'parent' in q_lower) and 'has_child' in c_lower:
        if 'parent_of' not in t_lower:
            return False, "Question asks about parent/child relations but TypeQL lacks parent_of relation"

    # Competitor checks
    if 'competitor' in q_lower and 'has_competitor' in c_lower:
        if 'competes_with' not in t_lower:
            return False, "Question asks about competitors but TypeQL lacks competes_with relation"

    # Subsidiary checks
    if ('subsidiary' in q_lower or 'subsidiaries' in q_lower) and ('has_subsidiary' in c_lower):
        if 'subsidiary_of' not in t_lower:
            return False, "Question asks about subsidiaries but TypeQL lacks subsidiary_of relation"

    # Article/mentions checks
    if ('article' in q_lower or 'mention' in q_lower) and ':mentions' in c_lower:
        if 'mentions' not in t_lower and 'isa article' not in t_lower:
            return False, "Question asks about articles/mentions but TypeQL lacks article entity or mentions relation"

    # City/location checks
    if 'city' in q_lower or 'cities' in q_lower:
        if ':city' in c_lower or 'in_city' in c_lower:
            if 'isa city' not in t_lower and 'city_name' not in t_lower:
                return False, "Question asks about cities but TypeQL lacks city entity"

    # Country checks
    if 'country' in q_lower or 'countries' in q_lower:
        if ':country' in c_lower or 'in_country' in c_lower:
            if 'isa country' not in t_lower and 'country_name' not in t_lower:
                return False, "Question asks about countries but TypeQL lacks country entity"

    # Dissolved status checks
    if 'dissolved' in q_lower and 'isdissolved' in c_lower:
        if 'is_dissolved' not in t_lower:
            return False, "Question asks about dissolved status but TypeQL lacks is_dissolved attribute"

    # Public status checks
    if 'public' in q_lower and 'ispublic' in c_lower:
        if 'is_public' not in t_lower:
            return False, "Question asks about public status but TypeQL lacks is_public attribute"

    # Industry category checks
    if 'industry' in q_lower and 'has_category' in c_lower:
        if 'in_category' not in t_lower and 'industry_category' not in t_lower:
            return False, "Question asks about industry but TypeQL lacks in_category relation"

    # Location checks (organization in city)
    if 'in_city' in c_lower or 'based in' in q_lower or 'located in' in q_lower:
        if 'located_in' not in t_lower:
            # Check if TypeQL has any location pattern
            if 'isa city' not in t_lower and 'city_name' not in t_lower:
                return False, "Question asks about organization location but TypeQL lacks located_in relation"

    # === AGGREGATION MISMATCH CHECKS ===

    # Count aggregation checks - only flag if missing reduce AND question asks for count
    if 'count(' in c_lower:
        # Check if question explicitly asks for count/number
        count_words = ['how many', 'number of', 'count']
        needs_count = any(w in q_lower for w in count_words)

        # Also check for "most" queries that require aggregation
        if ('most' in q_lower or 'top' in q_lower) and 'order by' in c_lower:
            needs_count = True

        if needs_count and 'reduce' not in t_lower and 'count' not in t_lower:
            # Check if TypeQL has workaround (self-join pattern)
            if not ('$o1' in t_lower and '$o2' in t_lower and '!=' in t_lower):
                return False, "Question asks for count/most but TypeQL lacks aggregation (reduce/count)"

    # === STRUCTURAL CHECKS ===

    # Check for completely mismatched query structure
    # If Cypher matches person but TypeQL matches organization (or vice versa) when not appropriate

    # Organization in question but only person in TypeQL
    if 'organization' in q_lower or 'company' in q_lower or 'companies' in q_lower:
        if 'isa organization' not in t_lower and '$o' not in t_lower:
            if 'isa person' in t_lower:
                # Check if the question is actually about persons related to organizations
                person_words = ['ceo', 'board member', 'investor', 'person']
                if not any(w in q_lower for w in person_words):
                    return False, "Question asks about organizations but TypeQL only matches persons"

    # === RETURN/FETCH MISMATCH CHECKS ===

    # If question asks for specific data but TypeQL returns something else
    # This is complex to automate reliably, skip for now

    return True, None


def main():
    # Read queries
    with open('/opt/text2typeql/dataset/companies/queries.csv', 'r') as f:
        reader = csv.DictReader(f)
        queries = list(reader)

    print(f"Analyzing {len(queries)} queries...")

    issues = []
    passed = []

    for row in queries:
        idx = int(row['original_index'])
        is_correct, issue = check_semantic_match(
            idx,
            row['question'],
            row['cypher'],
            row['typeql']
        )

        if not is_correct:
            issues.append({
                'index': idx,
                'question': row['question'],
                'issue': issue
            })
        else:
            passed.append(idx)

    # Print results
    print(f"\n{'='*60}")
    print("FINAL SEMANTIC REVIEW RESULTS")
    print(f"{'='*60}")
    print(f"Total queries: {len(queries)}")
    print(f"Passed: {len(passed)}")
    print(f"Failed: {len(issues)}")

    # Categorize issues
    categories = {}
    for item in issues:
        cat = item['issue'].split(' - ')[0] if ' - ' in item['issue'] else item['issue'].split(' but ')[0]
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(item['index'])

    print(f"\n--- ISSUE CATEGORIES ---")
    for cat, indices in sorted(categories.items(), key=lambda x: -len(x[1])):
        print(f"{len(indices):3} - {cat}")
        print(f"    Indices: {indices}")

    # Save results
    with open('/tmp/companies_final_review.json', 'w') as f:
        json.dump({
            'total': len(queries),
            'passed': len(passed),
            'failed': len(issues),
            'failed_indices': [i['index'] for i in issues],
            'issues': issues,
            'categories': categories
        }, f, indent=2)

    print(f"\nResults saved to /tmp/companies_final_review.json")

    # Print all failed indices
    print(f"\n--- ALL FAILED INDICES ({len(issues)}) ---")
    failed_indices = sorted([i['index'] for i in issues])
    print(failed_indices)


if __name__ == '__main__':
    main()
