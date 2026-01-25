#!/usr/bin/env python3
"""Deep semantic review of companies queries - check if TypeQL matches question intent."""

import csv
import json
import re

def analyze_query_match(idx, question, cypher, typeql):
    """Analyze if TypeQL correctly implements the question's intent."""
    issues = []
    question_lower = question.lower()
    typeql_lower = typeql.lower()
    cypher_lower = cypher.lower()

    # === CRITICAL SEMANTIC CHECKS ===

    # 1. Check if TypeQL references completely different entities than asked
    # Look for "Julie Spellman Sweet" in TypeQL when question doesn't mention her
    if 'julie spellman sweet' in typeql_lower and 'julie spellman sweet' not in question_lower:
        issues.append(f"TypeQL references 'Julie Spellman Sweet' but question doesn't mention this person")

    # 2. Check for mismatched query patterns
    # If question asks about suppliers but TypeQL doesn't have supplies relation
    if ('supplier' in question_lower or 'supplies' in question_lower) and 'supplies' not in typeql_lower:
        # Check if it's actually about supplies in the Cypher
        if 'has_supplier' in cypher_lower:
            issues.append(f"Question asks about suppliers (HAS_SUPPLIER in Cypher) but TypeQL lacks supplies relation")

    # 3. Check for organizations vs persons mismatch
    # If question asks for organizations but TypeQL only matches persons
    if ('organization' in question_lower or 'company' in question_lower or 'companies' in question_lower):
        if 'isa organization' not in typeql_lower and 'isa person' in typeql_lower:
            # Check if it should have organization
            if 'organization' in cypher_lower.replace('(', ' ').split():
                issues.append(f"Question asks about organizations but TypeQL only matches persons")

    # 4. Check for aggregation requirements
    # Cypher WITH ... count ... ORDER BY
    if 'count(' in cypher_lower and 'with' in cypher_lower:
        # This usually requires aggregation
        if 'reduce' not in typeql_lower and 'count' not in typeql_lower:
            issues.append(f"Cypher uses COUNT aggregation but TypeQL lacks reduce/count")

    # 5. Check for HAVING equivalent (filtering on aggregation)
    if re.search(r'with\s+\w+.*count.*where', cypher_lower, re.DOTALL):
        issues.append(f"Cypher uses WITH...COUNT...WHERE (HAVING equivalent) - complex aggregation")

    # 6. Check for proper sorting with 'most'/'top' queries
    if ('most' in question_lower or 'top' in question_lower or 'highest' in question_lower):
        if 'count(' in cypher_lower and 'order by' in cypher_lower:
            if 'reduce' not in typeql_lower:
                issues.append(f"Question asks for top/most with COUNT but TypeQL lacks aggregation")

    # 7. Check city vs country mismatches
    if 'cities' in question_lower and 'isa city' not in typeql_lower:
        if ':city' in cypher_lower:
            issues.append(f"Question asks about cities but TypeQL lacks city entity")

    if 'country' in question_lower or 'countries' in question_lower:
        if 'isa country' not in typeql_lower and 'country_name' not in typeql_lower:
            if ':country' in cypher_lower:
                issues.append(f"Question asks about countries but TypeQL lacks country entity")

    # 8. Check competitor relation
    if 'competitor' in question_lower:
        if 'competes_with' not in typeql_lower:
            if 'has_competitor' in cypher_lower:
                issues.append(f"Question asks about competitors but TypeQL lacks competes_with relation")

    # 9. Check investor/invested relation
    if 'investor' in question_lower or 'invested' in question_lower:
        if 'invested_in' not in typeql_lower:
            if 'has_investor' in cypher_lower or 'invested' in cypher_lower:
                issues.append(f"Question asks about investors but TypeQL lacks invested_in relation")

    # 10. Check board member relation
    if 'board member' in question_lower or 'board_member' in question_lower:
        if 'board_member_of' not in typeql_lower:
            if 'has_board_member' in cypher_lower:
                issues.append(f"Question asks about board members but TypeQL lacks board_member_of relation")

    # 11. Check CEO relation
    if 'ceo' in question_lower:
        if 'ceo_of' not in typeql_lower:
            if 'has_ceo' in cypher_lower:
                issues.append(f"Question asks about CEOs but TypeQL lacks ceo_of relation")

    # 12. Check parent/child person relations
    if 'parent' in question_lower or 'child' in question_lower or 'children' in question_lower:
        if 'person' in cypher_lower and 'has_child' in cypher_lower:
            if 'parent_of' not in typeql_lower:
                issues.append(f"Question asks about parent/child relations but TypeQL lacks parent_of")

    # 13. Check subsidiary relation
    if 'subsidiary' in question_lower or 'subsidiaries' in question_lower:
        if 'subsidiary_of' not in typeql_lower:
            if 'has_subsidiary' in cypher_lower or 'subsidiary' in cypher_lower:
                issues.append(f"Question asks about subsidiaries but TypeQL lacks subsidiary_of relation")

    # 14. Check location patterns
    if 'in city' in question_lower or 'in_city' in cypher_lower:
        if 'located_in' not in typeql_lower:
            issues.append(f"Question asks about city location but TypeQL lacks located_in relation")

    # 15. Check article/mentions patterns
    if 'article' in question_lower or 'mention' in question_lower:
        if 'article' in cypher_lower and 'mentions' in cypher_lower:
            if 'mentions' not in typeql_lower and 'isa article' not in typeql_lower:
                issues.append(f"Question asks about articles/mentions but TypeQL lacks mentions relation")

    # 16. Check OPTIONAL MATCH handling
    if 'optional match' in cypher_lower:
        if 'try {' not in typeql_lower and 'or {' not in typeql_lower:
            issues.append(f"Cypher uses OPTIONAL MATCH but TypeQL lacks try/or blocks")

    # 17. Check for collect() / list aggregation
    if 'collect(' in cypher_lower:
        issues.append(f"Cypher uses collect() which may need TypeQL array fetch")

    # 18. Check dissolved queries
    if 'dissolved' in question_lower:
        if 'is_dissolved' not in typeql_lower:
            if 'isdissolved' in cypher_lower:
                issues.append(f"Question asks about dissolved status but TypeQL lacks is_dissolved")

    # 19. Check public queries
    if 'public' in question_lower:
        if 'is_public' not in typeql_lower:
            if 'ispublic' in cypher_lower:
                issues.append(f"Question asks about public status but TypeQL lacks is_public")

    # 20. Check distinct/unique - TypeQL fetch usually handles this

    # 21. Check negation patterns
    if 'not ' in question_lower or "don't" in question_lower or "doesn't" in question_lower:
        # Check if it's a boolean check (handled by `has attr false`)
        if 'not o.is' in cypher_lower or 'not p.is' in cypher_lower:
            # Boolean negation, should use `has attr false`
            pass
        elif 'where not exists' in cypher_lower or 'where not (' in cypher_lower:
            if 'not {' not in typeql_lower:
                issues.append(f"Cypher uses WHERE NOT EXISTS but TypeQL lacks 'not {{ }}' block")

    # 22. Check EXISTS patterns
    if 'exists {' in cypher_lower or 'exists(' in cypher_lower:
        # EXISTS { pattern } should be present in TypeQL match
        pass  # Usually handled by including the pattern

    return issues


def main():
    # Read queries
    with open('/opt/text2typeql/output/companies/queries.csv', 'r') as f:
        reader = csv.DictReader(f)
        queries = list(reader)

    print(f"Analyzing {len(queries)} queries for semantic issues...")

    all_issues = []
    passed_count = 0

    for row in queries:
        idx = int(row['original_index'])
        question = row['question']
        cypher = row['cypher']
        typeql = row['typeql']

        issues = analyze_query_match(idx, question, cypher, typeql)

        if issues:
            all_issues.append({
                'index': idx,
                'question': question,
                'cypher': cypher[:500],
                'typeql': typeql[:500],
                'issues': issues
            })
        else:
            passed_count += 1

    # Print summary
    print(f"\n{'='*60}")
    print(f"SEMANTIC REVIEW RESULTS")
    print(f"{'='*60}")
    print(f"Total queries: {len(queries)}")
    print(f"Passed semantic review: {passed_count}")
    print(f"Queries with issues: {len(all_issues)}")

    # Categorize issues
    issue_categories = {}
    for item in all_issues:
        for issue in item['issues']:
            # Extract category (first part before specific details)
            cat = issue.split(' but ')[0] if ' but ' in issue else issue.split(' - ')[0]
            if cat not in issue_categories:
                issue_categories[cat] = []
            issue_categories[cat].append(item['index'])

    print(f"\n--- ISSUE CATEGORIES ---")
    for cat, indices in sorted(issue_categories.items(), key=lambda x: -len(x[1])):
        print(f"{len(indices):3} - {cat}")
        if len(indices) <= 10:
            print(f"    Indices: {indices}")
        else:
            print(f"    Indices: {indices[:10]}... (+{len(indices)-10} more)")

    # Print detailed issues
    print(f"\n--- DETAILED ISSUES ---")
    for item in all_issues[:50]:  # Show first 50
        print(f"\nIndex {item['index']}:")
        print(f"  Question: {item['question'][:100]}...")
        for issue in item['issues']:
            print(f"  ISSUE: {issue}")

    if len(all_issues) > 50:
        print(f"\n... and {len(all_issues) - 50} more queries with issues")

    # Save full results
    with open('/tmp/companies_semantic_issues.json', 'w') as f:
        json.dump({
            'total': len(queries),
            'passed': passed_count,
            'failed': len(all_issues),
            'issues': all_issues,
            'categories': {k: v for k, v in issue_categories.items()}
        }, f, indent=2)

    print(f"\nFull results saved to /tmp/companies_semantic_issues.json")

    # Print indices with issues for easy reference
    print(f"\n--- ALL INDICES WITH ISSUES ---")
    failed_indices = [item['index'] for item in all_issues]
    print(f"Indices: {failed_indices}")


if __name__ == '__main__':
    main()
