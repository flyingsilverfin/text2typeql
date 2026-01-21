#!/usr/bin/env python3
"""
Fix the 30 failed semantic review queries in the gameofthrones database.
This script adds the fixed queries back to queries.csv.
"""

import csv


def get_fixed_query(original_index: int, question: str, cypher: str, typeql: str, issues: str) -> str:
    """Fix a TypeQL query based on the review issues."""

    # Issue: wrong_sort_direction - Change desc to asc for "lowest" queries
    if "wrong_sort_direction" in issues:
        # Cypher uses ASC (default) but TypeQL has DESC - change to ASC
        fixed = typeql.replace("sort $c_pagerank desc;", "sort $c_pagerank asc;")
        fixed = fixed.replace("sort $c_book1_betweenness_centrality desc;", "sort $c_book1_betweenness_centrality asc;")
        fixed = fixed.replace("sort $c_book1_page_rank desc;", "sort $c_book1_page_rank asc;")
        fixed = fixed.replace("sort $rel_weight desc;", "sort $rel_weight asc;")
        fixed = fixed.replace("sort $c_degree desc;", "sort $c_degree asc;")
        fixed = fixed.replace("sort $c_book45_page_rank desc;", "sort $c_book45_page_rank asc;")
        return fixed

    # Issue: missing_relation (index 43, 130) - Add INTERACTS relation and target character
    if "missing_relation" in issues:
        if original_index == 43:
            # Who interacted with 'Aegon-Frey-(son-of-Stevron)'? List first 3 characters.
            return '''match
  $c isa character;
  $target isa character, has name "Aegon-Frey-(son-of-Stevron)";
  (character1: $c, character2: $target) isa interacts;
limit 3;
fetch { "c_name": $c.name };'''

        if original_index == 130:
            # List the characters that interact in book 45 with any character named 'Aegon-I-Targaryen'.
            return '''match
  $c1 isa character;
  $target isa character, has name "Aegon-I-Targaryen";
  (character1: $c1, character2: $target) isa interacts45;
fetch { "character": $c1.name };'''

    # Issue: missing_target_character - Add character name filter
    if "missing_target_character" in issues:
        if original_index == 49:
            # Find the characters who have interacted with 'Roose-Bolton' in 'book 1'. List the top 5.
            return '''match
  $c isa character;
  $target isa character, has name "Roose-Bolton";
  (character1: $c, character2: $target) isa interacts1, has weight $i_weight;
sort $i_weight desc;
limit 5;
fetch { "character": $c.name, "interactions": $i_weight };'''

        if original_index == 68:
            # Who interacted with 'Walder-Rivers' in 'book 45'? List first 3 characters.
            return '''match
  $c isa character;
  $target isa character, has name "Walder-Rivers";
  (character1: $c, character2: $target) isa interacts45;
limit 3;
fetch { "c_name": $c.name };'''

        if original_index == 73:
            # Find the characters who have interacted with 'Murenmure' in 'book 45'. List the top 5.
            return '''match
  $c isa character;
  $target isa character, has name "Murenmure";
  (character1: $c, character2: $target) isa interacts45, has weight $i_weight;
sort $i_weight desc;
limit 5;
fetch { "character": $c.name, "interactions": $i_weight };'''

        if original_index == 92:
            # Who interacted with 'Aeron-Greyjoy' in 'book 45'? List first 3 characters.
            return '''match
  $c isa character;
  $target isa character, has name "Aeron-Greyjoy";
  (character1: $c, character2: $target) isa interacts45;
limit 3;
fetch { "c_name": $c.name };'''

        if original_index == 157:
            # List the characters whose name contains 'Greyjoy'.
            return '''match
  $c isa character, has name $c_name;
  $c_name contains "Greyjoy";
fetch { "c_name": $c_name };'''

    # Issue: missing_in_filter - Add IN clause values with or blocks
    if "missing_in_filter" in issues:
        if original_index == 212:
            # Find characters in the top 3 louvain communities by centrality.
            # WHERE c.louvain IN [0, 1, 2]
            return '''match
  $c isa character, has louvain $c_louvain, has centrality $c_centrality;
  { $c_louvain == 0; } or { $c_louvain == 1; } or { $c_louvain == 2; };
sort $c_centrality desc;
fetch { "character": $c.name, "community": $c_louvain, "c_centrality": $c_centrality };'''

        if original_index == 353:
            # List the characters who are part of both community 578 and 579. Limit to 5 characters.
            # WHERE c.community IN [578, 579]
            return '''match
  $c isa character, has community $c_community;
  { $c_community == 578; } or { $c_community == 579; };
limit 5;
fetch { "c_name": $c.name };'''

    # Issue: missing_sort, missing_aggregation, missing_weight_attribute
    # These need aggregation with weight
    if "missing_aggregation" in issues or "missing_weight_attribute" in issues:
        if original_index == 296:
            # Who are the top 3 characters with the highest weight in INTERACTS relationships?
            # sum(i.weight) AS total_weight ORDER BY total_weight DESC
            return '''match
  $c isa character;
  $other isa character;
  (character1: $c, character2: $other) isa interacts, has weight $w;
reduce $total_weight = sum($w) groupby $c;
match $c has name $c_name;
sort $total_weight desc;
limit 3;
fetch { "character": $c_name, "total_weight": $total_weight };'''

        if original_index == 308:
            # Who are the top 3 characters with the highest weight in INTERACTS1 relationships?
            # max(i.weight) AS max_weight ORDER BY max_weight DESC
            return '''match
  $c isa character;
  $other isa character;
  (character1: $c, character2: $other) isa interacts1, has weight $w;
reduce $max_weight = max($w) groupby $c;
match $c has name $c_name;
sort $max_weight desc;
limit 3;
fetch { "character": $c_name, "max_weight": $max_weight };'''

        if original_index == 317:
            # Who are the top 5 characters with the highest weight in INTERACTS2 relationships?
            # max(i.weight) AS max_weight ORDER BY max_weight DESC
            return '''match
  $c isa character;
  $other isa character;
  (character1: $c, character2: $other) isa interacts2, has weight $w;
reduce $max_weight = max($w) groupby $c;
match $c has name $c_name;
sort $max_weight desc;
limit 5;
fetch { "character": $c_name, "max_weight": $max_weight };'''

        if original_index == 323:
            # Who are the top 5 characters with the lowest weight in INTERACTS3 relationships?
            # min(i.weight) AS minWeight ORDER BY minWeight (ASC)
            return '''match
  $c isa character;
  $other isa character;
  (character1: $c, character2: $other) isa interacts3, has weight $w;
reduce $min_weight = min($w) groupby $c;
match $c has name $c_name;
sort $min_weight asc;
limit 5;
fetch { "c_name": $c_name, "min_weight": $min_weight };'''

        if original_index == 329:
            # Who are the top 5 characters with the highest weight in INTERACTS45 relationships?
            # sum(i.weight) AS total_weight ORDER BY total_weight DESC
            return '''match
  $c isa character;
  $other isa character;
  (character1: $c, character2: $other) isa interacts45, has weight $w;
reduce $total_weight = sum($w) groupby $c;
match $c has name $c_name;
sort $total_weight desc;
limit 5;
fetch { "character": $c_name, "total_weight": $total_weight };'''

        if original_index == 339:
            # Find the top 5 characters with the highest weight in INTERACTS relationship.
            # sum(i.weight) AS total_weight ORDER BY total_weight DESC
            return '''match
  $c isa character;
  $other isa character;
  (character1: $c, character2: $other) isa interacts, has weight $w;
reduce $total_weight = sum($w) groupby $c;
match $c has name $c_name;
sort $total_weight desc;
limit 5;
fetch { "character": $c_name, "total_weight": $total_weight };'''

        if original_index == 355:
            # Who are the top 5 characters in terms of INTERACTS45 weight?
            # sum(i.weight) AS total_weight ORDER BY total_weight DESC
            return '''match
  $c isa character;
  $other isa character;
  (character1: $c, character2: $other) isa interacts45, has weight $w;
reduce $total_weight = sum($w) groupby $c;
match $c has name $c_name;
sort $total_weight desc;
limit 5;
fetch { "character": $c_name, "total_weight": $total_weight };'''

        if original_index == 369:
            # Identify the top 5 characters with the most interactions in book 3.
            # sum(i.weight) AS total_interactions ORDER BY total_interactions DESC
            return '''match
  $c isa character;
  $other isa character;
  (character1: $c, character2: $other) isa interacts3, has weight $w;
reduce $total_interactions = sum($w) groupby $c;
match $c has name $c_name;
sort $total_interactions desc;
limit 5;
fetch { "character": $c_name, "total_interactions": $total_interactions };'''

        if original_index == 391:
            # List the top 3 characters with the highest INTERACTS3 relationship weight.
            # max(i.weight) AS max_weight ORDER BY max_weight DESC
            return '''match
  $c isa character;
  $other isa character;
  (character1: $c, character2: $other) isa interacts3, has weight $w;
reduce $max_weight = max($w) groupby $c;
match $c has name $c_name;
sort $max_weight desc;
limit 3;
fetch { "character": $c_name, "max_weight": $max_weight };'''

    return typeql


def main():
    # Read the failed review CSV
    failed_path = "/opt/text2typeql/output/gameofthrones/failed_review.csv"
    queries_path = "/opt/text2typeql/output/gameofthrones/queries.csv"

    # Read all failed queries
    failed_queries = []
    with open(failed_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            failed_queries.append(row)

    print(f"Found {len(failed_queries)} failed queries to fix")

    # Read the existing queries CSV
    existing_rows = []
    existing_indices = set()
    with open(queries_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            existing_rows.append(row)
            existing_indices.add(int(row['original_index']))

    print(f"Found {len(existing_rows)} existing queries (indices from {min(existing_indices)} to {max(existing_indices)})")

    # Process failed queries and add them back
    fixed_count = 0
    added_count = 0
    for row in failed_queries:
        original_index = int(row['original_index'])
        question = row['question']
        cypher = row['cypher']
        typeql = row['typeql']
        issues = row['issues']

        fixed = get_fixed_query(original_index, question, cypher, typeql, issues)

        # Debug: show what changed
        if fixed != typeql:
            print(f"\n--- Fixed query {original_index} ---")
            print(f"Issues: {issues}")
            print(f"Fixed TypeQL:\n{fixed}")
            fixed_count += 1

        # Add the fixed query
        if original_index not in existing_indices:
            existing_rows.append({
                'original_index': str(original_index),
                'question': question,
                'cypher': cypher,
                'typeql': fixed
            })
            added_count += 1
        else:
            # Update existing entry
            for r in existing_rows:
                if int(r['original_index']) == original_index:
                    r['typeql'] = fixed
                    break

    # Sort by original_index
    existing_rows.sort(key=lambda r: int(r['original_index']))

    # Write back the updated queries
    with open(queries_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(existing_rows)

    print(f"\nFixed {fixed_count} queries")
    print(f"Added {added_count} queries back to {queries_path}")
    print(f"Total queries now: {len(existing_rows)}")


if __name__ == "__main__":
    main()
