#!/usr/bin/env python3
"""Fix gameofthrones queries that failed semantic review."""

import csv
import sys
sys.path.insert(0, '/opt/text2typeql')

from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# Category 1: Sort direction fixes - change desc to asc
SORT_DIRECTION_FIXES = {
    52: ('sort $c_book1_betweenness_centrality desc;', 'sort $c_book1_betweenness_centrality asc;'),
    53: ('sort $c_pagerank desc;', 'sort $c_pagerank asc;'),
    76: ('sort $c_book1_betweenness_centrality desc;', 'sort $c_book1_betweenness_centrality asc;'),
    77: ('sort $c_pagerank desc;', 'sort $c_pagerank asc;'),
    213: ('sort $c_book1_page_rank desc;', 'sort $c_book1_page_rank asc;'),
    267: ('sort $rel_weight desc;', 'sort $rel_weight asc;'),
    277: ('sort $c_pagerank desc;', 'sort $c_pagerank asc;'),
    290: ('sort $c_degree desc;', 'sort $c_degree asc;'),
    302: ('sort $c_pagerank desc;', 'sort $c_pagerank asc;'),
    342: ('sort $c_book1_page_rank desc;', 'sort $c_book1_page_rank asc;'),
    358: ('sort $c_degree desc;', 'sort $c_degree asc;'),
    381: ('sort $c_book45_page_rank desc;', 'sort $c_book45_page_rank asc;'),
}

# Category 2: Missing INTERACTS relation - provide complete fixed TypeQL
INTERACTS_FIXES = {
    43: """match
  $c isa character, has name $c_name;
  $target isa character, has name "Aegon-Frey-(son-of-Stevron)";
  (character1: $c, character2: $target) isa interacts;
limit 3;
fetch { "c_name": $c_name };""",

    49: """match
  $c isa character, has name $c_name;
  $target isa character, has name "Roose-Bolton";
  (character1: $c, character2: $target) isa interacts1, has weight $i_weight;
sort $i_weight desc;
limit 5;
fetch { "character": $c_name, "interactions": $i_weight };""",

    68: """match
  $c isa character, has name $c_name;
  $target isa character, has name "Walder-Rivers";
  (character1: $c, character2: $target) isa interacts45;
limit 3;
fetch { "c_name": $c_name };""",

    73: """match
  $c isa character, has name $c_name;
  $target isa character, has name "Murenmure";
  (character1: $c, character2: $target) isa interacts45, has weight $i_weight;
sort $i_weight desc;
limit 5;
fetch { "character": $c_name, "interactions": $i_weight };""",

    92: """match
  $c isa character, has name $c_name;
  $target isa character, has name "Aeron-Greyjoy";
  (character1: $c, character2: $target) isa interacts45;
limit 3;
fetch { "c_name": $c_name };""",

    130: """match
  $c1 isa character, has name $c1_name;
  $target isa character, has name "Aegon-I-Targaryen";
  (character1: $c1, character2: $target) isa interacts45;
fetch { "character": $c1_name };""",
}

# Category 3: Missing IN filter - use or conditions
IN_FILTER_FIXES = {
    212: """match
  $c isa character, has louvain $c_louvain, has centrality $c_centrality;
  { $c_louvain == 0; } or { $c_louvain == 1; } or { $c_louvain == 2; };
sort $c_centrality desc;
fetch { "character": $c.name, "community": $c_louvain, "c_centrality": $c_centrality };""",

    353: """match
  $c isa character, has community $c_community, has name $c_name;
  { $c_community == 578; } or { $c_community == 579; };
limit 5;
fetch { "c_name": $c_name };""",
}

# Category 4: Missing CONTAINS filter - use like
CONTAINS_FIXES = {
    157: """match
  $c isa character, has name $c_name;
  $c_name like ".*Greyjoy.*";
fetch { "c_name": $c_name };""",
}

# Category 5: Missing aggregation - need chained reduce pattern
# Pattern: reduce ... groupby $c; match $c has name $n; sort; limit; fetch
AGGREGATION_FIXES = {
    296: """match
  $c isa character;
  (character1: $c, character2: $other) isa interacts, has weight $w;
reduce $total_weight = sum($w) groupby $c;
match $c has name $n;
sort $total_weight desc;
limit 3;
fetch { "character": $n, "total_weight": $total_weight };""",

    308: """match
  $c isa character;
  (character1: $c, character2: $other) isa interacts1, has weight $w;
reduce $max_weight = max($w) groupby $c;
match $c has name $n;
sort $max_weight desc;
limit 3;
fetch { "character": $n, "max_weight": $max_weight };""",

    317: """match
  $c isa character;
  (character1: $c, character2: $other) isa interacts2, has weight $w;
reduce $max_weight = max($w) groupby $c;
match $c has name $n;
sort $max_weight desc;
limit 5;
fetch { "character": $n, "max_weight": $max_weight };""",

    323: """match
  $c isa character;
  (character1: $c, character2: $other) isa interacts3, has weight $w;
reduce $min_weight = min($w) groupby $c;
match $c has name $n;
sort $min_weight asc;
limit 5;
fetch { "c_name": $n, "min_weight": $min_weight };""",

    339: """match
  $c isa character;
  (character1: $c, character2: $other) isa interacts, has weight $w;
reduce $total_weight = sum($w) groupby $c;
match $c has name $n;
sort $total_weight desc;
limit 5;
fetch { "character": $n, "total_weight": $total_weight };""",

    355: """match
  $c isa character;
  (character1: $c, character2: $other) isa interacts45, has weight $w;
reduce $total_weight = sum($w) groupby $c;
match $c has name $n;
sort $total_weight desc;
limit 5;
fetch { "character": $n, "total_weight": $total_weight };""",

    369: """match
  $c isa character;
  (character1: $c, character2: $other) isa interacts3, has weight $w;
reduce $total_interactions = sum($w) groupby $c;
match $c has name $n;
sort $total_interactions desc;
limit 5;
fetch { "character": $n, "total_interactions": $total_interactions };""",

    391: """match
  $c isa character;
  (character1: $c, character2: $other) isa interacts3, has weight $w;
reduce $max_weight = max($w) groupby $c;
match $c has name $n;
sort $max_weight desc;
limit 3;
fetch { "character": $n, "max_weight": $max_weight };""",
}

# Category 7: Sum expression - use let
SUM_EXPRESSION_FIXES = {
    377: """match
  $c isa character, has pagerank $p, has book1_page_rank $p1, has book45_page_rank $p45;
let $total_pagerank = $p + $p1 + $p45;
sort $total_pagerank desc;
limit 5;
fetch { "character": $c.name, "totalPageRank": $total_pagerank };""",
}

# Category 6: Unsupported - array index (move to failed.csv)
UNSUPPORTED = [368]


def validate_query(driver, query):
    """Validate TypeQL query against TypeDB."""
    try:
        with driver.transaction("text2typeql_gameofthrones", TransactionType.READ) as tx:
            result = tx.query(query).resolve()
            # Try to consume results
            count = 0
            for doc in result.as_concept_documents():
                count += 1
                if count >= 1:
                    break
            return True, None
    except Exception as e:
        return False, str(e)


def main():
    # Connect to TypeDB
    credentials = Credentials("admin", "password")
    options = DriverOptions(is_tls_enabled=False)
    driver = TypeDB.driver("localhost:1729", credentials, options)

    # Read failed_review.csv
    failed_review_path = '/opt/text2typeql/output/gameofthrones/failed_review.csv'
    with open(failed_review_path, 'r') as f:
        reader = csv.DictReader(f)
        failed_rows = list(reader)

    # Read existing queries.csv
    queries_path = '/opt/text2typeql/output/gameofthrones/queries.csv'
    with open(queries_path, 'r') as f:
        reader = csv.DictReader(f)
        existing_queries = list(reader)

    # Read existing failed.csv
    failed_path = '/opt/text2typeql/output/gameofthrones/failed.csv'
    try:
        with open(failed_path, 'r') as f:
            reader = csv.DictReader(f)
            existing_failed = list(reader)
    except FileNotFoundError:
        existing_failed = []

    fixed_queries = []
    remaining_failed = []
    new_failed = []

    for row in failed_rows:
        idx = int(row['original_index'])
        question = row['question']
        cypher = row['cypher']
        old_typeql = row['typeql']

        new_typeql = None

        # Apply fixes based on category
        if idx in SORT_DIRECTION_FIXES:
            old_pattern, new_pattern = SORT_DIRECTION_FIXES[idx]
            new_typeql = old_typeql.replace(old_pattern, new_pattern)
        elif idx in INTERACTS_FIXES:
            new_typeql = INTERACTS_FIXES[idx]
        elif idx in IN_FILTER_FIXES:
            new_typeql = IN_FILTER_FIXES[idx]
        elif idx in CONTAINS_FIXES:
            new_typeql = CONTAINS_FIXES[idx]
        elif idx in AGGREGATION_FIXES:
            new_typeql = AGGREGATION_FIXES[idx]
        elif idx in SUM_EXPRESSION_FIXES:
            new_typeql = SUM_EXPRESSION_FIXES[idx]
        elif idx in UNSUPPORTED:
            # Move to failed.csv
            new_failed.append({
                'original_index': idx,
                'question': question,
                'cypher': cypher,
                'error': 'Array index access not supported in TypeQL'
            })
            print(f"[{idx}] UNSUPPORTED - moved to failed.csv")
            continue

        if new_typeql:
            # Validate the fixed query
            valid, error = validate_query(driver, new_typeql)
            if valid:
                fixed_queries.append({
                    'original_index': idx,
                    'question': question,
                    'cypher': cypher,
                    'typeql': new_typeql
                })
                print(f"[{idx}] FIXED - validated successfully")
            else:
                print(f"[{idx}] INVALID - {error[:80]}...")
                remaining_failed.append({
                    'original_index': idx,
                    'question': question,
                    'cypher': cypher,
                    'typeql': new_typeql,
                    'review_reason': f"Fixed query failed validation: {error}"
                })
        else:
            # Keep in failed_review
            remaining_failed.append(row)
            print(f"[{idx}] NO FIX - kept in failed_review.csv")

    # Append fixed queries to queries.csv
    all_queries = existing_queries + fixed_queries
    # Sort by original_index
    all_queries.sort(key=lambda x: int(x['original_index']))

    with open(queries_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
        writer.writeheader()
        writer.writerows(all_queries)

    # Write remaining failed_review.csv
    if remaining_failed:
        with open(failed_review_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql', 'review_reason'])
            writer.writeheader()
            writer.writerows(remaining_failed)
    else:
        # Empty file
        with open(failed_review_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql', 'review_reason'])
            writer.writeheader()

    # Append to failed.csv
    all_failed = existing_failed + new_failed
    all_failed.sort(key=lambda x: int(x['original_index']))

    with open(failed_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'error'])
        writer.writeheader()
        writer.writerows(all_failed)

    driver.close()

    print(f"\n=== Summary ===")
    print(f"Fixed and validated: {len(fixed_queries)}")
    print(f"Still failed review: {len(remaining_failed)}")
    print(f"Moved to failed.csv: {len(new_failed)}")
    print(f"Total queries.csv: {len(all_queries)}")


if __name__ == '__main__':
    main()
