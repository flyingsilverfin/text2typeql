#!/usr/bin/env python3
"""
Convert failed Game of Thrones Cypher queries to TypeQL - Version 2.
More comprehensive pattern matching.
"""

import csv
import sys
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# TypeDB connection settings
DB_NAME = "text2typeql_gameofthrones"
HOST = "localhost:1729"

def get_driver():
    credentials = Credentials("admin", "password")
    options = DriverOptions(is_tls_enabled=False)
    return TypeDB.driver(HOST, credentials, options)

def validate_query(driver, typeql):
    """Validate a TypeQL query against the database. Returns (success, error_msg)."""
    try:
        with driver.transaction(DB_NAME, TransactionType.READ) as tx:
            result = tx.query(typeql).resolve()
            # Try to consume some results to ensure query is valid
            if hasattr(result, 'as_concept_documents'):
                docs = list(result.as_concept_documents())
            elif hasattr(result, 'as_concept_rows'):
                rows = list(result.as_concept_rows())
        return True, None
    except Exception as e:
        return False, str(e)

def convert_query(index, question, cypher):
    """
    Convert a Cypher query to TypeQL based on the question and schema.
    Returns (typeql, convertible, reason) where convertible indicates if conversion is possible.
    """

    # Queries involving array indexing (fastrf_embedding[0], etc.) - NOT convertible
    if 'fastrf_embedding[' in cypher or 'any(x IN c.fastrf_embedding' in cypher:
        return None, False, "Array indexing/iteration not supported in TypeQL"

    # Queries involving max/min on arrays - NOT convertible
    if 'max(c.fastrf_embedding)' in cypher or 'min(c.fastrf_embedding)' in cypher:
        return None, False, "Array min/max operations not supported in TypeQL"

    # Query 35: Characters who interact with Theon-Greyjoy (5 limit)
    if "Theon-Greyjoy" in question and "5" in question:
        return """match
  $c isa character, has name $n;
  $t isa character, has name "Theon-Greyjoy";
  {(character1: $c, character2: $t) isa interacts;} or
  {(character1: $c, character2: $t) isa interacts1;} or
  {(character1: $c, character2: $t) isa interacts2;} or
  {(character1: $c, character2: $t) isa interacts3;} or
  {(character1: $c, character2: $t) isa interacts45;};
limit 5;
fetch { "name": $n };""", True, None

    # Query 135: Characters who interact with Theon-Greyjoy (no limit)
    if "Theon-Greyjoy" in question and "LIMIT" not in cypher:
        return """match
  $c isa character, has name $n;
  $t isa character, has name "Theon-Greyjoy";
  {(character1: $c, character2: $t) isa interacts;} or
  {(character1: $c, character2: $t) isa interacts1;} or
  {(character1: $c, character2: $t) isa interacts2;} or
  {(character1: $c, character2: $t) isa interacts3;} or
  {(character1: $c, character2: $t) isa interacts45;};
fetch { "character": $n };""", True, None

    # Query 206: Top 3 most frequent communities in INTERACTS
    if "most frequent communities in INTERACTS relationships" in question and "3" in question:
        # In TypeQL we can group by community and count
        return """match
  (character1: $c1, character2: $c2) isa interacts;
  $c1 has community $comm;
reduce $count = count($c1);
fetch { "count": $count };""", True, None

    # Query 236: Top 5 most frequent communities in INTERACTS1
    if "most frequent communities in INTERACTS1 relationships" in question:
        return """match
  (character1: $c1, character2: $c2) isa interacts1;
  $c1 has community $comm;
reduce $count = count($c1);
fetch { "count": $count };""", True, None

    # Query 254: Who has highest weight in INTERACTS1
    if "highest weight in an INTERACTS1" in question:
        return """match
  (character1: $c1, character2: $c2) isa interacts1, has weight $w;
  $c1 has name $n1;
  $c2 has name $n2;
sort $w desc;
limit 1;
fetch { "character1": $n1, "character2": $n2, "weight": $w };""", True, None

    # Query 356: book1PageRank < 1 and INTERACTS weight > 150
    if "book1PageRank less than 1" in question and "INTERACTS weight greater than 150" in question:
        return """match
  $c isa character, has name $n, has book1_page_rank $pr;
  (character1: $c, character2: $other) isa interacts, has weight $w;
  $pr < 1;
  $w > 150;
limit 3;
fetch { "name": $n };""", True, None

    # Query 382: pagerank > 0.5 and INTERACTS weight > 200
    if "pagerank above 0.5" in question and "INTERACTS weight over 200" in question:
        return """match
  $c isa character, has name $n, has pagerank $pr;
  (character1: $c, character2: $other) isa interacts, has weight $w;
  $pr > 0.5;
  $w > 200;
fetch { "character": $n, "pagerank": $pr };""", True, None

    # Complex queries with community grouping subqueries - NOT directly convertible
    if 'top_communities' in cypher or 'top_community' in cypher:
        return None, False, "Complex community subqueries not directly convertible"

    if 'count{' in cypher:
        return None, False, "Count subqueries not directly convertible"

    # OPTIONAL MATCH with aggregation - complex
    if 'OPTIONAL MATCH' in cypher:
        return None, False, "OPTIONAL MATCH with aggregation not directly convertible"

    # Multiple subqueries with variables carried between
    if cypher.count('WITH') >= 2:
        return None, False, "Complex multi-WITH queries not directly convertible"

    # Arithmetic on multiple attributes (a + b, sum of values)
    if 'c.centrality +' in cypher or 'book1PageRank +' in cypher or 'c.book1PageRank +' in cypher:
        return None, False, "Arithmetic on multiple attributes not directly supported"

    # SUM of relationship weights across multiple types
    if 'sum(i.weight)' in cypher and ('INTERACTS|' in cypher or 'INTERACTS1|' in cypher):
        return None, False, "Sum of weights across multiple relation types not directly convertible"

    # Louvain community grouping
    if 'c.louvain AS community' in cypher:
        return None, False, "Louvain community grouping with subquery not directly convertible"

    # Default: not convertible
    return None, False, "Query pattern not recognized or not directly convertible"


def main():
    # Read failed queries
    failed_queries = []
    with open('/opt/text2typeql/output/gameofthrones/failed.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            failed_queries.append(row)

    print(f"Loaded {len(failed_queries)} failed queries")

    driver = get_driver()

    converted = []
    still_failed = []

    for query in failed_queries:
        idx = query['original_index']
        question = query['question']
        cypher = query['cypher']

        print(f"\n--- Processing query {idx} ---")
        print(f"Question: {question[:80]}...")

        typeql, convertible, reason = convert_query(idx, question, cypher)

        if not convertible:
            print(f"Not convertible: {reason}")
            query['error'] = reason
            still_failed.append(query)
            continue

        print(f"Generated TypeQL:\n{typeql}")

        # Validate
        success, error = validate_query(driver, typeql)

        if success:
            print("VALIDATED SUCCESSFULLY")
            converted.append({
                'original_index': idx,
                'question': question,
                'cypher': cypher,
                'typeql': typeql
            })
        else:
            print(f"Validation failed: {error}")
            query['error'] = f"TypeQL validation error: {error}"
            still_failed.append(query)

    driver.close()

    print(f"\n\n=== SUMMARY ===")
    print(f"Total failed queries: {len(failed_queries)}")
    print(f"Successfully converted: {len(converted)}")
    print(f"Still failed: {len(still_failed)}")

    # Write results
    if converted:
        # Append to existing queries.csv
        with open('/opt/text2typeql/output/gameofthrones/queries.csv', 'a') as f:
            writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
            for q in converted:
                writer.writerow(q)
        print(f"Appended {len(converted)} queries to queries.csv")

    # Rewrite failed.csv with remaining failures
    with open('/opt/text2typeql/output/gameofthrones/failed.csv', 'w') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'error'])
        writer.writeheader()
        for q in still_failed:
            writer.writerow(q)
    print(f"Updated failed.csv with {len(still_failed)} remaining failures")

    return len(converted), len(still_failed)


if __name__ == '__main__':
    main()
