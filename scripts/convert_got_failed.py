#!/usr/bin/env python3
"""
Convert failed Game of Thrones Cypher queries to TypeQL.
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
    Returns (typeql, convertible) where convertible indicates if conversion is possible.
    """

    # Queries involving array indexing (fastrf_embedding[0], etc.) - NOT convertible
    if 'fastrf_embedding[' in cypher or 'any(x IN c.fastrf_embedding' in cypher:
        return None, False, "Array indexing/iteration not supported in TypeQL"

    # Queries involving max/min on arrays - NOT convertible
    if 'max(c.fastrf_embedding)' in cypher or 'min(c.fastrf_embedding)' in cypher:
        return None, False, "Array min/max operations not supported in TypeQL"

    # Queries with complex subqueries involving community grouping and counts - complex restructuring needed
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

    # Queries using EXISTS with nested conditions
    if 'EXISTS {' in cypher and 'WHERE' in cypher.split('EXISTS')[1]:
        return None, False, "EXISTS with nested WHERE not directly convertible"

    # Now handle convertible queries

    # Query 35: Characters who interact with Theon-Greyjoy (union of relation types)
    if "interact with 'Theon-Greyjoy'" in question.lower() and 'LIMIT 5' in cypher:
        # We need to check all interaction types - use a simple approach for one type first
        return """match
  $c isa character, has name $n;
  {(character1: $c, character2: $t) isa interacts;} or
  {(character1: $c, character2: $t) isa interacts1;} or
  {(character1: $c, character2: $t) isa interacts2;} or
  {(character1: $c, character2: $t) isa interacts3;} or
  {(character1: $c, character2: $t) isa interacts45;};
  $t isa character, has name "Theon-Greyjoy";
limit 5;
fetch { "name": $n };""", True, None

    # Query 135: Similar to 35 but no limit
    if "interact with 'Theon-Greyjoy'" in question.lower() and 'LIMIT' not in cypher:
        return """match
  $c isa character, has name $n;
  {(character1: $c, character2: $t) isa interacts;} or
  {(character1: $c, character2: $t) isa interacts1;} or
  {(character1: $c, character2: $t) isa interacts2;} or
  {(character1: $c, character2: $t) isa interacts3;} or
  {(character1: $c, character2: $t) isa interacts45;};
  $t isa character, has name "Theon-Greyjoy";
fetch { "character": $n };""", True, None

    # Query 45: Top 5 by book45PageRank in same community as Murenmure
    if "same community as 'Murenmure'" in question:
        return """match
  $m isa character, has name "Murenmure", has community $comm;
  $c isa character, has community $comm, has name $n, has book45_page_rank $pr;
sort $pr desc;
limit 5;
fetch { "name": $n, "book45_page_rank": $pr };""", True, None

    # Query 114: Same community as Daenerys Targaryen
    if "same community as Daenerys Targaryen" in question:
        return """match
  $d isa character, has name "Daenerys-Targaryen", has community $comm;
  $c isa character, has community $comm, has name $n;
fetch { "name": $n };""", True, None

    # Query 115: Top 5 lowest book1BetweennessCentrality
    if "lowest book1BetweennessCentrality" in question and "5" in question:
        return """match
  $c isa character, has name $n, has book1_betweenness_centrality $bc;
sort $bc asc;
limit 5;
fetch { "character": $n, "centrality": $bc };""", True, None

    # Query 263: Top 3 lowest book1BetweennessCentrality
    if "lowest book1BetweennessCentrality" in question and "3" in question:
        return """match
  $c isa character, has name $n, has book1_betweenness_centrality $bc;
sort $bc asc;
limit 3;
fetch { "character": $n, "centrality": $bc };""", True, None

    # Query 120: Names starting with 'A'
    if "name starts with 'A'" in question:
        return """match
  $c isa character, has name $n;
  $n like "^A.*";
fetch { "name": $n };""", True, None

    # Query 123: Lowest book45PageRank
    if "lowest book45PageRank" in question and "5" not in question and "3" not in question:
        return """match
  $c isa character, has name $n, has book45_page_rank $pr;
sort $pr asc;
limit 5;
fetch { "name": $n, "book45_page_rank": $pr };""", True, None

    # Query 143: Names ending in 'Targaryen'
    if "ends in 'Targaryen'" in question or "ends with 'Targaryen'" in question.lower():
        return """match
  $c isa character, has name $n;
  $n like ".*Targaryen$";
fetch { "name": $n };""", True, None

    # Query 173: Lowest 5 book1PageRanks
    if "lowest 5 book1PageRank" in question:
        return """match
  $c isa character, has name $n, has book1_page_rank $pr;
sort $pr asc;
limit 5;
fetch { "character": $n, "pageRank": $pr };""", True, None

    # Query 183: Characters connected by INTERACTS with weight over 200
    if "weight over 200" in question and "INTERACTS" in question:
        return """match
  (character1: $c1, character2: $c2) isa interacts, has weight $w;
  $c1 has name $n1;
  $c2 has name $n2;
  $w > 200;
fetch { "character1": $n1, "character2": $n2, "weight": $w };""", True, None

    # Query 197: Top 5 highest book45PageRank
    if "top 5 highest" in question.lower() and "book45PageRank" in question:
        return """match
  $c isa character, has name $n, has book45_page_rank $pr;
sort $pr desc;
limit 5;
fetch { "character": $n, "book45_page_rank": $pr };""", True, None

    # Query 206: Top 3 most frequent communities in INTERACTS
    if "most frequent communities" in question and "INTERACTS" in question and "3" in question:
        return """match
  (character1: $c1, character2: $c2) isa interacts;
  $c1 has community $comm;
reduce $count = count($comm);
fetch { "community": $comm, "frequency": $count };""", True, None

    # Query 227: Top 10 highest book45PageRank
    if "top 10 highest" in question.lower() and "book45PageRank" in question:
        return """match
  $c isa character, has name $n, has book45_page_rank $pr;
sort $pr desc;
limit 10;
fetch { "character": $n, "book45_page_rank": $pr };""", True, None

    # Query 236: Top 5 most frequent communities in INTERACTS1
    if "most frequent communities" in question and "INTERACTS1" in question:
        return """match
  (character1: $c1, character2: $c2) isa interacts1;
  $c1 has community $comm;
reduce $count = count($comm);
fetch { "community": $comm, "frequency": $count };""", True, None

    # Query 256: Least centrality but more than 30 degree
    if "least centrality" in question and "30 degree" in question:
        return """match
  $c isa character, has name $n, has centrality $cent, has degree $deg;
  $deg > 30;
sort $cent asc;
limit 10;
fetch { "name": $n, "centrality": $cent, "degree": $deg };""", True, None

    # Query 270: Top 5 highest book1PageRank
    if "highest book1PageRank" in question and "5" in question:
        return """match
  $c isa character, has name $n, has book1_page_rank $pr;
sort $pr desc;
limit 5;
fetch { "name": $n, "book1_page_rank": $pr };""", True, None

    # Query 280: Top 5 lowest book45PageRank
    if "lowest book45PageRank" in question and "5" in question:
        return """match
  $c isa character, has name $n, has book45_page_rank $pr;
sort $pr asc;
limit 5;
fetch { "name": $n, "book45_page_rank": $pr };""", True, None

    # Query 299: Top 5 lowest book1PageRank
    if "lowest book1PageRank" in question and "5" in question:
        return """match
  $c isa character, has name $n, has book1_page_rank $pr;
sort $pr asc;
limit 5;
fetch { "name": $n, "book1_page_rank": $pr };""", True, None

    # Query 311: Top 5 lowest centrality
    if "lowest centrality" in question and "5" in question:
        return """match
  $c isa character, has name $n, has centrality $cent;
sort $cent asc;
limit 5;
fetch { "name": $n, "centrality": $cent };""", True, None

    # Query 320: Top 3 lowest book45PageRank
    if "lowest book45PageRank" in question and "3" in question:
        return """match
  $c isa character, has name $n, has book45_page_rank $pr;
sort $pr asc;
limit 3;
fetch { "name": $n, "book45_page_rank": $pr };""", True, None

    # Query 337: Top 3 least betweenness centrality in book 1
    if "least betweenness centrality in book 1" in question:
        return """match
  $c isa character, has name $n, has book1_betweenness_centrality $bc;
sort $bc asc;
limit 3;
fetch { "character": $n, "betweenness": $bc };""", True, None

    # Query 345: High pagerank and high degree of centrality
    if "high pagerank" in question.lower() and "high degree" in question.lower() and "5" in question:
        return """match
  $c isa character, has name $n, has pagerank $pr, has centrality $cent;
sort $pr desc;
limit 5;
fetch { "character": $n, "pagerank": $pr, "centrality": $cent };""", True, None

    # Query 352: Top 3 lowest pagerank who interact in book 45
    if "lowest pagerank" in question.lower() and "book 45" in question.lower():
        return """match
  $c isa character, has name $n, has pagerank $pr;
  (character1: $c, character2: $other) isa interacts45;
sort $pr asc;
limit 3;
fetch { "name": $n, "pagerank": $pr };""", True, None

    # Query 359: Top 3 based on sum of centrality values
    if "sum of their centrality values" in question and "3" in question:
        return None, False, "Arithmetic on multiple attributes not directly supported"

    # Query 361: Top 3 based on combination of book1PageRank and book45PageRank
    if "combination of book1PageRank and book45PageRank" in question and "3" in question:
        return None, False, "Arithmetic on multiple attributes not directly supported"

    # Query 386: Top 3 based on sum of book1PageRank and book45PageRank
    if "sum of their book1PageRank and book45PageRank" in question:
        return None, False, "Arithmetic on multiple attributes not directly supported"

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

    # Read existing successful queries to get the format
    existing_queries = []
    try:
        with open('/opt/text2typeql/output/gameofthrones/queries.csv', 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_queries.append(row)
        print(f"Loaded {len(existing_queries)} existing successful queries")
    except FileNotFoundError:
        print("No existing queries.csv found, will create new one")

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
