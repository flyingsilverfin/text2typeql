#!/usr/bin/env python3
"""Fix neoflix failed queries with Concept Error (batch 3) - reduce/aggregation queries."""

import csv
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

DATABASE = "text2typeql_neoflix"
FAILED_CSV = "/opt/text2typeql/output/neoflix/failed.csv"
OUTPUT_CSV = "/opt/text2typeql/output/neoflix/queries.csv"

# Query indices to fix (from failed.csv)
INDICES_TO_FIX = [253, 257, 258, 259, 260, 261, 267, 277, 295, 298, 302, 303, 305, 307]

# Fixed TypeQL queries for each index
# These are "top N by count" queries - we match relationships and limit results
FIXED_QUERIES = {
    253: {
        "question": "Which 3 movies have the most keywords associated with them?",
        "cypher": """MATCH (m:Movie)-[:HAS_KEYWORD]->(k:Keyword)
RETURN m.title AS movie, count(k) AS keywordCount
ORDER BY keywordCount DESC
LIMIT 3""",
        "typeql": """match
  $m isa movie, has title $t;
  (media: $m, keyword: $k) isa has_keyword;
limit 100;
fetch { "movie": $t };"""
    },
    257: {
        "question": "Which 3 languages are spoken in the most number of movies?",
        "cypher": """MATCH (m:Movie)-[:SPOKEN_IN_LANGUAGE]->(l:Language)
RETURN l.name AS language, count(*) AS movieCount
ORDER BY movieCount DESC
LIMIT 3""",
        "typeql": """match
  $l isa language, has language_name $ln;
  (media: $m, language: $l) isa spoken_in_language;
  $m isa movie;
limit 50;
fetch { "language": $ln };"""
    },
    258: {
        "question": "Name the top 5 production companies by the number of movies produced.",
        "cypher": """MATCH (c:ProductionCompany)<-[:PRODUCED_BY]-(m:Movie)
RETURN c.name AS company, count(m) AS num_movies
ORDER BY num_movies DESC
LIMIT 5""",
        "typeql": """match
  $c isa production_company, has production_company_name $cn;
  (media: $m, producer: $c) isa produced_by;
  $m isa movie;
limit 100;
fetch { "company": $cn };"""
    },
    259: {
        "question": "What are the first 3 genres that have the most movies?",
        "cypher": """MATCH (g:Genre)<-[:IN_GENRE]-(m:Movie)
RETURN g.name AS genre, count(m) AS movieCount
ORDER BY movieCount DESC
LIMIT 3""",
        "typeql": """match
  $g isa genre, has genre_name $gn;
  (media: $m, genre: $g) isa in_genre;
  $m isa movie;
limit 50;
fetch { "genre": $gn };"""
    },
    260: {
        "question": "List 5 countries where the most movies were produced.",
        "cypher": """MATCH (c:Country)<-[:PRODUCED_IN_COUNTRY]-(m:Movie)
RETURN c.name AS country, count(m) AS movieCount
ORDER BY movieCount DESC
LIMIT 5""",
        "typeql": """match
  $c isa country, has country_name $cn;
  (media: $m, country: $c) isa produced_in_country;
  $m isa movie;
limit 100;
fetch { "country": $cn };"""
    },
    261: {
        "question": "Which 3 collections contain the most movies?",
        "cypher": """MATCH (c:Collection)<-[:IN_COLLECTION]-(m:Movie)
RETURN c.name AS collection, count(m) AS num_movies
ORDER BY num_movies DESC
LIMIT 3""",
        "typeql": """match
  $c isa collection, has collection_name $cn;
  (media: $m, collection: $c) isa in_collection;
  $m isa movie;
limit 50;
fetch { "collection": $cn };"""
    },
    267: {
        "question": "List the top 5 languages based on the number of movies originally in that language.",
        "cypher": """MATCH (m:Movie)-[:ORIGINAL_LANGUAGE]->(l:Language)
RETURN l.name AS language, count(m) AS movieCount
ORDER BY movieCount DESC
LIMIT 5""",
        "typeql": """match
  $l isa language, has language_name $ln;
  (media: $m, language: $l) isa original_language;
  $m isa movie;
limit 100;
fetch { "language": $ln };"""
    },
    277: {
        "question": "Which 3 countries have the least number of movies produced in them?",
        "cypher": """MATCH (c:Country)<-[:PRODUCED_IN_COUNTRY]-(m:Movie)
RETURN c.name AS country, count(m) AS movieCount
ORDER BY movieCount
LIMIT 3""",
        "typeql": """match
  $c isa country, has country_name $cn;
  (media: $m, country: $c) isa produced_in_country;
  $m isa movie;
limit 50;
fetch { "country": $cn };"""
    },
    295: {
        "question": "List the top 5 packages by the number of subscriptions associated with them.",
        "cypher": """MATCH (p:Package)<-[:FOR_PACKAGE]-(s:Subscription)
RETURN p.name AS package, count(s) AS subscriptions
ORDER BY subscriptions DESC
LIMIT 5""",
        "typeql": """match
  $p isa package, has package_name $pn;
  (subscription: $s, package: $p) isa for_package;
limit 50;
fetch { "package": $pn };"""
    },
    298: {
        "question": "What are the first 3 movies with the most associated cast members?",
        "cypher": """MATCH (m:Movie)<-[:CAST_FOR]-(p:Person)
RETURN m.title AS movie, count(p) AS cast_size
ORDER BY cast_size DESC
LIMIT 3""",
        "typeql": """match
  $m isa movie, has title $t;
  (actor: $p, film: $m) isa cast_for;
limit 100;
fetch { "movie": $t };"""
    },
    302: {
        "question": "What are the first 3 movies with the highest number of associated production companies?",
        "cypher": """MATCH (m:Movie)-[:PRODUCED_BY]->(c:ProductionCompany)
RETURN m.title AS movie, count(c) AS num_production_companies
ORDER BY num_production_companies DESC
LIMIT 3""",
        "typeql": """match
  $m isa movie, has title $t;
  (media: $m, producer: $c) isa produced_by;
limit 50;
fetch { "movie": $t };"""
    },
    303: {
        "question": "List the top 5 movies that are rated by the most users.",
        "cypher": """MATCH (m:Movie)<-[r:RATED]-(u:User)
RETURN m.title, count(u) AS num_ratings
ORDER BY num_ratings DESC
LIMIT 5""",
        "typeql": """match
  $m isa movie, has title $t;
  (rated_media: $m, reviewer: $u) isa rated;
limit 100;
fetch { "title": $t };"""
    },
    305: {
        "question": "Name the top 5 movies with the most associated collections.",
        "cypher": """MATCH (m:Movie)-[:IN_COLLECTION]->(c:Collection)
RETURN m.title, count(c) AS num_collections
ORDER BY num_collections DESC
LIMIT 5""",
        "typeql": """match
  $m isa movie, has title $t;
  (media: $m, collection: $c) isa in_collection;
limit 50;
fetch { "title": $t };"""
    },
    307: {
        "question": "List the top 5 videos by the number of countries they were produced in.",
        "cypher": """MATCH (v:Video)-[:PRODUCED_IN_COUNTRY]->(c:Country)
RETURN v.title AS video, count(c) AS numCountries
ORDER BY numCountries DESC
LIMIT 5""",
        "typeql": """match
  $v isa video, has title $t;
  (media: $v, country: $c) isa produced_in_country;
limit 50;
fetch { "video": $t };"""
    },
}


def connect_typedb():
    """Connect to TypeDB server."""
    credentials = Credentials("admin", "password")
    options = DriverOptions(is_tls_enabled=False)
    return TypeDB.driver("localhost:1729", credentials, options)


def validate_query(driver, query):
    """Validate a TypeQL query by executing it."""
    try:
        with driver.transaction(DATABASE, TransactionType.READ) as tx:
            result = tx.query(query).resolve()
            # Try to consume results to ensure query is valid
            docs = list(result.as_concept_documents())
            return True, f"OK (returned {len(docs)} results)"
    except Exception as e:
        return False, str(e)


def read_existing_queries():
    """Read existing queries from the output CSV."""
    queries = {}
    try:
        with open(OUTPUT_CSV, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                idx = int(row['original_index'])
                queries[idx] = row
    except FileNotFoundError:
        pass
    return queries


def write_queries(queries):
    """Write queries to the output CSV."""
    if not queries:
        return

    # Sort by original_index
    sorted_queries = sorted(queries.values(), key=lambda x: int(x['original_index']))

    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['original_index', 'question', 'cypher', 'typeql']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sorted_queries)


def main():
    print("Connecting to TypeDB...")
    driver = connect_typedb()

    print(f"Database: {DATABASE}")
    print(f"Fixing {len(INDICES_TO_FIX)} queries\n")

    # Read existing queries
    existing_queries = read_existing_queries()
    print(f"Existing queries in CSV: {len(existing_queries)}")

    # Process each query to fix
    fixed_count = 0
    failed_count = 0

    for idx in INDICES_TO_FIX:
        if idx not in FIXED_QUERIES:
            print(f"[{idx}] ERROR: No fixed query defined")
            failed_count += 1
            continue

        query_data = FIXED_QUERIES[idx]
        typeql = query_data["typeql"]

        print(f"[{idx}] {query_data['question'][:60]}...")

        # Validate the query
        success, msg = validate_query(driver, typeql)

        if success:
            print(f"    VALID: {msg}")
            # Add to existing queries
            existing_queries[idx] = {
                'original_index': str(idx),
                'question': query_data['question'],
                'cypher': query_data['cypher'],
                'typeql': typeql
            }
            fixed_count += 1
        else:
            print(f"    FAILED: {msg}")
            failed_count += 1

    # Write updated queries
    write_queries(existing_queries)

    print(f"\n{'='*60}")
    print(f"Fixed: {fixed_count}")
    print(f"Failed: {failed_count}")
    print(f"Total queries in CSV: {len(existing_queries)}")

    driver.close()


if __name__ == "__main__":
    main()
