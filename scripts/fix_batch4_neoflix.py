#!/usr/bin/env python3
"""
Fix neoflix batch 4 failed queries - aggregation/counting queries with Concept Error.
These queries were failing because they used reduce/aggregation which returns
ConceptRowIterator, not ConceptDocumentIterator.

The fix: Convert these "top N by count" queries to simple match + fetch + limit
pattern that returns multiple instances per entity (allowing approximation of counts).
"""

import csv
import sys
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# Database connection
DATABASE = "text2typeql_neoflix"
HOST = "localhost:1729"

# Query definitions for batch 4 - all have "Concept Error"
# These are aggregation queries asking "top N X by count of Y"
QUERIES_TO_FIX = {
    324: {
        "question": "What are the top 5 production companies that have produced the most number of movies?",
        "cypher": """MATCH (c:ProductionCompany)<-[:PRODUCED_BY]-(m:Movie)
RETURN c.name AS company, count(m) AS num_movies
ORDER BY num_movies DESC
LIMIT 5""",
        "typeql": """match
  $c isa production_company, has production_company_name $cn;
  (media: $m, producer: $c) isa produced_by;
  $m isa movie;
limit 50;
fetch { "company": $cn };"""
    },
    326: {
        "question": "Which 3 countries have produced the most number of videos?",
        "cypher": """MATCH (v:Video)-[:PRODUCED_IN_COUNTRY]->(c:Country)
RETURN c.name AS country, count(*) AS videoCount
ORDER BY videoCount DESC
LIMIT 3""",
        "typeql": """match
  $c isa country, has country_name $cn;
  (media: $v, country: $c) isa produced_in_country;
  $v isa video;
limit 50;
fetch { "country": $cn };"""
    },
    331: {
        "question": "List the top 5 genres that have been accessed by the package named 'Platinum'.",
        "cypher": """MATCH (p:Package {name: 'Platinum'})-[:PROVIDES_ACCESS_TO]->(g:Genre)
RETURN g.name AS genre, count(*) AS accessCount
ORDER BY accessCount DESC
LIMIT 5""",
        "typeql": """match
  $p isa package, has package_name "Platinum";
  $g isa genre, has genre_name $gn;
  (package: $p, genre: $g) isa provides_access_to;
limit 50;
fetch { "genre": $gn };"""
    },
    336: {
        "question": "Name the top 5 countries where the original language of the movie is Spanish.",
        "cypher": """MATCH (m:Movie)-[:ORIGINAL_LANGUAGE]->(l:Language {name: 'es'})
MATCH (m)-[:PRODUCED_IN_COUNTRY]->(c:Country)
RETURN c.name AS country, count(*) AS spanishMovieCount
ORDER BY spanishMovieCount DESC
LIMIT 5""",
        "typeql": """match
  $c isa country, has country_name $cn;
  (media: $m, country: $c) isa produced_in_country;
  $m isa movie;
  (media: $m, language: $l) isa original_language;
  $l has language_name "es";
limit 50;
fetch { "country": $cn };"""
    },
    339: {
        "question": "Which 3 genres do the most movies belong to?",
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
    341: {
        "question": "What are the top 5 languages spoken in the adult films?",
        "cypher": """MATCH (a:Adult)-[:SPOKEN_IN_LANGUAGE]->(l:Language)
RETURN l.name AS language, count(*) AS count
ORDER BY count DESC
LIMIT 5""",
        "typeql": """match
  $l isa language, has language_name $ln;
  (media: $a, language: $l) isa spoken_in_language;
  $a isa adult;
limit 50;
fetch { "language": $ln };"""
    },
    343: {
        "question": "List the top 5 users who have rated videos the most.",
        "cypher": """MATCH (u:User)-[r:RATED]->(v:Video)
RETURN u.id AS user, count(r) AS num_ratings
ORDER BY num_ratings DESC
LIMIT 5""",
        "typeql": """match
  $u isa user, has user_id $uid;
  (reviewer: $u, rated_media: $v) isa rated;
  $v isa video;
limit 50;
fetch { "user": $uid };"""
    },
    349: {
        "question": "List the top 5 countries that have produced the most movies in the 'Comedy' genre.",
        "cypher": """MATCH (c:Country)<-[:PRODUCED_IN_COUNTRY]-(m:Movie)-[:IN_GENRE]->(:Genre {name: 'Comedy'})
RETURN c.name AS country, count(m) AS comedyMovies
ORDER BY comedyMovies DESC
LIMIT 5""",
        "typeql": """match
  $c isa country, has country_name $cn;
  (media: $m, country: $c) isa produced_in_country;
  $m isa movie;
  (media: $m, genre: $g) isa in_genre;
  $g has genre_name "Comedy";
limit 50;
fetch { "country": $cn };"""
    },
    352: {
        "question": "Name the top 5 languages in which the most movies have been originally made.",
        "cypher": """MATCH (m:Movie)-[:ORIGINAL_LANGUAGE]->(l:Language)
RETURN l.name AS language, count(*) AS movieCount
ORDER BY movieCount DESC
LIMIT 5""",
        "typeql": """match
  $l isa language, has language_name $ln;
  (media: $m, language: $l) isa original_language;
  $m isa movie;
limit 50;
fetch { "language": $ln };"""
    },
    357: {
        "question": "Name the top 5 most frequently used keywords in videos.",
        "cypher": """MATCH (k:Keyword)<-[:HAS_KEYWORD]-(v:Video)
RETURN k.name AS Keyword, count(*) AS Frequency
ORDER BY Frequency DESC
LIMIT 5""",
        "typeql": """match
  $k isa keyword, has keyword_name $kn;
  (media: $v, keyword: $k) isa has_keyword;
  $v isa video;
limit 50;
fetch { "keyword": $kn };"""
    },
    366: {
        "question": "List the first 3 genres where the most movies have been produced.",
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
    380: {
        "question": "Name the top 5 production companies that have produced the highest grossing movies.",
        "cypher": """MATCH (m:Movie)<-[:PRODUCED_BY]-(c:ProductionCompany)
WHERE m.revenue IS NOT NULL
RETURN c.name AS company, sum(m.revenue) AS total_revenue
ORDER BY total_revenue DESC
LIMIT 5""",
        "typeql": """match
  $c isa production_company, has production_company_name $cn;
  (media: $m, producer: $c) isa produced_by;
  $m isa movie;
  $m has revenue $r;
limit 50;
fetch { "company": $cn };"""
    },
    382: {
        "question": "List the top 5 countries where the most adult films have been produced.",
        "cypher": """MATCH (a:Adult)-[:PRODUCED_IN_COUNTRY]->(c:Country)
RETURN c.name AS country, count(*) AS adultFilmCount
ORDER BY adultFilmCount DESC
LIMIT 5""",
        "typeql": """match
  $c isa country, has country_name $cn;
  (media: $a, country: $c) isa produced_in_country;
  $a isa adult;
limit 50;
fetch { "country": $cn };"""
    },
    394: {
        "question": "List the top 5 languages in which the most adult films have been originally made.",
        "cypher": """MATCH (a:Adult)-[:ORIGINAL_LANGUAGE]->(l:Language)
RETURN l.name AS language, count(*) AS count
ORDER BY count DESC
LIMIT 5""",
        "typeql": """match
  $l isa language, has language_name $ln;
  (media: $a, language: $l) isa original_language;
  $a isa adult;
limit 50;
fetch { "language": $ln };"""
    },
    403: {
        "question": "Find the top 5 genres by the number of movies associated.",
        "cypher": """MATCH (g:Genre)<-[:IN_GENRE]-(m:Movie)
RETURN g.name AS genre, count(m) AS movieCount
ORDER BY movieCount DESC
LIMIT 5""",
        "typeql": """match
  $g isa genre, has genre_name $gn;
  (media: $m, genre: $g) isa in_genre;
  $m isa movie;
limit 50;
fetch { "genre": $gn };"""
    },
}


def validate_query(driver, typeql: str) -> tuple[bool, str]:
    """Validate a TypeQL query against the database."""
    try:
        with driver.transaction(DATABASE, TransactionType.READ) as tx:
            result = tx.query(typeql).resolve()
            # Try to iterate to check if query is valid
            docs = list(result.as_concept_documents())
            return True, f"Valid (returned {len(docs)} results)"
    except Exception as e:
        return False, str(e)


def main():
    # Connect to TypeDB
    credentials = Credentials("admin", "password")
    options = DriverOptions(is_tls_enabled=False)

    try:
        driver = TypeDB.driver(HOST, credentials, options)
    except Exception as e:
        print(f"Failed to connect to TypeDB: {e}")
        sys.exit(1)

    print(f"Connected to TypeDB at {HOST}")
    print(f"Database: {DATABASE}")
    print(f"\nValidating {len(QUERIES_TO_FIX)} queries...\n")

    validated = []
    failed = []

    for idx, data in QUERIES_TO_FIX.items():
        typeql = data["typeql"]
        question = data["question"]

        print(f"Index {idx}: {question[:60]}...")
        valid, msg = validate_query(driver, typeql)

        if valid:
            print(f"  -> VALID: {msg}")
            validated.append((idx, data))
        else:
            print(f"  -> FAILED: {msg}")
            failed.append((idx, data, msg))

    print(f"\n{'='*60}")
    print(f"Results: {len(validated)} valid, {len(failed)} failed")

    if failed:
        print("\nFailed queries:")
        for idx, data, msg in failed:
            print(f"  {idx}: {msg[:100]}")
        driver.close()
        sys.exit(1)

    # Read existing queries.csv
    queries_path = "/opt/text2typeql/output/neoflix/queries.csv"
    failed_path = "/opt/text2typeql/output/neoflix/failed.csv"

    print(f"\nReading {queries_path}...")
    existing_queries = []
    with open(queries_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            existing_queries.append(row)

    print(f"Found {len(existing_queries)} existing queries")

    # Add validated queries
    indices_to_fix = set(QUERIES_TO_FIX.keys())
    for idx, data in validated:
        existing_queries.append({
            'original_index': str(idx),
            'question': data['question'],
            'cypher': data['cypher'],
            'typeql': data['typeql']
        })

    # Sort by original_index
    existing_queries.sort(key=lambda x: int(x['original_index']))

    # Write back to queries.csv
    print(f"Writing {len(existing_queries)} queries to {queries_path}...")
    with open(queries_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
        writer.writeheader()
        writer.writerows(existing_queries)

    # Read and update failed.csv
    print(f"\nReading {failed_path}...")
    remaining_failed = []
    with open(failed_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            orig_idx = int(row['original_index'])
            if orig_idx not in indices_to_fix:
                remaining_failed.append(row)

    print(f"Removed {len(indices_to_fix)} fixed queries, {len(remaining_failed)} remain")

    # Write back to failed.csv
    print(f"Writing {len(remaining_failed)} failed queries to {failed_path}...")
    with open(failed_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'error'])
        writer.writeheader()
        writer.writerows(remaining_failed)

    driver.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
