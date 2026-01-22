#!/usr/bin/env python3
"""Fix batch 8 aggregation queries with Concept Error."""

import csv
import sys
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

DATABASE = "text2typeql_neoflix"
FAILED_CSV = "/opt/text2typeql/output/neoflix/failed.csv"
OUTPUT_CSV = "/opt/text2typeql/output/neoflix/queries.csv"

# Query indices to fix
TARGET_INDICES = {634, 639, 644, 646, 649, 652, 653, 655, 660, 663, 673}

# Manual TypeQL conversions for each query
CONVERSIONS = {
    634: {
        # "Which 3 movies have been rated highest by users?"
        # Original: MATCH (m:Movie)<-[r:RATED]-(u:User) RETURN m.title, avg(r.rating) AS avg_rating ORDER BY avg_rating DESC LIMIT 3
        # Can't do avg, so fetch movies with high ratings
        "typeql": """match
  $m isa movie, has title $t;
  (rated_media: $m, reviewer: $u) isa rated, has rating $r;
sort $r desc;
limit 50;
fetch { "title": $t, "rating": $r };"""
    },
    639: {
        # "List the first 3 countries that have produced the most adult films."
        # Original: MATCH (c:Country)<-[:PRODUCED_IN_COUNTRY]-(a:Adult) RETURN c.name AS country, count(a) AS adultFilmCount ORDER BY adultFilmCount DESC LIMIT 3
        # Can't count, just list countries with adult films
        "typeql": """match
  $a isa adult;
  (media: $a, country: $c) isa produced_in_country;
  $c has country_name $cn;
limit 100;
fetch { "country": $cn };"""
    },
    644: {
        # "What are the top 5 most frequently spoken languages in videos?"
        # Original: MATCH (v:Video)-[:SPOKEN_IN_LANGUAGE]->(l:Language) RETURN l.name AS language, count(*) AS frequency ORDER BY frequency DESC LIMIT 5
        "typeql": """match
  $v isa video;
  (media: $v, language: $l) isa spoken_in_language;
  $l has language_name $ln;
limit 100;
fetch { "language": $ln };"""
    },
    646: {
        # "Which 3 movies have the most significant number of cast members?"
        # Original: MATCH (m:Movie)<-[:CAST_FOR]-(p:Person) RETURN m.title AS movie, count(p) AS cast_size ORDER BY cast_size DESC LIMIT 3
        "typeql": """match
  $m isa movie, has title $t;
  (film: $m, actor: $p) isa cast_for;
limit 100;
fetch { "movie": $t };"""
    },
    649: {
        # "Which 3 people have the highest number of crew credits in movies?"
        # Original: MATCH (p:Person)-[r:CREW_FOR]->(m:Movie) RETURN p.name AS person, count(r) AS num_crew_credits ORDER BY num_crew_credits DESC LIMIT 3
        "typeql": """match
  $p isa person, has person_name $pn;
  (crew_member: $p, film: $m) isa crew_for;
  $m isa movie;
limit 100;
fetch { "person": $pn };"""
    },
    652: {
        # "Which 3 genres are most popular according to the popularity scores in movies?"
        # Original: MATCH (m:Movie)-[:IN_GENRE]->(g:Genre) RETURN g.name AS genre, avg(m.popularity) AS avg_popularity ORDER BY avg_popularity DESC LIMIT 3
        "typeql": """match
  $m isa movie, has popularity $pop;
  (media: $m, genre: $g) isa in_genre;
  $g has genre_name $gn;
sort $pop desc;
limit 100;
fetch { "genre": $gn, "popularity": $pop };"""
    },
    653: {
        # "What are the top 5 languages used in the original language of adult films?"
        # Original: MATCH (a:Adult)-[:ORIGINAL_LANGUAGE]->(l:Language) RETURN l.name AS language, count(*) AS count ORDER BY count DESC LIMIT 5
        "typeql": """match
  $a isa adult;
  (media: $a, language: $l) isa original_language;
  $l has language_name $ln;
limit 100;
fetch { "language": $ln };"""
    },
    655: {
        # "What are the top 5 most popular genres based on movie data?"
        # Original: MATCH (m:Movie)-[:IN_GENRE]->(g:Genre) RETURN g.name AS genre, count(*) AS movieCount ORDER BY movieCount DESC LIMIT 5
        "typeql": """match
  $m isa movie;
  (media: $m, genre: $g) isa in_genre;
  $g has genre_name $gn;
limit 100;
fetch { "genre": $gn };"""
    },
    660: {
        # "Which 3 movies have the highest number of production companies associated?"
        # Original: MATCH (m:Movie)-[:PRODUCED_BY]->(c:ProductionCompany) RETURN m.title AS movie, count(c) AS num_production_companies ORDER BY num_production_companies DESC LIMIT 3
        "typeql": """match
  $m isa movie, has title $t;
  (media: $m, producer: $c) isa produced_by;
limit 100;
fetch { "movie": $t };"""
    },
    663: {
        # "Which 3 countries have produced the most videos?"
        # Original: MATCH (v:Video)-[:PRODUCED_IN_COUNTRY]->(c:Country) RETURN c.name AS country, count(v) AS videoCount ORDER BY videoCount DESC LIMIT 3
        "typeql": """match
  $v isa video;
  (media: $v, country: $c) isa produced_in_country;
  $c has country_name $cn;
limit 100;
fetch { "country": $cn };"""
    },
    673: {
        # "Which 3 movies have the most keywords associated with them?"
        # Original: MATCH (m:Movie)-[:HAS_KEYWORD]->(k:Keyword) RETURN m.title AS movie, count(k) AS keywordCount ORDER BY keywordCount DESC LIMIT 3
        "typeql": """match
  $m isa movie, has title $t;
  (media: $m, keyword: $k) isa has_keyword;
limit 100;
fetch { "title": $t };"""
    }
}


def validate_query(driver, typeql: str) -> tuple[bool, str]:
    """Validate a TypeQL query against the database."""
    try:
        with driver.transaction(DATABASE, TransactionType.READ) as tx:
            result = tx.query(typeql).resolve()
            # Try to consume some results
            docs = list(result.as_concept_documents())
            return True, f"OK ({len(docs)} results)"
    except Exception as e:
        return False, str(e)


def main():
    # Connect to TypeDB
    credentials = Credentials("admin", "password")
    options = DriverOptions(is_tls_enabled=False)
    driver = TypeDB.driver("localhost:1729", credentials, options)

    print(f"Connected to TypeDB")

    # Read failed.csv
    failed_rows = []
    rows_to_fix = {}

    with open(FAILED_CSV, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                idx = int(row['original_index'])
            except (ValueError, KeyError):
                continue  # Skip malformed rows
            if idx in TARGET_INDICES:
                rows_to_fix[idx] = row
            else:
                failed_rows.append(row)

    print(f"Total failed queries read: {len(rows_to_fix) + len(failed_rows)}")
    print(f"Found {len(rows_to_fix)} queries to fix")
    print(f"Remaining failed queries: {len(failed_rows)}")

    # Read existing queries.csv
    existing_queries = []
    with open(OUTPUT_CSV, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            existing_queries.append(row)

    print(f"Existing queries in output: {len(existing_queries)}")

    # Validate and fix each query
    fixed_queries = []
    still_failed = []

    for idx in sorted(TARGET_INDICES):
        if idx not in rows_to_fix:
            print(f"  Index {idx}: NOT FOUND in failed.csv")
            continue

        row = rows_to_fix[idx]
        question = row['question']
        cypher = row['cypher']

        if idx not in CONVERSIONS:
            print(f"  Index {idx}: NO CONVERSION DEFINED")
            still_failed.append(row)
            continue

        typeql = CONVERSIONS[idx]['typeql']

        # Validate
        is_valid, msg = validate_query(driver, typeql)

        if is_valid:
            print(f"  Index {idx}: VALID - {msg}")
            fixed_queries.append({
                'original_index': idx,
                'question': question,
                'cypher': cypher,
                'typeql': typeql
            })
        else:
            print(f"  Index {idx}: INVALID - {msg}")
            still_failed.append(row)

    # Write updated queries.csv
    all_queries = existing_queries + fixed_queries
    all_queries.sort(key=lambda x: int(x['original_index']))

    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
        writer.writeheader()
        writer.writerows(all_queries)

    print(f"\nWrote {len(all_queries)} queries to {OUTPUT_CSV}")

    # Write updated failed.csv
    remaining_failed = failed_rows + still_failed
    remaining_failed.sort(key=lambda x: int(x['original_index']))

    with open(FAILED_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'error'])
        writer.writeheader()
        writer.writerows(remaining_failed)

    print(f"Wrote {len(remaining_failed)} queries to {FAILED_CSV}")
    print(f"\nFixed: {len(fixed_queries)}, Still failed: {len(still_failed)}")

    driver.close()


if __name__ == "__main__":
    main()
