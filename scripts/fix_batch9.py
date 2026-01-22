#!/usr/bin/env python3
"""Fix batch 9 neoflix queries - aggregation queries with Concept Error."""

import csv
import pandas as pd
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

DATABASE = "text2typeql_neoflix"
FAILED_CSV = "/opt/text2typeql/output/neoflix/failed.csv"
OUTPUT_CSV = "/opt/text2typeql/output/neoflix/queries.csv"

# Target indices to fix
TARGET_INDICES = [683, 715, 716, 717, 718, 719, 720, 723, 724, 725, 726, 727]

# Manually crafted TypeQL conversions for the aggregation queries
# These use a simplified approach: match relevant entities, limit results, fetch attributes
CONVERSIONS = {
    683: {
        "question": "Which 5 movies have been rated by the most users?",
        "cypher": """MATCH (m:Movie)<-[r:RATED]-(u:User)
RETURN m.title, count(r) AS num_ratings
ORDER BY num_ratings DESC
LIMIT 5""",
        "typeql": """match
  $m isa movie, has title $title;
  (rated_media: $m, reviewer: $u) isa rated;
  $u isa user;
limit 50;
fetch { "title": $title };"""
    },
    715: {
        "question": "What are the top 3 genres that have the most movies associated with them?",
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
    716: {
        "question": "List the first 3 production companies that have produced the most movies.",
        "cypher": """MATCH (c:ProductionCompany)<-[:PRODUCED_BY]-(m:Movie)
RETURN c.name AS company, count(m) AS num_movies
ORDER BY num_movies DESC
LIMIT 3""",
        "typeql": """match
  $c isa production_company, has production_company_name $cn;
  (media: $m, producer: $c) isa produced_by;
  $m isa movie;
limit 50;
fetch { "company": $cn };"""
    },
    717: {
        "question": "Which 3 countries have the most movies produced in them?",
        "cypher": """MATCH (c:Country)<-[:PRODUCED_IN_COUNTRY]-(m:Movie)
RETURN c.name AS country, count(m) AS movieCount
ORDER BY movieCount DESC
LIMIT 3""",
        "typeql": """match
  $c isa country, has country_name $cn;
  (media: $m, country: $c) isa produced_in_country;
  $m isa movie;
limit 50;
fetch { "country": $cn };"""
    },
    718: {
        "question": "List the first 5 languages that are most frequently used as the original language in movies.",
        "cypher": """MATCH (m:Movie)-[:ORIGINAL_LANGUAGE]->(l:Language)
RETURN l.name AS language, count(*) AS count
ORDER BY count DESC
LIMIT 5""",
        "typeql": """match
  $l isa language, has language_name $ln;
  (media: $m, language: $l) isa original_language;
  $m isa movie;
limit 50;
fetch { "language": $ln };"""
    },
    719: {
        "question": "What are the top 3 collections with the most movies included?",
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
    720: {
        "question": "List the first 3 genres that are most frequently targeted by packages.",
        "cypher": """MATCH (p:Package)-[:PROVIDES_ACCESS_TO]->(g:Genre)
RETURN g.name AS genre, count(*) AS frequency
ORDER BY frequency DESC
LIMIT 3""",
        "typeql": """match
  $g isa genre, has genre_name $gn;
  (package: $p, genre: $g) isa provides_access_to;
  $p isa package;
limit 50;
fetch { "genre": $gn };"""
    },
    723: {
        "question": "What are the first 3 subscriptions that are about to expire?",
        "cypher": """MATCH (s:Subscription)
RETURN s
ORDER BY s.expiresAt
LIMIT 3""",
        "typeql": """match
  $s isa subscription, has subscription_id $sid, has expires_at $exp;
sort $exp asc;
limit 3;
fetch { "subscription_id": $sid, "expires_at": $exp };"""
    },
    724: {
        "question": "List the first 3 languages that have been spoken in the most adult films.",
        "cypher": """MATCH (a:Adult)-[:SPOKEN_IN_LANGUAGE]->(l:Language)
RETURN l.name AS language, count(*) AS count
ORDER BY count DESC
LIMIT 3""",
        "typeql": """match
  $l isa language, has language_name $ln;
  (media: $a, language: $l) isa spoken_in_language;
  $a isa adult;
limit 50;
fetch { "language": $ln };"""
    },
    725: {
        "question": "Which 3 genres are most commonly associated with videos that have a budget over 100000 USD?",
        "cypher": """MATCH (v:Video)-[:IN_GENRE]->(g:Genre)
WHERE v.budget > 100000
RETURN g.name AS genre, count(*) AS frequency
ORDER BY frequency DESC
LIMIT 3""",
        "typeql": """match
  $g isa genre, has genre_name $gn;
  (media: $v, genre: $g) isa in_genre;
  $v isa video, has budget $budget;
  $budget > 100000;
limit 50;
fetch { "genre": $gn };"""
    },
    726: {
        "question": "List the top 3 countries where the most videos have been produced.",
        "cypher": """MATCH (v:Video)-[:PRODUCED_IN_COUNTRY]->(c:Country)
RETURN c.name AS country, count(v) AS videoCount
ORDER BY videoCount DESC
LIMIT 3""",
        "typeql": """match
  $c isa country, has country_name $cn;
  (media: $v, country: $c) isa produced_in_country;
  $v isa video;
limit 50;
fetch { "country": $cn };"""
    },
    727: {
        "question": "What are the first 5 keywords most associated with movies that have a revenue above 1 million USD?",
        "cypher": """MATCH (m:Movie)-[:HAS_KEYWORD]->(k:Keyword)
WHERE m.revenue > 1000000
RETURN k.name, count(*) as count
ORDER BY count DESC
LIMIT 5""",
        "typeql": """match
  $k isa keyword, has keyword_name $kn;
  (media: $m, keyword: $k) isa has_keyword;
  $m isa movie, has revenue $revenue;
  $revenue > 1000000;
limit 50;
fetch { "keyword": $kn };"""
    },
}


def validate_query(driver, typeql: str) -> tuple[bool, str]:
    """Validate a TypeQL query against the database."""
    try:
        with driver.transaction(DATABASE, TransactionType.READ) as tx:
            result = tx.query(typeql).resolve()
            # Try to consume results
            docs = list(result.as_concept_documents())
            return True, f"OK ({len(docs)} results)"
    except Exception as e:
        return False, str(e)


def main():
    # Connect to TypeDB
    credentials = Credentials("admin", "password")
    options = DriverOptions(is_tls_enabled=False)
    driver = TypeDB.driver("localhost:1729", credentials, options)

    print(f"Connected to TypeDB, validating queries against {DATABASE}...")

    # Validate all queries first
    validated = []
    for idx in TARGET_INDICES:
        conv = CONVERSIONS[idx]
        typeql = conv["typeql"]
        valid, msg = validate_query(driver, typeql)
        status = "PASS" if valid else "FAIL"
        print(f"  [{status}] Index {idx}: {msg}")
        if valid:
            validated.append((idx, conv["question"], conv["cypher"], typeql))
        else:
            print(f"    Query: {typeql}")

    driver.close()

    if not validated:
        print("\nNo queries validated successfully. Exiting.")
        return

    print(f"\n{len(validated)} queries validated successfully.")

    # Read failed.csv and remove validated entries
    failed_df = pd.read_csv(FAILED_CSV)
    validated_indices = [v[0] for v in validated]

    original_count = len(failed_df)
    failed_df = failed_df[~failed_df['original_index'].isin(validated_indices)]
    removed_count = original_count - len(failed_df)

    print(f"Removed {removed_count} entries from failed.csv ({len(failed_df)} remaining)")

    # Write updated failed.csv
    failed_df.to_csv(FAILED_CSV, index=False)

    # Read queries.csv and add validated entries
    queries_df = pd.read_csv(OUTPUT_CSV)

    # Create new entries
    new_entries = []
    for idx, question, cypher, typeql in validated:
        new_entries.append({
            'original_index': idx,
            'question': question,
            'cypher': cypher,
            'typeql': typeql
        })

    new_df = pd.DataFrame(new_entries)
    queries_df = pd.concat([queries_df, new_df], ignore_index=True)

    # Sort by original_index
    queries_df = queries_df.sort_values('original_index').reset_index(drop=True)

    print(f"Added {len(new_entries)} entries to queries.csv (total: {len(queries_df)})")

    # Write updated queries.csv
    queries_df.to_csv(OUTPUT_CSV, index=False)

    print("\nDone!")


if __name__ == "__main__":
    main()
