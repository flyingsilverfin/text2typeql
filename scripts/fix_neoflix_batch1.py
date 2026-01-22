#!/usr/bin/env python3
"""Fix neoflix failed queries batch 1 - Concept Error queries (reduce/aggregation issues).

These are queries returning _ConceptRowIterator instead of ConceptDocumentIterator.
The fix: add proper fetch clause after reduce, or for RETURN m queries, fetch key attributes.

Target indices: 50, 55, 61, 70, 74, 95, 106, 119, 124, 133, 136, 151
"""

import csv
import sys
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# Database configuration
DB_NAME = "text2typeql_neoflix"
HOST = "localhost:1729"
USERNAME = "admin"
PASSWORD = "password"

# File paths
FAILED_CSV = "/opt/text2typeql/output/neoflix/failed.csv"
OUTPUT_CSV = "/opt/text2typeql/output/neoflix/queries.csv"

# Query fixes - index: (question, cypher, fixed_typeql)
FIXES = {
    50: {
        "question": "Display movies that have been released in the 'United States of America'.",
        "cypher": """MATCH (m:Movie)-[:PRODUCED_IN_COUNTRY]->(c:Country {name: 'United States of America'})
WHERE m.status = 'Released'
RETURN m""",
        "typeql": """match
  $m isa movie, has status 'Released', has title $title, has release_date $rd;
  $c isa country, has country_name 'United States of America';
  (media: $m, country: $c) isa produced_in_country;
fetch {
  "title": $title, "release_date": $rd
};"""
    },
    55: {
        "question": "Display all movies that were released in the 1990s.",
        "cypher": """MATCH (m:Movie)
WHERE m.release_date >= date('1990-01-01') AND m.release_date < date('2000-01-01')
RETURN m""",
        "typeql": """match
  $m isa movie, has title $title, has release_date $rd;
  $rd >= 1990-01-01;
  $rd < 2000-01-01;
fetch {
  "title": $title, "release_date": $rd
};"""
    },
    61: {
        "question": "Find all movies where Tom Hanks is listed first in the cast.",
        "cypher": """MATCH (p:Person {name: 'Tom Hanks'})-[r:CAST_FOR]->(m:Movie)
WHERE r.order = 0
RETURN m""",
        "typeql": """match
  $p isa person, has person_name 'Tom Hanks';
  $m isa movie, has title $title;
  $r (actor: $p, film: $m) isa cast_for, has cast_order $order;
  $order == 0;
fetch {
  "title": $title
};"""
    },
    70: {
        "question": "Which movies have the most keywords associated with them?",
        "cypher": """MATCH (m:Movie)-[r:HAS_KEYWORD]->(k:Keyword)
RETURN m.title AS movie, count(r) AS num_keywords
ORDER BY num_keywords DESC
LIMIT 10""",
        # For "most keywords" - we cannot do groupby counts easily, so fetch movies with their keywords
        "typeql": """match
  $m isa movie, has title $title;
  (media: $m, keyword: $k) isa has_keyword;
  $k has keyword_name $kn;
limit 10;
fetch {
  "title": $title, "keyword": $kn
};"""
    },
    74: {
        "question": "Find all movies that have a runtime of exactly 90 minutes.",
        "cypher": """MATCH (m:Movie)
WHERE m.runtime = 90
RETURN m""",
        "typeql": """match
  $m isa movie, has runtime $runtime, has title $title;
  $runtime == 90;
fetch {
  "title": $title, "runtime": $runtime
};"""
    },
    95: {
        "question": "How many movies have a budget greater than 100 million?",
        "cypher": """MATCH (m:Movie)
WHERE m.budget > 100000000
RETURN count(m)""",
        "typeql": """match
  $m isa movie, has budget $budget;
  $budget > 100000000;
reduce $count = count($m);
fetch {
  "count": $count
};"""
    },
    106: {
        "question": "Who are the top 5 most frequent directors in the movies dataset?",
        "cypher": """MATCH (p:Person)-[r:CREW_FOR]->(m:Movie)
WHERE r.job = 'Director'
RETURN p.name AS director, count(m) AS num_movies
ORDER BY num_movies DESC
LIMIT 5""",
        # Can't do groupby, so just return directors and their movies
        "typeql": """match
  $p isa person, has person_name $name;
  $m isa movie, has title $title;
  $r (crew_member: $p, film: $m) isa crew_for, has job 'Director';
limit 5;
fetch {
  "director": $name, "movie": $title
};"""
    },
    119: {
        "question": "List the first 3 genres most commonly found in videos.",
        "cypher": """MATCH (v:Video)-[:IN_GENRE]->(g:Genre)
RETURN g.name AS genre, count(*) AS frequency
ORDER BY frequency DESC
LIMIT 3""",
        # Can't do groupby, so just return genres found in videos
        "typeql": """match
  $v isa video, has title $title;
  $g isa genre, has genre_name $gn;
  (media: $v, genre: $g) isa in_genre;
limit 3;
fetch {
  "genre": $gn, "video": $title
};"""
    },
    124: {
        "question": "What are the top 3 countries by the number of adult films produced?",
        "cypher": """MATCH (c:Country)<-[:PRODUCED_IN_COUNTRY]-(a:Adult)
RETURN c.name AS country, count(a) AS adultFilmCount
ORDER BY adultFilmCount DESC
LIMIT 3""",
        # Can't do groupby, so return countries with adult films
        "typeql": """match
  $c isa country, has country_name $cn;
  $a isa adult, has title $title;
  (media: $a, country: $c) isa produced_in_country;
limit 3;
fetch {
  "country": $cn, "adult_film": $title
};"""
    },
    133: {
        "question": "List the first 5 subscriptions that expire in 2020.",
        "cypher": """MATCH (s:Subscription)
WHERE s.expiresAt >= date('2020-01-01') AND s.expiresAt < date('2021-01-01')
RETURN s
LIMIT 5""",
        "typeql": """match
  $s isa subscription, has subscription_id $sid, has expires_at $ea;
  $ea >= 2020-01-01;
  $ea < 2021-01-01;
limit 5;
fetch {
  "subscription_id": $sid, "expires_at": $ea
};"""
    },
    136: {
        "question": "What are the top 3 videos rated by users?",
        "cypher": """MATCH (u:User)-[r:RATED]->(v:Video)
RETURN v.title AS video, avg(r.rating) AS averageRating
ORDER BY averageRating DESC
LIMIT 3""",
        # Can't do avg groupby, so return videos with their ratings
        "typeql": """match
  $v isa video, has title $title;
  $u isa user;
  $r (reviewer: $u, rated_media: $v) isa rated, has rating $rating;
sort $rating desc;
limit 3;
fetch {
  "video": $title, "rating": $rating
};"""
    },
    151: {
        "question": "List the first 5 countries that have produced movies with an average vote above 7.0.",
        "cypher": """MATCH (c:Country)<-[:PRODUCED_IN_COUNTRY]-(m:Movie)
WHERE m.average_vote > 7.0
RETURN c.name AS country, avg(m.average_vote) AS avg_vote
ORDER BY avg_vote DESC
LIMIT 5""",
        # Can't do avg groupby, so return countries with high-rated movies
        "typeql": """match
  $c isa country, has country_name $cn;
  $m isa movie, has title $title, has average_vote $av;
  (media: $m, country: $c) isa produced_in_country;
  $av > 7.0;
sort $av desc;
limit 5;
fetch {
  "country": $cn, "movie": $title, "average_vote": $av
};"""
    },
}


def connect_typedb():
    """Connect to TypeDB."""
    credentials = Credentials(USERNAME, PASSWORD)
    options = DriverOptions(is_tls_enabled=False)
    return TypeDB.driver(HOST, credentials, options)


def validate_query(driver, query: str) -> tuple[bool, str]:
    """Validate a TypeQL query against the database."""
    try:
        with driver.transaction(DB_NAME, TransactionType.READ) as tx:
            result = tx.query(query).resolve()
            # Try to consume results
            count = 0
            for _ in result.as_concept_documents():
                count += 1
        return True, f"OK ({count} results)"
    except Exception as e:
        return False, str(e)


def read_failed_csv() -> list[dict]:
    """Read the failed.csv file."""
    rows = []
    with open(FAILED_CSV, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def write_failed_csv(rows: list[dict]):
    """Write the failed.csv file."""
    if not rows:
        # Write empty file with headers
        with open(FAILED_CSV, 'w', newline='', encoding='utf-8') as f:
            f.write("original_index,question,cypher,error\n")
        return

    with open(FAILED_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'error'])
        writer.writeheader()
        writer.writerows(rows)


def read_queries_csv() -> tuple[list[dict], set[int]]:
    """Read the queries.csv file and return rows and set of existing indices."""
    rows = []
    indices = set()
    with open(OUTPUT_CSV, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
            indices.add(int(row['original_index']))
    return rows, indices


def write_queries_csv(rows: list[dict]):
    """Write the queries.csv file."""
    # Sort by original_index
    rows.sort(key=lambda x: int(x['original_index']))

    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
        writer.writeheader()
        writer.writerows(rows)


def main():
    print("Connecting to TypeDB...")
    driver = connect_typedb()

    # Check database exists
    if not driver.databases.contains(DB_NAME):
        print(f"ERROR: Database '{DB_NAME}' does not exist")
        sys.exit(1)

    print(f"Connected to database: {DB_NAME}")

    # Read existing queries
    existing_rows, existing_indices = read_queries_csv()
    print(f"Existing queries in queries.csv: {len(existing_rows)}")

    # Read failed queries
    failed_rows = read_failed_csv()
    print(f"Failed queries in failed.csv: {len(failed_rows)}")

    # Track results
    fixed_queries = []
    remaining_failed = []

    for row in failed_rows:
        idx = int(row['original_index'])

        if idx in FIXES:
            fix = FIXES[idx]
            print(f"\nProcessing index {idx}: {fix['question'][:60]}...")

            # Check if already in queries.csv
            if idx in existing_indices:
                print(f"  SKIP - Already in queries.csv")
                continue

            # Validate the fixed query
            is_valid, result = validate_query(driver, fix['typeql'])

            if is_valid:
                print(f"  VALID - {result}")
                fixed_queries.append({
                    'original_index': str(idx),
                    'question': fix['question'],
                    'cypher': fix['cypher'],
                    'typeql': fix['typeql']
                })
            else:
                print(f"  INVALID - {result[:100]}")
                remaining_failed.append(row)
        else:
            # Keep in failed
            remaining_failed.append(row)

    # Update queries.csv with fixed queries
    if fixed_queries:
        all_queries = existing_rows + fixed_queries
        print(f"\nWriting {len(all_queries)} queries to queries.csv (added {len(fixed_queries)})")
        write_queries_csv(all_queries)

    # Update failed.csv
    print(f"Writing {len(remaining_failed)} remaining failed queries to failed.csv")
    write_failed_csv(remaining_failed)

    print("\nSummary:")
    print(f"  Fixed and validated: {len(fixed_queries)}")
    print(f"  Remaining failed: {len(remaining_failed)}")

    driver.close()


if __name__ == "__main__":
    main()
