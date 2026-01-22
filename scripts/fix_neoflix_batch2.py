#!/usr/bin/env python3
"""Fix neoflix failed queries batch 2 - constraints appearing after limit/fetch."""

import csv
import sys
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

DATABASE = "text2typeql_neoflix"

# Define the fixes for each query index
# Format: (original_index, question, cypher, fixed_typeql)
FIXES = [
    (
        128,
        "Which adult films have been released after 2010?",
        """MATCH (a:Adult)
WHERE a.release_date > date('2010-01-01')
RETURN a.title, a.release_date""",
        """match
  $a isa adult;
  $a has release_date $release_date;
  $a has title $title;
  $release_date > 2010-01-01;
fetch {
  "title": $title, "release_date": $release_date
};"""
    ),
    (
        138,
        "What are the top 3 movies by vote count that were released in the 1990s?",
        """MATCH (m:Movie)
WHERE m.release_date >= date('1990-01-01') AND m.release_date < date('2000-01-01')
RETURN m.title, m.vote_count
ORDER BY m.vote_count DESC
LIMIT 3""",
        """match
  $m isa movie;
  $m has release_date $release_date;
  $m has title $title;
  $m has vote_count $vote_count;
  $release_date >= 1990-01-01;
  $release_date < 2000-01-01;
sort $vote_count desc;
limit 3;
fetch {
  "title": $title, "vote_count": $vote_count
};"""
    ),
    (
        155,
        "List the first 3 movies with a revenue of zero.",
        """MATCH (m:Movie)
WHERE m.revenue = 0
RETURN m.title
LIMIT 3""",
        """match
  $m isa movie;
  $m has revenue $revenue;
  $m has title $title;
  $revenue == 0;
limit 3;
fetch {
  "title": $title
};"""
    ),
    (
        162,
        "List the first 3 videos with no revenue reported.",
        """MATCH (v:Video)
WHERE v.revenue IS NULL OR v.revenue = 0
RETURN v.title, v.revenue
LIMIT 3""",
        """match
  $v isa video;
  $v has revenue $revenue;
  $v has title $title;
  $revenue == 0;
limit 3;
fetch {
  "title": $title, "revenue": $revenue
};"""
    ),
    (
        168,
        "What are the first 3 videos that have been rated higher than 8.0?",
        """MATCH (v:Video)<-[r:RATED]-(u:User)
WHERE r.rating > 8.0
RETURN v.title, r.rating
ORDER BY r.rating DESC
LIMIT 3""",
        """match
  $v isa video;
  $u isa user;
  $r (rated_media: $v, reviewer: $u) isa rated, has rating $rating;
  $v has title $title_v;
  $rating > 8.0;
sort $rating desc;
limit 3;
fetch {
  "title": $title_v, "rating": $rating
};"""
    ),
    (
        180,
        "Which 3 highest budget movies were released after 2000?",
        """MATCH (m:Movie)
WHERE m.release_date > date('2000-01-01') AND m.budget IS NOT NULL
RETURN m.title, m.budget
ORDER BY m.budget DESC
LIMIT 3""",
        """match
  $m isa movie;
  $m has release_date $release_date;
  $m has budget $budget;
  $m has title $title;
  $release_date > 2000-01-01;
sort $budget desc;
limit 3;
fetch {
  "title": $title, "budget": $budget
};"""
    ),
    (
        232,
        "List the top 3 highest grossing movies in the genre 'Action'.",
        """MATCH (m:Movie)-[:IN_GENRE]->(:Genre {name: 'Action'})
RETURN m.title, m.revenue
ORDER BY m.revenue DESC
LIMIT 3""",
        """match
  $m isa movie;
  $m has title $title;
  $m has revenue $revenue;
  $genre isa genre, has genre_name "Action";
  (media: $m, genre: $genre) isa in_genre;
sort $revenue desc;
limit 3;
fetch {
  "title": $title, "revenue": $revenue
};"""
    ),
    (
        233,
        "What are the first 5 movies that have a character played by a person with gender 1?",
        """MATCH (p:Person)-[c:CAST_FOR]->(m:Movie)
WHERE p.gender = 1
RETURN m.title
LIMIT 5""",
        """match
  $p isa person;
  $m isa movie;
  $c (actor: $p, film: $m) isa cast_for;
  $p has gender $gender_p;
  $m has title $title_m;
  $gender_p == 1;
limit 5;
fetch {
  "title": $title_m
};"""
    ),
    (
        248,
        "List the first 3 movies released after 2010.",
        """MATCH (m:Movie)
WHERE m.release_date > date('2010-01-01')
RETURN m.title, m.release_date
ORDER BY m.release_date
LIMIT 3""",
        """match
  $m isa movie;
  $m has release_date $release_date;
  $m has title $title;
  $release_date > 2010-01-01;
sort $release_date asc;
limit 3;
fetch {
  "title": $title, "release_date": $release_date
};"""
    ),
]


def validate_query(driver, query: str) -> tuple[bool, str]:
    """Validate a TypeQL query against TypeDB."""
    try:
        with driver.transaction(DATABASE, TransactionType.READ) as tx:
            result = tx.query(query).resolve()
            # Try to consume results to ensure query is valid
            for doc in result.as_concept_documents():
                pass
        return True, ""
    except Exception as e:
        return False, str(e)


def main():
    # Connect to TypeDB
    credentials = Credentials("admin", "password")
    options = DriverOptions(is_tls_enabled=False)

    try:
        driver = TypeDB.driver("localhost:1729", credentials, options)
    except Exception as e:
        print(f"Error connecting to TypeDB: {e}")
        sys.exit(1)

    # Check database exists
    if not driver.databases.contains(DATABASE):
        print(f"Database {DATABASE} does not exist")
        sys.exit(1)

    print(f"Connected to TypeDB, database: {DATABASE}")

    # Validate and collect fixed queries
    valid_fixes = []

    for original_index, question, cypher, typeql in FIXES:
        print(f"\nValidating query {original_index}: {question[:50]}...")

        is_valid, error = validate_query(driver, typeql)

        if is_valid:
            print(f"  VALID")
            valid_fixes.append((original_index, question, cypher, typeql))
        else:
            print(f"  INVALID: {error}")

    print(f"\n{len(valid_fixes)} of {len(FIXES)} queries are valid")

    # Append valid fixes to queries.csv
    if valid_fixes:
        output_csv = "/opt/text2typeql/output/neoflix/queries.csv"

        with open(output_csv, 'a', newline='') as f:
            writer = csv.writer(f)
            for original_index, question, cypher, typeql in valid_fixes:
                writer.writerow([original_index, question, cypher, typeql])

        print(f"\nAppended {len(valid_fixes)} fixed queries to {output_csv}")
        print("Fixed indices:", [fix[0] for fix in valid_fixes])

    driver.close()


if __name__ == "__main__":
    main()
