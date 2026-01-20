#!/usr/bin/env python3
"""Batch convert Cypher queries to TypeQL with validation."""

import csv
import json
import subprocess
import sys
import re
from pathlib import Path
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# Schema summary for recommendations database
SCHEMA = """
Entities:
- movie: url, runtime, revenue, plot_embedding, poster_embedding, imdb_rating, released, countries, languages, plot, imdb_votes, imdb_id, year, poster, movie_id(@key), tmdb_id, title, budget
- genre: name(@key)
- user: user_id(@key), name
- person: url, name, tmdb_id(@key), born_in, bio, died, born, imdb_id, poster

Relations:
- in_genre: (film: movie, genre: genre)
- rated: (user: user, film: movie) [owns: rating, timestamp]
- acted_in: (actor: person, film: movie) [owns: character_role]
- directed: (director: person, film: movie) [owns: character_role]
"""

def get_query(database: str, index: int) -> dict:
    """Get query from dataset."""
    result = subprocess.run(
        ["python3", "/opt/text2typeql/scripts/get_query.py", database, str(index)],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        return None
    return json.loads(result.stdout)

def validate_typeql(driver, database: str, typeql: str) -> tuple[bool, str]:
    """Validate TypeQL query against TypeDB."""
    try:
        with driver.transaction(f"text2typeql_{database}", TransactionType.READ) as tx:
            result = tx.query(typeql).resolve()
            # Try to consume the results
            try:
                list(result.as_concept_documents())
            except:
                try:
                    list(result.as_concept_rows())
                except:
                    pass
        return True, ""
    except Exception as e:
        return False, str(e)

def append_success(database: str, index: int, question: str, cypher: str, typeql: str):
    """Append successful conversion to queries.csv."""
    path = Path(f"/opt/text2typeql/output/{database}/queries.csv")
    # Escape for CSV
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([index, question, cypher, typeql])

def append_failure(database: str, index: int, question: str, cypher: str, error: str):
    """Append failed conversion to failed.csv."""
    path = Path(f"/opt/text2typeql/output/{database}/failed.csv")
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([index, question, cypher, error])

if __name__ == "__main__":
    # Connect to TypeDB
    driver = TypeDB.driver(
        "localhost:1729",
        Credentials("admin", "password"),
        DriverOptions(is_tls_enabled=False)
    )

    database = "recommendations"

    # Test connection
    try:
        with driver.transaction(f"text2typeql_{database}", TransactionType.READ) as tx:
            tx.query("match $m isa movie; limit 1; fetch { \"t\": $m.title };").resolve()
        print("TypeDB connection OK")
    except Exception as e:
        print(f"TypeDB connection failed: {e}")
        sys.exit(1)

    print("Ready to validate queries")
