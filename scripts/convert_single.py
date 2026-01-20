#!/usr/bin/env python3
"""Convert a single Cypher query to TypeQL and validate."""

import csv
import json
import subprocess
import sys
from pathlib import Path
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

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
            try:
                docs = list(result.as_concept_documents())
                return True, f"OK ({len(docs)} results)"
            except:
                try:
                    rows = list(result.as_concept_rows())
                    return True, f"OK ({len(rows)} rows)"
                except:
                    return True, "OK"
    except Exception as e:
        return False, str(e)

def append_success(database: str, index: int, question: str, cypher: str, typeql: str):
    """Append successful conversion to queries.csv."""
    path = Path(f"/opt/text2typeql/output/{database}/queries.csv")
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
    if len(sys.argv) < 4:
        print("Usage: python convert_single.py <database> <index> <typeql>")
        sys.exit(1)

    database = sys.argv[1]
    index = int(sys.argv[2])
    typeql = sys.argv[3]

    # Get original query
    query_data = get_query(database, index)
    if not query_data:
        print(f"Failed to get query {index}")
        sys.exit(1)

    # Connect and validate
    driver = TypeDB.driver(
        "localhost:1729",
        Credentials("admin", "password"),
        DriverOptions(is_tls_enabled=False)
    )

    success, msg = validate_typeql(driver, database, typeql)

    if success:
        append_success(database, index, query_data["question"], query_data["cypher"], typeql)
        print(f"SUCCESS: {msg}")
    else:
        print(f"FAILED: {msg}")
        sys.exit(1)
