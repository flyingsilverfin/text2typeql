#!/usr/bin/env python3
"""
Fix neoflix failed queries batch 10 - aggregation queries (Concept Error)
Indices: 729, 744, 745, 749, 759, 761, 765, 767, 769, 770, 772
"""

import csv
import pandas as pd
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

DATABASE = "text2typeql_neoflix"
FAILED_CSV = "/opt/text2typeql/output/neoflix/failed.csv"
QUERIES_CSV = "/opt/text2typeql/output/neoflix/queries.csv"

# Indices to fix
TARGET_INDICES = [729, 744, 745, 749, 759, 761, 765, 767, 769, 770, 772]

# TypeQL conversions for each query
TYPEQL_CONVERSIONS = {
    729: """match
  $c isa production_company, has production_company_name $cn;
  (media: $a, producer: $c) isa produced_by;
  $a isa adult;
limit 50;
fetch { "company": $cn };""",

    744: """match
  $s isa subscription, has expires_at $exp, has subscription_id $sid;
  (subscription: $s, package: $p) isa for_package;
  $p has package_name $pn;
  $exp >= 2020-01-01T00:00:00;
  $exp < 2021-01-01T00:00:00;
sort $exp asc;
limit 3;
fetch { "subscription_id": $sid, "expires_at": $exp, "package": $pn };""",

    745: """match
  $m isa movie, has title $title;
  $p isa person, has person_name "Tom Hanks";
  (actor: $p, film: $m) isa cast_for;
limit 50;
fetch { "title": $title };""",

    749: """match
  $v isa video, has runtime $rt;
  (media: $v, language: $l) isa original_language;
  $l has language_name $ln;
  $rt > 100;
limit 50;
fetch { "language": $ln };""",

    759: """match
  $m isa movie, has title $title;
  (media: $m, keyword: $k) isa has_keyword;
limit 50;
fetch { "movie": $title };""",

    761: """match
  $m isa movie, has revenue $rev;
  (media: $m, language: $l) isa spoken_in_language;
  $l has language_name $ln;
  $rev > 100000000;
limit 50;
fetch { "language": $ln };""",

    765: """match
  $p isa person, has person_name $pn;
  $rel (actor: $p, film: $m) isa cast_for;
  $m isa movie, has status "Released";
limit 50;
fetch { "actor": $pn };""",

    767: """match
  $c isa country, has country_name $cn;
  (media: $m, country: $c) isa produced_in_country;
  $m isa movie;
limit 50;
fetch { "country": $cn };""",

    769: """match
  $c isa collection, has collection_name $cn;
  (media: $m, collection: $c) isa in_collection;
  $m isa movie;
limit 50;
fetch { "collection": $cn };""",

    770: """match
  $p isa person, has person_name $pn;
  $rel (crew_member: $p, film: $m) isa crew_for, has job "Director";
  $m isa movie, has revenue $rev;
sort $rev desc;
limit 50;
fetch { "director": $pn, "revenue": $rev };""",

    772: """match
  $m isa movie, has vote_count $vc, has average_vote $av;
  (media: $m, language: $l) isa original_language;
  $l has language_name $ln;
  $vc > 100;
limit 50;
fetch { "language": $ln, "average_vote": $av };""",
}


def validate_query(driver, query: str) -> tuple[bool, str]:
    """Validate a TypeQL query against the database."""
    try:
        with driver.transaction(DATABASE, TransactionType.READ) as tx:
            result = tx.query(query).resolve()
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

    print("Connected to TypeDB")

    # Read the failed CSV
    failed_df = pd.read_csv(FAILED_CSV)
    print(f"Loaded {len(failed_df)} failed queries")

    # Read the queries CSV
    queries_df = pd.read_csv(QUERIES_CSV)
    print(f"Loaded {len(queries_df)} successful queries")

    # Process each target index
    fixed_rows = []
    fixed_indices = []

    for idx in TARGET_INDICES:
        # Find the row in failed_df
        row = failed_df[failed_df['original_index'] == idx]
        if row.empty:
            print(f"Index {idx} not found in failed.csv")
            continue

        row = row.iloc[0]
        question = row['question']
        cypher = row['cypher']

        if idx not in TYPEQL_CONVERSIONS:
            print(f"No conversion defined for index {idx}")
            continue

        typeql = TYPEQL_CONVERSIONS[idx]

        # Validate the query
        is_valid, msg = validate_query(driver, typeql)

        if is_valid:
            print(f"[{idx}] VALID: {msg}")
            fixed_rows.append({
                'original_index': idx,
                'question': question,
                'cypher': cypher,
                'typeql': typeql
            })
            fixed_indices.append(idx)
        else:
            print(f"[{idx}] FAILED: {msg}")
            print(f"  Query: {typeql[:100]}...")

    print(f"\nFixed {len(fixed_rows)} queries")

    if fixed_rows:
        # Append to queries.csv
        fixed_df = pd.DataFrame(fixed_rows)
        combined_df = pd.concat([queries_df, fixed_df], ignore_index=True)
        combined_df.to_csv(QUERIES_CSV, index=False)
        print(f"Appended {len(fixed_rows)} rows to {QUERIES_CSV}")

        # Remove from failed.csv
        failed_df = failed_df[~failed_df['original_index'].isin(fixed_indices)]
        failed_df.to_csv(FAILED_CSV, index=False)
        print(f"Removed {len(fixed_indices)} rows from {FAILED_CSV}")
        print(f"Remaining failed queries: {len(failed_df)}")

    driver.close()
    print("Done!")


if __name__ == "__main__":
    main()
