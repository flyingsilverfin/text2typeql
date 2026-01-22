#!/usr/bin/env python3
"""
Convert remaining failed Cypher queries to TypeQL for the companies database.
Focus on fixing the investor type issues - in this schema only persons can be investors.
"""

import pandas as pd
import csv
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# Connect to TypeDB
credentials = Credentials("admin", "password")
options = DriverOptions(is_tls_enabled=False)
driver = TypeDB.driver("localhost:1729", credentials, options)

def validate_query(typeql_query):
    """Validate a TypeQL query against the database."""
    try:
        with driver.transaction("text2typeql_companies", TransactionType.READ) as tx:
            result = tx.query(typeql_query).resolve()
            # Try to consume some results to ensure the query is valid
            if hasattr(result, 'as_concept_documents'):
                docs = list(result.as_concept_documents())
            return True, None
    except Exception as e:
        return False, str(e)

# Read the current failed queries
failed_df = pd.read_csv('/opt/text2typeql/output/companies/failed.csv')
print(f"Remaining failed queries: {len(failed_df)}")

# Key insight from schema analysis:
# - organization plays invested_in:organization (receives investment)
# - person plays invested_in:investor (is the investor)
# - Organizations CANNOT be investors in this schema!
#
# Many of the failed queries ask about organizations as investors which is not
# supported by the schema. We need to either:
# 1. Convert them to use person as investor
# 2. Mark them as schema mismatch

def convert_query(idx, question, cypher):
    """Convert a Cypher query to TypeQL based on the question."""

    # Queries that explicitly require organizations as investors - cannot convert
    org_investor_queries = [7, 53, 70, 77, 94, 154, 212, 311, 348, 352, 409, 439, 466, 496, 612, 639, 667, 700, 732, 770]
    if idx in org_investor_queries:
        return None, "Schema mismatch - organizations cannot be investors, only persons can"

    # Query 189: Who are the first 3 investors in organizations that have a revenue greater than 500 million?
    # Changed to person investors
    if idx == 189:
        return '''match
  $o isa organization;
  $investor isa person, has name $in;
  (organization: $o, investor: $investor) isa invested_in;
  $o has revenue $r;
  $r > 500000000;
limit 3;
fetch { "investor": $in };''', None

    # Query 243: Who are the investors of 'New Energy Group'?
    # Changed to person investors
    if idx == 243:
        return '''match
  $o isa organization, has name "New Energy Group";
  $investor isa person, has name $in;
  (organization: $o, investor: $investor) isa invested_in;
fetch { "investor": $in };''', None

    # Query 329: Name the first 3 organizations that are headquartered in cities with more than 5 million inhabitants.
    # No population attribute in schema
    if idx == 329:
        return None, "Schema mismatch - city entity does not have population attribute"

    # Query 338: List the names of the cities where the headquarters of the first 5 organizations founded before 1950 are located.
    # No foundingDate attribute in schema
    if idx == 338:
        return None, "Schema mismatch - organization entity does not have foundingDate attribute"

    # Query 356: Which 3 organizations have the longest history of continuous operation according to their founding dates?
    # No foundingDate attribute
    if idx == 356:
        return None, "Schema mismatch - organization entity does not have foundingDate attribute"

    # Query 367: Name the top 3 organizations in terms of revenue that are headquartered in countries with developing economies.
    # Fix the syntax - use like instead of = for string comparison in disjunction
    if idx == 367:
        return '''match
  $o isa organization, has name $on;
  $c isa country, has country_name $cn;
  $city isa city;
  (organization: $o, city: $city) isa located_in;
  (city: $city, country: $c) isa in_country;
  { $cn like "China"; } or { $cn like "India"; } or { $cn like "Brazil"; } or { $cn like "Russia"; } or { $cn like "Mexico"; } or { $cn like "Indonesia"; } or { $cn like "Turkey"; };
  $o has revenue $r;
sort $r desc;
limit 3;
fetch { "organization": $on, "revenue": $r };''', None

    # Query 391: Which 3 organizations have the most patents filed according to the database?
    # No nbrPatents attribute
    if idx == 391:
        return None, "Schema mismatch - organization entity does not have nbrPatents attribute"

    # Query 411: What are the names of organizations that have at least one investor but no subsidiaries?
    # Changed to person investors
    if idx == 411:
        return '''match
  $o isa organization, has name $on;
  $investor isa person;
  (organization: $o, investor: $investor) isa invested_in;
  not { (parent: $o, subsidiary: $sub) isa subsidiary_of; $sub isa organization; };
fetch { "organization": $on };''', None

    # Query 448: Which organizations are based in a city with a population less than 100,000?
    # No population attribute
    if idx == 448:
        return None, "Schema mismatch - city entity does not have population attribute"

    # Query 452: Which organizations have CEOs who have been in their position for less than 3 years?
    # No startDate attribute
    if idx == 452:
        return None, "Schema mismatch - person entity does not have startDate attribute"

    # Query 548: Who are the investors of 'Deja vu Security'?
    # Changed to person investors
    if idx == 548:
        return '''match
  $o isa organization, has name "Deja vu Security";
  $investor isa person, has name $in;
  (organization: $o, investor: $investor) isa invested_in;
fetch { "investor": $in };''', None

    # Query 616: Who are the investors in 'New Energy Group'?
    # Can only return person investors
    if idx == 616:
        return '''match
  $o isa organization, has name "New Energy Group";
  $investor isa person, has name $in;
  (organization: $o, investor: $investor) isa invested_in;
fetch { "investor": $in };''', None

    # Query 695: Which 3 organizations have the oldest founding dates?
    # No foundingDate attribute
    if idx == 695:
        return None, "Schema mismatch - organization entity does not have foundingDate attribute"

    # Query 826: Who are the investors of organizations with a revenue exceeding $500 million?
    # Changed to person investors
    if idx == 826:
        return '''match
  $o isa organization, has name $on;
  $investor isa person, has name $in;
  (organization: $o, investor: $investor) isa invested_in;
  $o has revenue $r;
  $r > 500000000;
fetch { "organization": $on, "investor": $in };''', None

    return None, "Query not handled"

# Process each query
successful = []
still_failed = []

for _, row in failed_df.iterrows():
    idx = row['original_index']
    question = row['question']
    cypher = row['cypher']

    typeql, reason = convert_query(idx, question, cypher)

    if typeql is None:
        error_msg = reason if reason else 'Cannot convert - schema mismatch or unsupported pattern'
        still_failed.append({
            'original_index': idx,
            'question': question,
            'cypher': cypher,
            'error': error_msg
        })
        print(f"Query {idx}: Cannot convert - {error_msg[:50]}")
        continue

    # Validate the query
    valid, error = validate_query(typeql)

    if valid:
        successful.append({
            'original_index': idx,
            'question': question,
            'cypher': cypher,
            'typeql': typeql
        })
        print(f"Query {idx}: SUCCESS")
    else:
        still_failed.append({
            'original_index': idx,
            'question': question,
            'cypher': cypher,
            'error': error
        })
        print(f"Query {idx}: FAILED - {error[:100]}")

print(f"\n=== SUMMARY ===")
print(f"Successfully converted: {len(successful)}")
print(f"Still failed: {len(still_failed)}")

# Save results
if successful:
    # Read existing queries.csv and append
    existing_df = pd.read_csv('/opt/text2typeql/output/companies/queries.csv')
    new_df = pd.DataFrame(successful)
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    combined_df.to_csv('/opt/text2typeql/output/companies/queries.csv', index=False, quoting=csv.QUOTE_ALL)
    print(f"Appended {len(successful)} queries to queries.csv")

# Save still-failed queries
still_failed_df = pd.DataFrame(still_failed)
still_failed_df.to_csv('/opt/text2typeql/output/companies/failed.csv', index=False, quoting=csv.QUOTE_ALL)
print(f"Updated failed.csv with {len(still_failed)} queries")

driver.close()
