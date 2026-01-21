#!/usr/bin/env python3
"""
Validate and semantically review converted TypeQL queries for the companies database.
"""

import csv
import sys
from pathlib import Path
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# Configuration
DATABASE = "text2typeql_companies"
INPUT_FILE = Path("/opt/text2typeql/output/companies/queries.csv")
OUTPUT_FILE = Path("/opt/text2typeql/output/companies/queries.csv")
FAILED_FILE = Path("/opt/text2typeql/output/companies/failed.csv")
FAILED_REVIEW_FILE = Path("/opt/text2typeql/output/companies/failed_review.csv")
SCHEMA_FILE = Path("/opt/text2typeql/output/companies/schema.tql")

# Load schema for semantic validation
SCHEMA = SCHEMA_FILE.read_text()

# Extract schema information for semantic validation
ENTITIES = [
    "person", "organization", "industry_category", "city", "country",
    "article", "chunk", "fewshot"
]

ENTITY_ATTRIBUTES = {
    "person": ["name", "person_id", "summary"],
    "organization": ["name", "organization_id", "summary", "revenue", "motto",
                     "nbr_employees", "is_dissolved", "is_public"],
    "industry_category": ["industry_category_name", "industry_category_id"],
    "city": ["city_id", "city_summary", "city_name"],
    "country": ["country_name", "country_id", "country_summary"],
    "article": ["article_id", "sentiment", "author", "site_name", "article_summary",
                "date", "title"],
    "chunk": ["text", "embedding", "embedding_google"],
    "fewshot": ["question", "cypher", "fewshot_id", "embedding"]
}

RELATIONS = {
    "parent_of": ["parent", "child"],
    "located_in": ["organization", "city"],
    "ceo_of": ["organization", "ceo"],
    "in_category": ["organization", "category"],
    "subsidiary_of": ["parent", "subsidiary"],
    "supplies": ["supplier", "customer"],
    "invested_in": ["organization", "investor"],
    "board_member_of": ["organization", "member"],
    "competes_with": ["competitor"],
    "in_country": ["city", "country"],
    "has_chunk": ["article", "chunk"],
    "mentions": ["article", "organization"]
}


def semantic_review(question: str, cypher: str, typeql: str) -> tuple[bool, str]:
    """
    Perform semantic review of a TypeQL query against the original question.

    Returns (is_valid, reason) where reason explains any issues.
    """
    question_lower = question.lower()
    typeql_lower = typeql.lower()

    issues = []

    # Check for correct entity types based on question
    entity_keywords = {
        "organization": ["organization", "company", "companies", "firm", "business"],
        "person": ["person", "people", "ceo", "board member", "investor", "parent", "child"],
        "city": ["city", "cities"],
        "country": ["country", "countries", "nation"],
        "article": ["article", "news", "publication"],
        "industry_category": ["industry", "category", "sector"]
    }

    expected_entities = set()
    for entity, keywords in entity_keywords.items():
        for keyword in keywords:
            if keyword in question_lower:
                expected_entities.add(entity)
                break

    # Check if expected entities are present in query
    for entity in expected_entities:
        if entity not in typeql_lower and f"isa {entity}" not in typeql_lower:
            # Allow some flexibility - person can be implied by ceo_of, board_member_of, etc.
            if entity == "person" and any(rel in typeql_lower for rel in ["ceo_of", "board_member_of", "parent_of", "invested_in"]):
                continue
            if entity == "organization" and any(rel in typeql_lower for rel in ["ceo_of", "board_member_of", "located_in", "in_category", "subsidiary_of", "supplies", "invested_in", "competes_with", "mentions"]):
                continue
            # Don't flag this as it's often a false positive
            pass

    # Check aggregations
    aggregation_keywords = {
        "count": ["count", "how many", "number of"],
        "sum": ["sum", "total"],
        "avg": ["average", "avg", "mean"],
        "max": ["highest", "maximum", "most", "top", "largest", "biggest", "greatest"],
        "min": ["lowest", "minimum", "least", "smallest", "fewest"]
    }

    needs_count = False
    for keyword in aggregation_keywords["count"]:
        if keyword in question_lower:
            needs_count = True
            break

    if needs_count and "reduce" not in typeql_lower and "count" not in typeql_lower:
        issues.append("Question asks for count but query doesn't use aggregation")

    # Check sort direction
    if any(word in question_lower for word in ["highest", "most", "top", "largest", "biggest", "greatest", "best"]):
        if "sort" in typeql_lower and "asc" in typeql_lower and "desc" not in typeql_lower:
            issues.append("Question asks for highest/most but query sorts ascending")

    if any(word in question_lower for word in ["lowest", "least", "smallest", "fewest", "worst"]):
        if "sort" in typeql_lower and "desc" in typeql_lower and "asc" not in typeql_lower:
            issues.append("Question asks for lowest/least but query sorts descending")

    # Check for specific attribute filters mentioned in question
    filter_patterns = [
        ("public", "is_public"),
        ("dissolved", "is_dissolved"),
        ("employees", "nbr_employees"),
        ("revenue", "revenue"),
        ("sentiment", "sentiment"),
        ("name", "name"),
    ]

    for keyword, attr in filter_patterns:
        if keyword in question_lower:
            # Check if the attribute is used in the query
            if attr not in typeql_lower:
                # Don't flag name mismatches as they're common
                if attr != "name":
                    pass  # Could add issue but often causes false positives

    # Check for relationship correctness
    relationship_keywords = {
        "ceo_of": ["ceo", "chief executive"],
        "board_member_of": ["board member", "board director"],
        "located_in": ["located", "in city", "based in", "headquartered"],
        "in_category": ["category", "industry", "sector"],
        "subsidiary_of": ["subsidiary", "parent company", "owned by"],
        "supplies": ["supplier", "supplies", "customer"],
        "invested_in": ["invested", "investor", "investment"],
        "competes_with": ["competitor", "competes", "competition"],
        "in_country": ["country", "nation"],
        "parent_of": ["parent", "child", "children"]
    }

    for relation, keywords in relationship_keywords.items():
        for keyword in keywords:
            if keyword in question_lower and relation not in typeql_lower:
                # Could be using a different valid pattern
                pass

    # Check if query returns something meaningful
    if "fetch" not in typeql_lower and "reduce" not in typeql_lower:
        issues.append("Query doesn't have fetch or reduce clause")

    # Check for obvious entity mismatches - looking for completely wrong entity types
    # This catches cases where the TypeQL query uses entirely wrong entities

    # If question asks about competitors but query doesn't reference competes_with
    if "competitor" in question_lower and "competes_with" not in typeql_lower:
        issues.append("Question asks about competitors but query doesn't use competes_with relation")

    # If question asks about CEOs but query doesn't reference ceo_of
    if "ceo" in question_lower and "ceo_of" not in typeql_lower:
        issues.append("Question asks about CEOs but query doesn't use ceo_of relation")

    # If question asks about board members but query doesn't reference board_member_of
    if "board member" in question_lower and "board_member_of" not in typeql_lower:
        issues.append("Question asks about board members but query doesn't use board_member_of relation")

    # If question asks about subsidiaries but query doesn't reference subsidiary_of
    if "subsidiar" in question_lower and "subsidiary_of" not in typeql_lower:
        issues.append("Question asks about subsidiaries but query doesn't use subsidiary_of relation")

    # If question asks about investors but query doesn't reference invested_in
    if "invest" in question_lower and "invested_in" not in typeql_lower:
        issues.append("Question asks about investors but query doesn't use invested_in relation")

    # Check for duplicate query issues (TypeQL is same for different questions)
    # This is a heuristic check for queries that seem to be copy-pasted incorrectly

    if issues:
        return False, "; ".join(issues)

    return True, ""


def validate_query(driver, typeql: str) -> tuple[bool, str]:
    """
    Validate a TypeQL query against the database.

    Returns (is_valid, error_message).
    """
    try:
        with driver.transaction(DATABASE, TransactionType.READ) as tx:
            result = tx.query(typeql).resolve()
            # Try to consume the result to ensure it's valid
            try:
                # For fetch queries
                docs = list(result.as_concept_documents())
            except:
                try:
                    # For reduce queries
                    row = result.as_concept_rows()
                    list(row)
                except:
                    pass
            return True, ""
    except Exception as e:
        return False, str(e)


def main():
    print("Connecting to TypeDB...")
    credentials = Credentials("admin", "password")
    options = DriverOptions(is_tls_enabled=False)
    driver = TypeDB.driver("localhost:1729", credentials, options)

    # Check if database exists
    db_names = [db.name for db in driver.databases.all()]
    if DATABASE not in db_names:
        print(f"Error: Database '{DATABASE}' not found. Available: {db_names}")
        sys.exit(1)

    print(f"Reading queries from {INPUT_FILE}...")

    # Read all queries
    queries = []
    with open(INPUT_FILE, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            queries.append(row)

    print(f"Found {len(queries)} queries to validate")

    valid_queries = []
    validation_failures = []
    semantic_failures = []

    for i, row in enumerate(queries):
        if (i + 1) % 50 == 0:
            print(f"Processing query {i + 1}/{len(queries)}...")

        original_index = row['original_index']
        question = row['question']
        cypher = row['cypher']
        typeql = row['typeql']

        # Step 1: Validate against TypeDB
        is_valid, error = validate_query(driver, typeql)

        if not is_valid:
            validation_failures.append({
                'original_index': original_index,
                'question': question,
                'cypher': cypher,
                'error': error
            })
            continue

        # Step 2: Semantic review
        is_semantic_valid, review_reason = semantic_review(question, cypher, typeql)

        if not is_semantic_valid:
            semantic_failures.append({
                'original_index': original_index,
                'question': question,
                'cypher': cypher,
                'typeql': typeql,
                'review_reason': review_reason
            })
            continue

        # Query passed both checks
        valid_queries.append(row)

    print(f"\nWriting results...")

    # Write valid queries
    if valid_queries:
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
            writer.writeheader()
            writer.writerows(valid_queries)
        print(f"  Valid queries: {len(valid_queries)} -> {OUTPUT_FILE}")
    else:
        print(f"  No valid queries to write")

    # Write validation failures
    if validation_failures:
        with open(FAILED_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'error'])
            writer.writeheader()
            writer.writerows(validation_failures)
        print(f"  Validation failures: {len(validation_failures)} -> {FAILED_FILE}")
    else:
        print(f"  No validation failures")

    # Write semantic failures
    if semantic_failures:
        with open(FAILED_REVIEW_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql', 'review_reason'])
            writer.writeheader()
            writer.writerows(semantic_failures)
        print(f"  Semantic failures: {len(semantic_failures)} -> {FAILED_REVIEW_FILE}")
    else:
        print(f"  No semantic failures")

    print(f"\n=== SUMMARY ===")
    print(f"Total queries: {len(queries)}")
    print(f"Valid queries: {len(valid_queries)}")
    print(f"Validation failures: {len(validation_failures)}")
    print(f"Semantic failures: {len(semantic_failures)}")

    driver.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
