#!/usr/bin/env python3
"""
Validate and semantically review all converted queries in the gameofthrones database.
"""

import csv
import re
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# TypeDB connection settings
DB_NAME = "text2typeql_gameofthrones"
CREDENTIALS = Credentials("admin", "password")
OPTIONS = DriverOptions(is_tls_enabled=False)

# File paths
INPUT_FILE = "/opt/text2typeql/output/gameofthrones/queries.csv"
VALID_OUTPUT = "/opt/text2typeql/output/gameofthrones/queries.csv"
FAILED_VALIDATION = "/opt/text2typeql/output/gameofthrones/failed.csv"
FAILED_REVIEW = "/opt/text2typeql/output/gameofthrones/failed_review.csv"

# Schema info for semantic review
SCHEMA_ENTITIES = ["character"]
SCHEMA_ATTRIBUTES = [
    "centrality", "book45_page_rank", "fastrf_embedding",
    "book1_betweenness_centrality", "book1_page_rank", "louvain",
    "community", "degree", "name", "pagerank", "weight", "book"
]
SCHEMA_RELATIONS = ["interacts45", "interacts", "interacts1", "interacts2", "interacts3"]


def validate_query(driver, typeql_query):
    """Validate a TypeQL query against the database."""
    try:
        with driver.transaction(DB_NAME, TransactionType.READ) as tx:
            result = tx.query(typeql_query).resolve()
            # Try to consume the result to ensure query is valid
            if hasattr(result, 'as_concept_documents'):
                list(result.as_concept_documents())
            return True, None
    except Exception as e:
        return False, str(e)


def semantic_review(question, cypher, typeql):
    """
    Perform semantic review of the TypeQL query against the question.
    Returns (is_valid, reason) tuple.
    """
    question_lower = question.lower()
    typeql_lower = typeql.lower()
    cypher_lower = cypher.lower()

    issues = []

    # Check for aggregation keywords in question
    # Be more precise - "number of" could mean "community number" not count
    count_keywords = ["how many", "count(", "total count"]
    has_count_question = any(kw in question_lower for kw in count_keywords)
    # Also check cypher for COUNT to confirm
    has_count_cypher = "count(" in cypher_lower
    has_count_typeql = "reduce" in typeql_lower and "count" in typeql_lower

    if has_count_question and has_count_cypher and not has_count_typeql:
        issues.append("Question asks for count but query does not use reduce count")

    # Check for sorting/ordering
    highest_keywords = ["highest", "most", "maximum", "max", "greatest"]
    lowest_keywords = ["lowest", "least", "minimum", "min", "smallest", "fewest"]

    # "top N" usually implies sorting descending
    has_top_n = re.search(r"top\s+\d+", question_lower) is not None
    has_highest = any(kw in question_lower for kw in highest_keywords) or has_top_n
    has_lowest = any(kw in question_lower for kw in lowest_keywords)
    has_sort_desc = "sort" in typeql_lower and "desc" in typeql_lower
    has_sort_asc = "sort" in typeql_lower and "asc" in typeql_lower and "desc" not in typeql_lower

    # Check cypher to see if it has ORDER BY DESC
    cypher_has_desc = "desc" in cypher_lower
    cypher_has_asc = "asc" in cypher_lower and "desc" not in cypher_lower

    if has_highest and cypher_has_desc and not has_sort_desc:
        issues.append("Question asks for highest/top but query doesn't sort descending")

    if has_lowest and cypher_has_asc and has_sort_desc:
        issues.append("Question asks for lowest/least but query sorts descending instead of ascending")

    # Check for limit when question asks for specific count
    limit_patterns = [
        r"top (\d+)", r"first (\d+)", r"limit.*?(\d+)"
    ]
    question_limit = None
    for pattern in limit_patterns:
        match = re.search(pattern, question_lower)
        if match:
            question_limit = int(match.group(1))
            break

    # Also check cypher for LIMIT
    cypher_limit_match = re.search(r"limit\s+(\d+)", cypher_lower)
    cypher_limit = int(cypher_limit_match.group(1)) if cypher_limit_match else None

    if question_limit and cypher_limit:
        typeql_limit_match = re.search(r"limit\s+(\d+)", typeql_lower)
        if typeql_limit_match:
            typeql_limit = int(typeql_limit_match.group(1))
            if typeql_limit != cypher_limit:
                issues.append(f"Cypher has LIMIT {cypher_limit} but TypeQL limits to {typeql_limit}")
        else:
            issues.append(f"Cypher has LIMIT {cypher_limit} but TypeQL has no limit")

    # Check attribute usage - be smarter about partial matches
    # The schema has: book45_page_rank, book1_page_rank, pagerank (separate attribute)
    # If question mentions "book45PageRank", book45_page_rank is correct
    # If question mentions just "pagerank" (standalone), pagerank is correct

    # Check if question refers to specific book pagerank attributes
    question_normalized = question_lower.replace(" ", "").replace("-", "").replace("'", "")

    # Check for book-specific pagerank attributes
    if "book45pagerank" in question_normalized:
        if "book45_page_rank" not in typeql_lower:
            issues.append("Question mentions 'book45PageRank' but query doesn't use 'book45_page_rank'")
    elif "book1pagerank" in question_normalized:
        if "book1_page_rank" not in typeql_lower:
            issues.append("Question mentions 'book1PageRank' but query doesn't use 'book1_page_rank'")
    elif "pagerank" in question_normalized:
        # Standalone pagerank - either pagerank or any *_page_rank could be valid depending on context
        if "pagerank" not in typeql_lower and "page_rank" not in typeql_lower:
            issues.append("Question mentions 'pagerank' but query doesn't use any pagerank attribute")

    # Check for book1betweennesscentrality
    if "book1betweennesscentrality" in question_normalized:
        if "book1_betweenness_centrality" not in typeql_lower:
            issues.append("Question mentions 'book1BetweennessCentrality' but query doesn't use 'book1_betweenness_centrality'")

    # Check for louvain (louvain community refers to louvain attribute, not community attribute)
    if "louvain" in question_lower:
        if "louvain" not in typeql_lower:
            issues.append("Question mentions 'louvain' but query doesn't use 'louvain' attribute")

    # Check for standalone community (only if not preceded by "louvain")
    if re.search(r"(?<!louvain\s)community(?!\s*number)", question_lower):
        if "community" not in typeql_lower:
            # This might be referring to community attribute
            pass  # Don't flag, could be contextual

    # Check for centrality
    if "centrality" in question_lower and "betweenness" not in question_lower:
        if "centrality" not in typeql_lower:
            issues.append("Question mentions 'centrality' but query doesn't use any centrality attribute")

    # Check for degree - but not "degree of centrality" which means centrality level
    if "degree" in question_lower and "degree of" not in question_lower:
        if "degree" not in typeql_lower:
            issues.append("Question mentions 'degree' but query doesn't use 'degree' attribute")

    # Check for interaction/relationship queries
    interaction_keywords = ["interact", "connection", "relationship", "linked", "connected"]
    has_interaction_question = any(kw in question_lower for kw in interaction_keywords)
    has_relation_typeql = any(rel in typeql_lower for rel in ["interacts45", "interacts1", "interacts2", "interacts3", "interacts"])

    if has_interaction_question and not has_relation_typeql:
        # Check if cypher also uses a relationship
        if "]-[" in cypher_lower or "]->" in cypher_lower or "<-[" in cypher_lower:
            issues.append("Question asks about interactions but query doesn't use relationship")

    # Check for missing fetch in non-aggregation queries
    if "reduce" not in typeql_lower and "fetch" not in typeql_lower:
        issues.append("Query missing fetch clause for non-aggregation query")

    # Check entity type usage
    if "character" in question_lower and "character" not in typeql_lower:
        issues.append("Question mentions character but query doesn't use character entity")

    # Check for comparison operators matching question intent
    comparison_words = {
        "greater than": ">",
        "more than": ">",
        "above": ">",
        "less than": "<",
        "fewer than": "<",
        "below": "<",
        "equal to": "==",
        "equals": "==",
        "exactly": "=="
    }

    for word, op in comparison_words.items():
        if word in question_lower:
            # Check if the correct operator is used in typeql
            if op == ">" and ">" not in typeql and ">=" not in typeql:
                if "<" in typeql:
                    issues.append(f"Question says '{word}' but query uses wrong comparison operator")
            elif op == "<" and "<" not in typeql and "<=" not in typeql:
                if ">" in typeql:
                    issues.append(f"Question says '{word}' but query uses wrong comparison operator")

    if issues:
        return False, "; ".join(issues)
    return True, None


def main():
    print("Connecting to TypeDB...")
    driver = TypeDB.driver("localhost:1729", CREDENTIALS, OPTIONS)

    # Check database exists
    try:
        db = driver.databases.get(DB_NAME)
        print(f"Database '{DB_NAME}' found.")
    except Exception as e:
        print(f"Error: Database '{DB_NAME}' not found. Please ensure it exists.")
        print(f"Error details: {e}")
        return

    # Read all queries
    print(f"Reading queries from {INPUT_FILE}...")
    queries = []
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            queries.append(row)

    print(f"Found {len(queries)} queries to validate.")

    valid_queries = []
    validation_failures = []
    semantic_failures = []

    for i, query in enumerate(queries):
        original_index = query['original_index']
        question = query['question']
        cypher = query['cypher']
        typeql = query['typeql']

        if (i + 1) % 100 == 0:
            print(f"Processing query {i + 1}/{len(queries)}...")

        # Step 1: TypeDB Validation
        is_valid, error = validate_query(driver, typeql)

        if not is_valid:
            validation_failures.append({
                'original_index': original_index,
                'question': question,
                'cypher': cypher,
                'error': error
            })
            continue

        # Step 2: Semantic Review
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
        valid_queries.append(query)

    # Write results
    print("\nWriting results...")

    # Write valid queries
    with open(VALID_OUTPUT, 'w', newline='', encoding='utf-8') as f:
        if valid_queries:
            writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
            writer.writeheader()
            writer.writerows(valid_queries)
    print(f"Valid queries written to: {VALID_OUTPUT}")

    # Write validation failures
    with open(FAILED_VALIDATION, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'error'])
        writer.writeheader()
        if validation_failures:
            writer.writerows(validation_failures)
    print(f"Validation failures written to: {FAILED_VALIDATION}")

    # Write semantic failures
    with open(FAILED_REVIEW, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql', 'review_reason'])
        writer.writeheader()
        if semantic_failures:
            writer.writerows(semantic_failures)
    print(f"Semantic failures written to: {FAILED_REVIEW}")

    # Print summary
    print("\n" + "="*60)
    print("VALIDATION SUMMARY")
    print("="*60)
    print(f"Total queries processed: {len(queries)}")
    print(f"Valid queries: {len(valid_queries)}")
    print(f"TypeDB validation failures: {len(validation_failures)}")
    print(f"Semantic review failures: {len(semantic_failures)}")
    print("="*60)

    if validation_failures:
        print("\nSample validation errors:")
        for failure in validation_failures[:5]:
            print(f"  Index {failure['original_index']}: {failure['error'][:100]}...")

    if semantic_failures:
        print("\nSample semantic issues:")
        for failure in semantic_failures[:5]:
            print(f"  Index {failure['original_index']}: {failure['review_reason']}")

    driver.close()


if __name__ == "__main__":
    main()
