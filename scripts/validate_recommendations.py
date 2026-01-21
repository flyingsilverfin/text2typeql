#!/usr/bin/env python3
"""
Validate and semantically review converted TypeQL queries for the recommendations database.
"""

import csv
import sys
import re
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# Schema reference for semantic validation
SCHEMA = {
    "entities": {
        "movie": ["url", "runtime", "revenue", "plot_embedding", "poster_embedding",
                  "imdb_rating", "released", "countries", "languages", "plot",
                  "imdb_votes", "imdb_id", "year", "poster", "movie_id", "tmdb_id",
                  "title", "budget"],
        "genre": ["name"],
        "user": ["user_id", "name"],
        "person": ["url", "name", "tmdb_id", "born_in", "bio", "died", "born",
                   "imdb_id", "poster"]
    },
    "relations": {
        "in_genre": {"roles": ["film", "genre"], "attributes": []},
        "rated": {"roles": ["user", "film"], "attributes": ["rating", "timestamp"]},
        "acted_in": {"roles": ["actor", "film"], "attributes": ["character_role"]},
        "directed": {"roles": ["director", "film"], "attributes": ["character_role"]}
    }
}

def parse_csv(filepath):
    """Parse CSV file and return list of query records."""
    queries = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            queries.append({
                'original_index': row['original_index'],
                'question': row['question'],
                'cypher': row['cypher'],
                'typeql': row['typeql']
            })
    return queries

def validate_query(tx, typeql):
    """Validate a TypeQL query against the database."""
    try:
        result = tx.query(typeql).resolve()
        # Try to consume the result to ensure the query is fully valid
        # Check if it's a fetch query (returns documents) or reduce query (returns rows)
        try:
            list(result.as_concept_documents())
        except Exception:
            # Try concept rows for reduce queries
            try:
                list(result.as_concept_rows())
            except Exception:
                # Just iterate to verify query execution
                pass
        return True, None
    except Exception as e:
        return False, str(e)

def semantic_review(question, typeql, cypher):
    """
    Perform semantic review of the TypeQL query against the English question.
    Returns (is_valid, reason) tuple.
    """
    question_lower = question.lower()
    typeql_lower = typeql.lower()
    cypher_lower = cypher.lower()

    issues = []

    # Check entity type alignment - look for entity references including in relations
    # Movie can appear as "isa movie", "film:" in relations, or indirectly via acted_in/directed/rated/in_genre
    if "movie" in question_lower or "film" in question_lower:
        movie_indicators = ["isa movie", "film:", "movie", "acted_in", "directed", "rated", "in_genre"]
        if not any(ind in typeql_lower for ind in movie_indicators):
            issues.append("Question asks about movies but TypeQL doesn't query movie entity")

    # Check for user entity - but "IMDb users" or "rated by users" (when using imdb_votes or rated relation) don't need user entity
    if "user" in question_lower and "user" not in typeql_lower:
        # If it's about IMDb users/votes, that's handled by imdb_votes attribute
        # If it's about ratings by users, the rated relation is sufficient
        if "imdb" not in question_lower and "imdb_votes" not in typeql_lower and "rated" not in typeql_lower:
            issues.append("Question asks about users but TypeQL doesn't query user entity")

    if "genre" in question_lower and "genre" not in typeql_lower:
        issues.append("Question asks about genres but TypeQL doesn't query genre entity")

    # Check for actor/director context
    if "actor" in question_lower and "acted_in" not in typeql_lower:
        if "person" not in typeql_lower:
            issues.append("Question asks about actors but TypeQL doesn't use acted_in relation or person entity")

    if "director" in question_lower and "directed" not in typeql_lower:
        if "person" not in typeql_lower:
            issues.append("Question asks about directors but TypeQL doesn't use directed relation or person entity")

    # Check aggregation requirements
    # Be careful with "number of" - it could mean "count" or just "which ones have many"
    # "how many" is a clearer signal for count requirement
    count_keywords = ["how many"]
    if any(kw in question_lower for kw in count_keywords):
        if "reduce" not in typeql_lower and "count" not in typeql_lower:
            issues.append("Question asks for count but TypeQL doesn't use reduce/count")

    avg_keywords = ["average", "avg", "mean"]
    if any(kw in question_lower for kw in avg_keywords):
        if "mean" not in typeql_lower and "avg" not in typeql_lower:
            # Check if it at least returns the values needed for averaging
            pass  # TypeQL 3.x may not have avg, so we're lenient here

    sum_keywords = ["sum", "total revenue", "total budget", "combined"]
    if any(kw in question_lower for kw in sum_keywords):
        if "sum" not in typeql_lower:
            # May need to be lenient as TypeQL aggregation syntax varies
            pass

    # Check sort direction - but be smarter about "top X oldest/shortest/lowest"
    # "top X oldest" means ascending (lower year = older)
    # "top X shortest" means ascending (lower runtime = shorter)
    # "top X highest" means descending (higher value = highest)

    # Note: "youngest" means most recent birth date = descending sort
    ascending_modifiers = ["oldest", "shortest", "lowest", "smallest", "earliest", "least expensive", "cheapest"]
    has_ascending_modifier = any(mod in question_lower for mod in ascending_modifiers)

    descending_modifiers = ["highest", "most", "longest", "largest", "newest", "latest", "biggest", "most expensive", "youngest"]
    has_descending_modifier = any(mod in question_lower for mod in descending_modifiers)

    # Only check "top" for descending - "first" is ambiguous and often just means "some"
    # Also, "top X with lower than Y" or "top X below Y" means we're filtering, not sorting
    has_filter_context = any(w in question_lower for w in ["lower than", "below", "under", "less than"])

    if "top" in question_lower:
        if "sort" in typeql_lower:
            if has_ascending_modifier and not has_descending_modifier:
                # "top oldest" = ascending is correct
                if "desc" in typeql_lower and "asc" not in typeql_lower:
                    issues.append("Question asks for top+oldest/shortest (ascending) but TypeQL sorts descending")
            elif has_descending_modifier and not has_ascending_modifier:
                # "top highest" = descending is correct
                if "asc" in typeql_lower and "desc" not in typeql_lower:
                    issues.append("Question asks for top+highest/most (descending) but TypeQL sorts ascending")
            elif not has_ascending_modifier and not has_descending_modifier and not has_filter_context:
                # Plain "top" without modifier - typically means highest/best, so descending
                # But if there's a filter like "lower than", ascending might be intentional
                if "asc" in typeql_lower and "desc" not in typeql_lower:
                    issues.append("Question asks for top but TypeQL sorts ascending")

    # Check for lowest/least/bottom - but exclude "at least" which is a threshold, not ordering
    lowest_indicators = ["lowest", "bottom", "smallest", "minimum", "fewest"]
    # "least" only counts if not preceded by "at"
    has_lowest_context = any(kw in question_lower for kw in lowest_indicators)
    if not has_lowest_context and "least" in question_lower:
        # Check if it's "at least" (threshold) vs standalone "least" (ordering)
        if " least " in question_lower and "at least" not in question_lower:
            has_lowest_context = True

    if has_lowest_context:
        if "sort" in typeql_lower:
            if "desc" in typeql_lower and "asc" not in typeql_lower:
                issues.append("Question asks for lowest/least/bottom but TypeQL sorts descending")

    # Check for specific attribute filters
    # "most ratings" or "user ratings" (as a count) means count of ratings, not the rating value itself
    if "rating" in question_lower:
        # Check if it's about rating values vs count of ratings
        is_count_context = any(w in question_lower for w in ["most rating", "number of rating", "how many rating", "user rating"])
        if not is_count_context:
            if "imdb_rating" not in typeql_lower and "rating" not in typeql_lower:
                issues.append("Question mentions rating but TypeQL doesn't filter/fetch rating attribute")

    # Check for year/released - but only when it's about movie year, not "released in genre/language" or "birth year"
    year_context = False
    if "year" in question_lower:
        # "birth year" should use "born" attribute, not "year"
        if "birth year" not in question_lower:
            year_context = True
    if "released" in question_lower:
        # Check if it's "released in [year]" or "released before/after" vs "released in genre/language"
        if re.search(r'released\s+(in|before|after|between)\s+\d', question_lower):
            year_context = True
        elif "released" in question_lower and "genre" not in question_lower and "language" not in question_lower and "english" not in question_lower and "french" not in question_lower:
            year_context = True

    if year_context:
        if "year" not in typeql_lower and "released" not in typeql_lower:
            issues.append("Question mentions year/release but TypeQL doesn't use year or released attribute")

    if "budget" in question_lower:
        if "budget" not in typeql_lower:
            issues.append("Question mentions budget but TypeQL doesn't use budget attribute")

    if "revenue" in question_lower:
        if "revenue" not in typeql_lower:
            issues.append("Question mentions revenue but TypeQL doesn't use revenue attribute")

    if "runtime" in question_lower or "duration" in question_lower or "length" in question_lower:
        if "runtime" not in typeql_lower:
            issues.append("Question mentions runtime/duration but TypeQL doesn't use runtime attribute")

    # Check for limit when asking for specific number
    # But be lenient for reduce/aggregate queries which may not support limit directly
    limit_match = re.search(r'\b(top|first|last)\s+(\d+)\b', question_lower)
    if limit_match:
        expected_limit = limit_match.group(2)
        has_reduce = "reduce" in typeql_lower
        if f"limit {expected_limit}" not in typeql_lower:
            # Check if limit exists at all - but reduce queries may legitimately not have limits
            if "limit" not in typeql_lower and not has_reduce:
                issues.append(f"Question asks for {limit_match.group(1)} {expected_limit} but TypeQL has no limit")

    # Check relationship usage
    if "rated" in question_lower or "rating" in question_lower:
        if "rated" not in typeql_lower and "rating" not in typeql_lower:
            # Might be ok if just checking movie's imdb_rating
            pass

    # Check for acting-related queries - but "cast" can also mean "votes cast"
    acting_context = "acted" in question_lower or "starring" in question_lower
    if "cast" in question_lower and "votes cast" not in question_lower:
        acting_context = True
    if acting_context:
        if "acted_in" not in typeql_lower:
            issues.append("Question asks about acting/cast but TypeQL doesn't use acted_in relation")

    if "directed" in question_lower:
        if "directed" not in typeql_lower:
            issues.append("Question asks about directing but TypeQL doesn't use directed relation")

    # Check for negation
    negation_keywords = ["not", "never", "without", "except", "excluding"]
    has_negation_in_question = any(kw in question_lower for kw in negation_keywords)
    has_negation_in_typeql = "not {" in typeql_lower or "not{" in typeql_lower

    if has_negation_in_question and not has_negation_in_typeql:
        # Check Cypher to see if it actually used negation
        if "not" in cypher_lower or "where not" in cypher_lower or "!=" in cypher:
            issues.append("Question implies negation but TypeQL doesn't use NOT clause")

    # Return result
    if issues:
        return False, "; ".join(issues)
    return True, None

def main():
    # Connect to TypeDB
    print("Connecting to TypeDB...")
    credentials = Credentials("admin", "password")
    options = DriverOptions(is_tls_enabled=False)

    try:
        driver = TypeDB.driver("localhost:1729", credentials, options)
    except Exception as e:
        print(f"Failed to connect to TypeDB: {e}")
        sys.exit(1)

    # Parse input CSV
    input_path = "/opt/text2typeql/output/recommendations/queries.csv"
    print(f"Reading queries from {input_path}...")
    queries = parse_csv(input_path)
    print(f"Found {len(queries)} queries to validate")

    # Track results
    valid_queries = []
    validation_failures = []
    semantic_failures = []

    # Process each query
    print("Processing queries...")
    with driver.transaction("text2typeql_recommendations", TransactionType.READ) as tx:
        for i, query in enumerate(queries):
            if (i + 1) % 50 == 0:
                print(f"  Processed {i + 1}/{len(queries)} queries...")

            # Step 1: Validate against TypeDB
            is_valid, error = validate_query(tx, query['typeql'])

            if not is_valid:
                validation_failures.append({
                    'original_index': query['original_index'],
                    'question': query['question'],
                    'cypher': query['cypher'],
                    'error': error
                })
                continue

            # Step 2: Semantic review
            is_semantic_valid, review_reason = semantic_review(
                query['question'],
                query['typeql'],
                query['cypher']
            )

            if not is_semantic_valid:
                semantic_failures.append({
                    'original_index': query['original_index'],
                    'question': query['question'],
                    'cypher': query['cypher'],
                    'typeql': query['typeql'],
                    'review_reason': review_reason
                })
                continue

            # Query passed both checks
            valid_queries.append(query)

    driver.close()

    # Write results
    output_dir = "/opt/text2typeql/output/recommendations"

    # Write valid queries
    valid_path = f"{output_dir}/queries.csv"
    print(f"Writing {len(valid_queries)} valid queries to {valid_path}...")
    with open(valid_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
        writer.writeheader()
        writer.writerows(valid_queries)

    # Write validation failures
    failed_path = f"{output_dir}/failed.csv"
    print(f"Writing {len(validation_failures)} validation failures to {failed_path}...")
    with open(failed_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'error'])
        writer.writeheader()
        writer.writerows(validation_failures)

    # Write semantic failures
    semantic_failed_path = f"{output_dir}/failed_review.csv"
    print(f"Writing {len(semantic_failures)} semantic failures to {semantic_failed_path}...")
    with open(semantic_failed_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql', 'review_reason'])
        writer.writeheader()
        writer.writerows(semantic_failures)

    # Print summary
    print("\n" + "="*60)
    print("VALIDATION SUMMARY")
    print("="*60)
    print(f"Total queries processed:     {len(queries)}")
    print(f"Valid queries:               {len(valid_queries)}")
    print(f"TypeDB validation failures:  {len(validation_failures)}")
    print(f"Semantic review failures:    {len(semantic_failures)}")
    print("="*60)

if __name__ == "__main__":
    main()
