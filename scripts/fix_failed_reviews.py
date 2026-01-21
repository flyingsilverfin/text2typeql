#!/usr/bin/env python3
"""
Fix the 33 failed semantic review queries in the recommendations database.

This script reads the failed_review.csv, fixes each query based on the review_reason,
validates the fixed queries against TypeDB, and moves them to the appropriate file.
"""

import csv
import re
from pathlib import Path
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType


# Configuration
DATABASE = "text2typeql_recommendations"
OUTPUT_DIR = Path("/opt/text2typeql/output/recommendations")
FAILED_REVIEW_FILE = OUTPUT_DIR / "failed_review.csv"
QUERIES_FILE = OUTPUT_DIR / "queries.csv"
FAILED_FILE = OUTPUT_DIR / "failed.csv"


def connect_typedb():
    """Connect to TypeDB server."""
    credentials = Credentials("admin", "password")
    options = DriverOptions(is_tls_enabled=False)
    return TypeDB.driver("localhost:1729", credentials, options)


def validate_query(driver, typeql: str) -> tuple[bool, str]:
    """Validate a TypeQL query against the database."""
    try:
        with driver.transaction(DATABASE, TransactionType.READ) as tx:
            # Try to execute the query
            result = tx.query(typeql).resolve()
            # Consume the result to check for errors
            if hasattr(result, 'as_concept_documents'):
                list(result.as_concept_documents())
            elif hasattr(result, 'as_concept_rows'):
                list(result.as_concept_rows())
            return True, ""
    except Exception as e:
        return False, str(e)


def fix_missing_sort_after_reduce(typeql: str) -> str:
    """
    Fix queries that are missing sort after reduce.

    Pattern: Add 'sort $count desc;' after reduce and before limit/fetch
    """
    # Check if we have a reduce but no sort
    if 'reduce' not in typeql:
        return typeql

    if 'sort' in typeql:
        return typeql  # Already has sort

    # Find the groupby variable to use in the second match
    groupby_match = re.search(r'groupby\s+(\$\w+)', typeql)
    if not groupby_match:
        return typeql

    groupby_var = groupby_match.group(1)

    # Find what we're reducing into (e.g., $count, $avg)
    reduce_match = re.search(r'reduce\s+(\$\w+)\s*=\s*(\w+)\(', typeql)
    if not reduce_match:
        return typeql

    result_var = reduce_match.group(1)
    agg_func = reduce_match.group(2)

    # Parse the query to find the entity variable and its attributes
    # We need to restructure the query to have:
    # 1. First match clause (pattern matching)
    # 2. Reduce clause
    # 3. Second match clause (to fetch grouped variable's attributes)
    # 4. Sort clause
    # 5. Limit clause (if any)
    # 6. Fetch clause

    lines = typeql.strip().split('\n')

    # Find components
    match_lines = []
    reduce_line = None
    fetch_line = None
    limit_line = None

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith('reduce'):
            reduce_line = line
        elif line.startswith('limit'):
            limit_line = line
        elif line.startswith('fetch'):
            fetch_line = line
        else:
            match_lines.append(line)
        i += 1

    if not reduce_line:
        return typeql

    # Find the entity that owns the groupby variable
    # Look for patterns like: $u isa user, has name $n;
    # where $n is the groupby variable
    entity_var = None
    entity_type = None

    for line in match_lines:
        # Pattern: $x isa type, has attr $groupby_var
        pattern = rf'(\$\w+)\s+isa\s+(\w+).*has\s+\w+\s+{re.escape(groupby_var)}'
        m = re.search(pattern, line)
        if m:
            entity_var = m.group(1)
            entity_type = m.group(2)
            break

    if not entity_var:
        # Try another pattern - the groupby var might be an attribute variable directly
        # In this case, we just need to add the sort after reduce
        pass

    # Construct the fixed query
    match_block = '\n'.join(match_lines)

    # Check if this is a single-line match or multi-line
    if match_block.startswith('match'):
        # Multi-line format
        fixed_parts = [match_block.rstrip(';') + ';' if not match_block.rstrip().endswith(';') else match_block]
    else:
        fixed_parts = ['match\n' + match_block]

    fixed_parts.append(reduce_line.rstrip(';') + ';' if not reduce_line.rstrip().endswith(';') else reduce_line)

    # Add second match to retrieve the grouped variable for fetch
    if entity_var:
        fixed_parts.append(f'match {entity_var} has name {groupby_var};')

    fixed_parts.append(f'sort {result_var} desc;')

    if limit_line:
        fixed_parts.append(limit_line.rstrip(';') + ';' if not limit_line.rstrip().endswith(';') else limit_line)

    if fetch_line:
        fixed_parts.append(fetch_line)
    else:
        # Generate a fetch clause
        if agg_func == 'mean':
            fixed_parts.append(f'fetch {{ "name": {groupby_var}, "average": {result_var} }};')
        else:
            fixed_parts.append(f'fetch {{ "name": {groupby_var}, "count": {result_var} }};')

    return '\n'.join(fixed_parts)


def fix_query(original_index: int, question: str, cypher: str, typeql: str, review_reason: str) -> str:
    """
    Fix a TypeQL query based on the review reason.

    Args:
        original_index: Original index from the dataset
        question: Natural language question
        cypher: Original Cypher query
        typeql: Current TypeQL query
        review_reason: Reason the query failed review

    Returns:
        Fixed TypeQL query
    """

    # Normalize the typeql - handle multiline
    typeql = typeql.strip()

    # Issue 1: Missing sort after reduce
    if "sort' is missing after 'reduce'" in review_reason:
        # This is the most common issue
        # We need to add sort and potentially restructure the query
        return fix_aggregation_query(original_index, question, cypher, typeql, review_reason)

    # Issue 2: Count filter missing after reduce
    if "count >=" in review_reason or "count >" in review_reason or "filter may be missing after reduce" in review_reason:
        return fix_aggregation_query(original_index, question, cypher, typeql, review_reason)

    # Issue 3: imdb_votes vs rated users
    if "imdb_votes" in review_reason and "rated" in review_reason:
        return fix_imdb_votes_query(original_index, question, cypher, typeql)

    return typeql


def fix_aggregation_query(original_index: int, question: str, cypher: str, typeql: str, review_reason: str) -> str:
    """
    Fix aggregation queries that need sort and/or count filter after reduce.

    TypeQL pattern for aggregation with sort:
    match $a isa person; (actor: $a, film: $m) isa acted_in;
    reduce $count = count($m) groupby $a;
    match $a has name $n;
    sort $count desc;
    limit 5;
    fetch { "actor": $n, "count": $count };

    TypeQL pattern for aggregation with threshold:
    match $a isa person; (actor: $a, film: $m) isa acted_in;
    reduce $count = count($m) groupby $a;
    match $count > 5; $a has name $n;
    fetch { "actor": $n };
    """

    # Parse the query components
    typeql = typeql.strip()

    # Determine if it's single-line or multi-line
    is_single_line = '\n' not in typeql or typeql.count('\n') <= 1

    # Extract components using regex
    # Find match clause content
    match_content = ""
    reduce_clause = ""
    limit_value = None

    # For single-line queries
    if is_single_line:
        # Pattern: match ...; reduce ...; [sort ...;] [limit ...;] [fetch ...;]
        match_m = re.search(r'match\s+(.+?);\s*reduce', typeql)
        if match_m:
            match_content = match_m.group(1).strip()

        reduce_m = re.search(r'reduce\s+(\$\w+)\s*=\s*(\w+)\(([^)]+)\)\s*groupby\s+(\$\w+)', typeql)
        if reduce_m:
            result_var = reduce_m.group(1)
            agg_func = reduce_m.group(2)
            agg_target = reduce_m.group(3)
            groupby_var = reduce_m.group(4)
        else:
            return typeql  # Can't parse

        limit_m = re.search(r'limit\s+(\d+)', typeql)
        if limit_m:
            limit_value = int(limit_m.group(1))

        sort_m = re.search(r'sort\s+\$\w+', typeql)
        has_sort = sort_m is not None
    else:
        # Multi-line query
        lines = typeql.split('\n')
        match_lines = []

        for line in lines:
            line = line.strip()
            if line.startswith('match'):
                match_lines.append(line[5:].strip())
            elif line.startswith('reduce'):
                reduce_clause = line
            elif line.startswith('limit'):
                limit_m = re.search(r'limit\s+(\d+)', line)
                if limit_m:
                    limit_value = int(limit_m.group(1))
            elif not line.startswith('fetch') and not line.startswith('sort'):
                if line:
                    match_lines.append(line)

        match_content = ' '.join(match_lines).strip().rstrip(';')

        reduce_m = re.search(r'reduce\s+(\$\w+)\s*=\s*(\w+)\(([^)]+)\)\s*groupby\s+(\$\w+)', reduce_clause)
        if reduce_m:
            result_var = reduce_m.group(1)
            agg_func = reduce_m.group(2)
            agg_target = reduce_m.group(3)
            groupby_var = reduce_m.group(4)
        else:
            return typeql

        has_sort = 'sort' in typeql

    # Determine the entity variable that owns the groupby variable
    # Look for pattern: $var isa type, has attr $groupby_var
    entity_var = None
    entity_attr = None

    # Try to find entity owning the groupby variable
    pattern = rf'(\$\w+)\s+isa\s+\w+[^;]*has\s+(\w+)\s+{re.escape(groupby_var)}'
    m = re.search(pattern, match_content)
    if m:
        entity_var = m.group(1)
        entity_attr = m.group(2)

    # Check if we need to add a count filter
    needs_filter = False
    filter_value = None
    filter_op = None

    if "filter may be missing" in review_reason or "count >=" in review_reason or "count >" in review_reason:
        # Extract the filter requirement from the Cypher
        # Look for WHERE numMovies >= 3 or similar
        filter_m = re.search(r'WHERE\s+\w+\s*(>=?|<=?|>|<)\s*(\d+)', cypher)
        if filter_m:
            filter_op = filter_m.group(1)
            filter_value = int(filter_m.group(2))
            needs_filter = True

    # Check if we need sort
    needs_sort = "sort' is missing" in review_reason or ("ORDER BY" in cypher and not has_sort)

    # Extract limit from cypher if not in typeql
    if limit_value is None:
        limit_m = re.search(r'LIMIT\s+(\d+)', cypher)
        if limit_m:
            limit_value = int(limit_m.group(1))

    # Determine sort direction
    sort_dir = "desc"
    if "ASC" in cypher:
        sort_dir = "asc"

    # Build the fixed query
    parts = []

    # Match clause
    parts.append(f"match {match_content};")

    # Reduce clause
    parts.append(f"reduce {result_var} = {agg_func}({agg_target}) groupby {groupby_var};")

    # Second match clause (for filter and/or attribute retrieval)
    second_match_parts = []

    if needs_filter and filter_value is not None:
        second_match_parts.append(f"{result_var} {filter_op} {filter_value}")

    if entity_var and entity_attr:
        second_match_parts.append(f"{entity_var} has {entity_attr} {groupby_var}")

    if second_match_parts:
        parts.append(f"match {'; '.join(second_match_parts)};")

    # Sort clause
    if needs_sort:
        parts.append(f"sort {result_var} {sort_dir};")

    # Limit clause
    if limit_value:
        parts.append(f"limit {limit_value};")

    # Fetch clause
    if agg_func == "mean":
        parts.append(f'fetch {{ "{entity_attr}": {groupby_var}, "average": {result_var} }};')
    else:
        parts.append(f'fetch {{ "{entity_attr}": {groupby_var}, "count": {result_var} }};')

    return '\n'.join(parts)


def fix_imdb_votes_query(original_index: int, question: str, cypher: str, typeql: str) -> str:
    """
    Fix queries that incorrectly use imdb_votes instead of counting rated relations.

    Original (incorrect):
    match $m isa movie, has title $t, has imdb_rating $r, has imdb_votes $v;
    $v > 500;
    sort $r asc;
    limit 3;
    fetch { "title": $t, "imdbRating": $r };

    Fixed (correct):
    match
    $m isa movie, has title $t, has imdb_rating $r;
    (user: $u, film: $m) isa rated;
    reduce $count = count($u) groupby $m;
    match $count > 500; $m has title $t, has imdb_rating $r;
    sort $r asc;
    limit 3;
    fetch { "title": $t, "imdbRating": $r };
    """

    # These queries need to count the rated relations instead of using imdb_votes
    # For query 179: "List the top 3 movies with the lowest imdbRating that have been rated by more than 500 users."
    # For query 513: "List all movies that have a revenue greater than 100 million dollars and have been rated by more than 500 users."

    if original_index == 179:
        return """match
$m isa movie, has title $t, has imdb_rating $r;
(user: $u, film: $m) isa rated;
reduce $count = count($u) groupby $m;
match $count > 500; $m has title $t, has imdb_rating $r;
sort $r asc;
limit 3;
fetch { "title": $t, "imdb_rating": $r };"""

    elif original_index == 513:
        return """match
$m isa movie, has title $t, has revenue $rev;
(user: $u, film: $m) isa rated;
$rev > 100000000;
reduce $count = count($u) groupby $m;
match $count > 500; $m has title $t, has revenue $rev;
fetch { "title": $t, "revenue": $rev };"""

    return typeql


def read_failed_reviews() -> list[dict]:
    """Read the failed review CSV file."""
    rows = []
    with open(FAILED_REVIEW_FILE, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def read_queries_csv() -> list[dict]:
    """Read the existing queries CSV file."""
    rows = []
    with open(QUERIES_FILE, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def read_failed_csv() -> list[dict]:
    """Read the existing failed CSV file."""
    rows = []
    with open(FAILED_FILE, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def write_queries_csv(rows: list[dict]):
    """Write the queries CSV file."""
    if not rows:
        return
    fieldnames = ['original_index', 'question', 'cypher', 'typeql']
    with open(QUERIES_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, '') for k in fieldnames})


def write_failed_csv(rows: list[dict]):
    """Write the failed CSV file."""
    if not rows:
        return
    fieldnames = ['original_index', 'question', 'cypher', 'error']
    with open(FAILED_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, '') for k in fieldnames})


def main():
    """Main function to fix all failed review queries."""
    print("Reading failed reviews...")
    failed_reviews = read_failed_reviews()
    print(f"Found {len(failed_reviews)} failed review queries")

    print("\nConnecting to TypeDB...")
    driver = connect_typedb()

    # Track results
    fixed_queries = []
    still_failed = []

    print("\nProcessing queries...")
    for i, row in enumerate(failed_reviews):
        original_index = int(row['original_index'])
        question = row['question']
        cypher = row['cypher']
        typeql = row['typeql']
        review_reason = row['review_reason']

        print(f"\n[{i+1}/{len(failed_reviews)}] Query {original_index}: {question[:50]}...")
        print(f"  Review reason: {review_reason[:80]}...")

        # Fix the query
        fixed_typeql = fix_query(original_index, question, cypher, typeql, review_reason)

        if fixed_typeql != typeql:
            print(f"  Fixed query applied")

        # Validate the fixed query
        is_valid, error = validate_query(driver, fixed_typeql)

        if is_valid:
            print(f"  VALID - Moving to queries.csv")
            fixed_queries.append({
                'original_index': original_index,
                'question': question,
                'cypher': cypher,
                'typeql': fixed_typeql
            })
        else:
            print(f"  INVALID - {error[:60]}...")
            still_failed.append({
                'original_index': original_index,
                'question': question,
                'cypher': cypher,
                'error': error,
                'typeql': fixed_typeql,
                'review_reason': review_reason
            })

    print(f"\n\nResults:")
    print(f"  Fixed and valid: {len(fixed_queries)}")
    print(f"  Still failing: {len(still_failed)}")

    # Read existing files
    print("\nReading existing queries.csv...")
    existing_queries = read_queries_csv()
    print(f"  Found {len(existing_queries)} existing queries")

    print("Reading existing failed.csv...")
    existing_failed = read_failed_csv()
    print(f"  Found {len(existing_failed)} existing failed queries")

    # Add fixed queries to queries.csv
    if fixed_queries:
        existing_queries.extend(fixed_queries)
        print(f"\nWriting {len(existing_queries)} queries to queries.csv...")
        write_queries_csv(existing_queries)

    # Add still-failed queries to failed.csv
    if still_failed:
        for row in still_failed:
            existing_failed.append({
                'original_index': row['original_index'],
                'question': row['question'],
                'cypher': row['cypher'],
                'error': f"Review: {row['review_reason']} | Validation: {row['error']}"
            })
        print(f"Writing {len(existing_failed)} queries to failed.csv...")
        write_failed_csv(existing_failed)

    # Clear the failed_review.csv since we've processed all entries
    print("\nClearing failed_review.csv...")
    with open(FAILED_REVIEW_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql', 'review_reason'])
        writer.writeheader()

    print("\nDone!")

    # Print detailed info about still-failed queries
    if still_failed:
        print("\n" + "="*80)
        print("QUERIES STILL FAILING:")
        print("="*80)
        for row in still_failed:
            print(f"\nQuery {row['original_index']}: {row['question'][:60]}...")
            print(f"  Review reason: {row['review_reason']}")
            print(f"  Validation error: {row['error'][:100]}...")
            print(f"  TypeQL:\n    {row['typeql'][:200]}...")

    driver.close()


if __name__ == "__main__":
    main()
