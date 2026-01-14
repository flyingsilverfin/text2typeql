#!/usr/bin/env python3
"""
MCP-based batch conversion runner for subagents.

Each subagent converts ONE query, retries up to 3 times, and writes results
to individual files (allowing parallel execution without conflicts).

Usage from Claude Code:
    1. Get queries to convert:
       queries = load_pending_queries("movies", limit=10)

    2. Spawn parallel Task agents with get_conversion_prompt(db, query)

    3. After all done, merge results:
       merge_results("movies")
"""

import json
import csv
from pathlib import Path
from typing import Optional
import pandas as pd

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
OUTPUT_DIR = PROJECT_ROOT / "output"
DATA_DIR = PROJECT_ROOT / "data"


def get_total_queries(database: str) -> int:
    """Get total number of valid queries for a database."""
    csv_path = DATA_DIR / "text2cypher" / "datasets" / "synthetic_opus_demodbs" / "text2cypher_claudeopus.csv"
    if not csv_path.exists():
        return 0

    df = pd.read_csv(csv_path)
    df = df[df['database'] == database]
    df = df[~df['syntax_error']]
    df = df[df['false_schema'].isna()]
    return len(df)


def get_completed_indices(database: str) -> set[int]:
    """Get original indices of completed queries."""
    db_dir = OUTPUT_DIR / database
    completed = set()

    # Check individual result files (most reliable - has original_index)
    for f in db_dir.glob("result_*.json"):
        try:
            data = json.loads(f.read_text())
            if 'original_index' in data:
                completed.add(int(data['original_index']))
        except:
            pass

    # Check individual failed files
    for f in db_dir.glob("failed_*.json"):
        try:
            data = json.loads(f.read_text())
            if 'original_index' in data:
                completed.add(int(data['original_index']))
        except:
            pass

    # Check merged queries.csv
    queries_file = db_dir / "queries.csv"
    if queries_file.exists():
        try:
            qdf = pd.read_csv(queries_file)
            # Check both 'original_index' (new format) and 'index' (old format)
            for col in ['original_index', 'index']:
                if col in qdf.columns:
                    completed.update(qdf[col].dropna().astype(int).tolist())
                    break
        except:
            pass

    # Check failed.csv
    failed_file = db_dir / "failed.csv"
    if failed_file.exists():
        try:
            fdf = pd.read_csv(failed_file)
            for col in ['original_index', 'index']:
                if col in fdf.columns:
                    completed.update(fdf[col].dropna().astype(int).tolist())
                    break
        except:
            pass

    return completed


def get_completed_questions(database: str) -> set[str]:
    """Get questions already completed (for backwards compatibility with old format)."""
    db_dir = OUTPUT_DIR / database
    completed = set()

    # Check merged queries.csv (handles old format without original_index)
    queries_file = db_dir / "queries.csv"
    if queries_file.exists():
        try:
            qdf = pd.read_csv(queries_file)
            if 'question' in qdf.columns:
                completed.update(qdf['question'].dropna().tolist())
        except:
            pass

    # Check failed.csv
    failed_file = db_dir / "failed.csv"
    if failed_file.exists():
        try:
            fdf = pd.read_csv(failed_file)
            if 'question' in fdf.columns:
                completed.update(fdf['question'].dropna().tolist())
        except:
            pass

    return completed


def load_pending_queries(
    database: str,
    limit: int = 10,
    source: str = "neo4j"
) -> list[dict]:
    """
    Load queries that still need conversion.

    Automatically skips already completed queries.

    Args:
        database: Database name (e.g., 'movies')
        limit: Max number of queries to load
        source: 'neo4j' for fresh queries, 'failed' for retrying merged failures

    Returns:
        List of query dicts with: index, original_index, question, cypher
    """
    db_dir = OUTPUT_DIR / database
    db_dir.mkdir(parents=True, exist_ok=True)

    if source == "neo4j":
        csv_path = DATA_DIR / "text2cypher" / "datasets" / "synthetic_opus_demodbs" / "text2cypher_claudeopus.csv"
        if not csv_path.exists():
            return []

        df = pd.read_csv(csv_path)
        df = df[df['database'] == database]
        df = df[~df['syntax_error']]
        df = df[df['false_schema'].isna()]
        df = df.reset_index(drop=True)  # Reset to 0-based index

        # Skip already completed (by index and by question text for backwards compat)
        completed_indices = get_completed_indices(database)
        completed_questions = get_completed_questions(database)

        queries = []
        for idx, row in df.iterrows():
            # Skip if index is completed OR question text matches
            if idx in completed_indices or row['question'] in completed_questions:
                continue
            if len(queries) >= limit:
                break
            queries.append({
                "index": len(queries),  # Local index for this batch
                "original_index": idx,   # Global index for tracking
                "question": row['question'],
                "cypher": row['cypher']
            })
        return queries

    elif source == "failed":
        # Load from merged failed.csv for retry
        failed_file = db_dir / "failed.csv"
        if not failed_file.exists():
            return []

        queries = []
        with open(failed_file, 'r') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if len(queries) >= limit:
                    break
                queries.append({
                    "index": len(queries),
                    "original_index": int(row.get('original_index', i)),
                    "question": row['question'],
                    "cypher": row['cypher'],
                    "previous_typeql": row.get('typeql', ''),
                    "previous_error": row.get('error', '')
                })
        return queries

    return []


def get_conversion_prompt(database: str, query: dict) -> str:
    """
    Generate a subagent prompt for converting a single query.

    The subagent will:
    1. Get schema context via MCP tool
    2. Generate TypeQL
    3. Validate via MCP tool
    4. Retry up to 3 times if validation fails
    5. Write result to individual file
    """
    q = query
    original_idx = q.get('original_index', q['index'])

    prompt = f"""Convert this Cypher query to TypeQL for the '{database}' database.

**Query (index {original_idx})**
- Question: {q['question']}
- Cypher: `{q['cypher']}`
"""

    if q.get('previous_error') and q.get('previous_typeql'):
        prompt += f"""
**Previous attempt failed:**
- TypeQL: `{q['previous_typeql']}`
- Error: {q['previous_error'][:300]}
"""

    prompt += f"""
**Instructions:**
1. Call `mcp__text2typeql__convert_query` with:
   - database="{database}"
   - question="{q['question'][:100]}"
   - cypher (the full cypher query)
   This returns the full prompt with TypeQL schema.

2. Generate a valid TypeQL query based on the schema.

3. Call `mcp__text2typeql__validate_typeql` with database="{database}" and your typeql to validate against the running TypeDB instance.

4. If validation fails, carefully read the error, fix the query, and retry (up to 3 total attempts).

5. Write the result using the Write tool to:
   - SUCCESS: /home/user/text2typeql/output/{database}/result_{original_idx}.json
   - FAILURE (after 3 tries): /home/user/text2typeql/output/{database}/failed_{original_idx}.json

## TypeQL 3.x Quick Reference

**Match patterns:**
```typeql
match $x isa person;                           # Match by type
match $x isa person, has name "John";          # With attribute value
match $x isa person, has name $n;              # Bind attribute to variable
match $x isa person, has name $n, has age $a;  # Multiple attributes
```

**Relations (IMPORTANT - use role: $var syntax):**
```typeql
match (ceo: $p, led_org: $o) isa ceo_leadership;           # Match relation
match $p isa person; (ceo: $p, led_org: $o) isa ceo_leadership;  # With type constraint
```

**Filtering:**
```typeql
match $x isa organization, has revenue $r; $r > 1000000;   # Comparison
match $p isa person, has name $n; $n like "%Smith%";       # String contains
```

**Fetch results:**
```typeql
fetch {{ "name": $n }};                    # Single value
fetch {{ "name": $n, "revenue": $r }};     # Multiple values
fetch {{ "org": $o.name }};                # Attribute from entity
```

**Aggregations:**
```typeql
match $o isa organization; reduce $count = count($o);      # Count
match $o isa organization, has revenue $r; reduce $sum = sum($r);  # Sum
```

**Sorting and limiting (ORDER MATTERS: match -> sort -> limit -> fetch):**
```typeql
match $o isa organization, has revenue $r, has name $n;
sort $r desc;
limit 10;
fetch {{ "name": $n, "revenue": $r }};
```

**Negation:**
```typeql
match $o isa organization; not {{ (subsidiary_org: $o) isa subsidiary; }};
```

## Common Mistakes to Avoid
- Query order MUST be: match -> sort -> limit -> fetch/reduce
- Bind attributes to variables before sorting: `has revenue $r; sort $r desc;`
- Use double quotes for strings, not single quotes
- Relation syntax: `(role1: $var1, role2: $var2) isa relation_type`
- Do NOT use `$var.*` syntax - explicitly list attributes

**File format:**
```json
{{
  "original_index": {original_idx},
  "question": "the question",
  "cypher": "the original cypher",
  "typeql": "your converted query",
  "success": true,
  "error": null
}}
```

**Important:** Write the JSON file as your final action. Return "done" when complete.
"""
    return prompt


def merge_results(database: str) -> dict:
    """
    Merge individual result files into queries.csv and failed.csv.

    Returns stats dict with counts.
    """
    db_dir = OUTPUT_DIR / database

    successful = []
    failed = []

    # Collect individual results
    for f in sorted(db_dir.glob("result_*.json")):
        try:
            data = json.loads(f.read_text())
            successful.append(data)
        except:
            pass

    for f in sorted(db_dir.glob("failed_*.json")):
        try:
            data = json.loads(f.read_text())
            failed.append(data)
        except:
            pass

    # Append to queries.csv
    if successful:
        queries_file = db_dir / "queries.csv"
        write_header = not queries_file.exists()
        with open(queries_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
            if write_header:
                writer.writeheader()
            for r in successful:
                writer.writerow({
                    'original_index': r.get('original_index', ''),
                    'question': r.get('question', ''),
                    'cypher': r.get('cypher', ''),
                    'typeql': r.get('typeql', '')
                })

    # Write/append to failed.csv
    if failed:
        failed_file = db_dir / "failed.csv"
        write_header = not failed_file.exists()
        with open(failed_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql', 'error'])
            if write_header:
                writer.writeheader()
            for r in failed:
                writer.writerow({
                    'original_index': r.get('original_index', ''),
                    'question': r.get('question', ''),
                    'cypher': r.get('cypher', ''),
                    'typeql': r.get('typeql', ''),
                    'error': str(r.get('error', ''))[:500]
                })

    # Clean up individual files
    for f in db_dir.glob("result_*.json"):
        f.unlink()
    for f in db_dir.glob("failed_*.json"):
        f.unlink()

    return {
        "merged_successful": len(successful),
        "merged_failed": len(failed)
    }


def get_status(database: str) -> dict:
    """Get conversion status for a database."""
    db_dir = OUTPUT_DIR / database
    total = get_total_queries(database)

    # Count completed by index (most accurate for new format)
    completed_by_idx = len(get_completed_indices(database))
    # Count completed by question (for old format)
    completed_by_q = len(get_completed_questions(database))
    # Use the larger count as approximation
    completed = max(completed_by_idx, completed_by_q)

    # Count merged results
    successful = 0
    queries_file = db_dir / "queries.csv"
    if queries_file.exists():
        try:
            df = pd.read_csv(queries_file)
            successful = len(df)
        except:
            pass

    failed = 0
    failed_file = db_dir / "failed.csv"
    if failed_file.exists():
        try:
            df = pd.read_csv(failed_file)
            failed = len(df)
        except:
            pass

    # Count pending individual files
    pending_results = len(list(db_dir.glob("result_*.json")))
    pending_failed = len(list(db_dir.glob("failed_*.json")))

    return {
        "database": database,
        "total": total,
        "completed": completed,
        "successful": successful,
        "failed": failed,
        "pending_merge": pending_results + pending_failed,
        "remaining": total - completed
    }
