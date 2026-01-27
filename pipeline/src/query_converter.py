"""Convert Cypher queries to TypeQL using Claude."""

import csv
import json
from dataclasses import dataclass, asdict
from pathlib import Path

import anthropic

from .config import (
    ANTHROPIC_API_KEY,
    DEFAULT_MODEL,
    MAX_RETRIES,
    PROMPTS_DIR,
    get_output_dir,
)
from .neo4j_parser import (
    QueryRecord,
    get_schema,
    parse_queries,
    filter_valid_queries,
)
from .schema_converter import load_schema, is_schema_approved
from .typedb_validator import TypeDBValidator


@dataclass
class ConvertedQuery:
    """Result of a query conversion."""
    question: str
    cypher: str
    typeql: str
    success: bool
    error_message: str | None = None
    attempts: int = 1


def load_query_prompt() -> str:
    """Load the query conversion prompt template."""
    prompt_path = PROMPTS_DIR / "query_conversion.txt"
    if not prompt_path.exists():
        raise FileNotFoundError(f"Query prompt not found at {prompt_path}")
    return prompt_path.read_text()


def extract_typeql(response: str) -> str:
    """Extract TypeQL from Claude's response, removing any markdown."""
    text = response.strip()

    # Remove markdown code blocks if present
    if text.startswith("```"):
        lines = text.split("\n")
        # Remove first line (```typeql or ```)
        lines = lines[1:]
        # Remove last line if it's ```
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines)

    return text.strip()


def convert_query(
    query: QueryRecord,
    typeql_schema: str,
    neo4j_schema_json: str,
    validator: TypeDBValidator = None,
    model: str = None,
    max_retries: int = None
) -> ConvertedQuery:
    """
    Convert a single Cypher query to TypeQL using Claude.

    Args:
        query: The query record to convert
        typeql_schema: The TypeQL schema
        neo4j_schema_json: Original Neo4j schema JSON
        validator: TypeDB validator (optional)
        model: Claude model to use
        max_retries: Maximum conversion attempts

    Returns:
        ConvertedQuery with results
    """
    if not ANTHROPIC_API_KEY:
        raise ValueError(
            "ANTHROPIC_API_KEY environment variable not set. "
            "Set it with: export ANTHROPIC_API_KEY=your_key"
        )

    model = model or DEFAULT_MODEL
    max_retries = max_retries or MAX_RETRIES

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    prompt_template = load_query_prompt()

    # Build the prompt
    prompt = prompt_template.replace("{TYPEQL_SCHEMA}", typeql_schema)
    prompt = prompt.replace("{NEO4J_SCHEMA}", neo4j_schema_json)
    prompt = prompt.replace("{QUESTION}", query.question)
    prompt = prompt.replace("{CYPHER_QUERY}", query.cypher)

    errors = []
    typeql_query = None

    for attempt in range(max_retries):
        # Add error context for retries
        if errors:
            error_context = "\n\n## Previous Attempt Failed\n"
            error_context += f"Error: {errors[-1]}\n"
            error_context += "Please fix the issue and try again."
            current_prompt = prompt + error_context
        else:
            current_prompt = prompt

        # Call Claude
        response = client.messages.create(
            model=model,
            max_tokens=2048,
            messages=[{"role": "user", "content": current_prompt}]
        )

        typeql_query = extract_typeql(response.content[0].text)

        # Validate if validator provided
        if validator:
            result = validator.validate_query(typeql_query, typeql_schema)
            if result.success:
                return ConvertedQuery(
                    question=query.question,
                    cypher=query.cypher,
                    typeql=typeql_query,
                    success=True,
                    attempts=attempt + 1
                )
            else:
                errors.append(result.error_message)
        else:
            # No validation, return first attempt
            return ConvertedQuery(
                question=query.question,
                cypher=query.cypher,
                typeql=typeql_query,
                success=True,
                attempts=1
            )

    # All retries exhausted
    return ConvertedQuery(
        question=query.question,
        cypher=query.cypher,
        typeql=typeql_query or "",
        success=False,
        error_message=errors[-1] if errors else "Unknown error",
        attempts=len(errors)
    )


def convert_queries(
    database: str,
    validator: TypeDBValidator = None,
    model: str = None,
    limit: int = None,
    skip_approved_check: bool = False
) -> tuple[list[ConvertedQuery], list[ConvertedQuery]]:
    """
    Convert all queries for a database.

    Args:
        database: Database name
        validator: TypeDB validator (optional)
        model: Claude model to use
        limit: Maximum number of queries to convert
        skip_approved_check: Skip checking if schema is approved

    Returns:
        Tuple of (successful_queries, failed_queries)
    """
    # Load TypeQL schema
    typeql_schema = load_schema(database)
    if not typeql_schema:
        raise ValueError(
            f"No TypeQL schema found for '{database}'. "
            f"Run 'python main.py convert-schema {database}' first."
        )

    # Check if schema is approved
    if not skip_approved_check and not is_schema_approved(database):
        raise ValueError(
            f"Schema for '{database}' has not been approved. "
            f"Review dataset/{database}/schema.tql and run "
            f"'python main.py approve-schema {database}' to approve."
        )

    # Get Neo4j schema JSON
    neo4j_schema = get_schema(database)
    neo4j_schema_json = neo4j_schema.to_json_str()

    # Get and filter queries
    queries = parse_queries(database=database)
    queries = filter_valid_queries(queries)

    if limit:
        queries = queries[:limit]

    successful = []
    failed = []

    for i, query in enumerate(queries):
        print(f"Converting query {i + 1}/{len(queries)}...")

        result = convert_query(
            query=query,
            typeql_schema=typeql_schema,
            neo4j_schema_json=neo4j_schema_json,
            validator=validator,
            model=model
        )

        if result.success:
            successful.append(result)
        else:
            failed.append(result)

    return successful, failed


def save_query_results(
    database: str,
    successful: list[ConvertedQuery],
    failed: list[ConvertedQuery]
):
    """Save query conversion results to CSV files."""
    output_dir = get_output_dir(database)

    # Save successful queries
    if successful:
        queries_path = output_dir / "queries.csv"
        with open(queries_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["question", "cypher", "typeql", "attempts"]
            )
            writer.writeheader()
            for q in successful:
                writer.writerow({
                    "question": q.question,
                    "cypher": q.cypher,
                    "typeql": q.typeql,
                    "attempts": q.attempts
                })

    # Save failed queries
    if failed:
        failed_path = output_dir / "failed_queries.csv"
        with open(failed_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["question", "cypher", "typeql", "error_message", "attempts"]
            )
            writer.writeheader()
            for q in failed:
                writer.writerow({
                    "question": q.question,
                    "cypher": q.cypher,
                    "typeql": q.typeql,
                    "error_message": q.error_message,
                    "attempts": q.attempts
                })

    # Update status
    status_path = output_dir / "status.json"
    if status_path.exists():
        status = json.loads(status_path.read_text())
    else:
        status = {"database": database}

    status["queries"] = {
        "total_converted": len(successful) + len(failed),
        "successful": len(successful),
        "failed": len(failed)
    }
    status_path.write_text(json.dumps(status, indent=2))

    return output_dir / "queries.csv", output_dir / "failed_queries.csv"


def convert_and_save_queries(
    database: str,
    validator: TypeDBValidator = None,
    model: str = None,
    limit: int = None,
    skip_approved_check: bool = False
) -> tuple[int, int, Path, Path]:
    """
    Convert queries and save results.

    Args:
        database: Database name
        validator: TypeDB validator (optional)
        model: Claude model to use
        limit: Maximum queries to convert
        skip_approved_check: Skip schema approval check

    Returns:
        Tuple of (successful_count, failed_count, queries_path, failed_path)
    """
    successful, failed = convert_queries(
        database=database,
        validator=validator,
        model=model,
        limit=limit,
        skip_approved_check=skip_approved_check
    )

    queries_path, failed_path = save_query_results(database, successful, failed)

    return len(successful), len(failed), queries_path, failed_path
