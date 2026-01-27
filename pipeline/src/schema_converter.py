"""Convert Neo4j schemas to TypeQL using Claude."""

import json
from pathlib import Path

import anthropic

from .config import (
    ANTHROPIC_API_KEY,
    DEFAULT_MODEL,
    DEFAULT_SOURCE,
    MAX_RETRIES,
    PROMPTS_DIR,
    get_dataset_dir,
)
from .neo4j_parser import Neo4jSchema, get_schema
from .typedb_validator import TypeDBValidator, ValidationResult


def load_schema_prompt() -> str:
    """Load the schema conversion prompt template."""
    prompt_path = PROMPTS_DIR / "schema_conversion.txt"
    if not prompt_path.exists():
        raise FileNotFoundError(f"Schema prompt not found at {prompt_path}")
    return prompt_path.read_text()


def extract_typeql(response: str) -> str:
    """Extract TypeQL from Claude's response, removing any markdown."""
    text = response.strip()

    # Remove markdown code blocks if present
    if "```" in text:
        # Find content between ``` markers
        parts = text.split("```")
        for part in parts[1:]:  # Skip first part (before first ```)
            # Remove language identifier if present (e.g., "typeql\n")
            lines = part.split("\n", 1)
            if len(lines) > 1:
                content = lines[1] if lines[0].strip() in ("", "typeql", "tql") else part
            else:
                content = part
            # Check if this looks like TypeQL
            if "define" in content.lower():
                text = content
                break

    # Find where 'define' starts and extract from there
    # This handles cases where the LLM adds explanatory text before the schema
    lower_text = text.lower()
    define_idx = lower_text.find("define")
    if define_idx > 0:
        text = text[define_idx:]

    # Remove any trailing ``` if present
    if text.rstrip().endswith("```"):
        text = text.rstrip()[:-3]

    return text.strip()


def convert_schema(
    neo4j_schema: Neo4jSchema,
    validator: TypeDBValidator = None,
    model: str = None,
    max_retries: int = None
) -> tuple[str, list[str]]:
    """
    Convert a Neo4j schema to TypeQL using Claude.

    Args:
        neo4j_schema: Parsed Neo4j schema
        validator: TypeDB validator (optional, for validation)
        model: Claude model to use
        max_retries: Maximum conversion attempts

    Returns:
        Tuple of (typeql_schema, list of error messages from attempts)
    """
    if not ANTHROPIC_API_KEY:
        raise ValueError(
            "ANTHROPIC_API_KEY environment variable not set. "
            "Set it with: export ANTHROPIC_API_KEY=your_key"
        )

    model = model or DEFAULT_MODEL
    max_retries = max_retries or MAX_RETRIES

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    prompt_template = load_schema_prompt()

    # Build the initial prompt
    prompt = prompt_template.replace("{NEO4J_SCHEMA}", neo4j_schema.to_json_str())

    errors = []
    typeql_schema = None

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
            max_tokens=4096,
            messages=[{"role": "user", "content": current_prompt}]
        )

        typeql_schema = extract_typeql(response.content[0].text)

        # Validate if validator provided
        if validator:
            result = validator.validate_schema(typeql_schema)
            if result.success:
                return typeql_schema, errors
            else:
                errors.append(result.error_message)
        else:
            # No validation, return first attempt
            return typeql_schema, errors

    # All retries exhausted
    return typeql_schema, errors


def convert_and_save_schema(
    database: str,
    validator: TypeDBValidator = None,
    model: str = None,
    source: str = DEFAULT_SOURCE,
) -> tuple[bool, str, list[str]]:
    """
    Convert a Neo4j schema and save to output directory.

    Args:
        database: Database name to convert
        validator: TypeDB validator (optional)
        model: Claude model to use
        source: Source dataset name (e.g. "synthetic-1", "synthetic-2")

    Returns:
        Tuple of (success, typeql_schema, errors)
    """
    # Get the Neo4j schema
    neo4j_schema = get_schema(database, source=source)

    # Convert
    typeql_schema, errors = convert_schema(
        neo4j_schema,
        validator=validator,
        model=model
    )

    # Save outputs
    output_dir = get_dataset_dir(database, source)

    # Save TypeQL schema
    schema_path = output_dir / "schema.tql"
    schema_path.write_text(typeql_schema)

    # Save original Neo4j schema for reference
    neo4j_path = output_dir / "neo4j_schema.json"
    neo4j_path.write_text(neo4j_schema.to_json_str())

    # Determine success
    success = len(errors) == 0 or (validator is None)

    # Save conversion status
    status = {
        "database": database,
        "success": success,
        "attempts": len(errors) + 1,
        "errors": errors,
        "approved": False
    }
    status_path = output_dir / "status.json"
    status_path.write_text(json.dumps(status, indent=2))

    return success, typeql_schema, errors


def load_schema(database: str, source: str = DEFAULT_SOURCE) -> str | None:
    """Load a previously converted TypeQL schema."""
    output_dir = get_dataset_dir(database, source)
    schema_path = output_dir / "schema.tql"

    if schema_path.exists():
        return schema_path.read_text()
    return None


def is_schema_approved(database: str, source: str = DEFAULT_SOURCE) -> bool:
    """Check if a schema has been manually approved."""
    output_dir = get_dataset_dir(database, source)
    status_path = output_dir / "status.json"

    if status_path.exists():
        status = json.loads(status_path.read_text())
        return status.get("approved", False)
    return False


def approve_schema(database: str, source: str = DEFAULT_SOURCE) -> bool:
    """Mark a schema as manually approved."""
    output_dir = get_dataset_dir(database, source)
    status_path = output_dir / "status.json"

    if not status_path.exists():
        return False

    status = json.loads(status_path.read_text())
    status["approved"] = True
    status_path.write_text(json.dumps(status, indent=2))
    return True
