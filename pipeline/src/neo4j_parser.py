"""Parse Neo4j text2cypher dataset files."""

import ast
import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .config import (
    DEFAULT_SOURCE,
    get_source_queries_csv,
    get_source_schemas_csv,
    is_query_excluded,
)


@dataclass
class QueryRecord:
    """A single question/cypher pair from the dataset."""
    question: str
    cypher: str
    database: str
    query_type: str
    syntax_error: bool
    timeout: bool
    returns_results: bool
    excluded: bool


@dataclass
class Neo4jSchema:
    """Parsed Neo4j schema structure."""
    database: str
    node_props: dict[str, list[dict]]  # {NodeLabel: [{property, type}, ...]}
    rel_props: dict[str, list[dict]]   # {REL_TYPE: [{property, type}, ...]}
    relationships: list[dict]           # [{start, type, end}, ...]
    raw_json: dict                       # Original JSON for reference

    def to_json_str(self, indent: int = 2) -> str:
        """Convert schema to formatted JSON string."""
        return json.dumps(self.raw_json, indent=indent)


def parse_schemas(csv_path: Path = None, source: str = DEFAULT_SOURCE) -> dict[str, Neo4jSchema]:
    """
    Parse Neo4j schemas from CSV file.

    Args:
        csv_path: Path to text2cypher_schemas.csv (overrides source)
        source: Source dataset name (e.g. "synthetic-1", "synthetic-2")

    Returns:
        Dictionary mapping database names to Neo4jSchema objects
    """
    csv_path = csv_path or get_source_schemas_csv(source)

    if not csv_path.exists():
        raise FileNotFoundError(
            f"Schemas CSV not found at {csv_path}. "
            "Run 'python main.py setup' to clone the dataset."
        )

    df = pd.read_csv(csv_path)
    schemas = {}

    for _, row in df.iterrows():
        db_name = row['database']
        # structured_schema is stored as Python dict literal (single quotes)
        schema_data = ast.literal_eval(row['structured_schema'])

        schemas[db_name] = Neo4jSchema(
            database=db_name,
            node_props=schema_data.get('node_props', {}),
            rel_props=schema_data.get('rel_props', {}),
            relationships=schema_data.get('relationships', []),
            raw_json=schema_data
        )

    return schemas


def parse_queries(
    csv_path: Path = None,
    database: str = None,
    source: str = DEFAULT_SOURCE,
) -> list[QueryRecord]:
    """
    Parse queries from CSV file.

    Args:
        csv_path: Path to queries CSV (overrides source)
        database: Filter to specific database (optional)
        source: Source dataset name (e.g. "synthetic-1", "synthetic-2")

    Returns:
        List of QueryRecord objects
    """
    csv_path = csv_path or get_source_queries_csv(source)

    if not csv_path.exists():
        raise FileNotFoundError(
            f"Queries CSV not found at {csv_path}. "
            "Run 'python main.py setup' to clone the dataset."
        )

    df = pd.read_csv(csv_path)

    if database:
        df = df[df['database'] == database]

    queries = []
    for _, row in df.iterrows():
        row_dict = row.to_dict()
        queries.append(QueryRecord(
            question=row['question'],
            cypher=row['cypher'],
            database=row['database'],
            query_type=row.get('type', ''),
            syntax_error=bool(row.get('syntax_error', False)),
            timeout=bool(row.get('timeout', False)),
            returns_results=bool(row.get('returns_results', True)),
            excluded=is_query_excluded(row_dict, source)
        ))

    return queries


def filter_valid_queries(queries: list[QueryRecord]) -> list[QueryRecord]:
    """
    Filter out queries with known issues.

    Excludes queries with:
    - syntax_error: True
    - excluded: True (false_schema for opus, no_cypher for gpt4o)
    """
    return [
        q for q in queries
        if not q.syntax_error and not q.excluded
    ]


def list_databases(csv_path: Path = None, source: str = DEFAULT_SOURCE) -> list[str]:
    """List all available database names from schemas CSV."""
    schemas = parse_schemas(csv_path, source=source)
    return sorted(schemas.keys())


def get_schema(database: str, csv_path: Path = None, source: str = DEFAULT_SOURCE) -> Neo4jSchema:
    """Get schema for a specific database."""
    schemas = parse_schemas(csv_path, source=source)
    if database not in schemas:
        available = ', '.join(sorted(schemas.keys()))
        raise ValueError(
            f"Database '{database}' not found. Available: {available}"
        )
    return schemas[database]


def get_query_count(database: str, csv_path: Path = None, source: str = DEFAULT_SOURCE) -> dict:
    """Get query statistics for a database."""
    queries = parse_queries(csv_path, database, source=source)
    valid = filter_valid_queries(queries)

    return {
        'total': len(queries),
        'valid': len(valid),
        'syntax_errors': sum(1 for q in queries if q.syntax_error),
        'excluded': sum(1 for q in queries if q.excluded)
    }
