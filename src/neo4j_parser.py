"""Parse Neo4j text2cypher dataset files."""

import ast
import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .config import SCHEMAS_CSV, QUERIES_CSV


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
    false_schema: bool


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


def parse_schemas(csv_path: Path = None) -> dict[str, Neo4jSchema]:
    """
    Parse Neo4j schemas from CSV file.

    Args:
        csv_path: Path to text2cypher_schemas.csv

    Returns:
        Dictionary mapping database names to Neo4jSchema objects
    """
    csv_path = csv_path or SCHEMAS_CSV

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
    database: str = None
) -> list[QueryRecord]:
    """
    Parse queries from CSV file.

    Args:
        csv_path: Path to text2cypher_claudeopus.csv
        database: Filter to specific database (optional)

    Returns:
        List of QueryRecord objects
    """
    csv_path = csv_path or QUERIES_CSV

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
        # false_schema is NaN when valid, or contains a string describing the issue
        false_schema_val = row.get('false_schema', None)
        has_false_schema = pd.notna(false_schema_val) and false_schema_val != ''

        queries.append(QueryRecord(
            question=row['question'],
            cypher=row['cypher'],
            database=row['database'],
            query_type=row.get('type', ''),
            syntax_error=bool(row.get('syntax_error', False)),
            timeout=bool(row.get('timeout', False)),
            returns_results=bool(row.get('returns_results', True)),
            false_schema=has_false_schema
        ))

    return queries


def filter_valid_queries(queries: list[QueryRecord]) -> list[QueryRecord]:
    """
    Filter out queries with known issues.

    Excludes queries with:
    - syntax_error: True
    - false_schema: True
    """
    return [
        q for q in queries
        if not q.syntax_error and not q.false_schema
    ]


def list_databases(csv_path: Path = None) -> list[str]:
    """List all available database names from schemas CSV."""
    schemas = parse_schemas(csv_path)
    return sorted(schemas.keys())


def get_schema(database: str, csv_path: Path = None) -> Neo4jSchema:
    """Get schema for a specific database."""
    schemas = parse_schemas(csv_path)
    if database not in schemas:
        available = ', '.join(sorted(schemas.keys()))
        raise ValueError(
            f"Database '{database}' not found. Available: {available}"
        )
    return schemas[database]


def get_query_count(database: str, csv_path: Path = None) -> dict:
    """Get query statistics for a database."""
    queries = parse_queries(csv_path, database)
    valid = filter_valid_queries(queries)

    return {
        'total': len(queries),
        'valid': len(valid),
        'syntax_errors': sum(1 for q in queries if q.syntax_error),
        'false_schema': sum(1 for q in queries if q.false_schema)
    }
