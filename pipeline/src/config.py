"""Configuration settings for text2typeql."""

import os
from pathlib import Path

# Project paths
PIPELINE_ROOT = Path(__file__).parent.parent      # pipeline/
REPO_ROOT = PIPELINE_ROOT.parent                   # repository root
DATASET_DIR = REPO_ROOT / "dataset"                # dataset/ at repo root
DATA_DIR = PIPELINE_ROOT / "data"                  # pipeline/data/
OUTPUT_DIR = DATASET_DIR                           # backward compat alias
PROJECT_ROOT = REPO_ROOT                           # backward compat alias
PROMPTS_DIR = PIPELINE_ROOT / "prompts"

# Source dataset configurations
SOURCES = {
    "synthetic-1": {
        "neo4j_dir_name": "synthetic_opus_demodbs",
        "csv_filename": "text2cypher_claudeopus.csv",
        "exclude_column": "false_schema",
        "exclude_check": "notempty",  # excluded if column is non-empty string
    },
    "synthetic-2": {
        "neo4j_dir_name": "synthetic_gpt4o_demodbs",
        "csv_filename": "text2cypher_gpt4o.csv",
        "exclude_column": "no_cypher",
        "exclude_check": "true",  # excluded if value == "True"
    },
}
DEFAULT_SOURCE = "synthetic-1"

# Neo4j dataset paths (backward compat, point to synthetic-1)
TEXT2CYPHER_DIR = DATA_DIR / "text2cypher" / "datasets" / "synthetic_opus_demodbs"
SCHEMAS_CSV = TEXT2CYPHER_DIR / "text2cypher_schemas.csv"
QUERIES_CSV = TEXT2CYPHER_DIR / "text2cypher_claudeopus.csv"

# TypeDB connection settings
TYPEDB_ADDRESS = os.getenv("TYPEDB_ADDRESS", "localhost:1729")
TYPEDB_USERNAME = os.getenv("TYPEDB_USERNAME", "admin")
TYPEDB_PASSWORD = os.getenv("TYPEDB_PASSWORD", "password")

# Anthropic API settings
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
DEFAULT_MODEL = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-20250514")

# Conversion settings
MAX_RETRIES = 3
SCHEMA_VALIDATION_DB = "text2typeql_validation"


def get_source_config(source: str) -> dict:
    """Get configuration for a source dataset."""
    if source not in SOURCES:
        raise ValueError(f"Unknown source '{source}'. Available: {list(SOURCES.keys())}")
    return SOURCES[source]


def get_source_text2cypher_dir(source: str) -> Path:
    """Get the Neo4j text2cypher directory for a source."""
    config = get_source_config(source)
    return DATA_DIR / "text2cypher" / "datasets" / config["neo4j_dir_name"]


def get_source_queries_csv(source: str) -> Path:
    """Get the queries CSV path for a source."""
    config = get_source_config(source)
    return get_source_text2cypher_dir(source) / config["csv_filename"]


def get_source_schemas_csv(source: str) -> Path:
    """Get the schemas CSV path for a source (identical across sources)."""
    return get_source_text2cypher_dir(source) / "text2cypher_schemas.csv"


def is_query_excluded(row: dict, source: str) -> bool:
    """Check if a query row should be excluded based on source-specific column."""
    config = get_source_config(source)
    col = config["exclude_column"]
    check = config["exclude_check"]
    value = row.get(col, "")
    if check == "notempty":
        return bool(value and str(value).strip())
    elif check == "true":
        return str(value).strip().lower() == "true"
    return False


def get_dataset_dir(database_name: str, source: str = DEFAULT_SOURCE) -> Path:
    """Get dataset directory for a specific database and source."""
    dataset_dir = DATASET_DIR / source / database_name
    dataset_dir.mkdir(parents=True, exist_ok=True)
    return dataset_dir


# Backward compatibility alias
get_output_dir = get_dataset_dir


def ensure_dirs():
    """Ensure all required directories exist."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
