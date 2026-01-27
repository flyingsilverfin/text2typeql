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

# Neo4j dataset paths
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


def get_dataset_dir(database_name: str) -> Path:
    """Get dataset directory for a specific database."""
    dataset_dir = DATASET_DIR / database_name
    dataset_dir.mkdir(parents=True, exist_ok=True)
    return dataset_dir


# Backward compatibility alias
get_output_dir = get_dataset_dir


def ensure_dirs():
    """Ensure all required directories exist."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
