"""Configuration settings for text2typeql."""

import os
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"
PROMPTS_DIR = PROJECT_ROOT / "prompts"

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


def get_output_dir(database_name: str) -> Path:
    """Get output directory for a specific database."""
    output_dir = OUTPUT_DIR / database_name
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def ensure_dirs():
    """Ensure all required directories exist."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
