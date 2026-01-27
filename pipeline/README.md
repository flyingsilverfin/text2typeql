# Text2TypeQL Pipeline

Conversion pipeline that transforms Neo4j text2cypher datasets into validated TypeQL 3.0 query pairs.

## Overview

This pipeline converts Cypher queries from the [Neo4j Labs text2cypher](https://github.com/neo4j-labs/text2cypher) benchmark into TypeQL format. The output lives in `../dataset/`.

### Pipeline Stages

1. **Schema conversion** -- Neo4j schemas manually translated to TypeQL 3.0 (entity types, relations with explicit roles, attributes)
2. **Query conversion via AI agents** -- Each Cypher query converted using Claude Code subagents under a TypeQL 3.0 reference
3. **Validation against TypeDB** -- Every query executed against a live TypeDB instance
4. **Semantic review** -- Second pass verifying each TypeQL query answers the English question
5. **Failure documentation** -- Unconvertible queries documented with specific reasons

## Quick Start

```bash
# Start TypeDB server (required for validation)
typedb server --development-mode.enabled=true

# Clone source dataset
python pipeline/main.py setup

# List available schemas
python pipeline/main.py list-schemas

# Convert a schema
python pipeline/main.py convert-schema movies

# Convert a single query (via Claude Code skill)
/convert-query movies 0
```

## Project Structure

```
pipeline/
  main.py                 # CLI entry point
  mcp_server.py           # MCP server for tool integration
  requirements.txt        # Python dependencies
  src/                    # Pipeline source code
    config.py             # Path and connection configuration
    schema_converter.py   # Neo4j → TypeQL schema conversion
    query_converter.py    # Query conversion logic
    batch_converter.py    # Batch conversion runner
    mcp_batch_runner.py   # MCP-based batch runner
    neo4j_parser.py       # Neo4j dataset parser
    typedb_validator.py   # TypeDB validation client
  scripts/                # Utility scripts
    validate_typeql.py    # Validate TypeQL against TypeDB
    get_query.py          # Extract query by database and index
    csv_read_row.py       # Read single CSV row by index
    csv_append_row.py     # Append row to CSV
    csv_move_row.py       # Move row between CSVs
    review_helper.py      # Move queries during semantic review
    merge_dataset.py      # Generate dataset/all_queries.csv
    batch_validate.py     # Batch validate queries
    deep_semantic_review.py    # Deep semantic analysis
    final_semantic_check.py    # Final semantic verification
    move_semantic_failures.py  # Bulk move semantic failures
    validate_companies.py      # Companies-specific validation
    bulk_fix_schema_changes.py # Bulk fix after schema changes
  prompts/                # Prompt templates for AI agents
  docs/                   # Pipeline documentation
    typeql_reference.md   # TypeQL 3.0 reference guide
    semantic_review_notes.md  # Semantic review guidance
    suggestions.md        # Validated advanced pattern examples
  data/                   # Source data (gitignored)
    text2cypher/          # Cloned Neo4j dataset
```

## Conversion Status

All 7 databases fully converted.

| Database | Valid | Failed | Total |
|----------|-------|--------|-------|
| twitter | 491 | 2 | 493 |
| twitch | 553 | 8 | 561 |
| movies | 723 | 6 | 729 |
| neoflix | 910 | 5 | 915 |
| recommendations | 741 | 12 | 753 |
| companies | 929 | 4 | 933 |
| gameofthrones | 381 | 11 | 392 |
| **Total** | **4,728** | **48** | **4,776** |

### Failed Query Categories

| Category | Count | Description |
|----------|-------|-------------|
| `size()` function | ~15 | String/list length not supported |
| `collect()` aggregation | ~5 | No list collection equivalent |
| Array operations | ~8 | Array indexing, iteration |
| String functions | ~5 | `split()`, `left()`, regex not supported |
| Date/duration arithmetic | ~5 | Duration calculations, epoch conversion |
| Schema mismatches | ~5 | Cypher assumes features not in schema |
| Other | ~5 | `UNWIND`, complex patterns |

## Key Scripts

### Validation

```bash
# Validate a TypeQL query against TypeDB
python3 pipeline/scripts/validate_typeql.py <database> --file /tmp/query.tql

# Validate inline
python3 pipeline/scripts/validate_typeql.py <database> 'match $x isa user; limit 1; fetch { "name": $x.screen_name };'
```

### CSV Operations

```bash
# Read a row
python3 pipeline/scripts/csv_read_row.py dataset/<db>/queries.csv <index>

# Check if row exists
python3 pipeline/scripts/csv_read_row.py dataset/<db>/queries.csv <index> --exists

# Append a row
python3 pipeline/scripts/csv_append_row.py dataset/<db>/queries.csv '{"original_index": 0, "question": "...", "cypher": "...", "typeql": "..."}'

# Move a row between CSVs
python3 pipeline/scripts/csv_move_row.py <source.csv> <dest.csv> <index> '{"typeql": "..."}'
```

### Dataset Merge

```bash
# Regenerate the merged dataset
python3 pipeline/scripts/merge_dataset.py
```

## MCP Server

The MCP server (`mcp_server.py`) provides tools for integration with Claude Code:

- `convert_query` -- Get schema context for converting a query
- `validate_typeql` -- Validate TypeQL against running TypeDB
- `get_schema` -- Get TypeQL schema for a database
- `list_databases` -- List available databases

## Known Limitations / Future Work

Some Cypher patterns have no TypeQL equivalent:
- `size(property)` -- No string/list length function
- `array[-1]` -- No array index access
- `collect()` -- No list aggregation
- Date/duration arithmetic

### Schema Naming Convention

TypeDB convention prefers short, generic role names (e.g., `containing` / `contained`) over type-indicating names (e.g., `containing_tweet` / `contained_link`). All current schemas use the longer form. A future pass could normalize role names and update all queries accordingly.

## TypeQL Pattern Reference

See `docs/typeql_reference.md` for the comprehensive TypeQL 3.0 reference used during conversion, and `docs/suggestions.md` for validated examples of advanced patterns.
