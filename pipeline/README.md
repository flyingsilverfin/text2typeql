# Text2TypeQL Pipeline

Conversion pipeline that transforms Neo4j text2cypher datasets into validated TypeQL 3.0 query pairs.

## Overview

This pipeline converts Cypher queries from the [Neo4j Labs text2cypher](https://github.com/neo4j-labs/text2cypher) benchmark into TypeQL format. The output lives in `../dataset/`.

### Source Datasets

| Source | Directory | Neo4j Source | Databases | Queries |
|--------|-----------|-------------|-----------|---------|
| `synthetic-1` | `dataset/synthetic-1/` | `synthetic_opus_demodbs` | 7 | 4,776 valid |
| `synthetic-2` | `dataset/synthetic-2/` | `synthetic_gpt4o_demodbs` | 15 | 9,267 valid |

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

# List available schemas (default: synthetic-1)
python pipeline/main.py list-schemas
python pipeline/main.py list-schemas --source synthetic-2

# Convert a schema
python pipeline/main.py convert-schema movies --source synthetic-2

# Convert a single query (via Claude Code skill)
/convert-query movies 0
/convert-query movies 0 --source synthetic-2
```

## Project Structure

```
pipeline/
  main.py                 # CLI entry point (--source option on all commands)
  mcp_server.py           # MCP server for tool integration
  requirements.txt        # Python dependencies
  src/                    # Pipeline source code
    config.py             # Path, source, and connection configuration
    schema_converter.py   # Neo4j → TypeQL schema conversion
    neo4j_parser.py       # Neo4j dataset parser (source-aware)
    typedb_validator.py   # TypeDB validation client
  scripts/                # Utility scripts
    validate_typeql.py    # Validate TypeQL against TypeDB
    get_query.py          # Extract query by database, index, and source
    get_batch.py          # Extract batch of queries
    csv_read_row.py       # Read single CSV row by index
    csv_append_row.py     # Append row to CSV
    csv_move_row.py       # Move row between CSVs
    review_helper.py      # Move queries during semantic review
    merge_dataset.py      # Generate all_queries.csv (per-source or merged)
    batch_validate.py     # Batch validate queries
  prompts/                # Prompt templates for AI agents
  docs/                   # Pipeline documentation
    typeql_reference.md   # TypeQL 3.0 reference guide
    semantic_review_notes.md  # Semantic review guidance
    suggestions.md        # Validated advanced pattern examples
  data/                   # Source data (gitignored)
    text2cypher/          # Cloned Neo4j dataset
      datasets/
        synthetic_opus_demodbs/    # Source for synthetic-1
        synthetic_gpt4o_demodbs/   # Source for synthetic-2
```

## Conversion Status

### synthetic-1 (opus) -- All 7 databases fully converted

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

### synthetic-2 (gpt4o) -- 12/15 databases fully converted

| Database | Total | Converted | Failed | Status |
|----------|-------|-----------|--------|--------|
| bluesky | 135 | 135 | 0 | ✓ complete |
| buzzoverflow | 592 | 578 | 14 | ✓ complete |
| companies | 966 | 941 | 25 | ✓ complete |
| fincen | 614 | 584 | 30 | ✓ complete |
| gameofthrones | 393 | 384 | 9 | ✓ complete |
| grandstack | 807 | 793 | 14 | ✓ complete |
| movies | 738 | 728 | 10 | ✓ complete |
| neoflix | 923 | 913 | 10 | ✓ complete |
| network | 625 | 613 | 12 | ✓ complete |
| northwind | 807 | 780 | 27 | ✓ complete |
| offshoreleaks | 507 | 493 | 14 | ✓ complete |
| stackoverflow2 | 307 | 298 | 9 | ✓ complete |
| recommendations | 775 | -- | -- | pending |
| twitch | 576 | -- | -- | pending |
| twitter | 502 | -- | -- | pending |
| **Total** | **9,267** | **7,240** | **174** | **80%** |

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
python3 pipeline/scripts/csv_read_row.py dataset/<source>/<db>/queries.csv <index>

# Check if row exists
python3 pipeline/scripts/csv_read_row.py dataset/<source>/<db>/queries.csv <index> --exists

# Append a row
python3 pipeline/scripts/csv_append_row.py dataset/<source>/<db>/queries.csv '{"original_index": 0, "question": "...", "cypher": "...", "typeql": "..."}'

# Move a row between CSVs
python3 pipeline/scripts/csv_move_row.py <source.csv> <dest.csv> <index> '{"typeql": "..."}'
```

### Dataset Merge

```bash
# Merge a single source
python3 pipeline/scripts/merge_dataset.py --source synthetic-1

# Merge all sources into dataset/all_queries.csv
python3 pipeline/scripts/merge_dataset.py
```

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
