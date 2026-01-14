# Text2TypeQL Dataset Generation Plan

## Goal
Convert Neo4j text2cypher dataset (synthetic_opus_demodbs) to TypeQL format for training text-to-TypeQL models.

## Source Data
- Repository: https://github.com/neo4j-labs/text2cypher
- Directory: `datasets/synthetic_opus_demodbs/`
- Files:
  - `text2cypher_schemas.csv` - Neo4j schemas in JSON format (7 databases)
  - `text2cypher_claudeopus.csv` - Question/Cypher pairs with columns: question, cypher, type, database, syntax_error, timeout, returns_results, false_schema

## Project Structure

```
text2typeql/
├── CLAUDE.md                    # TypeDB 3.x reference & project notes
├── requirements.txt
├── main.py                      # CLI entry point
├── prompts/
│   ├── schema_conversion.txt    # Neo4j schema -> TypeQL prompt
│   └── query_conversion.txt     # Cypher -> TypeQL prompt
├── src/
│   ├── __init__.py
│   ├── config.py               # Settings (API key, TypeDB connection)
│   ├── neo4j_parser.py         # Parse Neo4j schema JSON and query CSV
│   ├── schema_converter.py     # Claude-based schema conversion
│   ├── query_converter.py      # Claude-based query conversion
│   └── typedb_validator.py     # TypeDB query validation
├── data/
│   └── text2cypher/            # Cloned Neo4j repo (gitignored)
└── output/
    └── {database_name}/        # One folder per schema
        ├── schema.tql          # Generated TypeQL schema
        ├── neo4j_schema.json   # Original Neo4j schema (for reference)
        ├── queries.csv         # Validated question/TypeQL pairs
        └── failed_queries.csv  # Failed conversions for review
```

## Implementation Steps

### Phase 1: Project Setup

1. **Initialize project**
   ```bash
   git init
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Clone Neo4j dataset**
   ```bash
   mkdir -p data
   git clone https://github.com/neo4j-labs/text2cypher.git data/text2cypher
   ```

### Phase 2: Core Modules

3. **src/config.py** - Configuration
   - TypeDB connection: localhost:1729, admin/password
   - Anthropic API key from environment
   - Model selection (claude-sonnet-4-20250514 default)

4. **src/neo4j_parser.py** - Parse Neo4j data
   - `parse_schemas(csv_path) -> dict[database_name, schema_json]`
   - `parse_queries(csv_path, database_name) -> list[QueryRecord]`
   - `filter_valid_queries(queries)` - exclude syntax_error=True, false_schema=True

5. **src/typedb_validator.py** - TypeDB validation
   - Connect using: `TypeDB.driver(address, Credentials(user, pass), DriverOptions(...))`
   - Schema validation: Execute `define` in SCHEMA transaction, catch exceptions
   - Query validation: Execute query in READ transaction with `.resolve()`, catch exceptions

6. **src/schema_converter.py** - Schema conversion with Claude
   - Load prompt template
   - Call Claude API with Neo4j schema JSON
   - Validate against TypeDB
   - Retry loop (max 3 attempts) on syntax errors

7. **src/query_converter.py** - Query conversion with Claude
   - For each question/cypher pair:
     - Call Claude with: TypeQL schema, Neo4j schema, question, cypher
     - Validate generated TypeQL against TypeDB
     - Retry loop on errors, record success/failure

### Phase 3: Prompts

8. **prompts/schema_conversion.txt** - Neo4j schema -> TypeQL
9. **prompts/query_conversion.txt** - Cypher -> TypeQL

### Phase 4: CLI

10. **main.py** - Click CLI with commands:
    - `setup` - Clone Neo4j repo
    - `list-schemas` - List available databases
    - `convert-schema <name>` - Convert one schema to TypeQL
    - `approve-schema <name>` - Mark schema as reviewed
    - `convert-queries <name> [--limit N]` - Convert queries

## Workflow

### Schema Conversion (do first, manually verify)

```bash
# 1. Setup
python main.py setup

# 2. List schemas
python main.py list-schemas

# 3. Convert first schema
python main.py convert-schema movies

# 4. MANUAL: Review output/movies/schema.tql
# 5. Approve after review
python main.py approve-schema movies
```

### Query Conversion (start with 10, then scale)

```bash
# 6. Convert 10 queries
python main.py convert-queries movies --limit 10

# 7. MANUAL: Review output/movies/queries.csv

# 8. Scale up
python main.py convert-queries movies
```

## Current Progress

- [x] Git repo initialized
- [x] Virtual environment created
- [x] requirements.txt created
- [x] .gitignore created
- [ ] Clone Neo4j dataset (run `python main.py setup`)
- [x] Create CLAUDE.md with TypeDB reference
- [x] Create src/config.py
- [x] Create src/neo4j_parser.py
- [x] Create src/typedb_validator.py
- [x] Create prompts/schema_conversion.txt
- [x] Create prompts/query_conversion.txt
- [x] Create src/schema_converter.py
- [x] Create src/query_converter.py
- [x] Create main.py CLI
