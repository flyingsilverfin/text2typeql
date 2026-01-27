#!/usr/bin/env python3
"""
MCP Server for Text2TypeQL conversion.

Provides tools for:
- Getting schema and conversion context
- Validating TypeQL queries against TypeDB
- Tracking conversion progress

The host (Claude Code) does the actual LLM conversion, while this server
provides validation and context.
"""

import asyncio
import json
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import (
    TextContent,
    Tool,
)

from src.config import get_output_dir, PROMPTS_DIR, OUTPUT_DIR
from src.schema_converter import load_schema
from src.neo4j_parser import get_schema


# Initialize MCP server
server = Server("text2typeql")


def load_query_prompt() -> str:
    """Load the query conversion prompt template."""
    prompt_path = PROMPTS_DIR / "query_conversion.txt"
    return prompt_path.read_text()


def get_typedb_driver():
    """Get TypeDB driver connection."""
    from typedb.driver import TypeDB, Credentials, DriverOptions
    from src.config import TYPEDB_ADDRESS, TYPEDB_USERNAME, TYPEDB_PASSWORD

    credentials = Credentials(TYPEDB_USERNAME, TYPEDB_PASSWORD)
    options = DriverOptions(is_tls_enabled=False)
    return TypeDB.driver(TYPEDB_ADDRESS, credentials, options)


def validate_typeql(database: str, typeql: str) -> dict:
    """Validate a TypeQL query against the database schema."""
    from typedb.driver import TransactionType

    db_name = f"text2typeql_{database}"

    try:
        driver = get_typedb_driver()

        # Check if database exists
        db_exists = any(db.name == db_name for db in driver.databases.all())
        if not db_exists:
            # Create database and load schema
            typeql_schema = load_schema(database)
            if not typeql_schema:
                driver.close()
                return {"valid": False, "error": f"No schema found for {database}"}

            driver.databases.create(db_name)
            with driver.transaction(db_name, TransactionType.SCHEMA) as tx:
                tx.query(typeql_schema).resolve()
                tx.commit()

        # Try to execute query
        with driver.transaction(db_name, TransactionType.READ) as tx:
            result = tx.query(typeql).resolve()
            # Consume the iterator to ensure query executes
            if hasattr(result, 'as_concept_documents'):
                docs = list(result.as_concept_documents())
                count = len(docs)
            elif hasattr(result, 'as_aggregate'):
                count = result.as_aggregate()
            else:
                count = 0

        driver.close()
        return {"valid": True, "result_count": count}

    except Exception as e:
        error_msg = str(e)
        # Clean up truncated error messages
        if len(error_msg) > 500:
            error_msg = error_msg[:500] + "..."
        return {"valid": False, "error": error_msg}


@server.list_tools()
async def list_tools():
    """List available tools."""
    return [
        Tool(
            name="convert_query",
            description="Get context and prompt for converting a Cypher query to TypeQL. Returns the prompt with schema context that Claude should use for conversion.",
            inputSchema={
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "Database name (e.g., 'movies', 'companies')"
                    },
                    "question": {
                        "type": "string",
                        "description": "Natural language question"
                    },
                    "cypher": {
                        "type": "string",
                        "description": "Cypher query to convert"
                    },
                    "previous_error": {
                        "type": "string",
                        "description": "Error from previous attempt (for retry)"
                    },
                    "previous_typeql": {
                        "type": "string",
                        "description": "Previous TypeQL attempt (for retry)"
                    }
                },
                "required": ["database", "question", "cypher"]
            }
        ),
        Tool(
            name="convert_queries_batch",
            description="Get context and prompt for batch converting multiple Cypher queries to TypeQL",
            inputSchema={
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "Database name"
                    },
                    "queries": {
                        "type": "array",
                        "description": "Array of query objects with index, question, cypher, and optional error/typeql for retries",
                        "items": {
                            "type": "object",
                            "properties": {
                                "index": {"type": "integer"},
                                "question": {"type": "string"},
                                "cypher": {"type": "string"},
                                "typeql": {"type": "string"},
                                "error": {"type": "string"}
                            },
                            "required": ["index", "question", "cypher"]
                        }
                    }
                },
                "required": ["database", "queries"]
            }
        ),
        Tool(
            name="list_databases",
            description="List available databases with converted schemas",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="validate_typeql",
            description="Validate a TypeQL query against the TypeDB database with schema loaded",
            inputSchema={
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "Database name"
                    },
                    "typeql": {
                        "type": "string",
                        "description": "TypeQL query to validate"
                    }
                },
                "required": ["database", "typeql"]
            }
        ),
        Tool(
            name="get_schema",
            description="Get the TypeQL schema for a database",
            inputSchema={
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "Database name"
                    }
                },
                "required": ["database"]
            }
        )
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict):
    """Handle tool calls."""

    if name == "list_databases":
        databases = []
        if OUTPUT_DIR.exists():
            for db_dir in OUTPUT_DIR.iterdir():
                if db_dir.is_dir() and (db_dir / "schema.tql").exists():
                    databases.append(db_dir.name)
        return [TextContent(type="text", text=json.dumps(sorted(databases)))]

    elif name == "get_schema":
        database = arguments["database"]
        schema = load_schema(database)
        if schema:
            return [TextContent(type="text", text=schema)]
        else:
            return [TextContent(type="text", text=f"No schema found for '{database}'")]

    elif name == "validate_typeql":
        database = arguments["database"]
        typeql = arguments["typeql"]
        result = validate_typeql(database, typeql)
        return [TextContent(type="text", text=json.dumps(result))]

    elif name == "convert_query":
        database = arguments["database"]
        question = arguments["question"]
        cypher = arguments["cypher"]
        previous_error = arguments.get("previous_error")
        previous_typeql = arguments.get("previous_typeql")

        # Load schemas
        typeql_schema = load_schema(database)
        if not typeql_schema:
            return [TextContent(type="text", text=json.dumps({
                "error": f"No TypeQL schema found for '{database}'"
            }))]

        neo4j_schema = get_schema(database)
        neo4j_schema_json = neo4j_schema.to_json_str()

        prompt_template = load_query_prompt()
        prompt = prompt_template.replace("{TYPEQL_SCHEMA}", typeql_schema)
        prompt = prompt.replace("{NEO4J_SCHEMA}", neo4j_schema_json)
        prompt = prompt.replace("{QUESTION}", question)
        prompt = prompt.replace("{CYPHER_QUERY}", cypher)

        if previous_error and previous_typeql:
            prompt += f"""

## Previous Attempt Failed
The previous TypeQL query was:
```typeql
{previous_typeql}
```

Error: {previous_error}

Please fix the issue and provide a corrected TypeQL query.
"""

        return [TextContent(type="text", text=prompt)]

    elif name == "convert_queries_batch":
        database = arguments["database"]
        queries = arguments["queries"]

        # Load schemas
        typeql_schema = load_schema(database)
        if not typeql_schema:
            return [TextContent(type="text", text=json.dumps({
                "error": f"No TypeQL schema found for '{database}'"
            }))]

        neo4j_schema = get_schema(database)
        neo4j_schema_json = neo4j_schema.to_json_str()

        prompt = f"""You are an expert at converting Cypher queries to TypeDB 3.x TypeQL queries.

## TypeQL Schema
```typeql
{typeql_schema}
```

## Neo4j Schema (for reference)
```json
{neo4j_schema_json}
```

## Task
Convert each of the following Cypher queries to valid TypeQL. Return your answers in JSON format as an array of objects with "index" and "typeql" fields.

## Queries to Convert
"""
        for q in queries:
            prompt += f"""
### Query {q['index']}
Question: {q['question']}
Cypher:
```cypher
{q['cypher']}
```
"""
            if q.get('error'):
                prompt += f"""
Previous failed attempt:
```typeql
{q.get('typeql', '')}
```
Error: {q['error'][:500]}
"""

        prompt += """
## Output Format
Return ONLY valid JSON array, no markdown. Example:
[
  {"index": 0, "typeql": "match $p isa person, has name $n; fetch { \\"name\\": $n };"},
  {"index": 1, "typeql": "match $m isa movie, has title $t; fetch { \\"title\\": $t };"}
]

## Important TypeQL Syntax Rules
1. Query order MUST be: match -> sort -> limit -> fetch
2. Do NOT use $var.* syntax - list attributes explicitly
3. Use double quotes for strings
4. For relations: (role1: $var1, role2: $var2) isa relation_name
5. Bind attributes to variables before using in sort: has attr $a; sort $a desc;
"""
        return [TextContent(type="text", text=prompt)]

    else:
        return [TextContent(type="text", text=f"Unknown tool: {name}")]


async def main():
    """Run the MCP server."""
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
