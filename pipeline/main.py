#!/usr/bin/env python3
"""CLI for text2typeql dataset generation."""

import subprocess
import sys
from pathlib import Path

import click

from src.config import (
    DATA_DIR,
    DATASET_DIR,
    DEFAULT_SOURCE,
    SOURCES,
    TEXT2CYPHER_DIR,
    ensure_dirs,
    get_dataset_dir,
)

SOURCE_OPTION = click.option(
    "--source", default=DEFAULT_SOURCE, show_default=True,
    type=click.Choice(list(SOURCES.keys())),
    help="Source dataset to use",
)


@click.group()
def cli():
    """Text2TypeQL - Convert Neo4j text2cypher dataset to TypeQL format."""
    pass


@cli.command()
def setup():
    """Clone the Neo4j text2cypher dataset."""
    ensure_dirs()

    repo_dir = DATA_DIR / "text2cypher"

    if repo_dir.exists():
        click.echo(f"Dataset already exists at {repo_dir}")
        click.echo("To re-clone, delete the directory first.")
        return

    click.echo("Cloning Neo4j text2cypher repository...")
    result = subprocess.run(
        ["git", "clone", "https://github.com/neo4j-labs/text2cypher.git", str(repo_dir)],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        click.echo(f"Error cloning repository: {result.stderr}", err=True)
        sys.exit(1)

    click.echo(f"Dataset cloned to {repo_dir}")

    # Verify the expected files exist
    if not TEXT2CYPHER_DIR.exists():
        click.echo(f"Warning: Expected directory not found: {TEXT2CYPHER_DIR}", err=True)
    else:
        click.echo(f"Dataset files available at {TEXT2CYPHER_DIR}")


@cli.command("list-schemas")
@SOURCE_OPTION
def list_schemas(source: str):
    """List available database schemas."""
    try:
        from src.neo4j_parser import list_databases, get_query_count

        databases = list_databases(source=source)

        click.echo(f"Available databases ({len(databases)}) for {source}:\n")

        for db in databases:
            stats = get_query_count(db, source=source)
            click.echo(f"  {db}")
            click.echo(f"    Queries: {stats['valid']} valid / {stats['total']} total")
            if stats['syntax_errors'] > 0:
                click.echo(f"    Skipped: {stats['syntax_errors']} syntax errors, {stats['excluded']} excluded")
            click.echo()

    except FileNotFoundError as e:
        click.echo(f"Error: {e}", err=True)
        click.echo("Run 'python main.py setup' first to clone the dataset.")
        sys.exit(1)


@cli.command("convert-schema")
@click.argument("database")
@click.option("--no-validate", is_flag=True, help="Skip TypeDB validation")
@click.option("--model", default=None, help="Claude model to use")
@SOURCE_OPTION
def convert_schema(database: str, no_validate: bool, model: str, source: str):
    """Convert a Neo4j schema to TypeQL."""
    from src.schema_converter import convert_and_save_schema
    from src.typedb_validator import TypeDBValidator

    click.echo(f"Converting schema for '{database}' (source: {source})...")

    validator = None
    if not no_validate:
        try:
            validator = TypeDBValidator()
            validator.connect()
            click.echo("Connected to TypeDB for validation")
        except Exception as e:
            click.echo(f"Warning: Could not connect to TypeDB: {e}", err=True)
            click.echo("Proceeding without validation...")
            validator = None

    try:
        success, schema, errors = convert_and_save_schema(
            database=database,
            validator=validator,
            model=model,
            source=source,
        )

        output_dir = get_dataset_dir(database, source)

        if success:
            click.echo(f"\nSchema converted successfully!")
            click.echo(f"Output: {output_dir}/schema.tql")
        else:
            click.echo(f"\nSchema conversion completed with errors:")
            for i, error in enumerate(errors, 1):
                click.echo(f"  Attempt {i}: {error}")
            click.echo(f"\nOutput (may have issues): {output_dir}/schema.tql")

        click.echo(f"\nNext steps:")
        click.echo(f"  1. Review the schema: cat {output_dir}/schema.tql")
        click.echo(f"  2. Approve it: python main.py approve-schema {database} --source {source}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    finally:
        if validator:
            validator.close()


@cli.command("approve-schema")
@click.argument("database")
@SOURCE_OPTION
def approve_schema_cmd(database: str, source: str):
    """Mark a schema as reviewed and approved."""
    from src.schema_converter import approve_schema, load_schema

    schema = load_schema(database, source=source)
    if not schema:
        click.echo(f"Error: No schema found for '{database}' (source: {source})", err=True)
        click.echo(f"Run 'python main.py convert-schema {database} --source {source}' first.")
        sys.exit(1)

    # Show the schema
    click.echo(f"Schema for '{database}' (source: {source}):\n")
    click.echo("=" * 60)
    click.echo(schema)
    click.echo("=" * 60)

    # Confirm approval
    if click.confirm("\nApprove this schema?"):
        if approve_schema(database, source=source):
            click.echo(f"Schema for '{database}' approved!")
        else:
            click.echo("Error approving schema", err=True)
            sys.exit(1)
    else:
        click.echo("Schema not approved.")


@cli.command("show-schema")
@click.argument("database")
@click.option("--neo4j", is_flag=True, help="Show original Neo4j schema instead")
@SOURCE_OPTION
def show_schema(database: str, neo4j: bool, source: str):
    """Display a converted schema."""
    output_dir = get_dataset_dir(database, source)

    if neo4j:
        path = output_dir / "neo4j_schema.json"
        if not path.exists():
            click.echo(f"No Neo4j schema found for '{database}' (source: {source})", err=True)
            sys.exit(1)
        click.echo(path.read_text())
    else:
        path = output_dir / "schema.tql"
        if not path.exists():
            click.echo(f"No TypeQL schema found for '{database}' (source: {source})", err=True)
            click.echo(f"Run 'python main.py convert-schema {database} --source {source}' first.")
            sys.exit(1)
        click.echo(path.read_text())


@cli.command("status")
@click.argument("database", required=False)
@SOURCE_OPTION
def status(database: str, source: str):
    """Show conversion status for a database or all databases."""
    import json

    source_dir = DATASET_DIR / source

    if database:
        # Show status for specific database
        output_dir = get_dataset_dir(database, source)
        status_path = output_dir / "status.json"

        if not status_path.exists():
            click.echo(f"No conversion data for '{database}' (source: {source})")
            return

        status_data = json.loads(status_path.read_text())
        click.echo(f"Status for '{database}' (source: {source}):")
        click.echo(f"  Schema: {'Approved' if status_data.get('approved') else 'Not approved'}")

        if "queries" in status_data:
            q = status_data["queries"]
            click.echo(f"  Queries: {q['successful']} successful, {q['failed']} failed")
        else:
            click.echo("  Queries: Not converted yet")

    else:
        # Show status for all databases in this source
        if not source_dir.exists():
            click.echo(f"No conversions yet for {source}.")
            return

        click.echo(f"Conversion status ({source}):\n")
        for db_dir in sorted(source_dir.iterdir()):
            if not db_dir.is_dir():
                continue

            status_path = db_dir / "status.json"
            if status_path.exists():
                status_data = json.loads(status_path.read_text())
                approved = "Yes" if status_data.get("approved") else "No"
                queries = status_data.get("queries", {})
                q_status = f"{queries.get('successful', 0)}/{queries.get('total_converted', 0)}" if queries else "-"
                click.echo(f"  {db_dir.name}: approved={approved}, queries={q_status}")
            else:
                click.echo(f"  {db_dir.name}: (no status)")


@cli.command("approve-all-schemas")
@SOURCE_OPTION
def approve_all_schemas(source: str):
    """Approve all converted schemas without prompting."""
    import json

    source_dir = DATASET_DIR / source

    if not source_dir.exists():
        click.echo(f"No schemas converted yet for {source}.")
        return

    approved_count = 0
    for db_dir in sorted(source_dir.iterdir()):
        if not db_dir.is_dir():
            continue

        schema_path = db_dir / "schema.tql"
        status_path = db_dir / "status.json"

        if schema_path.exists() and status_path.exists():
            status = json.loads(status_path.read_text())
            if not status.get("approved"):
                status["approved"] = True
                status_path.write_text(json.dumps(status, indent=2))
                click.echo(f"Approved: {db_dir.name}")
                approved_count += 1

    click.echo(f"\nApproved {approved_count} schemas")


if __name__ == "__main__":
    cli()
