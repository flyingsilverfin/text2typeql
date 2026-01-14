#!/usr/bin/env python3
"""CLI for text2typeql dataset generation."""

import subprocess
import sys
from pathlib import Path

import click

from src.config import (
    DATA_DIR,
    TEXT2CYPHER_DIR,
    ensure_dirs,
    get_output_dir,
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
def list_schemas():
    """List available database schemas."""
    try:
        from src.neo4j_parser import list_databases, get_query_count

        databases = list_databases()

        click.echo(f"Available databases ({len(databases)}):\n")

        for db in databases:
            stats = get_query_count(db)
            click.echo(f"  {db}")
            click.echo(f"    Queries: {stats['valid']} valid / {stats['total']} total")
            if stats['syntax_errors'] > 0:
                click.echo(f"    Skipped: {stats['syntax_errors']} syntax errors, {stats['false_schema']} false schema")
            click.echo()

    except FileNotFoundError as e:
        click.echo(f"Error: {e}", err=True)
        click.echo("Run 'python main.py setup' first to clone the dataset.")
        sys.exit(1)


@cli.command("convert-schema")
@click.argument("database")
@click.option("--no-validate", is_flag=True, help="Skip TypeDB validation")
@click.option("--model", default=None, help="Claude model to use")
def convert_schema(database: str, no_validate: bool, model: str):
    """Convert a Neo4j schema to TypeQL."""
    from src.schema_converter import convert_and_save_schema
    from src.typedb_validator import TypeDBValidator

    click.echo(f"Converting schema for '{database}'...")

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
            model=model
        )

        output_dir = get_output_dir(database)

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
        click.echo(f"  2. Approve it: python main.py approve-schema {database}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    finally:
        if validator:
            validator.close()


@cli.command("approve-schema")
@click.argument("database")
def approve_schema_cmd(database: str):
    """Mark a schema as reviewed and approved."""
    from src.schema_converter import approve_schema, load_schema

    schema = load_schema(database)
    if not schema:
        click.echo(f"Error: No schema found for '{database}'", err=True)
        click.echo(f"Run 'python main.py convert-schema {database}' first.")
        sys.exit(1)

    # Show the schema
    click.echo(f"Schema for '{database}':\n")
    click.echo("=" * 60)
    click.echo(schema)
    click.echo("=" * 60)

    # Confirm approval
    if click.confirm("\nApprove this schema?"):
        if approve_schema(database):
            click.echo(f"Schema for '{database}' approved!")
            click.echo(f"You can now convert queries: python main.py convert-queries {database}")
        else:
            click.echo("Error approving schema", err=True)
            sys.exit(1)
    else:
        click.echo("Schema not approved.")


@cli.command("convert-queries")
@click.argument("database")
@click.option("--limit", type=int, default=None, help="Maximum queries to convert")
@click.option("--no-validate", is_flag=True, help="Skip TypeDB validation")
@click.option("--model", default=None, help="Claude model to use")
@click.option("--skip-approval", is_flag=True, help="Skip schema approval check")
def convert_queries(
    database: str,
    limit: int,
    no_validate: bool,
    model: str,
    skip_approval: bool
):
    """Convert Cypher queries to TypeQL for a database."""
    from src.query_converter import convert_and_save_queries
    from src.typedb_validator import TypeDBValidator

    click.echo(f"Converting queries for '{database}'...")
    if limit:
        click.echo(f"Limiting to {limit} queries")

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
        success_count, fail_count, queries_path, failed_path = convert_and_save_queries(
            database=database,
            validator=validator,
            model=model,
            limit=limit,
            skip_approved_check=skip_approval
        )

        click.echo(f"\nConversion complete!")
        click.echo(f"  Successful: {success_count}")
        click.echo(f"  Failed: {fail_count}")
        click.echo(f"\nOutput files:")
        click.echo(f"  Queries: {queries_path}")
        if fail_count > 0:
            click.echo(f"  Failed: {failed_path}")

    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    finally:
        if validator:
            validator.close()


@cli.command("show-schema")
@click.argument("database")
@click.option("--neo4j", is_flag=True, help="Show original Neo4j schema instead")
def show_schema(database: str, neo4j: bool):
    """Display a converted schema."""
    output_dir = get_output_dir(database)

    if neo4j:
        path = output_dir / "neo4j_schema.json"
        if not path.exists():
            click.echo(f"No Neo4j schema found for '{database}'", err=True)
            sys.exit(1)
        click.echo(path.read_text())
    else:
        path = output_dir / "schema.tql"
        if not path.exists():
            click.echo(f"No TypeQL schema found for '{database}'", err=True)
            click.echo(f"Run 'python main.py convert-schema {database}' first.")
            sys.exit(1)
        click.echo(path.read_text())


@cli.command("status")
@click.argument("database", required=False)
def status(database: str):
    """Show conversion status for a database or all databases."""
    import json
    from src.config import OUTPUT_DIR

    if database:
        # Show status for specific database
        output_dir = get_output_dir(database)
        status_path = output_dir / "status.json"

        if not status_path.exists():
            click.echo(f"No conversion data for '{database}'")
            return

        status_data = json.loads(status_path.read_text())
        click.echo(f"Status for '{database}':")
        click.echo(f"  Schema: {'Approved' if status_data.get('approved') else 'Not approved'}")

        if "queries" in status_data:
            q = status_data["queries"]
            click.echo(f"  Queries: {q['successful']} successful, {q['failed']} failed")
        else:
            click.echo("  Queries: Not converted yet")

    else:
        # Show status for all databases
        if not OUTPUT_DIR.exists():
            click.echo("No conversions yet. Run 'python main.py convert-schema <database>' to start.")
            return

        click.echo("Conversion status:\n")
        for db_dir in sorted(OUTPUT_DIR.iterdir()):
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
def approve_all_schemas():
    """Approve all converted schemas without prompting."""
    import json
    from src.config import OUTPUT_DIR

    if not OUTPUT_DIR.exists():
        click.echo("No schemas converted yet.")
        return

    approved_count = 0
    for db_dir in sorted(OUTPUT_DIR.iterdir()):
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


@cli.command("batch-convert")
@click.argument("database")
@click.option("--batch-size", type=int, default=10, help="Number of queries per batch")
@click.option("--limit", type=int, default=None, help="Maximum queries to convert")
@click.option("--model", default=None, help="Claude model to use")
@click.option("--no-validate", is_flag=True, help="Skip TypeDB validation")
def batch_convert(
    database: str,
    batch_size: int,
    limit: int,
    model: str,
    no_validate: bool
):
    """Batch convert queries with crash recovery and progress tracking.

    Converts queries in batches to save API costs. Progress is saved after each
    batch, so conversion can be resumed if interrupted. Failed queries are
    written to failed.csv for later retry.
    """
    from src.batch_converter import run_batch_conversion

    click.echo(f"Batch converting queries for '{database}'...")
    click.echo(f"  Batch size: {batch_size}")
    if limit:
        click.echo(f"  Limit: {limit} queries")
    if no_validate:
        click.echo("  Validation: DISABLED")

    try:
        success_count, fail_count = run_batch_conversion(
            database=database,
            batch_size=batch_size,
            limit=limit,
            model=model,
            skip_validation=no_validate
        )

        output_dir = get_output_dir(database)
        click.echo(f"\nResults saved to: {output_dir}")
        click.echo(f"  queries.csv: {success_count} successful conversions")
        if fail_count > 0:
            click.echo(f"  failed.csv: {fail_count} failed conversions")
            click.echo(f"\nRun 'python main.py retry-failed {database}' to retry failed queries")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        click.echo("Progress has been saved. Re-run the command to resume.")
        sys.exit(1)


@cli.command("retry-failed")
@click.argument("database")
@click.option("--max-retries", type=int, default=3, help="Maximum retries per query")
@click.option("--model", default=None, help="Claude model to use")
@click.option("--no-validate", is_flag=True, help="Skip TypeDB validation")
def retry_failed(
    database: str,
    max_retries: int,
    model: str,
    no_validate: bool
):
    """Retry failed query conversions one by one.

    Processes queries from failed.csv with up to max_retries attempts each.
    Successfully converted queries are appended to queries.csv.
    Queries that still fail go to failed_retries.csv.
    """
    from src.batch_converter import run_retry_conversion

    click.echo(f"Retrying failed queries for '{database}'...")
    click.echo(f"  Max retries per query: {max_retries}")

    try:
        success_count, still_failed = run_retry_conversion(
            database=database,
            max_retries=max_retries,
            model=model,
            skip_validation=no_validate
        )

        output_dir = get_output_dir(database)
        if success_count > 0:
            click.echo(f"\n{success_count} queries successfully converted and added to queries.csv")
        if still_failed > 0:
            click.echo(f"{still_failed} queries still failed after {max_retries} retries")
            click.echo(f"See: {output_dir}/failed_retries.csv")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command("batch-retry")
@click.argument("database")
@click.option("--batch-size", type=int, default=10, help="Number of queries per batch")
@click.option("--max-rounds", type=int, default=3, help="Maximum retry rounds")
@click.option("--model", default=None, help="Claude model to use")
@click.option("--no-validate", is_flag=True, help="Skip TypeDB validation")
@click.option("--source", type=click.Choice(['failed', 'failed_retries']), default=None,
              help="Source file (auto-detected if not specified)")
def batch_retry(
    database: str,
    batch_size: int,
    max_rounds: int,
    model: str,
    no_validate: bool,
    source: str
):
    """Batch retry failed queries for cost efficiency.

    Processes failed queries in batches, sending multiple queries per API call.
    Each round processes all remaining failures, up to max_rounds total.
    Much more cost-efficient than retry-failed for large numbers of failures.
    """
    from src.batch_converter import run_batch_retry

    click.echo(f"Batch retrying failed queries for '{database}'...")

    try:
        success_count, still_failed = run_batch_retry(
            database=database,
            batch_size=batch_size,
            max_rounds=max_rounds,
            model=model,
            skip_validation=no_validate,
            source_file=source
        )

        output_dir = get_output_dir(database)
        if success_count > 0:
            click.echo(f"\n{success_count} queries successfully converted and added to queries.csv")
        if still_failed > 0:
            click.echo(f"{still_failed} queries still failed after {max_rounds} rounds")
            click.echo(f"See: {output_dir}/failed_retries.csv")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command("convert-all")
@click.option("--batch-size", type=int, default=10, help="Number of queries per batch")
@click.option("--limit", type=int, default=None, help="Maximum queries per database")
@click.option("--model", default=None, help="Claude model to use")
@click.option("--no-validate", is_flag=True, help="Skip TypeDB validation")
@click.option("--retry/--no-retry", default=True, help="Retry failed queries after initial pass")
def convert_all(
    batch_size: int,
    limit: int,
    model: str,
    no_validate: bool,
    retry: bool
):
    """Convert queries for all approved schemas.

    Runs batch conversion for all databases with approved schemas.
    If --retry is enabled (default), also retries failed queries.
    """
    import json
    from src.config import OUTPUT_DIR
    from src.batch_converter import run_batch_conversion, run_retry_conversion

    if not OUTPUT_DIR.exists():
        click.echo("No schemas converted yet.")
        return

    # Find all approved databases
    approved_dbs = []
    for db_dir in sorted(OUTPUT_DIR.iterdir()):
        if not db_dir.is_dir():
            continue

        status_path = db_dir / "status.json"
        if status_path.exists():
            status = json.loads(status_path.read_text())
            if status.get("approved"):
                approved_dbs.append(db_dir.name)

    if not approved_dbs:
        click.echo("No approved schemas found. Run 'python main.py approve-all-schemas' first.")
        return

    click.echo(f"Converting queries for {len(approved_dbs)} databases: {', '.join(approved_dbs)}")

    total_success = 0
    total_failed = 0

    for db in approved_dbs:
        click.echo(f"\n{'=' * 60}")
        click.echo(f"Processing: {db}")
        click.echo('=' * 60)

        try:
            success, failed = run_batch_conversion(
                database=db,
                batch_size=batch_size,
                limit=limit,
                model=model,
                skip_validation=no_validate
            )
            total_success += success
            total_failed += failed

            if retry and failed > 0:
                click.echo(f"\nRetrying {failed} failed queries...")
                retry_success, retry_failed = run_retry_conversion(
                    database=db,
                    max_retries=3,
                    model=model,
                    skip_validation=no_validate
                )
                total_success += retry_success
                total_failed = total_failed - failed + retry_failed

        except Exception as e:
            click.echo(f"Error processing {db}: {e}", err=True)
            click.echo("Continuing with next database...")

    click.echo(f"\n{'=' * 60}")
    click.echo("SUMMARY")
    click.echo('=' * 60)
    click.echo(f"Total successful: {total_success}")
    click.echo(f"Total failed: {total_failed}")


if __name__ == "__main__":
    cli()
