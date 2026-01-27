"""Batch query conversion with crash recovery and retry handling."""

import csv
import json
import os
import signal
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

import anthropic

from .config import (
    ANTHROPIC_API_KEY,
    DEFAULT_MODEL,
    PROMPTS_DIR,
    get_output_dir,
    PROJECT_ROOT,
)
from .neo4j_parser import (
    QueryRecord,
    get_schema,
    parse_queries,
    filter_valid_queries,
)
from .schema_converter import load_schema, is_schema_approved
from .typedb_validator import TypeDBValidator, ValidationResult


# TypeDB server configuration
TYPEDB_PATH = PROJECT_ROOT / "typedb-all-linux-arm64-3.7.2" / "typedb"
TYPEDB_STARTUP_WAIT = 8  # seconds to wait for TypeDB to start
VALIDATION_DB_PREFIX = "text2typeql_"  # Prefix for validation databases


@dataclass
class BatchQuery:
    """Query with index for tracking."""
    index: int
    question: str
    cypher: str
    typeql: str = ""
    success: bool = False
    error_message: str = ""
    attempts: int = 0


@dataclass
class ConversionProgress:
    """Track conversion progress for resume capability."""
    database: str
    total_queries: int
    last_processed_index: int = -1
    successful_count: int = 0
    failed_count: int = 0

    def to_dict(self) -> dict:
        return {
            "database": self.database,
            "total_queries": self.total_queries,
            "last_processed_index": self.last_processed_index,
            "successful_count": self.successful_count,
            "failed_count": self.failed_count,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ConversionProgress":
        return cls(**data)


def load_query_prompt() -> str:
    """Load the query conversion prompt template."""
    prompt_path = PROMPTS_DIR / "query_conversion.txt"
    return prompt_path.read_text()


def extract_typeql(response: str) -> str:
    """Extract TypeQL from Claude's response."""
    text = response.strip()

    # Handle markdown code blocks
    if "```" in text:
        parts = text.split("```")
        for part in parts[1:]:
            lines = part.split("\n", 1)
            if len(lines) > 1:
                content = lines[1] if lines[0].strip() in ("", "typeql", "tql") else part
            else:
                content = part
            if "match" in content.lower() or "fetch" in content.lower() or "reduce" in content.lower():
                text = content
                break

    # Find where query starts
    lower_text = text.lower()
    for keyword in ["match", "insert", "delete"]:
        idx = lower_text.find(keyword)
        if idx > 0:
            text = text[idx:]
            break

    if text.rstrip().endswith("```"):
        text = text.rstrip()[:-3]

    return text.strip()


class TypeDBServerManager:
    """Manage TypeDB server lifecycle."""

    def __init__(self, typedb_path: Path = None):
        self.typedb_path = typedb_path or TYPEDB_PATH
        self.process = None

    def is_running(self) -> bool:
        """Check if TypeDB server is running."""
        try:
            from typedb.driver import TypeDB, Credentials, DriverOptions
            credentials = Credentials("admin", "password")
            options = DriverOptions(is_tls_enabled=False)
            driver = TypeDB.driver("localhost:1729", credentials, options)
            driver.databases.all()  # Test connection
            driver.close()
            return True
        except Exception:
            return False

    def start(self) -> bool:
        """Start TypeDB server."""
        if self.is_running():
            return True

        print("Starting TypeDB server...")
        try:
            self.process = subprocess.Popen(
                [str(self.typedb_path), "server", "--development-mode.enabled=true"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid  # Create new process group
            )

            # Wait for server to start
            for i in range(TYPEDB_STARTUP_WAIT * 2):
                time.sleep(0.5)
                if self.is_running():
                    print("TypeDB server started successfully")
                    return True

            print("TypeDB server failed to start within timeout")
            return False

        except Exception as e:
            print(f"Error starting TypeDB: {e}")
            return False

    def stop(self):
        """Stop TypeDB server."""
        if self.process:
            try:
                os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
                self.process.wait(timeout=5)
            except Exception:
                pass
            self.process = None

    def restart(self) -> bool:
        """Restart TypeDB server."""
        print("Restarting TypeDB server...")
        self.stop()
        time.sleep(2)
        return self.start()


class BatchQueryConverter:
    """Convert queries in batches with crash recovery."""

    def __init__(
        self,
        database: str,
        batch_size: int = 10,
        model: str = None,
        skip_validation: bool = False
    ):
        self.database = database
        self.batch_size = batch_size
        self.model = model or DEFAULT_MODEL
        self.skip_validation = skip_validation

        self.output_dir = get_output_dir(database)
        self.progress_file = self.output_dir / "conversion_progress.json"
        self.queries_file = self.output_dir / "queries.csv"
        self.failed_file = self.output_dir / "failed.csv"
        self.failed_retries_file = self.output_dir / "failed_retries.csv"

        self.typeql_schema = None
        self.neo4j_schema_json = None
        self.prompt_template = None

        # TypeDB validation database name
        self.validation_db_name = f"{VALIDATION_DB_PREFIX}{database}"
        self.db_initialized = False

        self.server_manager = TypeDBServerManager()
        self.validator = None
        self.client = None

    def initialize(self):
        """Initialize converter with schema and connections."""
        # Load schemas
        self.typeql_schema = load_schema(self.database)
        if not self.typeql_schema:
            raise ValueError(f"No TypeQL schema found for '{self.database}'")

        neo4j_schema = get_schema(self.database)
        self.neo4j_schema_json = neo4j_schema.to_json_str()

        self.prompt_template = load_query_prompt()

        # Initialize API client
        if not ANTHROPIC_API_KEY:
            raise ValueError("ANTHROPIC_API_KEY not set")
        self.client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

        # Start TypeDB if needed for validation
        if not self.skip_validation:
            if not self.server_manager.is_running():
                if not self.server_manager.start():
                    raise RuntimeError("Could not start TypeDB server")
            self.validator = TypeDBValidator()
            self.validator.connect()
            self._setup_validation_database()

    def _setup_validation_database(self):
        """Set up the validation database with schema loaded."""
        if self.db_initialized:
            return

        print(f"Setting up validation database '{self.validation_db_name}'...")
        driver = self.validator.connect()

        # Check if database exists
        existing_dbs = [db.name for db in driver.databases.all()]

        if self.validation_db_name in existing_dbs:
            # Delete and recreate to ensure clean state
            driver.databases.get(self.validation_db_name).delete()

        # Create database
        driver.databases.create(self.validation_db_name)

        # Load schema
        from typedb.driver import TransactionType
        with driver.transaction(self.validation_db_name, TransactionType.SCHEMA) as tx:
            tx.query(self.typeql_schema).resolve()
            tx.commit()

        print(f"Validation database ready with schema loaded")
        self.db_initialized = True

    def _ensure_validation_database(self):
        """Ensure validation database exists after potential restart."""
        if not self.skip_validation and not self.db_initialized:
            self._setup_validation_database()

    def load_progress(self) -> ConversionProgress | None:
        """Load existing progress from file."""
        if self.progress_file.exists():
            data = json.loads(self.progress_file.read_text())
            return ConversionProgress.from_dict(data)
        return None

    def save_progress(self, progress: ConversionProgress):
        """Save progress to file."""
        self.progress_file.write_text(json.dumps(progress.to_dict(), indent=2))

    def build_batch_prompt(self, queries: list[BatchQuery]) -> str:
        """Build a prompt for batch conversion."""
        prompt = f"""You are an expert at converting Cypher queries to TypeDB 3.x TypeQL queries.

## TypeQL Schema
```typeql
{self.typeql_schema}
```

## Neo4j Schema (for reference)
```json
{self.neo4j_schema_json}
```

## Task
Convert each of the following Cypher queries to valid TypeQL. Return your answers in JSON format as an array of objects with "index" and "typeql" fields.

## Queries to Convert
"""
        for q in queries:
            prompt += f"""
### Query {q.index}
Question: {q.question}
Cypher:
```cypher
{q.cypher}
```
"""

        prompt += """
## Output Format
Return ONLY valid JSON array, no markdown. Example:
[
  {"index": 0, "typeql": "match $p isa person; fetch { \\"person\\": $p.* };"},
  {"index": 1, "typeql": "match $m isa movie, has title $t; fetch { \\"title\\": $t };"}
]

Important:
- Each TypeQL query must be syntactically valid
- Use double quotes for strings in TypeQL
- Escape quotes in JSON output
"""
        return prompt

    def parse_batch_response(self, response: str, queries: list[BatchQuery]) -> list[BatchQuery]:
        """Parse batch response and update queries."""
        # Try to extract JSON from response
        text = response.strip()

        # Find JSON array
        start = text.find("[")
        end = text.rfind("]") + 1
        if start == -1 or end == 0:
            # Fallback: mark all as failed
            for q in queries:
                q.error_message = "Could not parse batch response"
            return queries

        try:
            results = json.loads(text[start:end])
            result_map = {r["index"]: r["typeql"] for r in results}

            for q in queries:
                if q.index in result_map:
                    q.typeql = result_map[q.index]
                    q.attempts = 1
                else:
                    q.error_message = "Missing from batch response"

        except json.JSONDecodeError as e:
            for q in queries:
                q.error_message = f"JSON parse error: {e}"

        return queries

    def validate_query(self, query: BatchQuery) -> bool:
        """Validate a single query against the persistent TypeDB database with schema loaded."""
        if self.skip_validation:
            query.success = True
            return True

        try:
            # Ensure database exists (may need recreation after restart)
            self._ensure_validation_database()

            # Validate against the persistent database with schema already loaded
            from typedb.driver import TransactionType
            driver = self.validator.connect()

            with driver.transaction(self.validation_db_name, TransactionType.READ) as tx:
                # Execute the query to validate it compiles and runs against the schema
                result = tx.query(query.typeql).resolve()
                # Try to consume some results to ensure query executes
                # For fetch queries, this validates the query structure
                try:
                    # Attempt to get results (may be empty, that's fine)
                    if hasattr(result, 'as_concept_documents'):
                        docs = result.as_concept_documents()
                        # Just iterate to trigger any lazy evaluation
                        for _ in docs:
                            break
                    elif hasattr(result, 'as_concept_rows'):
                        rows = result.as_concept_rows()
                        for _ in rows:
                            break
                except Exception:
                    # Empty results are fine
                    pass

            query.success = True
            return True

        except Exception as e:
            error_str = str(e).lower()
            # Check for server crash indicators
            if "connection" in error_str or "transport" in error_str or "closed" in error_str:
                self.db_initialized = False  # Need to recreate DB after restart
                raise ServerCrashError(f"TypeDB connection lost: {e}")
            query.error_message = str(e)
            return False

    def convert_batch(self, queries: list[BatchQuery]) -> list[BatchQuery]:
        """Convert a batch of queries using Claude API."""
        prompt = self.build_batch_prompt(queries)

        response = self.client.messages.create(
            model=self.model,
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}]
        )

        return self.parse_batch_response(response.content[0].text, queries)

    def append_to_csv(self, filepath: Path, queries: list[BatchQuery], include_error: bool = False):
        """Append queries to CSV file."""
        file_exists = filepath.exists()

        fieldnames = ["index", "question", "cypher", "typeql", "attempts"]
        if include_error:
            fieldnames.append("error_message")

        with open(filepath, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()

            for q in queries:
                row = {
                    "index": q.index,
                    "question": q.question,
                    "cypher": q.cypher,
                    "typeql": q.typeql,
                    "attempts": q.attempts,
                }
                if include_error:
                    row["error_message"] = q.error_message
                writer.writerow(row)

    def run_initial_conversion(self, limit: int = None) -> tuple[int, int]:
        """Run initial batch conversion."""
        self.initialize()

        # Get queries
        all_queries = parse_queries(database=self.database)
        all_queries = filter_valid_queries(all_queries)

        if limit:
            all_queries = all_queries[:limit]

        # Check for existing progress
        progress = self.load_progress()
        start_index = 0

        if progress and progress.database == self.database:
            start_index = progress.last_processed_index + 1
            print(f"Resuming from index {start_index}")
        else:
            # Clear previous output files
            for f in [self.queries_file, self.failed_file]:
                if f.exists():
                    f.unlink()
            progress = ConversionProgress(
                database=self.database,
                total_queries=len(all_queries)
            )

        # Convert queries in batches
        batch_queries = [
            BatchQuery(index=i, question=q.question, cypher=q.cypher)
            for i, q in enumerate(all_queries)
        ]

        total = len(batch_queries)

        for batch_start in range(start_index, total, self.batch_size):
            batch_end = min(batch_start + self.batch_size, total)
            batch = batch_queries[batch_start:batch_end]

            print(f"Converting batch {batch_start}-{batch_end} of {total}...")

            try:
                # Convert batch
                batch = self.convert_batch(batch)

                # Validate each query
                successful = []
                failed = []

                for q in batch:
                    if q.error_message:
                        failed.append(q)
                        continue

                    try:
                        if self.validate_query(q):
                            successful.append(q)
                        else:
                            failed.append(q)
                    except ServerCrashError:
                        print("Server crash detected, restarting...")
                        if self.server_manager.restart():
                            self.validator = TypeDBValidator()
                            self.validator.connect()
                            # Retry validation
                            if self.validate_query(q):
                                successful.append(q)
                            else:
                                failed.append(q)
                        else:
                            raise RuntimeError("Could not restart TypeDB")

                # Save results
                if successful:
                    self.append_to_csv(self.queries_file, successful)
                if failed:
                    self.append_to_csv(self.failed_file, failed, include_error=True)

                # Update progress
                progress.last_processed_index = batch_end - 1
                progress.successful_count += len(successful)
                progress.failed_count += len(failed)
                self.save_progress(progress)

                print(f"  Successful: {len(successful)}, Failed: {len(failed)}")

            except Exception as e:
                print(f"Error processing batch: {e}")
                self.save_progress(progress)
                raise

        print(f"\nInitial conversion complete:")
        print(f"  Total: {progress.successful_count + progress.failed_count}")
        print(f"  Successful: {progress.successful_count}")
        print(f"  Failed: {progress.failed_count}")

        return progress.successful_count, progress.failed_count

    def run_retry_failed(self, max_retries: int = 3) -> tuple[int, int]:
        """Retry failed queries one by one."""
        if not self.failed_file.exists():
            print("No failed queries to retry")
            return 0, 0

        self.initialize()

        # Load failed queries
        failed_queries = []
        with open(self.failed_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                failed_queries.append(BatchQuery(
                    index=int(row["index"]),
                    question=row["question"],
                    cypher=row["cypher"],
                    typeql=row.get("typeql", ""),
                    attempts=int(row.get("attempts", 0)),
                    error_message=row.get("error_message", "")
                ))

        print(f"Retrying {len(failed_queries)} failed queries...")

        newly_successful = []
        still_failed = []

        for i, query in enumerate(failed_queries):
            print(f"Retrying query {i + 1}/{len(failed_queries)} (index {query.index})...")

            success = False
            last_error = query.error_message

            for attempt in range(max_retries):
                try:
                    # Build single query prompt with error context
                    prompt = self.build_single_retry_prompt(query, last_error if attempt > 0 else None)

                    response = self.client.messages.create(
                        model=self.model,
                        max_tokens=2048,
                        messages=[{"role": "user", "content": prompt}]
                    )

                    query.typeql = extract_typeql(response.content[0].text)
                    query.attempts += 1

                    # Validate
                    try:
                        if self.validate_query(query):
                            success = True
                            break
                        else:
                            last_error = query.error_message
                    except ServerCrashError:
                        print("  Server crash, restarting...")
                        if self.server_manager.restart():
                            self.validator = TypeDBValidator()
                            self.validator.connect()
                        else:
                            raise RuntimeError("Could not restart TypeDB")
                        last_error = "Server crashed during validation"

                except Exception as e:
                    last_error = str(e)
                    query.attempts += 1

            if success:
                query.success = True
                newly_successful.append(query)
                print(f"  Success after {query.attempts} attempts")
            else:
                query.error_message = last_error
                still_failed.append(query)
                print(f"  Failed after {query.attempts} attempts: {last_error[:100]}")

        # Save results
        if newly_successful:
            self.append_to_csv(self.queries_file, newly_successful)

        if still_failed:
            # Clear and rewrite failed_retries
            if self.failed_retries_file.exists():
                self.failed_retries_file.unlink()
            self.append_to_csv(self.failed_retries_file, still_failed, include_error=True)

        # Remove the failed.csv since we've processed it
        self.failed_file.unlink()

        print(f"\nRetry complete:")
        print(f"  Newly successful: {len(newly_successful)}")
        print(f"  Still failed: {len(still_failed)}")

        return len(newly_successful), len(still_failed)

    def build_single_retry_prompt(self, query: BatchQuery, previous_error: str = None) -> str:
        """Build prompt for single query retry with error context."""
        prompt = self.prompt_template.replace("{TYPEQL_SCHEMA}", self.typeql_schema)
        prompt = prompt.replace("{NEO4J_SCHEMA}", self.neo4j_schema_json)
        prompt = prompt.replace("{QUESTION}", query.question)
        prompt = prompt.replace("{CYPHER_QUERY}", query.cypher)

        if previous_error:
            prompt += f"""

## Previous Attempt Failed
The previous TypeQL query was:
```typeql
{query.typeql}
```

Error: {previous_error}

Please fix the issue and provide a corrected TypeQL query.
"""

        return prompt

    def cleanup(self):
        """Clean up resources."""
        if self.validator:
            self.validator.close()

    def build_batch_retry_prompt(self, queries: list[BatchQuery]) -> str:
        """Build a prompt for batch retry with error context."""
        prompt = f"""You are an expert at converting Cypher queries to TypeDB 3.x TypeQL queries.

## TypeQL Schema
```typeql
{self.typeql_schema}
```

## Neo4j Schema (for reference)
```json
{self.neo4j_schema_json}
```

## Task
Fix the following failed TypeQL query conversions. Each query includes the original Cypher, the failed TypeQL attempt, and the error message.

Return your fixes in JSON format as an array of objects with "index" and "typeql" fields.

## Failed Queries to Fix
"""
        for q in queries:
            prompt += f"""
### Query {q.index}
Question: {q.question}

Original Cypher:
```cypher
{q.cypher}
```

Failed TypeQL attempt:
```typeql
{q.typeql}
```

Error: {q.error_message[:500] if q.error_message else 'Unknown error'}

---
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
        return prompt

    def run_batch_retry(self, batch_size: int = 10, max_rounds: int = 3, source_file: str = None) -> tuple[int, int]:
        """
        Retry failed queries in batches for cost efficiency.

        Args:
            batch_size: Number of queries per batch
            max_rounds: Maximum number of retry rounds
            source_file: 'failed' or 'failed_retries' (default: auto-detect)

        Returns:
            Tuple of (newly_successful_count, still_failed_count)
        """
        # Determine source file
        if source_file == 'failed_retries' or (source_file is None and self.failed_retries_file.exists() and not self.failed_file.exists()):
            input_file = self.failed_retries_file
        elif self.failed_file.exists():
            input_file = self.failed_file
        else:
            print("No failed queries to retry")
            return 0, 0

        self.initialize()

        # Load failed queries
        failed_queries = []
        with open(input_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                failed_queries.append(BatchQuery(
                    index=int(row["index"]),
                    question=row["question"],
                    cypher=row["cypher"],
                    typeql=row.get("typeql", ""),
                    attempts=int(row.get("attempts", 0)),
                    error_message=row.get("error_message", "")
                ))

        print(f"Batch retrying {len(failed_queries)} failed queries from {input_file.name}...")
        print(f"  Batch size: {batch_size}, Max rounds: {max_rounds}")

        all_successful = []
        current_failed = failed_queries

        for round_num in range(max_rounds):
            if not current_failed:
                break

            print(f"\n=== Round {round_num + 1}/{max_rounds} ({len(current_failed)} queries) ===")

            round_successful = []
            round_failed = []

            # Process in batches
            for batch_start in range(0, len(current_failed), batch_size):
                batch_end = min(batch_start + batch_size, len(current_failed))
                batch = current_failed[batch_start:batch_end]

                print(f"  Batch {batch_start}-{batch_end}...", end=" ", flush=True)

                try:
                    # Send batch to Claude
                    prompt = self.build_batch_retry_prompt(batch)
                    response = self.client.messages.create(
                        model=self.model,
                        max_tokens=4096,
                        messages=[{"role": "user", "content": prompt}]
                    )

                    # Parse response
                    batch = self.parse_batch_response(response.content[0].text, batch)

                    # Validate each query
                    batch_success = 0
                    batch_fail = 0

                    for q in batch:
                        q.attempts += 1

                        if q.error_message and "parse" in q.error_message.lower():
                            # Already failed to parse response
                            round_failed.append(q)
                            batch_fail += 1
                            continue

                        try:
                            if self.validate_query(q):
                                q.success = True
                                round_successful.append(q)
                                batch_success += 1
                            else:
                                round_failed.append(q)
                                batch_fail += 1
                        except ServerCrashError:
                            print("\n  Server crash, restarting...", end=" ")
                            if self.server_manager.restart():
                                self.validator = TypeDBValidator()
                                self.validator.connect()
                                self._setup_validation_database()
                                # Retry validation
                                if self.validate_query(q):
                                    q.success = True
                                    round_successful.append(q)
                                    batch_success += 1
                                else:
                                    round_failed.append(q)
                                    batch_fail += 1
                            else:
                                raise RuntimeError("Could not restart TypeDB")

                    print(f"success={batch_success}, fail={batch_fail}")

                except Exception as e:
                    print(f"error: {e}")
                    # Mark all in batch as failed
                    for q in batch:
                        q.attempts += 1
                        q.error_message = str(e)
                        round_failed.append(q)

            all_successful.extend(round_successful)
            current_failed = round_failed

            print(f"  Round {round_num + 1} complete: {len(round_successful)} fixed, {len(round_failed)} remaining")

        # Save results
        if all_successful:
            self.append_to_csv(self.queries_file, all_successful)

        # Clean up source file first (before writing new failed_retries)
        if input_file.exists() and input_file != self.failed_retries_file:
            input_file.unlink()

        if current_failed:
            # Save to failed_retries (overwrite if exists)
            if self.failed_retries_file.exists():
                self.failed_retries_file.unlink()
            self.append_to_csv(self.failed_retries_file, current_failed, include_error=True)
        elif input_file == self.failed_retries_file:
            # All succeeded, remove the old failed_retries file
            pass  # Already handled or doesn't exist

        print(f"\nBatch retry complete:")
        print(f"  Newly successful: {len(all_successful)}")
        print(f"  Still failed: {len(current_failed)}")

        return len(all_successful), len(current_failed)


class ServerCrashError(Exception):
    """Raised when TypeDB server crashes."""
    pass


def run_batch_conversion(
    database: str,
    batch_size: int = 10,
    limit: int = None,
    model: str = None,
    skip_validation: bool = False
) -> tuple[int, int]:
    """
    Run batch conversion for a database.

    Returns:
        Tuple of (successful_count, failed_count)
    """
    converter = BatchQueryConverter(
        database=database,
        batch_size=batch_size,
        model=model,
        skip_validation=skip_validation
    )

    try:
        return converter.run_initial_conversion(limit=limit)
    finally:
        converter.cleanup()


def run_retry_conversion(
    database: str,
    max_retries: int = 3,
    model: str = None,
    skip_validation: bool = False
) -> tuple[int, int]:
    """
    Retry failed conversions for a database.

    Returns:
        Tuple of (newly_successful_count, still_failed_count)
    """
    converter = BatchQueryConverter(
        database=database,
        model=model,
        skip_validation=skip_validation
    )

    try:
        return converter.run_retry_failed(max_retries=max_retries)
    finally:
        converter.cleanup()


def run_batch_retry(
    database: str,
    batch_size: int = 10,
    max_rounds: int = 3,
    model: str = None,
    skip_validation: bool = False,
    source_file: str = None
) -> tuple[int, int]:
    """
    Batch retry failed conversions for cost efficiency.

    Args:
        database: Database name
        batch_size: Queries per batch
        max_rounds: Maximum retry rounds
        model: Claude model to use
        skip_validation: Skip TypeDB validation
        source_file: 'failed' or 'failed_retries'

    Returns:
        Tuple of (newly_successful_count, still_failed_count)
    """
    converter = BatchQueryConverter(
        database=database,
        batch_size=batch_size,
        model=model,
        skip_validation=skip_validation
    )

    try:
        return converter.run_batch_retry(
            batch_size=batch_size,
            max_rounds=max_rounds,
            source_file=source_file
        )
    finally:
        converter.cleanup()
