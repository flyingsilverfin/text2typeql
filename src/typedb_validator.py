"""TypeDB validation for schemas and queries."""

from dataclasses import dataclass
from contextlib import contextmanager

from typedb.driver import (
    TypeDB,
    Credentials,
    DriverOptions,
    TransactionType,
)

from .config import (
    TYPEDB_ADDRESS,
    TYPEDB_USERNAME,
    TYPEDB_PASSWORD,
    SCHEMA_VALIDATION_DB,
)


@dataclass
class ValidationResult:
    """Result of a validation attempt."""
    success: bool
    error_message: str | None = None


class TypeDBValidator:
    """Validates TypeQL schemas and queries against TypeDB."""

    def __init__(
        self,
        address: str = None,
        username: str = None,
        password: str = None
    ):
        self.address = address or TYPEDB_ADDRESS
        self.username = username or TYPEDB_USERNAME
        self.password = password or TYPEDB_PASSWORD
        self._driver = None

    def connect(self):
        """Establish connection to TypeDB."""
        if self._driver is None:
            credentials = Credentials(self.username, self.password)
            options = DriverOptions(is_tls_enabled=False)
            self._driver = TypeDB.driver(self.address, credentials, options)
        return self._driver

    def close(self):
        """Close the TypeDB connection."""
        if self._driver is not None:
            self._driver.close()
            self._driver = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _ensure_database(self, db_name: str, recreate: bool = False):
        """Ensure database exists, optionally recreating it."""
        driver = self.connect()

        # Check if database exists
        existing = [db.name for db in driver.databases.all()]

        if db_name in existing:
            if recreate:
                driver.databases.get(db_name).delete()
                driver.databases.create(db_name)
        else:
            driver.databases.create(db_name)

    def _delete_database(self, db_name: str):
        """Delete database if it exists."""
        driver = self.connect()
        existing = [db.name for db in driver.databases.all()]
        if db_name in existing:
            driver.databases.get(db_name).delete()

    def validate_schema(
        self,
        schema_tql: str,
        db_name: str = None
    ) -> ValidationResult:
        """
        Validate a TypeQL schema by executing it.

        Args:
            schema_tql: TypeQL schema definition
            db_name: Database name for validation (temporary)

        Returns:
            ValidationResult with success status and any error message
        """
        db_name = db_name or SCHEMA_VALIDATION_DB
        driver = self.connect()

        try:
            # Create fresh database
            self._ensure_database(db_name, recreate=True)

            # Execute schema in SCHEMA transaction
            with driver.transaction(db_name, TransactionType.SCHEMA) as tx:
                tx.query(schema_tql).resolve()
                tx.commit()

            return ValidationResult(success=True)

        except Exception as e:
            return ValidationResult(success=False, error_message=str(e))

        finally:
            # Clean up validation database
            self._delete_database(db_name)

    def validate_query(
        self,
        query_tql: str,
        schema_tql: str,
        db_name: str = None
    ) -> ValidationResult:
        """
        Validate a TypeQL query against a schema.

        Args:
            query_tql: TypeQL query to validate
            schema_tql: TypeQL schema the query runs against
            db_name: Database name for validation (temporary)

        Returns:
            ValidationResult with success status and any error message
        """
        db_name = db_name or SCHEMA_VALIDATION_DB
        driver = self.connect()

        try:
            # Create fresh database with schema
            self._ensure_database(db_name, recreate=True)

            # Apply schema
            with driver.transaction(db_name, TransactionType.SCHEMA) as tx:
                tx.query(schema_tql).resolve()
                tx.commit()

            # Validate query in READ transaction
            # Note: Query may not return results (no data), but should parse/compile
            with driver.transaction(db_name, TransactionType.READ) as tx:
                tx.query(query_tql).resolve()
                # Don't need to iterate results, just validate it compiles

            return ValidationResult(success=True)

        except Exception as e:
            return ValidationResult(success=False, error_message=str(e))

        finally:
            # Clean up validation database
            self._delete_database(db_name)

    def validate_schema_persistent(
        self,
        schema_tql: str,
        db_name: str
    ) -> ValidationResult:
        """
        Validate and persist a schema (don't delete database after).

        Args:
            schema_tql: TypeQL schema definition
            db_name: Database name to create/update

        Returns:
            ValidationResult with success status and any error message
        """
        driver = self.connect()

        try:
            # Create fresh database
            self._ensure_database(db_name, recreate=True)

            # Execute schema in SCHEMA transaction
            with driver.transaction(db_name, TransactionType.SCHEMA) as tx:
                tx.query(schema_tql).resolve()
                tx.commit()

            return ValidationResult(success=True)

        except Exception as e:
            # Clean up on failure
            self._delete_database(db_name)
            return ValidationResult(success=False, error_message=str(e))

    def validate_query_on_existing(
        self,
        query_tql: str,
        db_name: str
    ) -> ValidationResult:
        """
        Validate a query against an existing database.

        Args:
            query_tql: TypeQL query to validate
            db_name: Existing database name

        Returns:
            ValidationResult with success status and any error message
        """
        driver = self.connect()

        try:
            with driver.transaction(db_name, TransactionType.READ) as tx:
                tx.query(query_tql).resolve()

            return ValidationResult(success=True)

        except Exception as e:
            return ValidationResult(success=False, error_message=str(e))


@contextmanager
def get_validator():
    """Context manager for TypeDB validator."""
    validator = TypeDBValidator()
    try:
        validator.connect()
        yield validator
    finally:
        validator.close()
