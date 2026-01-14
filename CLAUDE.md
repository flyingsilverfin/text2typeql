# Text2TypeQL - Project Notes

## Project Overview

Pipeline to convert Neo4j text2cypher datasets to TypeQL format for training text-to-TypeQL models.

**Source**: https://github.com/neo4j-labs/text2cypher (datasets/synthetic_opus_demodbs/)

## Quick Start

```bash
# Activate virtual environment
source venv/bin/activate

# Set API key
export ANTHROPIC_API_KEY=your_key

# Start TypeDB server (must be running for validation)
typedb server --development-mode.enabled=true

# Run pipeline
python main.py setup                      # Clone Neo4j dataset
python main.py list-schemas               # List available schemas
python main.py convert-schema movies      # Convert schema
python main.py convert-queries movies --limit 10  # Convert queries
```

## TypeDB 3.x Reference

### Connection (Python Driver)

```python
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# Connect
credentials = Credentials(username, password)
options = DriverOptions(is_tls_enabled=False)
driver = TypeDB.driver("localhost:1729", credentials, options)

# Create database
driver.databases.create("my_database")

# Schema transaction
with driver.transaction("my_database", TransactionType.SCHEMA) as tx:
    tx.query("define ...").resolve()
    tx.commit()

# Read transaction
with driver.transaction("my_database", TransactionType.READ) as tx:
    result = tx.query("match ... fetch ...").resolve()
    for doc in result.as_concept_documents():
        print(doc)

# Write transaction
with driver.transaction("my_database", TransactionType.WRITE) as tx:
    tx.query("insert ...").resolve()
    tx.commit()

# Delete database
driver.databases.get("my_database").delete()
```

### TypeQL Schema Syntax

```typeql
define
  # Attributes (with value types: string, integer, double, boolean, datetime)
  attribute name value string;
  attribute age value integer;
  attribute score value double;
  attribute active value boolean;

  # Entity with attributes and roles
  entity person,
    owns name @key,        # @key = unique identifier
    owns age,
    plays friendship:friend,
    plays employment:employee;

  # Entity subtyping
  entity actor sub person,
    plays acted_in:actor;

  # Relation with roles
  relation friendship,
    relates friend;        # Both participants play same role

  relation employment,
    relates employee,
    relates employer;

  relation acted_in,
    relates actor,
    relates film,
    owns role_name;        # Relations can own attributes
```

### TypeQL Annotations

| Annotation | Purpose | Example |
|------------|---------|---------|
| `@key` | Unique identifier | `owns email @key` |
| `@unique` | Unique but optional | `owns ssn @unique` |
| `@card(min, max)` | Cardinality constraint | `owns phone @card(0, 3)` |
| `@abstract` | Abstract type | `entity person @abstract` |
| `@values(...)` | Allowed values | `@values("A", "B", "C")` |
| `@range(min, max)` | Value range | `@range(0, 100)` |

### TypeQL Query Syntax

```typeql
# Simple match + fetch
match $p isa person, has name "Alice", has age $a;
fetch { "name": "Alice", "age": $a };

# Fetch specific attributes
match $p isa person, has name $n, has age $a;
fetch { "name": $n, "age": $a };

# Relation traversal
match
  $p isa person, has name $n;
  (actor: $p, film: $m) isa acted_in;
  $m has title $t;
fetch { "actor": $n, "movie": $t };

# Filtering with comparison
match $m isa movie, has released $r, has title $t; $r > 2000;
fetch { "title": $t, "released": $r };

# Negation
match
  $p isa person, has name $n;
  not { (actor: $p) isa acted_in; };
fetch { "non_actor": $n };

# Aggregation
match $m isa movie;
reduce $count = count($m);

# Sorting and limiting
match $p isa person, has age $a, has name $n;
sort $a desc;
limit 10;
fetch { "name": $n, "age": $a };
```

### Cypher to TypeQL Pattern Mapping

| Cypher | TypeQL |
|--------|--------|
| `MATCH (n:Label)` | `match $n isa label` |
| `MATCH (n {prop: 'val'})` | `match $n isa type, has prop 'val'` |
| `MATCH (a)-[:REL]->(b)` | `match (role1: $a, role2: $b) isa rel` |
| `RETURN n.prop` | `fetch { "prop": $n.prop }` |
| `RETURN n` | `fetch { "attr1": $n.attr1, "attr2": $n.attr2 }` (list attributes explicitly) |
| `WHERE n.prop > 5` | `$n has prop $p; $p > 5;` |
| `COUNT(n)` | `reduce $count = count($n)` |
| `ORDER BY n.prop` | `sort $p asc;` |
| `LIMIT 10` | `limit 10;` |

### Reserved Keywords (avoid as type names)

`in`, `or`, `and`, `not`, `match`, `define`, `insert`, `delete`, `fetch`

If needed, rename: `in` -> `contained_in`, etc.

## Neo4j Dataset Format

**text2cypher_schemas.csv** - JSON per database:
```json
{
  "node_props": { "Movie": [{"property": "title", "type": "STRING"}] },
  "rel_props": { "ACTED_IN": [{"property": "roles", "type": "LIST"}] },
  "relationships": [{"start": "Person", "type": "ACTED_IN", "end": "Movie"}]
}
```

**text2cypher_claudeopus.csv** - Question/Cypher pairs:
- `question`: Natural language question
- `cypher`: Cypher query
- `database`: Target schema name
- `syntax_error`, `false_schema`: Quality flags (filter these out)

## Dependencies

```
anthropic>=0.40.0
typedb-driver>=3.0.0
click>=8.1.0
pandas>=2.0.0
```

## Current Status

**Phase**: Initial setup
**Next**: Clone Neo4j dataset, then implement core modules

See `plan.md` for full implementation plan.
