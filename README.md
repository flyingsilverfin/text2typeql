# Text2TypeQL

4,728 natural-language questions paired with validated TypeQL 3.0 queries across 7 domains.

## Overview

Foundation models generate SQL and Cypher reasonably well thanks to large public corpora and benchmark datasets. TypeQL 3.0, released with TypeDB 3.0, has none of that. Very little TypeQL exists in the wild, and even frontier models struggle to produce correct queries without heavy prompting. This dataset addresses that gap.

**Text2TypeQL** provides supervised training data for fine-tuning models on TypeQL generation, a retrieval corpus for few-shot prompting and RAG, and a standardized evaluation benchmark for TypeQL generation quality. Each entry includes the English question, the original Cypher query (from the source dataset), and a validated TypeQL 3.0 query. Seven TypeQL schemas model the domains.

The dataset was produced by converting Neo4j Labs' [text2cypher](https://github.com/neo4j-labs/text2cypher) benchmark using AI agents operating under a detailed TypeQL 3.0 reference, with every query validated against a live TypeDB instance and semantically reviewed to verify it correctly answers the English question.

## Domains

| Domain | Queries | Description |
|--------|---------|-------------|
| [twitter](dataset/twitter/) | 491 | Users, tweets, hashtags, retweets, follows |
| [twitch](dataset/twitch/) | 553 | Streamers, games, teams, subscriptions |
| [movies](dataset/movies/) | 723 | Actors, directors, producers, reviews, roles |
| [neoflix](dataset/neoflix/) | 910 | Movies, ratings, genres, subscriptions |
| [recommendations](dataset/recommendations/) | 741 | Users, movies, genres, ratings, actors |
| [companies](dataset/companies/) | 929 | Organizations, subsidiaries, CEOs, articles |
| [gameofthrones](dataset/gameofthrones/) | 381 | Characters, houses, battles, interactions |
| **Total** | **4,728** | + 48 documented failures |

## Data Format

### Merged dataset

`dataset/all_queries.csv` contains all queries in one file:

| Column | Description |
|--------|-------------|
| `domain` | Database domain name |
| `original_index` | Index in the source dataset |
| `question` | Natural-language question |
| `cypher` | Original Cypher query |
| `typeql` | Validated TypeQL 3.0 query |

### Per-domain files

Each `dataset/<domain>/` directory contains:

- `schema.tql` -- TypeQL schema definition
- `queries.csv` -- Query pairs (`original_index`, `question`, `cypher`, `typeql`)
- `neo4j_schema.json` -- Original Neo4j schema
- `README.md` -- Domain stats, failed queries with reasons, and Cypher errors found

## Usage

```python
import pandas as pd

# Load all queries
df = pd.read_csv("dataset/all_queries.csv")

# Filter by domain
twitter = df[df["domain"] == "twitter"]

# Load a single domain
movies = pd.read_csv("dataset/movies/queries.csv")
```

## What the Type System Caught

TypeDB's strict type system exposed roughly 30 queries across four databases where the original Cypher does not correctly answer the English question. Three patterns recurred:

- **Wrong property**: Twitter queries checking `favorites` when the question asks about retweets. TypeQL's explicit `retweets` relation forces correct semantics.
- **Wrong direction**: Companies queries reversing supplier/customer direction. TypeQL's `supplies (supplier: $x, customer: $y)` makes the reversal visible.
- **Wrong traversal**: Twitter queries returning tweets by the user instead of tweets by followers. TypeQL's role-based syntax eliminates the ambiguity.

In each case the TypeQL was written to correctly answer the English question. Details are in each domain's README.

## Failed Queries

48 of 4,776 source queries (1%) cannot be expressed in TypeQL 3.0. They require features not yet supported: `size()` for string/list length, array indexing, epoch timestamp conversion, duration arithmetic, date component extraction, and `collect()` aggregation. Each is documented with its original Cypher and the specific missing capability in the per-domain READMEs.

## Source

Derived from Neo4j Labs' [text2cypher](https://github.com/neo4j-labs/text2cypher) benchmark (`datasets/synthetic_opus_demodbs/`). Full credit to Neo4j Labs for creating and open-sourcing the original dataset.

## Conversion Pipeline

See [pipeline/](pipeline/) for the tooling used to produce this dataset, including schema conversion, AI-agent query conversion, TypeDB validation, and semantic review.

## Citation

```bibtex
@software{text2typeql,
  title  = {Text2TypeQL: Natural Language to TypeQL 3.0 Query Dataset},
  year   = {2025},
  url    = {https://github.com/vaticle/text2typeql},
  note   = {4,728 validated query pairs across 7 domains, derived from Neo4j Labs text2cypher}
}
```

## License

Apache 2.0 -- see [LICENSE](LICENSE).
