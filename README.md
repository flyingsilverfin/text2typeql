# Text2TypeQL

11,968 natural-language questions paired with validated TypeQL 3.0 queries across 15 domains (4,728 from synthetic-1, 7,240 from synthetic-2), with 1,853 more pending conversion.

## Overview

Foundation models generate SQL and Cypher reasonably well thanks to large public corpora and benchmark datasets. TypeQL 3.0, released with TypeDB 3.0, has little of that, and relies mostly on being close to natural language. Even frontier models struggle to produce correct queries without expensive reasoning sequences and context-heavy prompting. This dataset addresses that gap.

**Text2TypeQL** provides supervised training data for fine-tuning models on TypeQL generation, a retrieval corpus for few-shot prompting and RAG, and a standardized evaluation benchmark for TypeQL generation quality. Each entry includes the English question, the original Cypher query (from the source dataset), and a validated TypeQL 3.0 query. TypeQL schemas model the domains. Schemas were generated according to the loose Neo4j schema provided, and tweaked throughout the query generation process.

The dataset was produced by converting Neo4j Labs' [text2cypher](https://github.com/neo4j-labs/text2cypher) dataset, which itself was generated using AI. It was created using agents operating under a detailed TypeQL 3.0 reference, with every query validated against a live TypeDB instance and semantically reviewed to verify it correctly answers the English question. About 5-10% of remaining queries were then manually prompted with extra information.

Interestingly, the generation of TypeQL, which also relied on semantic validation against the schema, highlighted at least 30 cases in the synthetic-1 dataset (not done for second dataset yet), i.e. ~0.75% where Neo4j queries were incorrect against their own schema - but because it lacks a strong type system like TypeDB's, these were never found.

## Source Datasets

Two Neo4j text2cypher source datasets are used:

| Source | Neo4j Directory | Databases | Valid Queries | Status |
|--------|----------------|-----------|---------------|--------|
| `synthetic-1` | `synthetic_opus_demodbs` | 7 | 4,776 | 4,728 converted |
| `synthetic-2` | `synthetic_gpt4o_demodbs` | 15 | 9,267 | 7,240 converted (12/15 complete) |

## Domains

### synthetic-1 (fully converted)

| Domain | Queries | Description |
|--------|---------|-------------|
| [twitter](dataset/synthetic-1/twitter/) | 491 | Users, tweets, hashtags, retweets, follows |
| [twitch](dataset/synthetic-1/twitch/) | 553 | Streamers, games, teams, subscriptions |
| [movies](dataset/synthetic-1/movies/) | 723 | Actors, directors, producers, reviews, roles |
| [neoflix](dataset/synthetic-1/neoflix/) | 910 | Movies, ratings, genres, subscriptions |
| [recommendations](dataset/synthetic-1/recommendations/) | 741 | Users, movies, genres, ratings, actors |
| [companies](dataset/synthetic-1/companies/) | 929 | Organizations, subsidiaries, CEOs, articles |
| [gameofthrones](dataset/synthetic-1/gameofthrones/) | 381 | Characters, houses, battles, interactions |
| **Total** | **4,728** | + 48 documented failures |

### synthetic-2 (12/15 databases complete)

| Domain | Total | Converted | Description |
|--------|-------|-----------|-------------|
| [bluesky](dataset/synthetic-2/bluesky/) | 135 | 135 | Social network posts and interactions |
| [buzzoverflow](dataset/synthetic-2/buzzoverflow/) | 592 | 578 | Q&A platform (Stack Overflow-like) |
| [companies](dataset/synthetic-2/companies/) | 966 | 941 | Organizations, subsidiaries, CEOs, articles |
| [fincen](dataset/synthetic-2/fincen/) | 614 | 584 | Financial crime reports and filings |
| [gameofthrones](dataset/synthetic-2/gameofthrones/) | 393 | 384 | Characters, houses, battles, interactions |
| [grandstack](dataset/synthetic-2/grandstack/) | 807 | 793 | Movie reviews (GRANDstack demo) |
| [movies](dataset/synthetic-2/movies/) | 738 | 728 | Actors, directors, producers, reviews, roles |
| [neoflix](dataset/synthetic-2/neoflix/) | 923 | 913 | Movies, ratings, genres, subscriptions |
| [network](dataset/synthetic-2/network/) | 625 | 613 | Computer network topology |
| [northwind](dataset/synthetic-2/northwind/) | 807 | 780 | Products, orders, suppliers (Northwind) |
| [offshoreleaks](dataset/synthetic-2/offshoreleaks/) | 507 | 493 | Offshore financial entities |
| [stackoverflow2](dataset/synthetic-2/stackoverflow2/) | 307 | 298 | Q&A platform variant |
| [recommendations](dataset/synthetic-2/recommendations/) | 775 | -- | Users, movies, genres, ratings, actors |
| [twitch](dataset/synthetic-2/twitch/) | 576 | -- | Streamers, games, teams, subscriptions |
| [twitter](dataset/synthetic-2/twitter/) | 502 | -- | Users, tweets, hashtags, retweets, follows |
| **Total** | **9,267** | **7,240** | + 174 documented failures |

## Data Format

### Merged dataset

`dataset/synthetic-1/all_queries.csv` contains all converted queries for synthetic-1:

| Column | Description |
|--------|-------------|
| `domain` | Database domain name |
| `original_index` | Index in the source dataset |
| `question` | Natural-language question |
| `cypher` | Original Cypher query |
| `typeql` | Validated TypeQL 3.0 query |

### Per-domain files

Each `dataset/<source>/<domain>/` directory contains:

- `schema.tql` -- TypeQL schema definition
- `queries.csv` -- Query pairs (`original_index`, `question`, `cypher`, `typeql`)
- `neo4j_schema.json` -- Original Neo4j schema
- `README.md` -- Domain stats, failed queries with reasons, and Cypher errors found

## Usage

```python
import pandas as pd

# Load all synthetic-1 queries
df = pd.read_csv("dataset/synthetic-1/all_queries.csv")

# Filter by domain
twitter = df[df["domain"] == "twitter"]

# Load a single domain
movies = pd.read_csv("dataset/synthetic-1/movies/queries.csv")
```

## What the Type System Caught

TypeDB's strict type system exposed roughly 30 queries (there may be more) across four databases where the original Cypher does not correctly answer the English question. Three patterns recurred:

- **Wrong property**: Twitter queries checking `favorites` when the question asks about retweets. TypeQL's explicit `retweets` relation forces correct semantics.
- **Wrong direction**: Companies queries reversing supplier/customer direction. TypeQL's `supplies (supplier: $x, customer: $y)` makes the reversal visible.
- **Wrong traversal**: Twitter queries returning tweets by the user instead of tweets by followers. TypeQL's role-based syntax eliminates the ambiguity.

In each case the TypeQL was written to correctly answer the English question. Details are in each domain's README.

## Failed Queries

48 of 4,776 source queries (1%) from synthetic-1 cannot yet be expressed in TypeQL 3.0. They require features not yet supported: `size()` for string/list length, array indexing, epoch timestamp conversion, duration and date arithmetic, date component extraction, and `collect()` aggregation. Each is documented with its original Cypher and the specific missing capability in the per-domain READMEs.

## TODO

- [ ] Complete synthetic-2 conversion (3 databases remaining: recommendations, twitch, twitter)
- [ ] Merge synthetic-2 queries into all_queries.csv
- [ ] Standardize use of `_` in TypeQL variable names across all queries
- [ ] Regularize synthetic-1 TypeQL queries to use updated relation syntax: `reltype (role: $var)` instead of `$r (role: $var) isa reltype`

## Source

Derived from Neo4j Labs' [text2cypher](https://github.com/neo4j-labs/text2cypher) benchmark (`datasets/synthetic_opus_demodbs/` and `datasets/synthetic_gpt4o_demodbs/`). Full credit to Neo4j Labs for creating and open-sourcing the original dataset.

## Conversion Pipeline

See [pipeline/](pipeline/) for the tooling used to produce this dataset, including schema conversion, AI-agent query conversion, TypeDB validation, and semantic review.

## Citation

```bibtex
@software{text2typeql,
  title  = {Text2TypeQL: Natural Language to TypeQL 3.0 Query Dataset},
  year   = {2025},
  url    = {https://github.com/vaticle/text2typeql},
  note   = {11,968 validated query pairs across 15 domains, derived from Neo4j Labs text2cypher}
}
```

## License

Apache 2.0 -- see [LICENSE](LICENSE).
