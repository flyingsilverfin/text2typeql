# Text2TypeQL: An Open Dataset for Teaching Models to Query TypeDB

Foundation models generate SQL and Cypher reasonably well. Large public corpora, benchmark datasets, and years of Stack Overflow answers give them plenty to learn from. TypeQL 3.0, released with TypeDB 3.0, has none of that. Very little TypeQL exists in the wild, and even frontier models like GPT-4 and Claude struggle to produce correct queries without heavy prompting or chain-of-thought scaffolding. This limits adoption in a practical way: developers cannot simply ask an LLM for help writing TypeQL the way they can with SQL. We wanted to change that.

## What We Built

**text2typeql** is an open-source dataset of 4,776 natural-language questions paired with validated TypeQL 3.0 queries, spanning seven diverse domains:

- **Social networks** (Twitter) -- users, tweets, hashtags, retweets, follows
- **Streaming platforms** (Twitch, Neoflix) -- streamers, games, teams, subscriptions, movies, ratings
- **Film industry** (Movies) -- actors, directors, producers, reviews, roles
- **Recommendations** -- users, movies, genres, ratings, actors
- **Corporate graphs** (Companies) -- organizations, subsidiaries, CEOs, articles, cities
- **Fiction networks** (Game of Thrones) -- characters, houses, battles, interactions

Of the 4,776 source queries, 4,724 were successfully converted and validated (98.9%). The remaining 52 are documented with precise reasons for failure. Each successful entry includes the English question, the original Cypher query, and the validated TypeQL. The dataset also ships with seven TypeQL schemas modelling each domain.

## Where It Comes From

The source material is Neo4j Labs' [text2cypher](https://github.com/neo4j-labs/text2cypher) benchmark, a synthetically generated dataset of approximately 4,800 English/Cypher pairs across seven demo databases with realistic schemas. It was designed to evaluate and fine-tune LLMs on natural-language-to-Cypher translation. We took the same questions and schemas, converted everything to TypeQL 3.0, and preserved the original Cypher alongside each query for direct comparison. Full credit to Neo4j Labs for creating and open-sourcing the original dataset.

## How It Was Generated

The conversion pipeline had five stages, each catching a different class of error.

**Schema conversion.** Neo4j schemas were manually translated to TypeQL 3.0. Node labels became entity types, relationship types became relation types with explicit roles, and properties became attributes. TypeQL's richer type system sometimes required extending schemas beyond the Neo4j originals -- adding explicit entity subtypes, key constraints, or role distinctions to capture semantics that Cypher leaves implicit in property values or query-time conventions.

**Query conversion via AI agents.** Each Cypher query was converted to TypeQL using Claude Code subagents operating under a detailed TypeQL 3.0 reference. This was not mechanical transpilation. TypeQL's syntax and semantics differ from Cypher in fundamental ways: relations require explicit role names, query clauses follow a strict `match` then `sort` then `limit` then `fetch` ordering, aggregation uses `reduce` rather than implicit grouping, and subquery logic is expressed through custom functions. The agents reasoned about the English question's intent, not just the Cypher syntax. In several cases, the original Cypher was arguably wrong (using the wrong property or relation direction), and the TypeQL was written to correctly answer the English question instead.

**Validation against TypeDB.** Every generated query was executed against a live TypeDB 3.0 instance to verify parsing and type-checking. This step caught syntax errors, incorrect role names, missing attributes, and type mismatches that were syntactically plausible but semantically invalid against the loaded schema.

**Semantic review.** A second pass verified that each TypeQL query actually answers the English question -- not just that it is valid TypeQL. This caught wrong relation directions (e.g., "tweets retweeted by others" versus "tweets that retweet others"), missing filter conditions, incorrect aggregation targets, and cases where optional-match semantics required `try {}` blocks rather than mandatory patterns.

**Failure documentation.** Queries that genuinely cannot be expressed in TypeQL 3.0 were documented with specific reasons: string length functions (`size()`), array index access, epoch timestamp conversion, duration arithmetic, `collect()` aggregation, and similar gaps. These 52 entries provide a clear picture of TypeQL's current functional boundaries.

The dataset exercises a broad range of TypeQL 3.0 features: custom functions (`with fun`), chained reduce for HAVING-equivalent post-aggregation filtering, `let` expressions for computed values, type variables for polymorphic matching across relation types, negation, disjunction, and regex patterns via `like`.

## How This Helps

**Fine-tuning.** The dataset provides supervised training data for fine-tuning smaller, faster models (Llama, Mistral, Phi, and similar) on TypeQL generation. This enables local, low-latency, cost-effective text-to-TypeQL without relying on frontier model APIs.

**Few-shot prompting and RAG.** The 4,724 validated examples serve as a rich retrieval corpus for retrieval-augmented generation or few-shot in-context learning. Given a user's natural-language question, a system can retrieve similar questions from the dataset and include their TypeQL as examples in the prompt.

**Evaluation benchmark.** The dataset provides a standardized measure of TypeQL generation quality, analogous to what text2cypher provides for Cypher. Researchers and engineers can evaluate model accuracy against known-good queries across varying complexity levels.

**Learning resource.** Side-by-side Cypher and TypeQL for the same English question makes the dataset a practical reference for engineers learning TypeQL. Seeing how familiar Cypher patterns map to TypeQL -- explicit roles, `reduce` for aggregation, `let` for computed values -- builds intuition faster than documentation alone.

**Feature coverage.** The seven domains collectively exercise nearly the full breadth of TypeQL 3.0 query syntax, from simple entity lookups to custom functions with chained aggregation stages. Models trained on this data will encounter the patterns they need for real-world use.

## Get Involved

The dataset is available on GitHub. We welcome contributions: additional domains, alternative TypeQL formulations for existing queries, or conversions of the 52 currently-failed queries as TypeQL gains new features. The pipeline and tooling are included in the repository, so extending the dataset follows the same validated workflow.
