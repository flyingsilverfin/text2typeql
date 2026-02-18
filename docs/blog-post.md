# Lessons from text2typeql - why TypeQL > Cypher and Agents still need supervision

We recently announced our text2typeql dataset. It contains 14,000 pairs of English questions and TypeQL queries generated to conform to the question.

The creation of this dataset had two auxiliary lessons:
1. We really came to see how TypeQL, using TypeDB's strict schema, is a superior target for query generation than Cypher with Neo4j
2. We learned a lot about how to orchestrate automated agentic work to be as reliable as possible

Let's start by doing a quick dive into how we automated the creation of this dataset using Claude.

## Creating text2typeql

### Schema conversion

The source dataset, Neo4j's text2cypher, contains multiple synthetic datasets, with a kind of "schema" provided in JSON in each dataset. Critically, Neo4j is 'schema optional' - any data can be added at any time! Neo4j 'schemas', when they do exist, are simple constructs like key constraints, uniqueness and existence constraints, and indexes - hence my use of quotes. These aren't tools designed to allow the build and validation of expressive, advanced data models.

Still, the JSON provided helped Claude generate starting schemas for each dataset. We then manually reviewed the schemas for expressivity, and sense-checked them. In general, they were good! However, we were still able to simplify them by applying subtyping in some places, or relaxed/tightened cardinality constraints in the schema.

Example Neo4j schema snippet from the `companies` dataset:

```cypher
"node_props": {
    "Person": [
      {
        "property": "name",
        "type": "STRING",
        "values": [ // truncated // ],
        "distinct_count": 7987
      },
       {
        "property": "id",
        "type": "STRING",
        "values": [ // truncated // ],
        "distinct_count": 8064
      },
      {
        "property": "summary",
        "type": "STRING",
        "values": [ // truncated //],
        "distinct_count": 6401
      }
  },
  ...
},
"relationships": [
    {
      "start": "Person",
      "type": "HAS_PARENT",
      "end": "Person"
    },
    {
      "start": "Person",
      "type": "HAS_CHILD",
      "end": "Person"
    },
...
]
```

This produced the following TypeQL snippet:
```typeql
define
entity person,
    owns name,
    owns person_id @key,
    owns summary,
    plays parent_of:parent,
    plays parent_of:child;

relation parent_of,
    relates parent,
    relates child;
```

We have found, and keep finding, that keeping a human in the loop is best for building efficient, optimized schemas and embedding human domain knowledge. You tend to know your problem better than any LLM - help it out by tweaking the schema or giving it the context on what you want!

Because TypeDB uses a strict schema, you just have to encode your domain knowledge once, and then your system will have those patterns and requirements enforced forever.

### Query conversion: attempt 1

The first approach to automate query conversion was to have Claude write a script which handled the conversion of a row of data, build a prompt, fired it off to an LLM provider's API, and then parse the output back out. 

Ttake advantage of TypeDB's schema and close the loop, the produced query would be validated against a running TypeDB loaded with the schema. Any errors were fed back into the prompt and re-submitted, up to 3 times. Voilà!

But... this gets really expensive really fast. And we had thousands of queries to convert, with thousands of API calls. No thanks!

### Query conversion: attempt 2

The obviously savings exist by leveraging the much more cost effective Claude Code subscription that was driving the work. So, the next attempt was to reformulate the script as an MCP server, and use sampling in order to ask Claude Code to do the actual work of query conversion. 

Of course, Anthropic is very unfriendly here and has not implemented sampling in Claude Code. I think this is actually kind of terrible - their pricing already has rate limits at multiple levels: presumably to ensure they have a reasonable cost basis. That's fine - but let me burn my tokens however works for me please! 

Any anyway, we'll just find ways to work around this restriction...

### Query conversion: attempt 3

To me this was the most interesting attempt, and caught me out right when I thought I was done!

Instead of handing off the work to another API or sampling, I asked Claude to do the conversion itself. In fact - was downright eagerly doing so as soon as the API-based program failed to convert a query anyway. 

This ran successfull and cost effectively for hours, occasionally hitting the 5-hour rate limits.

But the story when looking into the _quality_ of the generated TypeQL was horrible!

---
TODO continue
---

**Schema conversion.** Neo4j schemas were manually translated to TypeQL 3.0. Node labels became entity types, relationship types became relation types with explicit roles, and properties became attributes. TypeQL's richer type system sometimes required extending schemas beyond the Neo4j originals -- adding explicit entity subtypes, key constraints, or role distinctions to capture semantics that Cypher leaves implicit in property values or query-time conventions.

**Query conversion via AI agents.** Each Cypher query was converted to TypeQL using Claude Code subagents operating under a detailed TypeQL 3.0 reference. This was not mechanical transpilation. TypeQL's syntax and semantics differ from Cypher in fundamental ways: relations require explicit role names, query clauses follow a strict `match` then `sort` then `limit` then `fetch` ordering, aggregation uses `reduce` rather than implicit grouping, and subquery logic is expressed through custom functions. The agents reasoned about the English question's intent, not just the Cypher syntax. In several cases, the original Cypher was arguably wrong (using the wrong property or relation direction), and the TypeQL was written to correctly answer the English question instead.

**Validation against TypeDB.** Every generated query was executed against a live TypeDB instance to verify parsing and type-checking. This step caught syntax errors, incorrect role names, missing attributes, and type mismatches that were syntactically plausible but semantically invalid against the loaded schema.

**Semantic review.** A second pass verified that each TypeQL query actually answers the English question -- not just that it is valid TypeQL. This caught wrong relation directions (e.g., "tweets retweeted by others" versus "tweets that retweet others"), missing filter conditions, incorrect aggregation targets, and cases where optional-match semantics required `try {}` blocks rather than mandatory patterns.

**Failure documentation.** Queries that genuinely cannot be expressed in TypeQL 3.0 were documented with specific reasons. Across synthetic-1, 43 queries fell into this category; synthetic-2 adds 61 more. The remaining gaps are concentrated in string manipulation (`split()`, `substring()`), date component extraction (year, month, day-of-week), and a handful of schema mismatches where the Cypher references relationships that don't exist in the domain model. These entries provide a clear picture of TypeQL's current functional boundaries.

Many patterns initially classified as unconvertible were subsequently resolved as new TypeQL features and creative conversion techniques were discovered: `collect()` was replaced by **fetch subqueries** (nested queries inside fetch blocks), variable-length paths (`[:REL*]`) by **recursive stream functions**, `size()` by `len()` (TypeDB 3.8), `timestamp()` by max-aggregate proxies with integer arithmetic, and several queries by schema refinements (adding missing attribute ownership or changing attribute types).

The dataset exercises a broad range of TypeQL features: custom functions (`with fun`), chained reduce for HAVING-equivalent post-aggregation filtering, `let` expressions for computed values, type variables for polymorphic matching across relation types, negation, disjunction, regex patterns via `like`, fetch subqueries for nested data collection, recursive stream functions for transitive closure, and datetime/integer arithmetic for temporal queries.

## What the Type System Caught

Three categories of issues surfaced during conversion that were not anticipated but turned out to be valuable signals.

**Queries that cannot be expressed in TypeQL.** Across both datasets, 104 of 14,043 source queries (0.7%) require language features that TypeQL does not yet support: `split()` and `substring()` for string manipulation, date component extraction (`year`, `month`, `day-of-week`), epoch-to-datetime conversion, and dynamic `CONTAINS` between two variable strings. Each is documented with its original Cypher and the specific missing capability. In synthetic-1, 43 of 4,776 queries fell into this category; synthetic-2 adds 61 of 9,267. Rather than noise, these entries constitute a precise feature-gap analysis -- a checklist of what TypeQL would need to achieve full parity with Cypher's function library. Notably, several categories initially classified as impossible were subsequently resolved: `collect()` via fetch subqueries, `size()` via `len()`, variable-length paths via recursive stream functions, and `timestamp()` via max-aggregate proxies.

**Original Cypher queries that were semantically wrong.** More unexpectedly, an automated scan of all 13,939 converted query pairs found that **597 Cypher queries (4.3%)** contain semantic errors that TypeDB's type system either prevented or corrected during conversion. These were not TypeQL bugs -- the Cypher itself was wrong, and the looseness of Cypher's schema model allowed the errors to go undetected. The errors fall into six categories, each corresponding to a structural advantage of TypeDB's type system over Cypher's pattern-matching model:

*Relationship direction errors* (198 queries, 33%). Cypher represents relationships with arrows (`->`, `<-`) whose direction is a syntactic convention detached from semantic meaning. An LLM can easily reverse `(a)-[:FOLLOWS]->(b)` because nothing in the query syntax explains which end is the follower. TypeQL uses named roles -- `follows (follower: $a, followed: $b)` -- where the role names carry the semantics. The companies schema was particularly susceptible: relationship names like `HAS_CEO`, `HAS_INVESTOR`, `HAS_BOARD_MEMBER` are ambiguous about direction, and LLMs consistently reversed them. TypeDB's explicit role assignments (`investor: $p, organization: $o`) eliminate this ambiguity entirely. Companies and Twitch together account for 131 of these 198 errors.

*Relation counting vs property shortcuts* (180 queries, 30%). When a question asks "how many times was X retweeted", Cypher allows two approaches: count the actual relationships, or read a denormalized property like `t.favorites`. LLMs frequently choose the shortcut even when the property measures something different. TypeQL has no denormalized property shortcut for relationship counts -- to answer "how many retweets", you must traverse the `retweets` relation and count it explicitly. This pattern appears across Twitter (favorites used for retweets), Northwind (discount read from Order node instead of the orders relationship), Movies (fragile `size(collect(roles))` instead of counting relation attributes), and FinCEN (self-referencing patterns that don't match the schema).

*Property ownership errors* (160 queries, 27%). Cypher's property access (`node.property`) has no compile-time validation -- accessing a non-existent property silently returns null. LLMs exploit this looseness by guessing property names based on common patterns rather than checking the schema. TypeQL validates every `has` clause against the schema at query time. Twenty-eight Twitch queries use `s.name` on Stream when the schema defines `stream_name`; 46 Movies queries access `p.roles` on Person when `roles` belongs to the `acted_in` relation; 29 Northwind queries read `o.discount` from Order when `discount` belongs to the orders junction relation.

*Schema hallucinations* (59 queries, 10%). LLMs invented relationship types, properties, and node labels that don't exist in the schema at all. Neo4j's schemaless model means these queries parse without error but return empty results. TypeDB rejects them at query validation time. Examples include `[:AUTHOR]` relationships in Companies (the schema has `MENTIONS`), `c.population` on City entities (no population data exists), and `Book`/`Category` node types hallucinated in a schema that has `Article`/`IndustryCategory`.

| Error Category | Queries | % | Root Cause |
|----------------|--------:|--:|------------|
| Direction errors | 198 | 33% | Arrow syntax detached from semantics |
| Counting errors | 180 | 30% | Denormalized property shortcuts |
| Property errors | 160 | 27% | Silent null on missing properties |
| Hallucinated relations | 28 | 5% | No schema validation at query time |
| Hallucinated properties | 19 | 3% | No schema validation at query time |
| Hallucinated labels | 12 | 2% | No schema validation at query time |
| **Total** | **597** | | |

In each case the TypeQL was written to correctly answer the English question rather than reproduce the Cypher's mistake. A [full analysis](docs/neo_semantic_analysis/) documents all 597 errors with their source queries, scanner checks, and the specific TypeDB mechanism that caught them.

**Additional Cypher generation issues.** Beyond the 597 semantic errors attributable to language-structural differences, a further 640 queries have Cypher issues attributable to LLM generation quality: missing `COUNT()` aggregations, absent `ORDER BY` clauses, missing negation, omitted threshold values, and similar instruction-following failures. These are included in the analysis for completeness but are less interesting from a language-comparison perspective -- any query language would need these constructs, and the errors reflect LLM instruction-following failures rather than TypeDB advantages.

Together, these findings suggest that a strongly typed query language can serve as a static analysis layer over a dataset -- catching semantic errors that pass silently in more permissive systems. The 4.3% error rate is a conservative estimate; additional errors may exist that automated scanning cannot detect (e.g., subtle role confusion where both directions are schema-valid).
queries as TypeQL gains new features. The pipeline and tooling are included in the repository, so extending the dataset follows the same validated workflow.
