# Neoflix Dataset - Unfixable Queries

The following 17 queries from the original dataset cannot be converted to TypeQL 
because they require features not supported by the language.

## String Length Operations (12 queries)

TypeQL does not support `size()` or string length functions for sorting.
These queries use Cypher's `ORDER BY size(field)` which has no TypeQL equivalent.

| Index | Question |
|-------|----------|
| 126 | List the top 3 most controversial adult films based on their overview |
| 255 | What are the first 3 movies with the most characters in their overview? |
| 620 | What are the top 5 movies with the longest taglines? |
| 638 | What are the top 5 longest taglines in adult films? |
| 643 | Which 3 movies have the most extensive poster paths? |
| 735 | What are the top 5 movies with the most complex taglines? |
| 737 | Which 3 adult films have the most provocative taglines? |
| 741 | Which 3 videos have the most detailed overviews? |
| 778 | List the top 5 movies with the most extensive taglines |
| 797 | What are the top 5 movies with the most complex taglines? |
| 819 | List the first 3 movies with the most extensive homepages |
| 831 | List the top 5 movies with the longest taglines |

## Aggregation with HAVING-like Filter (4 queries)

TypeQL's `reduce` clause returns a single aggregated value and cannot be filtered 
with a WHERE-like clause. These queries require grouping + counting + filtering 
(similar to SQL's GROUP BY + HAVING) which TypeQL does not support.

| Index | Question |
|-------|----------|
| 118 | Which languages have more than 5 movies spoken in them? |
| 143 | Which countries have produced more than 10 movies in the database? |
| 475 | List the top 5 movies that have been rated by users with an average rating below 4.0 |
| 508 | Find all actors who have appeared in more than 10 movies |

## Complex Date Arithmetic (1 query)

TypeQL does not support date arithmetic or duration calculations.

| Index | Question |
|-------|----------|
| 611 | What are the top 5 movies with the most extended release dates range within a collection? |
