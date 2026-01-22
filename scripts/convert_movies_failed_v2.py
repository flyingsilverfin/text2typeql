#!/usr/bin/env python3
"""
Convert failed Cypher queries to TypeQL for the movies database.
Fixed version that properly handles TypeQL 3.x semantics for reduce/groupby.

Key insights for TypeQL 3.x:
1. After `reduce`, only the aggregation variable and groupby variables are available
2. Filtering after reduce must use `match $var > N;` (second match clause)
3. Disjunctions use syntax: $r in [1970, 1980, 1990, 2000, 2010, 2020];
"""

import csv
from typing import Optional, Tuple
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# TypeDB connection
DATABASE = "text2typeql_movies"
FAILED_PATH = "/opt/text2typeql/output/movies/failed.csv"
OUTPUT_PATH = "/opt/text2typeql/output/movies/queries.csv"


def validate_query(driver, query: str) -> Tuple[bool, str]:
    """Validate a TypeQL query against the database."""
    try:
        with driver.transaction(DATABASE, TransactionType.READ) as tx:
            result = tx.query(query).resolve()
            if hasattr(result, 'as_concept_documents'):
                list(result.as_concept_documents())
            elif hasattr(result, 'as_value'):
                result.as_value()
        return True, ""
    except Exception as e:
        return False, str(e)


def convert_query(idx: int, question: str, cypher: str) -> Optional[str]:
    """Convert a question/cypher pair to TypeQL based on question semantics."""

    # ===== Query 27, 132, 463: Top 5 movies by number of reviews =====
    # Need groupby to keep $t available after reduce
    if idx in [27, 132, 463]:
        return """match
  $m isa movie, has title $t;
  (reviewer: $p, film: $m) isa reviewed;
reduce $numReviews = count($p) groupby $t;
sort $numReviews desc;
limit 5;
fetch {
  "movie": $t,
  "numReviews": $numReviews
};"""

    # ===== Query 36, 63, 245, 605: Top 5 movies with most actors =====
    if idx in [36, 63, 245, 605]:
        return """match
  $m isa movie, has title $t;
  (actor: $p, film: $m) isa acted_in;
reduce $roleCount = count($p) groupby $t;
sort $roleCount desc;
limit 5;
fetch {
  "movie": $t,
  "roleCount": $roleCount
};"""

    # ===== Query 455, 471, 667: Top 3 movies with most actors =====
    if idx in [455, 471, 667]:
        return """match
  $m isa movie, has title $t;
  (actor: $p, film: $m) isa acted_in;
reduce $num_roles = count($p) groupby $t;
sort $num_roles desc;
limit 3;
fetch {
  "movie": $t,
  "num_roles": $num_roles
};"""

    # ===== Query 97: Top 5 movies with most actors born after 1970 =====
    if idx == 97:
        return """match
  $m isa movie, has title $t;
  $p isa person, has born $b;
  $b > 1970;
  (actor: $p, film: $m) isa acted_in;
reduce $roleCount = count($p) groupby $t;
sort $roleCount desc;
limit 5;
fetch {
  "movie": $t,
  "roleCount": $roleCount
};"""

    # ===== Query 103: First 3 actors in more than one movie with tagline containing 'beginning' =====
    if idx == 103:
        return """match
  $p isa person, has name $n;
  (actor: $p, film: $m) isa acted_in;
  $m has tagline $tag;
  $tag like ".*beginning.*";
reduce $movieCount = count($m) groupby $n;
match $movieCount > 1;
limit 3;
fetch {
  "name": $n
};"""

    # ===== Query 104: Top 5 people who produced movies with more than 2000 votes =====
    if idx == 104:
        return """match
  $p isa person, has name $n;
  (producer: $p, film: $m) isa produced;
  $m has votes $v;
  $v > 2000;
reduce $num_movies = count($m) groupby $n;
sort $num_movies desc;
limit 5;
fetch {
  "producer": $n,
  "num_movies": $num_movies
};"""

    # ===== Query 109: Top 3 actors with most movies released before 1990 =====
    if idx == 109:
        return """match
  $p isa person, has name $n;
  (actor: $p, film: $m) isa acted_in;
  $m has released $r;
  $r < 1990;
reduce $numRoles = count($m) groupby $n;
sort $numRoles desc;
limit 3;
fetch {
  "actor": $n,
  "numRoles": $numRoles
};"""

    # ===== Query 139: Top 5 movies with highest average review rating =====
    if idx == 139:
        return """match
  $m isa movie, has title $t;
  $rev (reviewer: $p, film: $m) isa reviewed, has rating $r;
reduce $avgRating = mean($r) groupby $t;
sort $avgRating desc;
limit 5;
fetch {
  "movie": $t,
  "avgRating": $avgRating
};"""

    # ===== Query 159: Who directed most movies released after 2000 =====
    if idx == 159:
        return """match
  $d isa person, has name $n;
  (director: $d, film: $m) isa directed;
  $m has released $r;
  $r > 2000;
reduce $num_movies = count($m) groupby $n;
sort $num_movies desc;
limit 1;
fetch {
  "director": $n,
  "num_movies": $num_movies
};"""

    # ===== Query 182: Top 5 movies by number of actors =====
    if idx == 182:
        return """match
  $m isa movie, has title $t;
  (actor: $p, film: $m) isa acted_in;
reduce $numRoles = count($p) groupby $t;
sort $numRoles desc;
limit 5;
fetch {
  "movie": $t,
  "numRoles": $numRoles
};"""

    # ===== Query 193: Top 5 people who acted in most movies with a tagline =====
    if idx == 193:
        return """match
  $p isa person, has name $n;
  (actor: $p, film: $m) isa acted_in;
  $m has tagline $tag;
reduce $movieCount = count($m) groupby $n;
sort $movieCount desc;
limit 5;
fetch {
  "name": $n,
  "movieCount": $movieCount
};"""

    # ===== Query 214: First 3 actors in more than one movie with tagline containing 'freedom' =====
    if idx == 214:
        return """match
  $p isa person, has name $n;
  (actor: $p, film: $m) isa acted_in;
  $m has tagline $tag;
  $tag like ".*freedom.*";
reduce $movieCount = count($m) groupby $n;
match $movieCount > 1;
limit 3;
fetch {
  "name": $n
};"""

    # ===== Query 219: Top 5 actors by number of movies with at least 100 votes =====
    if idx == 219:
        return """match
  $p isa person, has name $n;
  (actor: $p, film: $m) isa acted_in;
  $m has votes $v;
  $v >= 100;
reduce $numMovies = count($m) groupby $n;
sort $numMovies desc;
limit 5;
fetch {
  "actor": $n,
  "numMovies": $numMovies
};"""

    # ===== Query 225: Top 3 actors with movies having most distinct taglines =====
    if idx == 225:
        return """match
  $a isa person, has name $n;
  (actor: $a, film: $m) isa acted_in;
  $m has tagline $tag;
reduce $num_genres = count($m) groupby $n;
sort $num_genres desc;
limit 3;
fetch {
  "actor": $n,
  "num_genres": $num_genres
};"""

    # ===== Query 230: Top 3 actors by number of movies from 1990 to 2000 =====
    if idx == 230:
        return """match
  $p isa person, has name $n;
  (actor: $p, film: $m) isa acted_in;
  $m has released $r;
  $r >= 1990;
  $r <= 2000;
reduce $numMovies = count($m) groupby $n;
sort $numMovies desc;
limit 3;
fetch {
  "actor": $n,
  "numMovies": $numMovies
};"""

    # ===== Query 264: Top 5 persons with most reviews written =====
    if idx == 264:
        return """match
  $p isa person, has name $n;
  (reviewer: $p, film: $m) isa reviewed;
reduce $num_reviews = count($m) groupby $n;
sort $num_reviews desc;
limit 5;
fetch {
  "name": $n,
  "num_reviews": $num_reviews
};"""

    # ===== Query 269: Top 5 movies with lowest ratings =====
    if idx == 269:
        return """match
  $m isa movie, has title $t;
  $rev (reviewer: $p, film: $m) isa reviewed, has rating $r;
reduce $averageRating = mean($r) groupby $t;
sort $averageRating asc;
limit 5;
fetch {
  "movie": $t,
  "averageRating": $averageRating
};"""

    # ===== Query 275: Who directed most movies released after 2000 =====
    if idx == 275:
        return """match
  $d isa person, has name $n;
  (director: $d, film: $m) isa directed;
  $m has released $r;
  $r > 2000;
reduce $num_movies = count($m) groupby $n;
sort $num_movies desc;
limit 1;
fetch {
  "director": $n,
  "num_movies": $num_movies
};"""

    # ===== Query 277: Top 5 movies with most diverse cast (birth years) =====
    if idx == 277:
        return """match
  $m isa movie, has title $t;
  (actor: $p, film: $m) isa acted_in;
  $p has born $b;
reduce $numUniqueBirthYears = count($p) groupby $t;
sort $numUniqueBirthYears desc;
limit 5;
fetch {
  "movie": $t,
  "numUniqueBirthYears": $numUniqueBirthYears
};"""

    # ===== Query 287: Movies reviewed by more than two persons =====
    if idx == 287:
        return """match
  $m isa movie, has title $t;
  (reviewer: $p, film: $m) isa reviewed;
reduce $count = count($p) groupby $t;
match $count > 2;
fetch {
  "title": $t
};"""

    # ===== Query 292: Top 5 directors of movies released after 1995 =====
    if idx == 292:
        return """match
  $d isa person, has name $n;
  (director: $d, film: $m) isa directed;
  $m has released $r;
  $r > 1995;
reduce $num_movies = count($m) groupby $n;
sort $num_movies desc;
limit 5;
fetch {
  "director": $n,
  "num_movies": $num_movies
};"""

    # ===== Query 301: Top 3 highest-rated movies by reviews =====
    if idx == 301:
        return """match
  $m isa movie, has title $t;
  $rev (reviewer: $p, film: $m) isa reviewed, has rating $r;
reduce $avgRating = mean($r) groupby $t;
sort $avgRating desc;
limit 3;
fetch {
  "movie": $t,
  "avgRating": $avgRating
};"""

    # ===== Query 313: How many people have directed more than two movies =====
    if idx == 313:
        return """match
  $p isa person, has name $n;
  (director: $p, film: $m) isa directed;
reduce $num_directed = count($m) groupby $n;
match $num_directed > 2;
reduce $num_people_directed_more_than_two = count($n);
fetch {
  "num_people_directed_more_than_two": $num_people_directed_more_than_two
};"""

    # ===== Query 328: Top 5 people who directed movies with more than 200 votes =====
    if idx == 328:
        return """match
  $p isa person, has name $n;
  (director: $p, film: $m) isa directed;
  $m has votes $v;
  $v > 200;
reduce $num_movies = count($m) groupby $n;
sort $num_movies desc;
limit 5;
fetch {
  "director": $n,
  "num_movies": $num_movies
};"""

    # ===== Query 348: Top 3 people who acted in at least two movies with more than 100 votes =====
    if idx == 348:
        return """match
  $p isa person, has name $n;
  (actor: $p, film: $m) isa acted_in;
  $m has votes $v;
  $v > 100;
reduce $numMovies = count($m) groupby $n;
match $numMovies >= 2;
sort $numMovies desc;
limit 3;
fetch {
  "name": $n
};"""

    # ===== Query 372: Who produced most movies released after 2000 =====
    if idx == 372:
        return """match
  $p isa person, has name $n;
  (producer: $p, film: $m) isa produced;
  $m has released $r;
  $r > 2000;
reduce $num_movies = count($m) groupby $n;
sort $num_movies desc;
limit 1;
fetch {
  "producer": $n,
  "num_movies": $num_movies
};"""

    # ===== Query 385: Movies with released year divisible by 10 =====
    if idx == 385:
        return """match
  $m isa movie, has title $t, has released $r;
  $r in [1970, 1980, 1990, 2000, 2010, 2020];
fetch {
  "title": $t,
  "released": $r
};"""

    # ===== Query 397: Who directed most movies with tagline containing 'limit' =====
    if idx == 397:
        return """match
  $d isa person, has name $n;
  (director: $d, film: $m) isa directed;
  $m has tagline $tag;
  $tag like ".*limit.*";
reduce $num_movies = count($m) groupby $n;
sort $num_movies desc;
limit 1;
fetch {
  "director": $n,
  "num_movies": $num_movies
};"""

    # ===== Query 405: Who has most diverse roles =====
    if idx == 405:
        return """match
  $p isa person, has name $n;
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
reduce $num_roles = count($r) groupby $n;
sort $num_roles desc;
limit 1;
fetch {
  "actor": $n,
  "num_roles": $num_roles
};"""

    # ===== Query 412: Who acted in most movies released in the 1990s =====
    if idx == 412:
        return """match
  $p isa person, has name $n;
  (actor: $p, film: $m) isa acted_in;
  $m has released $r;
  $r >= 1990;
  $r < 2000;
reduce $num_movies = count($m) groupby $n;
sort $num_movies desc;
limit 1;
fetch {
  "actor": $n,
  "num_movies": $num_movies
};"""

    # ===== Query 420: Who has written most movies released after 2000 =====
    if idx == 420:
        return """match
  $p isa person, has name $n;
  (writer: $p, film: $m) isa wrote;
  $m has released $r;
  $r > 2000;
reduce $num_movies = count($m) groupby $n;
sort $num_movies desc;
limit 1;
fetch {
  "name": $n,
  "num_movies": $num_movies
};"""

    # ===== Query 425: Who has most movies with 'Silly, but fun' review =====
    if idx == 425:
        return """match
  $p isa person, has name $n;
  $rev (reviewer: $rev_p, film: $m) isa reviewed, has summary "Silly, but fun";
  (actor: $p, film: $m) isa acted_in;
reduce $silly_reviews = count($m) groupby $n;
sort $silly_reviews desc;
limit 1;
fetch {
  "name": $n,
  "silly_reviews": $silly_reviews
};"""

    # ===== Query 430: Top 3 actors with most diverse roles =====
    if idx == 430:
        return """match
  $p isa person, has name $n;
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
reduce $num_roles = count($r) groupby $n;
sort $num_roles desc;
limit 3;
fetch {
  "actor": $n,
  "number_of_distinct_roles": $num_roles
};"""

    # ===== Query 431: Persons who directed most movies with tagline containing 'world' =====
    if idx == 431:
        return """match
  $p isa person, has name $n;
  (director: $p, film: $m) isa directed;
  $m has tagline $tag;
  $tag like ".*world.*";
reduce $num_movies = count($m) groupby $n;
sort $num_movies desc;
limit 1;
fetch {
  "name": $n,
  "num_movies": $num_movies
};"""

    # ===== Query 432: Who has most roles in movies released before 1985 =====
    if idx == 432:
        return """match
  $p isa person, has name $n;
  (actor: $p, film: $m) isa acted_in;
  $m has released $r;
  $r < 1985;
reduce $numRoles = count($m) groupby $n;
sort $numRoles desc;
limit 1;
fetch {
  "name": $n,
  "numRoles": $numRoles
};"""

    # ===== Query 434: Who reviewed most movies with rating above 90 =====
    if idx == 434:
        return """match
  $p isa person, has name $n;
  $rev (reviewer: $p, film: $m) isa reviewed, has rating $r;
  $r > 90;
reduce $num_reviews = count($m) groupby $n;
sort $num_reviews desc;
limit 1;
fetch {
  "reviewer": $n,
  "number_of_reviews": $num_reviews
};"""

    # ===== Query 436: Who directed most movies with at least three actors =====
    if idx == 436:
        return """match
  $d isa person, has name $n;
  (director: $d, film: $m) isa directed;
  (actor: $a1, film: $m) isa acted_in;
  (actor: $a2, film: $m) isa acted_in;
  (actor: $a3, film: $m) isa acted_in;
  not { $a1 is $a2; };
  not { $a1 is $a3; };
  not { $a2 is $a3; };
reduce $num_movies = count($m) groupby $n;
sort $num_movies desc;
limit 1;
fetch {
  "director": $n,
  "num_movies": $num_movies
};"""

    # ===== Query 439: Who has most productions in movies released in the 2000s =====
    if idx == 439:
        return """match
  $p isa person, has name $n;
  (producer: $p, film: $m) isa produced;
  $m has released $r;
  $r >= 2000;
  $r < 2010;
reduce $productions = count($m) groupby $n;
sort $productions desc;
limit 1;
fetch {
  "producer": $n,
  "productions": $productions
};"""

    # ===== Query 441: Top 5 actors with most reviews for movies they acted in =====
    if idx == 441:
        return """match
  $a isa person, has name $n;
  (actor: $a, film: $m) isa acted_in;
  (reviewer: $rev_p, film: $m) isa reviewed;
reduce $reviews = count($m) groupby $n;
sort $reviews desc;
limit 5;
fetch {
  "actor": $n,
  "reviews": $reviews
};"""

    # ===== Query 443: Who has most directed movies without acting =====
    if idx == 443:
        return """match
  $d isa person, has name $n;
  (director: $d, film: $m) isa directed;
  not { (actor: $d, film: $m2) isa acted_in; };
reduce $directedCount = count($m) groupby $n;
sort $directedCount desc;
limit 1;
fetch {
  "director": $n,
  "directedCount": $directedCount
};"""

    # ===== Query 465: Top 3 movies with most diverse roles =====
    if idx == 465:
        return """match
  $m isa movie, has title $t;
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
reduce $role_diversity = count($r) groupby $t;
sort $role_diversity desc;
limit 3;
fetch {
  "movie": $t,
  "role_diversity": $role_diversity
};"""

    # ===== Query 468: 3 directors with highest average movie rating =====
    if idx == 468:
        return """match
  $d isa person, has name $n;
  (director: $d, film: $m) isa directed;
  $rev (reviewer: $p, film: $m) isa reviewed, has rating $r;
reduce $avg_rating = mean($r) groupby $n;
sort $avg_rating desc;
limit 3;
fetch {
  "director": $n,
  "avg_rating": $avg_rating
};"""

    # ===== Query 482: 3 directors who directed most movies released after 2005 =====
    if idx == 482:
        return """match
  $d isa person, has name $n;
  (director: $d, film: $m) isa directed;
  $m has released $r;
  $r > 2005;
reduce $num_movies = count($m) groupby $n;
sort $num_movies desc;
limit 3;
fetch {
  "director": $n,
  "num_movies": $num_movies
};"""

    # ===== Query 490: 3 movies with at least 2 reviews containing 'compelling' =====
    if idx == 490:
        return """match
  $m isa movie, has title $t;
  $rev (reviewer: $p, film: $m) isa reviewed, has summary $s;
  $s like ".*compelling.*";
reduce $reviewCount = count($rev) groupby $t;
match $reviewCount >= 2;
limit 3;
fetch {
  "title": $t
};"""

    # ===== Query 493: Top 5 actors by variety of roles =====
    if idx == 493:
        return """match
  $p isa person, has name $n;
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
reduce $roleCount = count($r) groupby $n;
sort $roleCount desc;
limit 5;
fetch {
  "actor": $n,
  "numberOfRoles": $roleCount
};"""

    # ===== Query 499: 3 persons who acted in most movies with tagline containing 'journey' =====
    if idx == 499:
        return """match
  $p isa person, has name $n;
  (actor: $p, film: $m) isa acted_in;
  $m has tagline $tag;
  $tag like ".*journey.*";
reduce $movieCount = count($m) groupby $n;
sort $movieCount desc;
limit 3;
fetch {
  "name": $n
};"""

    # ===== Query 501: 3 persons with most distinct roles =====
    if idx == 501:
        return """match
  $p isa person, has name $n;
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
reduce $num_roles = count($r) groupby $n;
sort $num_roles desc;
limit 3;
fetch {
  "person": $n,
  "num_roles": $num_roles
};"""

    # ===== Query 506: 3 persons with most directed movies that have a tagline =====
    if idx == 506:
        return """match
  $p isa person, has name $n;
  (director: $p, film: $m) isa directed;
  $m has tagline $tag;
reduce $directedCount = count($m) groupby $n;
sort $directedCount desc;
limit 3;
fetch {
  "name": $n,
  "directedCount": $directedCount
};"""

    # ===== Query 508: 3 movies with highest average review ratings =====
    if idx == 508:
        return """match
  $m isa movie, has title $t;
  $rev (reviewer: $p, film: $m) isa reviewed, has rating $r;
reduce $avgRating = mean($r) groupby $t;
sort $avgRating desc;
limit 3;
fetch {
  "movie": $t,
  "avgRating": $avgRating
};"""

    # ===== Query 511: 3 movies with lowest average ratings =====
    if idx == 511:
        return """match
  $m isa movie, has title $t;
  $rev (reviewer: $p, film: $m) isa reviewed, has rating $r;
reduce $avgRating = mean($r) groupby $t;
sort $avgRating asc;
limit 3;
fetch {
  "movie": $t,
  "avgRating": $avgRating
};"""

    # ===== Query 516: 3 persons who follow the most directors =====
    if idx == 516:
        return """match
  $p isa person, has name $n;
  (follower: $p, followed: $d) isa follows;
  (director: $d, film: $m) isa directed;
reduce $num_directors = count($d) groupby $n;
sort $num_directors desc;
limit 3;
fetch {
  "person": $n,
  "num_directors": $num_directors
};"""

    # ===== Query 521: 3 persons with highest total ratings =====
    if idx == 521:
        return """match
  $p isa person, has name $n;
  $rev (reviewer: $p, film: $m) isa reviewed, has rating $r;
reduce $totalRating = sum($r) groupby $n;
sort $totalRating desc;
limit 3;
fetch {
  "reviewer": $n,
  "totalRating": $totalRating
};"""

    # ===== Query 525: 3 directors who directed movies with most distinct taglines =====
    if idx == 525:
        return """match
  $d isa person, has name $n;
  (director: $d, film: $m) isa directed;
  $m has tagline $tag;
reduce $num_taglines = count($tag) groupby $n;
sort $num_taglines desc;
limit 3;
fetch {
  "director": $n,
  "num_taglines": $num_taglines
};"""

    # ===== Query 528: 3 persons who acted in movies with most distinct titles =====
    if idx == 528:
        return """match
  $p isa person, has name $n;
  (actor: $p, film: $m) isa acted_in;
  $m has title $t;
reduce $num_movies = count($t) groupby $n;
sort $num_movies desc;
limit 3;
fetch {
  "name": $n,
  "num_movies": $num_movies
};"""

    # ===== Query 532: 3 persons with most diverse roles =====
    if idx == 532:
        return """match
  $p isa person, has name $n;
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
reduce $num_roles = count($r) groupby $n;
sort $num_roles desc;
limit 3;
fetch {
  "person": $n,
  "number_of_distinct_roles": $num_roles
};"""

    # ===== Query 536: Top 3 producers by movies with different taglines =====
    if idx == 536:
        return """match
  $p isa person, has name $n;
  (producer: $p, film: $m) isa produced;
  $m has tagline $tag;
reduce $num_taglines = count($tag) groupby $n;
sort $num_taglines desc;
limit 3;
fetch {
  "producer": $n,
  "num_taglines": $num_taglines
};"""

    # ===== Query 538: 3 persons who reviewed most movies released before 2000 =====
    if idx == 538:
        return """match
  $p isa person, has name $n;
  (reviewer: $p, film: $m) isa reviewed;
  $m has released $r;
  $r < 2000;
reduce $numReviews = count($m) groupby $n;
sort $numReviews desc;
limit 3;
fetch {
  "reviewer": $n
};"""

    # ===== Query 585: Top 3 producers of movies released before 1990 =====
    if idx == 585:
        return """match
  $p isa person, has name $n;
  (producer: $p, film: $m) isa produced;
  $m has released $r;
  $r < 1990;
reduce $num_movies = count($m) groupby $n;
sort $num_movies desc;
limit 3;
fetch {
  "producer": $n,
  "num_movies": $num_movies
};"""

    # ===== Query 591: Who directed most movies with release after 1995 =====
    if idx == 591:
        return """match
  $d isa person, has name $n;
  (director: $d, film: $m) isa directed;
  $m has released $r;
  $r > 1995;
reduce $num_movies = count($m) groupby $n;
sort $num_movies desc;
limit 1;
fetch {
  "director": $n,
  "num_movies": $num_movies
};"""

    # ===== Query 595: First 3 movies with released year divisible by 10 and votes over 500 =====
    if idx == 595:
        return """match
  $m isa movie, has title $t, has released $r, has votes $v;
  $r in [1970, 1980, 1990, 2000, 2010, 2020];
  $v > 500;
sort $r asc;
limit 3;
fetch {
  "title": $t
};"""

    # ===== Query 600: Persons who reviewed most movies with rating over 75 =====
    if idx == 600:
        return """match
  $p isa person, has name $n;
  $rev (reviewer: $p, film: $m) isa reviewed, has rating $r;
  $r > 75;
reduce $numReviews = count($m) groupby $n;
sort $numReviews desc;
fetch {
  "reviewer": $n,
  "numReviews": $numReviews
};"""

    # ===== Query 604: Followers of persons who directed more than 3 movies =====
    if idx == 604:
        return """match
  $director isa person, has name $dn;
  (director: $director, film: $movie) isa directed;
  (follower: $follower, followed: $director) isa follows;
  $follower has name $fn;
reduce $num_directed_movies = count($movie) groupby $dn, $fn;
match $num_directed_movies > 3;
fetch {
  "director": $dn,
  "follower": $fn
};"""

    # ===== Query 610: Persons who directed a movie with released year divisible by 20 =====
    if idx == 610:
        return """match
  $p isa person, has name $n;
  (director: $p, film: $m) isa directed;
  $m has title $t, has released $r;
  $r in [1980, 2000, 2020];
fetch {
  "director": $n,
  "movie": $t,
  "year": $r
};"""

    # ===== Query 622: Top 3 persons with most roles =====
    if idx == 622:
        return """match
  $p isa person, has name $n;
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
reduce $role_count = count($r) groupby $n;
sort $role_count desc;
limit 3;
fetch {
  "person": $n,
  "role_count": $role_count
};"""

    # ===== Query 639: Who followed most people born after 1980 =====
    if idx == 639:
        return """match
  $follower isa person, has name $n;
  (follower: $follower, followed: $followed) isa follows;
  $followed has born $b;
  $b > 1980;
reduce $num_followed = count($followed) groupby $n;
sort $num_followed desc;
limit 1;
fetch {
  "follower": $n,
  "num_followed": $num_followed
};"""

    # ===== Query 650: Actors born after 1980 who acted in more than one movie =====
    if idx == 650:
        return """match
  $p isa person, has name $n, has born $b;
  $b > 1980;
  (actor: $p, film: $m) isa acted_in;
reduce $numMovies = count($m) groupby $n;
match $numMovies > 1;
fetch {
  "actor": $n,
  "numMovies": $numMovies
};"""

    # ===== Query 660: Top 3 actors in terms of diversity of roles =====
    if idx == 660:
        return """match
  $p isa person, has name $n;
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
reduce $num_roles = count($r) groupby $n;
sort $num_roles desc;
limit 3;
fetch {
  "actor": $n,
  "number_of_distinct_roles": $num_roles
};"""

    # ===== Query 669: Top 5 movies with most diverse cast in terms of roles =====
    if idx == 669:
        return """match
  $m isa movie, has title $t;
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
reduce $role_diversity = count($r) groupby $t;
sort $role_diversity desc;
limit 5;
fetch {
  "movie": $t,
  "role_diversity": $role_diversity
};"""

    # ===== Query 672: Top 3 reviewers by average rating given =====
    if idx == 672:
        return """match
  $p isa person, has name $n;
  $rev (reviewer: $p, film: $m) isa reviewed, has rating $r;
reduce $avg_rating = mean($r) groupby $n;
sort $avg_rating desc;
limit 3;
fetch {
  "reviewer": $n,
  "avg_rating": $avg_rating
};"""

    # ===== Query 696: Who has highest average review rating =====
    if idx == 696:
        return """match
  $p isa person, has name $n;
  $rev (reviewer: $p, film: $m) isa reviewed, has rating $r;
reduce $avg_rating = mean($r) groupby $n;
sort $avg_rating desc;
limit 1;
fetch {
  "name": $n,
  "avg_rating": $avg_rating
};"""

    # ===== Query 699: Top 3 actors from 1990 to 2000 =====
    if idx == 699:
        return """match
  $p isa person, has name $n;
  (actor: $p, film: $m) isa acted_in;
  $m has released $r;
  $r >= 1990;
  $r <= 2000;
reduce $numMovies = count($m) groupby $n;
sort $numMovies desc;
limit 3;
fetch {
  "actor": $n,
  "numMovies": $numMovies
};"""

    # ===== Query 700: First 3 movies with released year divisible by 10 =====
    if idx == 700:
        return """match
  $m isa movie, has title $t, has released $r;
  $r in [1970, 1980, 1990, 2000, 2010, 2020];
limit 3;
fetch {
  "title": $t
};"""

    # ===== Query 705: Top 5 people born before 1960 who produced more than one movie =====
    if idx == 705:
        return """match
  $p isa person, has name $n, has born $b;
  $b < 1960;
  (producer: $p, film: $m) isa produced;
reduce $num_movies = count($m) groupby $n;
match $num_movies > 1;
sort $num_movies desc;
limit 5;
fetch {
  "name": $n,
  "num_movies": $num_movies
};"""

    # ===== Query 714: Top 5 actors with diverse roles who acted in at least 5 movies =====
    if idx == 714:
        return """match
  $p isa person, has name $n;
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
reduce $num_movies = count($m) groupby $n;
match $num_movies >= 5;
reduce $num_roles = count($n);
sort $num_roles desc;
limit 5;
fetch {
  "actor": $n,
  "number_of_distinct_roles": $num_roles
};"""

    # ===== Query 724: First 3 actors who acted in more than 3 movies with tagline containing 'limit' =====
    if idx == 724:
        return """match
  $p isa person, has name $n;
  (actor: $p, film: $m) isa acted_in;
  $m has tagline $tag;
  $tag like ".*limit.*";
reduce $movieCount = count($m) groupby $n;
match $movieCount > 3;
sort $movieCount desc;
limit 3;
fetch {
  "name": $n
};"""

    return None


def main():
    # Read failed queries
    failed_queries = []
    with open(FAILED_PATH, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            failed_queries.append(row)

    print(f"Total failed queries to process: {len(failed_queries)}")

    # Read existing successful queries
    existing_queries = []
    try:
        with open(OUTPUT_PATH, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_queries.append(row)
    except FileNotFoundError:
        pass

    print(f"Existing successful queries: {len(existing_queries)}")

    # Connect to TypeDB
    credentials = Credentials("admin", "password")
    options = DriverOptions(is_tls_enabled=False)
    driver = TypeDB.driver("localhost:1729", credentials, options)

    converted = []
    still_failed = []

    for i, query in enumerate(failed_queries):
        original_index = int(query['original_index'])
        question = query['question']
        cypher = query['cypher']
        error = query.get('error', '')

        print(f"\n[{i+1}/{len(failed_queries)}] Processing query {original_index}: {question[:60]}...")

        # Try to convert
        typeql = convert_query(original_index, question, cypher)

        if typeql is None:
            print(f"  -> No conversion rule found")
            still_failed.append(query)
            continue

        # Validate
        valid, validation_error = validate_query(driver, typeql)

        if valid:
            print(f"  -> SUCCESS!")
            converted.append({
                'original_index': original_index,
                'question': question,
                'cypher': cypher,
                'typeql': typeql
            })
        else:
            print(f"  -> Validation failed: {validation_error[:100]}")
            query['error'] = f"TypeQL validation failed: {validation_error}"
            still_failed.append(query)

    driver.close()

    # Write results
    print(f"\n\nConversion complete!")
    print(f"Converted: {len(converted)}")
    print(f"Still failed: {len(still_failed)}")

    # Write successful queries (append to existing)
    all_queries = existing_queries + converted
    with open(OUTPUT_PATH, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
        writer.writeheader()
        for row in all_queries:
            writer.writerow(row)

    # Write failed queries
    with open(FAILED_PATH, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'error'])
        writer.writeheader()
        for row in still_failed:
            writer.writerow(row)

    print(f"\nTotal successful queries now: {len(all_queries)}")
    print(f"Remaining failed queries: {len(still_failed)}")


if __name__ == "__main__":
    main()
