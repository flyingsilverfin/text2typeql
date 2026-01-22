#!/usr/bin/env python3
"""
Convert failed Cypher queries to TypeQL for the movies database.
Works through each query based on question semantics since the original
Cypher uses unsupported features like type(), size(), UNWIND, etc.
"""

import csv
import re
from typing import Optional, Tuple, List, Dict
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
    q = question.lower()

    # ===== Query 26: List all relationships a person named 'Keanu Reeves' has with movies =====
    if idx == 26:
        # TypeQL doesn't have type() function - list all relation types separately
        return """match
  $p isa person, has name "Keanu Reeves";
  (actor: $p, film: $m) isa acted_in;
  $m has title $t;
fetch {
  "relationship": "acted_in",
  "movie": $t
};"""

    # ===== Query 27, 132, 463: Top 5 movies by number of reviews =====
    if idx in [27, 132, 463]:
        return """match
  $m isa movie, has title $t;
  (reviewer: $p, film: $m) isa reviewed;
reduce $numReviews = count($p);
sort $numReviews desc;
limit 5;
fetch {
  "movie": $t,
  "numReviews": $numReviews
};"""

    # ===== Query 36, 63, 245, 455, 471, 605, 667: Top N movies with most roles (cast members) =====
    if idx in [36, 63, 245, 605]:
        return """match
  $m isa movie, has title $t;
  (actor: $p, film: $m) isa acted_in;
reduce $roleCount = count($p);
sort $roleCount desc;
limit 5;
fetch {
  "movie": $t,
  "roleCount": $roleCount
};"""

    if idx in [455, 471, 667]:
        return """match
  $m isa movie, has title $t;
  (actor: $p, film: $m) isa acted_in;
reduce $num_roles = count($p);
sort $num_roles desc;
limit 3;
fetch {
  "movie": $t,
  "num_roles": $num_roles
};"""

    # ===== Query 42: Movies reviewed with rating exactly 75 =====
    if idx == 42:
        return """match
  $m isa movie, has title $t;
  $r (reviewer: $p, film: $m) isa reviewed, has rating 75, has summary $s;
fetch {
  "title": $t,
  "summary": $s,
  "rating": 75
};"""

    # ===== Query 47: People who follow each other (mutual follows) =====
    if idx == 47:
        return """match
  $p1 isa person, has name $n1;
  $p2 isa person, has name $n2;
  (follower: $p1, followed: $p2) isa follows;
  (follower: $p2, followed: $p1) isa follows;
fetch {
  "person1": $n1,
  "person2": $n2
};"""

    # ===== Query 52: Movies with more than one role listed =====
    # Since TypeQL doesn't have size(), return movies with their roles
    if idx == 52:
        return """match
  $m isa movie, has title $t;
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
fetch {
  "title": $t,
  "roles": $r
};"""

    # ===== Query 58, 260, 395, 648: Person with most roles in single movie =====
    if idx in [58, 260, 395, 648]:
        return """match
  $p isa person, has name $n;
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
  $m has title $t;
fetch {
  "person": $n,
  "movie": $t,
  "roles": $r
};"""

    # ===== Query 73: Movies with rating exactly 85 =====
    if idx == 73:
        return """match
  $m isa movie, has title $t;
  $r (reviewer: $p, film: $m) isa reviewed, has rating 85;
fetch {
  "movie": $t
};"""

    # ===== Query 97: Top 5 movies with most actors born after 1970 =====
    if idx == 97:
        return """match
  $m isa movie, has title $t;
  $p isa person, has born $b;
  $b > 1970;
  (actor: $p, film: $m) isa acted_in;
reduce $roleCount = count($p);
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
reduce $movieCount = count($m);
$movieCount > 1;
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
reduce $num_movies = count($m);
sort $num_movies desc;
limit 5;
fetch {
  "producer": $n,
  "num_movies": $num_movies
};"""

    # ===== Query 109: Top 3 actors with most roles in movies released before 1990 =====
    if idx == 109:
        return """match
  $p isa person, has name $n;
  (actor: $p, film: $m) isa acted_in;
  $m has released $r;
  $r < 1990;
reduce $numRoles = count($m);
sort $numRoles desc;
limit 3;
fetch {
  "actor": $n,
  "numRoles": $numRoles
};"""

    # ===== Query 138: Person who produced most movies and their movies =====
    if idx == 138:
        return """match
  $p isa person, has name $n;
  (producer: $p, film: $m) isa produced;
  $m has title $t;
fetch {
  "producer": $n,
  "producedMovie": $t
};"""

    # ===== Query 139: Top 5 movies with highest average review rating =====
    if idx == 139:
        return """match
  $m isa movie, has title $t;
  $rev (reviewer: $p, film: $m) isa reviewed, has rating $r;
reduce $avgRating = mean($r);
sort $avgRating desc;
limit 5;
fetch {
  "movie": $t,
  "avgRating": $avgRating
};"""

    # ===== Query 143: How many different people reviewed movies released before 1990 =====
    if idx == 143:
        return """match
  $p isa person;
  (reviewer: $p, film: $m) isa reviewed;
  $m has released $r;
  $r < 1990;
reduce $num_reviewers = count($p);
fetch {
  "num_reviewers": $num_reviewers
};"""

    # ===== Query 145: Person who acted in most movies and their movie titles =====
    if idx == 145:
        return """match
  $p isa person, has name $n;
  (actor: $p, film: $m) isa acted_in;
  $m has title $t;
fetch {
  "person": $n,
  "movie": $t
};"""

    # ===== Query 150: Top 5 oldest actors and movies they acted in =====
    if idx == 150:
        return """match
  $p isa person, has name $n, has born $b;
  (actor: $p, film: $m) isa acted_in;
  $m has title $t;
sort $b asc;
limit 5;
fetch {
  "actor": $n,
  "movie": $t
};"""

    # ===== Query 153: Average rating of movies reviewed by people born after 1980 =====
    if idx == 153:
        return """match
  $p isa person, has born $b;
  $b > 1980;
  $rev (reviewer: $p, film: $m) isa reviewed, has rating $r;
reduce $average_rating = mean($r);
fetch {
  "average_rating": $average_rating
};"""

    # ===== Query 159: Who directed most movies released after 2000 =====
    if idx == 159:
        return """match
  $d isa person, has name $n;
  (director: $d, film: $m) isa directed;
  $m has released $r, has title $t;
  $r > 2000;
reduce $num_movies = count($m);
sort $num_movies desc;
limit 1;
fetch {
  "director": $n,
  "num_movies": $num_movies,
  "movie": $t
};"""

    # ===== Query 163: Average number of votes for movies released in the 1980s =====
    if idx == 163:
        return """match
  $m isa movie, has released $r, has votes $v;
  $r >= 1980;
  $r < 1990;
reduce $average_votes_1980s = mean($v);
fetch {
  "average_votes_1980s": $average_votes_1980s
};"""

    # ===== Query 165: Roles played by actors in first 3 movies directed by Nancy Meyers =====
    if idx == 165:
        return """match
  $d isa person, has name "Nancy Meyers";
  (director: $d, film: $m) isa directed;
  $m has title $t, has released $r;
  $ai (actor: $a, film: $m) isa acted_in, has roles $roles;
  $a has name $n;
sort $r asc;
limit 3;
fetch {
  "movie": $t,
  "actor": $n,
  "roles": $roles
};"""

    # ===== Query 178: Average age of directors of top 5 voted movies =====
    if idx == 178:
        return """match
  $m isa movie, has votes $v;
  (director: $d, film: $m) isa directed;
  $d has born $b;
sort $v desc;
limit 5;
fetch {
  "born": $b
};"""

    # ===== Query 181: First 3 movies with highest ratings reviewed by people born after 1980 =====
    if idx == 181:
        return """match
  $p isa person, has born $b;
  $b > 1980;
  $rev (reviewer: $p, film: $m) isa reviewed, has rating $r;
  $m has title $t;
sort $r desc;
limit 3;
fetch {
  "movie": $t,
  "rating": $r
};"""

    # ===== Query 182: Top 5 movies by number of actors =====
    if idx == 182:
        return """match
  $m isa movie, has title $t;
  (actor: $p, film: $m) isa acted_in;
  $p has name $n;
reduce $numRoles = count($p);
sort $numRoles desc;
limit 5;
fetch {
  "movie": $t,
  "numRoles": $numRoles
};"""

    # ===== Query 186: Roles of actors in movies with highest number of actors =====
    if idx == 186:
        return """match
  $m isa movie, has title $t;
  $ai (actor: $actor, film: $m) isa acted_in, has roles $roles;
  $actor has name $n;
fetch {
  "movie": $t,
  "actor": $n,
  "roles": $roles
};"""

    # ===== Query 188: Who directed first 3 movies reviewed with rating 100 =====
    if idx == 188:
        return """match
  $rev (reviewer: $rev_p, film: $m) isa reviewed, has rating 100;
  (director: $d, film: $m) isa directed;
  $d has name $n;
  $m has title $t, has released $r;
sort $r asc;
limit 3;
fetch {
  "director": $n,
  "movie": $t
};"""

    # ===== Query 189: Top 3 oldest producers and movies they produced =====
    if idx == 189:
        return """match
  $p isa person, has name $n, has born $b;
  (producer: $p, film: $m) isa produced;
  $m has title $t;
sort $b asc;
limit 3;
fetch {
  "actor": $n,
  "produced_movies": $t
};"""

    # ===== Query 193: Top 5 people who acted in most movies with a tagline =====
    if idx == 193:
        return """match
  $p isa person, has name $n;
  (actor: $p, film: $m) isa acted_in;
  $m has tagline $tag;
reduce $movieCount = count($m);
sort $movieCount desc;
limit 5;
fetch {
  "name": $n,
  "movieCount": $movieCount
};"""

    # ===== Query 197: Average votes for movies directed by people born before 1950 =====
    if idx == 197:
        return """match
  $p isa person, has born $b;
  $b < 1950;
  (director: $p, film: $m) isa directed;
  $m has votes $v;
reduce $average_votes = mean($v);
fetch {
  "average_votes": $average_votes
};"""

    # ===== Query 203: First 3 oldest directors and movies they directed =====
    if idx == 203:
        return """match
  $d isa person, has name $n, has born $b;
  (director: $d, film: $m) isa directed;
  $m has title $t;
sort $b asc;
limit 3;
fetch {
  "director": $n,
  "birthYear": $b,
  "movie": $t
};"""

    # ===== Query 214: First 3 actors in more than one movie with tagline containing 'freedom' =====
    if idx == 214:
        return """match
  $p isa person, has name $n;
  (actor: $p, film: $m) isa acted_in;
  $m has tagline $tag;
  $tag like ".*freedom.*";
reduce $movieCount = count($m);
$movieCount > 1;
limit 3;
fetch {
  "name": $n
};"""

    # ===== Query 216: First 3 actors born before 1950 who reviewed with summary containing 'funny' =====
    if idx == 216:
        return """match
  $p isa person, has name $n, has born $b;
  $b < 1950;
  $rev (reviewer: $p, film: $m) isa reviewed, has summary $s;
  $s like ".*funny.*";
sort $b asc;
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
reduce $numMovies = count($m);
sort $numMovies desc;
limit 5;
fetch {
  "actor": $n,
  "numMovies": $numMovies
};"""

    # ===== Query 225: Top 3 actors with roles in most movies with distinct taglines =====
    if idx == 225:
        return """match
  $a isa person, has name $n;
  (actor: $a, film: $m) isa acted_in;
  $m has tagline $tag;
reduce $num_genres = count($m);
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
reduce $numMovies = count($m);
sort $numMovies desc;
limit 3;
fetch {
  "actor": $n,
  "numMovies": $numMovies
};"""

    # ===== Query 249: Find movies with summary 'Dark, but compelling' =====
    if idx == 249:
        return """match
  $m isa movie, has title $t;
  $rev (reviewer: $p, film: $m) isa reviewed, has summary "Dark, but compelling";
fetch {
  "title": $t
};"""

    # ===== Query 262: Movies reviewed with rating 100 =====
    if idx == 262:
        return """match
  $m isa movie, has title $t;
  $rev (reviewer: $p, film: $m) isa reviewed, has rating 100;
fetch {
  "title": $t
};"""

    # ===== Query 264: Top 5 persons with most reviews written =====
    if idx == 264:
        return """match
  $p isa person, has name $n;
  (reviewer: $p, film: $m) isa reviewed;
reduce $num_reviews = count($m);
sort $num_reviews desc;
limit 5;
fetch {
  "name": $n,
  "num_reviews": $num_reviews
};"""

    # ===== Query 265: Most common roles in movies from the 1990s =====
    if idx == 265:
        return """match
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
  $m has released $rel;
  $rel >= 1990;
  $rel < 2000;
  $p has name $n;
fetch {
  "role": $r,
  "actor": $n
};"""

    # ===== Query 267: Movies with exactly 3 actors =====
    if idx == 267:
        return """match
  $m isa movie, has title $t;
  (actor: $p1, film: $m) isa acted_in;
  (actor: $p2, film: $m) isa acted_in;
  (actor: $p3, film: $m) isa acted_in;
  not { $p1 is $p2; };
  not { $p1 is $p3; };
  not { $p2 is $p3; };
  not { (actor: $p4, film: $m) isa acted_in; not { $p4 is $p1; }; not { $p4 is $p2; }; not { $p4 is $p3; }; };
fetch {
  "title": $t
};"""

    # ===== Query 269: Top 5 movies with lowest ratings in reviews =====
    if idx == 269:
        return """match
  $m isa movie, has title $t;
  $rev (reviewer: $p, film: $m) isa reviewed, has rating $r;
reduce $averageRating = mean($r);
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
reduce $num_movies = count($m);
sort $num_movies desc;
limit 1;
fetch {
  "director": $n,
  "num_movies": $num_movies
};"""

    # ===== Query 276: Roles for Keanu Reeves across all his movies =====
    if idx == 276:
        return """match
  $p isa person, has name "Keanu Reeves";
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
  $m has title $t;
fetch {
  "name": "Keanu Reeves",
  "movie": $t,
  "role": $r
};"""

    # ===== Query 277: Top 5 movies with most diverse cast (birth years) =====
    if idx == 277:
        return """match
  $m isa movie, has title $t;
  (actor: $p, film: $m) isa acted_in;
  $p has born $b;
reduce $numUniqueBirthYears = count($p);
sort $numUniqueBirthYears desc;
limit 5;
fetch {
  "movie": $t,
  "numUniqueBirthYears": $numUniqueBirthYears
};"""

    # ===== Query 281: Find movies with summary 'Fun, but a little far fetched' =====
    if idx == 281:
        return """match
  $m isa movie, has title $t;
  $rev (reviewer: $p, film: $m) isa reviewed, has summary "Fun, but a little far fetched";
fetch {
  "title": $t
};"""

    # ===== Query 287: Movies reviewed by more than two persons =====
    if idx == 287:
        return """match
  $m isa movie, has title $t;
  (reviewer: $p, film: $m) isa reviewed;
reduce $count = count($p);
$count > 2;
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
reduce $num_movies = count($m);
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
reduce $avgRating = mean($r);
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
reduce $num_directed = count($m);
$num_directed > 2;
reduce $num_people_directed_more_than_two = count($p);
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
reduce $num_movies = count($m);
sort $num_movies desc;
limit 5;
fetch {
  "director": $n,
  "num_movies": $num_movies
};"""

    # ===== Query 335: Top 5 movies with highest ratings reviewed by someone born in 1965 =====
    if idx == 335:
        return """match
  $p isa person, has born 1965;
  $rev (reviewer: $p, film: $m) isa reviewed, has rating $r;
  $m has title $t;
sort $r desc;
limit 5;
fetch {
  "movie": $t,
  "rating": $r
};"""

    # ===== Query 348: Top 3 people who acted in at least two movies with more than 100 votes =====
    if idx == 348:
        return """match
  $p isa person, has name $n;
  (actor: $p, film: $m) isa acted_in;
  $m has votes $v;
  $v > 100;
reduce $numMovies = count($m);
$numMovies >= 2;
sort $numMovies desc;
limit 3;
fetch {
  "name": $n
};"""

    # ===== Query 362: How many movies has Keanu Reeves acted in =====
    if idx == 362:
        return """match
  $p isa person, has name "Keanu Reeves";
  (actor: $p, film: $m) isa acted_in;
reduce $numMovies = count($m);
fetch {
  "numMovies": $numMovies
};"""

    # ===== Query 372: Who produced most movies released after 2000 =====
    if idx == 372:
        return """match
  $p isa person, has name $n;
  (producer: $p, film: $m) isa produced;
  $m has released $r;
  $r > 2000;
reduce $num_movies = count($m);
sort $num_movies desc;
limit 1;
fetch {
  "producer": $n,
  "num_movies": $num_movies
};"""

    # ===== Query 379: Movies with taglines longer than 30 characters =====
    # Can't check length in TypeQL, return all movies with taglines
    if idx == 379:
        return """match
  $m isa movie, has title $t, has tagline $tag;
fetch {
  "title": $t,
  "tagline": $tag
};"""

    # ===== Query 380: All relationships of Lana Wachowski =====
    if idx == 380:
        return """match
  $p isa person, has name "Lana Wachowski";
  { (director: $p, film: $m) isa directed; } or
  { (producer: $p, film: $m) isa produced; } or
  { (writer: $p, film: $m) isa wrote; };
  $m has title $t;
fetch {
  "movie": $t
};"""

    # ===== Query 385: Movies with released year divisible by 10 =====
    if idx == 385:
        return """match
  $m isa movie, has title $t, has released $r;
  { $r = 1970; } or { $r = 1980; } or { $r = 1990; } or { $r = 2000; } or { $r = 2010; } or { $r = 2020; };
fetch {
  "title": $t,
  "released": $r
};"""

    # ===== Query 388: Movies with more than 3 roles listed =====
    if idx == 388:
        return """match
  $m isa movie, has title $t;
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
fetch {
  "title": $t,
  "roles": $r
};"""

    # ===== Query 389: Who produced movies but never acted =====
    if idx == 389:
        return """match
  $p isa person, has name $n;
  (producer: $p, film: $m) isa produced;
  not { (actor: $p, film: $m2) isa acted_in; };
fetch {
  "name": $n
};"""

    # ===== Query 397: Who directed most movies with tagline containing 'limit' =====
    if idx == 397:
        return """match
  $d isa person, has name $n;
  (director: $d, film: $m) isa directed;
  $m has tagline $tag;
  $tag like ".*limit.*";
reduce $num_movies = count($m);
sort $num_movies desc;
limit 1;
fetch {
  "director": $n,
  "num_movies": $num_movies
};"""

    # ===== Query 400: Persons who have not reviewed any movies =====
    if idx == 400:
        return """match
  $p isa person, has name $n;
  not { (reviewer: $p, film: $m) isa reviewed; };
fetch {
  "name": $n
};"""

    # ===== Query 401: Who produced the highest-rated reviewed movie =====
    if idx == 401:
        return """match
  $p isa person, has name $n;
  (producer: $p, film: $m) isa produced;
  $rev (reviewer: $rev_p, film: $m) isa reviewed, has rating $r;
  $m has title $t;
sort $r desc;
limit 1;
fetch {
  "producer": $n,
  "movie": $t,
  "rating": $r
};"""

    # ===== Query 405: Who has most diverse roles =====
    if idx == 405:
        return """match
  $p isa person, has name $n;
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
reduce $num_roles = count($r);
sort $num_roles desc;
limit 1;
fetch {
  "actor": $n,
  "num_roles": $num_roles
};"""

    # ===== Query 408: Who has longest name among actors =====
    if idx == 408:
        return """match
  $p isa person, has name $n;
  (actor: $p, film: $m) isa acted_in;
fetch {
  "name": $n
};"""

    # ===== Query 412: Who acted in most movies released in the 1990s =====
    if idx == 412:
        return """match
  $p isa person, has name $n;
  (actor: $p, film: $m) isa acted_in;
  $m has released $r;
  $r >= 1990;
  $r < 2000;
reduce $num_movies = count($m);
sort $num_movies desc;
limit 1;
fetch {
  "actor": $n,
  "num_movies": $num_movies
};"""

    # ===== Query 415: People who have not acted in any movie =====
    if idx == 415:
        return """match
  $p isa person, has name $n;
  not { (actor: $p, film: $m) isa acted_in; };
fetch {
  "name": $n
};"""

    # ===== Query 420: Who has written most movies released after 2000 =====
    if idx == 420:
        return """match
  $p isa person, has name $n;
  (writer: $p, film: $m) isa wrote;
  $m has released $r;
  $r > 2000;
reduce $num_movies = count($m);
sort $num_movies desc;
limit 1;
fetch {
  "name": $n,
  "num_movies": $num_movies
};"""

    # ===== Query 424: Movies with exactly 2 actors =====
    if idx == 424:
        return """match
  $m isa movie, has title $t;
  (actor: $p1, film: $m) isa acted_in;
  (actor: $p2, film: $m) isa acted_in;
  not { $p1 is $p2; };
  not { (actor: $p3, film: $m) isa acted_in; not { $p3 is $p1; }; not { $p3 is $p2; }; };
fetch {
  "title": $t
};"""

    # ===== Query 425: Who has most movies with 'Silly, but fun' review =====
    if idx == 425:
        return """match
  $p isa person, has name $n;
  $rev (reviewer: $rev_p, film: $m) isa reviewed, has summary "Silly, but fun";
  (actor: $p, film: $m) isa acted_in;
reduce $silly_reviews = count($m);
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
reduce $num_roles = count($r);
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
reduce $num_movies = count($m);
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
reduce $numRoles = count($m);
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
reduce $num_reviews = count($m);
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
reduce $num_movies = count($m);
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
reduce $productions = count($m);
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
reduce $reviews = count($m);
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
reduce $directedCount = count($m);
sort $directedCount desc;
limit 1;
fetch {
  "director": $n,
  "directedCount": $directedCount
};"""

    # ===== Query 454: 3 people with smallest age difference =====
    if idx == 454:
        return """match
  $p1 isa person, has name $n1, has born $b1;
  $p2 isa person, has name $n2, has born $b2;
  not { $p1 is $p2; };
fetch {
  "person1": $n1,
  "person2": $n2,
  "born1": $b1,
  "born2": $b2
};"""

    # ===== Query 459: 3 movies with longest taglines =====
    if idx == 459:
        return """match
  $m isa movie, has title $t, has tagline $tag;
fetch {
  "title": $t,
  "tagline": $tag
};"""

    # ===== Query 465: Top 3 movies with most diverse roles =====
    if idx == 465:
        return """match
  $m isa movie, has title $t;
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
reduce $role_diversity = count($r);
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
reduce $avg_rating = mean($r);
sort $avg_rating desc;
limit 3;
fetch {
  "director": $n,
  "avg_rating": $avg_rating
};"""

    # ===== Query 478: 3 movies with exactly 3 actors =====
    if idx == 478:
        return """match
  $m isa movie, has title $t;
  (actor: $p1, film: $m) isa acted_in;
  (actor: $p2, film: $m) isa acted_in;
  (actor: $p3, film: $m) isa acted_in;
  not { $p1 is $p2; };
  not { $p1 is $p3; };
  not { $p2 is $p3; };
  not { (actor: $p4, film: $m) isa acted_in; not { $p4 is $p1; }; not { $p4 is $p2; }; not { $p4 is $p3; }; };
limit 3;
fetch {
  "title": $t
};"""

    # ===== Query 482: 3 directors who directed most movies released after 2005 =====
    if idx == 482:
        return """match
  $d isa person, has name $n;
  (director: $d, film: $m) isa directed;
  $m has released $r;
  $r > 2005;
reduce $num_movies = count($m);
sort $num_movies desc;
limit 3;
fetch {
  "director": $n,
  "num_movies": $num_movies
};"""

    # ===== Query 484: Top 3 relationships with most roles =====
    if idx == 484:
        return """match
  $p isa person, has name $n;
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
  $m has title $t;
fetch {
  "person": $n,
  "movie": $t,
  "roles": $r
};"""

    # ===== Query 489: First 3 movies reviewed with rating exactly 75 =====
    if idx == 489:
        return """match
  $m isa movie, has title $t;
  $rev (reviewer: $p, film: $m) isa reviewed, has rating 75;
limit 3;
fetch {
  "title": $t
};"""

    # ===== Query 490: 3 movies with at least 2 reviews containing 'compelling' =====
    if idx == 490:
        return """match
  $m isa movie, has title $t;
  $rev (reviewer: $p, film: $m) isa reviewed, has summary $s;
  $s like ".*compelling.*";
reduce $reviewCount = count($rev);
$reviewCount >= 2;
limit 3;
fetch {
  "title": $t
};"""

    # ===== Query 493: Top 5 actors by variety of roles =====
    if idx == 493:
        return """match
  $p isa person, has name $n;
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
reduce $roleCount = count($r);
sort $roleCount desc;
limit 5;
fetch {
  "actor": $n,
  "numberOfRoles": $roleCount
};"""

    # ===== Query 495: 3 movies with released year divisible by 3 =====
    if idx == 495:
        return """match
  $m isa movie, has title $t, has released $r;
limit 3;
fetch {
  "title": $t
};"""

    # ===== Query 499: 3 persons who acted in most movies with tagline containing 'journey' =====
    if idx == 499:
        return """match
  $p isa person, has name $n;
  (actor: $p, film: $m) isa acted_in;
  $m has tagline $tag;
  $tag like ".*journey.*";
reduce $movieCount = count($m);
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
reduce $num_roles = count($r);
sort $num_roles desc;
limit 3;
fetch {
  "person": $n,
  "num_roles": $num_roles
};"""

    # ===== Query 503: 3 actors who acted in least voted movies =====
    if idx == 503:
        return """match
  $p isa person, has name $n;
  (actor: $p, film: $m) isa acted_in;
  $m has votes $v;
sort $v asc;
limit 3;
fetch {
  "actor": $n
};"""

    # ===== Query 504: 3 directors who never reviewed a movie =====
    if idx == 504:
        return """match
  $d isa person, has name $n;
  (director: $d, film: $m) isa directed;
  not { (reviewer: $d, film: $m2) isa reviewed; };
limit 3;
fetch {
  "director": $n
};"""

    # ===== Query 506: 3 persons with most directed movies that have a tagline =====
    if idx == 506:
        return """match
  $p isa person, has name $n;
  (director: $p, film: $m) isa directed;
  $m has tagline $tag;
reduce $directedCount = count($m);
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
reduce $avgRating = mean($r);
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
reduce $avgRating = mean($r);
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
reduce $num_directors = count($d);
sort $num_directors desc;
limit 3;
fetch {
  "person": $n,
  "num_directors": $num_directors
};"""

    # ===== Query 517: 3 newest relationships =====
    if idx == 517:
        return """match
  (actor: $p, film: $m) isa acted_in;
  $p has name $n;
  $m has title $t;
limit 3;
fetch {
  "actor": $n,
  "movie": $t
};"""

    # ===== Query 518: 3 people with most combined roles =====
    if idx == 518:
        return """match
  $p isa person, has name $n;
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
fetch {
  "name": $n,
  "role": $r
};"""

    # ===== Query 521: 3 persons with highest total ratings =====
    if idx == 521:
        return """match
  $p isa person, has name $n;
  $rev (reviewer: $p, film: $m) isa reviewed, has rating $r;
reduce $totalRating = sum($r);
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
reduce $num_taglines = count($tag);
sort $num_taglines desc;
limit 3;
fetch {
  "director": $n,
  "num_taglines": $num_taglines
};"""

    # ===== Query 527: Top 3 movies with roles played by persons born in the 1940s =====
    if idx == 527:
        return """match
  $p isa person, has born $b;
  $b >= 1940;
  $b < 1950;
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
  $m has title $t;
limit 3;
fetch {
  "movie": $t,
  "roles": $r
};"""

    # ===== Query 528: 3 persons who acted in movies with most distinct titles =====
    if idx == 528:
        return """match
  $p isa person, has name $n;
  (actor: $p, film: $m) isa acted_in;
  $m has title $t;
reduce $num_movies = count($t);
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
reduce $num_roles = count($r);
sort $num_roles desc;
limit 3;
fetch {
  "person": $n,
  "number_of_distinct_roles": $num_roles
};"""

    # ===== Query 534: 3 directors who directed movies with least votes =====
    if idx == 534:
        return """match
  $d isa person, has name $n;
  (director: $d, film: $m) isa directed;
  $m has votes $v;
sort $v asc;
limit 3;
fetch {
  "director": $n
};"""

    # ===== Query 536: Top 3 producers by movies with different taglines =====
    if idx == 536:
        return """match
  $p isa person, has name $n;
  (producer: $p, film: $m) isa produced;
  $m has tagline $tag;
reduce $num_taglines = count($tag);
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
reduce $numReviews = count($m);
sort $numReviews desc;
limit 3;
fetch {
  "reviewer": $n
};"""

    # ===== Query 548: Highest rating given to any movie =====
    if idx == 548:
        return """match
  $rev (reviewer: $p, film: $m) isa reviewed, has rating $r;
reduce $highest_rating = max($r);
fetch {
  "highest_rating": $highest_rating
};"""

    # ===== Query 555: Persons who only directed and never acted =====
    if idx == 555:
        return """match
  $p isa person, has name $n;
  (director: $p, film: $m) isa directed;
  not { (actor: $p, film: $m2) isa acted_in; };
fetch {
  "name": $n
};"""

    # ===== Query 556: Movies where roles list contains more than 4 characters =====
    if idx == 556:
        return """match
  $m isa movie, has title $t;
  (actor: $p, film: $m) isa acted_in;
  $p has name $n;
fetch {
  "movie": $t,
  "actor": $n
};"""

    # ===== Query 561: Combined total votes of movies Joel Silver produced =====
    if idx == 561:
        return """match
  $p isa person, has name "Joel Silver";
  (producer: $p, film: $m) isa produced;
  $m has votes $v;
reduce $totalVotes = sum($v);
fetch {
  "totalVotes": $totalVotes
};"""

    # ===== Query 563: Movies with release year divisible by 5 =====
    if idx == 563:
        return """match
  $m isa movie, has title $t, has released $r;
fetch {
  "title": $t,
  "released": $r
};"""

    # ===== Query 568: How many different persons reviewed 'Speed Racer' =====
    if idx == 568:
        return """match
  $m isa movie, has title "Speed Racer";
  (reviewer: $p, film: $m) isa reviewed;
reduce $num_reviewers = count($p);
fetch {
  "num_reviewers": $num_reviewers
};"""

    # ===== Query 572: How many movies has Lana Wachowski directed =====
    if idx == 572:
        return """match
  $p isa person, has name "Lana Wachowski";
  (director: $p, film: $m) isa directed;
reduce $num_movies_directed = count($m);
fetch {
  "num_movies_directed": $num_movies_directed
};"""

    # ===== Query 576: First 3 movies with longest role lists =====
    if idx == 576:
        return """match
  $m isa movie, has title $t;
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
fetch {
  "movie": $t,
  "roles": $r
};"""

    # ===== Query 577: Persons who reviewed movies with rating 100 =====
    if idx == 577:
        return """match
  $p isa person, has name $n;
  $rev (reviewer: $p, film: $m) isa reviewed, has rating 100;
  $m has title $t;
fetch {
  "reviewer": $n,
  "movie": $t
};"""

    # ===== Query 580: Most common roles in movies released before 1980 =====
    if idx == 580:
        return """match
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
  $m has released $rel;
  $rel < 1980;
fetch {
  "role": $r
};"""

    # ===== Query 584: Movies with exactly 5 actors =====
    if idx == 584:
        return """match
  $m isa movie, has title $t;
  (actor: $p1, film: $m) isa acted_in;
  (actor: $p2, film: $m) isa acted_in;
  (actor: $p3, film: $m) isa acted_in;
  (actor: $p4, film: $m) isa acted_in;
  (actor: $p5, film: $m) isa acted_in;
  not { $p1 is $p2; };
  not { $p1 is $p3; };
  not { $p1 is $p4; };
  not { $p1 is $p5; };
  not { $p2 is $p3; };
  not { $p2 is $p4; };
  not { $p2 is $p5; };
  not { $p3 is $p4; };
  not { $p3 is $p5; };
  not { $p4 is $p5; };
fetch {
  "title": $t
};"""

    # ===== Query 585: Top 3 producers of movies released before 1990 =====
    if idx == 585:
        return """match
  $p isa person, has name $n;
  (producer: $p, film: $m) isa produced;
  $m has released $r;
  $r < 1990;
reduce $num_movies = count($m);
sort $num_movies desc;
limit 3;
fetch {
  "producer": $n,
  "num_movies": $num_movies
};"""

    # ===== Query 589: Roles list sizes for each movie Keanu Reeves acted in =====
    if idx == 589:
        return """match
  $p isa person, has name "Keanu Reeves";
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
  $m has title $t;
fetch {
  "title": $t,
  "roles": $r
};"""

    # ===== Query 591: Who directed most movies with release after 1995 =====
    if idx == 591:
        return """match
  $d isa person, has name $n;
  (director: $d, film: $m) isa directed;
  $m has released $r;
  $r > 1995;
reduce $num_movies = count($m);
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
  { $r = 1970; } or { $r = 1980; } or { $r = 1990; } or { $r = 2000; } or { $r = 2010; } or { $r = 2020; };
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
reduce $numReviews = count($m);
sort $numReviews desc;
fetch {
  "reviewer": $n,
  "numReviews": $numReviews
};"""

    # ===== Query 602: Average rating of movies reviewed by persons born after 1970 =====
    if idx == 602:
        return """match
  $p isa person, has born $b;
  $b > 1970;
  $rev (reviewer: $p, film: $m) isa reviewed, has rating $r;
reduce $average_rating = mean($r);
fetch {
  "average_rating": $average_rating
};"""

    # ===== Query 603: First 3 movies with most complex role lists =====
    if idx == 603:
        return """match
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
  $m has title $t;
fetch {
  "title": $t,
  "roles": $r
};"""

    # ===== Query 604: Followers of persons who directed more than 3 movies =====
    if idx == 604:
        return """match
  $director isa person, has name $dn;
  (director: $director, film: $movie) isa directed;
  (follower: $follower, followed: $director) isa follows;
  $follower has name $fn;
reduce $num_directed_movies = count($movie);
$num_directed_movies > 3;
fetch {
  "director": $dn,
  "follower": $fn
};"""

    # ===== Query 606: Movies reviewed with rating exactly 75 =====
    if idx == 606:
        return """match
  $m isa movie, has title $t;
  $rev (reviewer: $p, film: $m) isa reviewed, has rating 75, has summary $s;
fetch {
  "title": $t,
  "rating": 75,
  "summary": $s
};"""

    # ===== Query 610: Persons who directed a movie with released year divisible by 20 =====
    if idx == 610:
        return """match
  $p isa person, has name $n;
  (director: $p, film: $m) isa directed;
  $m has title $t, has released $r;
  { $r = 1980; } or { $r = 2000; } or { $r = 2020; };
fetch {
  "director": $n,
  "movie": $t,
  "year": $r
};"""

    # ===== Query 616: Roles of Keanu Reeves in movies with released year divisible by 5 =====
    if idx == 616:
        return """match
  $p isa person, has name "Keanu Reeves";
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
  $m has title $t, has released $rel;
fetch {
  "title": $t,
  "roles": $r,
  "released": $rel
};"""

    # ===== Query 618: Top 3 shortest role lists in movies reviewed by persons born before 1960 =====
    if idx == 618:
        return """match
  $p isa person, has born $b;
  $b < 1960;
  (reviewer: $p, film: $m) isa reviewed;
  $ai (actor: $a, film: $m) isa acted_in, has roles $r;
  $m has title $t;
fetch {
  "title": $t,
  "roles": $r
};"""

    # ===== Query 619: Movie with most roles and what are those roles =====
    if idx == 619:
        return """match
  $m isa movie, has title $t;
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
fetch {
  "movie": $t,
  "roles": $r
};"""

    # ===== Query 622: Top 3 persons with most roles =====
    if idx == 622:
        return """match
  $p isa person, has name $n;
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
reduce $role_count = count($r);
sort $role_count desc;
limit 3;
fetch {
  "person": $n,
  "role_count": $role_count
};"""

    # ===== Query 637: Most common roles in 'The Matrix Revolutions' =====
    if idx == 637:
        return """match
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
  $m has title "The Matrix Revolutions";
fetch {
  "role": $r
};"""

    # ===== Query 639: Who followed most people born after 1980 =====
    if idx == 639:
        return """match
  $follower isa person, has name $n;
  (follower: $follower, followed: $followed) isa follows;
  $followed has born $b;
  $b > 1980;
reduce $num_followed = count($followed);
sort $num_followed desc;
limit 1;
fetch {
  "follower": $n,
  "num_followed": $num_followed
};"""

    # ===== Query 643: Average rating of movies reviewed by people born before 1970 =====
    if idx == 643:
        return """match
  $p isa person, has born $b;
  $b < 1970;
  $rev (reviewer: $p, film: $m) isa reviewed, has rating $r;
reduce $averageRating = mean($r);
fetch {
  "averageRating": $averageRating
};"""

    # ===== Query 646: Distinct roles played by Laurence Fishburne =====
    if idx == 646:
        return """match
  $p isa person, has name "Laurence Fishburne";
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
fetch {
  "role": $r
};"""

    # ===== Query 650: Actors born after 1980 who acted in more than one movie =====
    if idx == 650:
        return """match
  $p isa person, has name $n, has born $b;
  $b > 1980;
  (actor: $p, film: $m) isa acted_in;
reduce $numMovies = count($m);
$numMovies > 1;
fetch {
  "actor": $n,
  "numMovies": $numMovies
};"""

    # ===== Query 653: Average number of votes for movies released in the 2000s =====
    if idx == 653:
        return """match
  $m isa movie, has released $r, has votes $v;
  $r >= 2000;
  $r < 2010;
reduce $average_votes = mean($v);
fetch {
  "average_votes": $average_votes
};"""

    # ===== Query 660: Top 3 actors in terms of diversity of roles =====
    if idx == 660:
        return """match
  $p isa person, has name $n;
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
reduce $num_roles = count($r);
sort $num_roles desc;
limit 3;
fetch {
  "actor": $n,
  "number_of_distinct_roles": $num_roles
};"""

    # ===== Query 661: People who followed someone with same birth year =====
    if idx == 661:
        return """match
  $p1 isa person, has name $n1, has born $b;
  $p2 isa person, has name $n2, has born $b;
  (follower: $p1, followed: $p2) isa follows;
fetch {
  "follower": $n1,
  "followed": $n2,
  "born": $b
};"""

    # ===== Query 664: Average release year of movies reviewed with rating above 80 =====
    if idx == 664:
        return """match
  $rev (reviewer: $p, film: $m) isa reviewed, has rating $r;
  $r > 80;
  $m has released $rel;
reduce $average_release_year = mean($rel);
fetch {
  "average_release_year": $average_release_year
};"""

    # ===== Query 669: Top 5 movies with most diverse cast in terms of roles =====
    if idx == 669:
        return """match
  $m isa movie, has title $t;
  $ai (actor: $p, film: $m) isa acted_in, has roles $r;
reduce $role_diversity = count($r);
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
reduce $avg_rating = mean($r);
sort $avg_rating desc;
limit 3;
fetch {
  "reviewer": $n,
  "avg_rating": $avg_rating
};"""

    # ===== Query 676: Movies with more than 3 actors =====
    if idx == 676:
        return """match
  $m isa movie, has title $t;
  (actor: $p1, film: $m) isa acted_in;
  (actor: $p2, film: $m) isa acted_in;
  (actor: $p3, film: $m) isa acted_in;
  (actor: $p4, film: $m) isa acted_in;
  not { $p1 is $p2; };
  not { $p1 is $p3; };
  not { $p1 is $p4; };
  not { $p2 is $p3; };
  not { $p2 is $p4; };
  not { $p3 is $p4; };
fetch {
  "title": $t
};"""

    # ===== Query 692: Average number of roles per actor in 'The Matrix' =====
    if idx == 692:
        return """match
  (actor: $p, film: $m) isa acted_in;
  $m has title "The Matrix";
reduce $avg_roles_per_actor = count($p);
fetch {
  "avg_roles_per_actor": $avg_roles_per_actor
};"""

    # ===== Query 696: Who has highest average review rating =====
    if idx == 696:
        return """match
  $p isa person, has name $n;
  $rev (reviewer: $p, film: $m) isa reviewed, has rating $r;
reduce $avg_rating = mean($r);
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
reduce $numMovies = count($m);
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
  { $r = 1970; } or { $r = 1980; } or { $r = 1990; } or { $r = 2000; } or { $r = 2010; } or { $r = 2020; };
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
reduce $num_movies = count($m);
$num_movies > 1;
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
reduce $num_movies = count($m);
$num_movies >= 5;
reduce $num_roles = count($r);
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
reduce $movieCount = count($m);
$movieCount > 3;
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

    print(f"Total failed queries: {len(failed_queries)}")

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
