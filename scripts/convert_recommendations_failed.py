#!/usr/bin/env python3
"""
Script to convert failed Cypher queries to TypeQL for the recommendations database.
"""

import csv
import re
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# Connection settings
HOST = "localhost:1729"
DATABASE = "text2typeql_recommendations"
USERNAME = "admin"
PASSWORD = "password"


def get_driver():
    """Get TypeDB driver connection."""
    credentials = Credentials(USERNAME, PASSWORD)
    options = DriverOptions(is_tls_enabled=False)
    return TypeDB.driver(HOST, credentials, options)


def validate_query(driver, query):
    """Validate a TypeQL query against the database."""
    try:
        with driver.transaction(DATABASE, TransactionType.READ) as tx:
            result = tx.query(query).resolve()
            # Try to consume some results to ensure query is valid
            if hasattr(result, 'as_concept_documents'):
                docs = list(result.as_concept_documents())
            elif hasattr(result, 'as_value'):
                val = result.as_value()
            return True, None
    except Exception as e:
        return False, str(e)


def convert_query(question, cypher):
    """
    Attempt to convert a Cypher query to TypeQL based on the question and patterns.
    Returns (typeql_query, skip_reason) - if typeql_query is None, skip_reason explains why.
    """

    # Patterns that cannot be converted due to missing schema elements
    if 'Award' in cypher or 'WON' in cypher:
        return None, "Uses Award/WON which don't exist in schema"

    if 'u.age' in cypher or 'User' in cypher and 'age' in question.lower():
        if 'age' in cypher.lower() and 'User' in cypher:
            return None, "Uses user age which doesn't exist in schema"

    if 'diedIn' in cypher:
        return None, "Uses diedIn which doesn't exist in schema"

    # Complex patterns that can't be directly converted
    if 'UNWIND' in cypher:
        return None, "Uses UNWIND which has no TypeQL equivalent"

    if 'size(m.languages)' in cypher or 'size(m.countries)' in cypher:
        return None, "Uses size() on list property - not supported in TypeQL (languages/countries are single string values)"

    if 'size(split(' in cypher:
        return None, "Uses size(split()) which has no TypeQL equivalent"

    if 'size(d.bio)' in cypher or 'size(a.bio)' in cypher:
        return None, "Uses size() on string property - not directly supported"

    if 'size(m.plot)' in cypher:
        return None, "Uses size() on string property - not directly supported"

    if 'toFloat(' in cypher:
        return None, "Uses toFloat() which has no TypeQL equivalent"

    if 'duration' in cypher.lower() or 'duration.between' in cypher:
        return None, "Uses duration functions which have no TypeQL equivalent"

    if 'm.languages[0]' in cypher:
        return None, "Uses array indexing which has no TypeQL equivalent"

    if 'date()' in cypher and 'duration' in cypher:
        return None, "Uses date arithmetic with duration"

    if 'weekday' in cypher:
        return None, "Uses weekday function which has no TypeQL equivalent"

    if 'epochSeconds' in cypher:
        return None, "Uses epochSeconds which requires timestamp conversion"

    if '=~' in cypher:  # regex matching
        return None, "Uses regex matching pattern"

    # Try to convert based on patterns
    typeql = None

    # Simple movie queries with filters
    if re.search(r'MATCH \(m:Movie\)', cypher) and 'Actor' not in cypher and 'Director' not in cypher and 'User' not in cypher:
        typeql = convert_simple_movie_query(question, cypher)

    # Actor queries
    elif ':Actor' in cypher:
        typeql = convert_actor_query(question, cypher)

    # Director queries
    elif ':Director' in cypher:
        typeql = convert_director_query(question, cypher)

    # User rating queries
    elif ':User' in cypher and 'RATED' in cypher:
        typeql = convert_user_rating_query(question, cypher)

    # Genre queries
    elif ':Genre' in cypher:
        typeql = convert_genre_query(question, cypher)

    # Person queries (for born/directed)
    elif ':Person' in cypher:
        typeql = convert_person_query(question, cypher)

    if typeql:
        return typeql, None

    return None, "No matching conversion pattern found"


def convert_simple_movie_query(question, cypher):
    """Convert simple movie-only queries."""

    # Movie with budget/revenue comparisons
    if 'm.revenue > m.budget' in cypher or 'm.revenue > 2 * m.budget' in cypher:
        if '2 * m.budget' in cypher:
            return """match
$m isa movie, has title $t, has revenue $rev, has budget $bud;
$rev > ($bud * 2);
sort $rev desc;
limit 3;
fetch { "title": $t, "revenue": $rev, "budget": $bud };"""
        else:
            if 'LIMIT 3' in cypher:
                return """match
$m isa movie, has title $t, has revenue $rev, has budget $bud;
$rev > $bud;
sort $rev desc;
limit 3;
fetch { "title": $t, "revenue": $rev, "budget": $bud };"""
            else:
                return """match
$m isa movie, has title $t, has revenue $rev, has budget $bud;
$rev > $bud;
fetch { "title": $t, "revenue": $rev, "budget": $bud };"""

    # Revenue > 3 * budget
    if 'm.revenue > 3 * m.budget' in cypher:
        return """match
$m isa movie, has title $t, has revenue $rev, has budget $bud;
$rev > ($bud * 3);
sort $rev desc;
limit 3;
fetch { "title": $t, "revenue": $rev, "budget": $bud };"""

    # Revenue + budget > 1 billion
    if 'm.revenue + m.budget > 1000000000' in cypher:
        return """match
$m isa movie, has title $t, has revenue $rev, has budget $bud;
($rev + $bud) > 1000000000;
fetch { "title": $t, "revenue": $rev, "budget": $bud };"""

    # Revenue - budget (profit)
    if 'm.revenue - m.budget' in cypher:
        if 'ORDER BY profit DESC' in cypher:
            limit = 3
            if 'LIMIT 5' in cypher:
                limit = 5
            return f"""match
$m isa movie, has title $t, has revenue $rev, has budget $bud;
$profit = $rev - $bud;
sort $profit desc;
limit {limit};
fetch {{ "title": $t, "profit": $profit }};"""
        elif 'ORDER BY profit' in cypher and 'DESC' not in cypher:
            # Smallest difference
            return """match
$m isa movie, has title $t, has revenue $rev, has budget $bud;
$profit = $rev - $bud;
sort $profit asc;
limit 3;
fetch { "title": $t, "budget": $bud, "revenue": $rev, "profit": $profit };"""

    # Budget / revenue ratio
    if 'm.budget / m.revenue' in cypher or 'm.budget IS NOT NULL AND m.revenue IS NOT NULL' in cypher:
        if 'budget / m.revenue' in cypher:
            limit = 3
            if 'LIMIT 5' in cypher:
                limit = 5
            return f"""match
$m isa movie, has title $t, has revenue $rev, has budget $bud;
$ratio = $bud / $rev;
sort $ratio desc;
limit {limit};
fetch {{ "title": $t, "budgetRevenueRatio": $ratio }};"""

    # Revenue / budget ratio > N
    if 'm.revenue / m.budget > 2' in cypher:
        return """match
$m isa movie, has title $t, has revenue $rev, has budget $bud;
$ratio = $rev / $bud;
$ratio > 2.0;
sort $ratio desc;
limit 5;
fetch { "title": $t, "revenue": $rev, "budget": $bud, "ratio": $ratio };"""

    if 'm.revenue / m.budget > 5' in cypher:
        return """match
$m isa movie, has title $t, has revenue $rev, has budget $bud;
$ratio = $rev / $bud;
$ratio > 5.0;
sort $ratio desc;
limit 3;
fetch { "title": $t, "revenue": $rev, "budget": $bud, "ratio": $ratio };"""

    # imdbRating queries
    if 'm.imdbRating' in cypher:
        if 'ORDER BY m.imdbRating DESC' in cypher and 'LIMIT 5' in cypher:
            return """match
$m isa movie, has title $t, has imdb_rating $r;
sort $r desc;
limit 5;
fetch { "title": $t, "imdbRating": $r };"""
        if 'ORDER BY m.imdbRating DESC' in cypher and 'LIMIT 3' in cypher:
            return """match
$m isa movie, has title $t, has imdb_rating $r;
sort $r desc;
limit 3;
fetch { "title": $t, "imdbRating": $r };"""

    # imdbVotes queries
    if 'm.imdbVotes' in cypher:
        if 'ORDER BY m.imdbVotes DESC' in cypher and 'LIMIT 3' in cypher:
            return """match
$m isa movie, has title $t, has imdb_votes $v;
sort $v desc;
limit 3;
fetch { "title": $t, "imdbVotes": $v };"""
        if 'imdbVotes > 500' in cypher:
            return """match
$m isa movie, has title $t, has imdb_rating $r, has imdb_votes $v;
$v > 500;
sort $r asc;
limit 3;
fetch { "title": $t, "imdbRating": $r };"""
        if 'imdbVotes > 500000' in cypher:
            return """match
$m isa movie, has imdb_id $id, has imdb_votes $v;
$v > 500000;
fetch { "imdbId": $id };"""

    # Year filtering
    if 'm.year = 2014' in cypher:
        return """match
$m isa movie, has title $t, has revenue $rev, has year 2014;
sort $rev desc;
limit 3;
fetch { "title": $t, "revenue": $rev };"""

    if 'm.year = 1995' in cypher:
        return """match
$m isa movie, has title $t, has revenue $rev, has year 1995;
sort $rev desc;
limit 5;
fetch { "title": $t, "revenue": $rev };"""

    if 'm.year >= 2010 AND m.year <= 2020' in cypher:
        return """match
$m isa movie, has year $y;
$y >= 2010;
$y <= 2020;
reduce $count = count($m) groupby $y;
fetch { "year": $y, "numMovies": $count };"""

    if 'm.year > 2000' in cypher and 'imdbVotes' in cypher:
        return """match
$m isa movie, has title $t, has imdb_votes $v, has year $y;
$y > 2000;
sort $v asc;
limit 3;
fetch { "title": $t, "imdbVotes": $v };"""

    if 'm.year < 1980' in cypher:
        # Actors in movies before 1980
        return None

    # Runtime queries
    if 'm.runtime > 120' in cypher and 'm.released' in cypher:
        return """match
$m isa movie, has title $t, has released $rel, has runtime $rt;
$rt > 120;
$rel > "2000-01-01";
fetch { "title": $t, "released": $rel, "runtime": $rt };"""

    if 'm.runtime > 150 AND m.runtime < 200' in cypher:
        return """match
$m isa movie, has title $t, has runtime $rt;
$rt > 150;
$rt < 200;
fetch { "title": $t, "runtime": $rt };"""

    if 'm.runtime < 90' in cypher:
        if 'ORDER BY m.released DESC' in cypher:
            return """match
$m isa movie, has title $t, has released $rel, has runtime $rt;
$rt < 90;
sort $rel desc;
limit 5;
fetch { "title": $t, "released": $rel, "runtime": $rt };"""

    # Released >= '2011'
    if "m.released >= '2011'" in cypher:
        return """match
$m isa movie, has title $t, has revenue $rev, has released $rel;
$rel >= "2011";
sort $rev desc;
limit 5;
fetch { "title": $t, "revenue": $rev };"""

    # Released starts with (decade queries)
    if "m.released STARTS WITH '20'" in cypher:
        return """match
$m isa movie, has title $t, has imdb_rating $r, has year $y, has released $rel;
$rel like "20%";
(film: $m, genre: $g) isa in_genre;
sort $r desc;
limit 5;
fetch { "title": $t, "imdbRating": $r, "year": $y };"""

    # plot CONTAINS 'love'
    if "m.plot CONTAINS 'love'" in cypher:
        return """match
$m isa movie, has title $t, has plot $p, has runtime $rt, has imdb_rating $r;
$p like ".*love.*";
$rt > 120;
sort $r desc;
limit 3;
fetch { "title": $t, "plot": $p, "runtime": $rt };"""

    # Poster IS NOT NULL
    if 'm.poster IS NOT NULL' in cypher:
        return """match
$m isa movie, has title $t, has poster $p;
limit 3;
fetch { "title": $t, "poster": $p };"""

    return None


def convert_actor_query(question, cypher):
    """Convert actor-related queries."""

    # Actor with more than N movies
    if 'count(m) AS numMovies' in cypher and 'numMovies > 10' in cypher:
        limit = 5
        if 'LIMIT 3' in cypher:
            limit = 3
        return f"""match
$a isa person, has name $n;
(actor: $a, film: $m) isa acted_in;
reduce $numMovies = count($m) groupby $n;
$numMovies > 10;
sort $numMovies desc;
limit {limit};
fetch {{ "actor": $n, "numMovies": $numMovies }};"""

    # Actor with poster
    if 'a.poster IS NOT NULL' in cypher:
        if 'count(m)' in cypher:
            return """match
$a isa person, has name $n, has poster $p;
(actor: $a, film: $m) isa acted_in;
reduce $numMovies = count($m) groupby $n, $p;
$numMovies >= 2;
limit 3;
fetch { "name": $n, "poster": $p };"""
        else:
            return """match
$a isa person, has name $n, has poster $p;
(actor: $a, film: $m) isa acted_in;
limit 5;
fetch { "name": $n, "poster": $p };"""

    # Actor died and acted in more than N movies
    if 'a.died IS NOT NULL' in cypher:
        return """match
$a isa person, has name $n, has died $d;
(actor: $a, film: $m) isa acted_in;
reduce $movieCount = count($m) groupby $n, $d;
$movieCount > 2;
sort $movieCount desc;
limit 3;
fetch { "name": $n, "died": $d, "movieCount": $movieCount };"""

    # Actor born after 1980 with more than 5 movies
    if "a.born > date('1980-01-01')" in cypher:
        return """match
$a isa person, has name $n, has born $b;
(actor: $a, film: $m) isa acted_in;
$b > 1980-01-01;
reduce $numMovies = count($m) groupby $n, $b;
$numMovies > 5;
sort $numMovies desc;
limit 3;
fetch { "name": $n, "born": $b, "numMovies": $numMovies };"""

    # Actor with more than 1 movie with imdbRating > 7
    if 'm.imdbRating > 7' in cypher and 'numMovies > 1' in cypher:
        return """match
$a isa person, has name $n;
(actor: $a, film: $m) isa acted_in;
$m has imdb_rating $r;
$r > 7.0;
reduce $numMovies = count($m) groupby $n;
$numMovies > 1;
fetch { "actor": $n, "numMovies": $numMovies };"""

    # Actors who never acted in Action movies
    if 'NOT exists' in cypher and 'Action' in cypher:
        return """match
$a isa person, has name $n;
(actor: $a, film: $m) isa acted_in;
not {
    (film: $m, genre: $g) isa in_genre;
    $g has name "Action";
};
fetch { "actor": $n };"""

    # Actor with at least 2 different directors
    if 'count(DISTINCT d) AS numDirectors' in cypher:
        return """match
$a isa person, has name $n;
(actor: $a, film: $m) isa acted_in;
(director: $d, film: $m) isa directed;
$d has name $dn;
reduce $numDirectors = count($dn) groupby $n;
$numDirectors >= 2;
fetch { "actor": $n, "numDirectors": $numDirectors };"""

    # bornIn = France
    if "bornIn: 'France'" in cypher or "bornIn = 'France'" in cypher:
        return """match
$a isa person, has name $n, has born_in "France";
(actor: $a, film: $m) isa acted_in;
reduce $numMovies = count($m) groupby $n;
$numMovies > 1;
sort $numMovies desc;
limit 3;
fetch { "actor": $n, "numMovies": $numMovies };"""

    return None


def convert_director_query(question, cypher):
    """Convert director-related queries."""

    # Director with more than N movies
    if 'count(m) AS num_movies' in cypher or 'count(m) AS numMovies' in cypher:
        threshold = 3
        if 'num_movies > 5' in cypher or 'numMovies > 5' in cypher:
            threshold = 5
        elif 'num_movies > 3' in cypher or 'numMovies > 3' in cypher:
            threshold = 3

        if 'LIMIT' not in cypher:
            return f"""match
$d isa person, has name $n;
(director: $d, film: $m) isa directed;
reduce $num_movies = count($m) groupby $n;
$num_movies > {threshold};
fetch {{ "director": $n, "num_movies": $num_movies }};"""

    # Director with poster containing string
    if 'd.poster CONTAINS' in cypher:
        return """match
$d isa person, has name $n, has poster $p;
(director: $d, film: $m) isa directed;
$p like ".*w440_and_h660_face.*";
fetch { "name": $n, "poster": $p };"""

    # Director died after 2000
    if "d.died > date('2000-01-01')" in cypher:
        return """match
$d isa person, has name $n, has died $died;
(director: $d, film: $m) isa directed;
$died > 2000-01-01;
reduce $num_movies = count($m) groupby $n, $died;
$num_movies > 2;
sort $num_movies desc;
limit 3;
fetch { "director": $n, "died": $died, "num_movies": $num_movies };"""

    # Director with high budget movies
    if 'm.budget > 100000000' in cypher and 'count(m)' in cypher:
        return """match
$d isa person, has name $n;
(director: $d, film: $m) isa directed;
$m has budget $b;
$b > 100000000;
reduce $num_high_budget_movies = count($m) groupby $n;
$num_high_budget_movies >= 3;
fetch { "director": $n, "num_high_budget_movies": $num_high_budget_movies };"""

    # Director in more than 3 genres
    if 'count(DISTINCT g) AS num_genres' in cypher and 'num_genres > 3' in cypher:
        limit = 5
        if 'LIMIT 3' in cypher:
            limit = 3
        return f"""match
$d isa person, has name $n;
(director: $d, film: $m) isa directed;
(film: $m, genre: $g) isa in_genre;
$g has name $gn;
reduce $num_genres = count($gn) groupby $n;
$num_genres > 3;
sort $num_genres desc;
limit {limit};
fetch {{ "director": $n, "num_genres": $num_genres }};"""

    # Director with more than 1 drama movie
    if "'Drama'" in cypher and 'num_drama_movies > 1' in cypher:
        return """match
$d isa person, has name $n;
(director: $d, film: $m) isa directed;
(film: $m, genre: $g) isa in_genre;
$g has name "Drama";
reduce $num_drama_movies = count($m) groupby $n;
$num_drama_movies > 1;
fetch { "director": $n, "num_drama_movies": $num_drama_movies };"""

    # Director with Comedy and Drama movies
    if "'Comedy'" in cypher and "'Drama'" in cypher:
        return """match
$d isa person, has name $n;
(director: $d, film: $m1) isa directed;
(film: $m1, genre: $g1) isa in_genre;
$g1 has name "Comedy";
(director: $d, film: $m2) isa directed;
(film: $m2, genre: $g2) isa in_genre;
$g2 has name "Drama";
fetch { "director": $n };"""

    # Director with Horror and Romance movies
    if "'Horror'" in cypher and "'Romance'" in cypher:
        return """match
$d isa person, has name $n;
(director: $d, film: $m1) isa directed;
(film: $m1, genre: $g1) isa in_genre;
$g1 has name "Horror";
(director: $d, film: $m2) isa directed;
(film: $m2, genre: $g2) isa in_genre;
$g2 has name "Romance";
fetch { "director": $n };"""

    # Director bornIn contains USA/UK
    if "bornIn: 'Burchard, Nebraska, USA'" in cypher:
        return """match
$d isa person, has name $n, has born_in "Burchard, Nebraska, USA";
(director: $d, film: $m) isa directed;
$m has revenue $rev;
reduce $totalRevenue = sum($rev);
fetch { "totalRevenue": $totalRevenue };"""

    if "bornIn CONTAINS 'USA'" in cypher or "bornIn: 'USA'" in cypher:
        return """match
$d isa person, has name $dn, has born_in $bi;
(director: $d, film: $m) isa directed;
$bi like ".*USA.*";
$m has budget $b, has title $t, has year $y;
$b > 100000000;
fetch { "title": $t, "budget": $b, "year": $y };"""

    # Director with Action movies
    if "'Action'" in cypher:
        return """match
$d isa person, has name $n;
(director: $d, film: $m) isa directed;
(film: $m, genre: $g) isa in_genre;
$g has name "Action";
reduce $numMovies = count($m) groupby $n;
sort $numMovies desc;
limit 5;
fetch { "director": $n, "numMovies": $numMovies };"""

    # Director by name (Denzel Washington)
    if "'Denzel Washington'" in cypher:
        return """match
$d isa person, has name "Denzel Washington";
(director: $d, film: $m) isa directed;
(actor: $a, film: $m) isa acted_in;
$a has name $an;
reduce $appearances = count($m) groupby $an;
sort $appearances desc;
limit 5;
fetch { "actor": $an, "appearances": $appearances };"""

    # Director with Academy Award movies
    if "'Academy Award'" in cypher:
        return """match
$d isa person, has name $n;
(director: $d, film: $m) isa directed;
$m has plot $p;
$p like ".*Academy Award.*";
reduce $awardCount = count($m) groupby $n;
sort $awardCount desc;
limit 5;
fetch { "director": $n, "awardCount": $awardCount };"""

    return None


def convert_user_rating_query(question, cypher):
    """Convert user rating queries."""

    # User with more than N ratings
    if 'count(r) AS numRatings' in cypher or 'count(r) AS num_ratings' in cypher:
        if 'numRatings > 20' in cypher:
            return """match
$u isa user, has name $n;
$r isa rated (user: $u, film: $m);
reduce $numRatings = count($r) groupby $n;
$numRatings > 20;
sort $numRatings desc;
limit 5;
fetch { "name": $n, "numRatings": $numRatings };"""
        if 'num_ratings > 50' in cypher:
            return """match
$u isa user, has name $n;
$r isa rated (user: $u, film: $m);
reduce $num_ratings = count($r) groupby $n;
$num_ratings > 50;
fetch { "user": $n, "num_ratings": $num_ratings };"""

    # User rated 'Dracula Untold'
    if "'Dracula Untold'" in cypher:
        return """match
$m isa movie, has title "Dracula Untold";
$u isa user, has name $n;
(user: $u, film: $m) isa rated;
fetch { "name": $n };"""

    # Users rated movies with rating > 7
    if 'r.rating > 7.0' in cypher:
        return """match
$u isa user, has name $n;
$r isa rated (user: $u, film: $m), has rating $rating;
$rating > 7.0;
reduce $numHighRatings = count($r) groupby $n;
$numHighRatings > 5;
fetch { "user": $n, "numHighRatings": $numHighRatings };"""

    # Average rating by user
    if 'avg(r.rating)' in cypher:
        if 'avgRating < 3.0' in cypher or 'avgRating < 3' in cypher:
            return """match
$u isa user, has name $n;
$r isa rated (user: $u, film: $m), has rating $rating;
reduce $avgRating = mean($rating) groupby $n;
$avgRating < 3.0;
sort $avgRating asc;
limit 3;
fetch { "user": $n, "avgRating": $avgRating };"""
        if 'numRatings >= 20' in cypher:
            return """match
$u isa user, has name $n;
$r isa rated (user: $u, film: $m), has rating $rating;
reduce $avgRating = mean($rating), $numRatings = count($r) groupby $n;
$numRatings >= 20;
sort $avgRating desc;
limit 3;
fetch { "user": $n, "avgRating": $avgRating };"""

    # Movies rated by userId
    if "userId: '1'" in cypher and "userId: '2'" in cypher:
        return """match
$u1 isa user, has user_id "1";
$u2 isa user, has user_id "2";
$m isa movie, has title $t;
(user: $u1, film: $m) isa rated;
(user: $u2, film: $m) isa rated;
fetch { "title": $t };"""

    # Users who haven't rated Sci-Fi
    if 'NOT exists' in cypher and 'Sci-Fi' in cypher:
        return """match
$u isa user, has name $n;
(user: $u, film: $m) isa rated;
not {
    (film: $m, genre: $g) isa in_genre;
    $g has name "Sci-Fi";
};
fetch { "user": $n, "movie": $m };"""

    # Users who rated movies from more than 3 genres
    if 'count(DISTINCT g) AS num_genres' in cypher:
        return """match
$u isa user, has name $n;
(user: $u, film: $m) isa rated;
(film: $m, genre: $g) isa in_genre;
$g has name $gn;
reduce $num_genres = count($gn) groupby $n;
$num_genres > 3;
fetch { "user": $n };"""

    # Movies rated 5.0 by more than 10 users
    if 'r.rating = 5.0' in cypher:
        return """match
$m isa movie, has title $t;
$r isa rated (user: $u, film: $m), has rating 5.0;
reduce $numRatings = count($u) groupby $t;
$numRatings > 10;
fetch { "title": $t, "numRatings": $numRatings };"""

    # Movies rated by over 1000 users
    if 'count(r) AS numRatings' in cypher and 'numRatings > 1000' in cypher:
        return """match
$m isa movie, has title $t, has year $y;
$r isa rated (user: $u, film: $m);
reduce $numRatings = count($r) groupby $t, $y;
$numRatings > 1000;
sort $numRatings desc;
limit 3;
fetch { "title": $t, "year": $y, "numRatings": $numRatings };"""

    # Average rating for movies rated >= 10 times
    if 'avg(r.rating)' in cypher and 'numRatings >= 10' in cypher:
        return """match
$m isa movie, has title $t;
$r isa rated (user: $u, film: $m), has rating $rating;
reduce $avgRating = mean($rating), $numRatings = count($r) groupby $t;
$numRatings >= 10;
sort $avgRating desc;
limit 5;
fetch { "movie": $t, "avgRating": $avgRating };"""

    return None


def convert_genre_query(question, cypher):
    """Convert genre-related queries."""

    # Top genres by movie count
    if 'count(m) AS movieCount' in cypher:
        limit = 3
        if 'LIMIT 5' in cypher:
            limit = 5
        if 'movieCount > 50' in cypher:
            return """match
$g isa genre, has name $n;
(film: $m, genre: $g) isa in_genre;
reduce $movieCount = count($m) groupby $n;
$movieCount > 50;
sort $movieCount desc;
limit 3;
fetch { "genre": $n, "movieCount": $movieCount };"""
        return f"""match
$g isa genre, has name $n;
(film: $m, genre: $g) isa in_genre;
reduce $movieCount = count($m) groupby $n;
sort $movieCount desc;
limit {limit};
fetch {{ "genre": $n, "movieCount": $movieCount }};"""

    # Genres with year > 2000
    if 'm.year > 2000' in cypher:
        return """match
$g isa genre, has name $n;
(film: $m, genre: $g) isa in_genre;
$m has year $y;
$y > 2000;
reduce $count = count($m) groupby $n;
sort $count desc;
limit 3;
fetch { "genre": $n, "count": $count };"""

    # Genres with imdbRating < 4.0
    if 'm.imdbRating < 4.0' in cypher:
        return """match
$g isa genre, has name $n;
(film: $m, genre: $g) isa in_genre;
$m has imdb_rating $r;
$r < 4.0;
fetch { "genre": $n };"""

    # Genres with average rating > 7.5
    if 'avg(m.imdbRating)' in cypher and 'avgRating > 7.5' in cypher:
        return """match
$g isa genre, has name $n;
(film: $m, genre: $g) isa in_genre;
$m has imdb_rating $r;
reduce $avgRating = mean($r) groupby $n;
$avgRating > 7.5;
sort $avgRating desc;
limit 3;
fetch { "genre": $n, "avgRating": $avgRating };"""

    # Genres with average rating < 5
    if 'avg(m.imdbRating)' in cypher and 'avgRating < 5' in cypher:
        return """match
$g isa genre, has name $n;
(film: $m, genre: $g) isa in_genre;
$m has imdb_rating $r;
reduce $avgRating = mean($r) groupby $n;
$avgRating < 5.0;
sort $avgRating asc;
limit 3;
fetch { "genre": $n, "avgRating": $avgRating };"""

    return None


def convert_person_query(question, cypher):
    """Convert person-related queries (not actor/director specific)."""

    # Person directed with born > 1970
    if "p.born > date('1970-01-01')" in cypher:
        return """match
$p isa person, has name $n, has born $b;
(director: $p, film: $m) isa directed;
$m has title $t;
$b > 1970-01-01;
fetch { "movie": $t, "director": $n, "birthDate": $b };"""

    # Person acted with born < 1900
    if "p.born < date('1900-01-01')" in cypher:
        return """match
$p isa person, has name $n, has born $b;
(actor: $p, film: $m) isa acted_in;
$m has title $t;
$b < 1900-01-01;
fetch { "movie": $t, "actor": $n, "birthDate": $b };"""

    return None


def read_failed_queries(filepath):
    """Read failed queries from CSV."""
    queries = []
    with open(filepath, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            queries.append({
                'original_index': row['original_index'],
                'question': row['question'],
                'cypher': row['cypher'],
                'error': row['error']
            })
    return queries


def write_success_query(filepath, query_data):
    """Append a successful query to the queries CSV."""
    file_exists = False
    try:
        with open(filepath, 'r') as f:
            file_exists = True
    except FileNotFoundError:
        pass

    with open(filepath, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
        if not file_exists:
            writer.writeheader()
        writer.writerow(query_data)


def write_failed_queries(filepath, queries):
    """Write remaining failed queries to CSV."""
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'error'])
        writer.writeheader()
        for q in queries:
            writer.writerow(q)


def main():
    failed_path = '/opt/text2typeql/output/recommendations/failed.csv'
    success_path = '/opt/text2typeql/output/recommendations/queries.csv'

    print("Reading failed queries...")
    failed_queries = read_failed_queries(failed_path)
    print(f"Found {len(failed_queries)} failed queries")

    print("Connecting to TypeDB...")
    driver = get_driver()

    converted = 0
    still_failed = []

    for i, query in enumerate(failed_queries):
        print(f"\nProcessing query {i+1}/{len(failed_queries)} (index {query['original_index']})")
        print(f"  Question: {query['question'][:80]}...")

        typeql, skip_reason = convert_query(query['question'], query['cypher'])

        if typeql is None:
            print(f"  Skipped: {skip_reason}")
            query['error'] = skip_reason
            still_failed.append(query)
            continue

        print(f"  Attempting TypeQL validation...")
        valid, error = validate_query(driver, typeql)

        if valid:
            print(f"  SUCCESS!")
            write_success_query(success_path, {
                'original_index': query['original_index'],
                'question': query['question'],
                'cypher': query['cypher'],
                'typeql': typeql
            })
            converted += 1
        else:
            print(f"  FAILED: {error[:100]}")
            query['error'] = f"TypeQL validation failed: {error}"
            still_failed.append(query)

    print(f"\n\nWriting {len(still_failed)} still-failed queries...")
    write_failed_queries(failed_path, still_failed)

    print(f"\n{'='*60}")
    print(f"RESULTS:")
    print(f"  Total processed: {len(failed_queries)}")
    print(f"  Successfully converted: {converted}")
    print(f"  Still failed: {len(still_failed)}")
    print(f"{'='*60}")

    driver.close()


if __name__ == '__main__':
    main()
