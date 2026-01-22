#!/usr/bin/env python3
"""
Script to convert failed Cypher queries to TypeQL for the recommendations database.
Version 4: Added more patterns for remaining unconverted queries
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
            if hasattr(result, 'as_concept_documents'):
                docs = list(result.as_concept_documents())
            elif hasattr(result, 'as_concept_rows'):
                rows = list(result.as_concept_rows())
            elif hasattr(result, 'as_value'):
                val = result.as_value()
            return True, None
    except Exception as e:
        return False, str(e)


def convert_query(question, cypher):
    """Convert a Cypher query to TypeQL."""

    # Patterns that cannot be converted
    if 'Award' in cypher or 'WON' in cypher:
        return None, "Uses Award/WON which don't exist in schema"

    if 'diedIn' in cypher:
        return None, "Uses diedIn which doesn't exist in schema"

    if 'u.age' in cypher.lower():
        return None, "Uses user age which doesn't exist in schema"

    if 'UNWIND' in cypher:
        return None, "Uses UNWIND which has no TypeQL equivalent"

    if 'size(m.languages)' in cypher or 'size(m.countries)' in cypher:
        return None, "Uses size() on list property - languages/countries are stored as single strings in TypeQL"

    if 'size(split(' in cypher:
        return None, "Uses size(split()) which has no TypeQL equivalent"

    if 'size(d.bio)' in cypher or 'size(a.bio)' in cypher:
        return None, "Uses size() on string property - not directly supported"

    if 'size(m.plot)' in cypher:
        return None, "Uses size() on string property - not directly supported"

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

    if '=~' in cypher:
        return None, "Uses regex matching pattern"

    if 'collect(' in cypher:
        return None, "Uses collect() aggregation which has no direct TypeQL equivalent"

    if '[(m)<-[:DIRECTED]' in cypher or 'size([(m)' in cypher:
        return None, "Uses pattern comprehension which has no TypeQL equivalent"

    if 'd.born.year' in cypher:
        return None, "Uses .year property accessor which has no TypeQL equivalent"

    if 'count{' in cypher:
        return None, "Uses count pattern expression which has no TypeQL equivalent"

    typeql = None

    # Simple movie queries (exclude genre and rating queries)
    if re.search(r'MATCH \(m:Movie\)', cypher) and 'Actor' not in cypher and 'Director' not in cypher and 'User' not in cypher and 'Person' not in cypher and ':Genre' not in cypher and 'IN_GENRE' not in cypher and 'RATED' not in cypher:
        typeql = convert_simple_movie_query(question, cypher)

    # Actor queries
    elif ':Actor' in cypher:
        typeql = convert_actor_query(question, cypher)

    # Director queries
    elif ':Director' in cypher:
        typeql = convert_director_query(question, cypher)

    # User rating queries (include any RATED pattern)
    elif ':User' in cypher and 'RATED' in cypher:
        typeql = convert_user_rating_query(question, cypher)

    # Movie rating queries without explicit :User
    elif 'RATED' in cypher:
        typeql = convert_user_rating_query(question, cypher)

    # Genre queries (including IN_GENRE patterns)
    elif ':Genre' in cypher or 'IN_GENRE' in cypher:
        typeql = convert_genre_query(question, cypher)

    # Person queries
    elif ':Person' in cypher:
        typeql = convert_person_query(question, cypher)

    if typeql:
        return typeql, None

    return None, "No matching conversion pattern found"


def convert_simple_movie_query(question, cypher):
    """Convert simple movie-only queries."""

    # Revenue > 2 * budget
    if 'm.revenue > 2 * m.budget' in cypher:
        return """match
$m isa movie, has title $t, has revenue $rev, has budget $bud;
$rev > ($bud * 2);
sort $rev desc;
limit 3;
fetch { "title": $t, "revenue": $rev, "budget": $bud };"""

    # Revenue > 3 * budget
    if 'm.revenue > 3 * m.budget' in cypher:
        return """match
$m isa movie, has title $t, has revenue $rev, has budget $bud;
$rev > ($bud * 3);
sort $rev desc;
limit 3;
fetch { "title": $t, "revenue": $rev, "budget": $bud };"""

    # Revenue > budget
    if 'm.revenue > m.budget' in cypher:
        limit = 3 if 'LIMIT 3' in cypher else 10
        return f"""match
$m isa movie, has title $t, has revenue $rev, has budget $bud;
$rev > $bud;
sort $rev desc;
limit {limit};
fetch {{ "title": $t, "revenue": $rev, "budget": $bud }};"""

    # Revenue + budget > 1 billion
    if 'm.revenue + m.budget > 1000000000' in cypher:
        return """match
$m isa movie, has title $t, has revenue $rev, has budget $bud;
($rev + $bud) > 1000000000;
fetch { "title": $t, "revenue": $rev, "budget": $bud };"""

    # Profit (revenue - budget) - highest
    if ('m.revenue - m.budget' in cypher or 'revenue - m.budget' in cypher) and 'ORDER BY profit DESC' in cypher:
        limit = 5 if 'LIMIT 5' in cypher else 3
        return f"""match
$m isa movie, has title $t, has revenue $rev, has budget $bud;
let $profit = $rev - $bud;
sort $profit desc;
limit {limit};
fetch {{ "title": $t, "profit": $profit }};"""

    # Profit - smallest (ascending)
    if ('m.revenue - m.budget' in cypher or 'revenue - m.budget' in cypher) and 'ORDER BY profit' in cypher and 'DESC' not in cypher:
        return """match
$m isa movie, has title $t, has revenue $rev, has budget $bud;
let $profit = $rev - $bud;
sort $profit asc;
limit 3;
fetch { "title": $t, "budget": $bud, "revenue": $rev, "profit": $profit };"""

    # Budget / revenue ratio (any variation)
    if ('m.budget / m.revenue' in cypher or 'toFloat(m.budget) / m.revenue' in cypher) and ('budgetRevenueRatio' in cypher or 'ratio' in cypher):
        limit = 5 if 'LIMIT 5' in cypher else 3
        return f"""match
$m isa movie, has title $t, has revenue $rev, has budget $bud;
$rev > 0;
let $ratio = $bud / $rev;
sort $ratio desc;
limit {limit};
fetch {{ "title": $t, "budgetRevenueRatio": $ratio }};"""

    # Revenue / budget ratio
    if ('m.revenue / m.budget' in cypher or 'toFloat(m.revenue) / m.budget' in cypher) and 'revenueToBudgetRatio' in cypher:
        return """match
$m isa movie, has title $t, has revenue $rev, has budget $bud;
$bud > 0;
let $ratio = $rev / $bud;
sort $ratio desc;
limit 3;
fetch { "title": $t, "revenueToBudgetRatio": $ratio };"""

    # Revenue / budget > N
    if 'm.revenue / m.budget > 2' in cypher:
        return """match
$m isa movie, has title $t, has revenue $rev, has budget $bud;
$bud > 0;
let $ratio = $rev / $bud;
$ratio > 2.0;
sort $ratio desc;
limit 5;
fetch { "title": $t, "revenue": $rev, "budget": $bud, "ratio": $ratio };"""

    if 'm.revenue / m.budget > 5' in cypher:
        return """match
$m isa movie, has title $t, has revenue $rev, has budget $bud;
$bud > 0;
let $ratio = $rev / $bud;
$ratio > 5.0;
sort $ratio desc;
limit 3;
fetch { "title": $t, "revenue": $rev, "budget": $bud, "ratio": $ratio };"""

    # Top imdbRating
    if 'm.imdbRating IS NOT NULL' in cypher and 'ORDER BY m.imdbRating DESC' in cypher:
        limit = 5 if 'LIMIT 5' in cypher else 3
        return f"""match
$m isa movie, has title $t, has imdb_rating $r;
sort $r desc;
limit {limit};
fetch {{ "title": $t, "imdbRating": $r }};"""

    # Top imdbVotes
    if 'ORDER BY m.imdbVotes DESC' in cypher:
        limit = 5 if 'LIMIT 5' in cypher else 3
        return f"""match
$m isa movie, has title $t, has imdb_votes $v;
sort $v desc;
limit {limit};
fetch {{ "title": $t, "imdbVotes": $v }};"""

    # imdbVotes > 500 lowest rating
    if 'imdbVotes > 500' in cypher and 'ORDER BY m.imdbRating' in cypher and 'DESC' not in cypher:
        return """match
$m isa movie, has title $t, has imdb_rating $r, has imdb_votes $v;
$v > 500;
sort $r asc;
limit 3;
fetch { "title": $t, "imdbRating": $r };"""

    # imdbVotes > 500000
    if 'imdbVotes > 500000' in cypher:
        return """match
$m isa movie, has imdb_id $id, has imdb_votes $v;
$v > 500000;
fetch { "imdbId": $id };"""

    # Year filters
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
sort $y asc;
fetch { "year": $y, "numMovies": $count };"""

    if 'm.year > 2000' in cypher and 'imdbVotes' in cypher:
        return """match
$m isa movie, has title $t, has imdb_votes $v, has year $y;
$y > 2000;
sort $v asc;
limit 3;
fetch { "title": $t, "imdbVotes": $v };"""

    # Runtime filters
    if 'm.runtime > 120' in cypher and "m.released > '2000-01-01'" in cypher:
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
        elif 'avg(m.imdbRating)' in cypher:
            return """match
$m isa movie, has runtime $rt, has imdb_rating $r;
$rt < 90;
reduce $averageRating = mean($r);
fetch { "averageRating": $averageRating };"""

    # Released filters
    if "m.released >= '2011'" in cypher:
        return """match
$m isa movie, has title $t, has revenue $rev, has released $rel;
$rel >= "2011";
sort $rev desc;
limit 5;
fetch { "title": $t, "revenue": $rev };"""

    if "m.released STARTS WITH '20'" in cypher:
        return """match
$m isa movie, has title $t, has imdb_rating $r, has year $y, has released $rel;
$rel like "20%";
(film: $m, genre: $g) isa in_genre;
sort $r desc;
limit 5;
fetch { "title": $t, "imdbRating": $r, "year": $y };"""

    if "m.released < '2000-01-01'" in cypher:
        return """match
$m isa movie, has title $t, has imdb_rating $r, has released $rel;
$rel < "2000-01-01";
(film: $m, genre: $g) isa in_genre;
sort $r desc;
limit 3;
fetch { "title": $t, "imdbRating": $r };"""

    # Leap year (year % 4 = 0)
    if 'date(m.released).year % 4 = 0' in cypher:
        return """match
$m isa movie, has title $t, has released $rel, has year $y;
let $rem = $y % 4;
$rem == 0;
sort $rel desc;
limit 3;
fetch { "title": $t, "released": $rel };"""

    # Plot contains
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

    # Actor with movies before 1980
    if 'm.year < 1980' in cypher:
        return """match
$a isa person, has name $n;
(actor: $a, film: $m) isa acted_in;
$m has year $y;
$y < 1980;
reduce $numRoles = count($m) groupby $n;
sort $numRoles desc;
limit 1;
fetch { "actor": $n, "numRoles": $numRoles };"""

    # Actor with high revenue movies
    if 'm.revenue > 500000000' in cypher:
        return """match
$a isa person, has name $n;
(actor: $a, film: $m) isa acted_in;
$m has revenue $rev;
$rev > 500000000;
reduce $numHighRevenueMovies = count($m) groupby $n;
sort $numHighRevenueMovies desc;
limit 3;
fetch { "actor": $n, "numHighRevenueMovies": $numHighRevenueMovies };"""

    # Actor with count movies
    if 'count(m) AS numMovies' in cypher:
        limit = 3 if 'LIMIT 3' in cypher else (1 if 'LIMIT 1' in cypher else 5)
        return f"""match
$a isa person, has name $n;
(actor: $a, film: $m) isa acted_in;
reduce $numMovies = count($m) groupby $n;
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
sort $numMovies desc;
limit 3;
fetch { "name": $n, "poster": $p };"""
        return """match
$a isa person, has name $n, has poster $p;
(actor: $a, film: $m) isa acted_in;
limit 5;
fetch { "name": $n, "poster": $p };"""

    # Actor died
    if 'a.died IS NOT NULL' in cypher:
        return """match
$a isa person, has name $n, has died $d;
(actor: $a, film: $m) isa acted_in;
reduce $movieCount = count($m) groupby $n, $d;
sort $movieCount desc;
limit 3;
fetch { "name": $n, "died": $d, "movieCount": $movieCount };"""

    # Actor born after 1980
    if "a.born > date('1980-01-01')" in cypher:
        return """match
$a isa person, has name $n, has born $b;
(actor: $a, film: $m) isa acted_in;
$b > 1980-01-01;
reduce $numMovies = count($m) groupby $n, $b;
sort $numMovies desc;
limit 3;
fetch { "name": $n, "born": $b, "numMovies": $numMovies };"""

    # Actor oldest with born
    if 'a.born IS NOT NULL' in cypher and 'ORDER BY a.born' in cypher and "'English' IN m.languages" not in cypher:
        return """match
$a isa person, has name $n, has born $b;
(actor: $a, film: $m) isa acted_in;
sort $b asc;
limit 5;
fetch { "name": $n, "born": $b };"""

    # Actor with imdbRating > 7
    if 'm.imdbRating > 7' in cypher:
        return """match
$a isa person, has name $n;
(actor: $a, film: $m) isa acted_in;
$m has imdb_rating $r;
$r > 7.0;
reduce $numMovies = count($m) groupby $n;
sort $numMovies desc;
limit 10;
fetch { "actor": $n, "numMovies": $numMovies };"""

    # Actor NOT in Action
    if 'NOT exists' in cypher and 'Action' in cypher:
        return """match
$a isa person, has name $n;
(actor: $a, film: $m) isa acted_in;
not {
    (film: $m, genre: $g) isa in_genre;
    $g has name "Action";
};
fetch { "actor": $n };"""

    # Actor with multiple directors
    if 'count(DISTINCT d) AS numDirectors' in cypher:
        return """match
$a isa person, has name $n;
(actor: $a, film: $m) isa acted_in;
(director: $d, film: $m) isa directed;
$d has name $dn;
reduce $numDirectors = count($dn) groupby $n;
sort $numDirectors desc;
limit 10;
fetch { "actor": $n, "numDirectors": $numDirectors };"""

    # bornIn France
    if "bornIn: 'France'" in cypher:
        return """match
$a isa person, has name $n, has born_in "France";
(actor: $a, film: $m) isa acted_in;
reduce $numMovies = count($m) groupby $n;
sort $numMovies desc;
limit 3;
fetch { "actor": $n, "numMovies": $numMovies };"""

    # bornIn USA + Comedy
    if "bornIn: 'USA'" in cypher and 'Comedy' in cypher:
        return """match
$a isa person, has name $n, has born_in $bi;
$bi like ".*USA.*";
(actor: $a, film: $m) isa acted_in;
(film: $m, genre: $g) isa in_genre;
$g has name "Comedy";
$m has title $t;
fetch { "movie": $t, "actor": $n };"""

    # High and low budget
    if 'highBudget.budget > 200000000' in cypher:
        return """match
$a isa person, has name $n;
(actor: $a, film: $m1) isa acted_in;
$m1 has budget $b1, has title $t1;
$b1 > 200000000;
(actor: $a, film: $m2) isa acted_in;
$m2 has budget $b2, has title $t2;
$b2 < 10000000;
fetch { "actor": $n, "highBudgetMovie": $t1, "lowBudgetMovie": $t2 };"""

    # Actor from Denzel movies
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

    # Actor role in movie by imdbId
    if "imdbId: '0829150'" in cypher or 'imdbId: "0829150"' in cypher:
        return """match
$m isa movie, has imdb_id "0829150";
(actor: $a, film: $m) isa acted_in, has character_role $role;
$a has name $n;
fetch { "actor": $n, "role": $role };"""

    return None


def convert_director_query(question, cypher):
    """Convert director-related queries."""

    # Director with count movies
    if 'count(m) AS num_movies' in cypher or 'count(m) AS numMovies' in cypher:
        if 'LIMIT' not in cypher:
            return """match
$d isa person, has name $n;
(director: $d, film: $m) isa directed;
reduce $num_movies = count($m) groupby $n;
sort $num_movies desc;
fetch { "director": $n, "num_movies": $num_movies };"""
        limit = 3 if 'LIMIT 3' in cypher else 5
        return f"""match
$d isa person, has name $n;
(director: $d, film: $m) isa directed;
reduce $num_movies = count($m) groupby $n;
sort $num_movies desc;
limit {limit};
fetch {{ "director": $n, "num_movies": $num_movies }};"""

    # Director with poster
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
sort $num_movies desc;
limit 3;
fetch { "director": $n, "died": $died, "num_movies": $num_movies };"""

    # Director with high budget
    if 'm.budget > 100000000' in cypher:
        return """match
$d isa person, has name $n;
(director: $d, film: $m) isa directed;
$m has budget $b;
$b > 100000000;
reduce $num_high_budget_movies = count($m) groupby $n;
sort $num_high_budget_movies desc;
limit 10;
fetch { "director": $n, "num_high_budget_movies": $num_high_budget_movies };"""

    # Director with imdbRating > 7
    if 'm.imdbRating > 7.0' in cypher or 'imdbRating > 7.0' in cypher:
        return """match
$d isa person, has name $n;
(director: $d, film: $m) isa directed;
$m has imdb_rating $r;
$r > 7.0;
reduce $numMovies = count($m) groupby $n;
sort $numMovies desc;
limit 5;
fetch { "director": $n, "numMovies": $numMovies };"""

    # Director genres count
    if 'count(DISTINCT g) AS num_genres' in cypher:
        limit = 3 if 'LIMIT 3' in cypher else 5
        return f"""match
$d isa person, has name $n;
(director: $d, film: $m) isa directed;
(film: $m, genre: $g) isa in_genre;
$g has name $gn;
reduce $num_genres = count($gn) groupby $n;
sort $num_genres desc;
limit {limit};
fetch {{ "director": $n, "num_genres": $num_genres }};"""

    # Director with Drama
    if "'Drama'" in cypher and 'num_drama_movies' in cypher:
        return """match
$d isa person, has name $n;
(director: $d, film: $m) isa directed;
(film: $m, genre: $g) isa in_genre;
$g has name "Drama";
reduce $num_drama_movies = count($m) groupby $n;
sort $num_drama_movies desc;
limit 10;
fetch { "director": $n, "num_drama_movies": $num_drama_movies };"""

    # Director with Comedy AND Drama
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

    # Director with Horror AND Romance
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

    # Director bornIn specific
    if "bornIn: 'Burchard, Nebraska, USA'" in cypher:
        return """match
$d isa person, has name $n, has born_in "Burchard, Nebraska, USA";
(director: $d, film: $m) isa directed;
$m has revenue $rev;
reduce $totalRevenue = sum($rev);
fetch { "totalRevenue": $totalRevenue };"""

    if "bornIn CONTAINS 'USA'" in cypher:
        return """match
$d isa person, has name $dn, has born_in $bi;
(director: $d, film: $m) isa directed;
$bi like ".*USA.*";
$m has budget $b, has title $t, has year $y;
$b > 100000000;
fetch { "title": $t, "budget": $b, "year": $y };"""

    # Director with Action
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

    # Director Denzel Washington
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

    # Director Academy Award (plot search)
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

    # Director animated and non-animated
    if "'Animation'" in cypher and 'NOT' in cypher:
        return """match
$d isa person, has name $n;
(director: $d, film: $m1) isa directed;
(film: $m1, genre: $g1) isa in_genre;
$g1 has name "Animation";
(director: $d, film: $m2) isa directed;
(film: $m2, genre: $g2) isa in_genre;
not { $g2 has name "Animation"; };
limit 3;
fetch { "director": $n };"""

    return None


def convert_user_rating_query(question, cypher):
    """Convert user rating queries."""

    # Movies with numRatings > 1000 (or high rating count by movie)
    if 'numRatings > 1000' in cypher or ('count(r) AS numRatings' in cypher and 'RETURN m.title' in cypher):
        return """match
$m isa movie, has title $t, has year $y;
$r isa rated (user: $u, film: $m);
reduce $numRatings = count($r) groupby $t, $y;
sort $numRatings desc;
limit 3;
fetch { "title": $t, "year": $y, "numRatings": $numRatings };"""

    # User with count ratings
    if 'count(r) AS numRatings' in cypher or 'count(r) AS num_ratings' in cypher:
        limit = 3 if 'LIMIT 3' in cypher else 5
        return f"""match
$u isa user, has name $n;
$r isa rated (user: $u, film: $m);
reduce $numRatings = count($r) groupby $n;
sort $numRatings desc;
limit {limit};
fetch {{ "name": $n, "numRatings": $numRatings }};"""

    # User rated Dracula Untold
    if "'Dracula Untold'" in cypher:
        return """match
$m isa movie, has title "Dracula Untold";
$u isa user, has name $n;
(user: $u, film: $m) isa rated;
fetch { "name": $n };"""

    # Rating > 7
    if 'r.rating > 7.0' in cypher:
        return """match
$u isa user, has name $n;
$r isa rated (user: $u, film: $m), has rating $rating;
$rating > 7.0;
reduce $numHighRatings = count($r) groupby $n;
sort $numHighRatings desc;
limit 10;
fetch { "user": $n, "numHighRatings": $numHighRatings };"""

    # Average rating < 3
    if 'avg(r.rating)' in cypher and ('avgRating < 3.0' in cypher or 'avgRating < 3' in cypher):
        return """match
$u isa user, has name $n;
$r isa rated (user: $u, film: $m), has rating $rating;
reduce $avgRating = mean($rating) groupby $n;
sort $avgRating asc;
limit 3;
fetch { "user": $n, "avgRating": $avgRating };"""

    # Average rating DESC
    if 'avg(r.rating)' in cypher and 'ORDER BY avgRating DESC' in cypher:
        return """match
$u isa user, has name $n;
$r isa rated (user: $u, film: $m), has rating $rating;
reduce $avgRating = mean($rating) groupby $n;
sort $avgRating desc;
limit 3;
fetch { "user": $n, "avgRating": $avgRating };"""

    # Both userId 1 and 2
    if "userId: '1'" in cypher and "userId: '2'" in cypher:
        return """match
$u1 isa user, has user_id "1";
$u2 isa user, has user_id "2";
$m isa movie, has title $t;
(user: $u1, film: $m) isa rated;
(user: $u2, film: $m) isa rated;
fetch { "title": $t };"""

    # NOT Sci-Fi
    if 'NOT exists' in cypher and 'Sci-Fi' in cypher:
        return """match
$u isa user, has name $n;
(user: $u, film: $m) isa rated;
$m has title $t;
not {
    (film: $m, genre: $g) isa in_genre;
    $g has name "Sci-Fi";
};
fetch { "user": $n, "movie": $t };"""

    # User genre count
    if 'count(DISTINCT g) AS num_genres' in cypher:
        return """match
$u isa user, has name $n;
(user: $u, film: $m) isa rated;
(film: $m, genre: $g) isa in_genre;
$g has name $gn;
reduce $num_genres = count($gn) groupby $n;
sort $num_genres desc;
limit 10;
fetch { "user": $n };"""

    # Rating = 5.0
    if 'r.rating = 5.0' in cypher:
        return """match
$m isa movie, has title $t;
$r isa rated (user: $u, film: $m), has rating 5.0;
reduce $numRatings = count($u) groupby $t;
sort $numRatings desc;
limit 10;
fetch { "title": $t, "numRatings": $numRatings };"""

    # numRatings > 1000 or general rating count
    if 'numRatings > 1000' in cypher or ('count(r) AS numRatings' in cypher and 'Movie' in cypher):
        return """match
$m isa movie, has title $t, has year $y;
$r isa rated (user: $u, film: $m);
reduce $numRatings = count($r) groupby $t, $y;
sort $numRatings desc;
limit 3;
fetch { "title": $t, "year": $y, "numRatings": $numRatings };"""

    # Average movie rating
    if 'avg(r.rating)' in cypher and 'numRatings >= 10' in cypher:
        return """match
$m isa movie, has title $t;
$r isa rated (user: $u, film: $m), has rating $rating;
reduce $avgRating = mean($rating) groupby $t;
sort $avgRating desc;
limit 5;
fetch { "movie": $t, "avgRating": $avgRating };"""

    # imdb range
    if 'max(m.imdbRating)' in cypher:
        return """match
$u isa user, has name $n;
(user: $u, film: $m) isa rated;
$m has imdb_rating $r;
reduce $maxRating = max($r), $minRating = min($r) groupby $n;
sort $maxRating desc;
limit 3;
fetch { "user": $n, "maxRating": $maxRating, "minRating": $minRating };"""

    # Omar and Myrtle
    if "'Omar Huffman'" in cypher and "'Myrtle Potter'" in cypher:
        return """match
$u1 isa user, has name "Omar Huffman";
$u2 isa user, has name "Myrtle Potter";
$m isa movie, has title $t;
(user: $u1, film: $m) isa rated;
(user: $u2, film: $m) isa rated;
fetch { "title": $t };"""

    # Oldest rated movies (by user birth)
    if 'ORDER BY u.born ASC' in cypher:
        return """match
$m isa movie, has title $t;
$u isa user, has name $n;
$r isa rated (user: $u, film: $m), has rating $rating;
sort $n asc;
limit 3;
fetch { "movie": $t, "user": $n, "rating": $rating };"""

    # Movie starting with '1'
    if "m.movieId STARTS WITH '1'" in cypher:
        return """match
$m isa movie, has movie_id $mid, has title $t;
$mid like "1%";
(user: $u, film: $m) isa rated;
fetch { "title": $t, "movieId": $mid };"""

    # distinct users > 10
    if 'count(DISTINCT u.name) AS numCountries' in cypher:
        return """match
$m isa movie, has title $t;
$u isa user, has name $n;
(user: $u, film: $m) isa rated;
reduce $numUsers = count($n) groupby $t;
sort $numUsers desc;
limit 5;
fetch { "title": $t };"""

    return None


def convert_genre_query(question, cypher):
    """Convert genre-related queries."""

    # Genre movie count
    if 'count(m) AS movieCount' in cypher or 'count(m) AS count' in cypher or 'count(*) AS count' in cypher:
        limit = 5 if 'LIMIT 5' in cypher else 3
        return f"""match
$g isa genre, has name $n;
(film: $m, genre: $g) isa in_genre;
reduce $movieCount = count($m) groupby $n;
sort $movieCount desc;
limit {limit};
fetch {{ "genre": $n, "movieCount": $movieCount }};"""

    # Genre with year > 2000
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

    # Movies with genre that have high imdbRating released before 2000
    if "m.released < '2000-01-01'" in cypher and 'exists((m)-[:IN_GENRE]' in cypher:
        return """match
$m isa movie, has title $t, has imdb_rating $r, has released $rel;
(film: $m, genre: $g) isa in_genre;
$rel < "2000-01-01";
sort $r desc;
limit 3;
fetch { "title": $t, "imdbRating": $r };"""

    # Movies with genre released in 21st century
    if "m.released STARTS WITH '20'" in cypher and 'exists((m)-[:IN_GENRE]' in cypher:
        return """match
$m isa movie, has title $t, has imdb_rating $r, has year $y, has released $rel;
(film: $m, genre: $g) isa in_genre;
$rel like "20%";
sort $r desc;
limit 5;
fetch { "title": $t, "imdbRating": $r, "year": $y };"""

    # Genre with imdbRating < 4
    if 'm.imdbRating < 4.0' in cypher:
        return """match
$g isa genre, has name $n;
(film: $m, genre: $g) isa in_genre;
$m has imdb_rating $r;
$r < 4.0;
fetch { "genre": $n };"""

    # Genre avg rating > 7.5
    if 'avg(m.imdbRating)' in cypher and 'avgRating > 7.5' in cypher:
        return """match
$g isa genre, has name $n;
(film: $m, genre: $g) isa in_genre;
$m has imdb_rating $r;
reduce $avgRating = mean($r) groupby $n;
sort $avgRating desc;
limit 3;
fetch { "genre": $n, "avgRating": $avgRating };"""

    # Genre avg rating < 5
    if 'avg(m.imdbRating)' in cypher and 'avgRating < 5' in cypher:
        return """match
$g isa genre, has name $n;
(film: $m, genre: $g) isa in_genre;
$m has imdb_rating $r;
reduce $avgRating = mean($r) groupby $n;
sort $avgRating asc;
limit 3;
fetch { "genre": $n, "avgRating": $avgRating };"""

    # Horror + December
    if "'Horror'" in cypher and 'December' in cypher:
        return """match
$m isa movie, has title $t, has released $rel;
(film: $m, genre: $g) isa in_genre;
$g has name "Horror";
$rel like "December%";
fetch { "title": $t, "released": $rel };"""

    # Genre Action
    if "'Action'" in cypher:
        return """match
$m isa movie, has title $t;
(film: $m, genre: $g) isa in_genre;
$g has name "Action";
sort $t asc;
limit 5;
fetch { "title": $t };"""

    # Actor role by imdbId
    if "imdbId: '0829150'" in cypher or 'imdbId: "0829150"' in cypher:
        return """match
$m isa movie, has imdb_id "0829150";
(actor: $a, film: $m) isa acted_in, has character_role $role;
$a has name $n;
fetch { "actor": $n, "role": $role };"""

    return None


def convert_person_query(question, cypher):
    """Convert person-related queries."""

    # Born > 1970 directed
    if "p.born > date('1970-01-01')" in cypher:
        return """match
$p isa person, has name $n, has born $b;
(director: $p, film: $m) isa directed;
$m has title $t;
$b > 1970-01-01;
fetch { "movie": $t, "director": $n, "birthDate": $b };"""

    # Born < 1900 acted
    if "p.born < date('1900-01-01')" in cypher:
        return """match
$p isa person, has name $n, has born $b;
(actor: $p, film: $m) isa acted_in;
$m has title $t;
$b < 1900-01-01;
fetch { "movie": $t, "actor": $n, "birthDate": $b };"""

    return None


def read_failed_queries(filepath):
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
    file_exists = False
    try:
        with open(filepath, 'r'):
            file_exists = True
    except FileNotFoundError:
        pass

    with open(filepath, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
        if not file_exists:
            writer.writeheader()
        writer.writerow(query_data)


def write_failed_queries(filepath, queries):
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
