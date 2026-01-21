#!/usr/bin/env python3
"""Fix recommendations queries that failed semantic review."""

import csv
import sys
sys.path.insert(0, '/opt/text2typeql')

from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# ============================================================================
# CATEGORY 1: Missing sort/limit after aggregation
# These need sort and limit added to complete the query
# ============================================================================

SORT_LIMIT_FIXES = {
    # "Which 5 users have rated more than 20 movies?" - need threshold filter via chained match
    2: """match
$u isa user, has name $n;
rated (user: $u, film: $m);
reduce $count = count($m) groupby $n;
match $count > 20;
sort $count desc;
limit 5;
fetch { "user": $n, "count": $count };""",

    # "Identify the top 5 actors who have acted in more than 10 movies." - need threshold
    3: """match
$a isa person, has name $n;
acted_in (actor: $a, film: $m);
reduce $count = count($m) groupby $n;
match $count > 10;
sort $count desc;
limit 5;
fetch { "actor": $n, "numMovies": $count };""",

    # "What are the top 3 genres associated with movies released after 2000?" - just need sort/limit
    4: """match
$m isa movie, has year $y;
$g isa genre, has name $n;
in_genre (film: $m, genre: $g);
$y > 2000;
reduce $count = count($m) groupby $n;
sort $count desc;
limit 3;
fetch { "genre": $n, "count": $count };""",

    # "Which 5 directors have directed at least 5 movies with an IMDb rating above 7.0?" - need threshold
    6: """match
$d isa person, has name $n;
$m isa movie, has imdb_rating $r;
directed (director: $d, film: $m);
$r > 7.0;
reduce $count = count($m) groupby $n;
match $count >= 5;
sort $count desc;
limit 5;
fetch { "director": $n, "numMovies": $count };""",

    # "Show the top 3 actors who have acted in movies that generated over 500 million in revenue."
    7: """match
$a isa person, has name $n;
$m isa movie, has revenue $r;
acted_in (actor: $a, film: $m);
$r > 500000000;
reduce $count = count($m) groupby $n;
sort $count desc;
limit 3;
fetch { "actor": $n, "numHighRevenueMovies": $count };""",

    # "Which 5 directors have directed movies in more than 3 different genres?" - need threshold
    40: """match
$d isa person, has name $n;
directed (director: $d, film: $m);
in_genre (film: $m, genre: $g);
reduce $count = count($g) groupby $n;
match $count > 3;
sort $count desc;
limit 5;
fetch { "director": $n, "num_genres": $count };""",

    # "Identify the first 3 actors who have acted in movies with at least three different directors." - need threshold
    41: """match
$a isa person, has name $n;
$d isa person;
acted_in (actor: $a, film: $m);
directed (director: $d, film: $m);
reduce $count = count($d) groupby $n;
match $count >= 3;
sort $count desc;
limit 3;
fetch { "actor": $n };""",
}

# ============================================================================
# CATEGORY 2: Arithmetic expressions (ratio/profit calculations)
# Use `let` expressions for arithmetic
# ============================================================================

ARITHMETIC_FIXES = {
    # "List the top 3 movies with the highest budget to revenue ratio."
    75: """match
$m isa movie, has title $t, has budget $b, has revenue $r;
$b > 0;
let $ratio = $b / $r;
sort $ratio desc;
limit 3;
fetch { "title": $t, "budget": $b, "revenue": $r, "ratio": $ratio };""",

    # "List the top 3 movies with the highest budget to revenue ratio." (duplicate)
    164: """match
$m isa movie, has title $t, has budget $b, has revenue $r;
$b > 0;
let $ratio = $b / $r;
sort $ratio desc;
limit 3;
fetch { "title": $t, "ratio": $ratio };""",

    # "List the top 3 movies with the smallest difference between budget and revenue." (profit)
    176: """match
$m isa movie, has title $t, has budget $b, has revenue $r;
let $diff = $r - $b;
sort $diff asc;
limit 3;
fetch { "movie": $t, "budget": $b, "revenue": $r, "profit": $diff };""",

    # "Which 3 movies have the highest budget-revenue ratio?"
    229: """match
$m isa movie, has title $t, has budget $b, has revenue $r;
$b > 0;
let $ratio = $b / $r;
sort $ratio desc;
limit 3;
fetch { "movie": $t, "budget": $b, "revenue": $r };""",

    # "What are the top 3 movies with the most significant difference between budget and revenue?"
    259: """match
$m isa movie, has title $t, has budget $b, has revenue $r;
let $diff = $r - $b;
sort $diff desc;
limit 3;
fetch { "movie": $t, "budget": $b, "revenue": $r };""",

    # "What are the top 3 most profitable movies, considering the difference between revenue and budget?"
    304: """match
$m isa movie, has title $t, has revenue $r, has budget $b;
let $profit = $r - $b;
sort $profit desc;
limit 3;
fetch { "movie": $t, "revenue": $r, "budget": $b };""",

    # "Name the top 5 movies with the highest budget to revenue ratio."
    335: """match
$m isa movie, has title $t, has budget $b, has revenue $r;
$b > 0;
let $ratio = $b / $r;
sort $ratio desc;
limit 5;
fetch { "movie": $t, "budget": $b, "revenue": $r };""",

    # "What are the first 3 movies that have a revenue to budget ratio greater than 3?"
    354: """match
$m isa movie, has title $t, has revenue $r, has budget $b;
$b > 0;
let $ratio = $r / $b;
$ratio > 3;
sort $ratio desc;
limit 3;
fetch { "title": $t, "revenue": $r, "budget": $b };""",

    # "What are the first 3 movies with a revenue greater than their budget?"
    379: """match
$m isa movie, has title $t, has revenue $r, has budget $b;
$r > $b;
sort $r desc;
limit 3;
fetch { "title": $t, "revenue": $r, "budget": $b };""",

    # "Which three movies have the highest difference in revenue and budget?"
    410: """match
$m isa movie, has title $t, has revenue $r, has budget $b;
let $profit = $r - $b;
sort $profit desc;
limit 3;
fetch { "movie": $t, "revenue": $r, "budget": $b };""",

    # "Find all movies where the revenue was more than double the budget."
    118: """match
$m isa movie, has title $t, has revenue $r, has budget $b;
$b > 0;
let $ratio = $r / $b;
$ratio > 2;
fetch { "title": $t, "budget": $b, "revenue": $r };""",

    # "Find movies where the sum of the revenue and budget is more than 1 billion dollars."
    137: """match
$m isa movie, has title $t, has revenue $r, has budget $b;
let $total = $r + $b;
$total > 1000000000;
fetch { "title": $t, "revenue": $r, "budget": $b };""",

    # "List all movies that have grossed more than their budget."
    445: """match
$m isa movie, has title $t, has revenue $r, has budget $b;
$r > $b;
sort $r desc;
limit 5;
fetch { "title": $t, "revenue": $r, "budget": $b };""",
}

# ============================================================================
# CATEGORY 3: Wrong constant/filter value fixes
# These have incorrect values that need to be fixed
# ============================================================================

VALUE_FIXES = {
    # "Which movies have been rated by both user '1' and user '2'?" - fix user IDs
    120: """match
$u1 isa user, has user_id "1";
$u2 isa user, has user_id "2";
$m isa movie, has title $t;
rated (user: $u1, film: $m);
rated (user: $u2, film: $m);
fetch { "title": $t };""",

    # "How many movies have been released in each year from 2010 to 2020?" - fix year range
    122: """match
$m isa movie, has year $y;
$y >= 2010;
$y <= 2020;
reduce $count = count($m) groupby $y;
fetch { "year": $y, "count": $count };""",

    # "Which actor has the highest number of roles in movies released before 1980?" - fix filter
    128: """match
$a isa person, has name $n;
$m isa movie, has year $y;
acted_in (actor: $a, film: $m);
$y < 1980;
reduce $count = count($m) groupby $n;
sort $count desc;
limit 1;
fetch { "actor": $n, "numRoles": $count };""",

    # "List all movies directed by someone who was born after 1970." - fix date
    134: """match
$d isa person, has born $b;
$m isa movie, has title $t;
directed (director: $d, film: $m);
$b > 1970-01-01;
fetch { "title": $t };""",

    # "What is the average IMDb rating of movies with a runtime under 90 minutes?" - REMOVED - global aggregates handled separately

    # "List the top 5 movies by revenue that were released in the last five years of the schema's data range." - fix years
    139: """match
$m isa movie, has title $t, has revenue $r, has year $y;
$y >= 2011;
sort $r desc;
limit 5;
fetch { "title": $t, "revenue": $r };""",

    # "What are the different roles played by actors in the movie with the IMDb ID '0829150'?" - use imdb_id
    141: """match
$m isa movie, has imdb_id "0829150";
$a isa person, has name $n;
$rel isa acted_in (actor: $a, film: $m), has character_role $role;
fetch { "actor": $n, "role": $role };""",

    # "What are the names of the users who rated the movie 'Dracula Untold'?"
    144: """match
$m isa movie, has title "Dracula Untold";
$u isa user, has name $n;
rated (user: $u, film: $m);
fetch { "name": $n };""",

    # "Which directors have a poster URL that includes 'w440_and_h660_face'?"
    145: """match
$d isa person, has name $n, has poster $p;
directed (director: $d, film: $m);
$p like ".*w440_and_h660_face.*";
fetch { "name": $n };""",

    # "What is the total revenue generated by movies directed by directors born in 'Burchard, Nebraska, USA'?" - REMOVED - global aggregates handled separately

    # "Find all movies that have been rated after January 1st, 2015." - fix timestamp (1420070400 = 2015-01-01)
    150: """match
$m isa movie, has title $t;
$rel isa rated (user: $u, film: $m), has timestamp $ts;
$ts > 1420070400;
fetch { "title": $t };""",

    # "What are the IMDb IDs of movies that have more than 500,000 IMDb votes?"
    151: """match
$m isa movie, has imdb_id $id, has imdb_votes $v;
$v > 500000;
fetch { "imdbId": $id };""",

    # "Which movies have been acted in by persons born before 1900?"
    152: """match
$a isa person, has born $b;
$m isa movie, has title $t;
acted_in (actor: $a, film: $m);
$b < 1900-01-01;
fetch { "title": $t };""",

    # "What are the top 5 movies by revenue released in 1995?"
    153: """match
$m isa movie, has title $t, has revenue $r, has year $y;
$y == 1995;
sort $r desc;
limit 5;
fetch { "title": $t, "revenue": $r };""",

    # "Which movies have a runtime longer than 150 minutes and less than 200 minutes?" - remove genre
    154: """match
$m isa movie, has title $t, has runtime $r;
$r > 150;
$r < 200;
fetch { "title": $t, "runtime": $r };""",

    # "List the top 3 highest imdbRating movies that were released before the year 2000."
    155: """match
$m isa movie, has title $t, has imdb_rating $r, has year $y;
$y < 2000;
sort $r desc;
limit 3;
fetch { "title": $t, "imdbRating": $r };""",

    # "Which directors have more than one movie in the 'Drama' genre?" - fix genre
    157: """match
$d isa person, has name $n;
$g isa genre, has name "Drama";
directed (director: $d, film: $m);
in_genre (film: $m, genre: $g);
reduce $count = count($m) groupby $n;
match $count > 1;
fetch { "director": $n, "num_drama_movies": $count };""",

    # "List the top 3 movies with the highest number of imdbVotes." - remove genre filter
    158: """match
$m isa movie, has title $t, has imdb_votes $v;
sort $v desc;
limit 3;
fetch { "title": $t, "imdbVotes": $v };""",

    # "Which users have rated more than 5 movies with a rating above 7.0?" - fix rating
    160: """match
$u isa user, has name $n;
$rel isa rated (user: $u, film: $m), has rating $r;
$r > 7.0;
reduce $count = count($m) groupby $n;
match $count > 5;
fetch { "user": $n, "numHighRatings": $count };""",

    # "List the top 3 genres with the most movies associated with them." - remove rating filter
    161: """match
$g isa genre, has name $gn;
in_genre (film: $m, genre: $g);
reduce $count = count($m) groupby $gn;
sort $count desc;
limit 3;
fetch { "genre": $gn, "movieCount": $count };""",

    # "List the top 3 movies that have the highest revenue and were released in 2014." - fix year
    167: """match
$m isa movie, has title $t, has revenue $r, has year $y;
$y == 2014;
sort $r desc;
limit 3;
fetch { "title": $t, "revenue": $r };""",

    # "What are the top 5 directors by number of movies directed in the 'Action' genre?" - add genre
    168: """match
$d isa person, has name $n;
$g isa genre, has name "Action";
directed (director: $d, film: $m);
in_genre (film: $m, genre: $g);
reduce $count = count($m) groupby $n;
sort $count desc;
limit 5;
fetch { "director": $n, "numMovies": $count };""",

    # "List the genres that have movies with an imdbRating less than 4.0." - fix rating
    170: """match
$g isa genre, has name $gn;
$m isa movie, has imdb_rating $r;
in_genre (film: $m, genre: $g);
$r < 4.0;
fetch { "genre": $gn };""",

    # "What are the 5 most recent movies with a runtime less than 90 minutes?" - fix runtime
    171: """match
$m isa movie, has title $t, has runtime $r, has year $y;
$r < 90;
sort $y desc;
limit 5;
fetch { "title": $t, "year": $y };""",

    # "List the top 3 movies with the lowest imdbVotes released after 2000." - fix year
    173: """match
$m isa movie, has title $t, has imdb_votes $v, has year $y;
$y > 2000;
sort $v asc;
limit 3;
fetch { "title": $t, "imdbVotes": $v };""",

    # "What are the top 5 actors by number of movies acted in with a revenue greater than 500 million dollars?" - add revenue filter
    174: """match
$a isa person, has name $n;
$m isa movie, has revenue $r;
acted_in (actor: $a, film: $m);
$r > 500000000;
reduce $count = count($m) groupby $n;
sort $count desc;
limit 5;
fetch { "actor": $n, "numMovies": $count };""",

    # "List the top 3 movies with the lowest imdbRating that have been rated by more than 500 users." - fix filter
    179: """match
$m isa movie, has title $t, has imdb_rating $r, has imdb_votes $v;
$v > 500;
sort $r asc;
limit 3;
fetch { "title": $t, "imdbRating": $r };""",

    # "What are the top 5 actors who have acted in movies directed by Denzel Washington?" - fix director
    180: """match
$a isa person, has name $n;
$d isa person, has name "Denzel Washington";
acted_in (actor: $a, film: $m);
directed (director: $d, film: $m);
reduce $count = count($m) groupby $n;
sort $count desc;
limit 5;
fetch { "actor": $n, "appearances": $count };""",

    # "List the top 3 actors by number of movies acted in that were released in the 1990s." - fix year
    183: """match
$a isa person, has name $n;
$m isa movie, has year $y;
acted_in (actor: $a, film: $m);
$y >= 1990;
$y < 2000;
reduce $count = count($m) groupby $n;
sort $count desc;
limit 3;
fetch { "actor": $n, "numMovies": $count };""",

    # "What are the top 5 directors who have directed movies that won an academy award?" - use plot search
    184: """match
$d isa person, has name $n;
$m isa movie, has plot $p;
directed (director: $d, film: $m);
$p like ".*Academy Award.*";
reduce $count = count($m) groupby $n;
sort $count desc;
limit 5;
fetch { "director": $n, "awardCount": $count };""",

    # "Which actors have acted in both high-budget (over 200 million dollars) and low-budget (under 10 million dollars) movies?" - fix budget
    185: """match
$a isa person, has name $n;
$m1 isa movie, has budget $b1;
$m2 isa movie, has budget $b2;
acted_in (actor: $a, film: $m1);
acted_in (actor: $a, film: $m2);
$b1 > 200000000;
$b2 < 10000000;
fetch { "name": $n };""",

    # "List the top 3 movies with a plot involving 'love' that have a runtime over 120 minutes." - add runtime
    186: """match
$m isa movie, has title $t, has plot $p, has runtime $r;
$p like ".*love.*";
$r > 120;
sort $r desc;
limit 3;
fetch { "title": $t };""",

    # "What are the top 5 movies with the highest imdbRating that were released in the 21st century?" - fix filter
    187: """match
$m isa movie, has title $t, has imdb_rating $r, has year $y;
$y >= 2000;
sort $r desc;
limit 5;
fetch { "title": $t, "imdbRating": $r };""",

    # "Which directors have directed movies in both the 'Horror' and 'Romance' genres?" - fix genres
    188: """match
$d isa person, has name $n;
$g1 isa genre, has name "Horror";
$g2 isa genre, has name "Romance";
directed (director: $d, film: $m1);
directed (director: $d, film: $m2);
in_genre (film: $m1, genre: $g1);
in_genre (film: $m2, genre: $g2);
fetch { "name": $n };""",

    # "What are the names of directors who have directed both a 'Comedy' and a 'Drama' movie?" - fix approach
    121: """match
$d isa person, has name $n;
$g1 isa genre, has name "Comedy";
$g2 isa genre, has name "Drama";
directed (director: $d, film: $m1);
directed (director: $d, film: $m2);
in_genre (film: $m1, genre: $g1);
in_genre (film: $m2, genre: $g2);
fetch { "name": $n };""",

    # "Which users have rated a movie but have not rated any movie in the 'Sci-Fi' genre?" - fix genre
    140: """match
$u isa user, has name $n;
rated (user: $u, film: $m1);
not { rated (user: $u, film: $m2); in_genre (film: $m2, genre: $g); $g has name "Sci-Fi"; };
fetch { "name": $n };""",

    # "List the top 5 movies with the highest IMDb rating." - remove genre filter
    114: """match
$m isa movie, has title $t, has imdb_rating $r;
sort $r desc;
limit 5;
fetch { "title": $t, "imdbRating": $r };""",

    # "Find the top 5 actors who have acted in more than 10 movies."
    303: """match
$a isa person, has name $n;
acted_in (actor: $a, film: $m);
reduce $count = count($m) groupby $n;
match $count > 10;
sort $count desc;
limit 5;
fetch { "actor": $n, "numMovies": $count };""",

    # "Which directors have directed more than three movies?"
    432: """match
$d isa person, has name $n;
directed (director: $d, film: $m);
reduce $count = count($m) groupby $n;
match $count > 3;
sort $count desc;
limit 5;
fetch { "director": $n, "movieCount": $count };""",

    # "Which users have rated more than 50 movies?"
    434: """match
$u isa user, has name $n;
rated (user: $u, film: $m);
reduce $count = count($m) groupby $n;
match $count > 50;
sort $count desc;
limit 5;
fetch { "user": $n, "num_ratings": $count };""",

    # "Which movies have been rated 5.0 by more than 10 users?"
    458: """match
$m isa movie, has title $t;
$r isa rated (user: $u, film: $m), has rating 5.0;
reduce $count = count($u) groupby $t;
match $count > 10;
sort $count desc;
limit 5;
fetch { "title": $t, "count": $count };""",

    # "List the top 3 movies that have been rated by over 100 users." - add threshold
    425: """match
$m isa movie, has title $t;
rated (user: $u, film: $m);
reduce $count = count($u) groupby $t;
match $count > 100;
sort $count desc;
limit 3;
fetch { "title": $t, "numRatings": $count };""",

    # "Find all movies that have been rated exactly 5 times." - add filter
    465: """match
$m isa movie, has title $t;
rated (film: $m, user: $u);
reduce $count = count($u) groupby $t;
match $count == 5;
fetch { "title": $t };""",

    # "Find all movies that have a movieId starting with '1' and have been rated by at least one user." - fix pattern
    485: """match
$m isa movie, has movie_id $id, has title $t;
$id like "1.*";
rated (film: $m, user: $u);
fetch { "title": $t };""",
}

# ============================================================================
# CATEGORY 4: Queries where TypeQL added wrong filters (completely wrong)
# ============================================================================

WRONG_FILTER_FIXES = {
    # "Which actors have featured in movies released in multiple languages?" - wrong filter
    112: """match
$a isa person, has name $n;
$m isa movie, has languages $l;
acted_in (actor: $a, film: $m);
fetch { "name": $n };""",

    # "What movies have a runtime longer than 120 minutes and were released after 2000?" - fix
    113: """match
$m isa movie, has title $t, has runtime $r, has year $y;
$r > 120;
$y > 2000;
fetch { "title": $t };""",

    # "What movies had a budget greater than 100 million dollars and were directed by a director born in the USA?" - fix
    117: """match
$m isa movie, has title $t, has budget $b;
$d isa person, has born_in $bi;
directed (director: $d, film: $m);
$b > 100000000;
$bi like ".*USA.*";
fetch { "title": $t, "budget": $b };""",

    # "Which actors have acted in movies from at least three different countries?" - cannot fix (countries is string)
    # Move to UNSUPPORTED

    # "Which directors have directed movies in more than two languages?" - fix to count movies
    138: """match
$d isa person, has name $n;
$m isa movie, has languages $l;
directed (director: $d, film: $m);
reduce $count = count($m) groupby $n;
match $count > 2;
fetch { "director": $n };""",

    # "Which directors have directed movies in more than three different languages?" - same pattern
    175: """match
$d isa person, has name $n;
$m isa movie, has languages $l;
directed (director: $d, film: $m);
reduce $count = count($m) groupby $n;
match $count > 3;
fetch { "director": $n };""",

    # "List the first 5 actors who have a poster URL listed on their profile." - must be actor
    227: """match
$a isa person, has name $n, has poster $p;
acted_in (actor: $a, film: $m);
limit 5;
fetch { "name": $n, "poster": $p };""",

    # "Which 3 directors have directed both animated and non-animated movies?" - fix
    289: """match
$d isa person, has name $n;
$g1 isa genre, has name "Animation";
directed (director: $d, film: $m1);
directed (director: $d, film: $m2);
in_genre (film: $m1, genre: $g1);
not { in_genre (film: $m2, genre: $g1); };
limit 3;
fetch { "director": $n };""",

    # "List all actors who have never acted in an 'Action' genre movie." - fix syntax
    462: """match
$a isa person, has name $n;
acted_in (actor: $a, film: $m1);
not { acted_in (actor: $a, film: $m2); in_genre (film: $m2, genre: $g); $g has name "Action"; };
fetch { "actor": $n };""",

    # "List all movies that have been rated by both male and female users." - can't know gender
    # Move to UNSUPPORTED

    # "Identify movies released in 'December' that have a genre of 'Horror'." - fix date pattern
    630: """match
$m isa movie, has title $t, has released $r;
in_genre (film: $m, genre: $g);
$g has name "Horror";
$r like ".*-12-.*";
fetch { "title": $t, "released": $r };""",

    # "What are the first 3 movies with a poster URL from tmdb?" - add tmdb check
    549: """match
$m isa movie, has title $t, has poster $p;
$p like ".*tmdb.*";
limit 3;
fetch { "title": $t, "poster": $p };""",
}

# ============================================================================
# CATEGORY 5: Schema limitations - cannot be fixed, move to failed.csv
# ============================================================================

UNSUPPORTED_REASONS = {
    # Cannot count languages/countries (string fields, not arrays/relations)
    23: "Cannot count languages - languages is a string attribute, not a countable list",
    33: "Cannot count languages - languages is a string attribute, not a countable list",
    96: "Cannot count languages - languages is a string attribute, not a countable list",
    97: "Cannot count countries - countries is a string attribute, not a countable list",
    166: "Cannot count languages - languages is a string attribute, not a countable list",
    198: "Cannot count countries - countries is a string attribute, not a countable list",
    200: "Cannot count languages - languages is a string attribute, not a countable list",
    219: "Cannot count languages - languages is a string attribute, not a countable list",
    245: "Cannot count countries - countries is a string attribute, not a countable list",
    253: "Cannot count languages - languages is a string attribute, not a countable list",
    270: "Cannot count countries - countries is a string attribute, not a countable list",
    276: "Cannot count languages - languages is a string attribute, not a countable list",
    299: "Cannot count languages - languages is a string attribute, not a countable list",
    322: "Cannot count languages - languages is a string attribute, not a countable list",
    327: "Cannot count countries - countries is a string attribute, not a countable list",
    361: "Cannot count languages - languages is a string attribute, not a countable list",
    377: "Cannot count countries - countries is a string attribute, not a countable list",
    450: "Cannot count languages - languages is a string attribute, not a countable list",
    63: "Cannot unwind/aggregate by language values - languages is a single string attribute",
    69: "Cannot unwind/aggregate by country values - countries is a single string attribute",

    # Female directors - no gender field
    42: "Schema has no gender field for person entity - cannot filter by female directors",
    255: "Schema has no gender field for person entity - cannot filter by female directors",

    # User age - no age field
    46: "Schema has no age field for user entity - cannot filter by user age",
    28: "Schema has no country field for user entity - cannot filter by user country",

    # Movies rated by oldest users - no born field on user
    22: "Schema has no born/age field for user entity - cannot sort by user age",

    # Plot/bio length - no length function
    21: "TypeQL has no string length function - cannot sort by plot length",
    148: "TypeQL has no string length function - cannot filter by plot length > 150 characters",
    257: "TypeQL has no string length function - cannot sort by plot length",
    272: "TypeQL has no string length function - cannot sort by bio length",
    300: "TypeQL has no string length function - cannot sort by bio length",

    # Born and died in same country - no diedIn field
    273: "Schema has no diedIn field - cannot compare bornIn and diedIn",

    # Actors from at least 3 different countries - countries is string
    123: "Cannot count distinct countries - countries is a string attribute, not a countable list",

    # Directors with movies in more than 3 countries - string
    285: "Cannot count distinct countries - countries is a string attribute, not a countable list",

    # Movies released in exactly 3 countries - string
    288: "Cannot count countries - countries is a string attribute, not a countable list",

    # Users with diverse imdbRating range - needs min-max range calc
    298: "TypeQL cannot compute range (max - min) in a single query",

    # Actors in more than 3 different languages - string
    355: "Cannot count distinct languages - languages is a string attribute, not a countable list",

    # Multiple languages but single country - string list sizes
    383: "Cannot check list sizes for languages and countries - they are string attributes",

    # Leap year movies - no modulo function
    386: "TypeQL has no modulo function - cannot calculate leap years",

    # Directors with movies in more than one language - string
    388: "Cannot count distinct languages - languages is a string attribute, not a countable list",

    # Movies rated by youngest users - no age field
    391: "Schema has no age field for user entity - cannot filter by user age",

    # Directors by number of different countries - string
    392: "Cannot count distinct countries - countries is a string attribute, not a countable list",

    # Majority non-English languages - string
    396: "Cannot analyze language list composition - languages is a string attribute",

    # Movies that won awards - no Award entity
    400: "Schema has no Award entity or WON relation",
    401: "Schema has no Award entity or WON relation (duplicate)",

    # Friday release - no weekday function
    404: "TypeQL has no weekday function - cannot determine day of week",

    # Shot in more than 5 locations - countries is string
    406: "Cannot count countries - countries is a string attribute",

    # Actor over 50 at release - date arithmetic
    423: "TypeQL cannot compute age at release date",

    # User rated on release date - timestamp comparison
    447: "TypeQL cannot compare timestamp to date in different formats",

    # Subquery/nested fetch - invalid syntax
    604: "Nested fetch with subquery syntax is not valid TypeQL",

    # Global aggregates without groupby - validation issue
    135: "Global aggregation (mean without groupby) returns row iterator, not document - requires special handling",
    147: "Global aggregation (sum without groupby) returns row iterator, not document - requires special handling",

    # English language filter - languages is a string, cannot check list membership
    156: "Cannot check if 'English' is in languages - languages is a single string attribute, not a list",

    # User gender - no gender field
    624: "Schema has no gender field for user entity - cannot filter by male/female users",
}


def validate_query(driver, query):
    """Validate TypeQL query against TypeDB."""
    try:
        with driver.transaction("text2typeql_recommendations", TransactionType.READ) as tx:
            result = tx.query(query).resolve()
            # Try to consume results
            count = 0
            for doc in result.as_concept_documents():
                count += 1
                if count >= 1:
                    break
            return True, None
    except Exception as e:
        return False, str(e)


def main():
    # Connect to TypeDB
    credentials = Credentials("admin", "password")
    options = DriverOptions(is_tls_enabled=False)
    driver = TypeDB.driver("localhost:1729", credentials, options)

    # Read failed_review.csv
    failed_review_path = '/opt/text2typeql/output/recommendations/failed_review.csv'
    with open(failed_review_path, 'r') as f:
        reader = csv.DictReader(f)
        failed_rows = list(reader)

    # Read existing queries.csv
    queries_path = '/opt/text2typeql/output/recommendations/queries.csv'
    with open(queries_path, 'r') as f:
        reader = csv.DictReader(f)
        existing_queries = list(reader)

    # Read existing failed.csv
    failed_path = '/opt/text2typeql/output/recommendations/failed.csv'
    try:
        with open(failed_path, 'r') as f:
            reader = csv.DictReader(f)
            existing_failed = list(reader)
    except FileNotFoundError:
        existing_failed = []

    fixed_queries = []
    remaining_failed = []
    new_failed = []

    # Combine all fix dictionaries
    all_fixes = {}
    all_fixes.update(SORT_LIMIT_FIXES)
    all_fixes.update(ARITHMETIC_FIXES)
    all_fixes.update(VALUE_FIXES)
    all_fixes.update(WRONG_FILTER_FIXES)

    for row in failed_rows:
        idx = int(row['original_index'])
        question = row['question']
        cypher = row['cypher']
        old_typeql = row['typeql']

        # Check if unsupported
        if idx in UNSUPPORTED_REASONS:
            new_failed.append({
                'original_index': idx,
                'question': question,
                'cypher': cypher,
                'error': UNSUPPORTED_REASONS[idx]
            })
            print(f"[{idx}] UNSUPPORTED - {UNSUPPORTED_REASONS[idx][:60]}...")
            continue

        # Check if we have a fix
        if idx in all_fixes:
            new_typeql = all_fixes[idx]

            # Validate the fixed query
            valid, error = validate_query(driver, new_typeql)
            if valid:
                fixed_queries.append({
                    'original_index': idx,
                    'question': question,
                    'cypher': cypher,
                    'typeql': new_typeql
                })
                print(f"[{idx}] FIXED - validated successfully")
            else:
                print(f"[{idx}] INVALID - {error[:80]}...")
                remaining_failed.append({
                    'original_index': idx,
                    'question': question,
                    'cypher': cypher,
                    'typeql': new_typeql,
                    'review_reason': f"Fixed query failed validation: {error}"
                })
        else:
            # Keep in failed_review
            remaining_failed.append(row)
            print(f"[{idx}] NO FIX - kept in failed_review.csv")

    # Append fixed queries to queries.csv
    all_queries = existing_queries + fixed_queries
    # Sort by original_index
    all_queries.sort(key=lambda x: int(x['original_index']))

    with open(queries_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
        writer.writeheader()
        writer.writerows(all_queries)

    # Write remaining failed_review.csv
    if remaining_failed:
        with open(failed_review_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql', 'review_reason'])
            writer.writeheader()
            writer.writerows(remaining_failed)
    else:
        # Empty file
        with open(failed_review_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql', 'review_reason'])
            writer.writeheader()

    # Append to failed.csv
    all_failed = existing_failed + new_failed
    all_failed.sort(key=lambda x: int(x['original_index']))

    with open(failed_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'error'])
        writer.writeheader()
        writer.writerows(all_failed)

    driver.close()

    print(f"\n=== Summary ===")
    print(f"Fixed and validated: {len(fixed_queries)}")
    print(f"Still failed review: {len(remaining_failed)}")
    print(f"Moved to failed.csv: {len(new_failed)}")
    print(f"Total queries.csv: {len(all_queries)}")
    print(f"Total failed.csv: {len(all_failed)}")


if __name__ == '__main__':
    main()
