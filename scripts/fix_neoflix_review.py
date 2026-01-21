#!/usr/bin/env python3
"""Fix neoflix queries that failed semantic review."""

import csv
import sys
sys.path.insert(0, '/opt/text2typeql')

from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# ============================================================================
# CATEGORY 1: Date filter fixes - add date comparison
# These queries are missing date range/comparison filters
# ============================================================================

DATE_FILTER_FIXES = {
    # "Which movies were released in 1995?" - need year filter
    0: """match
  $m isa movie, has release_date $release_date, has title $title;
  $release_date >= 1995-01-01;
  $release_date < 1996-01-01;
fetch { "title": $title, "release_date": $release_date };""",

    # "Name 3 movies with a release date after 2000."
    19: """match
  $m isa movie, has release_date $release_date, has title $title;
  $release_date > 2000-01-01;
limit 3;
fetch { "title": $title };""",

    # "Display all movies that were released in the 1990s."
    55: """match
  $m isa movie, has release_date $release_date, has title $title;
  $release_date >= 1990-01-01;
  $release_date < 2000-01-01;
fetch { "title": $title, "release_date": $release_date };""",

    # "List the top 3 movies with the most revenue that were released before the year 2000."
    56: """match
  $m isa movie, has release_date $release_date, has revenue $revenue, has title $title;
  $release_date < 2000-01-01;
sort $revenue desc;
limit 3;
fetch { "title": $title, "revenue": $revenue };""",

    # "Display movies that have been rated after January 1, 2015."
    69: """match
  $m isa movie, has title $title_m;
  $u isa user;
  $r (rated_media: $m, reviewer: $u) isa rated, has rating $rating, has timestamp $timestamp;
  $timestamp >= 2015-01-01;
fetch { "movie": $title_m, "rating": $rating, "rated_at": $timestamp };""",

    # "Which movies were released after the year 2000?"
    94: """match
  $m isa movie, has release_date $release_date, has title $title;
  $release_date > 2000-01-01;
fetch { "title": $title, "release_date": $release_date };""",

    # "Which adult films have been released after 2010?"
    128: """match
  $a isa adult, has release_date $release_date, has title $title;
  $release_date > 2010-01-01;
fetch { "title": $title, "release_date": $release_date };""",

    # "List the first 5 subscriptions that expire in 2020."
    133: """match
  $s isa subscription, has expires_at $expires_at, has subscription_id $id;
  $expires_at >= 2020-01-01;
  $expires_at < 2021-01-01;
limit 5;
fetch { "subscription_id": $id, "expires_at": $expires_at };""",

    # "What are the top 3 movies by vote count that were released in the 1990s?"
    138: """match
  $m isa movie, has release_date $release_date, has title $title, has vote_count $vote_count;
  $release_date >= 1990-01-01;
  $release_date < 2000-01-01;
sort $vote_count desc;
limit 3;
fetch { "title": $title, "vote_count": $vote_count };""",

    # "Which 3 highest budget movies were released after 2000?"
    180: """match
  $m isa movie, has release_date $release_date, has budget $budget, has title $title;
  $release_date > 2000-01-01;
sort $budget desc;
limit 3;
fetch { "title": $title, "budget": $budget };""",

    # "List the first 3 movies released after 2010."
    248: """match
  $m isa movie, has release_date $release_date, has title $title;
  $release_date > 2010-01-01;
sort $release_date asc;
limit 3;
fetch { "title": $title, "release_date": $release_date };""",

    # "Which 3 movies have the most revenue among those released in the 1990s?"
    271: """match
  $m isa movie, has release_date $release_date, has revenue $revenue, has title $title;
  $release_date >= 1990-01-01;
  $release_date < 2000-01-01;
sort $revenue desc;
limit 3;
fetch { "title": $title, "revenue": $revenue };""",

    # "Name the top 5 movies that were released after 2010 and have a budget over 100 million USD."
    321: """match
  $m isa movie, has release_date $release_date, has budget $budget, has title $title;
  $release_date > 2010-01-01;
  $budget > 100000000;
sort $budget desc;
limit 5;
fetch { "title": $title, "budget": $budget, "release_date": $release_date };""",

    # "Which 3 movies have Tom Hanks as a cast member and have been released after 1995?"
    334: """match
  $p isa person, has person_name "Tom Hanks";
  $m isa movie, has release_date $release_date_m, has title $title_m;
  (actor: $p, film: $m) isa cast_for;
  $release_date_m > 1995-01-01;
limit 3;
fetch { "title": $title_m };""",

    # "Name the top 5 most popular movies released before 2000."
    347: """match
  $m isa movie, has release_date $release_date, has title $title, has popularity $popularity;
  $release_date < 2000-01-01;
sort $popularity desc;
limit 5;
fetch { "title": $title, "popularity": $popularity };""",

    # "List the top 5 highest grossing movies that have been released on or after January 1, 2000."
    354: """match
  $m isa movie, has release_date $release_date, has revenue $revenue, has title $title;
  $release_date >= 2000-01-01;
sort $revenue desc;
limit 5;
fetch { "title": $title, "revenue": $revenue };""",

    # "List the first 3 adult films that were released in the year 2000 or later."
    375: """match
  $a isa adult, has release_date $release_date, has title $title;
  $release_date >= 2000-01-01;
sort $release_date asc;
limit 3;
fetch { "title": $title, "release_date": $release_date };""",

    # "List the top 5 movies with a release date on or before December 31, 1990."
    378: """match
  $m isa movie, has release_date $release_date, has title $title;
  $release_date <= 1990-12-31;
sort $release_date desc;
limit 5;
fetch { "title": $title, "release_date": $release_date };""",

    # "Name the top 5 videos that were released in the year 2010 or later."
    385: """match
  $v isa video, has release_date $release_date, has title $title, has popularity $popularity;
  $release_date >= 2010-01-01;
sort $popularity desc;
limit 5;
fetch { "title": $title };""",

    # "List the top 5 movies released in the 1990s."
    397: """match
  $m isa movie, has release_date $release_date, has title $title, has popularity $popularity;
  $release_date >= 1990-01-01;
  $release_date < 2000-01-01;
sort $popularity desc;
limit 5;
fetch { "title": $title, "release_date": $release_date };""",

    # "What are the names of the top 3 most popular movies released before 2000?"
    404: """match
  $m isa movie, has release_date $release_date, has title $title, has popularity $popularity;
  $release_date < 2000-01-01;
sort $popularity desc;
limit 3;
fetch { "movie": $title, "popularity": $popularity };""",

    # "Which 3 movies have the longest runtime and were released after 2000?"
    433: """match
  $m isa movie, has release_date $release_date, has runtime $runtime, has title $title;
  $release_date > 2000-01-01;
sort $runtime desc;
limit 3;
fetch { "title": $title, "runtime": $runtime };""",

    # "List the top 5 movies that have a runtime longer than 150 minutes and were released before 2010."
    442: """match
  $m isa movie, has runtime $runtime, has release_date $release_date, has title $title;
  $runtime > 150;
  $release_date < 2010-01-01;
sort $runtime desc;
limit 5;
fetch { "title": $title, "runtime": $runtime, "release_date": $release_date };""",

    # "List the first 5 movies that have a tagline and were released after 2010."
    449: """match
  $m isa movie, has tagline $tagline, has release_date $release_date, has title $title;
  $release_date > 2010-01-01;
limit 5;
fetch { "title": $title, "tagline": $tagline, "release_date": $release_date };""",

    # "Which 3 movies have the lowest average vote and were released before 2000?"
    455: """match
  $m isa movie, has release_date $release_date, has average_vote $average_vote, has title $title;
  $release_date < 2000-01-01;
sort $average_vote asc;
limit 3;
fetch { "title": $title, "average_vote": $average_vote, "release_date": $release_date };""",

    # "What are the 3 most popular movies that have been released since 2015?"
    458: """match
  $m isa movie, has release_date $release_date, has popularity $popularity, has title $title;
  $release_date >= 2015-01-01;
sort $popularity desc;
limit 3;
fetch { "title": $title, "popularity": $popularity };""",

    # "Name the first 5 movies that have a release date before 1990."
    468: """match
  $m isa movie, has release_date $release_date, has title $title;
  $release_date < 1990-01-01;
limit 5;
fetch { "title": $title };""",

    # "Find all movies released before 2000 with an average vote greater than 8."
    488: """match
  $m isa movie, has release_date $release_date, has average_vote $average_vote, has title $title;
  $release_date < 2000-01-01;
  $average_vote > 8;
fetch { "title": $title, "average_vote": $average_vote, "release_date": $release_date };""",

    # "List the top 3 highest-grossing movies of 1995."
    512: """match
  $m isa movie, has release_date $release_date, has title $title, has revenue $revenue;
  $release_date >= 1995-01-01;
  $release_date < 1996-01-01;
sort $revenue desc;
limit 3;
fetch { "title": $title, "revenue": $revenue };""",

    # "Which movies are associated with the keyword 'family' and have a release date in the last 5 years?"
    514: """match
  $m isa movie, has title $title_m, has release_date $release_date_m;
  $k isa keyword, has keyword_name "family";
  (media: $m, keyword: $k) isa has_keyword;
  $release_date_m >= 2020-01-01;
fetch { "title": $title_m, "release_date": $release_date_m };""",

    # "List the movies with a release date on '1995-10-30'."
    542: """match
  $m isa movie, has release_date $release_date, has title $title;
  $release_date == 1995-10-30;
fetch { "title": $title, "release_date": $release_date };""",
}

# ============================================================================
# CATEGORY 2: Missing country relation fixes
# ============================================================================

COUNTRY_RELATION_FIXES = {
    # "Display movies that have been released in the 'United States of America'."
    50: """match
  $m isa movie, has status $status_m, has title $title;
  $c isa country, has country_name "United States of America";
  (media: $m, country: $c) isa produced_in_country;
  $status_m == "Released";
fetch { "title": $title, "status": $status_m };""",

    # "List the top 3 movies released in the United States of America."
    174: """match
  $m isa movie, has title $title, has release_date $release_date, has popularity $popularity;
  $c isa country, has country_name "United States of America";
  (media: $m, country: $c) isa produced_in_country;
sort $popularity desc;
limit 3;
fetch { "title": $title, "release_date": $release_date, "popularity": $popularity };""",

    # "Which 3 adult films have the lowest average votes but were produced in 'United States of America'?"
    244: """match
  $a isa adult, has title $title, has average_vote $average_vote;
  $c isa country, has country_name "United States of America";
  (media: $a, country: $c) isa produced_in_country;
sort $average_vote asc;
limit 3;
fetch { "title": $title, "average_vote": $average_vote };""",

    # "Name the top 5 adult videos that were produced in Italy."
    286: """match
  $a isa adult, has title $title, has popularity $popularity;
  $c isa country, has country_name "Italy";
  (media: $a, country: $c) isa produced_in_country;
sort $popularity desc;
limit 5;
fetch { "title": $title };""",

    # "List the first 3 movies produced in the United States."
    317: """match
  $m isa movie, has title $title;
  $c isa country, has country_name "United States of America";
  (media: $m, country: $c) isa produced_in_country;
limit 3;
fetch { "title": $title };""",

    # "Which 3 movies have an average vote less than 5 and have been produced in the country with ID 'US'?"
    355: """match
  $m isa movie, has average_vote $average_vote, has title $title;
  $c isa country, has country_id "US";
  (media: $m, country: $c) isa produced_in_country;
  $average_vote < 5;
limit 3;
fetch { "title": $title };""",

    # "Which 3 movies have an average vote greater than 8 and have been produced in 'France'?"
    383: """match
  $m isa movie, has average_vote $average_vote, has title $title;
  $c isa country, has country_name "France";
  (media: $m, country: $c) isa produced_in_country;
  $average_vote > 8;
limit 3;
fetch { "title": $title };""",

    # "Which 3 movies have the highest revenue and were released in the 'United States of America'?"
    426: """match
  $m isa movie, has title $title, has revenue $revenue;
  $c isa country, has country_name "United States of America";
  (media: $m, country: $c) isa produced_in_country;
sort $revenue desc;
limit 3;
fetch { "title": $title, "revenue": $revenue };""",

    # "List the first 5 movies that were produced in the country 'United States of America' and have a revenue over 50 million dollars."
    438: """match
  $m isa movie, has revenue $revenue, has title $title;
  $c isa country, has country_name "United States of America";
  (media: $m, country: $c) isa produced_in_country;
  $revenue > 50000000;
limit 5;
fetch { "title": $title, "revenue": $revenue };""",

    # "Name the first 3 adult films that were produced in the country 'Italy'."
    451: """match
  $a isa adult, has title $title;
  $c isa country, has country_name "Italy";
  (media: $a, country: $c) isa produced_in_country;
limit 3;
fetch { "title": $title };""",

    # "Which 3 movies have the highest revenue and were released in the 'United Kingdom'?"
    477: """match
  $m isa movie, has title $title, has revenue $revenue;
  $c isa country, has country_name "United Kingdom";
  (media: $m, country: $c) isa produced_in_country;
sort $revenue desc;
limit 3;
fetch { "title": $title, "revenue": $revenue };""",
}

# ============================================================================
# CATEGORY 3: Missing genre relation fixes
# ============================================================================

GENRE_RELATION_FIXES = {
    # "List the top 3 highest grossing movies in the genre 'Action'."
    232: """match
  $m isa movie, has title $title, has revenue $revenue;
  $g isa genre, has genre_name "Action";
  (media: $m, genre: $g) isa in_genre;
sort $revenue desc;
limit 3;
fetch { "title": $title, "revenue": $revenue };""",

    # "What are the first 3 movies with the genre 'Drama' that have a budget less than 10 million USD?"
    367: """match
  $m isa movie, has budget $budget, has title $title;
  $g isa genre, has genre_name "Drama";
  (media: $m, genre: $g) isa in_genre;
  $budget < 10000000;
limit 3;
fetch { "title": $title };""",

    # "What are the 3 most popular movies in the genre 'Comedy'?"
    447: """match
  $m isa movie, has title $title, has popularity $popularity;
  $g isa genre, has genre_name "Comedy";
  (media: $m, genre: $g) isa in_genre;
sort $popularity desc;
limit 3;
fetch { "title": $title, "popularity": $popularity };""",
}

# ============================================================================
# CATEGORY 4: Missing job filter (Director, Producer, etc.)
# ============================================================================

JOB_FILTER_FIXES = {
    # "Which movies have Tom Hanks credited as a producer?"
    75: """match
  $p isa person, has person_name "Tom Hanks";
  $m isa movie, has title $title_m;
  (crew_member: $p, film: $m) isa crew_for, has job "Producer";
fetch { "title": $title_m };""",

    # "List the first 3 movies directed by a person with gender 2."
    183: """match
  $p isa person, has gender 2;
  $m isa movie, has title $title_m;
  (crew_member: $p, film: $m) isa crew_for, has job "Director";
limit 3;
fetch { "title": $title_m };""",

    # "What are the first 3 movies directed by a person named 'John Doe'?"
    320: """match
  $p isa person, has person_name "John Doe";
  $m isa movie, has title $title_m;
  (crew_member: $p, film: $m) isa crew_for, has job "Director";
limit 3;
fetch { "title": $title_m };""",

    # "List the first 3 persons who have directed a movie with a budget over 150 million USD."
    356: """match
  $p isa person, has person_name $person_name_p;
  $m isa movie, has budget $budget_m;
  (crew_member: $p, film: $m) isa crew_for, has job "Director";
  $budget_m > 150000000;
limit 3;
fetch { "name": $person_name_p };""",

    # "What are the top 3 highest-grossing movies directed by a person named 'Tom Hanks'?"
    413: """match
  $p isa person, has person_name "Tom Hanks";
  $m isa movie, has title $title_m, has revenue $revenue_m;
  (crew_member: $p, film: $m) isa crew_for, has job "Director";
sort $revenue_m desc;
limit 3;
fetch { "movie": $title_m, "revenue": $revenue_m };""",

    # "Which genres are associated with movies directed by 'Steven Spielberg'?"
    493: """match
  $p isa person, has person_name "Steven Spielberg";
  $m isa movie;
  $g isa genre, has genre_name $genre_name_g;
  (crew_member: $p, film: $m) isa crew_for, has job "Director";
  (media: $m, genre: $g) isa in_genre;
fetch { "genre": $genre_name_g };""",

    # "What are the names of people who have directed movies with a budget over 50 million USD?"
    497: """match
  $p isa person, has person_name $person_name_p;
  $m isa movie, has budget $budget_m;
  (crew_member: $p, film: $m) isa crew_for, has job "Director";
  $budget_m > 50000000;
fetch { "name": $person_name_p };""",

    # "What are the movies directed by 'Christopher Nolan'?"
    526: """match
  $p isa person, has person_name "Christopher Nolan";
  $m isa movie, has title $title_m;
  (crew_member: $p, film: $m) isa crew_for, has job "Director";
fetch { "title": $title_m };""",
}

# ============================================================================
# CATEGORY 5: Missing value filter (equality, comparison)
# ============================================================================

VALUE_FILTER_FIXES = {
    # "Name 3 movies with a budget of exactly 30 million."
    38: """match
  $m isa movie, has budget $budget, has title $title;
  $budget == 30000000;
limit 3;
fetch { "title": $title };""",

    # "Which movies have been rated exactly 5.0 by any user?"
    54: """match
  $m isa movie, has title $title_m;
  $u isa user;
  $r (rated_media: $m, reviewer: $u) isa rated, has rating $rating;
  $rating == 5.0;
fetch { "title": $title_m };""",

    # "Find all movies where Tom Hanks is listed first in the cast."
    61: """match
  $p isa person, has person_name "Tom Hanks";
  $m isa movie, has title $title;
  (actor: $p, film: $m) isa cast_for, has cast_order 0;
fetch { "title": $title };""",

    # "Identify all movies that were produced in a country other than the United States."
    66: """match
  $m isa movie, has title $title_m;
  $c isa country, has country_name $country_name_c;
  (media: $m, country: $c) isa produced_in_country;
  not { $country_name_c == "United States of America"; };
fetch { "title": $title_m, "name": $country_name_c };""",

    # "Find all movies that have a runtime of exactly 90 minutes."
    74: """match
  $m isa movie, has runtime $runtime, has title $title;
  $runtime == 90;
fetch { "title": $title, "runtime": $runtime };""",

    # "Display movies that have a revenue of zero."
    86: """match
  $m isa movie, has revenue $revenue, has title $title;
  $revenue == 0;
fetch { "title": $title, "revenue": $revenue };""",

    # "Which production companies have produced movies with a budget over 50 million?"
    101: """match
  $c isa production_company, has production_company_name $production_company_name_c;
  $m isa movie, has budget $budget_m;
  (media: $m, producer: $c) isa produced_by;
  $budget_m > 50000000;
fetch { "company": $production_company_name_c };""",

    # "What are the prices of the packages named 'Gold' and 'Platinum'?"
    134: """match
  $p isa package, has package_name $package_name, has package_price $package_price;
  { $package_name == "Gold"; } or { $package_name == "Platinum"; };
fetch { "package": $package_name, "price": $package_price };""",

    # "List the first 3 movies with a revenue of zero."
    155: """match
  $m isa movie, has revenue $revenue, has title $title;
  $revenue == 0;
limit 3;
fetch { "title": $title };""",

    # "List the first 3 videos with no revenue reported."
    162: """match
  $v isa video, has revenue $revenue, has title $title;
  $revenue == 0;
limit 3;
fetch { "title": $title, "revenue": $revenue };""",

    # "What are the first 3 videos that have been rated higher than 8.0?"
    168: """match
  $v isa video, has title $title_v;
  $u isa user;
  $r (rated_media: $v, reviewer: $u) isa rated, has rating $rating;
  $rating > 8.0;
sort $rating desc;
limit 3;
fetch { "title": $title_v, "rating": $rating };""",

    # "What are the first 5 movies rated by user with ID 1?" - add sort
    178: """match
  $u isa user, has user_id "1";
  $m isa movie, has title $title_m;
  $r (reviewer: $u, rated_media: $m) isa rated, has rating $rating, has timestamp $timestamp;
sort $timestamp asc;
limit 5;
fetch { "title": $title_m, "rating": $rating };""",

    # "Show the top 3 movies that have been reviewed most recently by any user." - add sort
    201: """match
  $m isa movie, has title $title_m;
  $u isa user;
  $r (rated_media: $m, reviewer: $u) isa rated, has timestamp $timestamp;
sort $timestamp desc;
limit 3;
fetch { "movie": $title_m, "reviewedAt": $timestamp };""",

    # "What are the first 5 movies that have a character played by a person with gender 1?"
    233: """match
  $p isa person, has gender $gender_p;
  $m isa movie, has title $title_m;
  (actor: $p, film: $m) isa cast_for;
  $gender_p == 1;
limit 5;
fetch { "title": $title_m };""",

    # "Name the top 5 persons who have played the character 'Woody' in any movie."
    329: """match
  $p isa person, has person_name $person_name_p;
  $m isa movie, has popularity $popularity_m;
  (actor: $p, film: $m) isa cast_for, has character $character;
  $character == "Woody";
sort $popularity_m desc;
limit 5;
fetch { "name": $person_name_p };""",

    # "List the top 5 movies that have been rated 10 by any user."
    338: """match
  $m isa movie, has title $title_m, has release_date $release_date_m, has popularity $popularity_m;
  $u isa user;
  $r (rated_media: $m, reviewer: $u) isa rated, has rating $rating;
  $rating == 10;
sort $popularity_m desc;
limit 5;
fetch { "title": $title_m, "release_date": $release_date_m, "popularity": $popularity_m };""",

    # "What are the first 3 production companies that produced videos with zero revenue?"
    344: """match
  $v isa video, has revenue $revenue_v;
  $c isa production_company, has production_company_name $production_company_name_c;
  (media: $v, producer: $c) isa produced_by;
  $revenue_v == 0;
limit 3;
fetch { "name": $production_company_name_c };""",

    # "What are the first 3 movies that have a budget exactly 50 million USD?"
    381: """match
  $m isa movie, has budget $budget, has title $title;
  $budget == 50000000;
limit 3;
fetch { "title": $title };""",

    # "List the top 5 movies that have a runtime exactly 90 minutes."
    384: """match
  $m isa movie, has runtime $runtime, has title $title, has popularity $popularity;
  $runtime == 90;
sort $popularity desc;
limit 5;
fetch { "title": $title, "runtime": $runtime };""",

    # "What are the first 3 movies that have been rated by the user with ID '1'?" - add sort
    377: """match
  $u isa user, has user_id "1";
  $m isa movie, has title $title_m;
  $r (reviewer: $u, rated_media: $m) isa rated, has timestamp $timestamp;
sort $timestamp asc;
limit 3;
fetch { "title": $title_m };""",

    # "Which movies have a runtime that is exactly 90 minutes?"
    507: """match
  $m isa movie, has runtime $runtime, has title $title;
  $runtime == 90;
fetch { "title": $title };""",

    # "List the movies that have a budget of zero."
    509: """match
  $m isa movie, has budget $budget, has title $title;
  $budget == 0;
fetch { "title": $title };""",

    # "Which movies have a popularity less than 5 and more than 2?" - add fetch
    516: """match
  $m isa movie, has popularity $popularity, has title $title;
  $popularity < 5;
  $popularity > 2;
fetch { "title": $title, "popularity": $popularity };""",

    # "List all movies that have an average vote of exactly 7.7."
    522: """match
  $m isa movie, has average_vote $average_vote, has title $title;
  $average_vote == 7.7;
fetch { "title": $title };""",

    # "List the movies that have been rated exactly 5.0."
    525: """match
  $m isa movie, has title $title_m;
  $u isa user;
  $r (rated_media: $m, reviewer: $u) isa rated, has rating $rating;
  $rating == 5.0;
fetch { "title": $title_m };""",

    # "List all movies that have an average_vote less than 3." - add fetch
    536: """match
  $m isa movie, has average_vote $average_vote, has title $title;
  $average_vote < 3;
fetch { "title": $title, "average_vote": $average_vote };""",
}

# ============================================================================
# CATEGORY 6: Missing comparison filter (revenue > budget, etc.)
# ============================================================================

COMPARISON_FILTER_FIXES = {
    # "Which movies have a revenue greater than their budget?"
    139: """match
  $m isa movie, has revenue $revenue, has budget $budget, has title $title;
  $revenue > $budget;
fetch { "title": $title, "budget": $budget, "revenue": $revenue };""",

    # "List the movies that have a revenue greater than their budget."
    538: """match
  $m isa movie, has revenue $revenue, has budget $budget, has title $title;
  $revenue > $budget;
fetch { "title": $title, "budget": $budget, "revenue": $revenue };""",
}

# ============================================================================
# CATEGORY 7: Ratio calculations - use let expression
# ============================================================================

RATIO_CALCULATION_FIXES = {
    # "Display the top 5 movies with the highest budget to revenue ratio."
    63: """match
  $m isa movie, has budget $budget, has revenue $revenue, has title $title;
  $revenue > 0;
let $ratio = $budget / $revenue;
sort $ratio desc;
limit 5;
fetch { "title": $title, "budget": $budget, "revenue": $revenue, "budget_revenue_ratio": $ratio };""",

    # "What are the first 3 movies with the highest budget to revenue ratio?"
    150: """match
  $m isa movie, has budget $budget, has revenue $revenue, has title $title;
  $revenue > 0;
let $ratio = $budget / $revenue;
sort $ratio desc;
limit 3;
fetch { "title": $title, "budget": $budget, "ratio": $ratio };""",

    # "What are the top 3 most budget-efficient adult films (highest revenue to budget ratio)?"
    171: """match
  $a isa adult, has budget $budget, has revenue $revenue, has title $title;
  $budget > 0;
let $efficiency = $revenue / $budget;
sort $efficiency desc;
limit 3;
fetch { "title": $title, "efficiency": $efficiency };""",

    # "Which 3 movies have the highest discrepancy between budget and revenue?"
    230: """match
  $m isa movie, has budget $budget, has revenue $revenue, has title $title;
let $profit = $revenue - $budget;
sort $profit desc;
limit 3;
fetch { "title": $title, "profit": $profit, "budget": $budget, "revenue": $revenue };""",

    # "Name the top 5 movies with the most budget to revenue ratio."
    266: """match
  $m isa movie, has budget $budget, has revenue $revenue, has title $title;
  $revenue > 0;
let $ratio = $budget / $revenue;
sort $ratio desc;
limit 5;
fetch { "title": $title, "budget": $budget, "ratio": $ratio };""",

    # "List the top 5 movies where the revenue exceeded the budget by over 100 million USD."
    368: """match
  $m isa movie, has revenue $revenue, has budget $budget, has title $title;
let $profit = $revenue - $budget;
  $profit > 100000000;
sort $profit desc;
limit 5;
fetch { "title": $title, "revenue": $revenue, "budget": $budget };""",
}

# ============================================================================
# CATEGORY 8: Missing in_collection relation
# ============================================================================

COLLECTION_RELATION_FIXES = {
    # "List the first 5 movies that are part of a collection and have a revenue greater than 100 million dollars."
    427: """match
  $m isa movie, has revenue $revenue, has title $title;
  $col isa collection;
  (media: $m, collection: $col) isa in_collection;
  $revenue > 100000000;
sort $revenue desc;
limit 5;
fetch { "title": $title, "revenue": $revenue };""",

    # "List the top 5 movies that have been part of at least one collection."
    464: """match
  $m isa movie, has title $title, has popularity $popularity;
  $col isa collection;
  (media: $m, collection: $col) isa in_collection;
sort $popularity desc;
limit 5;
fetch { "title": $title, "popularity": $popularity };""",

    # "List all movies that are part of any collection and have a revenue less than 1 million USD."
    499: """match
  $m isa movie, has revenue $revenue, has title $title;
  $col isa collection;
  (media: $m, collection: $col) isa in_collection;
  $revenue < 1000000;
fetch { "title": $title, "revenue": $revenue };""",
}

# ============================================================================
# CATEGORY 9: Missing produced_by relation
# ============================================================================

PRODUCED_BY_RELATION_FIXES = {
    # "Which production companies have produced movies that are part of a collection?"
    152: """match
  $c isa collection;
  $m isa movie;
  $p isa production_company, has production_company_name $production_company_name_p;
  (collection: $c, media: $m) isa in_collection;
  (media: $m, producer: $p) isa produced_by;
fetch { "ProductionCompany": $production_company_name_p };""",

    # "What are the first 5 production companies producing films in the 'Animation' genre?"
    406: """match
  $g isa genre, has genre_name "Animation";
  $m isa movie;
  $c isa production_company, has production_company_name $production_company_name_c;
  (genre: $g, media: $m) isa in_genre;
  (media: $m, producer: $c) isa produced_by;
sort $production_company_name_c asc;
limit 5;
fetch { "company": $production_company_name_c };""",

    # "List the production companies that have produced movies with an original language of Spanish."
    515: """match
  $m isa movie;
  $l isa language, has language_name "Spanish";
  $c isa production_company, has production_company_name $production_company_name_c;
  (media: $m, language: $l) isa original_language;
  (media: $m, producer: $c) isa produced_by;
fetch { "name": $production_company_name_c };""",
}

# ============================================================================
# CATEGORY 10: LIKE pattern fixes (STARTS WITH, ENDS WITH, CONTAINS)
# ============================================================================

LIKE_PATTERN_FIXES = {
    # "List the first 3 movies that have a poster path ending in '.jpg'."
    21: """match
  $m isa movie, has poster_path $poster_path, has title $title;
  $poster_path like ".*[.]jpg";
limit 3;
fetch { "title": $title };""",

    # "List the first 3 movies with an IMDb ID starting with 'tt'."
    31: """match
  $m isa movie, has imdb_id $imdb_id, has title $title;
  $imdb_id like "tt.*";
limit 3;
fetch { "title": $title, "imdb_id": $imdb_id };""",

    # "List the first 5 movies with a poster path ending in '.jpg'."
    146: """match
  $m isa movie, has poster_path $poster_path, has title $title;
  $poster_path like ".*[.]jpg";
limit 5;
fetch { "title": $title, "poster_path": $poster_path };""",

    # "Which movies have a cast member with the profile path ending in 'jpg'?"
    154: """match
  $m isa movie, has title $title_m;
  $p isa person, has profile_path $profile_path_p;
  (film: $m, actor: $p) isa cast_for;
  $profile_path_p like ".*[.]jpg";
fetch { "title": $title_m };""",

    # "What are the top 5 movies with a tagline containing the word 'adventure'?"
    195: """match
  $m isa movie, has tagline $tagline, has title $title, has popularity $popularity;
  $tagline like ".*[Aa]dventure.*";
sort $popularity desc;
limit 5;
fetch { "title": $title, "tagline": $tagline };""",

    # "Show the first 5 adult films based on their IMDB ID starting with 'tt007'."
    245: """match
  $a isa adult, has imdb_id $imdb_id, has title $title;
  $imdb_id like "tt007.*";
sort $imdb_id asc;
limit 5;
fetch { "title": $title, "imdb_id": $imdb_id };""",

    # "Name the top 5 movies with a tagline containing the word 'love'."
    272: """match
  $m isa movie, has tagline $tagline, has title $title, has popularity $popularity;
  $tagline like ".*[Ll]ove.*";
sort $popularity desc;
limit 5;
fetch { "title": $title };""",

    # "Name the top 5 movies that have an IMDb ID starting with 'tt'."
    376: """match
  $m isa movie, has imdb_id $imdb_id, has title $title, has popularity $popularity;
  $imdb_id like "tt.*";
sort $popularity desc;
limit 5;
fetch { "title": $title };""",

    # "Name the first 5 movies that have a poster path ending in '.jpg'."
    424: """match
  $m isa movie, has poster_path $poster_path, has title $title;
  $poster_path like ".*[.]jpg";
limit 5;
fetch { "title": $title };""",

    # "Name the first 3 movies that have a poster path ending in '/pQFoyx7rp09CJTAb932F2g8Nlho.jpg'."
    462: """match
  $m isa movie, has poster_path $poster_path, has title $title;
  $poster_path like ".*/pQFoyx7rp09CJTAb932F2g8Nlho[.]jpg";
limit 3;
fetch { "title": $title };""",

    # "What are the movies with a poster path that ends with 'T4B.jpg'?"
    523: """match
  $m isa movie, has poster_path $poster_path, has title $title;
  $poster_path like ".*T4B[.]jpg";
fetch { "title": $title };""",

    # "Which movies have a title that starts with 'The'?"
    543: """match
  $m isa movie, has title $title;
  $title like "The.*";
fetch { "title": $title };""",
}

# ============================================================================
# CATEGORY 11: NOT EXISTS / negation patterns
# ============================================================================

NEGATION_FIXES = {
    # "List all movies produced in countries where the primary language is not English."
    84: """match
  $m isa movie, has title $title_m;
  $c isa country, has country_name $country_name_c;
  (media: $m, country: $c) isa produced_in_country;
  not {
    $l isa language, has language_name "English";
    (media: $m, language: $l) isa original_language;
  };
fetch { "movie": $title_m, "country": $country_name_c };""",

    # "Show the first 5 movies where the original language is not English but have English as a spoken language."
    231: """match
  $m isa movie, has title $title_m;
  $ol isa language, has language_name $ol_name;
  $en isa language, has language_name "English";
  (media: $m, language: $ol) isa original_language;
  (media: $m, language: $en) isa spoken_in_language;
  not { $ol_name == "English"; };
limit 5;
fetch { "title": $title_m };""",

    # "What are the first 3 movies that are not part of any collection?"
    269: """match
  $m isa movie, has title $title;
  not { (media: $m) isa in_collection; };
limit 3;
fetch { "title": $title };""",

    # "What are the first 3 adult videos not produced in any country listed?"
    287: """match
  $a isa adult, has title $title;
  not { (media: $a) isa produced_in_country; };
limit 3;
fetch { "title": $title };""",

    # "What are the first 5 actors who have starred in both a movie and an adult film?"
    # NOTE: Adult entity doesn't play cast_for:film role - this query is unsupported
}

# ============================================================================
# CATEGORY 12: Dual language filter (English AND Spanish)
# ============================================================================

DUAL_LANGUAGE_FIXES = {
    # "Find the first 3 movies that feature both English and Spanish languages."
    192: """match
  $m isa movie, has title $title_m;
  $l1 isa language, has language_name "English";
  $l2 isa language, has language_name "Spanish";
  (media: $m, language: $l1) isa spoken_in_language;
  (media: $m, language: $l2) isa spoken_in_language;
limit 3;
fetch { "title": $title_m };""",

    # "List all movies where the spoken language includes both English and Spanish."
    533: """match
  $m isa movie, has title $title_m;
  $l1 isa language, has language_name "English";
  $l2 isa language, has language_name "Spanish";
  (media: $m, language: $l1) isa spoken_in_language;
  (media: $m, language: $l2) isa spoken_in_language;
fetch { "title": $title_m };""",
}

# ============================================================================
# CATEGORY 13: Show the first 5 movies that were originally titled differently from their English title.
# ============================================================================

TITLE_COMPARISON_FIXES = {
    # "Show the first 5 movies that were originally titled differently from their English title."
    197: """match
  $m isa movie, has title $title, has original_title $original_title;
  not { $title == $original_title; };
limit 5;
fetch { "title": $title, "original_title": $original_title };""",

    # "What are the top 5 movies that have undergone a title change after release?"
    229: """match
  $m isa movie, has title $title, has original_title $original_title, has popularity $popularity;
  not { $title == $original_title; };
sort $popularity desc;
limit 5;
fetch { "title": $title, "original_title": $original_title };""",
}

# ============================================================================
# CATEGORY 14: Sort fixes (add missing sort)
# ============================================================================

SORT_FIXES = {
    # "What are the first 3 languages spoken in movies with a budget over $50 million?"
    276: """match
  $m isa movie, has budget $budget_m;
  $l isa language, has language_name $language_name_l;
  (media: $m, language: $l) isa spoken_in_language;
  $budget_m > 50000000;
sort $language_name_l asc;
limit 3;
fetch { "language": $language_name_l };""",
}

# ============================================================================
# CATEGORY 15: Wrong filter fixes (character name vs person name)
# ============================================================================

CHARACTER_FILTER_FIXES = {
    # "Show the first 5 movies that have a character named 'Charlie Wilson' in their cast."
    223: """match
  $m isa movie, has title $title_m;
  $p isa person;
  (actor: $p, film: $m) isa cast_for, has character $character;
  $character == "Charlie Wilson";
limit 5;
fetch { "title": $title_m };""",
}

# ============================================================================
# CATEGORY 16: Unsupported features - move to failed.csv
# ============================================================================

UNSUPPORTED = {
    # "Name 3 movies spoken in more than one language." - requires groupby+filter
    29: "Groupby with HAVING filter not supported in TypeQL",

    # "Which movies have been produced in more than one country?" - requires groupby+filter
    32: "Groupby with HAVING filter not supported in TypeQL",

    # "Which movies have been translated into more than three languages?" - requires groupby+filter
    59: "Groupby with HAVING filter not supported in TypeQL",

    # "Which movies are associated with more than one production company?" - requires groupby+filter
    62: "Groupby with HAVING filter not supported in TypeQL",

    # "Which movies have more than three genres associated with them?" - requires groupby+filter
    81: "Groupby with HAVING filter not supported in TypeQL",

    # "List the first 5 movies that have been associated with more than one genre." - requires groupby+filter
    116: "Groupby with HAVING filter not supported in TypeQL",

    # "Which languages have more than 5 movies spoken in them?" - requires groupby+filter
    118: "Groupby with HAVING filter not supported in TypeQL",

    # "List the top 3 most controversial adult films based on their overview." - length of string
    126: "String length function not supported in TypeQL",

    # "Which countries have produced more than 10 movies in the database?" - requires groupby+filter
    143: "Groupby with HAVING filter not supported in TypeQL",

    # "Which movies have been produced in more than one country?" - requires groupby+filter
    145: "Groupby with HAVING filter not supported in TypeQL",

    # "Which movies have been translated into more than three languages?" - requires groupby+filter
    148: "Groupby with HAVING filter not supported in TypeQL",

    # "Which movies have the most keywords associated with them?" - requires groupby+sort
    70: "Groupby with sort not fully supported in TypeQL",

    # "What are the top 5 movies that premiered in the last quarter of any year?" - month extraction
    191: "Date month extraction not supported in TypeQL",

    # "What are the first 5 movies that have no revenue recorded?" - IS NULL
    199: "IS NULL semantics differ - having attribute means not null",

    # "What are the first 5 movies with a release date on a weekend?" - day of week
    217: "Day of week extraction not supported in TypeQL",

    # "Which 3 adult films have been released during a leap year?" - leap year check
    240: "Leap year date check not supported in TypeQL",

    # "What are the first 3 movies with the most characters in their overview?" - string length
    255: "String length function not supported in TypeQL",

    # "Which 3 persons have the most crew credits in movies released after 2010?" - requires groupby+sort+date
    365: "Groupby with date filter and sort not fully supported",

    # "List the first 3 persons who have worked as producers in more than 5 movies." - requires groupby+filter
    370: "Groupby with HAVING filter not supported in TypeQL",

    # "Which 3 movies have the highest popularity and were released in the last 5 years?" - relative date
    444: "Relative date (now - 5 years) not supported in TypeQL",

    # "Which movies have been produced by companies based in 'France'?" - production company location
    524: "Production company country relation not in schema",

    # "Which movies have a budget less than 1 million USD and have won an award?" - award relation
    529: "Award relation not in schema",

    # "What are the first 5 actors who have starred in both a movie and an adult film?"
    421: "Adult entity does not play cast_for:film role in schema",

    # "List all movies that have a revenue of zero but have been highly rated."
    530: """match
  $m isa movie, has revenue $revenue, has average_vote $average_vote, has title $title;
  $revenue == 0;
  $average_vote >= 7.0;
fetch { "title": $title, "average_vote": $average_vote, "revenue": $revenue };""",

    # "List the movies that have been produced in more than three countries." - requires groupby+filter
    528: "Groupby with HAVING filter not supported in TypeQL",
}


def validate_query(driver, query):
    """Validate TypeQL query against TypeDB."""
    try:
        with driver.transaction("text2typeql_neoflix", TransactionType.READ) as tx:
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


def get_fix(idx):
    """Get the fix for a given index, returns (fix_type, fix_value)."""
    if idx in DATE_FILTER_FIXES:
        return ("typeql", DATE_FILTER_FIXES[idx])
    if idx in COUNTRY_RELATION_FIXES:
        return ("typeql", COUNTRY_RELATION_FIXES[idx])
    if idx in GENRE_RELATION_FIXES:
        return ("typeql", GENRE_RELATION_FIXES[idx])
    if idx in JOB_FILTER_FIXES:
        return ("typeql", JOB_FILTER_FIXES[idx])
    if idx in VALUE_FILTER_FIXES:
        return ("typeql", VALUE_FILTER_FIXES[idx])
    if idx in COMPARISON_FILTER_FIXES:
        return ("typeql", COMPARISON_FILTER_FIXES[idx])
    if idx in RATIO_CALCULATION_FIXES:
        return ("typeql", RATIO_CALCULATION_FIXES[idx])
    if idx in COLLECTION_RELATION_FIXES:
        return ("typeql", COLLECTION_RELATION_FIXES[idx])
    if idx in PRODUCED_BY_RELATION_FIXES:
        return ("typeql", PRODUCED_BY_RELATION_FIXES[idx])
    if idx in LIKE_PATTERN_FIXES:
        return ("typeql", LIKE_PATTERN_FIXES[idx])
    if idx in NEGATION_FIXES:
        return ("typeql", NEGATION_FIXES[idx])
    if idx in DUAL_LANGUAGE_FIXES:
        return ("typeql", DUAL_LANGUAGE_FIXES[idx])
    if idx in TITLE_COMPARISON_FIXES:
        return ("typeql", TITLE_COMPARISON_FIXES[idx])
    if idx in SORT_FIXES:
        return ("typeql", SORT_FIXES[idx])
    if idx in CHARACTER_FILTER_FIXES:
        return ("typeql", CHARACTER_FILTER_FIXES[idx])
    if idx in UNSUPPORTED:
        value = UNSUPPORTED[idx]
        if value.startswith("match"):
            # It's actually a fix, not an error
            return ("typeql", value)
        return ("unsupported", value)
    return (None, None)


def main():
    # Connect to TypeDB
    credentials = Credentials("admin", "password")
    options = DriverOptions(is_tls_enabled=False)
    driver = TypeDB.driver("localhost:1729", credentials, options)

    # Read failed_review.csv
    failed_review_path = '/opt/text2typeql/output/neoflix/failed_review.csv'
    with open(failed_review_path, 'r') as f:
        reader = csv.DictReader(f)
        failed_rows = list(reader)

    # Read existing queries.csv
    queries_path = '/opt/text2typeql/output/neoflix/queries.csv'
    with open(queries_path, 'r') as f:
        reader = csv.DictReader(f)
        existing_queries = list(reader)

    # Read existing failed.csv
    failed_path = '/opt/text2typeql/output/neoflix/failed.csv'
    try:
        with open(failed_path, 'r') as f:
            reader = csv.DictReader(f)
            existing_failed = list(reader)
    except FileNotFoundError:
        existing_failed = []

    fixed_queries = []
    remaining_failed = []
    new_failed = []

    for row in failed_rows:
        idx = int(row['original_index'])
        question = row['question']
        cypher = row['cypher']
        old_typeql = row['typeql']

        fix_type, fix_value = get_fix(idx)

        if fix_type == "typeql":
            # Validate the fixed query
            valid, error = validate_query(driver, fix_value)
            if valid:
                fixed_queries.append({
                    'original_index': idx,
                    'question': question,
                    'cypher': cypher,
                    'typeql': fix_value
                })
                print(f"[{idx}] FIXED - validated successfully")
            else:
                print(f"[{idx}] INVALID - {error[:100]}...")
                remaining_failed.append({
                    'original_index': idx,
                    'question': question,
                    'cypher': cypher,
                    'typeql': fix_value,
                    'review_reason': f"Fixed query failed validation: {error}"
                })
        elif fix_type == "unsupported":
            # Move to failed.csv
            new_failed.append({
                'original_index': idx,
                'question': question,
                'cypher': cypher,
                'error': fix_value
            })
            print(f"[{idx}] UNSUPPORTED - {fix_value}")
        else:
            # Keep in failed_review
            remaining_failed.append(row)
            print(f"[{idx}] NO FIX - kept in failed_review.csv")

    # Remove any existing queries with the same original_index as fixed ones
    fixed_indices = {int(q['original_index']) for q in fixed_queries}
    existing_queries = [q for q in existing_queries if int(q['original_index']) not in fixed_indices]

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


if __name__ == '__main__':
    main()
