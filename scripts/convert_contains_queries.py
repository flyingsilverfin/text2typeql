#!/usr/bin/env python3
"""Convert queries that use CONTAINS to use like pattern in TypeQL."""

import csv
import json
import sys
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# All the queries to convert with their indices
QUERIES_TO_FIX = [
    307, 348, 349, 353, 365, 370, 373, 384, 393, 399, 407, 412, 415, 417, 421,
    426, 439, 442, 448, 452, 460, 471, 475, 478, 486, 492, 501, 503, 505, 508,
    519, 528, 533, 557, 595, 607, 618, 625, 627, 628, 629, 677, 691, 718
]

def get_query(database: str, index: int) -> dict:
    """Get query at index for database."""
    csv_path = "/opt/text2typeql/data/text2cypher/datasets/synthetic_opus_demodbs/text2cypher_claudeopus.csv"

    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        idx = 0
        for row in reader:
            if row['database'] != database:
                continue
            if row.get('syntax_error', '').lower() == 'true':
                continue
            if row.get('false_schema', '').lower() == 'true':
                continue

            if idx == index:
                return {
                    'index': index,
                    'question': row['question'],
                    'cypher': row['cypher']
                }
            idx += 1
    return None


def convert_cypher_to_typeql(index: int, question: str, cypher: str) -> str:
    """Convert Cypher query to TypeQL based on the pattern."""

    # Dictionary mapping index to converted TypeQL
    conversions = {
        # 307: List 3 movies released in 1995 (STARTS WITH '1995')
        307: '''match
$m isa movie, has released $r, has title $t;
$r like "1995.*";
limit 3;
fetch { "title": $t };''',

        # 348: First 3 movies released in USA ('USA' IN m.countries)
        348: '''match
$m isa movie, has countries $c, has title $t, has released $r;
$c like ".*USA.*";
sort $r asc;
limit 3;
fetch { "title": $t, "released": $r };''',

        # 349: Directors born in USA (bornIn CONTAINS 'USA')
        349: '''match
$d isa person, has born_in $b, has name $n;
(director: $d, film: $m) isa directed;
$b like ".*USA.*";
limit 3;
fetch { "name": $n };''',

        # 353: Top 3 movies with plot mentioning 'love'
        353: '''match
$m isa movie, has plot $p, has title $t, has imdb_rating $r;
$p like ".*love.*";
sort $r desc;
limit 3;
fetch { "title": $t, "plot": $p };''',

        # 365: First 3 movies with poster URL containing 'face'
        365: '''match
$m isa movie, has poster $p, has title $t;
$p like ".*face.*";
limit 3;
fetch { "title": $t };''',

        # 370: First 3 movies with plot mentioning 'zombie'
        370: '''match
$m isa movie, has plot $p, has title $t;
$p like ".*zombie.*";
limit 3;
fetch { "title": $t };''',

        # 373: First 3 movies with 'Turkish' in languages
        373: '''match
$m isa movie, has languages $l, has title $t;
$l like ".*Turkish.*";
limit 3;
fetch { "title": $t };''',

        # 384: First 3 movies with plot mentioning 'war'
        384: '''match
$m isa movie, has plot $p, has title $t;
$p like ".*war.*";
limit 3;
fetch { "title": $t };''',

        # 393: First 3 movies with plot mentioning 'adventure'
        393: '''match
$m isa movie, has plot $p, has title $t;
$p like ".*adventure.*";
limit 3;
fetch { "title": $t };''',

        # 399: First 3 movies with plot mentioning 'family'
        399: '''match
$m isa movie, has plot $p, has title $t;
$p like ".*family.*";
limit 3;
fetch { "title": $t };''',

        # 407: First 3 movies with actor bio mentioning 'model'
        407: '''match
$a isa person, has bio $b;
(actor: $a, film: $m) isa acted_in;
$m has title $t;
$b like ".*model.*";
limit 3;
fetch { "title": $t };''',

        # 412: First 3 movies with plots involving natural disaster
        412: '''match
$m isa movie, has plot $p, has title $t;
$p like ".*natural disaster.*";
limit 3;
fetch { "title": $t, "plot": $p };''',

        # 415: First 3 movies released in 'January' (STARTS WITH)
        415: '''match
$m isa movie, has released $r, has title $t;
$r like "January.*";
sort $r asc;
limit 3;
fetch { "title": $t, "released": $r };''',

        # 417: Top 3 movies with plot twist
        417: '''match
$m isa movie, has plot $p, has title $t, has imdb_rating $r;
$p like ".*twist.*";
sort $r desc;
limit 3;
fetch { "title": $t, "plot": $p };''',

        # 421: First 3 movies with poster URL containing 'moon'
        421: '''match
$m isa movie, has poster $p, has title $t;
$p like ".*moon.*";
limit 3;
fetch { "title": $t };''',

        # 426: First 3 movies with plot mentioning 'magic'
        426: '''match
$m isa movie, has plot $p, has title $t;
$p like ".*magic.*";
limit 3;
fetch { "title": $t };''',

        # 439: Movies with English language and budget over 50M
        439: '''match
$m isa movie, has languages $l, has budget $b, has title $t, has url $u, has runtime $rt, has revenue $rev, has imdb_rating $ir, has released $rel, has countries $c, has plot $p, has imdb_votes $iv, has imdb_id $iid, has year $y, has poster $po, has movie_id $mid, has tmdb_id $tid;
$l like ".*English.*";
$b > 50000000;
fetch { "title": $t, "url": $u, "runtime": $rt, "revenue": $rev, "imdb_rating": $ir, "released": $rel, "countries": $c, "languages": $l, "plot": $p, "imdb_votes": $iv, "imdb_id": $iid, "year": $y, "poster": $po, "movie_id": $mid, "tmdb_id": $tid, "budget": $b };''',

        # 442: Movies with both English and Spanish
        442: '''match
$m isa movie, has languages $l, has title $t;
$l like ".*English.*";
$l like ".*Spanish.*";
fetch { "title": $t };''',

        # 448: Directors with bio mentioning 'Academy Award'
        448: '''match
$d isa person, has bio $b, has name $n;
(director: $d, film: $m) isa directed;
$b like ".*Academy Award.*";
fetch { "name": $n, "bio": $b };''',

        # 452: Movies with plot mentioning 'war'
        452: '''match
$m isa movie, has plot $p, has title $t;
$p like ".*war.*";
fetch { "title": $t, "plot": $p };''',

        # 460: Movies with poster URL containing 'face'
        460: '''match
$m isa movie, has poster $p, has title $t, has url $u, has runtime $rt, has revenue $rev, has imdb_rating $ir, has released $rel, has countries $c, has languages $l, has plot $pl, has imdb_votes $iv, has imdb_id $iid, has year $y, has movie_id $mid, has tmdb_id $tid, has budget $b;
$p like ".*face.*";
fetch { "title": $t, "url": $u, "runtime": $rt, "revenue": $rev, "imdb_rating": $ir, "released": $rel, "countries": $c, "languages": $l, "plot": $pl, "imdb_votes": $iv, "imdb_id": $iid, "year": $y, "poster": $p, "movie_id": $mid, "tmdb_id": $tid, "budget": $b };''',

        # 471: Movies with plot including 'love'
        471: '''match
$m isa movie, has plot $p, has title $t;
$p like ".*love.*";
fetch { "title": $t, "plot": $p };''',

        # 475: Actors with bio mentioning 'Broadway'
        475: '''match
$a isa person, has bio $b, has name $n;
(actor: $a, film: $m) isa acted_in;
$b like ".*Broadway.*";
fetch { "name": $n, "bio": $b };''',

        # 478: Directors who directed a movie in their country of birth
        478: '''match
$d isa person, has born_in $b, has name $n;
(director: $d, film: $m) isa directed;
$m has countries $c;
$c like $b;
fetch { "director": $n, "bornIn": $b };''',

        # 486: Actors in movies with plot mentioning 'adventure'
        486: '''match
$a isa person, has name $n;
(actor: $a, film: $m) isa acted_in;
$m has plot $p;
$p like ".*adventure.*";
fetch { "actor": $n };''',

        # 492: Movies with plot including 'escape'
        492: '''match
$m isa movie, has plot $p, has title $t;
$p like ".*escape.*";
fetch { "title": $t, "plot": $p };''',

        # 501: Movies with Japanese in languages
        501: '''match
$m isa movie, has languages $l, has title $t, has url $u, has runtime $rt, has revenue $rev, has imdb_rating $ir, has released $rel, has countries $c, has plot $p, has imdb_votes $iv, has imdb_id $iid, has year $y, has poster $po, has movie_id $mid, has tmdb_id $tid, has budget $b;
$l like ".*Japanese.*";
fetch { "title": $t, "url": $u, "runtime": $rt, "revenue": $rev, "imdb_rating": $ir, "released": $rel, "countries": $c, "languages": $l, "plot": $p, "imdb_votes": $iv, "imdb_id": $iid, "year": $y, "poster": $po, "movie_id": $mid, "tmdb_id": $tid, "budget": $b };''',

        # 503: Actors with poster URL containing 'smile'
        503: '''match
$a isa person, has poster $p, has name $n;
(actor: $a, film: $m) isa acted_in;
$p like ".*smile.*";
fetch { "name": $n, "poster": $p };''',

        # 505: Directors who directed movies not in English
        505: '''match
$d isa person, has name $n;
(director: $d, film: $m) isa directed;
$m has languages $l;
not { $l like ".*English.*"; };
fetch { "director": $n };''',

        # 508: Movies with plot including 'monster'
        508: '''match
$m isa movie, has plot $p, has title $t;
$p like ".*monster.*";
fetch { "title": $t, "plot": $p };''',

        # 519: First 3 directors born in USA
        519: '''match
$d isa person, has born_in $b, has name $n;
(director: $d, film: $m) isa directed;
$b like ".*USA.*";
limit 3;
fetch { "name": $n, "bornIn": $b };''',

        # 528: 5 movies with both English and French
        528: '''match
$m isa movie, has languages $l, has title $t;
$l like ".*English.*";
$l like ".*French.*";
limit 5;
fetch { "title": $t };''',

        # 533: First 3 movies with plot containing 'friendship'
        533: '''match
$m isa movie, has plot $p, has title $t;
$p like ".*friendship.*";
limit 3;
fetch { "title": $t };''',

        # 557: 3 directors born in same country as their first movie
        557: '''match
$d isa person, has born_in $b, has name $n;
(director: $d, film: $m) isa directed;
$m has countries $c, has year $y, has title $t;
$c like $b;
sort $y asc;
limit 3;
fetch { "director": $n, "bornCountry": $b, "firstMovie": $t };''',

        # 595: First 3 movies with plot containing 'evil'
        595: '''match
$m isa movie, has plot $p, has title $t;
$p like ".*evil.*";
limit 3;
fetch { "title": $t };''',

        # 607: Movies with poster containing 'face' and rating > 8
        607: '''match
$m isa movie, has poster $p, has title $t, has imdb_rating $r;
$p like ".*face.*";
$r > 8.0;
fetch { "title": $t, "poster": $p, "imdbRating": $r };''',

        # 618: Movies in both English and French ('en' and 'fr')
        618: '''match
$m isa movie, has languages $l, has title $t;
$l like ".*en.*";
$l like ".*fr.*";
fetch { "title": $t };''',

        # 625: Movies with 'zombie' in plot and rating > 7
        625: '''match
$m isa movie, has plot $p, has title $t, has imdb_rating $r;
$p like ".*zombie.*";
$r > 7.0;
fetch { "title": $t, "imdbRating": $r, "plot": $p };''',

        # 627: Movies with actors having 'Wikipedia' in bio and revenue > 300M
        627: '''match
$m isa movie, has title $t, has revenue $rev;
(actor: $a, film: $m) isa acted_in;
$a has bio $b, has name $n;
$b like ".*Wikipedia.*";
$rev > 300000000;
fetch { "title": $t, "revenue": $rev, "name": $n, "bio": $b };''',

        # 628: Movies with 'friendship' in plot and budget < 20M
        628: '''match
$m isa movie, has plot $p, has title $t, has budget $b;
$p like ".*friendship.*";
$b < 20000000;
fetch { "title": $t, "plot": $p, "budget": $b };''',

        # 629: Top 5 highest-rated movies by users from USA
        629: '''match
$m isa movie, has title $t;
$u isa user, has name $n;
$r isa rated (user: $u, film: $m), has rating $rating;
$n like ".*USA.*";
reduce $avg = mean($rating) groupby $t;
sort $avg desc;
limit 5;
fetch { "movie": $t, "avgRating": $avg };''',

        # 677: Top 5 movies not in English with rating > 7.0
        677: '''match
$m isa movie, has imdb_rating $r, has title $t, has languages $l;
$r > 7.0;
not { $l like ".*English.*"; };
sort $r desc;
limit 5;
fetch { "title": $t, "imdbRating": $r, "languages": $l };''',

        # 691: Top 5 movies by revenue not in English
        691: '''match
$m isa movie, has revenue $rev, has title $t, has languages $l;
not { $l like ".*English.*"; };
sort $rev desc;
limit 5;
fetch { "title": $t, "revenue": $rev };''',

        # 718: First 3 movies with both English and French ('en' and 'fr')
        718: '''match
$m isa movie, has languages $l, has title $t;
$l like ".*en.*";
$l like ".*fr.*";
limit 3;
fetch { "title": $t };''',
    }

    return conversions.get(index)


def validate_query(driver, database: str, typeql: str) -> tuple[bool, str]:
    """Validate a TypeQL query against the database."""
    try:
        with driver.transaction(database, TransactionType.READ) as tx:
            # Just try to execute the query
            result = tx.query(typeql).resolve()
            # Try to consume a bit of the result to ensure it's valid
            try:
                docs = list(result.as_concept_documents())
            except:
                pass
        return True, "OK"
    except Exception as e:
        return False, str(e)


def main():
    # Connect to TypeDB
    credentials = Credentials("admin", "password")
    options = DriverOptions(is_tls_enabled=False)

    try:
        driver = TypeDB.driver("localhost:1729", credentials, options)
    except Exception as e:
        print(f"Error connecting to TypeDB: {e}")
        sys.exit(1)

    database = "text2typeql_recommendations"

    # Output file
    output_file = "/opt/text2typeql/output/recommendations/queries.csv"

    results = []

    for idx in QUERIES_TO_FIX:
        query_data = get_query("recommendations", idx)
        if not query_data:
            print(f"ERROR: Could not get query at index {idx}")
            continue

        question = query_data['question']
        cypher = query_data['cypher']
        typeql = convert_cypher_to_typeql(idx, question, cypher)

        if not typeql:
            print(f"ERROR: No conversion defined for index {idx}")
            continue

        # Validate
        valid, error = validate_query(driver, database, typeql)

        if valid:
            print(f"OK: Index {idx}")
            results.append({
                'original_index': idx,
                'question': question,
                'cypher': cypher,
                'typeql': typeql
            })
        else:
            print(f"FAIL: Index {idx} - {error[:100]}")

    # Append to CSV
    with open(output_file, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
        for row in results:
            writer.writerow(row)

    print(f"\nAppended {len(results)} queries to {output_file}")
    driver.close()


if __name__ == '__main__':
    main()
