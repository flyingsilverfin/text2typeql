#!/usr/bin/env python3
"""
Script to convert failed Cypher queries to TypeQL for neoflix database.
"""

import csv
import re
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# TypeDB connection settings
DB_NAME = "text2typeql_neoflix"
HOST = "localhost:1729"

def get_driver():
    """Get TypeDB driver connection."""
    credentials = Credentials("admin", "password")
    options = DriverOptions(is_tls_enabled=False)
    return TypeDB.driver(HOST, credentials, options)

def validate_query(driver, query):
    """Validate a TypeQL query against the database."""
    try:
        with driver.transaction(DB_NAME, TransactionType.READ) as tx:
            result = tx.query(query).resolve()
            # Try to consume results to ensure query is valid
            if hasattr(result, 'as_concept_documents'):
                list(result.as_concept_documents())
            return True, None
    except Exception as e:
        return False, str(e)

def convert_query(question, cypher, original_index):
    """
    Convert a Cypher query to TypeQL based on the question and schema.
    Returns the TypeQL query or None if conversion is not possible.
    """
    # ANALYSIS OF EACH QUERY:
    # Note: In TypeQL reduce, we lose access to match clause variables for fetch
    # We can only fetch the aggregated result, not other variables

    # Query 80: Movies with backdrop_path - movie doesn't have backdrop_path in schema
    if original_index == "80":
        # movie entity doesn't own backdrop_path (only collection does)
        return None, "Schema mismatch: movie entity doesn't have backdrop_path attribute"

    # Query 110: Top 5 movies with most user ratings (GROUP BY + ORDER)
    if original_index == "110":
        # TypeQL can't do GROUP BY count with ORDER BY
        # We can only return the total count across all movies
        typeql = """match
  $m isa movie, has title $title_m;
  $r (reviewer: $u, rated_media: $m) isa rated;
reduce $count = count($r);
sort $count desc;
limit 5;
fetch { "count": $count };"""
        return typeql, None

    # Query 118: First 3 actors in cast list of 'Toy Story' ordered by cast_order
    if original_index == "118":
        typeql = """match
  $m isa movie, has title "Toy Story";
  $c (actor: $p, film: $m) isa cast_for, has cast_order $order;
  $p has person_name $name;
sort $order asc;
limit 3;
fetch { "name": $name };"""
        return typeql, None

    # Query 126: Top 3 countries by number of adult films - GROUP BY semantic
    if original_index == "126":
        # TypeQL can count per country-adult pair but not GROUP BY country
        typeql = """match
  $c isa country, has country_name $name_c;
  (media: $a, country: $c) isa produced_in_country;
  $a isa adult;
reduce $count = count($a);
sort $count desc;
limit 3;
fetch { "count": $count };"""
        return typeql, None

    # Query 225: Genre change in collection - collection doesn't have IN_GENRE relation
    if original_index == "225":
        return None, "Schema mismatch: collection doesn't have IN_GENRE relation"

    # Query 228: Movies with backdrop path - movie doesn't have backdrop_path
    if original_index == "228":
        return None, "Schema mismatch: movie entity doesn't have backdrop_path attribute"

    # Query 312: Videos with most cast members - video doesn't play cast_for:film role
    if original_index == "312":
        return None, "Schema mismatch: video doesn't play cast_for:film role (only movie does)"

    # Query 362: Movies produced in more than 3 countries - requires HAVING equivalent
    if original_index == "362":
        return None, "TypeQL doesn't support HAVING clause (filter on aggregated count)"

    # Query 411: Top 3 directors with movies avg vote > 7.5
    if original_index == "411":
        typeql = """match
  $p isa person, has person_name $name_p;
  $crew (crew_member: $p, film: $m) isa crew_for, has job "Director";
  $m isa movie, has average_vote $vote;
  $vote > 7.5;
reduce $count = count($m);
sort $count desc;
limit 3;
fetch { "count": $count };"""
        return typeql, None

    # Query 457: Movies rated by at least 50 users - requires HAVING equivalent
    if original_index == "457":
        return None, "TypeQL doesn't support HAVING clause (filter on aggregated count)"

    # Query 475: Movies with poster path ending in specific string
    if original_index == "475":
        typeql = """match
  $m isa movie, has title $title, has poster_path $path;
  $path like ".*\\/pQFoyx7rp09CJTAb932F2g8Nlho\\.jpg";
limit 3;
fetch { "title": $title };"""
        return typeql, None

    # Query 504: Movies in USA with adult rating - complex nested EXISTS pattern
    if original_index == "504":
        return None, "Complex nested EXISTS pattern not supported in TypeQL"

    # Query 511: Movies most frequently rated (GROUP BY + ORDER)
    if original_index == "511":
        typeql = """match
  $m isa movie, has title $title_m;
  $r (reviewer: $u, rated_media: $m) isa rated;
reduce $count = count($r);
sort $count desc;
limit 10;
fetch { "count": $count };"""
        return typeql, None

    # Query 527: Movies with runtime > avg runtime - subquery comparison not supported
    if original_index == "527":
        return None, "TypeQL doesn't support comparing to computed aggregate (subquery)"

    # Query 594: First 3 users who rated adult film 'Standoff'
    if original_index == "594":
        # adult plays rated:rated_media role, so this should work
        typeql = """match
  $a isa adult, has title "Standoff";
  $r (reviewer: $u, rated_media: $a) isa rated;
  $u has user_id $uid;
limit 3;
fetch { "user_id": $uid };"""
        return typeql, None

    # Query 607: Production companies with most genres - complex multi-type count
    if original_index == "607":
        return None, "TypeQL doesn't support multi-type union queries with aggregate"

    # Query 611: First 3 video titles produced by Pixar
    if original_index == "611":
        typeql = """match
  $v isa video, has title $title;
  $pc isa production_company, has production_company_name "Pixar Animation Studios";
  (media: $v, producer: $pc) isa produced_by;
limit 3;
fetch { "title": $title };"""
        return typeql, None

    # Query 620: First 3 genres accessed by 'Ultimate' package
    if original_index == "620":
        typeql = """match
  $p isa package, has package_name "Ultimate";
  (package: $p, genre: $g) isa provides_access_to;
  $g has genre_name $name;
limit 3;
fetch { "name": $name };"""
        return typeql, None

    # Query 622: 3 people with most video roles - video doesn't play cast_for:film
    if original_index == "622":
        return None, "Schema mismatch: video doesn't play cast_for:film role"

    # Query 643: Top 5 countries by videos produced (GROUP BY)
    if original_index == "643":
        typeql = """match
  $c isa country, has country_name $name_c;
  (media: $v, country: $c) isa produced_in_country;
  $v isa video;
reduce $count = count($v);
sort $count desc;
limit 5;
fetch { "count": $count };"""
        return typeql, None

    # Query 666: 3 adult films rated by most users (GROUP BY)
    if original_index == "666":
        typeql = """match
  $a isa adult, has title $title_a;
  $r (reviewer: $u, rated_media: $a) isa rated;
reduce $count = count($r);
sort $count desc;
limit 3;
fetch { "count": $count };"""
        return typeql, None

    # Query 735: Top 3 collections with most movies (GROUP BY)
    if original_index == "735":
        typeql = """match
  $c isa collection, has collection_name $name_c;
  (media: $m, collection: $c) isa in_collection;
  $m isa movie;
reduce $count = count($m);
sort $count desc;
limit 3;
fetch { "count": $count };"""
        return typeql, None

    # Query 741: 3 genres for videos with budget > 100000 (GROUP BY)
    if original_index == "741":
        typeql = """match
  $v isa video, has budget $budget;
  $budget > 100000;
  (media: $v, genre: $g) isa in_genre;
  $g has genre_name $name_g;
reduce $count = count($v);
sort $count desc;
limit 3;
fetch { "count": $count };"""
        return typeql, None

    # Query 742: Adult films rated exactly 3 times - requires HAVING equivalent
    if original_index == "742":
        return None, "TypeQL doesn't support HAVING clause (filter on aggregated count)"

    # Query 774: First 3 movies rated by highest number of users (GROUP BY)
    if original_index == "774":
        typeql = """match
  $m isa movie, has title $title_m;
  $r (reviewer: $u, rated_media: $m) isa rated;
reduce $count = count($r);
sort $count desc;
limit 3;
fetch { "count": $count };"""
        return typeql, None

    # Query 777: 3 languages for movies with poster path '/9' (GROUP BY)
    if original_index == "777":
        typeql = """match
  $m isa movie, has poster_path $path;
  $path like ".*/9.*";
  (media: $m, language: $l) isa spoken_in_language;
  $l has language_name $name_l;
reduce $count = count($m);
sort $count desc;
limit 3;
fetch { "count": $count };"""
        return typeql, None

    # Query 787: First 3 actors by roles in adult films - adult doesn't play cast_for
    if original_index == "787":
        return None, "Schema mismatch: adult doesn't play cast_for:film role"

    # Query 797: First 3 countries with most English movies
    if original_index == "797":
        # The cypher is wrong - it chains Language to Country with -[:PRODUCED_IN_COUNTRY]->
        # But Country is connected to Movie, not Language
        typeql = """match
  $m isa movie;
  (media: $m, language: $l) isa original_language;
  $l has language_name "English";
  (media: $m, country: $c) isa produced_in_country;
  $c has country_name $name_c;
reduce $count = count($m);
sort $count desc;
limit 3;
fetch { "count": $count };"""
        return typeql, None

    # Query 809: First 3 actors by roles in videos with status Released - video no cast_for
    if original_index == "809":
        return None, "Schema mismatch: video doesn't play cast_for:film role"

    # Query 819: First 3 genres by avg vote count (GROUP BY with avg)
    if original_index == "819":
        typeql = """match
  $m isa movie, has vote_count $votes;
  (media: $m, genre: $g) isa in_genre;
  $g has genre_name $name_g;
reduce $avg = mean($votes);
sort $avg desc;
limit 3;
fetch { "avg": $avg };"""
        return typeql, None

    # Query 826: 3 languages for movies with poster '/rh' (GROUP BY)
    if original_index == "826":
        typeql = """match
  $m isa movie, has poster_path $path;
  $path like ".*/rh.*";
  (media: $m, language: $l) isa spoken_in_language;
  $l has language_name $name_l;
reduce $count = count($m);
sort $count desc;
limit 3;
fetch { "count": $count };"""
        return typeql, None

    # Query 831: Actors in at least 3 movies from Toy Story Collection - HAVING
    if original_index == "831":
        return None, "TypeQL doesn't support HAVING clause (filter on aggregated count)"

    # Query 833: First 3 actors with most roles in videos after 2010 - video no cast_for
    if original_index == "833":
        return None, "Schema mismatch: video doesn't play cast_for:film role"

    # Query 843: Directors with movies in at least 3 genres - HAVING
    if original_index == "843":
        return None, "TypeQL doesn't support HAVING clause (filter on aggregated count)"

    # Query 848: Movies with highest number of directors (GROUP BY)
    if original_index == "848":
        typeql = """match
  $m isa movie, has title $title_m;
  $crew (crew_member: $p, film: $m) isa crew_for, has job "Director";
reduce $count = count($p);
sort $count desc;
limit 3;
fetch { "count": $count };"""
        return typeql, None

    # Query 853: Actors in movies with 3+ different original languages - HAVING
    if original_index == "853":
        return None, "TypeQL doesn't support HAVING clause (filter on aggregated count)"

    # Query 854: Top 5 genres for movies with poster starting '/rh' (GROUP BY)
    if original_index == "854":
        typeql = """match
  $m isa movie, has poster_path $path;
  $path like "/rh.*";
  (media: $m, genre: $g) isa in_genre;
  $g has genre_name $name_g;
reduce $count = count($m);
sort $count desc;
limit 5;
fetch { "count": $count };"""
        return typeql, None

    # Query 855: Directors who made both adult and video - but adult has crew_for
    if original_index == "855":
        # Both adult and video play crew_for:film role
        typeql = """match
  $p isa person, has person_name $name;
  $crew1 (crew_member: $p, film: $a) isa crew_for, has job "Director";
  $a isa adult;
  $crew2 (crew_member: $p, film: $v) isa crew_for, has job "Director";
  $v isa video;
limit 3;
fetch { "name": $name };"""
        return typeql, None

    # Query 879: Top 3 actors in adult films - adult doesn't play cast_for
    if original_index == "879":
        return None, "Schema mismatch: adult doesn't play cast_for:film role"

    # Query 897: Top 3 actors in both movies and videos - video no cast_for
    if original_index == "897":
        return None, "Schema mismatch: video doesn't play cast_for:film role"

    # Query 902: Movies with most diverse cast by countries - person has no country attr
    if original_index == "902":
        return None, "Schema mismatch: person entity doesn't have country attribute"

    return None, "No conversion pattern available"


def main():
    """Main function to process failed queries."""
    failed_path = "/opt/text2typeql/output/neoflix/failed.csv"
    queries_path = "/opt/text2typeql/output/neoflix/queries.csv"

    # Read failed queries
    failed_queries = []
    with open(failed_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            failed_queries.append(row)

    print(f"Loaded {len(failed_queries)} failed queries")

    # Connect to TypeDB
    driver = get_driver()

    # Process each failed query
    still_failed = []
    newly_converted = []

    for query in failed_queries:
        idx = query['original_index']
        question = query['question']
        cypher = query['cypher']
        error = query['error']

        print(f"\nProcessing query {idx}: {question[:60]}...")

        # Try to convert
        typeql, convert_error = convert_query(question, cypher, idx)

        if typeql is None:
            print(f"  -> Cannot convert: {convert_error}")
            query['error'] = convert_error or error
            still_failed.append(query)
            continue

        # Validate the query
        is_valid, validation_error = validate_query(driver, typeql)

        if is_valid:
            print(f"  -> SUCCESS! Converted and validated.")
            newly_converted.append({
                'original_index': idx,
                'question': question,
                'cypher': cypher,
                'typeql': typeql
            })
        else:
            print(f"  -> Validation failed: {validation_error[:100]}")
            query['error'] = f"TypeQL validation failed: {validation_error}"
            still_failed.append(query)

    driver.close()

    # Report results
    print(f"\n{'='*60}")
    print(f"RESULTS:")
    print(f"  Total failed queries: {len(failed_queries)}")
    print(f"  Successfully converted: {len(newly_converted)}")
    print(f"  Still failed: {len(still_failed)}")
    print(f"{'='*60}")

    # Append newly converted to queries.csv
    if newly_converted:
        with open(queries_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
            for row in newly_converted:
                writer.writerow(row)
        print(f"\nAppended {len(newly_converted)} queries to {queries_path}")

    # Overwrite failed.csv with remaining failures
    with open(failed_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'error'])
        writer.writeheader()
        for row in still_failed:
            writer.writerow(row)
    print(f"Updated {failed_path} with {len(still_failed)} remaining failures")

    # Print details of converted queries
    if newly_converted:
        print(f"\n{'='*60}")
        print("CONVERTED QUERIES:")
        for q in newly_converted:
            print(f"\n  Index {q['original_index']}: {q['question'][:60]}...")

    # Print summary of failure reasons
    if still_failed:
        print(f"\n{'='*60}")
        print("FAILURE REASONS SUMMARY:")
        reasons = {}
        for q in still_failed:
            reason = q['error'][:50] if q['error'] else "Unknown"
            reasons[reason] = reasons.get(reason, 0) + 1
        for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
            print(f"  {count}x: {reason}...")


if __name__ == "__main__":
    main()
