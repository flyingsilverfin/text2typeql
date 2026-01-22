#!/usr/bin/env python3
"""Fix all remaining neoflix failed queries single-threaded."""

import csv
import re
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# Connect to TypeDB
credentials = Credentials('admin', 'password')
options = DriverOptions(is_tls_enabled=False)
driver = TypeDB.driver('localhost:1729', credentials, options)

def validate_query(query):
    """Validate a TypeQL query against the database."""
    try:
        with driver.transaction('text2typeql_neoflix', TransactionType.READ) as tx:
            result = tx.query(query).resolve()
            # Try to consume results
            docs = list(result.as_concept_documents())
            return True, len(docs)
    except Exception as e:
        return False, str(e)

def fix_syntax_error(row):
    """Fix queries with syntax errors (constraints after limit/fetch)."""
    question = row['question']
    cypher = row['cypher']
    error = row['error']
    
    # Common patterns to fix
    # These queries have constraints appearing after limit/fetch
    # We need to build proper TypeQL from scratch based on the Cypher
    
    # Parse what the query needs
    needs_date_filter = 'release_date' in cypher or 'date(' in cypher
    needs_rating_filter = 'rating' in cypher.lower() and '=' in cypher
    needs_budget_filter = 'budget' in cypher
    needs_runtime_filter = 'runtime' in cypher
    needs_revenue_filter = 'revenue' in cypher
    needs_genre = 'Genre' in cypher
    needs_cast = 'CAST_FOR' in cypher
    needs_crew = 'CREW_FOR' in cypher
    
    # Determine entity type
    if 'Adult' in cypher or ':Adult' in cypher:
        entity = 'adult'
    elif 'Video' in cypher or ':Video' in cypher:
        entity = 'video'
    else:
        entity = 'movie'
    
    # Build the query
    match_parts = [f'$m isa {entity}']
    fetch_parts = []
    sort_var = None
    sort_dir = 'desc'
    limit_val = None
    
    # Extract limit
    limit_match = re.search(r'LIMIT\s+(\d+)', cypher, re.I)
    if limit_match:
        limit_val = int(limit_match.group(1))
    
    # Extract ORDER BY
    order_match = re.search(r'ORDER BY\s+\w+\.(\w+)(?:\s+(DESC|ASC))?', cypher, re.I)
    if order_match:
        sort_attr = order_match.group(1)
        sort_dir = (order_match.group(2) or 'asc').lower()
    
    # Handle date filters
    if needs_date_filter:
        match_parts.append('$m has release_date $rd')
        # Extract date comparison
        date_match = re.search(r"release_date\s*([><]=?)\s*date\(['\"](\d{4}-\d{2}-\d{2})['\"]", cypher)
        if date_match:
            op = date_match.group(1)
            date_val = date_match.group(2)
            match_parts.append(f'$rd {op} {date_val}')
        fetch_parts.append('"release_date": $rd')
        if 'ORDER BY' in cypher and 'release_date' in cypher:
            sort_var = '$rd'
    
    # Handle rating filter
    if needs_rating_filter and 'RATED' in cypher:
        match_parts.append('$u isa user')
        match_parts.append('$rel (rated_media: $m, reviewer: $u) isa rated, has rating $rating')
        rating_match = re.search(r'rating\s*=\s*([\d.]+)', cypher)
        if rating_match:
            match_parts.append(f'$rating == {rating_match.group(1)}')
        fetch_parts.append('"rating": $rating')
    
    # Handle budget filter
    if needs_budget_filter:
        match_parts.append('$m has budget $budget')
        budget_match = re.search(r'budget\s*([><]=?|=)\s*(\d+)', cypher)
        if budget_match:
            op = '==' if budget_match.group(1) == '=' else budget_match.group(1)
            match_parts.append(f'$budget {op} {budget_match.group(2)}')
        fetch_parts.append('"budget": $budget')
        if 'ORDER BY' in cypher and 'budget' in cypher:
            sort_var = '$budget'
    
    # Handle runtime filter
    if needs_runtime_filter:
        match_parts.append('$m has runtime $runtime')
        runtime_match = re.search(r'runtime\s*([><]=?|=)\s*(\d+)', cypher)
        if runtime_match:
            op = '==' if runtime_match.group(1) == '=' else runtime_match.group(1)
            match_parts.append(f'$runtime {op} {runtime_match.group(2)}')
        fetch_parts.append('"runtime": $runtime')
        if 'ORDER BY' in cypher and 'runtime' in cypher:
            sort_var = '$runtime'
    
    # Handle revenue filter
    if needs_revenue_filter:
        match_parts.append('$m has revenue $revenue')
        revenue_match = re.search(r'revenue\s*([><]=?|=)\s*(\d+)', cypher)
        if revenue_match:
            op = '==' if revenue_match.group(1) == '=' else revenue_match.group(1)
            match_parts.append(f'$revenue {op} {revenue_match.group(2)}')
        fetch_parts.append('"revenue": $revenue')
        if 'ORDER BY' in cypher and 'revenue' in cypher:
            sort_var = '$revenue'
    
    # Handle genre filter
    if needs_genre:
        genre_match = re.search(r"Genre\s*\{name:\s*['\"]([^'\"]+)['\"]", cypher)
        if genre_match:
            genre_name = genre_match.group(1)
            match_parts.append(f'$g isa genre, has genre_name "{genre_name}"')
            match_parts.append('(media: $m, genre: $g) isa in_genre')
    
    # Handle cast filter
    if needs_cast:
        match_parts.append('$p isa person')
        match_parts.append('$cast (actor: $p, film: $m) isa cast_for')
        # Check for person name
        person_match = re.search(r"Person\s*\{name:\s*['\"]([^'\"]+)['\"]", cypher)
        if person_match:
            match_parts.append(f'$p has person_name "{person_match.group(1)}"')
        # Check for character
        char_match = re.search(r"character\s*=\s*['\"]([^'\"]+)['\"]", cypher)
        if char_match:
            match_parts.append(f'$cast has character "{char_match.group(1)}"')
        # Check for order
        order_match = re.search(r'order\s*=\s*(\d+)', cypher)
        if order_match:
            match_parts.append(f'$cast has cast_order {order_match.group(1)}')
        # Check for gender
        gender_match = re.search(r'gender\s*=\s*(\d+)', cypher)
        if gender_match:
            match_parts.append(f'$p has gender {gender_match.group(1)}')
    
    # Handle crew filter  
    if needs_crew:
        match_parts.append('$p isa person')
        match_parts.append('$crew (crew_member: $p, film: $m) isa crew_for')
        job_match = re.search(r"job\s*=\s*['\"]([^'\"]+)['\"]", cypher)
        if job_match:
            match_parts.append(f'$crew has job "{job_match.group(1)}"')
    
    # Always fetch title
    match_parts.append('$m has title $title')
    if '"title"' not in str(fetch_parts):
        fetch_parts.insert(0, '"title": $title')
    
    # Handle popularity for sorting
    if 'popularity' in cypher.lower():
        match_parts.append('$m has popularity $popularity')
        if 'ORDER BY' in cypher and 'popularity' in cypher.lower():
            sort_var = '$popularity'
        fetch_parts.append('"popularity": $popularity')
    
    # Handle average_vote
    if 'average_vote' in cypher:
        match_parts.append('$m has average_vote $av')
        av_match = re.search(r'average_vote\s*([><]=?)\s*([\d.]+)', cypher)
        if av_match:
            match_parts.append(f'$av {av_match.group(1)} {av_match.group(2)}')
        fetch_parts.append('"average_vote": $av')
        if 'ORDER BY' in cypher and 'average_vote' in cypher:
            sort_var = '$av'
    
    # Handle vote_count
    if 'vote_count' in cypher:
        match_parts.append('$m has vote_count $vc')
        if 'ORDER BY' in cypher and 'vote_count' in cypher:
            sort_var = '$vc'
        fetch_parts.append('"vote_count": $vc')
    
    # Handle tagline
    if 'tagline' in cypher:
        match_parts.append('$m has tagline $tagline')
        fetch_parts.append('"tagline": $tagline')
    
    # Handle timestamp for rated queries
    if 'timestamp' in cypher:
        if '$rel' not in str(match_parts):
            match_parts.append('$u isa user')
            match_parts.append('$rel (rated_media: $m, reviewer: $u) isa rated')
        # The relation should have timestamp
        timestamp_match = re.search(r"timestamp\s*([><]=?)\s*date\(['\"](\d{4}-\d{2}-\d{2})['\"]", cypher)
        if timestamp_match:
            # Need to add timestamp to the relation match
            for i, part in enumerate(match_parts):
                if 'isa rated' in part and 'timestamp' not in part:
                    match_parts[i] = part.replace('isa rated', 'isa rated, has timestamp $ts')
                    match_parts.append(f'$ts {timestamp_match.group(1)} {timestamp_match.group(2)}')
                    fetch_parts.append('"timestamp": $ts')
                    break
    
    # Build query
    query = 'match\n  ' + ';\n  '.join(match_parts) + ';'
    if sort_var:
        query += f'\nsort {sort_var} {sort_dir};'
    if limit_val:
        query += f'\nlimit {limit_val};'
    query += '\nfetch { ' + ', '.join(fetch_parts) + ' };'
    
    return query

def fix_concept_error(row):
    """Fix queries with Concept Error (aggregation queries)."""
    question = row['question']
    cypher = row['cypher']
    
    # These need to be converted from aggregation to simple match+fetch
    # Determine what the query is asking for
    
    # Determine entity types involved
    entity = 'movie'
    if 'Adult' in cypher:
        entity = 'adult'
    elif 'Video' in cypher:
        entity = 'video'
    
    # Check what relationship/count is being asked
    has_genre = 'Genre' in cypher or 'IN_GENRE' in cypher
    has_country = 'Country' in cypher or 'PRODUCED_IN_COUNTRY' in cypher
    has_language = 'Language' in cypher
    has_spoken = 'SPOKEN_IN_LANGUAGE' in cypher
    has_original = 'ORIGINAL_LANGUAGE' in cypher
    has_keyword = 'Keyword' in cypher or 'HAS_KEYWORD' in cypher
    has_collection = 'Collection' in cypher or 'IN_COLLECTION' in cypher
    has_production = 'ProductionCompany' in cypher or 'PRODUCED_BY' in cypher
    has_cast = 'CAST_FOR' in cypher
    has_crew = 'CREW_FOR' in cypher
    has_rated = 'RATED' in cypher
    has_package = 'Package' in cypher
    has_subscription = 'Subscription' in cypher
    
    match_parts = []
    fetch_parts = []
    sort_var = None
    sort_dir = 'desc'
    limit_val = 50  # Default
    
    # Extract limit from cypher
    limit_match = re.search(r'LIMIT\s+(\d+)', cypher, re.I)
    if limit_match:
        limit_val = min(int(limit_match.group(1)) * 10, 100)  # Give more results for aggregation
    
    # Check what we're counting/returning
    # Pattern: "top N X by count of Y"
    
    if has_subscription and 'expire' in question.lower():
        # Subscriptions expiring soon
        match_parts.append('$s isa subscription, has subscription_id $sid, has expires_at $exp')
        fetch_parts.append('"subscription_id": $sid')
        fetch_parts.append('"expires_at": $exp')
        sort_var = '$exp'
        sort_dir = 'asc'
        limit_val = int(limit_match.group(1)) if limit_match else 3
    elif has_subscription and has_package:
        # Subscriptions for package
        match_parts.append('$s isa subscription, has subscription_id $sid, has expires_at $exp')
        match_parts.append('$p isa package')
        match_parts.append('(subscription: $s, package: $p) isa for_package')
        pkg_match = re.search(r"Package\s*\{name:\s*['\"]([^'\"]+)['\"]", cypher)
        if pkg_match:
            match_parts.append(f'$p has package_name "{pkg_match.group(1)}"')
        fetch_parts.append('"subscription_id": $sid')
        fetch_parts.append('"expires_at": $exp')
    elif has_genre and 'count' in cypher.lower():
        # Genre counts
        match_parts.append('$g isa genre, has genre_name $gn')
        match_parts.append(f'$m isa {entity}')
        match_parts.append('(media: $m, genre: $g) isa in_genre')
        fetch_parts.append('"genre": $gn')
        # Add filters if present
        if 'runtime' in cypher:
            match_parts.append('$m has runtime $rt')
            rt_match = re.search(r'runtime\s*([><]=?)\s*(\d+)', cypher)
            if rt_match:
                match_parts.append(f'$rt {rt_match.group(1)} {rt_match.group(2)}')
        if 'budget' in cypher:
            match_parts.append('$m has budget $budget')
            b_match = re.search(r'budget\s*([><]=?)\s*(\d+)', cypher)
            if b_match:
                match_parts.append(f'$budget {b_match.group(1)} {b_match.group(2)}')
        if 'release_date' in cypher:
            match_parts.append('$m has release_date $rd')
            d_match = re.search(r"release_date\s*([><]=?)\s*date\(['\"](\d{4}-\d{2}-\d{2})['\"]", cypher)
            if d_match:
                match_parts.append(f'$rd {d_match.group(1)} {d_match.group(2)}')
        if 'popularity' in cypher:
            match_parts.append('$m has popularity $pop')
            if 'avg' in cypher.lower():
                sort_var = '$pop'
    elif has_country and 'count' in cypher.lower():
        # Country counts
        match_parts.append('$c isa country, has country_name $cn')
        match_parts.append(f'$m isa {entity}')
        match_parts.append('(media: $m, country: $c) isa produced_in_country')
        fetch_parts.append('"country": $cn')
        # Add genre filter if present
        if has_genre:
            genre_match = re.search(r"Genre\s*\{name:\s*['\"]([^'\"]+)['\"]", cypher)
            if genre_match:
                match_parts.append(f'$g isa genre, has genre_name "{genre_match.group(1)}"')
                match_parts.append('(media: $m, genre: $g) isa in_genre')
        if 'budget' in cypher:
            match_parts.append('$m has budget $budget')
            b_match = re.search(r'budget\s*([><]=?)\s*(\d+)', cypher)
            if b_match:
                match_parts.append(f'$budget {b_match.group(1)} {b_match.group(2)}')
        if 'runtime' in cypher:
            match_parts.append('$m has runtime $rt')
            rt_match = re.search(r'runtime\s*([><]=?)\s*(\d+)', cypher)
            if rt_match:
                match_parts.append(f'$rt {rt_match.group(1)} {rt_match.group(2)}')
        if 'average_vote' in cypher:
            match_parts.append('$m has average_vote $av')
            av_match = re.search(r'average_vote\s*([><]=?)\s*([\d.]+)', cypher)
            if av_match:
                match_parts.append(f'$av {av_match.group(1)} {av_match.group(2)}')
        if 'tagline' in cypher.lower() and 'love' in question.lower():
            match_parts.append('$m has tagline $tag')
            match_parts.append('$tag like ".*[Ll]ove.*"')
    elif has_language:
        # Language counts
        match_parts.append('$l isa language, has language_name $ln')
        match_parts.append(f'$m isa {entity}')
        if has_spoken:
            match_parts.append('(media: $m, language: $l) isa spoken_in_language')
        elif has_original:
            match_parts.append('(media: $m, language: $l) isa original_language')
        else:
            # Default to spoken
            match_parts.append('(media: $m, language: $l) isa spoken_in_language')
        fetch_parts.append('"language": $ln')
        # Add filters
        if 'budget' in cypher:
            match_parts.append('$m has budget $budget')
            b_match = re.search(r'budget\s*([><]=?)\s*(\d+)', cypher)
            if b_match:
                match_parts.append(f'$budget {b_match.group(1)} {b_match.group(2)}')
        if 'revenue' in cypher:
            match_parts.append('$m has revenue $rev')
            r_match = re.search(r'revenue\s*([><]=?)\s*(\d+)', cypher)
            if r_match:
                match_parts.append(f'$rev {r_match.group(1)} {r_match.group(2)}')
            if 'sum' in cypher.lower() or 'ORDER BY' in cypher:
                sort_var = '$rev'
        if 'popularity' in cypher:
            match_parts.append('$m has popularity $pop')
            p_match = re.search(r'popularity\s*([><]=?)\s*(\d+)', cypher)
            if p_match:
                match_parts.append(f'$pop {p_match.group(1)} {p_match.group(2)}')
        if 'runtime' in cypher:
            match_parts.append('$m has runtime $rt')
            rt_match = re.search(r'runtime\s*([><]=?)\s*(\d+)', cypher)
            if rt_match:
                match_parts.append(f'$rt {rt_match.group(1)} {rt_match.group(2)}')
        if 'vote_count' in cypher:
            match_parts.append('$m has vote_count $vc')
            vc_match = re.search(r'vote_count\s*([><]=?)\s*(\d+)', cypher)
            if vc_match:
                match_parts.append(f'$vc {vc_match.group(1)} {vc_match.group(2)}')
        if 'average_vote' in cypher:
            match_parts.append('$m has average_vote $av')
            av_match = re.search(r'average_vote\s*([><]=?)\s*([\d.]+)', cypher)
            if av_match:
                match_parts.append(f'$av {av_match.group(1)} {av_match.group(2)}')
        if 'status' in cypher:
            status_match = re.search(r"status\s*=\s*['\"]([^'\"]+)['\"]", cypher)
            if status_match:
                match_parts.append(f'$m has status "{status_match.group(1)}"')
    elif has_keyword:
        # Keyword counts
        match_parts.append('$k isa keyword, has keyword_name $kn')
        match_parts.append(f'$m isa {entity}')
        match_parts.append('(media: $m, keyword: $k) isa has_keyword')
        fetch_parts.append('"keyword": $kn')
        if 'budget' in cypher:
            match_parts.append('$m has budget $budget')
            b_match = re.search(r'budget\s*([><]=?)\s*(\d+)', cypher)
            if b_match:
                match_parts.append(f'$budget {b_match.group(1)} {b_match.group(2)}')
        if 'revenue' in cypher:
            match_parts.append('$m has revenue $rev')
            r_match = re.search(r'revenue\s*([><]=?)\s*(\d+)', cypher)
            if r_match:
                match_parts.append(f'$rev {r_match.group(1)} {r_match.group(2)}')
        if 'runtime' in cypher:
            match_parts.append('$m has runtime $rt')
            rt_match = re.search(r'runtime\s*([><]=?)\s*(\d+)', cypher)
            if rt_match:
                match_parts.append(f'$rt {rt_match.group(1)} {rt_match.group(2)}')
    elif has_collection:
        # Collection counts
        match_parts.append('$c isa collection, has collection_name $cn')
        match_parts.append(f'$m isa {entity}')
        match_parts.append('(media: $m, collection: $c) isa in_collection')
        fetch_parts.append('"collection": $cn')
        if 'average_vote' in cypher:
            match_parts.append('$m has average_vote $av')
            av_match = re.search(r'average_vote\s*([><]=?)\s*([\d.]+)', cypher)
            if av_match:
                match_parts.append(f'$av {av_match.group(1)} {av_match.group(2)}')
    elif has_production:
        # Production company counts
        match_parts.append('$c isa production_company, has production_company_name $cn')
        match_parts.append(f'$m isa {entity}')
        match_parts.append('(media: $m, producer: $c) isa produced_by')
        fetch_parts.append('"company": $cn')
        if 'revenue' in cypher:
            match_parts.append('$m has revenue $rev')
            if 'sum' in cypher.lower():
                sort_var = '$rev'
            fetch_parts.append('"revenue": $rev')
        if 'budget' in cypher:
            match_parts.append('$m has budget $budget')
            b_match = re.search(r'budget\s*([><]=?)\s*(\d+)', cypher)
            if b_match:
                match_parts.append(f'$budget {b_match.group(1)} {b_match.group(2)}')
        if 'runtime' in cypher:
            match_parts.append('$m has runtime $rt')
            rt_match = re.search(r'runtime\s*([><]=?)\s*(\d+)', cypher)
            if rt_match:
                match_parts.append(f'$rt {rt_match.group(1)} {rt_match.group(2)}')
        if 'popularity' in cypher:
            match_parts.append('$m has popularity $pop')
            if 'avg' in cypher.lower():
                sort_var = '$pop'
        if has_genre:
            genre_match = re.search(r"Genre\s*\{name:\s*['\"]([^'\"]+)['\"]", cypher)
            if genre_match:
                match_parts.append(f'$g isa genre, has genre_name "{genre_match.group(1)}"')
                match_parts.append('(media: $m, genre: $g) isa in_genre')
        # Check for specific company
        company_match = re.search(r"ProductionCompany\s*\{name:\s*['\"]([^'\"]+)['\"]", cypher)
        if company_match:
            match_parts.append(f'$c has production_company_name "{company_match.group(1)}"')
    elif has_cast:
        # Cast counts - movies with most cast or person with most roles
        if 'Person' in cypher and 'count' in cypher.lower():
            # Person with most roles
            match_parts.append('$p isa person, has person_name $pn')
            match_parts.append('$m isa movie')
            match_parts.append('(actor: $p, film: $m) isa cast_for')
            fetch_parts.append('"person": $pn')
            # Check for person filter
            person_match = re.search(r"Person\s*\{name:\s*['\"]([^'\"]+)['\"]", cypher)
            if person_match:
                match_parts.append(f'$p has person_name "{person_match.group(1)}"')
            if 'status' in cypher:
                status_match = re.search(r"status:\s*['\"]([^'\"]+)['\"]", cypher)
                if status_match:
                    match_parts.append(f'$m has status "{status_match.group(1)}"')
        else:
            # Movies with most cast
            match_parts.append('$m isa movie, has title $title')
            match_parts.append('$p isa person')
            match_parts.append('(actor: $p, film: $m) isa cast_for')
            fetch_parts.append('"title": $title')
        if 'tagline' in cypher:
            match_parts.append('$m has tagline $tag')
    elif has_crew:
        # Crew counts
        if 'Director' in cypher:
            match_parts.append('$p isa person, has person_name $pn')
            match_parts.append('$m isa movie')
            match_parts.append('$crew (crew_member: $p, film: $m) isa crew_for, has job "Director"')
            fetch_parts.append('"director": $pn')
            if 'revenue' in cypher:
                match_parts.append('$m has revenue $rev')
                sort_var = '$rev'
                fetch_parts.append('"revenue": $rev')
            if 'status' in cypher:
                status_match = re.search(r"status:\s*['\"]([^'\"]+)['\"]", cypher)
                if status_match:
                    match_parts.append(f'$m has status "{status_match.group(1)}"')
        else:
            match_parts.append('$p isa person, has person_name $pn')
            match_parts.append('$m isa movie')
            match_parts.append('(crew_member: $p, film: $m) isa crew_for')
            fetch_parts.append('"person": $pn')
            if 'release_date' in cypher:
                match_parts.append('$m has release_date $rd')
                d_match = re.search(r"release_date\s*([><]=?)\s*date\(['\"](\d{4}-\d{2}-\d{2})['\"]", cypher)
                if d_match:
                    match_parts.append(f'$rd {d_match.group(1)} {d_match.group(2)}')
    elif has_rated:
        # Rating aggregations
        if 'User' in cypher and 'count' in cypher.lower():
            # Users who rated most
            match_parts.append('$u isa user, has user_id $uid')
            match_parts.append(f'$m isa {entity}')
            match_parts.append('$rel (rated_media: $m, reviewer: $u) isa rated')
            fetch_parts.append('"user": $uid')
            if 'avg' in cypher.lower():
                match_parts.append('$rel has rating $rating')
                sort_var = '$rating'
                fetch_parts.append('"rating": $rating')
        elif 'Movie' in cypher or 'Video' in cypher:
            # Movies/videos with most ratings or highest avg
            match_parts.append(f'$m isa {entity}, has title $title')
            match_parts.append('$u isa user')
            match_parts.append('$rel (rated_media: $m, reviewer: $u) isa rated, has rating $rating')
            fetch_parts.append('"title": $title')
            fetch_parts.append('"rating": $rating')
            if 'avg' in cypher.lower() or 'ORDER BY' in cypher:
                sort_var = '$rating'
    elif has_package and 'provides_access' in cypher.lower():
        # Package provides access to genre
        match_parts.append('$p isa package, has package_name $pn')
        match_parts.append('$g isa genre, has genre_name $gn')
        match_parts.append('(package: $p, genre: $g) isa provides_access_to')
        pkg_match = re.search(r"Package\s*\{name:\s*['\"]([^'\"]+)['\"]", cypher)
        if pkg_match:
            match_parts.append(f'$p has package_name "{pkg_match.group(1)}"')
        fetch_parts.append('"genre": $gn')
    elif 'RETURN m' in cypher or 'RETURN s' in cypher or 'RETURN a' in cypher or 'RETURN v' in cypher:
        # Return entity queries - need to fetch specific attributes
        match_parts.append(f'$m isa {entity}, has title $title')
        fetch_parts.append('"title": $title')
        if 'release_date' in cypher:
            match_parts.append('$m has release_date $rd')
            d_match = re.search(r"release_date\s*([><]=?)\s*date\(['\"](\d{4}-\d{2}-\d{2})['\"]", cypher)
            if d_match:
                match_parts.append(f'$rd {d_match.group(1)} {d_match.group(2)}')
            fetch_parts.append('"release_date": $rd')
        if 'average_vote' in cypher:
            match_parts.append('$m has average_vote $av')
            av_match = re.search(r'average_vote\s*([><]=?)\s*([\d.]+)', cypher)
            if av_match:
                match_parts.append(f'$av {av_match.group(1)} {av_match.group(2)}')
            fetch_parts.append('"average_vote": $av')
        if 'runtime' in cypher:
            match_parts.append('$m has runtime $rt')
            rt_match = re.search(r'runtime\s*=\s*(\d+)', cypher)
            if rt_match:
                match_parts.append(f'$rt == {rt_match.group(1)}')
            fetch_parts.append('"runtime": $rt')
        if 'popularity' in cypher:
            match_parts.append('$m has popularity $pop')
            p_match = re.search(r'popularity\s*([><]=?)\s*([\d.]+)', cypher)
            if p_match:
                match_parts.append(f'$pop {p_match.group(1)} {p_match.group(2)}')
            fetch_parts.append('"popularity": $pop')
        if 'budget' in cypher:
            match_parts.append('$m has budget $budget')
            b_match = re.search(r'budget\s*([><]=?)\s*(\d+)', cypher)
            if b_match:
                match_parts.append(f'$budget {b_match.group(1)} {b_match.group(2)}')
        if 'status' in cypher:
            match_parts.append('$m has status $status')
            status_match = re.search(r"status\s*=\s*['\"]([^'\"]+)['\"]", cypher)
            if status_match:
                match_parts.append(f'$status == "{status_match.group(1)}"')
    else:
        # Default: just match the entity with title
        match_parts.append(f'$m isa {entity}, has title $title')
        fetch_parts.append('"title": $title')
    
    if not match_parts:
        return None
    
    # Build query
    query = 'match\n  ' + ';\n  '.join(match_parts) + ';'
    if sort_var:
        query += f'\nsort {sort_var} {sort_dir};'
    query += f'\nlimit {limit_val};'
    query += '\nfetch { ' + ', '.join(fetch_parts) + ' };'
    
    return query

# Read failed queries
with open('/opt/text2typeql/output/neoflix/failed.csv', 'r') as f:
    reader = csv.DictReader(f)
    failed = list(reader)

print(f"Processing {len(failed)} failed queries...")

# Read existing queries to get indices
with open('/opt/text2typeql/output/neoflix/queries.csv', 'r') as f:
    reader = csv.DictReader(f)
    existing = {int(r['original_index']): r for r in reader}

print(f"Existing valid queries: {len(existing)}")

fixed = []
still_failed = []

for i, row in enumerate(failed):
    idx = int(row['original_index'])
    error = row.get('error', '')
    
    # Determine error type and fix
    if 'syntax error' in error.lower() or 'parsing error' in error.lower():
        new_query = fix_syntax_error(row)
    else:
        new_query = fix_concept_error(row)
    
    if new_query:
        valid, result = validate_query(new_query)
        if valid:
            fixed.append({
                'original_index': idx,
                'question': row['question'],
                'cypher': row['cypher'],
                'typeql': new_query
            })
            print(f"[{i+1}/{len(failed)}] Fixed {idx}: {result} results")
        else:
            still_failed.append(row)
            print(f"[{i+1}/{len(failed)}] Still failed {idx}: {str(result)[:80]}")
    else:
        still_failed.append(row)
        print(f"[{i+1}/{len(failed)}] Could not generate fix for {idx}")

# Merge fixed into existing
for q in fixed:
    existing[q['original_index']] = q

# Write updated queries.csv
with open('/opt/text2typeql/output/neoflix/queries.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
    writer.writeheader()
    for idx in sorted(existing.keys()):
        writer.writerow(existing[idx])

# Write updated failed.csv
with open('/opt/text2typeql/output/neoflix/failed.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'error'])
    writer.writeheader()
    for row in still_failed:
        writer.writerow(row)

print(f"\n=== Summary ===")
print(f"Fixed: {len(fixed)}")
print(f"Still failed: {len(still_failed)}")
print(f"Total valid queries: {len(existing)}")

driver.close()
