#!/usr/bin/env python3
"""Fix failed semantic review queries for the movies database."""

import csv
import re
from pathlib import Path
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType


def connect_typedb():
    """Connect to TypeDB."""
    credentials = Credentials("admin", "password")
    options = DriverOptions(is_tls_enabled=False)
    driver = TypeDB.driver("localhost:1729", credentials, options)
    return driver


def validate_query(driver, database: str, query: str) -> tuple[bool, str]:
    """Validate a TypeQL query against the database."""
    try:
        with driver.transaction(database, TransactionType.READ) as tx:
            # For fetch queries, we try to execute them
            if "fetch" in query:
                result = tx.query(query).resolve()
                # Try to get at least the first result to validate
                docs = result.as_concept_documents()
                for doc in docs:
                    break
            else:
                # For reduce queries
                result = tx.query(query).resolve()
        return True, ""
    except Exception as e:
        return False, str(e)


def find_person_var(typeql: str) -> str:
    """Find the person variable in a TypeQL query."""
    # Common patterns
    patterns = [
        r'\$(\w+)\s+isa\s+person',
        r'\((?:actor|director|producer|writer|reviewer|follower):\s*\$(\w+)',
    ]
    for pattern in patterns:
        m = re.search(pattern, typeql)
        if m:
            return m.group(1)
    return 'p'


def find_movie_var(typeql: str) -> str:
    """Find the movie variable in a TypeQL query."""
    patterns = [
        r'\$(\w+)\s+isa\s+movie',
        r'film:\s*\$(\w+)\)',
    ]
    for pattern in patterns:
        m = re.search(pattern, typeql)
        if m:
            return m.group(1)
    return 'm'


def add_relation(typeql: str, relation_type: str, role1: str, role2: str, person_var: str, movie_var: str) -> str:
    """Add a relation constraint to a TypeQL query."""
    relation_line = f"  ({role1}: ${person_var}, {role2}: ${movie_var}) isa {relation_type};"

    # Check if relation already exists
    if f"isa {relation_type}" in typeql and f"{role1}:" in typeql:
        return typeql

    lines = typeql.split('\n')
    new_lines = []
    inserted = False

    for i, line in enumerate(lines):
        new_lines.append(line)

        # Insert after the last entity declaration or existing relation, before fetch/sort/limit
        if not inserted:
            # Check if next line is fetch, sort, limit, reduce
            if i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                if any(next_line.startswith(x) for x in ['fetch', 'sort', 'limit', 'reduce']):
                    new_lines.append(relation_line)
                    inserted = True

    # If not inserted, find insertion point before fetch
    if not inserted:
        result = []
        for line in new_lines:
            if not inserted and line.strip().startswith('fetch'):
                result.append(relation_line)
                inserted = True
            result.append(line)
        return '\n'.join(result)

    return '\n'.join(new_lines)


def add_contains_filter(typeql: str, cypher: str, attr_name: str = None) -> str:
    """Add a CONTAINS filter to a TypeQL query."""
    # Extract CONTAINS value from Cypher
    contains_match = re.search(r"(?:toLower\()?\w+\.(\w+)\)?\s+CONTAINS\s+'([^']+)'", cypher, re.IGNORECASE)
    if not contains_match:
        return typeql

    attr = attr_name or contains_match.group(1)
    search_term = contains_match.group(2)

    # Find the attribute variable in TypeQL
    attr_var_match = re.search(rf'\$\w+\s+has\s+{attr}\s+(\$\w+)', typeql)
    if attr_var_match:
        attr_var = attr_var_match.group(1)
        # Check if filter already exists
        if f'{attr_var} like' in typeql or f'{attr_var} contains' in typeql:
            return typeql

        # Add filter after the has line
        lines = typeql.split('\n')
        new_lines = []
        for line in lines:
            new_lines.append(line)
            if f'has {attr} {attr_var}' in line:
                new_lines.append(f'  {attr_var} like ".*{search_term}.*";')
        return '\n'.join(new_lines)
    else:
        # Need to add the attribute first
        movie_var = find_movie_var(typeql)
        lines = typeql.split('\n')
        new_lines = []
        inserted = False
        for i, line in enumerate(lines):
            new_lines.append(line)
            if not inserted and f'${movie_var} isa movie' in line:
                new_lines.append(f'  ${movie_var} has {attr} ${attr}{movie_var};')
                new_lines.append(f'  ${attr}{movie_var} like ".*{search_term}.*";')
                inserted = True
        return '\n'.join(new_lines)


def add_starts_with_filter(typeql: str, cypher: str) -> str:
    """Add a STARTS WITH filter to a TypeQL query."""
    starts_match = re.search(r"(\w+)\.(\w+)\s+STARTS\s+WITH\s+'([^']+)'", cypher, re.IGNORECASE)
    if not starts_match:
        return typeql

    entity = starts_match.group(1)
    attr = starts_match.group(2)
    search_term = starts_match.group(3)

    # Find the attribute variable in TypeQL
    attr_var_match = re.search(rf'\$\w+\s+has\s+{attr}\s+(\$\w+)', typeql)
    if attr_var_match:
        attr_var = attr_var_match.group(1)
        if f'{attr_var} like' in typeql:
            return typeql

        lines = typeql.split('\n')
        new_lines = []
        for line in lines:
            new_lines.append(line)
            if f'has {attr} {attr_var}' in line:
                new_lines.append(f'  {attr_var} like "^{search_term}.*";')
        return '\n'.join(new_lines)

    return typeql


def add_ends_with_filter(typeql: str, cypher: str) -> str:
    """Add an ENDS WITH filter to a TypeQL query."""
    ends_match = re.search(r"(\w+)\.(\w+)\s+ENDS\s+WITH\s+'([^']+)'", cypher, re.IGNORECASE)
    if not ends_match:
        return typeql

    attr = ends_match.group(2)
    search_term = ends_match.group(3)

    # Find the attribute variable in TypeQL
    attr_var_match = re.search(rf'\$\w+\s+has\s+{attr}\s+(\$\w+)', typeql)
    if attr_var_match:
        attr_var = attr_var_match.group(1)
        if f'{attr_var} like' in typeql:
            return typeql

        lines = typeql.split('\n')
        new_lines = []
        for line in lines:
            new_lines.append(line)
            if f'has {attr} {attr_var}' in line:
                new_lines.append(f'  {attr_var} like ".*{search_term}$";')
        return '\n'.join(new_lines)

    return typeql


def apply_specific_fixes():
    """Return a dictionary of specific fixes for each query by original_index."""

    fixes = {}

    # Query 13: Missing PRODUCED relation
    fixes[13] = """match
  $p isa person;
  (actor: $p, film: $m1) isa acted_in;
  (producer: $p, film: $m2) isa produced;
  $p has name $namep;
fetch {
  "name": $namep
};"""

    # Query 24: Missing tagline filter
    fixes[24] = """match
  $m isa movie;
  $m has tagline $taglinem;
  $taglinem like ".*limits.*";
  $m has title $titlem;
limit 3;
fetch {
  "title": $titlem
};"""

    # Query 28: Missing REVIEWED relation
    fixes[28] = """match
  $p isa person;
  $m isa movie;
  (producer: $p, film: $m) isa produced;
  (reviewer: $p, film: $m) isa reviewed;
  $p has name $namep;
  $m has title $titlem;
fetch {
  "person": $namep,
  "movie": $titlem
};"""

    # Query 29: Missing WROTE relation
    fixes[29] = """match
  $p isa person;
  $m isa movie;
  (director: $p, film: $m) isa directed;
  (writer: $p, film: $m) isa wrote;
  $p has name $namep;
limit 3;
fetch {
  "person": $namep
};"""

    # Query 37: Missing DIRECTED and PRODUCED - needs all three
    fixes[37] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  (director: $p, film: $m) isa directed;
  (producer: $p, film: $m) isa produced;
  $p has name $namep;
  $m has title $titlem;
fetch {
  "person": $namep,
  "movie": $titlem
};"""

    # Query 39: Missing ACTED_IN relation
    fixes[39] = """match
  $p isa person;
  $m isa movie;
  (writer: $p, film: $m) isa wrote;
  (actor: $p, film: $m) isa acted_in;
  $p has name $namep;
  $m has title $titlem;
fetch {
  "person": $namep,
  "movie": $titlem
};"""

    # Query 48: Missing PRODUCED relation
    fixes[48] = """match
  $p isa person;
  $m isa movie, has title "The Matrix Revolutions";
  (reviewer: $p, film: $m) isa reviewed;
  (producer: $p, film: $m) isa produced;
  $p has name $namep;
fetch {
  "name": $namep
};"""

    # Query 49: Missing DIRECTED relation
    fixes[49] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  (director: $p, film: $m) isa directed;
  $m has title $titlem;
limit 3;
fetch {
  "title": $titlem
};"""

    # Query 65: Missing ACTED_IN relation
    fixes[65] = """match
  $p isa person;
  $m isa movie;
  (reviewer: $p, film: $m) isa reviewed;
  (actor: $p, film: $m) isa acted_in;
  $m has title $titlem;
  $p has name $namep;
fetch {
  "title": $titlem,
  "name": $namep
};"""

    # Query 66: Missing DIRECTED relation
    fixes[66] = """match
  $p isa person;
  $m isa movie;
  (director: $p, film: $m) isa directed;
  $p2 isa person;
  (follower: $p, followed: $p2) isa follows;
  $p has name $namep;
limit 3;
fetch {
  "name": $namep
};"""

    # Query 72: Missing DIRECTED relation - oldest directors
    fixes[72] = """match
  $p isa person;
  $m isa movie;
  (director: $p, film: $m) isa directed;
  $p has name $namep;
  $p has born $bornp;
sort $bornp asc;
limit 5;
fetch {
  "name": $namep,
  "born": $bornp
};"""

    # Query 74: Missing DIRECTED relation
    fixes[74] = """match
  $p isa person;
  $m isa movie;
  (writer: $p, film: $m) isa wrote;
  (director: $p, film: $m) isa directed;
  $p has name $namep;
  $m has title $titlem;
limit 3;
fetch {
  "person": $namep,
  "movie": $titlem
};"""

    # Query 75: Missing movie constraint and ACTED_IN
    fixes[75] = """match
  $m isa movie, has tagline "Speed has no limits";
  $p isa person;
  (actor: $p, film: $m) isa acted_in;
  $p has name $namep;
limit 3;
fetch {
  "name": $namep
};"""

    # Query 78: Missing REVIEWED relation
    fixes[78] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  (reviewer: $p, film: $m) isa reviewed;
  $m has title $titlem;
limit 3;
fetch {
  "title": $titlem
};"""

    # Query 83: Missing tagline filter
    fixes[83] = """match
  $p isa person;
  $m isa movie;
  (director: $p, film: $m) isa directed;
  $m has tagline $taglinem;
  $taglinem like ".*limits.*";
  $p has name $namep;
limit 5;
fetch {
  "name": $namep
};"""

    # Query 96: Missing REVIEWED relation
    fixes[96] = """match
  $p isa person;
  $m isa movie;
  (writer: $p, film: $m) isa wrote;
  (reviewer: $p, film: $m) isa reviewed;
  $p has name $namep;
  $m has title $titlem;
limit 3;
fetch {
  "person": $namep,
  "movie": $titlem
};"""

    # Query 100: Missing REVIEWED relation
    fixes[100] = """match
  $d isa person;
  $m isa movie;
  (director: $d, film: $m) isa directed;
  (reviewer: $d, film: $m) isa reviewed;
  $m has title $titlem;
limit 3;
fetch {
  "title": $titlem
};"""

    # Query 107: Missing ACTED_IN and actor born constraint
    fixes[107] = """match
  $director isa person, has born 1965;
  $actor isa person, has born 1952;
  $movie isa movie;
  (director: $director, film: $movie) isa directed;
  (actor: $actor, film: $movie) isa acted_in;
  $movie has title $titlemovie;
limit 3;
fetch {
  "title": $titlemovie
};"""

    # Query 110: Missing PRODUCED relation
    fixes[110] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  (producer: $p, film: $m) isa produced;
  $p has name $namep;
  $m has title $titlem;
limit 3;
fetch {
  "person": $namep,
  "movie": $titlem
};"""

    # Query 115: Missing ACTED_IN relation
    fixes[115] = """match
  $p isa person;
  $m isa movie;
  (director: $p, film: $m) isa directed;
  (actor: $p, film: $m) isa acted_in;
  $m has title $titlem;
  $p has name $namep;
  $m has votes $votesm;
sort $votesm desc;
limit 5;
fetch {
  "movie": $titlem,
  "person": $namep
};"""

    # Query 118: Missing REVIEWED relation with summary
    fixes[118] = """match
  $p isa person;
  $m isa movie;
  (producer: $p, film: $m) isa produced;
  $r (reviewer: $reviewer, film: $m) isa reviewed, has summary $summary;
  $summary == "Dark, but compelling";
  $p has born $bornp;
  $bornp < 1960;
  $m has title $titlem;
limit 3;
fetch {
  "title": $titlem
};"""

    # Query 119: Missing DIRECTED relation
    fixes[119] = """match
  $m isa movie;
  $d isa person;
  (director: $d, film: $m) isa directed;
  $m has tagline $taglinem;
  $taglinem like ".*Real World.*";
  $d has name $named;
reduce $num_movies = count($m) groupby $named;
sort $num_movies desc;
limit 5;
fetch {
  "director": $named,
  "num_movies": $num_movies
};"""

    # Query 122: Missing PRODUCED relation
    fixes[122] = """match
  $a isa person;
  $m isa movie;
  $p isa person;
  (actor: $a, film: $m) isa acted_in;
  (producer: $p, film: $m) isa produced;
  $a has born $borna;
  $borna > 1980;
  $p has born $bornp;
  $bornp < 1950;
  $a has name $namea;
  $m has title $titlem;
sort $borna asc;
limit 3;
fetch {
  "actor": $namea,
  "movie": $titlem
};"""

    # Query 125: Missing tagline filter
    fixes[125] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  $p has born $bornp;
  $bornp < 1950;
  $m has tagline $taglinem;
  $taglinem like ".*limits.*";
  $p has name $namep;
limit 3;
fetch {
  "name": $namep
};"""

    # Query 126: Missing REVIEWED relation
    fixes[126] = """match
  $d isa person;
  $m isa movie;
  (director: $d, film: $m) isa directed;
  (reviewer: $d, film: $m) isa reviewed;
  $d has name $named;
reduce $num_reviewed = count($m) groupby $named;
sort $num_reviewed desc;
limit 5;
fetch {
  "director": $named,
  "num_reviewed": $num_reviewed
};"""

    # Query 128: Missing REVIEWED relation with summary
    fixes[128] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  $r (reviewer: $reviewer, film: $m) isa reviewed, has summary $summary;
  $summary == "Slapstick redeemed only by the Robin Williams and ";
  $p has name $namep;
reduce $movie_count = count($m) groupby $namep;
sort $movie_count desc;
limit 5;
fetch {
  "actor": $namep,
  "movie_count": $movie_count
};"""

    # Query 142: Missing DIRECTED relation
    fixes[142] = """match
  $p isa person;
  $m isa movie;
  (writer: $p, film: $m) isa wrote;
  (director: $p, film: $m) isa directed;
  $m has title $titlem;
fetch {
  "title": $titlem
};"""

    # Query 147: Missing PRODUCED relation
    fixes[147] = """match
  $p isa person;
  $m isa movie;
  (producer: $p, film: $m) isa produced;
  $p has name $namep;
  $p has born $bornp;
sort $bornp desc;
limit 5;
fetch {
  "producer": $namep,
  "birthYear": $bornp
};"""

    # Query 152: Missing DIRECTED and WROTE
    fixes[152] = """match
  $p isa person;
  $m isa movie;
  (producer: $p, film: $m) isa produced;
  (director: $p, film: $m) isa directed;
  (writer: $p, film: $m) isa wrote;
  $p has name $namep;
limit 3;
fetch {
  "name": $namep
};"""

    # Query 167: Missing movie entity, tagline constraint, and PRODUCED relation
    fixes[167] = """match
  $m isa movie, has tagline "Speed has no limits";
  $p isa person;
  (producer: $p, film: $m) isa produced;
  $m has title $titlem;
  $p has name $namep;
fetch {
  "movie": $titlem,
  "producer": $namep
};"""

    # Query 168: Missing ACTED_IN relation
    fixes[168] = """match
  $p isa person;
  $m isa movie;
  (writer: $p, film: $m) isa wrote;
  (actor: $p, film: $m) isa acted_in;
  $p has name $namep;
fetch {
  "person": $namep
};"""

    # Query 171: Missing DIRECTED relation
    fixes[171] = """match
  $p isa person;
  $m isa movie;
  (producer: $p, film: $m) isa produced;
  (director: $p, film: $m) isa directed;
  $m has title $titlem;
limit 3;
fetch {
  "title": $titlem
};"""

    # Query 172: Missing ACTED_IN relation
    fixes[172] = """match
  $p isa person, has name "Keanu Reeves";
  $m isa movie;
  $r (reviewer: $p, film: $m) isa reviewed, has rating $ratingr;
  $ratingr > 90;
  $a (actor: $p, film: $m) isa acted_in, has roles $rolesa;
  $m has title $titlem;
fetch {
  "movie": $titlem,
  "roles": $rolesa
};"""

    # Query 173: Missing REVIEWED relation
    fixes[173] = """match
  $m isa movie;
  $p isa person;
  (reviewer: $p, film: $m) isa reviewed;
  $m has tagline $taglinem;
  $taglinem like ".*journey.*";
  $m has title $titlem;
fetch {
  "title": $titlem
};"""

    # Query 175: Missing roles CONTAINS 'Captain' filter
    fixes[175] = """match
  $m isa movie;
  $p isa person;
  $ai (actor: $p, film: $m) isa acted_in, has roles $rolesai;
  $rolesai like ".*Captain.*";
  $m has title $titlem;
  $p has name $namep;
limit 3;
fetch {
  "movie": $titlem,
  "actors": $namep
};"""

    # Query 177: Missing PRODUCED relation
    fixes[177] = """match
  $p isa person;
  $m isa movie;
  (reviewer: $p, film: $m) isa reviewed;
  (producer: $p, film: $m) isa produced;
  $p has name $namep;
limit 3;
fetch {
  "name": $namep
};"""

    # Query 179: Missing tagline filter
    fixes[179] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  $m has tagline $taglinem;
  $taglinem like ".*limits.*";
  $p has name $namep;
reduce $movie_count = count($m) groupby $namep;
sort $movie_count desc;
limit 3;
fetch {
  "actor": $namep,
  "movie_count": $movie_count
};"""

    # Query 183: Missing ACTED_IN relation
    fixes[183] = """match
  $p isa person;
  $m1 isa movie;
  $m2 isa movie;
  (actor: $p, film: $m1) isa acted_in;
  (director: $p, film: $m2) isa directed;
  $p has name $namep;
  $p has born $bornp;
sort $bornp desc;
limit 3;
fetch {
  "name": $namep,
  "birthYear": $bornp
};"""

    # Query 191: Missing DIRECTED relation
    fixes[191] = """match
  $m isa movie;
  $p isa person;
  (actor: $p, film: $m) isa acted_in;
  (director: $p, film: $m) isa directed;
  $m has released $releasedm;
  $releasedm < 2000;
  $m has title $titlem;
  $p has name $namep;
fetch {
  "movie": $titlem,
  "person": $namep
};"""

    # Query 198: Missing DIRECTED relation
    fixes[198] = """match
  $p isa person;
  $m isa movie;
  (writer: $p, film: $m) isa wrote;
  (director: $p, film: $m) isa directed;
  $p has name $namep;
  $m has title $titlem;
limit 3;
fetch {
  "person": $namep,
  "movie": $titlem
};"""

    # Query 199: Missing roles CONTAINS 'Doctor' filter
    fixes[199] = """match
  $m isa movie;
  $p isa person;
  $ai (actor: $p, film: $m) isa acted_in, has roles $rolesai;
  $rolesai like ".*Doctor.*";
  $m has title $titlem;
  $p has name $namep;
  $m has votes $votesm;
sort $votesm desc;
limit 5;
fetch {
  "movie": $titlem,
  "actor": $namep
};"""

    # Query 200: Missing REVIEWED relation
    fixes[200] = """match
  $p isa person;
  $m isa movie;
  (producer: $p, film: $m) isa produced;
  (reviewer: $p, film: $m) isa reviewed;
  $m has title $titlem;
fetch {
  "title": $titlem
};"""

    # Query 201: Missing ACTED_IN relation
    fixes[201] = """match
  $p isa person;
  $m isa movie;
  (director: $p, film: $m) isa directed;
  (actor: $p, film: $m) isa acted_in;
  $m has title $titlem;
limit 3;
fetch {
  "title": $titlem
};"""

    # Query 205: Missing ACTED_IN relation
    fixes[205] = """match
  $p isa person;
  $m isa movie;
  $a isa person;
  (producer: $p, film: $m) isa produced;
  $ai (actor: $a, film: $m) isa acted_in, has roles $rolesai;
  $p has born $bornp;
  $bornp > 1980;
  $m has title $titlem;
  $m has released $releasedm;
sort $releasedm asc;
limit 3;
fetch {
  "movie": $titlem,
  "roles": $rolesai
};"""

    # Query 211: Missing ACTED_IN relation
    fixes[211] = """match
  $p isa person;
  $m isa movie;
  (director: $p, film: $m) isa directed;
  (actor: $p, film: $m) isa acted_in;
  $p has name $namep;
  $m has title $titlem;
limit 3;
fetch {
  "name": $namep,
  "title": $titlem
};"""

    # Query 213: Missing REVIEWED relation with rating filter
    fixes[213] = """match
  $m isa movie;
  $p isa person;
  $r (reviewer: $p, film: $m) isa reviewed, has rating $rating;
  $rating > 80;
  $m has released $releasedm;
  $releasedm >= 1980;
  $releasedm <= 1990;
  $m has title $titlem;
fetch {
  "title": $titlem,
  "released": $releasedm
};"""

    # Query 218: Missing REVIEWED relation
    fixes[218] = """match
  $p isa person;
  $m isa movie;
  (writer: $p, film: $m) isa wrote;
  (reviewer: $p, film: $m) isa reviewed;
  $m has title $titlem;
limit 3;
fetch {
  "title": $titlem
};"""

    # Query 220: Missing REVIEWED relation
    fixes[220] = """match
  $p isa person;
  $m isa movie;
  (producer: $p, film: $m) isa produced;
  $r (reviewer: $reviewer, film: $m) isa reviewed, has rating $ratingr;
  $ratingr > 90;
  $p has born $bornp;
  $bornp < 1960;
  $m has title $titlem;
  $m has released $releasedm;
sort $releasedm asc;
limit 3;
fetch {
  "title": $titlem
};"""

    # Query 222: Missing REVIEWED relation
    fixes[222] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  (reviewer: $p, film: $m) isa reviewed;
  $p has name $namep;
  $m has title $titlem;
limit 3;
fetch {
  "person": $namep,
  "movie": $titlem
};"""

    # Query 224: Missing DIRECTED relation
    fixes[224] = """match
  $p isa person;
  $m isa movie;
  (writer: $p, film: $m) isa wrote;
  (director: $p, film: $m) isa directed;
  $m has released $releasedm;
  $releasedm > 2000;
  $p has name $namep;
  $m has title $titlem;
limit 3;
fetch {
  "name": $namep,
  "title": $titlem
};"""

    # Query 227: Missing ACTED_IN relation
    fixes[227] = """match
  $p isa person;
  $m isa movie;
  (producer: $p, film: $m) isa produced;
  (actor: $p, film: $m) isa acted_in;
  $p has name $namep;
limit 3;
fetch {
  "name": $namep
};"""

    # Query 233: Missing tagline filter
    fixes[233] = """match
  $m isa movie;
  $m has tagline $taglinem;
  $taglinem like ".*limit.*";
  $m has title $titlem;
fetch {
  "title": $titlem
};"""

    # Query 244: Missing DIRECTED relation
    fixes[244] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  (director: $p, film: $m) isa directed;
  $m has title $titlem;
  $p has name $namep;
fetch {
  "movie": $titlem,
  "person": $namep
};"""

    # Query 248: Missing ACTED_IN and DIRECTED relations
    fixes[248] = """match
  $p isa person;
  $m isa movie;
  (producer: $p, film: $m) isa produced;
  (director: $p, film: $m) isa directed;
  (actor: $p, film: $m) isa acted_in;
  $p has name $namep;
  $m has title $titlem;
fetch {
  "person": $namep,
  "movie": $titlem
};"""

    # Query 251: Missing DIRECTED relation
    fixes[251] = """match
  $p isa person;
  $m isa movie;
  (director: $p, film: $m) isa directed;
  $p has name $namep;
  $p has born $bornp;
sort $bornp desc;
limit 3;
fetch {
  "director": $namep,
  "birthYear": $bornp
};"""

    # Query 252: Missing REVIEWED relation
    fixes[252] = """match
  $p isa person;
  $m isa movie;
  $r (reviewer: $p, film: $m) isa reviewed, has summary $summaryr;
fetch {
  "summary": $summaryr
};"""

    # Query 255: Missing DIRECTED by Lana Wachowski
    fixes[255] = """match
  $p isa person;
  $m isa movie;
  $d isa person, has name "Lana Wachowski";
  (actor: $p, film: $m) isa acted_in;
  (director: $d, film: $m) isa directed;
  $p has name $namep;
  $m has title $titlem;
fetch {
  "actor": $namep,
  "movie": $titlem
};"""

    # Query 258: Missing REVIEWED relation
    fixes[258] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  (reviewer: $p, film: $m) isa reviewed;
  $p has name $namep;
  $m has title $titlem;
fetch {
  "person": $namep,
  "movie": $titlem
};"""

    # Query 259: Missing DIRECTED relation
    fixes[259] = """match
  $p isa person;
  $m isa movie;
  (director: $p, film: $m) isa directed;
  $p2 isa person;
  (follower: $p, followed: $p2) isa follows;
  $p has name $namep;
fetch {
  "name": $namep
};"""

    # Query 261: Missing PRODUCED relation
    fixes[261] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  (producer: $p, film: $m) isa produced;
  $m has title $titlem;
  $p has name $namep;
fetch {
  "movie": $titlem,
  "person": $namep
};"""

    # Query 263: Missing DIRECTED and PRODUCED
    fixes[263] = """match
  $p isa person;
  $m isa movie;
  (writer: $p, film: $m) isa wrote;
  (director: $p, film: $m) isa directed;
  (producer: $p, film: $m) isa produced;
  $p has name $namep;
  $m has title $titlem;
fetch {
  "person": $namep,
  "movie": $titlem
};"""

    # Query 278: Missing DIRECTED by Lana Wachowski
    fixes[278] = """match
  $reviewer isa person;
  $movie isa movie;
  $director isa person, has name "Lana Wachowski";
  (reviewer: $reviewer, film: $movie) isa reviewed;
  (director: $director, film: $movie) isa directed;
  $reviewer has name $namereviewer;
  $movie has title $titlemovie;
fetch {
  "reviewer": $namereviewer,
  "movie": $titlemovie
};"""

    # Query 280: Missing DIRECTED relation
    fixes[280] = """match
  $p isa person;
  $m isa movie;
  (producer: $p, film: $m) isa produced;
  (director: $p, film: $m) isa directed;
  $m has title $titlem;
  $p has name $namep;
fetch {
  "title": $titlem,
  "name": $namep
};"""

    # Query 283: Missing REVIEWED relation
    fixes[283] = """match
  $p isa person, has name "Joel Silver";
  $m isa movie;
  (producer: $p, film: $m) isa produced;
  (reviewer: $p, film: $m) isa reviewed;
  $m has title $titlem;
  $m has votes $votesm;
  $m has tagline $taglinem;
  $m has released $releasedm;
fetch {
  "title": $titlem,
  "votes": $votesm,
  "tagline": $taglinem,
  "released": $releasedm
};"""

    # Query 284: Missing DIRECTED relation
    fixes[284] = """match
  $m isa movie;
  $d isa person;
  (director: $d, film: $m) isa directed;
  $m has votes $votesm;
  $votesm > 1000;
  $d has name $named;
  $m has title $titlem;
fetch {
  "director": $named,
  "movie": $titlem
};"""

    # Query 305: Missing ACTED_IN relation
    fixes[305] = """match
  $p isa person;
  $m1 isa movie;
  $m2 isa movie;
  (actor: $p, film: $m1) isa acted_in;
  (producer: $p, film: $m2) isa produced;
  $p has name $namep;
limit 3;
fetch {
  "actor_producer": $namep
};"""

    # Query 306: Missing tagline filter
    fixes[306] = """match
  $m isa movie;
  $m has tagline $taglinem;
  $taglinem like ".*limit.*";
  $m has title $titlem;
fetch {
  "title": $titlem
};"""

    # Query 308: Missing DIRECTED relation
    fixes[308] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  (director: $p, film: $m) isa directed;
  $m has title $titlem;
  $p has name $namep;
fetch {
  "movie": $titlem,
  "person": $namep
};"""

    # Query 309: Missing PRODUCED relation
    fixes[309] = """match
  $p isa person;
  $m isa movie;
  (producer: $p, film: $m) isa produced;
  $p has name $namep;
  $p has born $bornp;
sort $bornp desc;
limit 3;
fetch {
  "name": $namep,
  "born": $bornp
};"""

    # Query 310: Missing tagline filter
    fixes[310] = """match
  $m isa movie;
  $m has tagline $taglinem;
  $taglinem like ".*real.*";
  $m has title $titlem;
limit 3;
fetch {
  "title": $titlem
};"""

    # Query 312: Missing REVIEWED relation
    fixes[312] = """match
  $p isa person;
  $m1 isa movie;
  $m2 isa movie;
  (reviewer: $p, film: $m1) isa reviewed;
  (actor: $p, film: $m2) isa acted_in;
  $p has name $namep;
limit 3;
fetch {
  "name": $namep
};"""

    # Query 314: Missing DIRECTED relation
    fixes[314] = """match
  $p isa person;
  $m isa movie;
  (producer: $p, film: $m) isa produced;
  (director: $p, film: $m) isa directed;
  $m has title $titlem;
  $p has name $namep;
fetch {
  "movie": $titlem,
  "person": $namep
};"""

    # Query 332: Missing REVIEWED relation
    fixes[332] = """match
  $p isa person;
  $m isa movie;
  (writer: $p, film: $m) isa wrote;
  $r (reviewer: $reviewer, film: $m) isa reviewed, has rating $ratingr;
  $ratingr >= 80;
  $p has name $namep;
  $m has title $titlem;
limit 5;
fetch {
  "writer": $namep,
  "movie": $titlem,
  "rating": $ratingr
};"""

    # Query 337: Missing REVIEWED relation
    fixes[337] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  (reviewer: $p, film: $m) isa reviewed;
  $p has name $namep;
reduce $count = count($m) groupby $namep;
sort $count desc;
limit 5;
fetch {
  "name": $namep
};"""

    # Query 339: Missing PRODUCED relation
    fixes[339] = """match
  $p isa person;
  $m isa movie;
  (director: $p, film: $m) isa directed;
  (producer: $p, film: $m) isa produced;
  $p has name $namep;
reduce $num_movies = count($m) groupby $namep;
sort $num_movies desc;
limit 3;
fetch {
  "person": $namep,
  "num_movies": $num_movies
};"""

    # Query 342: Missing REVIEWED relation
    fixes[342] = """match
  $p isa person;
  $m isa movie;
  (producer: $p, film: $m) isa produced;
  (reviewer: $p, film: $m) isa reviewed;
  $m has title $titlem;
  $p has name $namep;
  $m has votes $votesm;
sort $votesm desc;
limit 3;
fetch {
  "movie": $titlem,
  "person": $namep
};"""

    # Query 343: Missing REVIEWED relation
    fixes[343] = """match
  $m isa movie;
  $p isa person;
  (actor: $p, film: $m) isa acted_in;
  $r (reviewer: $reviewer, film: $m) isa reviewed, has rating $ratingr;
  $ratingr < 60;
  $m has title $titlem;
limit 5;
fetch {
  "title": $titlem
};"""

    # Query 345: Missing tagline filter
    fixes[345] = """match
  $m isa movie;
  $m has tagline $taglinem;
  $taglinem like ".*limits.*";
  $m has released $releasedm;
  $releasedm > 2005;
  $m has title $titlem;
limit 3;
fetch {
  "title": $titlem,
  "tagline": $taglinem,
  "released": $releasedm
};"""

    # Query 353: Missing REVIEWED relation
    fixes[353] = """match
  $p isa person;
  $m isa movie;
  (director: $p, film: $m) isa directed;
  $r (reviewer: $reviewer, film: $m) isa reviewed, has summary $summaryr;
  $summaryr == "Pretty funny at times";
  $p has name $namep;
reduce $num_movies = count($m) groupby $namep;
sort $num_movies desc;
limit 3;
fetch {
  "director": $namep,
  "num_movies": $num_movies
};"""

    # Query 378: Missing ACTED_IN and PRODUCED
    fixes[378] = """match
  $p isa person;
  $m isa movie;
  (director: $p, film: $m) isa directed;
  (producer: $p, film: $m) isa produced;
  (actor: $p, film: $m) isa acted_in;
  $p has name $namep;
reduce $movieCount = count($m) groupby $namep;
sort $movieCount desc;
limit 3;
fetch {
  "person": $namep,
  "movieCount": $movieCount
};"""

    # Query 382: Missing DIRECTED relation
    fixes[382] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  (director: $p, film: $m) isa directed;
  $m has title $titlem;
  $p has name $namep;
fetch {
  "movie": $titlem,
  "person": $namep
};"""

    # Query 387: Missing DIRECTED and PRODUCED
    fixes[387] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  (director: $p, film: $m) isa directed;
  (producer: $p, film: $m) isa produced;
  $p has name $namep;
  $m has title $titlem;
fetch {
  "person": $namep,
  "movie": $titlem
};"""

    # Query 392: Missing PRODUCED relation
    fixes[392] = """match
  $p isa person;
  $m isa movie;
  (producer: $p, film: $m) isa produced;
  $p has name $namep;
  $p has born $bornp;
sort $bornp desc;
limit 5;
fetch {
  "name": $namep,
  "born": $bornp
};"""

    # Query 399: Missing DIRECTED relation
    fixes[399] = """match
  $p isa person;
  $m isa movie;
  (director: $p, film: $m) isa directed;
  $p has name $namep;
  $p has born $bornp;
sort $bornp desc;
limit 3;
fetch {
  "director": $namep,
  "birthYear": $bornp
};"""

    # Query 403: Missing REVIEWED relation
    fixes[403] = """match
  $p isa person;
  $m isa movie;
  (director: $p, film: $m) isa directed;
  (reviewer: $p, film: $m) isa reviewed;
  $p has name $namep;
  $m has title $titlem;
fetch {
  "person": $namep,
  "movie": $titlem
};"""

    # Query 407: Missing ACTED_IN relation
    fixes[407] = """match
  $director isa person;
  $movie isa movie;
  (director: $director, film: $movie) isa directed;
  (actor: $director, film: $movie) isa acted_in;
  $director has name $namedirector;
  $movie has title $titlemovie;
fetch {
  "director": $namedirector,
  "movies": $titlemovie
};"""

    # Query 410: Missing WROTE relation
    fixes[410] = """match
  $p isa person;
  $m isa movie;
  (producer: $p, film: $m) isa produced;
  (writer: $p, film: $m) isa wrote;
  $p has name $namep;
  $m has title $titlem;
fetch {
  "person": $namep,
  "movie": $titlem
};"""

    # Query 413: Missing PRODUCED relation
    fixes[413] = """match
  $m isa movie;
  $p isa person;
  (director: $p, film: $m) isa directed;
  (producer: $p, film: $m) isa produced;
  $m has title $titlem;
  $p has name $namep;
fetch {
  "movie": $titlem,
  "person": $namep
};"""

    # Query 418: Missing tagline STARTS WITH filter
    fixes[418] = """match
  $m isa movie;
  $m has tagline $taglinem;
  $taglinem like "^The.*";
  $m has title $titlem;
fetch {
  "title": $titlem,
  "tagline": $taglinem
};"""

    # Query 421: Missing PRODUCED relation
    fixes[421] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  (producer: $p, film: $m) isa produced;
  $m has title $titlem;
  $p has name $namep;
fetch {
  "movie": $titlem,
  "person": $namep
};"""

    # Query 422: Missing follows relation
    fixes[422] = """match
  $follower isa person;
  $keanu isa person, has name "Keanu Reeves";
  $movie isa movie;
  (follower: $follower, followed: $keanu) isa follows;
  (reviewer: $follower, film: $movie) isa reviewed;
  $movie has title $titlemovie;
  $follower has name $namefollower;
fetch {
  "movie": $titlemovie,
  "reviewer": $namefollower
};"""

    # Query 423: Missing REVIEWED relation
    fixes[423] = """match
  $p isa person;
  $m isa movie;
  (director: $p, film: $m) isa directed;
  (reviewer: $p, film: $m) isa reviewed;
  $p has name $namep;
  $m has title $titlem;
fetch {
  "person": $namep,
  "movie": $titlem
};"""

    # Query 428: Missing DIRECTED by Lana Wachowski
    fixes[428] = """match
  $p isa person;
  $m isa movie;
  $d isa person, has name "Lana Wachowski";
  (actor: $p, film: $m) isa acted_in;
  (director: $d, film: $m) isa directed;
  $p has name $namep;
  $m has title $titlem;
fetch {
  "actor": $namep,
  "movie": $titlem
};"""

    # Query 429: Missing PRODUCED relation
    fixes[429] = """match
  $p isa person;
  $m isa movie;
  (reviewer: $p, film: $m) isa reviewed;
  (producer: $p, film: $m) isa produced;
  $m has title $titlem;
fetch {
  "movie": $titlem
};"""

    # Query 435: Missing ACTED_IN relation
    fixes[435] = """match
  $p isa person;
  $m isa movie;
  (producer: $p, film: $m) isa produced;
  (actor: $p, film: $m) isa acted_in;
  $m has title $titlem;
  $p has name $namep;
fetch {
  "movie": $titlem,
  "producer_actor": $namep
};"""

    # Query 438: Missing WROTE relation
    fixes[438] = """match
  $p isa person;
  $m isa movie;
  (writer: $p, film: $m) isa wrote;
  $p has name $namep;
  $p has born $bornp;
sort $bornp desc;
limit 5;
fetch {
  "name": $namep,
  "born": $bornp
};"""

    # Query 452: Missing DIRECTED relation
    fixes[452] = """match
  $p isa person;
  $m isa movie;
  (producer: $p, film: $m) isa produced;
  (director: $p, film: $m) isa directed;
  $m has title $titlem;
  $p has name $namep;
limit 3;
fetch {
  "title": $titlem,
  "name": $namep
};"""

    # Query 466: Missing REVIEWED relation
    fixes[466] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  (reviewer: $p, film: $m) isa reviewed;
  $p has name $namep;
  $m has title $titlem;
limit 3;
fetch {
  "person": $namep,
  "movie": $titlem
};"""

    # Query 469: Missing NOT DIRECTED negation
    fixes[469] = """match
  $p isa person;
  $m isa movie;
  (producer: $p, film: $m) isa produced;
  not { (director: $p, film: $m2) isa directed; };
  $p has name $namep;
limit 3;
fetch {
  "name": $namep
};"""

    # Query 470: Missing name STARTS WITH 'L' filter
    fixes[470] = """match
  $p isa person;
  $p has name $namep;
  $namep like "^L.*";
limit 3;
fetch {
  "name": $namep
};"""

    # Query 475: Missing DIRECTED relation
    fixes[475] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  (director: $p, film: $m) isa directed;
  $p has name $namep;
  $m has title $titlem;
limit 3;
fetch {
  "person": $namep,
  "movie": $titlem
};"""

    # Query 488: Missing DIRECTED and PRODUCED
    fixes[488] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  (director: $p, film: $m) isa directed;
  (producer: $p, film: $m) isa produced;
  $p has name $namep;
  $m has title $titlem;
limit 3;
fetch {
  "person": $namep,
  "movie": $titlem
};"""

    # Query 492: Missing tagline filter
    fixes[492] = """match
  $p isa person;
  $m isa movie;
  (director: $p, film: $m) isa directed;
  $m has tagline $taglinem;
  $taglinem like ".*limits.*";
  $p has name $namep;
limit 3;
fetch {
  "name": $namep
};"""

    # Query 496: Missing DIRECTED relation
    fixes[496] = """match
  $p isa person;
  $m isa movie;
  (director: $p, film: $m) isa directed;
  $p has name $namep;
  $p has born $bornp;
sort $bornp desc;
limit 3;
fetch {
  "name": $namep,
  "born": $bornp
};"""

    # Query 497: Missing REVIEWED relation
    fixes[497] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  (reviewer: $p, film: $m) isa reviewed;
  $m has title $titlem;
limit 3;
fetch {
  "title": $titlem
};"""

    # Query 498: Missing REVIEWED relation
    fixes[498] = """match
  $m isa movie;
  $p isa person;
  (producer: $p, film: $m) isa produced;
  (reviewer: $reviewer, film: $m) isa reviewed;
  $p has born $bornp;
  $bornp < 1960;
  $m has title $titlem;
  $m has votes $votesm;
sort $votesm desc;
limit 3;
fetch {
  "movie": $titlem,
  "votes": $votesm
};"""

    # Query 502: Missing REVIEWED relation
    fixes[502] = """match
  $p isa person;
  $m isa movie;
  $r (reviewer: $p, film: $m) isa reviewed, has summary $summaryr;
reduce $count = count($r) groupby $summaryr;
sort $count desc;
limit 3;
fetch {
  "summary": $summaryr,
  "count": $count
};"""

    # Query 509: Missing PRODUCED relation
    fixes[509] = """match
  $p isa person;
  $m isa movie;
  (reviewer: $p, film: $m) isa reviewed;
  (producer: $p, film: $m) isa produced;
  $p has name $namep;
  $m has title $titlem;
limit 3;
fetch {
  "name": $namep,
  "title": $titlem
};"""

    # Query 514: Missing REVIEWED relation
    fixes[514] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  (reviewer: $reviewer, film: $m) isa reviewed;
  $p has name $namep;
reduce $reviewedMovies = count($m) groupby $namep;
sort $reviewedMovies desc;
limit 3;
fetch {
  "name": $namep,
  "reviewedMovies": $reviewedMovies
};"""

    # Query 515: Missing DIRECTED relation
    fixes[515] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  (director: $p, film: $m) isa directed;
  $m has title $titlem;
limit 3;
fetch {
  "title": $titlem
};"""

    # Query 519: Missing REVIEWED relation
    fixes[519] = """match
  $p isa person;
  $m isa movie;
  (director: $p, film: $m) isa directed;
  (reviewer: $p, film: $m) isa reviewed;
  $m has title $titlem;
  $p has name $namep;
limit 3;
fetch {
  "title": $titlem,
  "name": $namep
};"""

    # Query 531: Missing tagline filter
    fixes[531] = """match
  $m isa movie;
  $m has tagline $taglinem;
  $taglinem like ".*limit.*";
  $m has title $titlem;
  $m has votes $votesm;
sort $votesm desc;
limit 3;
fetch {
  "title": $titlem,
  "tagline": $taglinem,
  "votes": $votesm
};"""

    # Query 546: Missing ACTED_IN and PRODUCED
    fixes[546] = """match
  $p isa person;
  $m isa movie;
  (director: $p, film: $m) isa directed;
  (producer: $p, film: $m) isa produced;
  (actor: $p, film: $m) isa acted_in;
  $p has name $namep;
  $m has title $titlem;
limit 3;
fetch {
  "person": $namep,
  "movie": $titlem
};"""

    # Query 552: Missing DIRECTED relation
    fixes[552] = """match
  $p isa person;
  $m isa movie;
  (writer: $p, film: $m) isa wrote;
  (director: $p, film: $m) isa directed;
  $m has title $titlem;
  $p has name $namep;
fetch {
  "movie": $titlem,
  "person": $namep
};"""

    # Query 557: Missing tagline filter
    fixes[557] = """match
  $m isa movie;
  $p isa person;
  (producer: $p, film: $m) isa produced;
  $m has tagline $taglinem;
  $taglinem like ".*limits.*";
  $p has name $namep;
fetch {
  "producer": $namep
};"""

    # Query 558: Missing REVIEWED relation
    fixes[558] = """match
  $m isa movie;
  $p isa person;
  (reviewer: $p, film: $m) isa reviewed;
  $m has released $releasedm;
  $releasedm >= 1990;
  $releasedm <= 2000;
  $m has title $titlem;
  $m has votes $votesm;
  $m has tagline $taglinem;
fetch {
  "title": $titlem,
  "votes": $votesm,
  "tagline": $taglinem,
  "released": $releasedm
};"""

    # Query 581: Missing DIRECTED relation
    fixes[581] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  (director: $p, film: $m) isa directed;
  $p has name $namep;
  $m has title $titlem;
fetch {
  "person": $namep,
  "movie": $titlem
};"""

    # Query 587: Missing NOT REVIEWED negation
    fixes[587] = """match
  $m isa movie;
  not { (reviewer: $p, film: $m) isa reviewed; };
  $m has title $titlem;
limit 5;
fetch {
  "title": $titlem
};"""

    # Query 594: Missing DIRECTED relation
    fixes[594] = """match
  $p isa person;
  $m isa movie;
  (director: $p, film: $m) isa directed;
  $p has name $namep;
  $p has born $bornp;
sort $bornp desc;
limit 3;
fetch {
  "director": $namep,
  "birthYear": $bornp
};"""

    # Query 615: Missing REVIEWED relation
    fixes[615] = """match
  $p isa person;
  $m isa movie;
  (reviewer: $p, film: $m) isa reviewed;
  $p has name $namep;
  $p has born $bornp;
sort $bornp asc;
limit 3;
fetch {
  "name": $namep,
  "born": $bornp
};"""

    # Query 617: Missing DIRECTED relation (same title condition)
    fixes[617] = """match
  $p isa person;
  $m isa movie;
  (producer: $p, film: $m) isa produced;
  (director: $p, film: $m) isa directed;
  $m has title $titlem;
fetch {
  "movie": $titlem
};"""

    # Query 620: Missing PRODUCED relation
    fixes[620] = """match
  $p isa person;
  $m isa movie;
  (director: $p, film: $m) isa directed;
  (producer: $p, film: $m) isa produced;
  $m has title $titlem;
fetch {
  "movie": $titlem
};"""

    # Query 627: Missing DIRECTED relation
    fixes[627] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  (director: $p, film: $m) isa directed;
  $p has name $namep;
  $m has title $titlem;
fetch {
  "person": $namep,
  "movie": $titlem
};"""

    # Query 635: Missing DIRECTED relation
    fixes[635] = """match
  $p isa person;
  $m isa movie;
  (producer: $p, film: $m) isa produced;
  (director: $p, film: $m) isa directed;
  $m has title $titlem;
  $p has name $namep;
fetch {
  "movie": $titlem,
  "person": $namep
};"""

    # Query 642: Missing ACTED_IN relation
    fixes[642] = """match
  $p isa person;
  $m isa movie;
  (writer: $p, film: $m) isa wrote;
  (actor: $p, film: $m) isa acted_in;
  $p has name $namep;
  $m has title $titlem;
fetch {
  "person": $namep,
  "movie": $titlem
};"""

    # Query 647: Missing tagline STARTS WITH filter
    fixes[647] = """match
  $m isa movie;
  $m has tagline $taglinem;
  $taglinem like "^Everything.*";
  $m has title $titlem;
fetch {
  "title": $titlem,
  "tagline": $taglinem
};"""

    # Query 654: Missing tagline filter
    fixes[654] = """match
  $m isa movie;
  $m has tagline $taglinem;
  $taglinem like ".*limits.*";
  $m has title $titlem;
fetch {
  "title": $titlem
};"""

    # Query 655: Missing PRODUCED relation
    fixes[655] = """match
  $p isa person;
  $m isa movie;
  (writer: $p, film: $m) isa wrote;
  (producer: $p, film: $m) isa produced;
  $p has name $namep;
  $m has title $titlem;
fetch {
  "person": $namep,
  "movie": $titlem
};"""

    # Query 659: Missing PRODUCED relation
    fixes[659] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  (producer: $p, film: $m) isa produced;
  $m has title $titlem;
  $p has name $namep;
fetch {
  "movie": $titlem,
  "person": $namep
};"""

    # Query 665: Missing REVIEWED relation
    fixes[665] = """match
  $p isa person;
  $follower isa person;
  $m isa movie;
  (follower: $p, followed: $follower) isa follows;
  (reviewer: $follower, film: $m) isa reviewed;
  $p has born 1990;
  $m has title $titlem;
fetch {
  "title": $titlem
};"""

    # Query 668: Missing DIRECTED relation
    fixes[668] = """match
  $p isa person;
  $m isa movie;
  (director: $p, film: $m) isa directed;
  $p has born $bornp;
  $bornp >= 1960;
  $bornp < 1970;
  $p has name $namep;
fetch {
  "director": $namep,
  "birthYear": $bornp
};"""

    # Query 673: Missing PRODUCED relation
    fixes[673] = """match
  $p isa person;
  $m isa movie;
  (reviewer: $p, film: $m) isa reviewed;
  (producer: $p, film: $m) isa produced;
  $m has title $titlem;
fetch {
  "title": $titlem
};"""

    # Query 682: Missing REVIEWED relation
    fixes[682] = """match
  $p isa person;
  $m isa movie;
  (writer: $p, film: $m) isa wrote;
  (reviewer: $p, film: $m) isa reviewed;
  $p has name $namep;
  $m has title $titlem;
fetch {
  "person": $namep,
  "movie": $titlem
};"""

    # Query 687: Missing DIRECTED relation
    fixes[687] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  (director: $p, film: $m) isa directed;
  $p has name $namep;
reduce $num_actor_director_movies = count($m) groupby $namep;
sort $num_actor_director_movies desc;
limit 1;
fetch {
  "person": $namep,
  "num_actor_director_movies": $num_actor_director_movies
};"""

    # Query 689: Missing ACTED_IN relation
    fixes[689] = """match
  $m isa movie;
  $p isa person;
  (director: $p, film: $m) isa directed;
  (actor: $p, film: $m) isa acted_in;
  $m has title $titlem;
  $p has name $namep;
fetch {
  "movie": $titlem,
  "person": $namep
};"""

    # Query 691: Missing PRODUCED relation
    fixes[691] = """match
  $p isa person;
  $m isa movie;
  (actor: $p, film: $m) isa acted_in;
  (producer: $p, film: $m) isa produced;
  $p has name $namep;
limit 3;
fetch {
  "name": $namep
};"""

    # Query 698: Missing REVIEWED relation
    fixes[698] = """match
  $p isa person;
  $m isa movie;
  (director: $p, film: $m) isa directed;
  (reviewer: $p, film: $m) isa reviewed;
  $p has name $namep;
  $m has title $titlem;
fetch {
  "person": $namep,
  "movie": $titlem
};"""

    # Query 703: Missing tagline filter
    fixes[703] = """match
  $p isa person;
  $m isa movie;
  (director: $p, film: $m) isa directed;
  $m has tagline $taglinem;
  $taglinem like ".*real.*";
  $p has name $namep;
limit 3;
fetch {
  "name": $namep
};"""

    # Query 706: Missing DIRECTED and PRODUCED
    fixes[706] = """match
  $m isa movie;
  $p isa person;
  (director: $p, film: $m) isa directed;
  (producer: $p, film: $m) isa produced;
  $m has released $releasedm;
  $releasedm >= 2000;
  $releasedm < 2010;
  $m has title $titlem;
  $m has votes $votesm;
sort $votesm desc;
limit 5;
fetch {
  "title": $titlem,
  "released": $releasedm
};"""

    # Query 711: Missing DIRECTED relation
    fixes[711] = """match
  $p isa person;
  $m isa movie;
  (producer: $p, film: $m) isa produced;
  (director: $p, film: $m) isa directed;
  $m has title $titlem;
  $p has name $namep;
  $m has votes $votesm;
sort $votesm desc;
limit 5;
fetch {
  "movie": $titlem,
  "person": $namep
};"""

    # Query 715: Missing PRODUCED relation and name filter
    fixes[715] = """match
  $p isa person;
  $m isa movie;
  (producer: $p, film: $m) isa produced;
  $p has name $namep;
  $namep like "^J.*";
limit 3;
fetch {
  "name": $namep
};"""

    # Query 717: Missing ENDS WITH filter
    fixes[717] = """match
  $p isa person;
  $m isa movie;
  (producer: $p, film: $m) isa produced;
  $p has name $namep;
  $namep like ".*son$";
  $m has released $releasedm;
  $releasedm > 2000;
  $m has title $titlem;
limit 3;
fetch {
  "title": $titlem
};"""

    # Query 719: Missing REVIEWED relation
    fixes[719] = """match
  $p isa person;
  $m isa movie;
  (director: $p, film: $m) isa directed;
  (reviewer: $p, film: $m) isa reviewed;
  $p has name $namep;
limit 3;
fetch {
  "name": $namep
};"""

    # Query 721: Missing REVIEWED relation with summary
    fixes[721] = """match
  $m isa movie;
  $p isa person;
  $r (reviewer: $p, film: $m) isa reviewed, has summary $summaryr;
  $summaryr == "Silly, but fun";
  $m has released $releasedm;
  $releasedm >= 1990;
  $releasedm < 2000;
  $m has title $titlem;
limit 3;
fetch {
  "title": $titlem
};"""

    # Query 722: Missing WROTE relation
    fixes[722] = """match
  $p isa person;
  $m1 isa movie;
  $m2 isa movie;
  (writer: $p, film: $m1) isa wrote;
  (actor: $p, film: $m2) isa acted_in;
  $p has name $namep;
limit 3;
fetch {
  "name": $namep
};"""

    # Query 725: Missing REVIEWED relation
    fixes[725] = """match
  $p isa person;
  $m isa movie;
  (writer: $p, film: $m) isa wrote;
  (reviewer: $p, film: $m) isa reviewed;
  $m has title $titlem;
  $p has name $namep;
  $m has votes $votesm;
sort $votesm desc;
limit 5;
fetch {
  "movie": $titlem,
  "person": $namep
};"""

    return fixes


def main():
    """Main function to fix all failed review queries."""

    base_path = Path("/opt/text2typeql/output/movies")
    failed_review_path = base_path / "failed_review.csv"
    queries_path = base_path / "queries.csv"
    failed_path = base_path / "failed.csv"

    # Load specific fixes
    fixes = apply_specific_fixes()

    # Connect to TypeDB
    print("Connecting to TypeDB...")
    driver = connect_typedb()
    database = "text2typeql_movies"

    # Read failed_review.csv
    print(f"Reading {failed_review_path}...")
    failed_reviews = []
    with open(failed_review_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            failed_reviews.append(row)

    print(f"Found {len(failed_reviews)} queries to fix")

    # Read existing queries.csv
    existing_queries = []
    with open(queries_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            existing_queries.append(row)

    print(f"Existing queries in queries.csv: {len(existing_queries)}")

    # Read existing failed.csv
    existing_failed = []
    if failed_path.exists():
        with open(failed_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_failed.append(row)

    print(f"Existing failed queries in failed.csv: {len(existing_failed)}")

    # Process each failed review query
    fixed_queries = []
    unfixable_queries = []

    for row in failed_reviews:
        original_index = int(row['original_index'])
        question = row['question']
        cypher = row['cypher']
        typeql = row['typeql']
        review_reason = row['review_reason']

        print(f"\nProcessing query {original_index}...")

        # Check if we have a specific fix
        if original_index in fixes:
            fixed_typeql = fixes[original_index]
            if fixed_typeql is None:
                print(f"  Query {original_index} marked as unfixable: {review_reason[:50]}...")
                unfixable_queries.append({
                    'original_index': original_index,
                    'question': question,
                    'cypher': cypher,
                    'error': f"Unfixable: {review_reason}"
                })
                continue
        else:
            print(f"  No specific fix found for query {original_index}, skipping...")
            unfixable_queries.append({
                'original_index': original_index,
                'question': question,
                'cypher': cypher,
                'error': f"No fix available: {review_reason}"
            })
            continue

        # Validate the fixed query
        is_valid, error = validate_query(driver, database, fixed_typeql)

        if is_valid:
            print(f"  Query {original_index} fixed and validated successfully!")
            fixed_queries.append({
                'original_index': original_index,
                'question': question,
                'cypher': cypher,
                'typeql': fixed_typeql
            })
        else:
            print(f"  Query {original_index} fix failed validation: {error[:100]}...")
            unfixable_queries.append({
                'original_index': original_index,
                'question': question,
                'cypher': cypher,
                'error': f"Validation failed: {error}"
            })

    # Write fixed queries to queries.csv
    print(f"\n\nWriting {len(fixed_queries)} fixed queries to queries.csv...")
    all_queries = existing_queries + fixed_queries
    with open(queries_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
        writer.writeheader()
        for query in all_queries:
            writer.writerow(query)

    # Write unfixable queries to failed.csv
    print(f"Writing {len(unfixable_queries)} unfixable queries to failed.csv...")
    all_failed = existing_failed + unfixable_queries
    with open(failed_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'error'])
        writer.writeheader()
        for query in all_failed:
            writer.writerow(query)

    # Summary
    print(f"\n\nSummary:")
    print(f"  Fixed queries: {len(fixed_queries)}")
    print(f"  Unfixable queries: {len(unfixable_queries)}")
    print(f"  Total queries in queries.csv: {len(all_queries)}")
    print(f"  Total queries in failed.csv: {len(all_failed)}")

    driver.close()


if __name__ == "__main__":
    main()
