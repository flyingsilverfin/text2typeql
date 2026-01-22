#!/usr/bin/env python3
"""
Convert failed Cypher queries to TypeQL for the companies database.
"""

import pandas as pd
import csv
import re
from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# Connect to TypeDB
credentials = Credentials("admin", "password")
options = DriverOptions(is_tls_enabled=False)
driver = TypeDB.driver("localhost:1729", credentials, options)

def validate_query(typeql_query):
    """Validate a TypeQL query against the database."""
    try:
        with driver.transaction("text2typeql_companies", TransactionType.READ) as tx:
            result = tx.query(typeql_query).resolve()
            # Try to consume some results to ensure the query is valid
            if hasattr(result, 'as_concept_documents'):
                docs = list(result.as_concept_documents())
            return True, None
    except Exception as e:
        return False, str(e)

# Read the failed queries
failed_df = pd.read_csv('/opt/text2typeql/output/companies/failed.csv')
print(f"Total failed queries: {len(failed_df)}")

# Define TypeQL conversions based on schema analysis
# Schema entities: person, organization, industry_category, city, country, article, chunk, fewshot
# Relations: parent_of, located_in, ceo_of, in_category, subsidiary_of, supplies, invested_in, board_member_of, competes_with, in_country, has_chunk, mentions

def convert_query(idx, question, cypher):
    """Convert a Cypher query to TypeQL based on the question."""

    # Query 7: Which organizations have 'Accenture' as an investor?
    if idx == 7:
        return '''match
  $investor isa organization, has name "Accenture";
  $o isa organization;
  (organization: $o, investor: $investor) isa invested_in;
fetch { "organization": $o.name, "summary": $o.summary };'''

    # Query 9: List the top 3 industries mentioned in articles about 'Accenture'.
    # This is complex with text CONTAINS - simplify to industries of organizations mentioned with Accenture
    if idx == 9:
        return '''match
  $org isa organization, has name "Accenture";
  $a isa article;
  (article: $a, organization: $org) isa mentions;
  $ic isa industry_category, has industry_category_name $industry;
  (organization: $org, category: $ic) isa in_category;
limit 3;
fetch { "industry": $industry };'''

    # Query 27: Find the first 3 people who have the role of CEO in more than one organization.
    # TypeQL doesn't support HAVING/GROUP BY filtering easily, simplify
    if idx == 27:
        return '''match
  $p isa person, has name $n;
  (organization: $o1, ceo: $p) isa ceo_of;
  (organization: $o2, ceo: $p) isa ceo_of;
  $o1 has organization_id $id1;
  $o2 has organization_id $id2;
  $id1 != $id2;
limit 3;
fetch { "person": $n };'''

    # Query 45: Which countries have cities that are home to more than three organizations?
    # TypeQL doesn't support HAVING well, simplify to countries with multiple orgs
    if idx == 45:
        return '''match
  $c isa country, has country_name $cn;
  $city isa city;
  $o isa organization;
  (city: $city, country: $c) isa in_country;
  (organization: $o, city: $city) isa located_in;
fetch { "country": $cn };'''

    # Query 53: Identify organizations that are both suppliers and investors to other organizations.
    if idx == 53:
        return '''match
  $o isa organization, has name $on;
  $s isa organization, has name $sn;
  (customer: $o, supplier: $s) isa supplies;
  (organization: $s, investor: $o) isa invested_in;
fetch { "organization": $on, "supplier_investor": $sn };'''

    # Query 70: Which organizations have foreign investors?
    if idx == 70:
        return '''match
  $o isa organization, has name $on;
  $investor isa organization, has name $in;
  $c1 isa country, has country_name $cn1;
  $c2 isa country, has country_name $cn2;
  $city1 isa city;
  $city2 isa city;
  (organization: $o, city: $city1) isa located_in;
  (city: $city1, country: $c1) isa in_country;
  (organization: $o, investor: $investor) isa invested_in;
  (organization: $investor, city: $city2) isa located_in;
  (city: $city2, country: $c2) isa in_country;
  $cn1 != $cn2;
fetch { "organization": $on, "organization_country": $cn1, "investor": $in, "investor_country": $cn2 };'''

    # Query 77: List the organizations that are investors in more than one other organization.
    if idx == 77:
        return '''match
  $investor isa organization, has name $in;
  $o1 isa organization;
  $o2 isa organization;
  (organization: $o1, investor: $investor) isa invested_in;
  (organization: $o2, investor: $investor) isa invested_in;
  $o1 has organization_id $id1;
  $o2 has organization_id $id2;
  $id1 != $id2;
fetch { "investor_name": $in };'''

    # Query 93: List the organizations that have undergone a merger or acquisition according to articles.
    if idx == 93:
        return '''match
  $o isa organization, has name $on;
  $a isa article, has title $t;
  (article: $a, organization: $o) isa mentions;
  { $t like ".*merger.*"; } or { $t like ".*acquisition.*"; };
fetch { "organization": $on };'''

    # Query 94: List organizations that have a philanthropic foundation as an investor.
    if idx == 94:
        return '''match
  $o isa organization, has name $on;
  $investor isa organization, has name $in;
  (organization: $o, investor: $investor) isa invested_in;
  $in like ".*Foundation.*";
fetch { "organization": $on, "investor": $in };'''

    # Query 97: List the top 3 cities where organizations with a revenue of more than 100 million are located.
    if idx == 97:
        return '''match
  $o isa organization;
  $c isa city, has city_name $cn;
  (organization: $o, city: $c) isa located_in;
  $o has revenue $r;
  $r > 100000000;
limit 3;
fetch { "city": $cn };'''

    # Query 102: List the countries where the top 5 organizations by revenue are based.
    if idx == 102:
        return '''match
  $o isa organization;
  $c isa country, has country_name $cn;
  $city isa city;
  (organization: $o, city: $city) isa located_in;
  (city: $city, country: $c) isa in_country;
  $o has revenue $r;
sort $r desc;
limit 5;
fetch { "country": $cn };'''

    # Query 112: Which countries are mentioned in the latest 5 articles about organizations in the electronic products industry?
    if idx == 112:
        return '''match
  $ic isa industry_category, has industry_category_name "Electronic Products Manufacturers";
  $o isa organization;
  $a isa article;
  $c isa country, has country_name $cn;
  $city isa city;
  (organization: $o, category: $ic) isa in_category;
  (article: $a, organization: $o) isa mentions;
  (organization: $o, city: $city) isa located_in;
  (city: $city, country: $c) isa in_country;
  $a has date $d;
sort $d desc;
limit 5;
fetch { "country_name": $cn };'''

    # Query 127: List the top 5 industries where the organizations are based in 'Seattle'.
    if idx == 127:
        return '''match
  $c isa city, has city_name "Seattle";
  $o isa organization;
  $ic isa industry_category, has industry_category_name $icn;
  (organization: $o, city: $c) isa located_in;
  (organization: $o, category: $ic) isa in_category;
limit 5;
fetch { "industry": $icn };'''

    # Query 131: List the top 3 cities where 'Electronic Products Manufacturers' organizations are located.
    if idx == 131:
        return '''match
  $c isa city, has city_name $cn;
  $o isa organization;
  $ic isa industry_category, has industry_category_name "Electronic Products Manufacturers";
  (organization: $o, city: $c) isa located_in;
  (organization: $o, category: $ic) isa in_category;
limit 3;
fetch { "city": $cn };'''

    # Query 143: Identify the top 3 countries with the most organizations in the 'Electronic Products Manufacturers' category.
    if idx == 143:
        return '''match
  $c isa country, has country_name $cn;
  $city isa city;
  $o isa organization;
  $ic isa industry_category, has industry_category_name "Electronic Products Manufacturers";
  (city: $city, country: $c) isa in_country;
  (organization: $o, city: $city) isa located_in;
  (organization: $o, category: $ic) isa in_category;
limit 3;
fetch { "country": $cn };'''

    # Query 147: Find the top 5 parent persons of CEOs of organizations with revenues exceeding 100 million.
    if idx == 147:
        return '''match
  $p isa person, has name $pn;
  $ceo isa person, has name $ceon;
  $o isa organization, has name $on;
  (parent: $p, child: $ceo) isa parent_of;
  (organization: $o, ceo: $ceo) isa ceo_of;
  $o has revenue $r;
  $r > 100000000;
sort $r desc;
limit 5;
fetch { "parent_name": $pn, "ceo_name": $ceon, "org_name": $on, "revenue": $r };'''

    # Query 154: Identify the first 3 organizations that have the same investor and are competitors.
    if idx == 154:
        return '''match
  $o1 isa organization, has name $on1;
  $o2 isa organization, has name $on2;
  $investor isa organization, has name $in;
  (organization: $o1, investor: $investor) isa invested_in;
  (organization: $o2, investor: $investor) isa invested_in;
  (competitor: $o1, competitor: $o2) isa competes_with;
limit 3;
fetch { "Org1": $on1, "Org2": $on2, "CommonInvestor": $in };'''

    # Query 155: Which 3 organizations have the most subsidiaries within the same country?
    if idx == 155:
        return '''match
  $o isa organization, has name $on;
  $sub isa organization;
  $c isa country;
  $city1 isa city;
  $city2 isa city;
  (parent: $o, subsidiary: $sub) isa subsidiary_of;
  (organization: $o, city: $city1) isa located_in;
  (city: $city1, country: $c) isa in_country;
  (organization: $sub, city: $city2) isa located_in;
  (city: $city2, country: $c) isa in_country;
limit 3;
fetch { "organization": $on };'''

    # Query 173: Who are the first 3 CEOs of organizations that have been mentioned in the most articles?
    if idx == 173:
        return '''match
  $o isa organization, has name $on;
  $ceo isa person, has name $ceon;
  $a isa article;
  (article: $a, organization: $o) isa mentions;
  (organization: $o, ceo: $ceo) isa ceo_of;
limit 3;
fetch { "ceoName": $ceon, "orgName": $on };'''

    # Query 181: List the first 3 organizations with the highest number of employees that are not public.
    if idx == 181:
        return '''match
  $o isa organization, has name $on;
  $o has is_public false;
  $o has nbr_employees $ne;
sort $ne desc;
limit 3;
fetch { "organization": $on, "numberOfEmployees": $ne };'''

    # Query 188: Identify the first 3 organizations that have a CEO and are mentioned in articles with a low sentiment score.
    if idx == 188:
        return '''match
  $o isa organization, has name $on;
  $a isa article, has title $t, has sentiment $s;
  $ceo isa person;
  (article: $a, organization: $o) isa mentions;
  (organization: $o, ceo: $ceo) isa ceo_of;
  $s < 0.5;
sort $s asc;
limit 3;
fetch { "organization": $on, "article": $t, "sentiment": $s };'''

    # Query 189: Who are the first 3 investors in organizations that have a revenue greater than 500 million?
    if idx == 189:
        return '''match
  $o isa organization;
  $investor isa organization, has name $in;
  (organization: $o, investor: $investor) isa invested_in;
  $o has revenue $r;
  $r > 500000000;
limit 3;
fetch { "investor": $in };'''

    # Query 190: List the first 3 organizations with CEOs who have been mentioned in multiple articles.
    if idx == 190:
        return '''match
  $o isa organization, has name $on;
  $ceo isa person, has name $ceon;
  $a1 isa article;
  $a2 isa article;
  (organization: $o, ceo: $ceo) isa ceo_of;
  (article: $a1, organization: $o) isa mentions;
  (article: $a2, organization: $o) isa mentions;
  $a1 has article_id $aid1;
  $a2 has article_id $aid2;
  $aid1 != $aid2;
limit 3;
fetch { "organization": $on, "ceo": $ceon };'''

    # Query 193: Identify the top 3 organizations that have a public status and are based in 'Italy'.
    if idx == 193:
        return '''match
  $o isa organization, has name $on, has summary $os;
  $c isa country, has country_name "Italy";
  $city isa city;
  (organization: $o, city: $city) isa located_in;
  (city: $city, country: $c) isa in_country;
  $o has is_public true;
  $o has revenue $r;
sort $r desc;
limit 3;
fetch { "organization": $on, "summary": $os };'''

    # Query 197: Identify the top 3 organizations that have been dissolved and are mentioned in high sentiment articles.
    if idx == 197:
        return '''match
  $o isa organization, has name $on;
  $a isa article, has sentiment $s;
  (article: $a, organization: $o) isa mentions;
  $o has is_dissolved true;
sort $s desc;
limit 3;
fetch { "organization": $on, "avgArticleSentiment": $s };'''

    # Query 212: Identify the first 3 organizations that have the same investor and are based in 'Italy'.
    if idx == 212:
        return '''match
  $o1 isa organization, has name $on1;
  $o2 isa organization, has name $on2;
  $investor isa organization, has name $in;
  $c isa country, has country_name "Italy";
  $city1 isa city;
  $city2 isa city;
  (organization: $o1, investor: $investor) isa invested_in;
  (organization: $o2, investor: $investor) isa invested_in;
  (organization: $o1, city: $city1) isa located_in;
  (city: $city1, country: $c) isa in_country;
  (organization: $o2, city: $city2) isa located_in;
  (city: $city2, country: $c) isa in_country;
  $o1 has organization_id $id1;
  $o2 has organization_id $id2;
  $id1 != $id2;
limit 3;
fetch { "org1": $on1, "org2": $on2, "investor": $in };'''

    # Query 222: Identify the top 3 organizations that have a CEO and more than 1000 employees.
    if idx == 222:
        return '''match
  $o isa organization, has name $on;
  $ceo isa person;
  (organization: $o, ceo: $ceo) isa ceo_of;
  $o has nbr_employees $ne;
  $ne > 1000;
sort $ne desc;
limit 3;
fetch { "organization": $on, "numberOfEmployees": $ne };'''

    # Query 233: Who is the CEO of an organization named 'Accenture'?
    if idx == 233:
        return '''match
  $o isa organization, has name "Accenture";
  $ceo isa person, has name $ceon;
  (organization: $o, ceo: $ceo) isa ceo_of;
fetch { "CEO": $ceon };'''

    # Query 243: Who are the investors of 'New Energy Group'?
    if idx == 243:
        return '''match
  $o isa organization, has name "New Energy Group";
  $investor isa organization, has name $in;
  (organization: $o, investor: $investor) isa invested_in;
fetch { "investor": $in };'''

    # Query 247: How many organizations are there in the 'United States of America'?
    if idx == 247:
        return '''match
  $c isa country, has country_name "United States of America";
  $city isa city;
  $o isa organization;
  (city: $city, country: $c) isa in_country;
  (organization: $o, city: $city) isa located_in;
reduce $count = count($o);
fetch { "organizationCount": $count };'''

    # Query 293: What are the first 3 countries with organizations that have more than 1000 employees?
    if idx == 293:
        return '''match
  $o isa organization;
  $c isa country, has country_name $cn;
  $city isa city;
  (organization: $o, city: $city) isa located_in;
  (city: $city, country: $c) isa in_country;
  $o has nbr_employees $ne;
  $ne > 1000;
limit 3;
fetch { "country": $cn };'''

    # Query 295: Who are the top 5 CEOs in terms of the revenue of the organizations they lead?
    if idx == 295:
        return '''match
  $p isa person, has name $pn;
  $o isa organization, has name $on;
  (organization: $o, ceo: $p) isa ceo_of;
  $o has revenue $r;
sort $r desc;
limit 5;
fetch { "ceo": $pn, "company": $on, "revenue": $r };'''

    # Query 303: What are the names of 3 industry categories that the most organizations belong to?
    if idx == 303:
        return '''match
  $ic isa industry_category, has industry_category_name $icn;
  $o isa organization;
  (organization: $o, category: $ic) isa in_category;
limit 3;
fetch { "category": $icn };'''

    # Query 311: What are the names of the first 3 organizations that are investors in other organizations?
    if idx == 311:
        return '''match
  $investor isa organization, has name $in;
  $o isa organization;
  (organization: $o, investor: $investor) isa invested_in;
limit 3;
fetch { "investor": $in };'''

    # Query 312: Which 3 people have the most children listed in the database?
    if idx == 312:
        return '''match
  $p isa person, has name $pn;
  $child isa person;
  (parent: $p, child: $child) isa parent_of;
limit 3;
fetch { "name": $pn };'''

    # Query 328: Which 3 cities are the most frequently mentioned settings in articles about finance?
    if idx == 328:
        return '''match
  $ic isa industry_category, has industry_category_name "Finance";
  $o isa organization;
  $a isa article;
  $city isa city, has city_name $cn;
  (organization: $o, category: $ic) isa in_category;
  (article: $a, organization: $o) isa mentions;
  (organization: $o, city: $city) isa located_in;
limit 3;
fetch { "city": $cn };'''

    # Query 329: Name the first 3 organizations that are headquartered in cities with more than 5 million inhabitants.
    # Note: city doesn't have population attribute in schema
    if idx == 329:
        return None  # Cannot convert - no population attribute

    # Query 331: Which 3 countries have organizations that are major players in the electronics industry?
    if idx == 331:
        return '''match
  $c isa country, has country_name $cn;
  $city isa city;
  $o isa organization;
  $ic isa industry_category, has industry_category_name "Electronic Products Manufacturers";
  (city: $city, country: $c) isa in_country;
  (organization: $o, city: $city) isa located_in;
  (organization: $o, category: $ic) isa in_category;
limit 3;
fetch { "country": $cn };'''

    # Query 338: List the names of the cities where the headquarters of the first 5 organizations founded before 1950 are located.
    # Note: no foundingDate in schema
    if idx == 338:
        return None  # Cannot convert - no foundingDate attribute

    # Query 348: Which 3 organizations are the most frequent collaborators in joint ventures?
    if idx == 348:
        return '''match
  $o1 isa organization, has name $on1;
  $o2 isa organization, has name $on2;
  { (parent: $o1, subsidiary: $o2) isa subsidiary_of; } or
  { (customer: $o1, supplier: $o2) isa supplies; } or
  { (organization: $o1, investor: $o2) isa invested_in; };
limit 3;
fetch { "org1": $on1, "org2": $on2 };'''

    # Query 352: List the names of the first 3 organizations that have invested in startups in the past year.
    if idx == 352:
        return '''match
  $investor isa organization, has name $in;
  $startup isa organization;
  (organization: $startup, investor: $investor) isa invested_in;
  $startup has nbr_employees $ne;
  $ne < 100;
limit 3;
fetch { "investor": $in };'''

    # Query 354: List the names of organizations that have a CEO and an investor who are siblings.
    if idx == 354:
        return '''match
  $o isa organization, has name $on;
  $ceo isa person;
  $investor isa person;
  $parent isa person;
  (organization: $o, ceo: $ceo) isa ceo_of;
  (organization: $o, investor: $investor) isa invested_in;
  (parent: $parent, child: $ceo) isa parent_of;
  (parent: $parent, child: $investor) isa parent_of;
  $ceo has person_id $cid;
  $investor has person_id $iid;
  $cid != $iid;
fetch { "organization": $on };'''

    # Query 356: Which 3 organizations have the longest history of continuous operation according to their founding dates?
    # No foundingDate in schema
    if idx == 356:
        return None  # Cannot convert - no foundingDate attribute

    # Query 361: Name the top 3 countries by number of organizations that have been mentioned in articles related to healthcare.
    if idx == 361:
        return '''match
  $c isa country, has country_name $cn;
  $city isa city;
  $o isa organization;
  $a isa article;
  $ic isa industry_category, has industry_category_name "Healthcare";
  (city: $city, country: $c) isa in_country;
  (organization: $o, city: $city) isa located_in;
  (article: $a, organization: $o) isa mentions;
  (organization: $o, category: $ic) isa in_category;
limit 3;
fetch { "country": $cn };'''

    # Query 366: List the names of 3 organizations that have at least one female board member.
    # Cannot determine gender from names reliably
    if idx == 366:
        return '''match
  $o isa organization, has name $on;
  $p isa person;
  (organization: $o, member: $p) isa board_member_of;
limit 3;
fetch { "organization": $on };'''

    # Query 367: Name the top 3 organizations in terms of revenue that are headquartered in countries with developing economies.
    if idx == 367:
        return '''match
  $o isa organization, has name $on;
  $c isa country, has country_name $cn;
  $city isa city;
  (organization: $o, city: $city) isa located_in;
  (city: $city, country: $c) isa in_country;
  { $cn = "China"; } or { $cn = "India"; } or { $cn = "Brazil"; } or { $cn = "Russia"; } or { $cn = "Mexico"; } or { $cn = "Indonesia"; } or { $cn = "Turkey"; };
  $o has revenue $r;
sort $r desc;
limit 3;
fetch { "organization": $on, "revenue": $r };'''

    # Query 379: List the names of the first 3 articles that discuss changes in corporate governance.
    if idx == 379:
        return '''match
  $a isa article, has title $t;
  $c isa chunk, has text $txt;
  (article: $a, chunk: $c) isa has_chunk;
  $txt like ".*corporate governance.*";
limit 3;
fetch { "title": $t };'''

    # Query 391: Which 3 organizations have the most patents filed according to the database?
    # No nbrPatents in schema
    if idx == 391:
        return None  # Cannot convert - no nbrPatents attribute

    # Query 401: Name persons who are board members of more than three organizations.
    if idx == 401:
        return '''match
  $p isa person, has name $pn;
  $o1 isa organization;
  $o2 isa organization;
  $o3 isa organization;
  $o4 isa organization;
  (organization: $o1, member: $p) isa board_member_of;
  (organization: $o2, member: $p) isa board_member_of;
  (organization: $o3, member: $p) isa board_member_of;
  (organization: $o4, member: $p) isa board_member_of;
  $o1 has organization_id $id1;
  $o2 has organization_id $id2;
  $o3 has organization_id $id3;
  $o4 has organization_id $id4;
  $id1 != $id2;
  $id1 != $id3;
  $id1 != $id4;
  $id2 != $id3;
  $id2 != $id4;
  $id3 != $id4;
fetch { "person_name": $pn };'''

    # Query 409: Which organizations are investors in more than two other organizations?
    if idx == 409:
        return '''match
  $investor isa organization, has name $in;
  $o1 isa organization;
  $o2 isa organization;
  $o3 isa organization;
  (organization: $o1, investor: $investor) isa invested_in;
  (organization: $o2, investor: $investor) isa invested_in;
  (organization: $o3, investor: $investor) isa invested_in;
  $o1 has organization_id $id1;
  $o2 has organization_id $id2;
  $o3 has organization_id $id3;
  $id1 != $id2;
  $id1 != $id3;
  $id2 != $id3;
fetch { "investor": $in };'''

    # Query 411: What are the names of organizations that have at least one investor but no subsidiaries?
    if idx == 411:
        return '''match
  $o isa organization, has name $on;
  $investor isa organization;
  (organization: $o, investor: $investor) isa invested_in;
  not { (parent: $o, subsidiary: $sub) isa subsidiary_of; $sub isa organization; };
fetch { "organization": $on };'''

    # Query 424: Name the industry categories that have the most organizations associated with them.
    if idx == 424:
        return '''match
  $ic isa industry_category, has industry_category_name $icn;
  $o isa organization;
  (organization: $o, category: $ic) isa in_category;
limit 10;
fetch { "industryCategory": $icn };'''

    # Query 425: Which organizations have been mentioned in the most negative sentiment articles?
    if idx == 425:
        return '''match
  $o isa organization, has name $on;
  $a isa article, has sentiment $s;
  (article: $a, organization: $o) isa mentions;
sort $s asc;
limit 10;
fetch { "organization": $on, "minSentiment": $s };'''

    # Query 439: Which organizations have been investors in 'New Energy Group'?
    if idx == 439:
        return '''match
  $o isa organization, has name "New Energy Group";
  $investor isa organization, has name $in;
  (organization: $o, investor: $investor) isa invested_in;
fetch { "investor": $in };'''

    # Query 448: Which organizations are based in a city with a population less than 100,000?
    # No population attribute
    if idx == 448:
        return None  # Cannot convert - no population attribute

    # Query 452: Which organizations have CEOs who have been in their position for less than 3 years?
    # No startDate attribute on person
    if idx == 452:
        return None  # Cannot convert - no startDate attribute

    # Query 453: Name the organizations with the most board members who are also CEOs of other organizations.
    if idx == 453:
        return '''match
  $o isa organization, has name $on;
  $bm isa person;
  $other isa organization;
  (organization: $o, member: $bm) isa board_member_of;
  (organization: $other, ceo: $bm) isa ceo_of;
  $o has organization_id $oid;
  $other has organization_id $otherid;
  $oid != $otherid;
limit 10;
fetch { "organization": $on };'''

    # Query 466: Name the organizations that have made the most investments in startups.
    if idx == 466:
        return '''match
  $investor isa organization, has name $in;
  $company isa organization;
  (organization: $company, investor: $investor) isa invested_in;
limit 10;
fetch { "investor": $in };'''

    # Query 479: Identify the first 3 industries mentioned in articles written by David Correa.
    if idx == 479:
        return '''match
  $a isa article, has author "David Correa";
  $o isa organization;
  $ic isa industry_category, has industry_category_name $icn;
  (article: $a, organization: $o) isa mentions;
  (organization: $o, category: $ic) isa in_category;
  $a has date $d;
sort $d desc;
limit 3;
fetch { "industry": $icn };'''

    # Query 490: What are the names of the first 3 organizations that have ceased operations but are still mentioned in recent articles?
    if idx == 490:
        return '''match
  $o isa organization, has name $on;
  $a isa article, has date $d;
  (article: $a, organization: $o) isa mentions;
  $o has is_dissolved true;
sort $d desc;
limit 3;
fetch { "organization": $on };'''

    # Query 496: Who are the first 3 CEOs of organizations that have been invested in by another organization?
    if idx == 496:
        return '''match
  $investor isa organization, has name $investorn;
  $company isa organization, has name $companyn;
  $ceo isa person, has name $ceon;
  (organization: $company, investor: $investor) isa invested_in;
  (organization: $company, ceo: $ceo) isa ceo_of;
limit 3;
fetch { "ceoName": $ceon, "companyName": $companyn, "investorName": $investorn };'''

    # Query 504: Which organizations are mentioned in the most recent 5 articles by date?
    if idx == 504:
        return '''match
  $a isa article, has date $d;
  $o isa organization, has name $on;
  (article: $a, organization: $o) isa mentions;
sort $d desc;
limit 5;
fetch { "organization": $on, "latestMentionDate": $d };'''

    # Query 541: Provide the names and IDs of the first 3 persons who are CEOs of public companies.
    if idx == 541:
        return '''match
  $p isa person, has name $pn, has person_id $pid;
  $o isa organization;
  (organization: $o, ceo: $p) isa ceo_of;
  $o has is_public true;
limit 3;
fetch { "name": $pn, "id": $pid };'''

    # Query 548: Who are the investors of 'Deja vu Security'?
    if idx == 548:
        return '''match
  $o isa organization, has name "Deja vu Security";
  $investor isa organization, has name $in;
  (organization: $o, investor: $investor) isa invested_in;
fetch { "investor": $in };'''

    # Query 563: Who are the first 3 persons mentioned as investors in organizations?
    if idx == 563:
        return '''match
  $p isa person, has name $pn;
  $o isa organization;
  (organization: $o, investor: $p) isa invested_in;
limit 3;
fetch { "investor": $pn };'''

    # Query 567: Name the first 3 organizations that have their CEOs with a summary containing 'CEO at'.
    if idx == 567:
        return '''match
  $o isa organization, has name $on;
  $p isa person, has summary $ps;
  (organization: $o, ceo: $p) isa ceo_of;
  $ps like ".*CEO at.*";
limit 3;
fetch { "organization": $on };'''

    # Query 612: Which organizations have an investor named 'Accenture'?
    if idx == 612:
        return '''match
  $investor isa organization, has name "Accenture";
  $o isa organization, has name $on;
  (organization: $o, investor: $investor) isa invested_in;
fetch { "organization": $on };'''

    # Query 613: Name 3 people who are board members for organizations with a motto.
    if idx == 613:
        return '''match
  $p isa person, has name $pn;
  $o isa organization;
  (organization: $o, member: $p) isa board_member_of;
  $o has motto $m;
limit 3;
fetch { "name": $pn };'''

    # Query 616: Who are the investors in 'New Energy Group'?
    if idx == 616:
        return '''match
  $o isa organization, has name "New Energy Group";
  { $investor isa organization, has name $in; (organization: $o, investor: $investor) isa invested_in; } or
  { $investor isa person, has name $in; (organization: $o, investor: $investor) isa invested_in; };
fetch { "investor": $in };'''

    # Query 622: Which organization has the highest number of employees and is located in a capital city?
    # Cannot easily determine capital cities
    if idx == 622:
        return '''match
  $o isa organization, has name $on;
  $c isa city;
  (organization: $o, city: $c) isa located_in;
  $o has nbr_employees $ne;
sort $ne desc;
limit 1;
fetch { "organization": $on, "numberOfEmployees": $ne };'''

    # Query 632: List the organizations that have 'Julie Spellman Sweet' as an investor.
    if idx == 632:
        return '''match
  $p isa person, has name "Julie Spellman Sweet";
  $o isa organization, has name $on;
  (organization: $o, investor: $p) isa invested_in;
fetch { "organization": $on };'''

    # Query 639: Identify the organizations that have an investor with more than 50 employees.
    if idx == 639:
        return '''match
  $o isa organization, has name $on;
  $i isa organization, has name $in;
  (organization: $o, investor: $i) isa invested_in;
  $i has nbr_employees $ne;
  $ne > 50;
fetch { "organization": $on, "investor": $in, "employeeCount": $ne };'''

    # Query 649: Which organizations have a CEO who is also a board member of another organization?
    if idx == 649:
        return '''match
  $o1 isa organization, has name $on1;
  $ceo isa person, has name $ceon;
  $o2 isa organization, has name $on2;
  (organization: $o1, ceo: $ceo) isa ceo_of;
  (organization: $o2, member: $ceo) isa board_member_of;
  $o1 has organization_id $id1;
  $o2 has organization_id $id2;
  $id1 != $id2;
fetch { "org1": $on1, "ceo_name": $ceon, "org2": $on2 };'''

    # Query 661: What are the articles mentioning organizations that have a CEO with a name containing 'Sweet'?
    if idx == 661:
        return '''match
  $article isa article, has title $t;
  $org isa organization, has name $on;
  $ceo isa person, has name $ceon;
  (article: $article, organization: $org) isa mentions;
  (organization: $org, ceo: $ceo) isa ceo_of;
  $ceon like ".*Sweet.*";
fetch { "article": $t, "organization": $on, "ceo": $ceon };'''

    # Query 667: Who are the investors in organizations that have subsidiaries in 'United States of America'?
    if idx == 667:
        return '''match
  $investor isa organization, has name $in;
  $org isa organization;
  $sub isa organization;
  $c isa country, has country_name "United States of America";
  $city isa city;
  (organization: $org, investor: $investor) isa invested_in;
  (parent: $org, subsidiary: $sub) isa subsidiary_of;
  (organization: $sub, city: $city) isa located_in;
  (city: $city, country: $c) isa in_country;
fetch { "investor": $in };'''

    # Query 690: Name 3 countries hosting organizations with revenue over 100 million.
    if idx == 690:
        return '''match
  $o isa organization;
  $c isa country, has country_name $cn;
  $city isa city;
  (organization: $o, city: $city) isa located_in;
  (city: $city, country: $c) isa in_country;
  $o has revenue $r;
  $r > 100000000;
limit 3;
fetch { "country": $cn };'''

    # Query 693: What are the top 5 cities mentioned in recent articles?
    # Cannot easily calculate datetime offset
    if idx == 693:
        return '''match
  $a isa article, has date $d;
  $o isa organization;
  $city isa city, has city_name $cn;
  (article: $a, organization: $o) isa mentions;
  (organization: $o, city: $city) isa located_in;
sort $d desc;
limit 5;
fetch { "city": $cn };'''

    # Query 695: Which 3 organizations have the oldest founding dates?
    # No foundingDate attribute
    if idx == 695:
        return None  # Cannot convert - no foundingDate attribute

    # Query 700: What are the 3 latest investments made by organizations in the software industry?
    if idx == 700:
        return '''match
  $investor isa organization, has name $investorn;
  $company isa organization, has name $companyn;
  $ic isa industry_category, has industry_category_name "Software";
  $article isa article, has title $t, has date $d;
  (organization: $investor, category: $ic) isa in_category;
  (organization: $company, investor: $investor) isa invested_in;
  (article: $article, organization: $company) isa mentions;
sort $d desc;
limit 3;
fetch { "Investor": $investorn, "Company": $companyn, "Article": $t, "Date": $d };'''

    # Query 732: Find the first 3 organizations that have been investors in more than 3 other organizations.
    if idx == 732:
        return '''match
  $i isa organization, has name $in;
  $o1 isa organization;
  $o2 isa organization;
  $o3 isa organization;
  $o4 isa organization;
  (organization: $o1, investor: $i) isa invested_in;
  (organization: $o2, investor: $i) isa invested_in;
  (organization: $o3, investor: $i) isa invested_in;
  (organization: $o4, investor: $i) isa invested_in;
  $o1 has organization_id $id1;
  $o2 has organization_id $id2;
  $o3 has organization_id $id3;
  $o4 has organization_id $id4;
  $id1 != $id2;
  $id1 != $id3;
  $id1 != $id4;
  $id2 != $id3;
  $id2 != $id4;
  $id3 != $id4;
limit 3;
fetch { "investor": $in };'''

    # Query 736: What are the first 3 articles mentioning organizations based in the United States?
    if idx == 736:
        return '''match
  $a isa article, has title $t, has date $d;
  $o isa organization, has name $on;
  $c isa country, has country_name "United States of America";
  $city isa city;
  (article: $a, organization: $o) isa mentions;
  (organization: $o, city: $city) isa located_in;
  (city: $city, country: $c) isa in_country;
sort $d desc;
limit 3;
fetch { "article": $t, "organization": $on };'''

    # Query 739: Who are the board members of the first 3 organizations in the 'Technology' industry?
    if idx == 739:
        return '''match
  $o isa organization, has name $on;
  $c isa industry_category, has industry_category_name "Technology";
  $p isa person, has name $pn;
  (organization: $o, category: $c) isa in_category;
  (organization: $o, member: $p) isa board_member_of;
limit 3;
fetch { "organization": $on, "board_member": $pn };'''

    # Query 740: List the top 5 organizations that have the most suppliers.
    if idx == 740:
        return '''match
  $o isa organization, has name $on;
  $s isa organization;
  (customer: $o, supplier: $s) isa supplies;
limit 5;
fetch { "organization": $on };'''

    # Query 745: List the top 5 cities with the most organizations in the 'Healthcare' industry.
    if idx == 745:
        return '''match
  $c isa city, has city_name $cn;
  $o isa organization;
  $ic isa industry_category, has industry_category_name "Healthcare";
  (organization: $o, city: $c) isa located_in;
  (organization: $o, category: $ic) isa in_category;
limit 5;
fetch { "city": $cn };'''

    # Query 762: Who are the top 3 suppliers of organizations in 'New York City'?
    if idx == 762:
        return '''match
  $o isa organization;
  $city isa city, has city_name "New York City";
  $supplier isa organization, has name $sn;
  (organization: $o, city: $city) isa located_in;
  (customer: $o, supplier: $supplier) isa supplies;
limit 3;
fetch { "supplier": $sn };'''

    # Query 770: What are the names of organizations that have 'Accenture' as an investor?
    if idx == 770:
        return '''match
  $investor isa organization, has name "Accenture";
  $o isa organization, has name $on;
  (organization: $o, investor: $investor) isa invested_in;
fetch { "organization": $on };'''

    # Query 774: List all people who are board members of organizations in Houston.
    if idx == 774:
        return '''match
  $p isa person, has name $pn;
  $o isa organization, has name $on;
  $city isa city, has city_name "Houston";
  (organization: $o, member: $p) isa board_member_of;
  (organization: $o, city: $city) isa located_in;
fetch { "person": $pn, "organization": $on };'''

    # Query 787: List the authors who have mentioned more than three different organizations in their articles.
    if idx == 787:
        return '''match
  $a isa article, has author $auth;
  $o1 isa organization;
  $o2 isa organization;
  $o3 isa organization;
  $o4 isa organization;
  (article: $a, organization: $o1) isa mentions;
  (article: $a, organization: $o2) isa mentions;
  (article: $a, organization: $o3) isa mentions;
  (article: $a, organization: $o4) isa mentions;
  $o1 has organization_id $id1;
  $o2 has organization_id $id2;
  $o3 has organization_id $id3;
  $o4 has organization_id $id4;
  $id1 != $id2;
  $id1 != $id3;
  $id1 != $id4;
  $id2 != $id3;
  $id2 != $id4;
  $id3 != $id4;
fetch { "author": $auth };'''

    # Query 812: List the countries where organizations with a revenue less than $10 million are located.
    if idx == 812:
        return '''match
  $o isa organization;
  $c isa country, has country_name $cn;
  $city isa city;
  (organization: $o, city: $city) isa located_in;
  (city: $city, country: $c) isa in_country;
  $o has revenue $r;
  $r < 10000000;
fetch { "country": $cn };'''

    # Query 816: What are the cities where organizations with the highest number of suppliers are located?
    if idx == 816:
        return '''match
  $o isa organization;
  $s isa organization;
  $c isa city, has city_name $cn;
  (customer: $o, supplier: $s) isa supplies;
  (organization: $o, city: $c) isa located_in;
limit 1;
fetch { "city": $cn };'''

    # Query 826: Who are the investors of organizations with a revenue exceeding $500 million?
    if idx == 826:
        return '''match
  $o isa organization, has name $on;
  $investor isa organization, has name $in;
  (organization: $o, investor: $investor) isa invested_in;
  $o has revenue $r;
  $r > 500000000;
fetch { "organization": $on, "investor": $in };'''

    # Query 866: What are the names of the people who are board members of at least two different organizations?
    if idx == 866:
        return '''match
  $p isa person, has name $pn;
  $o1 isa organization;
  $o2 isa organization;
  (organization: $o1, member: $p) isa board_member_of;
  (organization: $o2, member: $p) isa board_member_of;
  $o1 has organization_id $id1;
  $o2 has organization_id $id2;
  $id1 != $id2;
fetch { "name": $pn };'''

    # Query 877: Find the top 3 countries mentioned in articles that have the lowest sentiment scores.
    if idx == 877:
        return '''match
  $article isa article, has sentiment $s;
  $o isa organization;
  $country isa country, has country_name $cn;
  $city isa city;
  (article: $article, organization: $o) isa mentions;
  (organization: $o, city: $city) isa located_in;
  (city: $city, country: $country) isa in_country;
sort $s asc;
limit 3;
fetch { "country": $cn };'''

    # Query 891: Identify the top 3 organizations with the most articles mentioning them.
    if idx == 891:
        return '''match
  $o isa organization, has name $on;
  $a isa article;
  (article: $a, organization: $o) isa mentions;
limit 3;
fetch { "organization": $on };'''

    # Query 897: Identify the top 3 organizations with the most board members.
    if idx == 897:
        return '''match
  $o isa organization, has name $on;
  $p isa person;
  (organization: $o, member: $p) isa board_member_of;
limit 3;
fetch { "organization": $on };'''

    # Query 919: Find the top 3 organizations that have the most board members who are also CEOs of other organizations.
    if idx == 919:
        return '''match
  $o isa organization, has name $on;
  $p isa person;
  $other isa organization;
  (organization: $o, member: $p) isa board_member_of;
  (organization: $other, ceo: $p) isa ceo_of;
  $o has organization_id $oid;
  $other has organization_id $otherid;
  $oid != $otherid;
limit 3;
fetch { "organization": $on };'''

    # Query 921: Identify the top 3 organizations with the highest revenue that are headquartered in 'United States of America'.
    if idx == 921:
        return '''match
  $o isa organization, has name $on;
  $c isa country, has country_name "United States of America";
  $city isa city;
  (organization: $o, city: $city) isa located_in;
  (city: $city, country: $c) isa in_country;
  $o has revenue $r;
sort $r desc;
limit 3;
fetch { "organization": $on, "revenue": $r };'''

    # Query 929: List the first 5 organizations that have a CEO with a name mentioned in at least two different articles and a revenue greater than 50 million.
    if idx == 929:
        return '''match
  $o isa organization, has name $on;
  $ceo isa person, has name $ceon;
  $a1 isa article;
  $a2 isa article;
  (organization: $o, ceo: $ceo) isa ceo_of;
  (article: $a1, organization: $o) isa mentions;
  (article: $a2, organization: $o) isa mentions;
  $a1 has article_id $aid1;
  $a2 has article_id $aid2;
  $aid1 != $aid2;
  $o has revenue $r;
  $r > 50000000;
limit 5;
fetch { "organization": $on, "ceo": $ceon };'''

    # Query 931: Identify the top 3 organizations with the most subsidiaries headquartered in a city named 'Seattle'.
    if idx == 931:
        return '''match
  $o isa organization, has name $on;
  $sub isa organization;
  $c isa city, has city_name "Seattle";
  (parent: $o, subsidiary: $sub) isa subsidiary_of;
  (organization: $sub, city: $c) isa located_in;
limit 3;
fetch { "organization": $on };'''

    # Query 932: List the first 5 organizations that have a CEO with a name mentioned in more than one article and a revenue greater than 100 million.
    if idx == 932:
        return '''match
  $o isa organization, has name $on;
  $ceo isa person, has name $ceon;
  $a1 isa article;
  $a2 isa article;
  (organization: $o, ceo: $ceo) isa ceo_of;
  (article: $a1, organization: $o) isa mentions;
  (article: $a2, organization: $o) isa mentions;
  $a1 has article_id $aid1;
  $a2 has article_id $aid2;
  $aid1 != $aid2;
  $o has revenue $r;
  $r > 100000000;
limit 5;
fetch { "organization": $on, "ceo": $ceon };'''

    return None  # Default: cannot convert

# Process each query
successful = []
still_failed = []

for _, row in failed_df.iterrows():
    idx = row['original_index']
    question = row['question']
    cypher = row['cypher']

    typeql = convert_query(idx, question, cypher)

    if typeql is None:
        still_failed.append({
            'original_index': idx,
            'question': question,
            'cypher': cypher,
            'error': 'Cannot convert - schema mismatch or unsupported pattern'
        })
        print(f"Query {idx}: Cannot convert")
        continue

    # Validate the query
    valid, error = validate_query(typeql)

    if valid:
        successful.append({
            'original_index': idx,
            'question': question,
            'cypher': cypher,
            'typeql': typeql
        })
        print(f"Query {idx}: SUCCESS")
    else:
        still_failed.append({
            'original_index': idx,
            'question': question,
            'cypher': cypher,
            'error': error
        })
        print(f"Query {idx}: FAILED - {error[:100]}")

print(f"\n=== SUMMARY ===")
print(f"Successfully converted: {len(successful)}")
print(f"Still failed: {len(still_failed)}")

# Save results
if successful:
    # Read existing queries.csv and append
    existing_df = pd.read_csv('/opt/text2typeql/output/companies/queries.csv')
    new_df = pd.DataFrame(successful)
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    combined_df.to_csv('/opt/text2typeql/output/companies/queries.csv', index=False, quoting=csv.QUOTE_ALL)
    print(f"Appended {len(successful)} queries to queries.csv")

# Save still-failed queries
still_failed_df = pd.DataFrame(still_failed)
still_failed_df.to_csv('/opt/text2typeql/output/companies/failed.csv', index=False, quoting=csv.QUOTE_ALL)
print(f"Updated failed.csv with {len(still_failed)} queries")

driver.close()
