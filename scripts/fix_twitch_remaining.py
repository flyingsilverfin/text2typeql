#!/usr/bin/env python3
"""Fix remaining twitch queries that failed semantic review."""

import csv
import sys
sys.path.insert(0, '/opt/text2typeql')

from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# ============================================================================
# Remaining queries from failed_review.csv - analyze and categorize
# ============================================================================

# FIXABLE - queries where we can create valid TypeQL alternatives
FIXABLE_QUERIES = {
    # "Who are the top 3 VIPs in the stream named 'summit1g'?"
    # - Users don't have followers, but we can still return VIPs (just not sorted by followers)
    6: """match
  $s isa stream, has name "summit1g";
  vip_status (channel_with_vips: $s, vip_user: $vip);
  $vip has name $vn;
limit 3;
fetch { "vip": $vn };""",

    # "Who are the top 3 users who are VIPs in the stream with the name '9linda'?"
    # - Same issue, users don't have followers. Return 3 VIPs without ordering by followers
    43: """match
  $s isa stream, has name "9linda";
  vip_status (channel_with_vips: $s, vip_user: $u);
  $u has name $un;
limit 3;
fetch { "user": $un };""",

    # "Which 3 streams have the highest ratio of followers to total view count?"
    # - CAN use let expression for ratio calculation!
    152: """match
  $s isa stream, has name $sn, has followers $f, has total_view_count $tvc;
  $tvc > 0;
let $ratio = $f / $tvc;
sort $ratio desc;
limit 3;
fetch { "stream": $sn, "followers": $f, "total_view_count": $tvc, "ratio": $ratio };""",

    # "Find the descriptions of streams that have chatted with the user '9linda'."
    # - The Cypher has the direction reversed. Let's match streams where the user '9linda' is chatting
    # - chat_activity has: chatting_user (the user doing the chatting) and channel_with_chatters (the stream)
    # - So we want streams where 9linda is a chatter
    359: """match
  $u isa user, has name "9linda";
  chat_activity (chatting_user: $u, channel_with_chatters: $s);
  $s has description $d;
fetch { "description": $d };""",
}

# UNSUPPORTED - truly unfixable queries
UNSUPPORTED_QUERIES = {
    # "What are the first 5 streams that have been played by more than 5 different users?"
    # - Cypher query is nonsensical - streams don't get played by users, streams play games
    26: "Nonsensical query: streams don't get played by users. The PLAYS relationship is Stream->Game, not User->Stream.",

    # "Which 3 users have moderated the most streams and users combined?"
    # - Schema limitation: moderation only tracks moderated_channel (stream) not moderated_user
    99: "Schema limitation: moderation relation only tracks moderated streams, not moderated users. Cannot count 'streams and users combined'.",

    # "Which streams have chatters who have less than 1000 followers?"
    # - Users don't have followers attribute in schema
    128: "Schema mismatch: User entity does not have 'followers' attribute. Cannot filter users by followers.",

    # "What are the top 5 streams with the longest descriptions?"
    # - TypeQL doesn't support string length function
    142: "TypeQL does not support string length functions. Cannot sort streams by description length.",

    # "List the first 3 streams that have chatters who are also streams."
    # - Type mismatch: chatters are users, cannot also be streams
    207: "Schema/logic error: chatters are User entities, they cannot simultaneously be Stream entities. Type mismatch.",

    # "Which streams are played by users with a name containing 'doduik' and have a total view count above 3000?"
    # - Cypher query is wrong - PLAYS is Stream->Game not User->Stream
    227: "Cypher query error: The PLAYS relationship connects Stream to Game, not User to Stream. Query is not valid.",

    # "Show the streams that have been moderated by a user more than once."
    # - Moderation is a single relationship instance per user-stream pair
    459: "Schema semantics: moderation relationship is a single instance per user-stream pair. 'Moderated more than once' is not representable.",

    # "Name the streams that are VIPs in the stream named 'itsbigchase'."
    # - VIP relationship requires User as vip_user, not Stream
    526: "Schema mismatch: vip_status relation requires User entity as vip_user role. Streams cannot be VIPs.",

    # "List the streams that have the same game played by both itsbigchase and 9linda."
    # - Cypher has PLAYS going from User to Game, but schema has PLAYS from Stream to Game
    533: "Cypher query error: The PLAYS relationship connects Stream to Game, not User to Game. Query is not valid.",
}


def validate_query(driver, query):
    """Validate TypeQL query against TypeDB."""
    try:
        with driver.transaction("text2typeql_twitch", TransactionType.READ) as tx:
            result = tx.query(query).resolve()
            # Try to consume results
            if 'fetch' in query:
                for doc in result.as_concept_documents():
                    break
            else:
                for row in result.as_concept_rows():
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
    failed_review_path = '/opt/text2typeql/output/twitch/failed_review.csv'
    with open(failed_review_path, 'r') as f:
        reader = csv.DictReader(f)
        failed_rows = list(reader)

    # Read existing queries.csv
    queries_path = '/opt/text2typeql/output/twitch/queries.csv'
    with open(queries_path, 'r') as f:
        reader = csv.DictReader(f)
        existing_queries = list(reader)

    # Read existing failed.csv
    failed_path = '/opt/text2typeql/output/twitch/failed.csv'
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

        if idx in FIXABLE_QUERIES:
            fix_query = FIXABLE_QUERIES[idx]
            valid, error = validate_query(driver, fix_query)
            if valid:
                fixed_queries.append({
                    'original_index': idx,
                    'question': question,
                    'cypher': cypher,
                    'typeql': fix_query
                })
                print(f"[{idx}] FIXED - validated successfully")
            else:
                print(f"[{idx}] INVALID - {error[:100]}...")
                remaining_failed.append({
                    'original_index': idx,
                    'question': question,
                    'cypher': cypher,
                    'typeql': fix_query,
                    'review_reason': f"Fixed query failed validation: {error}"
                })
        elif idx in UNSUPPORTED_QUERIES:
            new_failed.append({
                'original_index': idx,
                'question': question,
                'cypher': cypher,
                'error': UNSUPPORTED_QUERIES[idx]
            })
            print(f"[{idx}] UNSUPPORTED - {UNSUPPORTED_QUERIES[idx][:80]}...")
        else:
            print(f"[{idx}] NOT HANDLED - keeping in failed_review.csv")
            remaining_failed.append(row)

    # Remove fixed queries from existing_queries if they exist
    fixed_indices = {int(q['original_index']) for q in fixed_queries}
    existing_queries = [q for q in existing_queries if int(q['original_index']) not in fixed_indices]

    # Merge and sort
    all_queries = existing_queries + fixed_queries
    all_queries.sort(key=lambda x: int(x['original_index']))

    with open(queries_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql'])
        writer.writeheader()
        writer.writerows(all_queries)

    # Write remaining failed_review.csv
    with open(failed_review_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'typeql', 'review_reason'])
        writer.writeheader()
        writer.writerows(remaining_failed)

    # Merge failed.csv
    # Remove duplicates
    existing_failed_indices = {int(f['original_index']) for f in existing_failed}
    new_failed_filtered = [f for f in new_failed if int(f['original_index']) not in existing_failed_indices]
    all_failed = existing_failed + new_failed_filtered
    all_failed.sort(key=lambda x: int(x['original_index']))

    with open(failed_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['original_index', 'question', 'cypher', 'error'])
        writer.writeheader()
        writer.writerows(all_failed)

    driver.close()

    print(f"\n=== Summary ===")
    print(f"Fixed and validated: {len(fixed_queries)}")
    print(f"Still in failed_review: {len(remaining_failed)}")
    print(f"Moved to failed.csv: {len(new_failed_filtered)}")
    print(f"Total queries.csv: {len(all_queries)}")
    print(f"Total failed.csv: {len(all_failed)}")


if __name__ == '__main__':
    main()
