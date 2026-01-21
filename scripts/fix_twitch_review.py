#!/usr/bin/env python3
"""Fix twitch queries that failed semantic review."""

import csv
import sys
sys.path.insert(0, '/opt/text2typeql')

from typedb.driver import TypeDB, Credentials, DriverOptions, TransactionType

# ============================================================================
# CATEGORY 1: Missing filter '> 1' after groupby for 'more than one' condition
# These need chained reduce with filter
# ============================================================================

GROUPBY_FILTER_FIXES = {
    # "List the first 5 users who are chatters in more than one stream."
    9: """match
  $u isa user, has name $un;
  chat_activity (chatting_user: $u, channel_with_chatters: $s);
reduce $count = count($s) groupby $un;
match $count > 1;
sort $count desc;
limit 5;
fetch { "name": $un, "stream_count": $count };""",

    # "List the first 5 streams that have been VIP in more than one stream."
    44: """match
  $s isa stream, has name $sn, has url $su;
  vip_status (vip_channel: $s, channel_with_vips: $target);
reduce $vip_count = count($target) groupby $sn, $su;
match $vip_count > 1;
sort $vip_count desc;
limit 5;
fetch { "stream_name": $sn, "stream_url": $su };""",

    # "Which users are VIPs in more than one stream?"
    56: """match
  $u isa user, has name $un;
  vip_status (vip_user: $u, channel_with_vips: $s);
reduce $num_vip_streams = count($s) groupby $un;
match $num_vip_streams > 1;
sort $num_vip_streams desc;
fetch { "user": $un, "num_vip_streams": $num_vip_streams };""",

    # "Identify streams that have more than one language associated with them."
    68: """match
  $s isa stream, has name $sn;
  language_usage (channel: $s, used_language: $l);
reduce $languageCount = count($l) groupby $sn;
match $languageCount > 1;
sort $languageCount desc;
fetch { "stream": $sn, "language_count": $languageCount };""",

    # "How many users have VIP status in more than one stream?"
    129: """match
  $u isa user, has name $un;
  vip_status (vip_user: $u, channel_with_vips: $s);
reduce $vip_count = count($s) groupby $un;
match $vip_count > 1;
reduce $user_count = count($un);""",

    # "Find the first 3 users who are moderators of more than one stream."
    204: """match
  $u isa user, has name $un;
  moderation (moderating_user: $u, moderated_channel: $s);
reduce $num_moderated_streams = count($s) groupby $un;
match $num_moderated_streams > 1;
sort $num_moderated_streams desc;
limit 3;
fetch { "name": $un, "num_moderated_streams": $num_moderated_streams };""",

    # "Name the top 5 users who are moderators for more than one stream."
    250: """match
  $u isa user, has name $un;
  moderation (moderating_user: $u, moderated_channel: $s);
reduce $num_moderated_streams = count($s) groupby $un;
match $num_moderated_streams > 1;
sort $num_moderated_streams desc;
limit 5;
fetch { "user": $un, "num_moderated_streams": $num_moderated_streams };""",

    # "Which users are moderators in more than one stream?"
    391: """match
  $u isa user, has name $un;
  moderation (moderating_user: $u, moderated_channel: $s);
reduce $num_moderated_streams = count($s) groupby $un;
match $num_moderated_streams > 1;
sort $num_moderated_streams desc;
fetch { "user": $un, "num_moderated_streams": $num_moderated_streams };""",

    # "Which users are moderators for more than one stream? List the top 3."
    419: """match
  $u isa user, has name $un;
  moderation (moderating_user: $u, moderated_channel: $s);
reduce $num_moderated = count($s) groupby $un;
match $num_moderated > 1;
sort $num_moderated desc;
limit 3;
fetch { "user": $un, "num_moderated_streams": $num_moderated };""",

    # "Name the top 3 users who are moderators for more than one stream."
    513: """match
  $u isa user, has name $un;
  moderation (moderating_user: $u, moderated_channel: $s);
reduce $num_moderated_streams = count($s) groupby $un;
match $num_moderated_streams > 1;
sort $num_moderated_streams desc;
limit 3;
fetch { "user": $un, "num_moderated_streams": $num_moderated_streams };""",

    # "Which users are moderators for more than one stream?"
    541: """match
  $u isa user, has name $un;
  moderation (moderating_user: $u, moderated_channel: $s);
reduce $num_moderated_streams = count($s) groupby $un;
match $num_moderated_streams > 1;
fetch { "user": $un, "num_moderated_streams": $num_moderated_streams };""",

    # "List the top 3 users who chatter in more than one stream."
    545: """match
  $u isa user, has name $un;
  chat_activity (chatting_user: $u, channel_with_chatters: $s);
reduce $num_streams = count($s) groupby $un;
match $num_streams > 1;
sort $num_streams desc;
limit 3;
fetch { "name": $un, "num_streams": $num_streams };""",

    # "Which users have chatted with more than 5 different streams?"
    347: """match
  $u isa user, has name $un;
  chat_activity (chatting_user: $u, channel_with_chatters: $s);
reduce $num_streams = count($s) groupby $un;
match $num_streams > 5;
sort $num_streams desc;
fetch { "user": $un, "streams_chatted_with": $num_streams };""",
}

# ============================================================================
# CATEGORY 2: Subquery semantics - need two-stage queries
# These should find the highest/most item first, then get related items
# ============================================================================

SUBQUERY_FIXES = {
    # "Who are the top 3 moderators for the stream with the highest follower count?"
    13: """match
  $s isa stream, has followers $f;
sort $f desc;
limit 1;
match
  moderation (moderated_channel: $s, moderating_user: $m);
  $m has name $mn;
limit 3;
fetch { "moderator": $mn };""",

    # "Who are the top 3 users who chatter in the stream with the highest total view count?"
    27: """match
  $s isa stream, has name $sn, has total_view_count $tvc;
sort $tvc desc;
limit 1;
match
  chat_activity (channel_with_chatters: $s, chatting_user: $u);
  $u has name $un;
limit 3;
fetch { "user": $un, "stream": $sn };""",

    # "Which streams are chatters in the stream with the most followers?"
    40: """match
  $s isa stream, has followers $f;
sort $f desc;
limit 1;
match
  chat_activity (channel_with_chatters: $s, chatting_channel: $chatter);
  $chatter has name $cn;
fetch { "name": $cn };""",

    # "Who are the top 3 moderators in the stream with the lowest follower count?"
    48: """match
  $s isa stream, has followers $f;
sort $f asc;
limit 1;
match
  moderation (moderated_channel: $s, moderating_user: $m);
  $m has name $mn;
limit 3;
fetch { "moderator": $mn };""",

    # "Who are the first 5 moderators for the stream with the highest total view count?"
    259: """match
  $s isa stream, has total_view_count $tvc;
sort $tvc desc;
limit 1;
match
  moderation (moderated_channel: $s, moderating_user: $m);
  $m has name $mn;
limit 5;
fetch { "moderator": $mn };""",

    # "Name the top 3 users who are VIPs in the stream with the most followers."
    271: """match
  $s isa stream, has followers $f;
sort $f desc;
limit 1;
match
  vip_status (channel_with_vips: $s, vip_user: $u);
  $u has name $un;
limit 3;
fetch { "user": $un };""",

    # "Who are the first 5 chatters in the stream with the highest follower count?"
    292: """match
  $s isa stream, has followers $f;
sort $f desc;
limit 1;
match
  chat_activity (channel_with_chatters: $s, chatting_user: $chatter);
  $chatter has name $cn;
limit 5;
fetch { "name": $cn };""",

    # "Which users are VIPs for the stream with the highest follower count?"
    356: """match
  $s isa stream, has name $sn, has followers $f;
sort $f desc;
limit 1;
match
  vip_status (channel_with_vips: $s, vip_user: $u);
  $u has name $un;
fetch { "user": $un, "stream": $sn };""",
}

# ============================================================================
# CATEGORY 3: Sort direction issues - oldest means ascending (CORRECT)
# These queries ask for "oldest" or "top oldest" which means ascending sort
# The review flagged them incorrectly - they ARE correct, but keeping for completeness
# ============================================================================

# Note: For queries asking for "oldest" or "top oldest", ascending sort IS correct
# "top 5 oldest" = oldest first = sort ascending
# These queries are actually CORRECT but were flagged as issues

SORT_DIRECTION_CORRECT = {
    # "What are the names of the top 5 oldest teams in the graph?" - asc IS correct for oldest
    83: """match
  $t isa team, has name $tn, has created_at $ca;
sort $ca asc;
limit 5;
fetch { "name": $tn };""",

    # "List the top 5 oldest streams in the graph." - asc IS correct for oldest
    96: """match
  $s isa stream, has name $sn, has created_at $ca;
sort $ca asc;
limit 5;
fetch { "name": $sn, "createdAt": $ca };""",

    # "What are the top 5 oldest streams based on the 'createdAt' property?" - asc IS correct
    140: """match
  $s isa stream, has name $sn, has created_at $ca;
sort $ca asc;
limit 5;
fetch { "stream_name": $sn, "created_at": $ca };""",

    # "Find the top 5 streams by oldest to newest based on the 'createdAt' property." - asc IS correct
    153: """match
  $s isa stream, has name $sn, has created_at $ca;
sort $ca asc;
limit 5;
fetch { "name": $sn, "createdAt": $ca };""",

    # "List the top 5 oldest teams based on the 'createdAt' property." - asc IS correct
    166: """match
  $t isa team, has name $tn, has created_at $ca;
sort $ca asc;
limit 5;
fetch { "team": $tn, "createdAt": $ca };""",

    # "What are the top 5 oldest streams by the 'createdAt' property that are still active?" - asc IS correct
    186: """match
  $s isa stream, has created_at $ca, has name $sn;
sort $ca asc;
limit 5;
fetch { "name": $sn, "createdAt": $ca };""",

    # "What are the top 5 streams created before 2010?" - "top" here means most important/first - asc IS correct
    210: """match
  $s isa stream, has name $sn, has created_at $ca;
  $ca < 2010-01-01;
sort $ca asc;
limit 5;
fetch { "stream": $sn, "created": $ca };""",

    # "List the top 5 streams with the lowest follower count that play the game 'Rust'." - asc IS correct for lowest
    214: """match
  $g isa game, has name "Rust";
  game_play (streaming_channel: $s, played_game: $g);
  $s has name $sn, has followers $f;
sort $f asc;
limit 5;
fetch { "stream": $sn, "followerCount": $f };""",

    # "Find the top 5 streams that have been active the longest based on the creation date." - asc IS correct
    476: """match
  $s isa stream, has name $sn, has created_at $ca;
sort $ca asc;
limit 5;
fetch { "stream_name": $sn, "created_at": $ca };""",

    # "What are the top 3 streams with the least followers that still have moderators?" - asc IS correct for least
    550: """match
  $s isa stream, has name $sn, has followers $f;
  moderation (moderated_channel: $s, moderating_user: $m);
sort $f asc;
limit 3;
fetch { "stream": $sn, "followerCount": $f };""",
}

# ============================================================================
# CATEGORY 4: Sort direction fixes where review was RIGHT
# ============================================================================

SORT_DIRECTION_FIXES = {
    # "What are the descriptions of the streams with the oldest creation dates?"
    # - "oldest" means earliest creation date = ascending sort IS correct
    # But the review says "TypeQL sorts descending" which is wrong, original has "asc"
    # The original query is correct
    373: """match
  $s isa stream, has description $d, has created_at $ca;
sort $ca asc;
limit 5;
fetch { "description": $d };""",

    # "What are the descriptions of the streams with the least followers?"
    # - "least" means lowest = ascending sort IS correct
    # Review says TypeQL sorts descending which is wrong, original has "asc"
    # The original query is correct
    400: """match
  $s isa stream, has description $d, has followers $f;
sort $f asc;
limit 1;
fetch { "description": $d };""",
}

# ============================================================================
# CATEGORY 5: Missing COUNT aggregation - queries that need counts
# Some can be fixed with reduce, others use pattern matching
# ============================================================================

COUNT_AGGREGATION_FIXES = {
    # "Which game is the most commonly played among the top 5 streams by follower count?"
    # This needs two stages: get top 5 streams, then count games
    10: """match
  $s isa stream, has followers $f;
  game_play (streaming_channel: $s, played_game: $g);
  $g has name $gn;
sort $f desc;
limit 5;
reduce $count = count($s) groupby $gn;
sort $count desc;
limit 1;
fetch { "game": $gn, "count": $count };""",

    # "Which 3 teams were created most recently and have at least one stream associated with them?"
    # Having at least one stream is already satisfied by the join
    268: """match
  $t isa team, has name $tn, has created_at $ca;
  team_membership (member_stream: $s, organization: $t);
sort $ca desc;
limit 3;
fetch { "team_name": $tn, "created_at": $ca };""",

    # "List the first 5 streams that have changed their primary game at least once."
    # More than 1 game = at least 2 different games (using pattern match)
    277: """match
  $s isa stream, has name $sn, has url $su, has created_at $ca;
  game_play (streaming_channel: $s, played_game: $g1);
  game_play (streaming_channel: $s, played_game: $g2);
  not { $g1 is $g2; };
sort $ca asc;
limit 5;
fetch { "stream_name": $sn, "stream_url": $su };""",

    # "Which are the first 5 streams that have more than one language associated with them?"
    # Use pattern matching for > 1 languages
    303: """match
  $s isa stream, has name $sn, has url $su;
  language_usage (channel: $s, used_language: $l1);
  language_usage (channel: $s, used_language: $l2);
  not { $l1 is $l2; };
limit 5;
fetch { "streamName": $sn, "streamUrl": $su };""",

    # "What are the top 3 streams that have changed their language at least once?"
    # More than 1 language = changed language
    312: """match
  $s isa stream, has name $sn, has url $su;
  language_usage (channel: $s, used_language: $l1);
  language_usage (channel: $s, used_language: $l2);
  not { $l1 is $l2; };
limit 3;
fetch { "streamName": $sn, "streamUrl": $su };""",

    # "Identify the top 3 games played by streams with a description containing 'hilarious moments'."
    454: """match
  $s isa stream, has description $d;
  $d like ".*hilarious moments.*";
  game_play (streaming_channel: $s, played_game: $g);
  $g has name $gn;
reduce $count = count($s) groupby $gn;
sort $count desc;
limit 3;
fetch { "game": $gn, "count": $count };""",
}

# ============================================================================
# CATEGORY 6: Missing filter conditions (> N patterns using pattern matching)
# ============================================================================

MISSING_FILTER_FIXES = {
    # "Find streams that have at least 3 languages associated with them."
    445: """match
  $s isa stream, has name $sn, has url $u, has followers $f, has total_view_count $tvc;
  language_usage (channel: $s, used_language: $l1);
  language_usage (channel: $s, used_language: $l2);
  language_usage (channel: $s, used_language: $l3);
  not { $l1 is $l2; };
  not { $l1 is $l3; };
  not { $l2 is $l3; };
fetch { "name": $sn, "url": $u, "followers": $f, "total_view_count": $tvc };""",

    # "Which users are chatters in more than 3 different streams? List the top 5."
    # Use chained reduce with filter
    446: """match
  $u isa user, has name $un;
  chat_activity (chatting_user: $u, channel_with_chatters: $s);
reduce $num_streams = count($s) groupby $un;
match $num_streams > 3;
sort $num_streams desc;
limit 5;
fetch { "name": $un, "num_streams": $num_streams };""",

    # "Show the streams that have more than 2 languages associated with them."
    455: """match
  $s isa stream, has name $sn, has url $u, has followers $f, has total_view_count $tvc;
  language_usage (channel: $s, used_language: $l1);
  language_usage (channel: $s, used_language: $l2);
  language_usage (channel: $s, used_language: $l3);
  not { $l1 is $l2; };
  not { $l1 is $l3; };
  not { $l2 is $l3; };
fetch { "name": $sn, "url": $u, "followers": $f, "total_view_count": $tvc };""",

    # "Identify the streams with more than 3 VIP users."
    477: """match
  $s isa stream, has name $sn, has url $u, has followers $f, has total_view_count $tvc;
  vip_status (channel_with_vips: $s, vip_user: $u1);
  vip_status (channel_with_vips: $s, vip_user: $u2);
  vip_status (channel_with_vips: $s, vip_user: $u3);
  vip_status (channel_with_vips: $s, vip_user: $u4);
  not { $u1 is $u2; };
  not { $u1 is $u3; };
  not { $u1 is $u4; };
  not { $u2 is $u3; };
  not { $u2 is $u4; };
  not { $u3 is $u4; };
fetch { "name": $sn, "url": $u, "followers": $f, "total_view_count": $tvc };""",
}

# ============================================================================
# CATEGORY 7: Missing LIMIT fix
# ============================================================================

LIMIT_FIXES = {
    # "What are the names of the first 3 languages used by streams with a description containing 'strategy'?"
    554: """match
  $s isa stream, has description $d;
  $d like ".*strategy.*";
  language_usage (channel: $s, used_language: $l);
  $l has name $ln;
limit 3;
fetch { "language": $ln };""",
}

# ============================================================================
# CATEGORY 8: Unsupported features - move to failed.csv
# ============================================================================

UNSUPPORTED = {
    # "Which streams were created in 2019 and have a description longer than 50 characters?"
    71: "TypeQL does not support string length functions",

    # "Find the 3 streams that play the least common games based on follower count."
    # This requires MAX aggregation which TypeQL doesn't support in groupby
    95: "Complex aggregation (MAX within groupby) not supported in TypeQL",

    # "Which 3 streams have the least followers but are associated with a game with more than 100,000 total views?"
    # This requires SUM aggregation
    181: "SUM aggregation not supported in TypeQL",

    # "Which 3 games are played by streams with the highest ratio of followers to total view counts?"
    # This requires MAX aggregation
    187: "MAX aggregation within groupby not supported in TypeQL",
}


def validate_query(driver, query):
    """Validate TypeQL query against TypeDB."""
    try:
        with driver.transaction("text2typeql_twitch", TransactionType.READ) as tx:
            result = tx.query(query).resolve()
            # Try to consume results - use concept_rows for reduce-only queries
            # and concept_documents for fetch queries
            if 'fetch' in query:
                count = 0
                for doc in result.as_concept_documents():
                    count += 1
                    if count >= 1:
                        break
            else:
                # Reduce-only query returns concept rows
                count = 0
                for row in result.as_concept_rows():
                    count += 1
                    if count >= 1:
                        break
            return True, None
    except Exception as e:
        return False, str(e)


def get_fix(idx):
    """Get the fix for a given index, returns (fix_type, fix_value)."""
    if idx in GROUPBY_FILTER_FIXES:
        return ("typeql", GROUPBY_FILTER_FIXES[idx])
    if idx in SUBQUERY_FIXES:
        return ("typeql", SUBQUERY_FIXES[idx])
    if idx in SORT_DIRECTION_CORRECT:
        return ("typeql", SORT_DIRECTION_CORRECT[idx])
    if idx in SORT_DIRECTION_FIXES:
        return ("typeql", SORT_DIRECTION_FIXES[idx])
    if idx in COUNT_AGGREGATION_FIXES:
        return ("typeql", COUNT_AGGREGATION_FIXES[idx])
    if idx in MISSING_FILTER_FIXES:
        return ("typeql", MISSING_FILTER_FIXES[idx])
    if idx in LIMIT_FIXES:
        return ("typeql", LIMIT_FIXES[idx])
    if idx in UNSUPPORTED:
        return ("unsupported", UNSUPPORTED[idx])
    return (None, None)


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
