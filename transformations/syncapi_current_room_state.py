import pandas as pd
import json
import re
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed

from util import map_dataframe, generate_sql_insert_statement, get_df_from_table, get_nid_from_table, server_name, escape_pg_value

import json

def extract_membership(json_string):
    data = json.loads(json_string)
    if "content" in data and "membership" in data["content"]:
        return data["content"]["membership"]
    else:
        return None

room_events_mapping = {
    'm.room.create': 1,
    'm.room.power_levels': 2,
    'm.room.join_rules': 3,
    'm.room.third_party_invite': 4,
    'm.room.member': 5,
    'm.room.redaction': 6,
    'm.room.history_visibility': 7,
    'm.room.guest_access': 65538,
    'm.room.message': 65539,
    'm.room.encryption': 65540,
    'm.room.encrypted': 65541,
}

event_state_keys_mapping = {
    5: True
}

conn_dendrite_target = psycopg2.connect(
        dbname="dendrite",
        user="dendrite_user",
        password="dendrite_password",
        host="localhost",
        port="5432"
)

cursor_target = conn_dendrite_target.cursor()


mapping = {
    'room_id': ('room_id', lambda x: re.sub(r':\S*', f":{server_name}", x)),
    'event_id': ('event_id', str),
    'type': ('type', str),
    'sender': ('sender', lambda x: re.sub(r':\S*', f":{server_name}", x)),
    'contains_url': (None, lambda x: False),
    'state_key': ('state_key', lambda x: re.sub(r':\S*', f":{server_name}", x) if x not in ('', None) else ''),
    # todo update
    # 'headered_event_json': ('json', lambda x: re.sub(r':new\.server', f":{server_name}", escape_pg_value(x))), check if this is needed. depends!
    'membership': ('membership', str),
    'added_at': ('event_nid', int),
    'history_visibility': (None, lambda x: 2)
}


def transform_syncapi_current_room_state(conn_synapse):
    cursor_synapse = conn_synapse.cursor()

    def get_df_from_table_threaded(connection, table_name):
        cursor = connection.cursor()
        return table_name, get_df_from_table(cursor, table_name)

    table_names = [
        (conn_synapse, 'events'),
        (conn_synapse, 'current_state_delta_stream'),
        (conn_synapse, 'state_events'),
        (conn_synapse, 'current_state_events'),
        (conn_synapse, 'state_groups_state'),
        (conn_synapse, 'event_json'),
        (conn_synapse, 'event_txn_id'),
        (conn_synapse, 'room_stats_state'),
        (conn_dendrite_target,  'roomserver_events'),
    ]

    num_threads = len(table_names)

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(get_df_from_table_threaded, connection, table_name) for connection, table_name in
                   table_names]

        results = {}
        for future in as_completed(futures):
            table_name, df = future.result()
            results[table_name] = df

    events_df = results['events']
    current_state_stream_df = results['current_state_delta_stream']
    state_events_df = results['state_events']
    current_state_events_df = results['current_state_events']
    state_groups_state_df = results['state_groups_state']
    event_json_df = results['event_json']
    event_txn_id = results['event_txn_id']
    room_stats_state_df = results['room_stats_state']
    room_memberships_df = results['roomserver_events']

    df = events_df.merge(current_state_stream_df, how='left', on='event_id', suffixes=('', '_y')).merge(state_events_df, how='left', on='event_id', suffixes=('', '_y')).merge(current_state_events_df, how='left', on='event_id', suffixes=('', '_y')).merge(state_groups_state_df, how='left', on='event_id', suffixes=('', '_y')).merge(event_json_df, how='left', on='event_id', suffixes=('', '_y')).merge(event_txn_id, how='left', on='event_id', suffixes=('', '_y')).merge(room_stats_state_df, how='left', on='room_id', suffixes=('', '_y')).merge(room_memberships_df, how='left', on='event_id', suffixes=('', '_y'))

    df['membership'] = df['json'].apply(lambda x: extract_membership(x))

    df = map_dataframe(df, df.columns.tolist(), mapping)

    df = df.sort_values('added_at', ascending=False, na_position='last')
    df = df.drop_duplicates(subset=['room_id', 'type', 'state_key'], keep='first')
    df = df.reset_index(drop=True)

    insert_statements = (generate_sql_insert_statement(df, 'syncapi_current_room_state'))

    return insert_statements
