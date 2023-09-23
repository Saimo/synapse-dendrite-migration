import pandas as pd
import json
import re
from util import map_dataframe, generate_sql_insert_statement, get_df_from_table, server_name, escape_pg_value

mapping = {
    'event_id': ('event_id', str),
    'room_id': ('room_id', str),
    'headered_event_json': ('json', escape_pg_value),
    'type': ('type', str),
    'sender': ('sender', lambda x: re.sub(r':\S*', f":{server_name}", x)),
    'contains_url': ('contains_url', bool),
    'add_state_ids': ('event_id', lambda x: '{}' if pd.isna(x) else f'{{"{({x}).pop()}"}}'),
    'remove_state_ids': ('type', lambda x: '{}'), # TODO: A real solution
    'transaction_id': ('txn_id', str),
    'exclude_from_sync': ('type', lambda x: False), # TODO: A real solution
    'history_visibility': ('history_visibility', lambda x: 2 if x == 'shared' else 1),
}


def transform_output_room_events(conn_synapse):
    cursor_synapse = conn_synapse.cursor()

    events_df = get_df_from_table(cursor_synapse, 'events')
    current_state_stream_df = get_df_from_table(cursor_synapse, 'current_state_delta_stream')
    state_events_df = get_df_from_table(cursor_synapse, 'state_events')
    current_state_events_df = get_df_from_table(cursor_synapse, 'current_state_events')
    state_groups_state_df = get_df_from_table(cursor_synapse, 'state_groups_state')
    event_json_df = get_df_from_table(cursor_synapse, 'event_json')
    event_txn_id = get_df_from_table(cursor_synapse, 'event_txn_id')
    room_stats_state_df = get_df_from_table(cursor_synapse, 'room_stats_state')

    df = events_df.merge(current_state_stream_df, how='left', on='event_id', suffixes=('', '_y')).merge(state_events_df, how='left', on='event_id', suffixes=('', '_y')).merge(current_state_events_df, how='left', on='event_id', suffixes=('', '_y')).merge(state_groups_state_df, how='left', on='event_id', suffixes=('', '_y')).merge(event_json_df, how='left', on='event_id', suffixes=('', '_y')).merge(event_txn_id, how='left', on='event_id', suffixes=('', '_y')).merge(room_stats_state_df, how='left', on='room_id', suffixes=('', '_y'))


    df = map_dataframe(df, df.columns.tolist(), mapping)

    insert_statements = (generate_sql_insert_statement(df, 'syncapi_output_room_events'))

    return insert_statements
