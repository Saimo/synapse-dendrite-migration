import pandas as pd
import json

import psycopg2

from util import map_dataframe, generate_sql_insert_statement, get_df_from_table, server_name, escape_pg_value

mapping = {
    'event_nid': ('event_nid', int),
    'event_json': ('json', lambda x: escape_pg_value(x)),
}

conn_dendrite_target = psycopg2.connect(
        dbname="dendrite",
        user="dendrite_user",
        password="dendrite_password",
        host="localhost",
        port="5432"
)

def transform_roomserver_event_json(conn_synapse):
    cursor_dendrite = conn_dendrite_target.cursor()
    cursor_synapse = conn_synapse.cursor()

    events_df = get_df_from_table(cursor_synapse, 'events')
    current_state_stream_df = get_df_from_table(cursor_synapse, 'current_state_delta_stream')
    state_events_df = get_df_from_table(cursor_synapse, 'state_events')
    current_state_events_df = get_df_from_table(cursor_synapse, 'current_state_events')
    state_groups_state_df = get_df_from_table(cursor_synapse, 'state_groups_state')
    event_json_df = get_df_from_table(cursor_synapse, 'event_json')
    event_txn_id = get_df_from_table(cursor_synapse, 'event_txn_id')
    room_stats_state_df = get_df_from_table(cursor_synapse, 'room_stats_state')
    roomserver_events_df = get_df_from_table(cursor_dendrite, 'roomserver_events')

    df = events_df.merge(current_state_stream_df, how='left', on='event_id', suffixes=('', '_y')).merge(state_events_df, how='left', on='event_id', suffixes=('', '_y')).merge(current_state_events_df, how='left', on='event_id', suffixes=('', '_y')).merge(state_groups_state_df, how='left', on='event_id', suffixes=('', '_y')).merge(event_json_df, how='left', on='event_id', suffixes=('', '_y')).merge(event_txn_id, how='left', on='event_id', suffixes=('', '_y')).merge(room_stats_state_df, how='left', on='room_id', suffixes=('', '_y')).merge(roomserver_events_df, how='left', on='event_id', suffixes=('', '_y'))

    df = map_dataframe(df, df.columns.tolist(), mapping)

    insert_statements = (generate_sql_insert_statement(df, 'roomserver_event_json'))

    return insert_statements
