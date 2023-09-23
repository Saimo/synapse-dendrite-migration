import pandas as pd
import psycopg2
import re
from util import map_dataframe, generate_sql_insert_statement, transform_data, server_name, get_df_from_table, get_nid_from_table

conn_dendrite_target = psycopg2.connect(
        dbname="dendrite",
        user="dendrite_user",
        password="dendrite_password",
        host="localhost",
        port="5432"
)

cursor_target = conn_dendrite_target.cursor()

event_state_keys_mapping = {}

cursor_target.execute("SELECT event_state_key_nid, event_state_key FROM roomserver_event_state_keys")
for row in cursor_target.fetchall():
    event_state_keys_mapping[row[1]] = row[0]



mapping = {
    'room_nid': ('room_nid', int),
    'target_nid': ('user_id', lambda x: event_state_keys_mapping[x]),
    'sender_nid': ('sender', lambda x: event_state_keys_mapping[x]),
    'membership_nid': ('membership', lambda x: 1 if (x == 'leave' or x == 'ban') else 2 if x == 'invite' else 3 if x == 'join' else 4 if x == 'knock' else 1),
    'event_nid': ('event_nid', int),
    'target_local': ('room_nid', lambda x: True),
    'forgotten': ('room_nid', lambda x: False),
}

conn_dendrite_target = psycopg2.connect(
        dbname="dendrite",
        user="dendrite_user",
        password="dendrite_password",
        host="localhost",
        port="5432"
)


def transform_roomserver_memberships(conn_synapse, conn_target=None):
    cursor_synapse = conn_synapse.cursor()

    roomserver_rooms_df = get_df_from_table(cursor_target, 'roomserver_rooms')
    roomserver_events = get_df_from_table(cursor_target, 'roomserver_events', where='event_type_nid = 5')

    cursor_synapse.execute(f"SELECT * FROM room_memberships;")
    room_memberships_columns = [desc[0] for desc in cursor_synapse.description]
    room_memberships_data = cursor_synapse.fetchall()
    room_memberships_df = pd.DataFrame(room_memberships_data, columns=room_memberships_columns)

    # drop rows where event_type_nid is not 5 in roomserver_events
    roomserver_events = roomserver_events[roomserver_events['event_type_nid'] == 5]

    # join room_memberships_df with roomserver_rooms_df on room_id
    room_memberships_df = room_memberships_df.merge(roomserver_events, on='event_id').merge(roomserver_rooms_df, on='room_nid')

    df = map_dataframe(room_memberships_df, room_memberships_df.columns.tolist(), mapping)



    # keep rows with the highest membership_nid
    df = df.sort_values('membership_nid').drop_duplicates(['room_nid', 'target_nid'], keep='last')

    insert_statements = (generate_sql_insert_statement(df, 'roomserver_membership'))

    return insert_statements