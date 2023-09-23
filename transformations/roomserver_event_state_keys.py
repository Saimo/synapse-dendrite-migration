import pandas as pd
import psycopg2
from tqdm import tqdm

from util import map_dataframe, generate_sql_insert_statement, transform_data, server_name, get_df_from_table

mapping = {
    'event_state_key': ('name', lambda x: '@' + x[1:].split(":", 1)[0] + f":{server_name}"),
}

room_events_mapping = {}

conn_dendrite_target = psycopg2.connect(
    dbname="dendrite",
    user="dendrite_user",
    password="dendrite_password",
    host="localhost",
    port="5432"
)

cursor_target = conn_dendrite_target.cursor()


def transform_roomserver_event_state_keys(conn_synapse):
    cursor_synapse = conn_synapse.cursor()

    cursor_target.execute("SELECT event_type_nid, event_type FROM roomserver_event_types")
    for row in cursor_target.fetchall():
        room_events_mapping[row[1]] = row[0]

    events_df = get_df_from_table(cursor_synapse, 'events')

    # Find the maximum value in the room_events_mapping dictionary
    max_value = max(room_events_mapping.values())

    # Iterate through the DataFrame to find missing event types
    for index, row in tqdm(events_df.iterrows(), total=events_df.shape[0], desc="Finding missing event types"):
        event_type = row['type']

        if not event_type.startswith('m.'):
            continue

        if event_type not in room_events_mapping:
            # Increment the max_value and assign it to the missing event type
            max_value += 1
            room_events_mapping[event_type] = max_value

    # insert room_events_mapping into roomserver_event_state_keys
    for key, value in room_events_mapping.items():
        cursor_target.execute(
            f"INSERT INTO roomserver_event_types (event_type_nid, event_type) VALUES ({value}, '{key}') ON CONFLICT DO NOTHING")
        conn_dendrite_target.commit()

    return transform_data(conn_synapse, mapping, ['users'], 'roomserver_event_state_keys')
