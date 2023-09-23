import numpy as np
import pandas as pd
import json
import re
import psycopg2
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

from util import map_dataframe, generate_sql_insert_statement, get_df_from_table, get_nid_from_table, server_name


def event_state_key_nid_ow(row, state_events_ids):
    if row['event_id'] in state_events_ids:
        if row['state_key'] != '':
            if row['state_key'] not in event_state_keys_mapping:
                cursor_target.execute(
                    f"INSERT INTO roomserver_event_state_keys (event_state_key) VALUES ('{row['state_key']}') ON CONFLICT (event_state_key) DO UPDATE SET event_state_key = EXCLUDED.event_state_key RETURNING event_state_key_nid;"
                )
                conn_dendrite_target.commit()
                event_state_keys_mapping[row['state_key']] = cursor_target.fetchone()[0]

            return event_state_keys_mapping[row['state_key']]
        else:
            return 1
    return 0


def parallel_apply(df, func, num_threads, **kwargs):
    # Split the DataFrame into equal parts based on the number of threads
    df_split = np.array_split(df, num_threads)

    # Create a list of tqdm progress bars, one for each thread
    progress_bars = [tqdm(total=len(df_part), position=i, desc=f'Thread {i}', leave=True)
                     for i, df_part in enumerate(df_split)]

    # Define a wrapper function that updates the progress bar for the corresponding thread
    def apply_with_progress(df_part, progress_bar):
        result = func(df_part, **kwargs)
        progress_bar.update(len(df_part))
        return result

    # Create a ThreadPoolExecutor and run the apply function on each part of the DataFrame with its progress bar
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(apply_with_progress, df_part, progress_bar)
                   for df_part, progress_bar in zip(df_split, progress_bars)]

        results = [future.result() for future in as_completed(futures)]

    # Close the progress bars
    for progress_bar in progress_bars:
        progress_bar.close()

    print("Finished parallel apply")

    # Concatenate the results back into a single DataFrame
    return pd.concat(results, ignore_index=True)


def format_auth_json(auth_nids):
    if any(pd.isna(x) for x in auth_nids):
        return '{}'
    return '{' + ', '.join(str(int(x)) for x in auth_nids) + '}'


room_events_mapping = {}

event_state_keys_mapping = {}

conn_dendrite_target = psycopg2.connect(
    dbname="dendrite",
    user="dendrite_user",
    password="dendrite_password",
    host="localhost",
    port="5432"
)

cursor_target = conn_dendrite_target.cursor()

mapping = {
    'event_nid': ('event_nid', int),
    'room_nid': ('room_id', lambda x: get_nid_from_table(cursor_target, 'roomserver_rooms', 'room_id',
                                                         re.sub(r':\S*', f":{server_name}", x), 'room_nid')),
    'event_type_nid': ('type', lambda x: room_events_mapping[x]),
    'event_state_key_nid': ('event_state_key_nid_ow', lambda x: 1 if x != x else x),
    'sent_to_output': ('type', lambda x: True),
    'depth': ('depth', int),
    'event_id': ('event_id', str),
    # 'auth_event_nids': ('type', lambda x: json.dumps({})),
    'auth_event_nids': ('auth_json', str),
    'is_rejected': ('type', lambda x: False),
}


def transform_roomserver_events(conn_synapse):
    cursor_synapse = conn_synapse.cursor()

    # get room_events_mapping from roomserver_event_state_keys
    cursor_target.execute("SELECT event_type_nid, event_type FROM roomserver_event_types")
    for row in cursor_target.fetchall():
        room_events_mapping[row[1]] = row[0]

    # get event_state_keys_mapping from roomserver_event_state_keys
    cursor_target.execute("SELECT event_state_key_nid, event_state_key FROM roomserver_event_state_keys")
    for row in cursor_target.fetchall():
        event_state_keys_mapping[row[1]] = row[0]

    events_df = get_df_from_table(cursor_synapse, 'events')[['event_id', 'room_id', 'type', 'state_key', 'depth']]
    event_json_df = get_df_from_table(cursor_synapse, 'event_json')[['event_id']]
    event_auth_df = get_df_from_table(cursor_synapse, 'event_auth')[['event_id', 'auth_id']]
    state_events_df = get_df_from_table(cursor_synapse, 'state_events')[['event_id']]

    df = pd.concat([events_df, event_json_df]).drop_duplicates(subset=['event_id']).reset_index(drop=True)

    df['event_nid'] = range(1, len(df) + 1)

    event_auths = event_auth_df.merge(df[['event_id', 'event_nid']], left_on='auth_id', right_on='event_id',
                                      suffixes=('', '_nid'))
    event_auths.drop(columns=['event_id_nid', 'auth_id'], inplace=True)
    event_auths.rename(columns={'event_nid': 'auth_nid'}, inplace=True)

    merged_df = df.merge(event_auths, on='event_id', how='left')

    # Group the merged DataFrame by the event_id column and aggregate the auth_nid's into a list
    grouped_df = merged_df.groupby('event_id')['auth_nid'].apply(list).reset_index()

    # Convert the auth_nid list to a string in the desired format
    grouped_df['auth_json'] = grouped_df['auth_nid'].apply(format_auth_json)

    # Join the grouped information back to the original df
    df = df.merge(grouped_df[['event_id', 'auth_json']], on='event_id', how='left')

    state_events_ids = set(state_events_df['event_id'].values)

    def apply_event_state_key_nid_ow(df_part):
        return df_part.apply(lambda row: event_state_key_nid_ow(row, state_events_ids), axis=1)

    num_threads = 24  # Adjust this value based on the number of available CPU cores
    df['event_state_key_nid_ow'] = parallel_apply(df, apply_event_state_key_nid_ow, num_threads)

    df = map_dataframe(df, df.columns.tolist(), mapping)

    insert_statements = (generate_sql_insert_statement(df, 'roomserver_events'))

    return insert_statements
