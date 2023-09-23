import pandas as pd
from util import map_dataframe, generate_sql_insert_statement

mapping = {
    'access_token': ('token', str),
    'session_id': ('id', int),
    'device_id': ('device_id', str),
    'localpart': ('user_id', lambda x: x[1:].split(":", 1)[0]),
    'server_name': ('user_id', lambda x: x[1:].split(":", 1)[1]),
    'created_ts': ('last_validated', int),
    'display_name': ('display_name', str),
    # TODO: Last seen ts update
    'last_seen_ts': ('last_seen', lambda x: 1685681573 if pd.isna(x) else x),
    'ip': ('ip', str),
    'user_agent': ('user_agent', str),
}


def transform_userapi_devices(conn_synapse):
    cursor_synapse = conn_synapse.cursor()

    cursor_synapse.execute(f"SELECT * FROM access_tokens;")
    access_token_columns = [desc[0] for desc in cursor_synapse.description]
    access_token_data = cursor_synapse.fetchall()
    access_token_df = pd.DataFrame(access_token_data, columns=access_token_columns)

    cursor_synapse.execute(f"SELECT * FROM devices;")
    devices_columns = [desc[0] for desc in cursor_synapse.description]
    devices_data = cursor_synapse.fetchall()
    devices_df = pd.DataFrame(devices_data, columns=devices_columns)

    # join access_tokens and devices tables on device_id
    source_data = pd.merge(access_token_df, devices_df, on='device_id', how='left', suffixes=('', '_y'))

    # drop entries where hidden is true
    source_data = source_data[source_data['hidden'] == 0]

    df = map_dataframe(source_data, source_data.columns.tolist(), mapping)

    insert_statements = (generate_sql_insert_statement(df, 'userapi_devices'))

    return insert_statements
