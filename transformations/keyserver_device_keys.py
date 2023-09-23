import pandas as pd
from util import map_dataframe, generate_sql_insert_statement, transform_data, server_name

mapping = {
    'user_id': ('user_id', lambda x: '@' + x[1:].split(":", 1)[0] + f":{server_name}"),
    'device_id': ('device_id', str),
    'ts_added_secs': ('ts_added_ms', lambda x: x / 1000),
    'key_json': ('key_json', str),
    'stream_id': ('ts_added_ms', lambda x: str('nextval(\'keyserver_key_changes_seq\')')),
}


def transform_keyserver_device_keys(conn_synapse):
    return transform_data(conn_synapse, mapping, ['e2e_device_keys_json'], 'keyserver_device_keys')