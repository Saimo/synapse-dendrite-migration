import pandas as pd
from util import map_dataframe, generate_sql_insert_statement, transform_data, server_name

mapping = {
    'room_id': ('room_id', lambda x: '!' + x[1:].split(":", 1)[0] + f":{server_name}"),
    'room_version': ('room_version', lambda x: 10),
}


def transform_roomserver_rooms(conn_synapse):
    return transform_data(conn_synapse, mapping, ['rooms'], 'roomserver_rooms')