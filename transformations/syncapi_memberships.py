import pandas as pd
from util import map_dataframe, generate_sql_insert_statement, transform_data, server_name

mapping = {
    'room_id': ('room_id', lambda x: '!' + x[1:].split(":", 1)[0] + f":{server_name}"),
    'user_id': ('user_id', lambda x: '@' + x[1:].split(":", 1)[0] + f":{server_name}"),
    'membership': ('membership', str),
    'event_id': ('event_id', str),
    'stream_pos': ('stream_ordering', int),
    'topological_pos': ('topological_ordering', int),
}


def transform_syncapi_memberships(conn_synapse):
    return transform_data(conn_synapse, mapping, ['room_memberships', 'events'], 'syncapi_memberships')