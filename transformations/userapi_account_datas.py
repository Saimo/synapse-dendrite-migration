import json
import re
import pandas as pd
from util import map_dataframe, generate_sql_insert_statement, server_name


mapping = {
    'localpart': ('user_id', lambda x: x[1:].split(":", 1)[0]),
    'server_name': ('user_id', lambda x: x[1:].split(":", 1)[1]),
    'room_id': ('room_id', lambda x: x if x else ""),
    'type': ('account_data_type', str),
    # 'content': ('content', lambda x: re.sub(r':SERVER_NAME_BEFORE', f":{server_name}", x)), # needs to be adjusted accordingly
}


def transform_userapi_account_datas(conn_synapse):
    cursor_synapse = conn_synapse.cursor()

    cursor_synapse.execute(f"SELECT * FROM account_data;")
    account_data_columns = [desc[0] for desc in cursor_synapse.description]
    account_data_data = cursor_synapse.fetchall()
    account_data_df = pd.DataFrame(account_data_data, columns=account_data_columns)

    cursor_synapse.execute(f"SELECT * FROM room_account_data;")
    room_account_data_columns = [desc[0] for desc in cursor_synapse.description]
    room_account_data_data = cursor_synapse.fetchall()
    room_account_data_df = pd.DataFrame(room_account_data_data, columns=room_account_data_columns)

    # join access_tokens and devices tables on device_id
    source_data = pd.concat([account_data_df, room_account_data_df])

    df = map_dataframe(source_data, source_data.columns.tolist(), mapping)

    insert_statements = (generate_sql_insert_statement(df, 'userapi_account_datas'))

    return insert_statements
