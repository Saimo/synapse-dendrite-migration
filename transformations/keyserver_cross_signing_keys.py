import pandas as pd
import psycopg2

from util import map_dataframe, generate_sql_insert_statement, transform_data, server_name, get_df_from_table
import json


def extract_key_id(json_string):
    data = json.loads(json_string)
    if "signatures" in data:
        user_id = list(data["signatures"].keys())[0]
        target_key_id = list(data["signatures"][user_id].keys())[0]
        return target_key_id
    else:
        return None


import json

def extract_key(json_string):
    data = json.loads(json_string)
    if "keys" in data:
        key_id = list(data["keys"].keys())[0]
        return key_id
    else:
        return None

mapping = {
    'user_id': ('user_id', str),
    'key_type': ('keytype', lambda x: 1 if x == 'master' else 2 if x == 'self_signing' else 3 if x == 'user_signing' else 4),
    'key_data': ('key_id', lambda x: x.split(":", 1)[1]),
}

def transform_keyserver_cross_signing_keys(conn_synapse):
    cursor_synapse = conn_synapse.cursor()

    signing_keys_df = get_df_from_table(cursor_synapse, 'e2e_cross_signing_keys')
    signing_signatures_df = get_df_from_table(cursor_synapse, 'e2e_cross_signing_signatures')

    signing_keys_df['target_key_id'] = signing_keys_df['keydata'].apply(lambda x: extract_key_id(x))

    signing_keys_df['key_id'] = signing_keys_df['keydata'].apply(lambda x: extract_key(x))

    signing_keys_df = signing_keys_df.merge(signing_signatures_df, on=['user_id', 'key_id'], how='left')

    # drop rows where target_key_id has no ':' in it
    signing_keys_df = signing_keys_df[signing_keys_df['target_key_id'].str.contains(":")]

    df = map_dataframe(signing_keys_df, signing_keys_df.columns.tolist(), mapping)

    insert_statements = (generate_sql_insert_statement(df, 'keyserver_cross_signing_keys'))

    return insert_statements
