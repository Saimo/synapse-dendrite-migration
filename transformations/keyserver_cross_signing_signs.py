import pandas as pd
from util import map_dataframe, generate_sql_insert_statement, transform_data, server_name, get_df_from_table
import json

def extract_user_id(json_string):
    data = json.loads(json_string)
    if "signatures" in data:
        user_id = list(data["signatures"].keys())[0]
        return user_id
    else:
        return None

def extract_key_id(json_string):
    data = json.loads(json_string)
    if "signatures" in data:
        user_id = list(data["signatures"].keys())[0]
        target_key_id = list(data["signatures"][user_id].keys())[0]
        return target_key_id
    else:
        return None

def extract_key(json_string):
    data = json.loads(json_string)
    if "keys" in data:
        key_id = list(data["keys"].keys())[0]
        return key_id
    else:
        return None

def extract_signature_value(json_string):
    data = json.loads(json_string)
    if "signatures" in data:
        user_id = list(data["signatures"].keys())[0]
        key_id = list(data["signatures"][user_id].keys())[0]
        return data["signatures"][user_id][key_id]
    else:
        return None

mapping = {
    'origin_user_id': ('user_id', lambda x: '@' + x[1:].split(":", 1)[0] + f":{server_name}"),
    'origin_key_id': ('key_id', str),
    'target_user_id': ('target_user_id', lambda x: '@' + x[1:].split(":", 1)[0] + f":{server_name}"),
    'target_key_id': ('target_key_id', str),
    'signature': ('signature', str),
}


def transform_keyserver_cross_signing_signs(conn_synapse):
    cursor_synapse = conn_synapse.cursor()

    signing_keys_df = get_df_from_table(cursor_synapse, 'e2e_cross_signing_keys')

    signing_keys_df['key_id'] = signing_keys_df['keydata'].apply(lambda x: extract_key_id(x))

    signing_keys_df['target_key_id'] = signing_keys_df['keydata'].apply(lambda x: extract_key(x))
    signing_keys_df['signature'] = signing_keys_df['keydata'].apply(lambda x: extract_signature_value(x))
    signing_keys_df['target_user_id'] = signing_keys_df['keydata'].apply(lambda x: extract_user_id(x))

    df = map_dataframe(signing_keys_df, signing_keys_df.columns.tolist(), mapping)

    insert_statements = (generate_sql_insert_statement(df, 'keyserver_cross_signing_sigs'))

    return insert_statements
