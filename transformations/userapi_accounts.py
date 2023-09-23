import pandas as pd
from util import map_dataframe, generate_sql_insert_statement

mapping = {
    'localpart': ('name', lambda x: x[1:].split(":", 1)[0]),
    'server_name': ('name', lambda x: x[1:].split(":", 1)[1]),
    'created_ts': ('creation_ts', int),
    'password_hash': ('password_hash', str),
    'appservice_id': ('appservice_id', str),
    'is_deactivated': ('approved', lambda x: not bool(x)),
    'account_type': ('user_type', lambda x: 1 if x is None else x),
}


def transform_userapi_accounts(conn_synapse):
    cursor_synapse = conn_synapse.cursor()

    cursor_synapse.execute(f"SELECT * FROM users;")
    users_columns = [desc[0] for desc in cursor_synapse.description]
    users_data = cursor_synapse.fetchall()
    users_df = pd.DataFrame(users_data, columns=users_columns)


    df = map_dataframe(users_df, users_df.columns.tolist(), mapping)

    insert_statements = (generate_sql_insert_statement(df, 'userapi_accounts'))

    return insert_statements
