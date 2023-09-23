import pandas as pd

from util import map_dataframe, generate_sql_insert_statement, escape_pg_value, server_name


mapping = {
    'localpart': ('user_id', str),
    'server_name': ('user_id', lambda x: x + f":{server_name}"),
    'display_name': ('displayname', escape_pg_value),
    'avatar_url': ('avatar_url', lambda x: x if x else ""),
}


def transform_userapi_profiles(conn_synapse):
    cursor_synapse = conn_synapse.cursor()

    cursor_synapse.execute(f"SELECT * FROM profiles;")
    profiles_columns = [desc[0] for desc in cursor_synapse.description]
    profiles_data = cursor_synapse.fetchall()
    profiles_df = pd.DataFrame(profiles_data, columns=profiles_columns)


    df = map_dataframe(profiles_df, profiles_df.columns.tolist(), mapping)

    insert_statements = (generate_sql_insert_statement(df, 'userapi_profiles'))

    return insert_statements
