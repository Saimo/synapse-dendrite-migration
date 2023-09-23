import pandas as pd
from tqdm import tqdm

overwrite_mapping = {
    'server_name': 'your_servername',
}

server_name = 'your_servername'

thread_amount = 8

def escape_pg_value(value):
    """Escape a value for PostgreSQL queries"""
    if isinstance(value, str):
        return value.replace("'", "''")
    else:
        return str(value)

def map_dataframe(data, column_names, mapping):
    df = pd.DataFrame(data, columns=column_names)

    for new_col, (old_col, func) in mapping.items():
        if old_col is not None:
            df[new_col] = df[old_col].apply(func)
            if new_col in overwrite_mapping:
                df[new_col] = overwrite_mapping[new_col]

    for new_col, (_, func) in _filter_mapping_by_none(mapping).items():
        df[new_col] = df[new_col].apply(func)

    new_columns = list(mapping.keys()) + [col for col in column_names if col in mapping]
    # remove duplicates
    new_columns = list(dict.fromkeys(new_columns))
    new_df = df[new_columns]

    return new_df


def generate_sql_insert_statement(df, table_name):
    columns = ', '.join(df.columns)
    values = []
    statements = []

    # Wrap the df.iterrows() with tqdm for the progress bar
    for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Generating SQL statements"):
        row_values = ', '.join([
            "NULL" if value in ('None', None, 'NaN', 'nan') or value != value
            else (f"{value}" if 'nextval' in str(value)
                  else (
                "NaN" if pd.isna(value) else (f"'{value}'" if isinstance(value, str) else str(value))
            ))
            for value in row
        ])
        values.append(f"({row_values})")

        if (index + 1) % 500 == 0:
            values_str = ',\n'.join(values)
            sql = f"INSERT INTO {table_name} ({columns})\nVALUES\n{values_str} ON CONFLICT DO NOTHING;"
            statements.append(sql)
            values = []

    if len(values) > 0:
        values_str = ',\n'.join(values)
        sql = f"INSERT INTO {table_name} ({columns})\nVALUES\n{values_str} ON CONFLICT DO NOTHING;"
        statements.append(sql)

    return statements


def get_df_from_table(cursor, table_name, where=None):
    query = f"SELECT * FROM {table_name}"

    if where:
        query += f" WHERE {where}"

    query += ";"

    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    return pd.DataFrame(data, columns=columns)


def transform_data(conn_synapse, mapping, source_table_names, target_table_name, merge_on=None):
    cursor_synapse = conn_synapse.cursor()

    source_data_df = None

    # check source_table_names length
    if len(source_table_names) == 1:
        cursor_synapse.execute(f"SELECT * FROM {source_table_names[0]};")
        source_data_columns = [desc[0] for desc in cursor_synapse.description]
        source_data_data = cursor_synapse.fetchall()
        source_data_df = pd.DataFrame(source_data_data, columns=source_data_columns)
    if len(source_table_names) == 2:
        # iterate through source_table_names
        df_list = []
        for source_table_name in source_table_names:
            cursor_synapse.execute(f"SELECT * FROM {source_table_name};")
            source_data_columns = [desc[0] for desc in cursor_synapse.description]
            source_data_data = cursor_synapse.fetchall()
            source_data_df = pd.DataFrame(source_data_data, columns=source_data_columns)
            df_list.append(source_data_df)
        # join access_tokens and devices tables on device_id
        source_data_df = pd.merge(df_list[0], df_list[1], on=merge_on, how='left', suffixes=('', '_y'))

    df = map_dataframe(source_data_df, source_data_df.columns.tolist(), mapping)

    insert_statements = generate_sql_insert_statement(df, target_table_name)

    return insert_statements


def get_nid_from_table(cursor, table_name, column_name, column_value, nid_column_name):
    cursor.execute(f"SELECT * FROM {table_name} WHERE {column_name} = '{column_value}';")
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)
    return df[nid_column_name].values[0]


def _filter_mapping_by_none(mapping_dict):
    return {k: v for k, v in mapping_dict.items() if v[0] is None}
