import psycopg2
from util import thread_amount
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

conn_dendrite_target = psycopg2.connect(
    dbname="dendrite",
    user="dendrite_user",
    password="dendrite_password",
    host="localhost",
    port="5432"
)

# Connect to synapse database
conn_synapse = psycopg2.connect(
    dbname="synapse",
    user="synapse_user",
    password="dendrite_password",
    host="localhost",
    port="5432"
)

table_database_mapping = {
    'userapi_devices': 'userapi',
    'userapi_accounts': 'userapi',
}


def execute_sql_statement(statement):
    cursor_migration_dendrite = conn_dendrite_target.cursor()
    cursor_migration_dendrite.execute(statement)
    conn_dendrite_target.commit()


def migrate_data(transformation_function):
    insert_statements = transformation_function(conn_synapse)

    with ThreadPoolExecutor(max_workers=thread_amount) as executor:
        # Wrap the statements in tqdm to display the progress bar
        sql_statements_with_progress = tqdm(insert_statements, total=len(insert_statements),
                                            desc="Executing SQL statements")

        # Submit the statements for execution
        futures = [executor.submit(execute_sql_statement, statement) for statement in sql_statements_with_progress]

        # Wait for the results to complete and update the progress bar
        for future in as_completed(futures):
            sql_statements_with_progress.update(1)
