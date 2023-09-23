def transform(data):
    # Transform access_tokens data to userapi_devices format and generate INSERT INTO statements
    transformed_data = []

    for row in data:
        localpart = row[0][1:].split(":", 1)[0]  # Extract localpart from the first column
        server_name = row[0][1:].split(":", 1)[1]  # Extract server_name from the first column
        display_name = row[2]  # display_name
        avatar_url = row[3]  # avatar_url

        transformed_row = (
            localpart,
            server_name,
            created_ts,
            password_hash,
            appservice_id,
            is_deactivated,
            account_type,
        )
        insert_statement = f"""
                    INSERT INTO userapi_accounts 
                        VALUES ({localpart!r}, {server_name!r}, {created_ts}, {password_hash!r}, {appservice_id}, {is_deactivated}, {account_type})
                    ON CONFLICT DO NOTHING
                """
        transformed_data.append(insert_statement)

    return transformed_data