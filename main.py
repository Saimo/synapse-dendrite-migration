import os

from migration import migrate_data
from transformations.roomserver_event_state_keys import transform_roomserver_event_state_keys
from transformations.userapi_devices import transform_userapi_devices
from transformations.userapi_accounts import transform_userapi_accounts
from transformations.userapi_account_datas import transform_userapi_account_datas
from transformations.userapi_profiles import transform_userapi_profiles
from transformations.syncapi_output_room_events import transform_output_room_events
from transformations.syncapi_memberships import transform_syncapi_memberships
from transformations.roomserver_rooms import transform_roomserver_rooms
from transformations.roomserver_memberships import transform_roomserver_memberships
from transformations.syncapi_current_room_state import transform_syncapi_current_room_state
from transformations.keyserver_device_keys import transform_keyserver_device_keys
from transformations.keyserver_cross_signing_signs import transform_keyserver_cross_signing_signs
from transformations.roomserver_event_json import transform_roomserver_event_json
from transformations.snapshot_handling import transform_snapshot

# Needs to be called before memberships
from transformations.roomserver_events import transform_roomserver_events
from transformations.keyserver_cross_signing_keys import transform_keyserver_cross_signing_keys

import prettytable
from prettytable import PrettyTable


def update_progress_table_status(progress_table, table_name, status):
    for idx, row in enumerate(progress_table._rows):
        if row[0] == table_name:
            updated_row = list(row)
            updated_row[1] = status
            progress_table.del_row(idx)
            progress_table.add_row(updated_row)
            break


def clear_console():
    os.system('cls' if os.name == 'nt' else 'clear')


table_transformations = [
    # transform_userapi_devices,
    # transform_userapi_accounts,
    # transform_userapi_account_datas,
    # transform_userapi_profiles,
    # transform_output_room_events,
    # transform_roomserver_rooms,
    # transform_roomserver_event_state_keys,
    # transform_roomserver_events,
    # transform_syncapi_memberships,
    # transform_syncapi_current_room_state,
    # transform_roomserver_event_json,
    # transform_keyserver_device_keys,
    # transform_roomserver_memberships,
    # transform_keyserver_cross_signing_signs,
    # transform_keyserver_cross_signing_keys,
    transform_snapshot
]

# Initialize a pretty table
progress_table = PrettyTable()
progress_table.field_names = ["Table", "Status"]

# Add all transformations to the table with the initial status
for transformation in table_transformations:
    progress_table.add_row([transformation.__name__, "Pending"])

print(progress_table)

# Iterate through the table_transformations
for transformation in table_transformations:
    # Update the status in the progress table
    update_progress_table_status(progress_table, transformation.__name__, "In Progress")

    # Clear the console output and print the updated table
    clear_console()
    print(progress_table)

    # Perform the transformation
    print("Transforming table: " + transformation.__name__)
    migrate_data(transformation)

    # Update the status in the progress table
    update_progress_table_status(progress_table, transformation.__name__, "\033[92mDone\033[0m")

    # Clear the console output and print the updated table
    clear_console()
    print(progress_table)