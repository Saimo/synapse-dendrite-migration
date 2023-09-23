import json
import re
import pandas as pd
from psycopg2.pool import SimpleConnectionPool

from util import map_dataframe, generate_sql_insert_statement, server_name

import psycopg2
from psycopg2.extras import execute_values
from concurrent.futures import ThreadPoolExecutor, as_completed


def migrate_room(room_nid, pool):
    with pool.getconn() as conn:
        try:
            room_blocks = []

            with conn.cursor() as cur:
                cur.execute("SELECT event_nid FROM roomserver_events WHERE room_nid = %s ORDER BY event_nid;",
                            (room_nid,))
                event_nids = [row[0] for row in cur.fetchall()]

            if not event_nids:
                print(f"MIGRATION: No Events for migration in room_nid {room_nid}")
                pool.putconn(conn)
                return


            event_nids = sorted(set(event_nids))

            for i, event_nid in enumerate(event_nids):

                local_event_nids = [event_nid]

                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO roomserver_state_block (state_block_hash, event_nids)
                        VALUES (%s, %s)
                        ON CONFLICT (state_block_hash) DO UPDATE SET event_nids = %s
                        RETURNING state_block_nid
                    """, (str(hash(tuple(local_event_nids))), local_event_nids, local_event_nids))
                    block_nid = cur.fetchone()[0]

                newsnapshot = None
                if i != 0:
                    room_blocks.append(block_nid)

                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO roomserver_state_snapshots (state_snapshot_hash, room_nid, state_block_nids)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (state_snapshot_hash) DO UPDATE SET room_nid = %s
                            RETURNING state_snapshot_nid
                        """, (str(hash(tuple(room_blocks))), room_nid, room_blocks, room_nid))
                        newsnapshot = cur.fetchone()[0]

                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE roomserver_events SET state_snapshot_nid = %s WHERE room_nid = %s AND event_nid = %s",
                            (newsnapshot, room_nid, event_nid))

                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE roomserver_rooms SET state_snapshot_nid = %s, latest_event_nids = %s, last_event_sent_nid = %s WHERE room_nid = %s",
                            (newsnapshot, local_event_nids, event_nid, room_nid))
                else:
                    empty_array = []

                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO roomserver_state_snapshots (state_snapshot_hash, room_nid, state_block_nids)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (state_snapshot_hash) DO UPDATE SET room_nid = %s
                            RETURNING state_snapshot_nid
                        """, (str(hash(tuple(room_blocks))), room_nid, empty_array, room_nid))
                        newsnapshot = cur.fetchone()[0]

                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE roomserver_events SET state_snapshot_nid = %s WHERE room_nid = %s AND event_nid = %s",
                            (newsnapshot, room_nid, event_nid))

                    with conn.cursor() as cur:
                        cur.execute("UPDATE roomserver_rooms SET state_snapshot_nid = %s WHERE room_nid = %s",
                                    (newsnapshot, room_nid))

            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            pool.putconn(conn)


def migrate_synapse(pool):
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT rr.room_nid FROM roomserver_rooms rr INNER JOIN roomserver_events re ON re.room_nid = rr.room_nid WHERE rr.state_snapshot_nid = 0 GROUP BY rr.room_nid;")
            room_nids = [row[0] for row in cur.fetchall()]
            cur.close()
    finally:
        pool.putconn(conn)

    num_threads = 2

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(migrate_room, room_nid, pool) for room_nid in room_nids]
        for future in as_completed(futures):
            future.result()


conn_dendrite_target = SimpleConnectionPool(
    minconn=1,
    maxconn=25,
    dbname="dendrite",
    user="dendrite_user",
    password="dendrite_password",
    host="localhost",
    port="5432"
)

def transform_snapshot(conn_synapse):
    migrate_synapse(conn_dendrite_target)

    return []
