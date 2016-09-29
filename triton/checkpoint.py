# -*- coding: utf-8 -*-
import os
import logging
import psycopg2.pool
import time

from triton import errors

log = logging.getLogger(__name__)


triton_dsn = os.environ.get('TRITON_DB')
triton_client_name = os.environ.get('TRITON_CLIENT_NAME')
postal_rds_pool = None

CREATE_TABLE_STMT = """
CREATE TABLE IF NOT EXISTS triton_checkpoint (
    client VARCHAR(255) NOT NULL,
    stream VARCHAR(255) NOT NULL,
    shard VARCHAR(255) NOT NULL,
    seq_num VARCHAR(255) NOT NULL,
    updated INTEGER NOT NULL,
    PRIMARY KEY (client, stream, shard))
"""


def get_triton_connection_pool():
    global postal_rds_pool
    if postal_rds_pool is None:
        if not triton_dsn:
            raise errors.TritonCheckpointError(
                'triton_dsn not configured with TRITON_DB evn variable')
        postal_rds_pool = psycopg2.pool.ThreadedConnectionPool(
            1, 20, triton_dsn
        )
    return postal_rds_pool


def init_db(db_pool_function=get_triton_connection_pool):
    """Create the required table in DB if not already there"""
    db_pool = db_pool_function()
    conn = db_pool.getconn()
    curs = conn.cursor()
    curs.execute(CREATE_TABLE_STMT)
    conn.commit()


class TritonCheckpointer(object):
    """Handles checkpoints for triton"""

    def __init__(self, stream_name, client_name=triton_client_name):
        if not client_name:
            raise errors.TritonCheckpointError(
                'client_name is required to create a TritonCheckpointer')
        self.client_name = client_name
        self.stream_name = stream_name
        self.db_pool = get_triton_connection_pool()

        self.checkpoint_exists_sql = (
            "SELECT 1 FROM triton_checkpoint WHERE client=%s "
            "AND stream=%s AND shard=%s"
        )

        self.update_checkpoint_sql = (
            "UPDATE triton_checkpoint SET seq_num=%s, updated=%s"
            " WHERE client=%s AND stream=%s AND shard=%s"
        )

        self.create_checkpoint_sql = (
            "INSERT INTO triton_checkpoint VALUES (%s, %s, %s, %s, %s)"
        )

        self.last_seq_no_sql = (
            "SELECT seq_num FROM triton_checkpoint WHERE "
            "client=%s AND stream=%s AND shard=%s"
        )

    def checkpoint(self, shard_id, sequence_number):
        conn = self.db_pool.getconn()
        try:
            checkpoint_exists = True
            curs = conn.cursor()
            curs.execute(
                self.checkpoint_exists_sql,
                (self.client_name, self.stream_name, shard_id)
            )
            try:
                prev_sequence_number = curs.fetchone()
                log.info('checkpoint: {}'.format(prev_sequence_number))
                if prev_sequence_number is None:
                    checkpoint_exists = False
            except psycopg2.ProgrammingError:
                checkpoint_exists = False

            if checkpoint_exists:
                log.info('Updating checkpoint for {}-{}: {}'.format(
                    self.stream_name, shard_id, sequence_number))
                curs.execute(
                    self.update_checkpoint_sql,
                    (
                        str(sequence_number), time.time(),
                        self.client_name, self.stream_name, shard_id
                    )
                )
            else:
                log.info('creating checkpoint for {}-{}: {}'.format(
                    self.stream_name, shard_id, sequence_number))
                curs.execute(
                    self.create_checkpoint_sql,
                    (
                        self.client_name, self.stream_name, shard_id,
                        str(sequence_number), time.time(),
                    )
                )
        except Exception:
            conn.rollback()
            raise
        else:
            conn.commit()
        finally:
            self.db_pool.putconn(conn)

    def last_sequence_number(self, shard_id):
        conn = self.db_pool.getconn()
        try:
            log.info('getting last_sequence_number for {}-{}'.format(
                self.stream_name, shard_id))
            curs = conn.cursor()
            curs.execute(
                self.last_seq_no_sql,
                (self.client_name, self.stream_name, shard_id)
            )
            try:
                sequence_number = curs.fetchone()
                if sequence_number is None:
                    return  # no checkpoint, we're done
                return sequence_number[0]
            except psycopg2.ProgrammingError:
                return  # no checkpoint, we're done
        except Exception:
            conn.rollback()
            raise
        else:
            conn.commit()
        finally:
            self.db_pool.putconn(conn)
