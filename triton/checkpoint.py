# -*- coding: utf-8 -*-
import os
import logging
import psycopg2.pool
import time

log = logging.getLogger(__name__)


triton_dsn = os.environ.get('TRITON_DB')
postal_rds_pool = None


def get_triton_connection_pool():
    global postal_rds_pool
    if postal_rds_pool is None:
        postal_rds_pool = psycopg2.pool.ThreadedConnectionPool(
            1, 20, triton_dsn
        )
    return postal_rds_pool


class TritonCheckpointer(object):
    """Handles checkpoints for triton"""

    def __init__(self, client_name, stream_name):
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
                log.info(sequence_number)
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
