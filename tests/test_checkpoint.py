# -*- coding: utf-8 -*-
from testify import *
import tempfile
import sqlite3
import mock
import logging
import sys

from triton import checkpoint

level = logging.DEBUG
log_format = "%(asctime)s %(levelname)s:%(name)s: %(message)s"
logging.basicConfig(level=level, format=log_format)

log = logging.getLogger(__name__)


CREATE_TABLE_STMT = """
CREATE TABLE IF NOT EXISTS triton_checkpoint (
    client VARCHAR(255) NOT NULL,
    stream VARCHAR(255) NOT NULL,
    shard VARCHAR(255) NOT NULL,
    seq_num VARCHAR(255) NOT NULL,
    updated INTEGER NOT NULL,
    PRIMARY KEY (client, stream, shard))
"""


class SqlitePool(object):
    def __init__(self, conn):
        self.conn = conn

    def getconn(self):
        return self.conn

    def putconn(self, conn):
        pass


class TritonCheckpointerTest(checkpoint.TritonCheckpointer):
    """patch TritonCheckpointer for sqlite3"""

    def __init__(self, *args):
        super(TritonCheckpointerTest, self).__init__(*args)
        for prop_name in dir(self):
            if prop_name.endswith('_sql'):
                prop = getattr(self, prop_name)
                setattr(self, prop_name, prop.replace('%s', '?'))


class CheckpointTest(TestCase):
    """full end-to-end test case"""

    @setup
    def setup_db(self):
        self.tempfile = tempfile.NamedTemporaryFile()
        self.tempfile_name = self.tempfile.name
        self.conn = sqlite3.connect(self.tempfile_name)
        c = self.conn.cursor()
        c.execute(CREATE_TABLE_STMT)
        self.conn.commit()
        self.pool = SqlitePool(self.conn)

    def test_new_checkpoint(self):
        stream_name = 'test1'
        client_name = 'test_client'
        shard_id = 'shardId-000000000000'
        patch_string = 'triton.checkpoint.get_triton_connection_pool'
        with mock.patch(patch_string, new=lambda: self.pool):
            chkpt = TritonCheckpointerTest(
                client_name, stream_name)
            last = chkpt.last_sequence_number(shard_id)
            assert_truthy(last is None)

            seq_no = '1234'
            chkpt.checkpoint(shard_id, seq_no)
            last = chkpt.last_sequence_number(shard_id)
            assert_truthy(last == seq_no)

            seq_no = '456'
            chkpt.checkpoint(shard_id, seq_no)
            last = chkpt.last_sequence_number(shard_id)
            assert_truthy(last == seq_no)
