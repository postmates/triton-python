# -*- coding: utf-8 -*-
from testify import *
import tempfile
import sqlite3
import mock
import msgpack
import base64

from triton import checkpoint, stream

CLIENT_NAME = 'test_client'


class SqlitePool(object):
    def __init__(self, conn):
        self.conn = conn

    def getconn(self):
        return self.conn

    def putconn(self, conn):
        pass


class TritonCheckpointerTest(checkpoint.TritonCheckpointer):
    """patch TritonCheckpointer for sqlite3"""

    def __init__(self, *args, **kwargs):
        if not kwargs.get('client_name'):
            kwargs['client_name'] = CLIENT_NAME
        super(TritonCheckpointerTest, self).__init__(*args, **kwargs)
        for prop_name in dir(self):
            if prop_name.endswith('_sql'):
                prop = getattr(self, prop_name)
                setattr(self, prop_name, prop.replace('%s', '?'))


class CheckpointTest(TestCase):
    """Test checkpoint functionality"""

    @setup
    def setup_db(self):
        self.tempfile = tempfile.NamedTemporaryFile()
        self.tempfile_name = self.tempfile.name
        self.conn = sqlite3.connect(self.tempfile_name)
        self.pool = SqlitePool(self.conn)
        checkpoint.init_db(lambda: self.pool)

    def test_new_checkpoint(self):
        stream_name = 'test1'
        shard_id = 'shardId-000000000000'
        patch_string = 'triton.checkpoint.get_triton_connection_pool'
        with mock.patch(patch_string, new=lambda: self.pool):
            chkpt = TritonCheckpointerTest(
                stream_name, client_name=CLIENT_NAME)
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


def generate_raw_record(seq_no):
    data = base64.b64encode(msgpack.packb({'value': True}))

    raw_record = {'SequenceNumber': seq_no, 'Data': data}

    return raw_record


def get_records(n, offset=0, *args, **kwargs):
    n = int(n)
    offset = int(offset)
    records = [generate_raw_record(str(i + offset)) for i in range(n)]
    return {
        'NextShardIterator': n + offset,
        'MillisBehindLatest': 0,
        'Records': records
    }


def get_batches_of_10_records(
    iter_val, *args, **kwargs
):
    recs = get_records(10, iter_val)
    return recs


class StreamIteratorCheckpointTest(TestCase):
    """Test end to end checkpoint functionality"""

    @setup
    def setup_db(self):
        self.tempfile = tempfile.NamedTemporaryFile()
        self.tempfile_name = self.tempfile.name
        self.conn = sqlite3.connect(self.tempfile_name)
        self.pool = SqlitePool(self.conn)
        checkpoint.init_db(lambda: self.pool)

    def test_iterator_checkpoint(self):
        stream_name = 'test1'
        shard_id = 'shardId-000000000000'
        pool_patch = 'triton.checkpoint.get_triton_connection_pool'
        chkpt_patch = 'triton.stream.TritonCheckpointer'
        with mock.patch(pool_patch, new=lambda: self.pool):
            with mock.patch(chkpt_patch, new=TritonCheckpointerTest):
                s = turtle.Turtle()
                s.name = stream_name

                def get_20_records(*args, **kwargs):
                    return get_records(20)

                s.conn.get_records = get_20_records

                i = stream.StreamIterator(s, shard_id, stream.ITER_TYPE_LATEST)
                i._iter_value = 1

                for j in range(10):
                    val = i.next()
                i.checkpoint()

                last_chkpt_seqno = i.checkpointer.last_sequence_number(
                    shard_id)
                assert_truthy(val.seq_num == last_chkpt_seqno)

    def test_combined_iterator_checkpoint(self):
        stream_name = 'test2'
        shard_id0 = 'shardId-000000000000'
        shard_id1 = 'shardId-000000000001'
        pool_patch = 'triton.checkpoint.get_triton_connection_pool'
        chkpt_patch = 'triton.stream.TritonCheckpointer'
        with mock.patch(pool_patch, new=lambda: self.pool):
            with mock.patch(chkpt_patch, new=TritonCheckpointerTest):
                s = turtle.Turtle()
                s.name = stream_name
                s.conn.get_records = get_batches_of_10_records

                i1 = stream.StreamIterator(
                    s, shard_id0, stream.ITER_TYPE_LATEST)
                i1._iter_value = 0
                i2 = stream.StreamIterator(
                    s, shard_id1, stream.ITER_TYPE_LATEST)
                i2._iter_value = 100

                combined_i = stream.CombinedStreamIterator([i1, i2])

                for j in range(25):
                    val = combined_i.next()
                combined_i.checkpoint()

                last_chkpts = [
                    this_i.checkpointer.last_sequence_number(this_i.shard_id)
                    for this_i in [i1, i2]
                ]

                assert_truthy(val.seq_num in last_chkpts)
                # because combined iterator uses a set of iterators,
                # we neet to test both cases
                if int(val.seq_num) > 100:
                    assert_truthy('9' in last_chkpts)
                else:
                    assert_truthy('109' in last_chkpts)

    def test_stream_get_iterator_from_checkpoint(self):
        stream_name = 'test3'
        shard_id = 'shardId-000000000000'
        pool_patch = 'triton.checkpoint.get_triton_connection_pool'
        chkpt_patch = 'triton.stream.TritonCheckpointer'
        with mock.patch(pool_patch, new=lambda: self.pool):
            with mock.patch(chkpt_patch, new=TritonCheckpointerTest):
                c = turtle.Turtle()
                s = stream.AWSStream(stream_name, 'p_key', conn=c)
                s._shard_ids = [shard_id, ]

                s.conn.get_records = get_batches_of_10_records

                def get_shard_iterator(*args, **kwargs):
                    return {'ShardIterator': 0}

                s.conn.get_shard_iterator = get_shard_iterator

                ci = s.build_iterator_from_checkpoint()
                for j in range(10):
                    val = ci.next()
                i = ci.iterators[0]
                assert_truthy(i.iterator_type == i.fallback_iterator_type)
                ci.checkpoint()

                ci = s.build_iterator_from_checkpoint()
                i = ci.iterators[0]
                i._iter_value = i.checkpointer.last_sequence_number(
                    shard_id)
                assert_truthy(val.seq_num == i._iter_value)
                for j in range(10):
                    val = ci.next()
                #NOTE: iter_value function is not being called since
                #we explicitly set the iterator above
                #https://github.com/postmates/triton-python/commit/7441b5d46d639e7ee2783c43b6a0bfd4adb6c0d6#diff-c1624f14b4890c5253e57cdf3fee2366R92
                #TODO: rewrite test to properly instantiate iterator
                #assert_truthy(
                #    i.iterator_type == stream.ITER_TYPE_FROM_SEQNUM)
