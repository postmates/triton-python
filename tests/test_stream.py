from testify import *
import base64
import time

import msgpack

from triton import stream
from triton import errors


def generate_raw_record():
    data = base64.b64encode(msgpack.packb({'value': True}))

    raw_record = {'SequenceNumber': 1, 'Data': data}

    return raw_record


class RecordTest(TestCase):

    def test_from_raw_record(self):
        raw_record = generate_raw_record()

        r = stream.Record.from_raw_record(0, raw_record)
        assert_equal(r.seq_num, 1)
        assert_equal(r.shard_id, 0)
        assert_equal(r.data['value'], True)


class StreamIteratorTest(TestCase):

    def test_iter_value(self):
        s = turtle.Turtle()

        def get_shard_iterator(*args, **kwargs):
            return {'ShardIterator': 1}

        s.conn.get_shard_iterator = get_shard_iterator

        i = stream.StreamIterator(s, 0, stream.ITER_TYPE_LATEST)

        assert_equal(i.iter_value, 1)

    def test_fill_empty(self):
        s = turtle.Turtle()

        def get_records(*args, **kwargs):
            return {
                'NextShardIterator': 2,
                'MillisBehindLatest': 0,
                'Records': []
            }

        s.conn.get_records = get_records

        i = stream.StreamIterator(s, 0, stream.ITER_TYPE_LATEST)
        i._iter_value = 1

        i.fill()

        assert_equal(i.iter_value, 2)
        assert_equal(i.records, [])

    def test_fill_records(self):
        raw_record = generate_raw_record()
        s = turtle.Turtle()

        def get_records(*args, **kwargs):
            return {
                'NextShardIterator': 2,
                'MillisBehindLatest': 0,
                'Records': [raw_record]
            }

        s.conn.get_records = get_records

        i = stream.StreamIterator(s, 0, stream.ITER_TYPE_LATEST)
        i._iter_value = 1

        i.fill()

        assert not i._empty

        assert_equal(i.iter_value, 2)
        assert_equal(len(i.records), 1)
        assert i.records[0].data['value'] is True

    def test_next_empty(self):
        s = turtle.Turtle()
        i = stream.StreamIterator(s, 0, stream.ITER_TYPE_LATEST)
        i.fill = turtle.Turtle()

        for r in i:
            assert False

        assert i._empty

    def test_next_not_empty(self):
        s = turtle.Turtle()
        i = stream.StreamIterator(s, 0, stream.ITER_TYPE_LATEST)

        records = [generate_raw_record(), generate_raw_record()]
        i.records += records
        i._empty = False

        def fill():
            assert False

        i.fill = fill

        found_records = []
        for r in i:
            found_records.append(r)

        assert_equal(found_records, records)
        assert i._empty


class CombinedStreamIteratorTest(TestCase):

    def test_first_no_wait(self):
        c = stream.CombinedStreamIterator([])

        start_t = time.time()
        c._wait()
        duration_s = time.time() - start_t
        assert_lt(duration_s, 0.1)

    def test_next_wait(self):
        c = stream.CombinedStreamIterator([])
        c._last_wait = time.time()

        start_t = time.time()
        c._wait()
        duration_s = time.time() - start_t
        assert_gt(duration_s, 0.1)

    def test_single(self):
        s = turtle.Turtle()
        s.name = 'test stream'
        i = stream.StreamIterator(s, 0, stream.ITER_TYPE_LATEST)
        i.records = [1]
        i._empty = False

        c = stream.CombinedStreamIterator([i])
        c._fill()
        c._running = False

        records = list(c)
        assert_equal(records, [1])

    def test_multiple(self):
        s = turtle.Turtle()
        s.name = 'test stream'

        i1 = stream.StreamIterator(s, 0, stream.ITER_TYPE_LATEST)
        i1.records = [1]
        i1._empty = False

        i2 = stream.StreamIterator(s, 1, stream.ITER_TYPE_LATEST)
        i2.records = [2]
        i2._empty = False

        c = stream.CombinedStreamIterator([i1, i2])
        # Each iterator will be filled in sequence. Since we need to run this
        # test with running = False (so it ends), we have to pre-fill.
        c._fill()
        c._fill()
        c._running = False

        records = list(c)

        assert_equal(set(records), set([1, 2]))


class StreamTest(TestCase):

    def test_partition_key(self):
        c = turtle.Turtle()
        s = stream.Stream(c, 'test stream', 'value')

        assert_equal(s._partition_key({'value': 1}), "1")

    def test_shards_ids(self):
        c = turtle.Turtle()

        def describe_stream(_):
            return {
                'StreamDescription': {
                    'HasMoreShards': False,
                    'Shards': [
                        {'ShardId': '0001'}, {'ShardId': '0002'}
                    ]
                }
            }

        c.describe_stream = describe_stream
        s = stream.Stream(c, 'test stream', 'value')

        assert_equal(set(s.shard_ids), set(['0001', '0002']))

    def test_select_shard_ids(self):
        c = turtle.Turtle()
        s = stream.Stream(c, 'test stream', 'value')
        s._shard_ids = ['0001', '0002', '0003']

        shard_ids = s._select_shard_ids([0, 2])
        assert_equal(shard_ids, ['0001', '0003'])

    def test_select_shard_ids_empty(self):
        c = turtle.Turtle()
        s = stream.Stream(c, 'test stream', 'value')
        s._shard_ids = ['0001', '0002', '0003']

        shard_ids = s._select_shard_ids([])
        assert_equal(shard_ids, ['0001', '0002', '0003'])

    def test_select_shard_ids_missing(self):
        c = turtle.Turtle()
        s = stream.Stream(c, 'test stream', 'value')
        s._shard_ids = ['0001', '0002', '0003']

        with assert_raises(errors.ShardNotFoundError):
            shard_ids = s._select_shard_ids([4])

    def test_put(self):
        c = turtle.Turtle()

        def put_record(*args):
            return {'ShardId': '0001', 'SequenceNumber': 1}

        c.put_record = put_record

        s = stream.Stream(c, 'test stream', 'value')

        shard_id, seq_num = s.put(value=0)
        assert_equal(seq_num, 1)
        assert_equal(shard_id, '0001')

    def test_put_many_basic(self):
        c = turtle.Turtle()

        def put_records(*args):
            return {'Records': [{'ShardId': '0001', 'SequenceNumber': 1}]}

        c.put_records = put_records

        s = stream.Stream(c, 'test stream', 'value')

        resp = s.put_many([dict(value=0)])

        shard_id, seq_num = resp[0]
        assert_equal(seq_num, 1)
        assert_equal(shard_id, '0001')

    def test_put_many_gt_500(self):
        c = turtle.Turtle()

        def put_records(*args):
            return {'Records': [
                {'ShardId': '0001', 'SequenceNumber': n}
                for n in range(len(args[0]))
            ]}

        c.put_records = put_records

        s = stream.Stream(c, 'test stream', 'value')

        for test_count in [1, 499, 500, 501, 1201]:
            resp = s.put_many([dict(value=0)] * test_count)
            assert_equal(len(resp), test_count)

    def test_build_iterator(self):
        c = turtle.Turtle()

        s = stream.Stream(c, 'test stream', 'value')
        shard_ids = ['0001', '0002']

        i = s._build_iterator(stream.ITER_TYPE_LATEST, shard_ids, None)
        assert_equal(len(i.iterators), 2)
