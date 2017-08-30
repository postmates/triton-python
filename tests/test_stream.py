# -*- coding: utf-8 -*-

from testify import *
import mock
import base64
import time
import datetime
import decimal
import random

import msgpack

from triton import stream
from triton import errors
from boto.exception import BotoServerError


def generate_raw_record(n=1):
    data = base64.b64encode(msgpack.packb({'value': True}))

    raw_record = {'SequenceNumber': n, 'Data': data}

    return raw_record

def generate_unicode_raw_record(n=42):
    data = base64.b64encode(msgpack.packb({u'value': True, u'test_üñîçø∂é_ké¥_宇宙': u'test_üñîçø∂é_√ål_宇宙'}))

    raw_record = {u'SequenceNumber': n, u'Data': data}

    return raw_record

def generate_escaped_unicode_raw_record(n=42):
    data = base64.b64encode(msgpack.packb({'value': True, 'test_üñîçø∂é_ké¥_宇宙': 'test_üñîçø∂é_√ål_宇宙'}))

    raw_record = {'SequenceNumber': n, 'Data': data}

    return raw_record

def generate_record(n=1):
    return stream.Record.from_raw_record(0, generate_raw_record(n))

def generate_unicode_record(n=1):
    return stream.Record.from_raw_record(0, generate_unicode_raw_record(n))

def generate_escaped_unicode_record(n=1):
    return stream.Record.from_raw_record(0, generate_escaped_unicode_raw_record(n))

def generate_messy_test_data(primary_key='my_key'):
    data = {
        'pkey': primary_key,
        'value': True,
        'time': datetime.datetime.now(),
        'date': datetime.date.today(),
        'pi': decimal.Decimal('3.14'),
        'point': Point(-122.42083, 37.75512)
    }
    return data


class Point(object):

    def __init__(self, lat, lng):
        self.coords = (lat, lng)


class RecordTest(TestCase):

    def test_from_raw_record(self):
        raw_record = generate_raw_record()

        r = stream.Record.from_raw_record(0, raw_record)
        assert_equal(r.seq_num, 1)
        assert_equal(r.shard_id, 0)
        assert_equal(r.data['value'], True)

    def test_from_unicode_raw_record(self):
        unicode_raw_record = generate_unicode_raw_record()

        ur = stream.Record.from_raw_record(3, unicode_raw_record)
        assert_equal(ur.seq_num, 42)
        assert_equal(ur.shard_id, 3)
        assert_equal(ur.data[u'value'], True)
        assert_equal(ur.data[u'test_üñîçø∂é_ké¥_宇宙'], u'test_üñîçø∂é_√ål_宇宙')

    def test_from_escaped_unicode_raw_record(self):
        esc_unicode_raw_record = generate_escaped_unicode_raw_record()

        eur = stream.Record.from_raw_record(3, esc_unicode_raw_record)
        assert_equal(eur.seq_num, 42)
        assert_equal(eur.shard_id, 3)
        assert_equal(eur.data['value'], True)
        #NOTE: escaped unicode comes out the other side as unicode
        assert_equal(eur.data[u'test_üñîçø∂é_ké¥_宇宙'], u'test_üñîçø∂é_√ål_宇宙')

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

        records = [generate_record(), generate_record()]
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
        r = generate_record()
        i.records = [r, ]
        i._empty = False

        c = stream.CombinedStreamIterator([i])
        c._fill()
        c._running = False

        records = list(c)
        assert_equal(records, [r, ])

    def test_multiple(self):
        s = turtle.Turtle()
        s.name = 'test stream'

        sent_records = [generate_record(1), generate_record(2)]

        i1 = stream.StreamIterator(s, 0, stream.ITER_TYPE_LATEST)
        i1.records = [sent_records[0], ]
        i1._empty = False

        i2 = stream.StreamIterator(s, 1, stream.ITER_TYPE_LATEST)
        i2.records = [sent_records[1], ]
        i2._empty = False

        c = stream.CombinedStreamIterator([i1, i2])
        # Each iterator will be filled in sequence. Since we need to run this
        # test with running = False (so it ends), we have to pre-fill.
        c._fill()
        c._fill()
        c._running = False

        records = list(c)

        assert_equal(set(records), set(sent_records))


class StreamTest(TestCase):

    def test_partition_key(self):
        c = turtle.Turtle()
        s = stream.Stream(c, 'test stream', 'value')

        assert_equal(s._partition_key({'value': 1}), "1")

    def test_unicode_partition_key(self):
        c = turtle.Turtle()
        s = stream.Stream(c, u'test_üñîçø∂é_stream', u'ünîcødé_πå®tîtîøñ_ke¥_宇宙')

        assert_equal(s._partition_key({u'ünîcødé_πå®tîtîøñ_ke¥_宇宙': u'ünîcødé_πå®tîtîøñ_√al_宇宙'}), u'ünîcødé_πå®tîtîøñ_√al_宇宙')
        #NOTE: even when we throw escaped unicode in, return unicode
        assert_equal(s._partition_key({'ünîcødé_πå®tîtîøñ_ke¥_宇宙': 'ünîcødé_πå®tîtîøñ_√al_宇宙'}), u'ünîcødé_πå®tîtîøñ_√al_宇宙')

    def test_escaped_unicode_partition_key(self):
        c = turtle.Turtle()
        s = stream.Stream(c, 'test_üñîçø∂é_stream', 'ünîcødé_πå®tîtîøñ_ke¥_宇宙')
        #NOTE: when we create a stream with escaped unicode, convert to unicode it works with unicode data
        assert_equal(s._partition_key({u'ünîcødé_πå®tîtîøñ_ke¥_宇宙': u'ünîcødé_πå®tîtîøñ_√al_宇宙'}), u'ünîcødé_πå®tîtîøñ_√al_宇宙')
        #NOTE: even when we throw escaped unicode in, return unicode
        assert_equal(s._partition_key({'ünîcødé_πå®tîtîøñ_ke¥_宇宙': 'ünîcødé_πå®tîtîøñ_√al_宇宙'}), u'ünîcødé_πå®tîtîøñ_√al_宇宙')

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

        with mock.patch('pystatsd.increment') as increment:
            shard_id, seq_num = s.put(value=0)
        assert_equal(seq_num, 1)
        assert_equal(shard_id, '0001')
        increment.assert_called_with('triton.stream.put_attempt.test stream')

    def test_put_hard_to_encode_data(self):
        c = turtle.Turtle()

        mock_sent_message_data = list()

        def put_record(*args):
            mock_sent_message_data.append(args[1])
            return {'ShardId': '0001', 'SequenceNumber': 1}

        c.put_record = put_record

        s = stream.Stream(c, 'test_stream', 'pkey')

        test_data = generate_messy_test_data()
        s.put(**test_data)

        sent_data = msgpack.unpackb(mock_sent_message_data[0])

        assert_equal(
            sent_data['time'],
            test_data['time'].isoformat(' '))
        assert_equal(
            sent_data['date'],
            test_data['date'].strftime("%Y-%m-%d"))
        assert_equal(sent_data['pi'], str(test_data['pi']))
        assert_equal(
            sent_data['point'],
            str(test_data['point'].coords))

    def test_put_fail(self):
        c = turtle.Turtle()

        def put_record(*args):
            return {'error': 'unknown'}

        c.put_record = put_record

        s = stream.Stream(c, 'test stream', 'value')

        assert_raises(errors.KinesisError, s.put, value=0)

    def test_put_many_basic(self):
        c = turtle.Turtle()

        def put_records(*args, **kwargs):
            return {'Records': [{'ShardId': '0001', 'SequenceNumber': 1}]}

        c.put_records = put_records

        s = stream.Stream(c, 'test stream', 'value')

        with mock.patch('pystatsd.increment') as increment:
            resp = s.put_many([dict(value=0)])

        shard_id, seq_num = resp[0]
        assert_equal(seq_num, 1)
        assert_equal(shard_id, '0001')
        increment.assert_called_with('triton.stream.put_attempt.test stream', 1)

    def test_put_many_hard_to_encode(self):
        c = turtle.Turtle()

        mock_sent_message_data = list()

        def put_records(*args, **kwargs):
            mock_sent_message_data.extend(args[0])
            return {'Records': [
                {'ShardId': '0001', 'SequenceNumber': n}
                for n in range(len(args[0]))
            ]}

        c.put_records = put_records

        s = stream.Stream(c, 'test stream', 'value')

        test_data = generate_messy_test_data()

        resp = s.put_many([test_data, ] * 2)
        assert_equal(len(resp), 2)

        sent_data = msgpack.unpackb(mock_sent_message_data[1]['Data'])

        assert_equal(
            sent_data['time'],
            test_data['time'].isoformat(' '))
        assert_equal(
            sent_data['date'],
            test_data['date'].strftime("%Y-%m-%d"))
        assert_equal(sent_data['pi'], str(test_data['pi']))
        assert_equal(
            sent_data['point'],
            str(test_data['point'].coords))

    def test_put_many_gt_500(self):
        c = turtle.Turtle()

        def put_records(*args, **kwargs):
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

    def test_put_retry(self):
        c = turtle.Turtle()

        class PutRecord(object):
            """callable class for call count tracking"""
            def __init__(self, succeed_on_call=2, error_code=500):
                self.calls = 0
                self.succeed_on_call = succeed_on_call
                self.error_code = error_code

            def __call__(self, *args, **kwargs):
                self.calls += 1
                if self.calls < self.succeed_on_call:
                    raise BotoServerError(self.error_code, "test", "test")
                else:
                    return {'ShardId': '0001', 'SequenceNumber': 1}

        put_record = PutRecord()

        c.put_record = put_record

        st = stream.Stream(c, 'test stream', 'value')

        shard_id, seq_num = st.put(value=0)
        assert_equal(seq_num, 1)
        assert_equal(shard_id, '0001')

        put_record = PutRecord(succeed_on_call=4)
        c.put_record = put_record
        assert_raises(BotoServerError, st.put, value=0)

        put_record = PutRecord(error_code=400)
        c.put_record = put_record
        assert_raises(BotoServerError, st.put, value=0)

    def test_put_many_retry(self):
        c = turtle.Turtle()

        class PutRecords(object):
            """callable class for call count tracking"""
            def __init__(self, fail_for_n_calls=1, initial_error_rate=0.9):
                self.calls = 0
                self.fail_for_n_calls = fail_for_n_calls
                self.initial_error_rate = initial_error_rate

            def __call__(self, records, string_name, **kwargs):
                self.calls += 1
                if self.calls <= self.fail_for_n_calls:
                    return self.put_records(records, self.initial_error_rate)
                else:
                    return self.put_records(records, 0)

            def put_records(self, records, error_rate):
                return_records = []
                for n in range(len(records)):
                    if random.random() >= error_rate:
                        return_records.append(
                            {'ShardId': '0001', 'SequenceNumber': n})
                    else:
                        return_records.append({'error': 'test_error'})
                return {'Records': return_records}

        put_records = PutRecords()
        c.put_records = put_records
        s = stream.Stream(c, 'test stream', 'value')
        test_count = 600
        resp = s.put_many([dict(value=0)] * test_count)
        assert_equal(len(resp), test_count)

        put_records = PutRecords(fail_for_n_calls=4, initial_error_rate=1.)
        c.put_records = put_records
        s = stream.Stream(c, 'test stream', 'value')
        test_count = 100
        assert_raises(
            errors.KinesisPutManyError, s.put_many,
            [dict(value=0)] * test_count)

        put_records = PutRecords(fail_for_n_calls=4, initial_error_rate=.5)
        c.put_records = put_records
        s = stream.Stream(c, 'test stream', 'value')
        test_count = 100
        try:
            resp = s.put_many([dict(value=0)] * test_count)
        except errors.KinesisPutManyError as e:
            assert_lt(len(e.failed_data), test_count)
        else:
            raise Exception('Expected failed records')
