# -*- coding: utf-8 -*-

from testify import *
import mock

import json
import msgpack
import struct
import time
import datetime
import decimal
import os
import shutil
import subprocess
import random
import tempfile
from collections import defaultdict

from triton import nonblocking_stream, config
from triton.encoding import msgpack_encode_default, unicode_to_ascii_str, ascii_to_unicode_str

TEST_LOGS_BASE_DIRECTORY_SLUG = 'test_logs'
TEST_TRITON_ZMQ_PORT = 3517  # in case tritond is running


class Point(object):

    def __init__(self, lat, lng):
        self.coords = (lat, lng)


def generate_test_data(primary_key='my_key'):
    data = {
        'pkey': primary_key,
        'value': True,
    }
    return data


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

def generate_unicode_data(primary_key=u'my_key'):
    data = {
        u'pkey': primary_key,
        u'value': True,
        u'ascii_key': u'sømé_ünîcode_vàl',
        u'ünîcødé_key': u'ascii_val',
        u'ünîcødé_πå®tîtîøñ_ke¥_宇宙': u'ünîcødé_πå®tîtîøñ_√al_宇宙'
    }
    return data

def generate_escaped_unicode_data(primary_key='my_key'):
    data = {
        'pkey': primary_key,
        'value': True,
        'ascii_key': 'sømé_ünîcode_vàl',
        'ünîcødé_key': 'ascii_val',
        'ünîcødé_πå®tîtîøñ_ke¥_宇宙': 'ünîcødé_πå®tîtîøñ_√al_宇宙'
    }
    return data

def generate_transmitted_record(data, stream_name='test_stream', partition_key='pkey'):
    message_data = msgpack.packb(
        data, default=msgpack_encode_default)

    meta_data = struct.pack(
        nonblocking_stream.META_STRUCT_FMT,
        nonblocking_stream.META_STRUCT_VERSION,
        unicode_to_ascii_str(stream_name),
        unicode_to_ascii_str(data[partition_key]))
    return meta_data, message_data


def generate_transmitted_record_json(data, stream_name='test_stream'):
    message_data = msgpack.packb(
        data, default=msgpack_encode_default)

    meta_data = json.dumps(dict(
        stream_name=stream_name,
        partition_key=data['pkey']
    ))

    return meta_data, message_data


def _serialize_context(self, data):
    # mock serializer for JSON header
    if len(self._partition_key(data)) > 64:
        raise ValueError("Partition Key Too Long")

    meta_data = json.dumps(dict(
        stream_name=self.name,
        partition_key=data['pkey']
    ))

    try:
        message_data = msgpack.packb(data)
    except TypeError:
        # If we fail to serialize our context, we can try again with an
        # enhanced packer (it's slower though)
        message_data = msgpack.packb(data, default=msgpack_encode_default)

    return meta_data, message_data


def decode_debug_data(file_object):
    unpacker = msgpack.Unpacker(file_object, encoding='utf-8')
    data = defaultdict(list)
    current_list = None
    for item in unpacker:
        if type(item) == unicode:
            current_list = data[item]
        else:
            current_list.append(item)
    return data


class NonblockingStreamTest(TestCase):
    @setup
    def init_mock_stream(self):
        self.temp_storage = nonblocking_stream.threadLocal
        nonblocking_stream._zmq_context = turtle.Turtle()
        nonblocking_stream.threadLocal = turtle.Turtle()

    @teardown
    def teardown_mock_stream(self):
        nonblocking_stream._zmq_context = None
        nonblocking_stream.threadLocal = self.temp_storage

    def test_send_event(self):
        s = nonblocking_stream.NonblockingStream('test_stream', 'pkey')
        test_data = generate_test_data()
        s.put(**test_data)
        meta_data, message_data = generate_transmitted_record(test_data)
        mock_sent_meta_data, mock_sent_message_data = (
            nonblocking_stream.threadLocal
            .zmq_socket.send_multipart.calls[0][0][0])
        assert_equal(mock_sent_meta_data, meta_data)
        assert_equal(mock_sent_message_data, message_data)

    def test_send_unicode_event(self):
        s = nonblocking_stream.NonblockingStream(u'tést_üñîçødé_stream_宇宙', u'ünîcødé_πå®tîtîøñ_ke¥_宇宙')
        test_data = generate_unicode_data()
        s.put(**test_data)
        meta_data, message_data = generate_transmitted_record(test_data, stream_name=u'tést_üñîçødé_stream_宇宙', partition_key=u'ünîcødé_πå®tîtîøñ_ke¥_宇宙')
        mock_sent_meta_data, mock_sent_message_data = (
            nonblocking_stream.threadLocal
            .zmq_socket.send_multipart.calls[0][0][0])
        assert_equal(mock_sent_meta_data, meta_data)
        assert_equal(mock_sent_message_data, message_data)
        sent_data = msgpack.unpackb(mock_sent_message_data, encoding='utf-8')
        #NOTE: ascii key does correct lookup for 1-byte unicode key
        assert_equal(
            sent_data['ascii_key'],
            test_data['ascii_key']
        )
        assert_equal(
            sent_data[u'ascii_key'],
            test_data[u'ascii_key']
        )
        #NOTE: for multi-byte unicode keys, "escaped" ascii bytestrings will KeyError.
        #you need to give unicode objects as keys
        assert_equal(
            sent_data[u'ünîcødé_πå®tîtîøñ_ke¥_宇宙'],
            test_data[u'ünîcødé_πå®tîtîøñ_ke¥_宇宙']
        )
        assert_falsey('ünîcødé_πå®tîtîøñ_ke¥_宇宙' in sent_data)

    def test_send_escaped_unicode_event(self):
        s = nonblocking_stream.NonblockingStream('tést_éßçåπéd_üñîçødé_stream_宇宙', 'ünîcødé_πå®tîtîøñ_ke¥_宇宙')
        test_data = generate_escaped_unicode_data()
        s.put(**test_data)
        meta_data, message_data = generate_transmitted_record(test_data, stream_name='tést_éßçåπéd_üñîçødé_stream_宇宙', partition_key='ünîcødé_πå®tîtîøñ_ke¥_宇宙')
        mock_sent_meta_data, mock_sent_message_data = (
            nonblocking_stream.threadLocal
            .zmq_socket.send_multipart.calls[0][0][0])
        assert_equal(mock_sent_meta_data, meta_data)
        assert_equal(mock_sent_message_data, message_data)
        #NOTE: NOT unpacking with encoding='utf-8' to test escaped ascii data
        sent_data = msgpack.unpackb(mock_sent_message_data)
        #NOTE: both ascii and single-byte unicode keys correctly look up
        #ascii keys
        assert_equal(
            sent_data['ascii_key'],
            test_data['ascii_key']
        )
        assert_equal(
            sent_data[u'ascii_key'],
            test_data[u'ascii_key']
        )
        assert_equal(
            sent_data['ünîcødé_πå®tîtîøñ_ke¥_宇宙'],
            test_data['ünîcødé_πå®tîtîøñ_ke¥_宇宙']
        )
        #NOTE: for multi-byte unicode keys, unicode will KeyError when trying
        #to look up "escaped" ascii
        assert_falsey(u'ünîcødé_πå®tîtîøñ_ke¥_宇宙' in sent_data)

    def test_unicode_serialize_context(self):
        s = nonblocking_stream.NonblockingStream(u'tést_üñîçødé_stream', u'ünîcødé_πå®tîtîøñ_ke¥_宇宙')
        test_data = generate_unicode_data()
        meta_data, message_data = s._serialize_context(test_data)
        assert_meta_data, assert_message_data = generate_transmitted_record(test_data, stream_name=u'tést_üñîçødé_stream', partition_key=u'ünîcødé_πå®tîtîøñ_ke¥_宇宙')
        assert_equal(assert_meta_data, meta_data)
        assert_equal(assert_message_data, message_data)

    def test_escaped_unicode_serialize_context(self):
        s = nonblocking_stream.NonblockingStream('tést_éßçåπéd_üñîçødé_stream_宇宙', 'ünîcødé_πå®tîtîøñ_ke¥_宇宙')
        test_data = generate_escaped_unicode_data()
        meta_data, message_data = s._serialize_context(test_data)
        assert_meta_data, assert_message_data = generate_transmitted_record(test_data, stream_name='tést_éßçåπéd_üñîçødé_stream_宇宙', partition_key='ünîcødé_πå®tîtîøñ_ke¥_宇宙')
        assert_equal(assert_meta_data, meta_data)
        assert_equal(assert_message_data, message_data)

    def test_send_hard_to_encode_data(self):
        s = nonblocking_stream.NonblockingStream('test_stream', 'pkey')
        test_data = generate_messy_test_data()
        s.put(**test_data)
        meta_data, message_data = generate_transmitted_record(test_data)
        mock_sent_meta_data, mock_sent_message_data = (
            nonblocking_stream.threadLocal
            .zmq_socket.send_multipart.calls[0][0][0])
        assert_equal(mock_sent_meta_data, meta_data)
        assert_equal(mock_sent_message_data, message_data)
        sent_data = msgpack.unpackb(mock_sent_message_data)
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


class NonblockingStreamEndToEnd(TestCase):
    """full end-to-end test case"""

    @setup
    def setup_server(self):
        self.base_directory = tempfile.gettempdir()
        self.log_directory = os.path.join(
            self.base_directory,
            'streamtest' + str(random.randint(100000, 999999)))
        self.log_file = os.path.join(self.log_directory, 'streamtest')
        os.makedirs(self.log_directory)
        process_env = os.environ.copy()
        process_env['TRITON_ZMQ_PORT'] = str(TEST_TRITON_ZMQ_PORT)
        self.server_process = subprocess.Popen(
            [
                'python',
                './bin/tritond',
                '--skip-kinesis',
                '--output_file',
                self.log_file
            ],
            env=process_env)
        time.sleep(2)
        config.ZMQ_DEFAULT_PORT = TEST_TRITON_ZMQ_PORT

    @teardown
    def teardown_server(self):
        self.server_process.terminate()
        time.sleep(1)
        shutil.rmtree(self.log_directory)

    def test_end_to_end(self):
        stream_name = 'test_stream'
        test_stream = nonblocking_stream.NonblockingStream(
            stream_name, 'pkey')
        for i in range(10):
            test_stream.put(**generate_test_data())
        time.sleep(1)
        assert_truthy(os.path.exists(self.log_file))
        if os.path.exists(self.log_file):
            with open(self.log_file, 'rb') as output_file:
                received_data = decode_debug_data(output_file)
            assert_truthy(stream_name in received_data)
            if stream_name in received_data:
                assert_equal(len(received_data[stream_name]), 10)

    def test_unicode_end_to_end(self):
        stream_name = u'téßt_üñîçø∂é_st®éåµ_宇宙'
        test_stream = nonblocking_stream.NonblockingStream(
            stream_name, u'ünîcødé_πå®tîtîøñ_ke¥_宇宙')
        for i in range(10):
            test_stream.put(**generate_unicode_data())
        time.sleep(1)
        assert_truthy(os.path.exists(self.log_file))
        if os.path.exists(self.log_file):
            with open(self.log_file, u'rb') as output_file:
                received_data = decode_debug_data(output_file)
            assert_truthy(stream_name in received_data)
            if stream_name in received_data:
                assert_equal(len(received_data[stream_name]), 10)

    def test_escaped_unicode_end_to_end(self):
        stream_name = 'téßt_üñîçø∂é_st®éåµ_宇宙'
        test_stream = nonblocking_stream.NonblockingStream(
            stream_name, 'ünîcødé_πå®tîtîøñ_ke¥_宇宙')
        for i in range(10):
            test_stream.put(**generate_escaped_unicode_data())
        time.sleep(1)
        assert_truthy(os.path.exists(self.log_file))
        if os.path.exists(self.log_file):
            with open(self.log_file, u'rb') as output_file:
                received_data = decode_debug_data(output_file)
            #NOTE: "escaped" unicode comes out the read-side of the stream as
            #pure unicode, so we need to convert the stream name to unicode for
            #assert
            assert_truthy(ascii_to_unicode_str(stream_name) in received_data)
            if stream_name in received_data:
                assert_equal(len(received_data[stream_name]), 10)

    def test_multiple_streams(self):
        streams = set(['stream_a', 'stream_b'])
        for stream_name in streams:
            stream = nonblocking_stream.NonblockingStream(
                stream_name, 'pkey')
            stream.put(**generate_test_data())
        time.sleep(1)
        if os.path.exists(self.log_file):
            with open(self.log_file, 'rb') as output_file:
                received_data = (decode_debug_data(output_file))
            # does testify have assertIsNotNone?
            assert_equal(streams, set(received_data.keys()))
            target_file_counts = dict((n, 1) for n in streams)
            received_file_counts = dict(
                (stream, len(messages))
                for stream, messages in received_data.items())
            assert_equal(received_file_counts, target_file_counts)

    def test_volume(self):
        stream_name = 'test_stream2'
        send_count = 20000
        test_stream = nonblocking_stream.NonblockingStream(
            stream_name, 'pkey')
        for i in range(send_count):
            test_stream.put(**generate_test_data())
        time.sleep(1)
        if os.path.exists(self.log_file):
            with open(self.log_file, 'rb') as output_file:
                received_data = (decode_debug_data(output_file))
            assert_truthy(stream_name in received_data)
            assert_equal(len(received_data[stream_name]), send_count)

    def test_end_to_end_json_header(self):
        stream_name = 'test_stream'
        with mock.patch.object(
            nonblocking_stream.NonblockingStream,
            '_serialize_context',
            new=_serialize_context
        ):
            test_stream = nonblocking_stream.NonblockingStream(
                stream_name, 'pkey')
            for i in range(10):
                test_stream.put(**generate_test_data())
            time.sleep(1)
            assert_truthy(os.path.exists(self.log_file))
            if os.path.exists(self.log_file):
                with open(self.log_file, 'rb') as output_file:
                    received_data = (decode_debug_data(output_file))
                assert_truthy(stream_name in received_data)
                if stream_name in received_data:
                    assert_equal(len(received_data[stream_name]), 10)
