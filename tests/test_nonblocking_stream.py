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
from triton.encoding import msgpack_encode_default

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

#TODO: generate non-futurized ASCII strings!

#NOTE: stream name is guaranteed to be a string, therefore we can safely do
#.encode('utf-8') to convert to ascii so struct can handle it. But
#data[partition_key] could return either a unicode string or another non-string
#value. Therefore, we have to safely convert to a unicode string before encoding,
#just as NonblockingStream._partition_key does.
def generate_transmitted_record(data, stream_name='test_stream', partition_key='pkey'):
    message_data = msgpack.packb(
        data, default=msgpack_encode_default)

    meta_data = struct.pack(
        nonblocking_stream.META_STRUCT_FMT,
        nonblocking_stream.META_STRUCT_VERSION,
        stream_name.encode('utf-8'),
        unicode(data[partition_key]).encode('utf-8'))
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
    unpacker = msgpack.Unpacker(file_object)
    data = defaultdict(list)
    current_list = None
    for item in unpacker:
        if type(item) == str:
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
        #TODO: assert equal for data (see test_send_hard_to_encode_data below)

    def test_send_escaped_unicode_event(self):
        s = nonblocking_stream.NonblockingStream('tést_üñîçødé_stream_宇宙', 'ünîcødé_πå®tîtîøñ_ke¥_宇宙')
        test_data = generate_escaped_unicode_data()
        s.put(**test_data)
        meta_data, message_data = generate_transmitted_record(test_data, stream_name='tést_üñîçødé_stream_宇宙', partition_key='ünîcødé_πå®tîtîøñ_ke¥_宇宙')
        mock_sent_meta_data, mock_sent_message_data = (
            nonblocking_stream.threadLocal
            .zmq_socket.send_multipart.calls[0][0][0])
        assert_equal(mock_sent_meta_data, meta_data)
        assert_equal(mock_sent_message_data, message_data)
        #TODO: assert equal for data (see test_send_hard_to_encode_data below)

    def test_unicode_serialize_context(self):
        s = nonblocking_stream.NonblockingStream(u'tést_üñîçødé_stream', u'ünîcødé_πå®tîtîøñ_ke¥_宇宙')
        test_data = generate_unicode_data()
        meta_data, message_data = s._serialize_context(test_data)
        assert_meta_data, assert_message_data = generate_transmitted_record(test_data, stream_name=u'tést_üñîçødé_stream', partition_key=u'ünîcødé_πå®tîtîøñ_ke¥_宇宙')
        assert_equal(assert_meta_data, meta_data)
        assert_equal(assert_message_data, message_data)

    def test_escaped_unicode_serialize_context(self):
        s = nonblocking_stream.NonblockingStream('tést_üñîçødé_stream', 'ünîcødé_πå®tîtîøñ_ke¥_宇宙')
        test_data = generate_escaped_unicode_data()
        meta_data, message_data = s._serialize_context(test_data)
        assert_meta_data, assert_message_data = generate_transmitted_record(test_data, stream_name='tést_üñîçødé_stream', partition_key='ünîcødé_πå®tîtîøñ_ke¥_宇宙')
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
                received_data = (decode_debug_data(output_file))
            assert_truthy(stream_name in received_data)
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
