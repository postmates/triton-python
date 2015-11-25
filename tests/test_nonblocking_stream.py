from testify import *

import msgpack
import struct
import time
import os
import shutil
import subprocess
import random
import tempfile
from collections import defaultdict

from triton import nonblocking_stream

TEST_LOGS_BASE_DIRECTORY_SLUG = 'test_logs'


def generate_test_data(primary_key='my_key'):
    data = {
        'pkey': primary_key,
        'value': True,
    }
    return data


def generate_transmitted_record(data, stream_name='test_stream'):
    message_data = msgpack.packb(data)

    meta_data = struct.pack(
        nonblocking_stream.META_STRUCT_FMT,
        nonblocking_stream.META_STRUCT_VERSION,
        stream_name,
        data['pkey'])
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
        s.put(test_data)
        meta_data, message_data = generate_transmitted_record(test_data)
        mock_sent_meta_data, mock_sent_message_data = (
            nonblocking_stream.threadLocal
            .zmq_socket.send_multipart.calls[0][0][0])
        assert_equal(mock_sent_meta_data, meta_data)
        assert_equal(mock_sent_message_data, message_data)


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
        self.server_process = subprocess.Popen(
            [
                'python',
                './bin/tritond',
                '--skip-kinesis',
                '--output_file',
                self.log_file
            ])
        time.sleep(2)

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
            test_stream.put(generate_test_data())
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
            stream.put(generate_test_data())
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
            test_stream.put(generate_test_data())
        time.sleep(1)
        if os.path.exists(self.log_file):
            with open(self.log_file, 'rb') as output_file:
                received_data = (decode_debug_data(output_file))
            assert_truthy(stream_name in received_data)
            assert_equal(len(received_data[stream_name]), send_count)
