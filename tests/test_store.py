# -*- coding: utf-8 -*-
from testify import *
import datetime
import time
import os.path
import shutil

from triton import store


class StreamArchiveWriterFilePathTest(TestCase):

    @setup
    def build_stream(self):
        self.dt = datetime.datetime(2015, 7, 24)
        self.stream = store.StreamArchiveWriter({'name': "foo"}, self.dt,
                                                "/tmp")
        self.unicode_stream = store.StreamArchiveWriter(
                                                {u'name': u'føø_üñîçødé_宇宙'},
                                                self.dt, u'/tµπ_üñîçødé_宇宙')

    def test(self):
        assert self.stream.file_path.startswith(
            "/tmp/20150724/foo-archive-"), self.stream.file_path
        assert self.stream.file_path.endswith('.tri')

    def test_unicode(self):
        assert self.unicode_stream.file_path.startswith(
            u'/tµπ_üñîçødé_宇宙/20150724/føø_üñîçødé_宇宙-archive-'), self.unicode_stream.file_path
        assert self.unicode_stream.file_path.endswith(u'.tri')


class StreamArchiveWriterWriteTest(TestCase):

    @setup
    def build_stream(self):
        self.dt = datetime.datetime(2015, 7, 24)
        self.stream = store.StreamArchiveWriter({'name': "foo"}, self.dt,
                                                "/tmp")
        # self.unicode_stream = store.StreamArchiveWriter(
        #                                         {u'name': u'føø_üñîçødé_宇宙'},
        #                                         self.dt, u'/tµπ_üñîçødé_宇宙')

    def test_buffer(self):
        self.stream.put(ts=time.time(), value="hello")
        assert_is(self.stream.writer, None)
        assert self.stream.buffer.tell() > 0

    def test_flush(self):
        self.stream.put(ts=time.time(), value="hello")
        self.stream.flush()

        assert_equal(self.stream.buffer.tell(), 0)
        assert self.stream.writer
        assert os.path.exists(self.stream.file_path)

        shutil.rmtree(os.path.dirname(self.stream.file_path))


class StreamArchiveReaderShortTest(TestCase):

    @setup
    def create_data(self):
        writer = store.StreamArchiveWriter({'name': "foo"},
                                           datetime.datetime.utcnow(), "/tmp")
        self.file_path = writer.file_path

        writer.put(ts=time.time(), value="hello")

        writer.close()

    @setup
    def build_reader(self):
        self.reader = store.StreamArchiveReader(self.file_path)

    def test(self):
        recs = list(self.reader)

        assert_equal(len(recs), 1)
        assert_equal(recs[0]['value'], "hello")
        assert recs[0]['ts']

    @teardown
    def cleanup_data(self):
        shutil.rmtree(os.path.dirname(self.file_path))


class StreamArchiveReaderFullTest(TestCase):

    @setup
    def create_data(self):
        writer = store.StreamArchiveWriter({'name': "foo"},
                                           datetime.datetime.utcnow(), "/tmp")
        self.file_path = writer.file_path

        # We want multiple objects, in multiple snappy frames
        writer.put(ts=time.time(), value="hello")
        writer.put(ts=time.time(), value="hello 1")

        writer.flush()

        writer.put(ts=time.time(), value="hello 2")
        writer.put(ts=time.time(), value="hello 3")

        writer.close()

    @setup
    def build_reader(self):
        self.reader = store.StreamArchiveReader(self.file_path)

    def test(self):
        recs = list(self.reader)

        assert_equal(len(recs), 4)
        assert_equal(recs[0]['value'], "hello")
        assert_equal(recs[3]['value'], "hello 3")

    @teardown
    def cleanup_data(self):
        shutil.rmtree(os.path.dirname(self.file_path))
