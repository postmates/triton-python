# -*- coding: utf-8 -*-
from testify import *
import datetime
import time
import os.path
import shutil

from triton import store
from triton.encoding import unicode_to_ascii_str


class StreamArchiveWriterFilePathTest(TestCase):

    @setup
    def build_stream(self):
        self.dt = datetime.datetime(2015, 7, 24)
        self.stream = store.StreamArchiveWriter({'name': "foo"}, self.dt,
                                                "/tmp")

    @setup
    def build_unicode_stream(self):
        self.dt_u = datetime.datetime(2015, 7, 24)
        self.unicode_stream = store.StreamArchiveWriter(
                                                {u'name': u'føø_üñîçødé_宇宙'},
                                                self.dt_u, u'/tmp/uni')

    @setup
    def build_escaped_unicode_stream(self):
        self.dt_ue = datetime.datetime(2015, 7, 24)
        self.escaped_unicode_stream = store.StreamArchiveWriter(
                                                {'name': 'føø_üñîçødé_宇宙_esc'},
                                                self.dt_ue, '/tmp/uni-esc')

    def test(self):
        assert self.stream.file_path.startswith(
            "/tmp/20150724/foo-archive-"), self.stream.file_path
        assert self.stream.file_path.endswith('.tri')

    def test_unicode(self):
        assert self.unicode_stream.file_path.startswith(
            u'/tmp/uni/20150724/føø_üñîçødé_宇宙-archive-'), self.unicode_stream.file_path
        assert self.unicode_stream.file_path.endswith(u'.tri')

    #NOTE: as usual, anything put in as escaped unicode will come out as unicode!
    def test_escaped_unicode(self):
        assert self.escaped_unicode_stream.file_path.startswith(
            u'/tmp/uni-esc/20150724/føø_üñîçødé_宇宙_esc-archive-'), self.escaped_unicode_stream.file_path
        assert self.escaped_unicode_stream.file_path.endswith(u'.tri')


class StreamArchiveWriterWriteTest(TestCase):

    @setup
    def build_stream(self):
        self.dt = datetime.datetime(2015, 7, 24)
        self.stream = store.StreamArchiveWriter({'name': "foo"}, self.dt,
                                                "/tmp")

    @setup
    def build_unicode_stream(self):
        self.dt_u = datetime.datetime(2015, 7, 24)
        self.unicode_stream = store.StreamArchiveWriter(
                                                {u'name': u'føø_üñîçødé_宇宙'},
                                                self.dt_u, u'/tmp/uni')

    @setup
    def build_escaped_unicode_stream(self):
        self.dt_ue = datetime.datetime(2015, 7, 24)
        self.escaped_unicode_stream = store.StreamArchiveWriter(
                                                {'name': 'føø_üñîçødé_宇宙_escaped'},
                                                self.dt_ue, '/tmp/uni-esc')

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

    def test_buffer_unicode(self):
        self.unicode_stream.put(ts=time.time(), value=u"üñîçødé")
        assert_is(self.unicode_stream.writer, None)
        assert self.unicode_stream.buffer.tell() > 0

    def test_flush_unicode(self):
        self.unicode_stream.put(ts=time.time(), value=u"üñîçødé")
        self.unicode_stream.flush()

        assert_equal(self.unicode_stream.buffer.tell(), 0)
        assert self.unicode_stream.writer
        assert os.path.exists(self.unicode_stream.file_path)

        shutil.rmtree(os.path.dirname(self.unicode_stream.file_path))

    def test_buffer_escaped_unicode(self):
        self.escaped_unicode_stream.put(ts=time.time(), value="üñîçødé")
        assert_is(self.escaped_unicode_stream.writer, None)
        assert self.escaped_unicode_stream.buffer.tell() > 0

    def test_flush_escaped_unicode(self):
        self.escaped_unicode_stream.put(ts=time.time(), value="üñîçødé")
        self.escaped_unicode_stream.flush()

        assert_equal(self.escaped_unicode_stream.buffer.tell(), 0)
        assert self.escaped_unicode_stream.writer
        assert os.path.exists(self.escaped_unicode_stream.file_path)

        shutil.rmtree(os.path.dirname(self.escaped_unicode_stream.file_path))


class StreamArchiveReaderShortTest(TestCase):

    @setup
    def create_data(self):
        writer = store.StreamArchiveWriter({'name': "foo"},
                                           datetime.datetime.utcnow(), "/tmp")
        self.file_path = writer.file_path

        writer.put(ts=time.time(), value="hello")

        writer.close()

    @setup
    def create_unicode_data(self):
        unicode_writer = store.StreamArchiveWriter(
                                                {u'name': u'føø_üñîçødé_宇宙'},
                                                datetime.datetime.utcnow(), u'/tmp/uni')
        self.unicode_file_path = unicode_writer.file_path

        unicode_writer.put(ts=time.time(), value=u"hello_üñîçødé_宇宙")

        unicode_writer.close()

    @setup
    def create_escaped_unicode_data(self):
        escaped_unicode_writer = store.StreamArchiveWriter(
                                                {'name': 'føø_üñîçødé_宇宙_escaped'},
                                                datetime.datetime.utcnow(), '/tmp/uni-esc')
        self.escaped_unicode_file_path = escaped_unicode_writer.file_path

        escaped_unicode_writer.put(ts=time.time(), value="hello_üñîçødé_宇宙")

        escaped_unicode_writer.close()

    @setup
    def build_reader(self):
        self.reader = store.StreamArchiveReader(self.file_path)

    @setup
    def build_unicode_reader(self):
        self.unicode_reader = store.StreamArchiveReader(self.unicode_file_path)

    @setup
    def build_escaped_unicode_reader(self):
        self.escaped_unicode_reader = store.StreamArchiveReader(self.escaped_unicode_file_path)

    def test(self):
        recs = list(self.reader)

        assert_equal(len(recs), 1)
        assert_equal(recs[0]['value'], "hello")
        assert recs[0]['ts']

    def test_unicode(self):
        u_recs = list(self.unicode_reader)

        assert_equal(len(u_recs), 1)
        assert_equal(u_recs[0][u'value'], u"hello_üñîçødé_宇宙")
        assert u_recs[0][u'ts']

    #NOTE: as usual, anything put in as escaped unicode will come out as unicode!
    def test_escaped_unicode(self):
        ue_recs = list(self.escaped_unicode_reader)

        assert_equal(len(ue_recs), 1)
        assert_equal(ue_recs[0]['value'], u"hello_üñîçødé_宇宙")
        assert ue_recs[0]['ts']

    @teardown
    def cleanup_data(self):
        shutil.rmtree(os.path.dirname(self.file_path))
        shutil.rmtree(os.path.dirname(self.unicode_file_path))
        shutil.rmtree(os.path.dirname(self.escaped_unicode_file_path))


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
    def create_unicode_data(self):
        unicode_writer = store.StreamArchiveWriter({u'name': u"üñîçødé_foo_宇宙"},
                                           datetime.datetime.utcnow(), u"/tmp/uni")
        self.unicode_file_path = unicode_writer.file_path

        # We want multiple objects, in multiple snappy frames
        unicode_writer.put(ts=time.time(), value=u"hello üñîçodé 宇宙")
        unicode_writer.put(ts=time.time(), value=u"hello 1 µø®é üñîçødé 宇宙")

        unicode_writer.flush()

        unicode_writer.put(ts=time.time(), value=u"hello 2 宇宙 üñîçodé")
        unicode_writer.put(ts=time.time(), value=u"hello 3 宇宙 µø®é üñîçødé")

        unicode_writer.close()

    @setup
    def create_escaped_unicode_data(self):
        escaped_unicode_writer = store.StreamArchiveWriter({'name': "üñîçødé_foo_宇宙_esc"},
                                           datetime.datetime.utcnow(), "/tmp/uni-esc")
        self.escaped_unicode_file_path = escaped_unicode_writer.file_path

        # We want multiple objects, in multiple snappy frames
        escaped_unicode_writer.put(ts=time.time(), value="hello üñîçodé 宇宙")
        escaped_unicode_writer.put(ts=time.time(), value="hello 1 µø®é üñîçødé 宇宙")

        escaped_unicode_writer.flush()

        escaped_unicode_writer.put(ts=time.time(), value="hello 2 宇宙 üñîçodé")
        escaped_unicode_writer.put(ts=time.time(), value="hello 3 宇宙 µø®é üñîçødé")

        escaped_unicode_writer.close()

    @setup
    def build_reader(self):
        self.reader = store.StreamArchiveReader(self.file_path)

    @setup
    def build_unicode_reader(self):
        self.unicode_reader = store.StreamArchiveReader(self.unicode_file_path)

    @setup
    def build_escaped_unicode_reader(self):
        self.escaped_unicode_reader = store.StreamArchiveReader(self.escaped_unicode_file_path)

    def test(self):
        recs = list(self.reader)

        assert_equal(len(recs), 4)
        assert_equal(recs[0]['value'], "hello")
        assert_equal(recs[3]['value'], "hello 3")

    def test_unicode(self):
        u_recs = list(self.unicode_reader)

        assert_equal(len(u_recs), 4)
        assert_equal(u_recs[0][u'value'], u"hello üñîçodé 宇宙")
        assert_equal(u_recs[3][u'value'], u"hello 3 宇宙 µø®é üñîçødé")

    #NOTE: as usual, anything put in as escaped unicode will come out as unicode!
    def test_escaped_unicode(self):
        ue_recs = list(self.escaped_unicode_reader)

        assert_equal(len(ue_recs), 4)
        assert_equal(ue_recs[0]['value'], u"hello üñîçodé 宇宙")
        assert_equal(ue_recs[3]['value'], u"hello 3 宇宙 µø®é üñîçødé")

    @teardown
    def cleanup_data(self):
        shutil.rmtree(os.path.dirname(self.file_path))
        shutil.rmtree(os.path.dirname(unicode_to_ascii_str(self.unicode_file_path)))
        shutil.rmtree(os.path.dirname(self.escaped_unicode_file_path))
