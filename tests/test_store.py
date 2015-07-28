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
        self.stream = store.StreamArchiveWriter("foo", self.dt, "/tmp")

    def test(self):
        assert self.stream.file_path.startswith("/tmp/20150724/foo-archive-"), self.stream.file_path
        assert self.stream.file_path.endswith('.tri')


class StreamArchiveWriterWriteTest(TestCase):
    @setup
    def build_stream(self):
        self.dt = datetime.datetime(2015, 7, 24)
        self.stream = store.StreamArchiveWriter("foo", self.dt, "/tmp")

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
