import time
import io
import os.path

import msgpack
import snappy

MAX_BUFFER_SIZE = 1024 * 1024


class StreamArchiveWriter(object):

    def __init__(self, name, base_dt, base_path):
        self.name = name
        self.base_dt = base_dt
        self.ts = time.time()
        self.base_path = base_path

        self.buffer = io.BytesIO()
        self.writer = None

    @property
    def file_path(self):
        date_str = self.base_dt.strftime('%Y%m%d')
        file_name = "{}-archive-{}.tri".format(self.name, int(self.ts))
        return os.path.join(self.base_path, date_str, file_name)

    def put(self, **kwargs):
        data = msgpack.packb(kwargs)

        self.buffer.write(data)

        if self.buffer.tell() >= MAX_BUFFER_SIZE:
            self.flush()

    def flush(self):
        if self.buffer.tell() == 0:
            return

        if self.writer is None:
            try:
                os.makedirs(os.path.dirname(self.file_path))
            except OSError:
                pass

            self.writer = io.open(self.file_path, mode="wb")
            self.snappy_compressor = snappy.StreamCompressor()

        data = self.snappy_compressor.add_chunk(self.buffer.getvalue())
        self.writer.write(data)

        # Reset our buffer
        self.buffer.truncate(0)
        self.buffer.seek(0)

    def close(self):
        self.flush()

        if self.writer:
            self.writer.close()
            self.writer = None


def decoder(stream):
    snappy_stream = snappy.StreamDecompressor()
    unpacker = msgpack.Unpacker()
    for data in stream:
        buf = snappy_stream.decompress(data)
        if buf:
            unpacker.feed(buf)
            # Oh to have yield from
            for rec in unpacker:
                yield rec


class StreamArchiveReader(object):

    def __init__(self, file_path):
        self.file_path = file_path
        self.reader = None
        self.records = []
        self.snappy_stream = snappy.StreamDecompressor()
        self.unpacker = msgpack.Unpacker()

    def _open(self):
        if self.reader is None:
            self.reader = io.open(self.file_path, mode="rb")

    def _fill(self):
        self._open()
        while True:
            data = self.reader.read(io.DEFAULT_BUFFER_SIZE)
            if not data:
                return

            buf = self.snappy_stream.decompress(data)
            if buf:
                self.unpacker.feed(buf)
                self.records += list(self.unpacker)
                # We found some records, we can get out of here.
                return

    def __iter__(self):
        return self

    def next(self):
        if len(self.records) == 0:
            self._fill()

        try:
            return self.records.pop(0)
        except IndexError:
            raise StopIteration
