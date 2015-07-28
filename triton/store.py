import time
import io
import datetime
import os.path
import re
import logging
import itertools

import msgpack
import snappy
import boto.s3
from boto.s3.connection import OrdinaryCallingFormat

MAX_BUFFER_SIZE = 1024 * 1024

log = logging.getLogger(__name__)


class StreamArchiveWriter(object):

    def __init__(self, stream_config, base_dt, base_path):
        self.config = stream_config
        self.base_dt = base_dt
        self.ts = time.time()
        self.base_path = base_path

        self.buffer = io.BytesIO()
        self.writer = None

    @property
    def file_path(self):
        date_str = self.base_dt.strftime('%Y%m%d')
        file_name = "{}-archive-{}.tri".format(self.config['name'],
                                               int(self.ts))
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
    """Generator that processes data from the stream (by iterating) and yields
    triton records"""
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

    def __iter__(self):
        f = io.open(self.file_path, mode="rb")
        return decoder(f)


class ArchiveFile(object):
    """Represents a stream archive stored in S3 file

    This mostly translates between file names and the properties that we can
    infer from the names.
    """

    def __init__(self, stream_name, day, ts, shard=None):
        self.stream_name = stream_name
        self.day = day
        self.ts = ts
        self.shard = shard
        self.is_archive = bool(shard is None)

    @property
    def file_path(self):
        return "{}/{}-{}-{}.tri".format(
            self.day.strftime('%Y%m%d'), self.stream_name,
            self.shard or 'archive', self.ts)

    def s3_key(self, bucket):
        return boto.s3.key.Key(bucket, name=self.file_path)

    def open(self, bucket):
        """Create a iterable stream of data from the log file.
        """
        return decoder(self.s3_key(bucket))

    @classmethod
    def from_s3_key(cls, key):
        match = re.match(r"(?P<day>\d{8})\/(?P<stream>.+)\-(?P<ts>\d+)\.tri$",
                         key.name)
        if match is None:
            raise ValueError(key)

        shard = None
        match_info = match.groupdict()
        if match_info['stream'].endswith('-archive'):
            stream_name = match_info['stream'][:-len('-archive')]
        else:
            stream_name = match_info['stream'][:match_info['stream'].find(
                '-shardId')]
            shard = re.match(r".*(shardId-\d+)$", match_info['stream']).group(1)
            if not shard:
                raise ValueError(stream_name)

        ts = int(match_info['ts'])
        dt = datetime.datetime.strptime(match_info['day'], '%Y%m%d')
        return cls(stream_name, dt.date(), ts, shard)


def inclusive_date_range(start_dt, end_dt):
    start_date = start_dt.date()
    end_date = end_dt.date()

    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += datetime.timedelta(days=1)


def s3_prefix_for_date_and_name(date, stream_name):
    date_str = date.strftime('%Y%m%d')
    return "{}/{}-".format(date_str, stream_name)


def find_log_files_in_s3(bucket, stream_name, start_dt, end_dt):
    prefixes = []
    for dt in inclusive_date_range(start_dt, end_dt):
        prefixes.append(s3_prefix_for_date_and_name(dt, stream_name))

    all_files = []
    for pf in prefixes:
        day_files = []
        for key in bucket.list(pf):
            try:
                af = ArchiveFile.from_s3_key(key)
                # If the file is marked as an archive, it's the only file we
                # should pay attention to. It represents a full dump of the day.
                if af.is_archive:
                    day_files = [af]
                    break

                day_files.append(af)
            except ValueError as e:
                log.warning("S3 key %r not a log file: %r", key, e)
                continue

        day_files.sort(key=lambda f: f.ts)
        all_files += day_files

    return all_files


def open_bucket(bucket_name, region_name):
    # We need to specify calling_format to get around bugs in having a '.' in
    # the bucket name
    conn = boto.s3.connect_to_region(region_name,
                                     calling_format=OrdinaryCallingFormat())

    return conn.get_bucket(bucket_name)


def stream_from_s3_store(bucket, stream_config, start_dt, end_dt):
    log_files = find_log_files_in_s3(
        bucket, stream_config['name'], start_dt, end_dt)

    streams = []
    for lf in log_files:
        data_stream = lf.open(bucket)
        streams.append(data_stream)

    return itertools.chain(*streams)
