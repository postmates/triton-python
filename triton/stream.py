import base64
import time
import logging

import msgpack

from triton import errors


STREAMS = {
    'rhett_test': {
        'name': 'rhett_test',
        'partition_key': 'pid',
    },
}

ITER_TYPE_LATEST = 'LATEST'
ITER_TYPE_ALL = 'TRIM_HORIZON'
ITER_TYPE_FROM_SEQNUM = 'AFTER_SEQUENCE_NUMBER'

log = logging.getLogger(__name__)


class Record(object):
    def __init__(self, seq_num, shard_id, data):
        self.seq_num = seq_num
        self.shard_id = shard_id
        self.data = data

    @classmethod
    def _decode_record_data(self, record_data):
        return msgpack.unpackb(base64.b64decode(record_data))

    @classmethod
    def from_raw_record(cls, shard_id, raw_record):
        return cls(raw_record['SequenceNumber'], shard_id,
                   cls._decode_record_data(raw_record['Data']))


class StreamIterator(object):
    def __init__(self, stream, shard_id, iterator_type, seq_num=None):
        self.stream = stream
        self.shard_id = shard_id
        self.iterator_type = iterator_type
        self.seq_num = seq_num

        self._iter_value = None
        self.records = []

        self._empty = True

    @property
    def iter_value(self):
        if self._iter_value is None:
            i = self.stream.conn.get_shard_iterator(self.stream.name,
                                                    self.shard_id,
                                                    self.iterator_type,
                                                    self.seq_num)
            self._iter_value = i['ShardIterator']

        return self._iter_value

    def fill(self):
        record_resp = self.stream.conn.get_records(self.iter_value, b64_decode=False)
        for r in record_resp['Records']:
            self.records.append(Record.from_raw_record(self.shard_id, r))

        self._iter_value = record_resp['NextShardIterator']

    def __iter__(self):
        return self

    def next(self):
        if self._empty:
            self.fill()

        try:
            return self.records.pop(0)
        except IndexError:
            self._empty = True
            raise StopIteration


class CombinedStreamIterator(object):
    """Combines multiple StreamIterators for reading from multiple shards
    
    Handles load balancing between streams.
    """
    MIN_FILL_INTERVAL_SECS = 0.250

    def __init__(self, iterators):
        self.iterators = iterators
        self._fill_iterators = set()
        self._running = True
        self._last_wait = None

        self._records = []

    def _wait(self):
        if self._last_wait is None:
            # First fill, no waiting
            self._last_wait = time.time()
            return

        secs_since_fill = time.time() - self._last_wait

        if secs_since_fill <= self.MIN_FILL_INTERVAL_SECS:
            throttle_secs = self.MIN_FILL_INTERVAL_SECS - secs_since_fill
            log.debug("Throttling for %r secs", throttle_secs)
            time.sleep(throttle_secs)

        self.last_wait = time.time()

    def _fill(self):
        if not self._fill_iterators:
            self._wait()
            self._fill_iterators.update(set(self.iterators))

        iter_to_fill = self._fill_iterators.pop()
        log.debug("Checking stream (%s, %s) ", iter_to_fill.stream.name,
                  iter_to_fill.shard_id)
        self._records += list(iter_to_fill)

    def __iter__(self):
        return self

    def next(self):
        # The goal is simple:
        # 1. Deliver any record already loaded before loading any new ones.
        # 2. Don't starve any streams
        # 3. Don't hammer empty shards
        # 4. Don't load more records after stop() is called
        while True:
            try:
                return self._records.pop(0)
            except IndexError:
                if not self.running:
                    raise StopIteration

                self._fill()

    def stop(self):
        self.running = False


class Stream(object):
    def __init__(self, conn, name, partition_key):
        self.conn = conn
        self.name = name
        self.partition_key = partition_key
        self._shard_ids = None

    def _get_connection(self):
        if self._conn is None:
            self._conn = connect_to_region(region)

        return self._conn

    def _partition_key(self, data):
        return unicode(data[self.partition_key])

    @property
    def shard_ids(self):
        if self._shard_ids is None:
            self._shard_ids = []
            stream_describe_resp = self.conn.describe_stream(self.name)

            if stream_describe_resp['StreamDescription']['HasMoreShards']:
                raise NotImplementedError

            for shard in stream_describe_resp['StreamDescription']['Shards']:
                self._shard_ids.append(shard['ShardId'])

        return self._shard_ids

    def _select_shard_ids(self, shard_nums):
        shard_ids = []
        if shard_nums:
            for shard_num in shard_nums:
                try:
                    shard_ids.append(self.shard_ids[shard_num])
                except IndexError:
                    raise errors.ShardNotFoundError()
        else:
            shard_ids = self.shard_ids

        return shard_ids

    def emit(self, **kwargs):
        data = msgpack.packb(kwargs)
        partition_key = self._partition_key(kwargs)
        resp = self.conn.put_record(self.name, data, self._partition_key(kwargs))

        return resp['ShardId'], resp['SequenceNumber']

    def build_iterator_for_all(self, shard_nums=None):
        return self._build_iterator(ITER_TYPE_ALL, shard_nums, None)

    def build_iterator_from_seqnum(self, seqnum, shard_num):
        return self._build_iterator(ITER_TYPE_FROM_SEQNUM, [shard_num], seq_num)

    def build_iterator_from_latest(self, shard_nums=None):
        return self._build_iterator(ITER_TYPE_LATEST, shard_nums, None)

    def _build_iterator(self, iterator_type, shard_nums, seq_num):
        all_iters = []
        for shard_id in self._select_shard_ids(shard_nums):
            i = StreamIterator(self, shard_id, iterator_type, seq_num)
            all_iters.append(i)

        return CombinedStreamIterator(all_iters)


def connect_to_region(region_name, **kw_params):
    # NOTE(rhettg): current version of boto doesn't know about us-west-1 for
    # kinesis
    region = boto.regioninfo.RegionInfo(
                name=region_name,
                endpoint='kinesis.{}.amazonaws.com'.format(region_name),
                connection_cls=boto.kinesis.layer1.KinesisConnection
            )

    return region.connect(**kw_params)


def get_stream(stream_name, region_name):
    config = STREAMS[stream_name]

    conn = connect_to_region(region)

    return Stream(conn, config['name'], config['partition_key'])
