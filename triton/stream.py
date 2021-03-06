from __future__ import unicode_literals
import base64
import time
import logging

import msgpack
import boto.kinesis.layer1
from boto.kinesis.exceptions import ProvisionedThroughputExceededException
from boto.exception import BotoServerError
import boto.regioninfo

from triton import errors
from triton.checkpoint import TritonCheckpointer
from triton.encoding import msgpack_encode_default, unicode_to_ascii_str, ascii_to_unicode_str

MIN_POLL_INTERVAL_SECS = 1.0
KINESIS_MAX_LENGTH = 500  # Can't write more than 500 records at a time
KINESIS_MAX_RETRYS = 2  # Kinesis 'InternalFailure' retry attempts

ITER_TYPE_LATEST = 'LATEST'
ITER_TYPE_ALL = 'TRIM_HORIZON'
ITER_TYPE_FROM_SEQNUM = 'AFTER_SEQUENCE_NUMBER'
ITER_TYPE_FROM_CHECKPOINT = 'FROM_CHECKPOINT'

log = logging.getLogger(__name__)


class Record(object):
    __slots__ = ['shard_id', 'seq_num', 'data']

    def __init__(self, shard_id, seq_num, data):
        self.shard_id = shard_id
        self.seq_num = seq_num
        self.data = data

    @classmethod
    def _decode_record_data(cls, record_data):
        return msgpack.unpackb(base64.b64decode(record_data), encoding='utf-8')

    @classmethod
    def from_raw_record(cls, shard_id, raw_record):
        return cls(shard_id, raw_record['SequenceNumber'],
                   cls._decode_record_data(raw_record['Data']))

    def __repr__(self):
        return u'<Record {} {}>'.format(self.shard_id, self.seq_num)


class StreamIterator(object):
    """Handles the workflow of reading from a shard

    Args:
        stream - Instance of Stream()
        shard_id - Which shard we're reading from (shardId-00001)
        iterator_type - How we're iterating, like 'LATEST' or 'TRIM_HORIZON'
        seq_num - For 'AFTER_SEQUENCE_NUMBER' type, tells us where to start.
        fallback_iterator_type - For 'AFTER_SEQUENCE_NUMBER', if there is no
            checkpoint availible, create an iterator with this iterator_type
            instead
    """

    def __init__(
        self, stream, shard_id, iterator_type,
        seq_num=None, fallback_iterator_type=ITER_TYPE_ALL
    ):
        self.stream = stream
        self.shard_id = shard_id
        self.iterator_type = iterator_type
        self.seq_num = seq_num
        self.fallback_iterator_type = fallback_iterator_type

        self._iter_value = None
        self.records = []

        self._empty = True
        self.behind_latest_secs = None

        self._checkpointer = None
        self.last_seq_num = None

    @property
    def checkpointer(self):
        if self._checkpointer is None:
            self._checkpointer = TritonCheckpointer(self.stream.name)
        return self._checkpointer

    def checkpoint(self):
        if self.last_seq_num is not None:
            self.checkpointer.checkpoint(self.shard_id, self.last_seq_num)

    @property
    def iter_value(self):
        if self._iter_value is None:
            if self.iterator_type == ITER_TYPE_FROM_CHECKPOINT:
                self.seq_num = self.checkpointer.last_sequence_number(
                    self.shard_id)
                if self.seq_num is None:
                    self.iterator_type = self.fallback_iterator_type
                else:
                    self.iterator_type = ITER_TYPE_FROM_SEQNUM
            log.info(
                "Creating iterator %r", (
                    self.stream.name, self.shard_id,
                    self.iterator_type, self.seq_num))
            i = self.stream.conn.get_shard_iterator(
                self.stream.name, self.shard_id, self.iterator_type,
                self.seq_num)
            self._iter_value = i['ShardIterator']

        return self._iter_value

    def fill(self):
        try:
            record_resp = self.stream.conn.get_records(self.iter_value,
                                                       b64_decode=False)
        except ProvisionedThroughputExceededException:
            # We set our poll interval to be conservative (and match
            # recommended kinesis libraries) but it's always possible that
            # insufficient capacity has been provisioned. Most likely, this is
            # transient and something we can recover from. But we should
            # complain loudly.
            log.error("Rate exceeded for %r:%r", self.stream.name,
                      self.shard_id)
            return

        behind_latest_secs = record_resp['MillisBehindLatest'] / 1000.0
        log.debug("Found %d records filling %r (behind %d secs)",
                  len(record_resp['Records']), self, int(behind_latest_secs))

        if self.behind_latest_secs > 0.0 and behind_latest_secs == 0:
            log.info("%r has caught up with latest", self)

        if self.behind_latest_secs is None:
            log.info("%r behind latest by %ds", self, behind_latest_secs)

        self.behind_latest_secs = behind_latest_secs

        for rec in record_resp['Records']:
            self.records.append(Record.from_raw_record(self.shard_id, rec))
            self._empty = False

        if record_resp.get('NextShardIterator'):
            self._iter_value = record_resp['NextShardIterator']
        else:
            # If a next iterator isn't provided, it probably indicates the
            # shard has be ended due to split or merge.
            raise errors.EndOfShardError()

    def __iter__(self):
        return self

    def next(self):
        if self._empty:
            self.fill()

        try:
            rec = self.records.pop(0)
            self.last_seq_num = rec.seq_num
            return rec

        except IndexError:
            self._empty = True
            raise StopIteration

    def __repr__(self):
        return u'<StreamIterator {} {} ({})>'.format(
            self.stream.name, self.shard_id, self.iterator_type)


class CombinedStreamIterator(object):
    """Combines multiple StreamIterators for reading from multiple shards

    Handles load balancing between streams.
    """

    def __init__(self, iterators):
        self.iterators = iterators
        self._fill_iterators = set()
        self._running = True
        self._last_wait = None

        self.last_iterator = None
        self.last_seq_num = None

        self._records = []

    def _wait(self):
        if self._last_wait is None:
            # First fill, no waiting
            self._last_wait = time.time()
            return

        secs_since_fill = time.time() - self._last_wait

        throttle_secs = MIN_POLL_INTERVAL_SECS - secs_since_fill
        if throttle_secs > 0.0:
            throttle_secs = MIN_POLL_INTERVAL_SECS - secs_since_fill
            log.debug("Throttling for %f secs", throttle_secs)
            time.sleep(throttle_secs)

        self._last_wait = time.time()

    def _fill(self):
        if not self._fill_iterators:
            self._wait()
            self._fill_iterators.update(set(self.iterators))

        iter_to_fill = self._fill_iterators.pop()
        self.last_iterator = iter_to_fill
        log.debug("Checking stream (%s, %s) ", iter_to_fill.stream.name,
                  iter_to_fill.shard_id)
        self._records += list(iter_to_fill)

    def __iter__(self):
        return self

    def next(self):
        # The goals is simple:
        # 1. Deliver any record already loaded before loading any new ones.
        # 2. Don't starve any streams
        # 3. Don't hammer empty shards
        # 4. Don't load more records after stop() is called
        while True:
            try:
                rec = self._records.pop(0)
                self.last_seq_num = rec.seq_num
                return rec
            except IndexError:
                if not self._running:
                    raise StopIteration

                self._fill()

    def stop(self):
        self._running = False

    def checkpoint(self):
        for this_iterator in set(self.iterators):
            if this_iterator != self.last_iterator:
                # we've already processed all data pulled from this iterator
                this_iterator.checkpoint()
            else:
                # we could be in the middle of processing this data
                # checkpoint only as far as the data retrieved via next()
                this_iterator.checkpointer.checkpoint(
                    this_iterator.shard_id, self.last_seq_num)


class Stream(object):

    def __init__(self, conn, name, partition_key):
        self.conn = conn
        self.name = ascii_to_unicode_str(name)
        self.partition_key = ascii_to_unicode_str(partition_key)
        self._shard_ids = None

    #NOTE: explanation of the convoluted try blocks in _partition_key!
    #when looking up the partition_key in the data, we need to first check
    #the unicode version of the key, then check the escaped/ascii version.
    #If the key truly is missing, we want to swallow the second KeyError and
    #return the first KeyError that points to self.partition_key and not the
    #copy created by calling unicode_to_ascii_str
    def _partition_key(self, data):
        try:
            return ascii_to_unicode_str(data[self.partition_key])
        except KeyError as original_error:
            #supports a user putting escaped unicode data in, still returns
            #unicode partition key
            try:
                return ascii_to_unicode_str(data[unicode_to_ascii_str(self.partition_key)])
            except KeyError:
                pass
            raise original_error

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

    def put(self, **kwargs):
        try:
            data = msgpack.packb(kwargs)
        except TypeError:
            # If we fail to serialize our context, we can try again with an
            # enhanced packer (it's slower though)
            data = msgpack.packb(kwargs, default=msgpack_encode_default)
        resp = _call_and_retry(
            self.conn.put_record,
            self.name, data,
            self._partition_key(kwargs)
        )

        try:
            return resp['ShardId'], resp['SequenceNumber']
        except KeyError:
            raise errors.KinesisError((
                'An unknown error occurred for stream {},'
                ' response was {}').format(self.name, resp))

    def put_many(self, records):
        data_recs = []
        for r in records:
            try:
                data = msgpack.packb(r)
            except TypeError:
                # If we fail to serialize our context, we can try again with an
                # enhanced packer (it's slower though)
                data = msgpack.packb(r, default=msgpack_encode_default)
            data_recs.append({
                'Data': data,
                'PartitionKey': self._partition_key(r),
            })

        return self._put_many_packed(data_recs)

    def _put_many_packed(self, records, retry_count=0, b64_encode=True):
        """Re-usable method for already packed messages,
            used here and by tritond for non-blocking writes

        args:
            records - list() of dicts with the following structure:
                {
                    'Data': msgpacked-data,
                    'PartitionKey': partition_key of the record
                }
            retry_count - number of retries for individual failed records
            b64_encode  - parameter to boto kinesis library included b/c boto
                          changes the data record itself. We need to pass false
                          to prevent re-encoding on failure.
        """
        resp_value = []
        num_records = len(records)
        max_record = 0
        retry_records = []

        while max_record < num_records:
            # iterate through all the records MAX_LENTH records at a time

            # Note that the following _call_and_retry will only
            # retry for 500 server errors;
            # ProvisionedThroughputExceededException
            # will not happen for put_records
            resp = _call_and_retry(
                self.conn.put_records,
                records[max_record:max_record + KINESIS_MAX_LENGTH],
                self.name,
                b64_encode=b64_encode)

            for idx, r in enumerate(resp['Records']):
                # Per http://docs.aws.amazon.com/kinesis/
                # ...latest/APIReference/API_PutRecordsResultEntry.html
                # ProvisionedThroughputExceededException and InternalFailure
                # happen on a message by message basis.
                # Check for individual failed messages and queue for retry
                try:
                    resp_value.append((r['ShardId'], r['SequenceNumber']))
                except KeyError:
                    retry_records.append(records[max_record + idx])
            max_record += KINESIS_MAX_LENGTH
        if retry_records:
            # if any individual messages have failed, retry them now
            if retry_count > KINESIS_MAX_RETRYS:
                raise errors.KinesisPutManyError(
                    'Failed to put_many records to Kinesis',
                    failed_data=retry_records)
            else:
                time.sleep(2 ** retry_count * .1)
                resp_value.extend(self._put_many_packed(
                    retry_records,
                    retry_count=retry_count + 1,
                    b64_encode=False)
                )
        return resp_value

    def build_iterator_for_all(self, shard_nums=None):
        shard_ids = self._select_shard_ids(shard_nums)
        return self._build_iterator(ITER_TYPE_ALL, shard_ids, None)

    def build_iterator_from_seqnum(self, shard_id, seq_num):
        return self._build_iterator(ITER_TYPE_FROM_SEQNUM, [shard_id], seq_num)

    def build_iterator_from_latest(self, shard_nums=None):
        shard_ids = self._select_shard_ids(shard_nums)
        return self._build_iterator(ITER_TYPE_LATEST, shard_ids, None)

    def build_iterator_from_checkpoint(self, shard_nums=None):
        shard_ids = self._select_shard_ids(shard_nums)
        return self._build_iterator(ITER_TYPE_FROM_CHECKPOINT, shard_ids, None)

    def _build_iterator(self, iterator_type, shard_ids, seq_num):
        all_iters = []
        for shard_id in shard_ids:
            i = StreamIterator(self, shard_id, iterator_type, seq_num)
            all_iters.append(i)

        return CombinedStreamIterator(all_iters)


def connect_to_region(region_name, **kw_params):
    # NOTE(rhettg): current version of boto doesn't know about us-west-1 for
    # kinesis
    region = boto.regioninfo.RegionInfo(
        name=region_name,
        endpoint='kinesis.{}.amazonaws.com'.format(region_name),
        connection_cls=boto.kinesis.layer1.KinesisConnection)

    return region.connect(**kw_params)


def get_stream(stream_name, config):
    s_config = config.get(stream_name)
    if not s_config:
        raise errors.StreamNotConfiguredError()

    conn = connect_to_region(s_config.get('region', 'us-east-1'))

    return Stream(conn, s_config['name'], s_config['partition_key'])


def _call_and_retry(kinesis_function, *args, **kwargs):
    """
    Retry Logic for generic kinesis calls.

    This code follows the exponetial backoff pattern suggested by
    http://docs.aws.amazon.com/general/latest/gr/api-retries.html
    """
    retries = 0
    while True:
        try:
            return kinesis_function(*args, **kwargs)
        except BotoServerError as e:
            if retries >= KINESIS_MAX_RETRYS:
                raise e
            if (
                    e.status / 100 == 5  # 5xx error
                    or
                    type(e) == ProvisionedThroughputExceededException
            ):
                time.sleep(2 ** retries * .1)
                retries += 1
            else:
                raise e
