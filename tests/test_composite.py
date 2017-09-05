import requests
import triton
import pubsub_util

from testify import *
from google.cloud import pubsub
from triton import stream


class FaultyGCPException(Exception):
    pass


class FaultyGCPStream(stream.GCPStream):

    class EmptyIterator(object):

        def __iter__(self):
            return self

        def __next__(self):
            raise StopIteration

        def next(self):
            return self.__next__()

    def __init__(self, raise_exception=True, **kwargs):
        self.raise_exception = raise_exception
        super(FaultyGCPStream, self).__init__(**kwargs)

    def put_many(self, records):
        if self.raise_exception:
            raise FaultyGCPException("oops")

    def __iter__(self):
        return self.build_iterator_from_latest()

    def build_iterator_from_latest(self):
        return FaultyGCPStream.EmptyIterator()


class CompositeTest(TestCase):

    @setup
    def setup(self):
        self.project = 'integration'
        self.topic_name = 'foobar'

        self.client = pubsub.Client(project=self.project, _http=requests.Session())
        self.topic = self.client.topic(self.topic_name)
        self.topic.create()

        self.sub = self.topic.subscription(self.project)
        self.sub.create()

    @teardown
    def cleanup(self):
        self.topic.delete()
        self.sub.delete()

    def test_construction(self):
        kinesis_config = dict(
            name='my_kinesis_stream',
            partition_key='value',
            region='us-west-1',
            conn=requests.Session())

        pubsub_config = dict(
            provider='gcp',
            project='foobar',
            topic='baz',
            private_key_file=None)

        composite_config = [
            kinesis_config,
            pubsub_config]

        config = dict(my_composite_stream=composite_config)
        composite_stream = triton.get_stream('my_composite_stream', config)

        assert_equal(type(composite_stream), stream.CompositeStream)
        assert_equal(len(composite_stream.streams), len(composite_config))
        assert_equal(type(composite_stream.streams[0]), stream.AWSStream)
        assert_equal(type(composite_stream.streams[1]), stream.GCPStream)

    def test_multiplexing(self):
        pubsub_config = dict(
            provider='gcp',
            project=self.project,
            topic=self.topic_name,
            private_key_file=None)

        composite_config = [
            dict(**pubsub_config),
            dict(**pubsub_config)
        ]

        config = dict(my_composite_stream=composite_config)
        composite_stream = triton.get_stream('my_composite_stream', config)

        batch = [dict(blob=blob) for blob in ['foobar', 'baz', 'foomatic']]
        composite_stream.put_many(batch)

        # Expect to receive 2x back again
        pubsub_util.assert_stream_equals(self.sub, batch + batch, decoder=composite_stream.streams[0].decode)

    def test_exception_results_in_loss_of_parity(self):
        """
            Existing retry logic is lossy, composite streams
            carry this forward (for now) allowing events to be published
            in one stream and go missing in the other.

            In the future, when at least once delivery is achieved then this
            test will need to be reworked.
        """
        batch = [dict(blob=blob) for blob in ['foobar', 'baz', 'foomatic']]
        pubsub_config = dict(
            project=self.project,
            topic=self.topic_name,
            private_key_file=None)
        good_pubsub = stream.GCPStream(**pubsub_config)
        bad_pubsub = FaultyGCPStream(**pubsub_config)

        composite = stream.CompositeStream([good_pubsub, bad_pubsub])
        assert_raises(FaultyGCPException, composite.put_many, batch)

        pubsub_util.assert_stream_equals(self.sub, batch, decoder=good_pubsub.decode)


class CompositeIteratorTest(TestCase):

    @setup
    def setup(self):
        self.project = 'integration'
        self.topic_name = 'foobar'

        self.client = pubsub.Client(project=self.project, _http=requests.Session())
        self.topic = self.client.topic(self.topic_name)
        self.topic.create()

        self.sub = self.topic.subscription(self.project)
        self.sub.create()

    @teardown
    def cleanup(self):
        self.topic.delete()
        self.sub.delete()

    def get_stream(self):
        pubsub_config = dict(
            provider='gcp',
            project=self.project,
            topic=self.topic_name,
            private_key_file=None)

        composite_config = [
            dict(**pubsub_config),
            dict(**pubsub_config)
        ]

        config = dict(my_composite_stream=composite_config)
        return triton.get_stream('my_composite_stream', config)

    def test_iterator_latest(self):
        stream = self.get_stream()
        combo_iter = stream.build_iterator_from_latest()

        batch = pubsub_util.random_batch()
        stream.put_many(batch)

        acc1 = []
        acc2 = []
        for record1, record2 in combo_iter:
            acc1.append(record1)
            acc2.append(record2)

        for message in acc1:
            assert_truthy(message in acc2)

        for message in acc2:
            assert_truthy(message in acc1)

        for message in batch:
            assert_truthy(message in acc1)

    def test_iterator_checkpoint(self):
        stream = self.get_stream()

        subscription_id1 = 'custom-subscription-id-1'
        subscription1 = self.topic.subscription(subscription_id1)
        subscription1.create()

        subscription_id2 = 'custom-subscription-id-2'
        subscription2 = self.topic.subscription(subscription_id2)
        subscription2.create()

        first_batch = pubsub_util.random_batch(size=15)
        stream.put_many(first_batch)
        pubsub_util.fetch_all(subscription1)
        pubsub_util.fetch_all(subscription2)

        second_batch = pubsub_util.random_batch()
        stream.put_many(second_batch)
        combo_iter = stream.build_iterator_from_checkpoint([subscription_id1, subscription_id2])

        acc1 = []
        acc2 = []
        for record1, record2 in combo_iter:
            acc1.append(record1)
            acc2.append(record2)

        subscription1.delete()
        subscription2.delete()

        for message in acc1:
            assert_truthy(message in acc2)

        for message in acc2:
            assert_truthy(message in acc1)

        for message in second_batch:
            assert_truthy(message in acc1)

    def test_iterator_favors_longest(self):
        batch = pubsub_util.random_batch(size=15)
        pubsub_config = dict(
            project=self.project,
            topic=self.topic_name,
            private_key_file=None)
        good_pubsub = stream.GCPStream(**pubsub_config)
        bad_pubsub = FaultyGCPStream(raise_exception=False, **pubsub_config)

        composite = stream.CompositeStream([good_pubsub, bad_pubsub])
        composite_iter = composite.build_iterator_from_latest()

        composite.put_many(batch)

        for good_item, bad_item in composite_iter:
            assert_truthy(good_item in batch)
            assert_equal(bad_item, None)
