import requests
import pubsub_util

from testify import *
from google.cloud import pubsub
from triton import get_stream, stream

class FaultyGCPException(Exception):
    pass

class FaultyGCPStream(stream.GCPStream):


    def __init__(self):
        pass


    def put_many(self, records):
        raise FaultyGCPException("oops")


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
            conn = requests.Session())

        pubsub_config = dict(
            provider='gcp',
            project='foobar',
            topic='baz',
            private_key_file=None)

        composite_config = [
            kinesis_config,
            pubsub_config]

        config = dict(my_composite_stream=composite_config)

        composite_stream = get_stream('my_composite_stream', config)
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
        composite_stream = get_stream('my_composite_stream', config)

        batch = [dict(blob=blob) for blob in ['foobar', 'baz', 'foomatic']]
        composite_stream.put_many(batch)

        #Expect to receive 2x back again
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
        bad_pubsub = FaultyGCPStream()

        composite = stream.CompositeStream([good_pubsub, bad_pubsub])
        assert_raises(FaultyGCPException, composite.put_many, batch)

        pubsub_util.assert_stream_equals(self.sub, batch, decoder=good_pubsub.decode)
