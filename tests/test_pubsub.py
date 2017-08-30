import os

import triton
import pubsub_util
import google.cloud.exceptions

from testify import *
from triton import stream

BATCH_MAX_MSGS = stream.GCPStream.BATCH_MAX_MSGS

dir_path = os.path.dirname(os.path.realpath(__file__))


class GCPTest(TestCase):

    @setup
    def setup(self):
        self.project = 'integration'
        self.topic_name = 'foobar'
        self.client, self.topic, self.sub = pubsub_util.setup(self.project, self.topic_name)

    @teardown
    def cleanup(self):
        pubsub_util.teardown(
            self.client,
            self.topic,
            self.sub)

    def get_stream(self):
        pubsub_config = dict(
            provider='gcp',
            project=self.project,
            topic=self.topic_name,
            private_key_file=None)

        config = dict(
            test_stream=pubsub_config)

        return triton.get_stream('test_stream', config)

    def test_construction(self):
        self.get_stream()
        assert_truthy(True) # Just ensure we survive

    def test_publish_oneoff(self):
        stream = self.get_stream()
        record = dict(
            blob='foobar',
            timestamp=10234
        )
        stream.put(**record)

        pubsub_util.assert_stream_equals(self.sub, [record], decoder=stream.decode)

    def test_publish_batch(self):
        stream = self.get_stream()

        batch = []
        for i in range(0, 101):
            record = dict(
                blob='foobar',
                timestamp=i
            )
            batch.append(record)

        stream.put_many(batch)
        pubsub_util.assert_stream_equals(self.sub, batch, decoder=stream.decode)

    def test_publish_batch_larger_than_limits(self):
        stream = self.get_stream()

        batch = pubsub_util.random_batch(2 * BATCH_MAX_MSGS + 10)
        stream.put_many(batch)

        pubsub_util.assert_stream_equals(self.sub, batch, decoder=stream.decode)

    def test_publish_batch_has_too_many_bytes(self):
        stream = self.get_stream()

        batch = pubsub_util.random_batch(2 * BATCH_MAX_MSGS + 10)
        stream.put_many(batch)

        pubsub_util.assert_stream_equals(self.sub, batch, decoder=stream.decode)


class GCPStreamIteratorTest(TestCase):

    @setup
    def setup(self):
        self.project = 'integration'
        self.topic_name = 'foobar'
        self.client, self.topic, self.sub = pubsub_util.setup(self.project, self.topic_name)

    @teardown
    def cleanup(self):
        pubsub_util.teardown(
            self.client,
            self.topic,
            self.sub)

    def get_stream(self):
        pubsub_config = dict(
            provider='gcp',
            project=self.project,
            topic=self.topic_name,
            private_key_file=None)

        config = dict(
            test_stream=pubsub_config)

        return triton.get_stream('test_stream', config)

    def test_latest_iterator_is_default(self):
        stream = self.get_stream()

        batch = pubsub_util.random_batch()
        stream.put_many(batch)

        # Default iterator should be at the HEAD
        # of the stream.  Hence, the previous batch
        # should go unnoticed.
        for msg in stream:
            assert_truthy(False)

    def test_latest_iterator(self):
        stream = self.get_stream()
        latest_iter = stream.build_iterator_from_latest()

        batch = pubsub_util.random_batch()
        stream.put_many(batch)

        for msg in latest_iter:
            batch.remove(msg)

            if len(batch) == 0:
                break

        assert_equal(latest_iter.prefetch, [])

    def test_checkpoint_iterator(self):
        stream = self.get_stream()

        # Setup a throw-away subscription.
        subscription_id = 'bogus-subscription-id'
        sub = self.topic.subscription(subscription_id)
        sub.create()

        # Send a first round of messages to our temp
        # subscription.
        first_batch = pubsub_util.random_batch(15)
        stream.put_many(first_batch)

        # Fetches and acks the first batch of messages.
        pubsub_util.assert_stream_equals(
            sub,
            first_batch,
            decoder=stream.decode)

        # Publish a second batch of messages.
        second_batch = pubsub_util.random_batch()
        stream.put_many(second_batch)

        # Ensure that our checkpoint iterator resumes from
        # the second batch of messages.
        checkpoint_iter = stream.build_iterator_from_checkpoint(subscription_id)
        for msg in checkpoint_iter:
            second_batch.remove(msg)

            if len(second_batch) == 0:
                break

        sub.delete()

    def test_checkpoint_iterator_raises_when_sub_dne(self):
        stream = self.get_stream()
        checkpoint_iter = stream.build_iterator_from_checkpoint('bogus-stream-id')
        assert_raises(google.cloud.exceptions.NotFound, checkpoint_iter.next)
