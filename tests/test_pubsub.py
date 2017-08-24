import os

import requests

from testify import *
from google.cloud import pubsub
from triton import stream

BATCH_MAX_MSGS = stream.GCPStream.BATCH_MAX_MSGS

dir_path = os.path.dirname(os.path.realpath(__file__))

class GCPTest(TestCase):


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
        return stream.GCPStream(
            project = self.project,
            topic = self.topic_name,
            private_key_file = None
        )


    def fetch_all(self):
        results = []

        while True:
            batch = self.sub.pull(return_immediately=True)
            if (batch == []) or (batch is None):
                break

            results = results + [message for ack_id, message in batch]
            self.sub.acknowledge([ack_id for ack_id, message in batch])     

        return results


    def test_construction(self):
        stream = self.get_stream()
        assert_truthy(True) # Just ensure we survive


    def test_publish_oneoff(self):
        stream = self.get_stream()
        record = dict(
            blob = 'foobar',
            timestamp = 10234
        )
        stream.put(**record)

        messages = self.fetch_all()
        assert_equal(len(messages), 1)
        for message in messages:
            unpacked = stream.decode(message)
            assert_equal(record, unpacked)


    def test_publish_batch(self):
        stream = self.get_stream()

        batch = []
        for i in range(0, 101):
            record = dict(
                blob = 'foobar',
                timestamp = i
            )
            batch.append(record)

        stream.put_many(batch)

        pubsub_messages = self.fetch_all()
        assert_truthy(len(pubsub_messages) >= len(batch))

        # Pubsub is at least once publishing, so we do some work to dedup a list of dicts.
        message_dicts = [stream.decode(pubsub_message) for pubsub_message in pubsub_messages]
        messages = [dict(tup) for tup in set([tuple(message_dict.items()) for message_dict in message_dicts])]
        assert_equal(len(batch), len(messages))

        for message in messages:
            assert_truthy(message in batch)

    def test_publish_batch_larger_than_limits(self):
        stream = self.get_stream()

        batch = []
        for i in range(0, 2 * BATCH_MAX_MSGS + 10):
            record = dict(
                blob = 'foobar',
                timestamp = i
            )
            batch.append(record)

        stream.put_many(batch)

        pubsub_messages = self.fetch_all()
        assert_truthy(len(pubsub_messages) >= len(batch))

        # Pubsub is at least once publishing, so we do some work to dedup a list of dicts.
        message_dicts = [stream.decode(pubsub_message) for pubsub_message in pubsub_messages]
        messages = [dict(tup) for tup in set([tuple(message_dict.items()) for message_dict in message_dicts])]
        assert_equal(len(batch), len(messages))

        for message in messages:
            assert_truthy(message in batch)
