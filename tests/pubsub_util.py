import requests

from testify import assert_truthy, assert_equal
from google.cloud import pubsub


def setup(project='integration', topic_name='foobar'):
    topic_name = 'foobar'

    client = pubsub.Client(project=project, _http=requests.Session())
    topic = client.topic(topic_name)
    topic.create()

    sub = topic.subscription(project)
    sub.create()
    return client, topic, sub


def teardown(client, topic, sub):
    if topic is not None:
        topic.delete()

    if sub is not None:
        sub.delete()


def fetch_all(sub):
    results = []

    while True:
        batch = sub.pull(return_immediately=True)
        if (batch == []) or (batch is None):
            break

        results = results + [message for ack_id, message in batch]
        sub.acknowledge([ack_id for ack_id, message in batch])

    return results


def random_batch(size=100):
    batch = []
    for i in range(0, size):
        record = dict(
            blob='foobar',
            timestamp=i
        )

        batch.append(record)

    return batch


def assert_stream_equals(sub, batch, decoder=None):
    pubsub_messages = fetch_all(sub)
    assert_truthy(len(pubsub_messages) >= len(batch))

    messages = [decoder(pubsub_message) if decoder else pubsub_message for pubsub_message in pubsub_messages]
    assert_equal(len(batch), len(messages))

    for message in messages:
        assert_truthy(message in batch)
