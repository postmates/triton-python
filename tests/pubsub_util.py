from testify import *
from google.cloud import pubsub

def fetch_all(sub):
    results = []

    while True:
        batch = sub.pull(return_immediately=True)
        if (batch == []) or (batch is None):
            break

        results = results + [message for ack_id, message in batch]
        sub.acknowledge([ack_id for ack_id, message in batch])

    return results

def assert_stream_equals(sub, batch, decoder=None):
    pubsub_messages = fetch_all(sub)
    assert_truthy(len(pubsub_messages) >= len(batch))

    messages = [decoder(pubsub_message) if decoder else pubsub_message for pubsub_message in pubsub_messages]
    assert_equal(len(batch), len(messages))

    for message in messages:
        assert_truthy(message in batch)
