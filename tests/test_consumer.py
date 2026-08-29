import os

import pytest

from src.consumer import DualPathConsumer
from src.gcp_client import GCPClientFactory, GCPMode
from src.publisher import DualPathPublisher, PublishPath


def test_consumer_reconstitutes_fast_path():
    client = GCPClientFactory.get_client(mode=GCPMode.MOCK, project_id="pub-sub-kamo")
    topic_id = "test-consumer-topic"
    bucket = "test-consumer-bucket"

    publisher = DualPathPublisher(
        client=client,
        topic_id=topic_id,
        bucket_name=bucket,
        offload_threshold_bytes=10 * 1024,
    )
    consumer = DualPathConsumer(client=client)

    raw_payload = b"prompt: tell me about pub/sub streaming architecture"
    pub_res = publisher.publish_event(
        event_id="evt-fast-cons-1",
        source="client-agent",
        payload=raw_payload,
        payload_type="text/plain",
    )
    assert pub_res.path == PublishPath.FAST

    # Fetch published message from mock client
    messages = client.get_published_messages(topic_id)
    assert len(messages) == 1
    raw_msg = messages[0]

    reconstituted = consumer.consume_message(
        data=raw_msg.data,
        attributes=raw_msg.attributes,
    )

    assert reconstituted.event_id == "evt-fast-cons-1"
    assert reconstituted.path == PublishPath.FAST
    assert reconstituted.payload == raw_payload
    assert reconstituted.source == "client-agent"
    assert reconstituted.payload_type == "text/plain"


def test_consumer_reconstitutes_gcs_offload_path():
    client = GCPClientFactory.get_client(mode=GCPMode.MOCK, project_id="pub-sub-kamo")
    topic_id = "test-consumer-topic-offload"
    bucket = "test-consumer-bucket"

    publisher = DualPathPublisher(
        client=client,
        topic_id=topic_id,
        bucket_name=bucket,
        offload_threshold_bytes=1024,  # low threshold
    )
    consumer = DualPathConsumer(client=client)

    # Incompressible high entropy data > 1024
    raw_payload = os.urandom(3000)
    pub_res = publisher.publish_event(
        event_id="evt-offload-cons-1",
        source="vision-worker",
        payload=raw_payload,
        payload_type="image/png",
    )
    assert pub_res.path == PublishPath.GCS_OFFLOAD

    messages = client.get_published_messages(topic_id)
    assert len(messages) == 1
    raw_msg = messages[0]

    reconstituted = consumer.consume_message(
        data=raw_msg.data,
        attributes=raw_msg.attributes,
    )

    assert reconstituted.event_id == "evt-offload-cons-1"
    assert reconstituted.path == PublishPath.GCS_OFFLOAD
    assert reconstituted.payload == raw_payload
    assert reconstituted.payload_uri == pub_res.payload_uri


def test_consumer_raises_on_corrupted_flag():
    client = GCPClientFactory.get_client(mode=GCPMode.MOCK, project_id="pub-sub-kamo")
    topic_id = "test-consumer-corrupted"
    bucket = "test-consumer-bucket"

    publisher = DualPathPublisher(client=client, topic_id=topic_id, bucket_name=bucket)
    consumer = DualPathConsumer(client=client)

    publisher.publish_event(
        event_id="evt-corrupt-1",
        source="bad-actor",
        payload=b"malformed event",
        is_corrupted=True,
    )

    messages = client.get_published_messages(topic_id)
    raw_msg = messages[0]

    with pytest.raises(ValueError, match="Corrupted event detected"):
        consumer.consume_message(data=raw_msg.data, attributes=raw_msg.attributes)
