import os

from src.gcp_client import GCPClientFactory, GCPMode
from src.publisher import DualPathPublisher, PublishPath


def test_fast_path_routing_under_threshold():
    client = GCPClientFactory.get_client(mode=GCPMode.MOCK, project_id="pub-sub-kamo")
    topic_id = "events-topic"
    bucket_name = "pubsub-kamo-payloads"

    publisher = DualPathPublisher(
        client=client,
        topic_id=topic_id,
        bucket_name=bucket_name,
        offload_threshold_bytes=10 * 1024,
    )

    small_payload = b"short prompt text" * 20
    result = publisher.publish_event(
        event_id="evt-small-1",
        source="claude-code-cli",
        payload=small_payload,
        payload_type="text/plain",
    )

    assert result.path == PublishPath.FAST
    assert result.payload_uri == ""
    assert result.uncompressed_bytes == len(small_payload)
    assert result.compressed_bytes < len(small_payload)

    messages = client.get_published_messages(topic_id)
    assert len(messages) == 1
    assert messages[0].attributes["path"] == "fast"
    assert messages[0].attributes["content-encoding"] == "zstd"


def test_large_payload_gcs_offload_routing():
    client = GCPClientFactory.get_client(mode=GCPMode.MOCK, project_id="pub-sub-kamo")
    client.clear_topic("events-topic")
    topic_id = "events-topic"
    bucket_name = "pubsub-kamo-payloads"

    publisher = DualPathPublisher(
        client=client,
        topic_id=topic_id,
        bucket_name=bucket_name,
        offload_threshold_bytes=1 * 1024,
    )

    # Incompressible random bytes (e.g. high entropy embedding tensor/image bytes)
    large_payload = os.urandom(4096)
    result = publisher.publish_event(
        event_id="evt-large-1",
        source="claude-vision-api",
        payload=large_payload,
        payload_type="application/octet-stream",
    )

    assert result.path == PublishPath.GCS_OFFLOAD
    assert result.payload_uri == f"gs://{bucket_name}/payloads/evt-large-1.bin"

    blob_bytes = client.download_blob(bucket_name, "payloads/evt-large-1.bin")
    assert len(blob_bytes) > 0

    messages = client.get_published_messages(topic_id)
    assert len(messages) == 1
    assert messages[0].attributes["path"] == "gcs_offload"
    assert messages[0].attributes["payload-uri"] == result.payload_uri


def test_publisher_auto_enrichment_and_fingerprint():
    client = GCPClientFactory.get_client(mode=GCPMode.MOCK, project_id="pub-sub-kamo")
    client.clear_topic("events-topic")

    publisher = DualPathPublisher(
        client=client,
        topic_id="events-topic",
        bucket_name="pubsub-kamo-payloads",
    )

    result = publisher.publish_event(
        event_id="evt-enrich-1",
        source="anthropic-eval",
        payload=b"eval trace",
        extra_pod_env={"custom_job": "eval-run-99"},
    )

    assert result.schema_fingerprint.startswith("sha256-")
    assert "node" in result.pod_env_vars
    assert result.pod_env_vars["custom_job"] == "eval-run-99"
    assert result.pod_env_vars["csp"] == "gcp"
