from src.gcp_client import GCPClientFactory, GCPMode
from src.generator import SyntheticWorkloadGenerator
from src.publisher import DualPathPublisher, PublishPath


def test_generator_emits_fast_and_offload_events():
    client = GCPClientFactory.get_client(mode=GCPMode.MOCK, project_id="pub-sub-kamo")
    topic_id = "gen-test-topic"
    bucket = "gen-test-bucket"

    publisher = DualPathPublisher(
        client=client,
        topic_id=topic_id,
        bucket_name=bucket,
        offload_threshold_bytes=5 * 1024,  # 5KB threshold
    )

    generator = SyntheticWorkloadGenerator(
        publisher=publisher,
        large_payload_pct=50.0,
        corrupt_pct=0.0,
    )

    results = generator.generate_batch(count=10)
    assert len(results) == 10

    paths = [r.path for r in results]
    assert PublishPath.FAST in paths
    assert PublishPath.GCS_OFFLOAD in paths


def test_generator_injects_corruption_on_demand():
    client = GCPClientFactory.get_client(mode=GCPMode.MOCK, project_id="pub-sub-kamo")
    topic_id = "gen-corrupt-topic"
    bucket = "gen-corrupt-bucket"

    publisher = DualPathPublisher(client=client, topic_id=topic_id, bucket_name=bucket)
    generator = SyntheticWorkloadGenerator(publisher=publisher, corrupt_pct=100.0)

    results = generator.generate_batch(count=5)
    assert len(results) == 5

    # All published events should have is_corrupted set to True in the message proto
    from src.proto_gen import streaming_event_pb2

    messages = client.get_published_messages(topic_id)
    assert len(messages) >= 5
    for msg in messages[-5:]:
        event = streaming_event_pb2.StreamingEvent()
        event.ParseFromString(msg.data)
        assert event.is_corrupted is True
