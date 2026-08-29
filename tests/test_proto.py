import time


def test_proto_serialization_and_deserialization():
    from src.proto import streaming_event_pb2

    event = streaming_event_pb2.StreamingEvent(
        event_id="evt-12345",
        source="test-generator",
        payload=b"test-payload-data",
        payload_type="text/plain",
        timestamp_ms=int(time.time() * 1000),
        pod_env_vars={"node": "gke-node-1", "namespace": "ai-serving"},
        is_corrupted=False,
        schema_fingerprint="sha256-abc12345",
        payload_uri="",
        uncompressed_bytes=17,
        compressed_bytes=17,
    )

    serialized = event.SerializeToString()
    assert isinstance(serialized, bytes)
    assert len(serialized) > 0

    reconstructed = streaming_event_pb2.StreamingEvent()
    reconstructed.ParseFromString(serialized)

    assert reconstructed.event_id == "evt-12345"
    assert reconstructed.source == "test-generator"
    assert reconstructed.payload == b"test-payload-data"
    assert reconstructed.payload_type == "text/plain"
    assert reconstructed.pod_env_vars["node"] == "gke-node-1"
    assert reconstructed.is_corrupted is False
    assert reconstructed.schema_fingerprint == "sha256-abc12345"
