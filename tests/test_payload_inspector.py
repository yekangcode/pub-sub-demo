import time
from src.compression import CompressionManager
from src.payload_inspector import PayloadInspector
from src.proto_gen import streaming_event_pb2


def test_payload_inspector_compressed_proto():
    cm = CompressionManager(level=3)
    inspector = PayloadInspector()

    original_text = b"Anthropic-Prompt-Engineering-Fast-Path-Sample-Payload-" * 5
    compressed_payload = cm.compress(original_text)

    evt = streaming_event_pb2.StreamingEvent(
        event_id="test-evt-001",
        source="serving-claude",
        payload=compressed_payload,
        payload_type="text/plain",
        timestamp_ms=int(time.time() * 1000),
        pod_env_vars={"node": "gke-node-1", "namespace": "prod"},
        schema_fingerprint="sha256-test1234",
    )
    raw_proto = evt.SerializeToString()

    res = inspector.inspect_raw(
        raw_data=raw_proto,
        attributes={"event-id": "test-evt-001", "source": "serving-claude"},
        message_id="msg-101",
        publish_time="2026-08-31 08:00:00 UTC",
    )

    assert res.is_valid_proto is True
    assert res.event_id == "test-evt-001"
    assert res.source == "serving-claude"
    assert res.is_zstd_compressed is True
    assert res.compressed_payload_bytes == len(compressed_payload)
    assert res.uncompressed_payload_bytes == len(original_text)
    assert res.reduction_percent > 50.0
    assert "Anthropic-Prompt-Engineering" in res.decompressed_text
    assert res.pod_env_vars["node"] == "gke-node-1"


def test_payload_inspector_plain_bytes():
    inspector = PayloadInspector()
    plain_text = b"Simple non-protobuf plain text message"

    res = inspector.inspect_raw(
        raw_data=plain_text,
        attributes='{"event-id": "plain-001"}',
        message_id="msg-102",
    )

    assert res.is_valid_proto is False
    assert res.is_zstd_compressed is False
    assert res.uncompressed_payload_bytes == len(plain_text)
    assert res.decompressed_text == "Simple non-protobuf plain text message"


def test_payload_inspector_empty():
    inspector = PayloadInspector()
    res = inspector.inspect_raw(raw_data=None, attributes=None)
    assert res.raw_bytes_len == 0
    assert res.error_message != ""
