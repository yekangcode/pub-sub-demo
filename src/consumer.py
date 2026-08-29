"""Dual-Path consumer reconstituting payloads from inline or GCS offloaded blobs."""

import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

from src.compression import CompressionManager
from src.gcp_client import GCPClientInterface
from src.proto import streaming_event_pb2
from src.publisher import PublishPath


@dataclass
class ReconstitutedEvent:
    """Represents a fully reconstituted streaming event."""

    event_id: str
    source: str
    payload: bytes
    payload_type: str
    path: PublishPath
    payload_uri: str
    schema_fingerprint: str
    pod_env_vars: dict[str, str] = field(default_factory=dict)
    processing_latency_ms: float = 0.0


class DualPathConsumer:
    """Consumer capable of transparently reconstituting events from Fast Path

    (inline compressed bytes) or Offload Path (GCS blob download + decompression).
    """

    def __init__(self, client: GCPClientInterface):
        self.client = client
        self.compression = CompressionManager()

    def _parse_gs_uri(self, uri: str) -> tuple[str, str]:
        """Parse gs://bucket/blob_name into (bucket, blob_name)."""
        parsed = urlparse(uri)
        if parsed.scheme != "gs":
            raise ValueError(f"Invalid GCS URI scheme: {uri}")
        bucket = parsed.netloc
        blob_name = parsed.path.lstrip("/")
        return bucket, blob_name

    def consume_message(
        self, data: bytes, attributes: dict[str, Any] | None = None
    ) -> ReconstitutedEvent:
        """Parse and reconstitute a Pub/Sub message."""
        start_time = time.perf_counter()

        # 1. Deserialize Protobuf message
        event_proto = streaming_event_pb2.StreamingEvent()
        event_proto.ParseFromString(data)

        # 2. Check intentional corruption / poison pill for DLQ testing
        if event_proto.is_corrupted:
            raise ValueError(f"Corrupted event detected: {event_proto.event_id}")

        # 3. Determine retrieval path and reconstitute payload
        if event_proto.payload_uri:
            # GCS Offload path: download compressed blob from Cloud Storage
            path = PublishPath.GCS_OFFLOAD
            bucket, blob_name = self._parse_gs_uri(event_proto.payload_uri)
            compressed_blob = self.client.download_blob(bucket_name=bucket, blob_name=blob_name)
            decompressed_payload = self.compression.decompress(compressed_blob)
        else:
            # Fast path: payload was transmitted inline
            path = PublishPath.FAST
            decompressed_payload = self.compression.decompress(event_proto.payload)

        latency_ms = (time.perf_counter() - start_time) * 1000.0

        return ReconstitutedEvent(
            event_id=event_proto.event_id,
            source=event_proto.source,
            payload=decompressed_payload,
            payload_type=event_proto.payload_type,
            path=path,
            payload_uri=event_proto.payload_uri,
            schema_fingerprint=event_proto.schema_fingerprint,
            pod_env_vars=dict(event_proto.pod_env_vars),
            processing_latency_ms=latency_ms,
        )
