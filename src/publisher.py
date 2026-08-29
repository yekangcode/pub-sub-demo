"""Dual-Path publisher with zstd compression and GCS offloading."""

import hashlib
import os
import socket
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from src.compression import CompressionManager
from src.gcp_client import GCPClientInterface
from src.proto import streaming_event_pb2

DEFAULT_OFFLOAD_THRESHOLD_BYTES = 8 * 1024 * 1024  # 8 MB Anthropic production pattern


class PublishPath(str, Enum):
    """Routing path taken by the publisher."""

    FAST = "fast"
    GCS_OFFLOAD = "gcs_offload"


@dataclass
class PublishResult:
    """Outcome of a published streaming event."""

    event_id: str
    message_id: str
    path: PublishPath
    uncompressed_bytes: int
    compressed_bytes: int
    reduction_percentage: float
    payload_uri: str = ""
    schema_fingerprint: str = ""
    pod_env_vars: dict[str, str] = field(default_factory=dict)
    publish_latency_ms: float = 0.0


class DualPathPublisher:
    """Publishes events using the Dual-Path pattern:

    1. All payloads compressed with Zstandard.
    2. Payloads under threshold sent inline via Pub/Sub (Fast Path).
    3. Payloads meeting or exceeding threshold uploaded to GCS with pointer in Pub/Sub (Offload Path).
    """

    def __init__(
        self,
        client: GCPClientInterface,
        topic_id: str,
        bucket_name: str,
        offload_threshold_bytes: int = DEFAULT_OFFLOAD_THRESHOLD_BYTES,
        compression_level: int = 3,
    ):
        self.client = client
        self.topic_id = topic_id
        self.bucket_name = bucket_name
        self.offload_threshold_bytes = offload_threshold_bytes
        self.compression = CompressionManager(level=compression_level)
        self._schema_fingerprint = self._compute_schema_fingerprint()

    def _compute_schema_fingerprint(self) -> str:
        descriptor_bytes = streaming_event_pb2.StreamingEvent.DESCRIPTOR.file.serialized_pb
        digest = hashlib.sha256(descriptor_bytes).hexdigest()[:16]
        return f"sha256-{digest}"

    def _collect_pod_env_vars(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        env_vars = {
            "node": os.environ.get("HOSTNAME", socket.gethostname()),
            "namespace": os.environ.get("KUBE_NAMESPACE", "default"),
            "job": os.environ.get("JOB_NAME", "streaming-publisher"),
            "csp": "gcp",
        }
        if extra:
            env_vars.update(extra)
        return env_vars

    def publish_event(
        self,
        event_id: str,
        source: str,
        payload: bytes,
        payload_type: str = "text/plain",
        is_corrupted: bool = False,
        extra_pod_env: dict[str, str] | None = None,
        custom_attributes: dict[str, Any] | None = None,
    ) -> PublishResult:
        """Publish a single streaming event following the Dual-Path pattern."""
        start_time = time.perf_counter()
        uncompressed_size = len(payload)

        # 1. Compress payload with zstandard
        compressed_payload = self.compression.compress(payload)
        compressed_size = len(compressed_payload)
        reduction = self.compression.reduction_percentage(uncompressed_size, compressed_size)

        pod_env = self._collect_pod_env_vars(extra_pod_env)
        timestamp_ms = int(time.time() * 1000)

        # 2. Dual-Path decision based on compressed payload size
        if compressed_size < self.offload_threshold_bytes:
            path = PublishPath.FAST
            payload_uri = ""
            event_payload = compressed_payload
        else:
            path = PublishPath.GCS_OFFLOAD
            blob_name = f"payloads/{event_id}.bin"
            payload_uri = self.client.upload_blob(
                bucket_name=self.bucket_name,
                blob_name=blob_name,
                data=compressed_payload,
                content_type="application/zstd",
            )
            event_payload = b""  # Empty in message; points to GCS blob

        # 3. Construct Protobuf message
        event_proto = streaming_event_pb2.StreamingEvent(
            event_id=event_id,
            source=source,
            payload=event_payload,
            payload_type=payload_type,
            timestamp_ms=timestamp_ms,
            pod_env_vars=pod_env,
            is_corrupted=is_corrupted,
            schema_fingerprint=self._schema_fingerprint,
            payload_uri=payload_uri,
            uncompressed_bytes=uncompressed_size,
            compressed_bytes=compressed_size,
        )

        serialized_data = event_proto.SerializeToString()

        # 4. Prepare message attributes
        attrs = {
            "path": path.value,
            "event-id": event_id,
            "source": source,
            "payload-type": payload_type,
            "content-encoding": "zstd",
        }
        if payload_uri:
            attrs["payload-uri"] = payload_uri
        if custom_attributes:
            attrs.update({k: str(v) for k, v in custom_attributes.items()})

        # 5. Publish to Pub/Sub
        msg_id = self.client.publish(
            topic_id=self.topic_id,
            data=serialized_data,
            attributes=attrs,
        )

        latency_ms = (time.perf_counter() - start_time) * 1000.0

        return PublishResult(
            event_id=event_id,
            message_id=msg_id,
            path=path,
            uncompressed_bytes=uncompressed_size,
            compressed_bytes=compressed_size,
            reduction_percentage=reduction,
            payload_uri=payload_uri,
            schema_fingerprint=self._schema_fingerprint,
            pod_env_vars=pod_env,
            publish_latency_ms=latency_ms,
        )
