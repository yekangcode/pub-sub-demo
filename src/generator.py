"""Synthetic LLM workload generator for streaming ingestion benchmarks."""

import os
import random
import uuid
from typing import Any

from src.publisher import DualPathPublisher, PublishResult

SAMPLE_PROMPTS = [
    "Explain the internal architecture of Google Cloud Pub/Sub and StreamingPull RPCs.",
    "Write a high-performance Python consumer implementing backpressure and flow control.",
    "How does Anthropic scale real-time LLM inference telemetry with Protocol Buffers?",
    "Compare BigQuery Zero-ETL direct subscription streaming versus batch Dataflow pipelines.",
    "Generate a summary of Zstandard compression benchmarks on repetitive JSON log events.",
]

SAMPLE_SOURCES = [
    "claude-opus-serving-pod-01",
    "claude-sonnet-batch-runner",
    "anthropic-eval-worker-4",
    "agentic-workflow-router",
]


class SyntheticWorkloadGenerator:
    """Generates synthetic LLM traffic spanning small prompts and large multimodal blobs."""

    def __init__(
        self,
        publisher: DualPathPublisher,
        large_payload_pct: float = 10.0,
        corrupt_pct: float = 0.0,
    ):
        self.publisher = publisher
        self.large_payload_pct = max(0.0, min(100.0, large_payload_pct))
        self.corrupt_pct = max(0.0, min(100.0, corrupt_pct))
        self._seq = 0

    def generate_single_event(
        self,
        force_large: bool | None = None,
        force_corrupt: bool | None = None,
        extra_attributes: dict[str, Any] | None = None,
    ) -> PublishResult:
        """Generate and publish a single synthetic event."""
        self._seq += 1
        event_id = f"evt-{uuid.uuid4().hex[:8]}-{self._seq:06d}"
        source = random.choice(SAMPLE_SOURCES)

        # Decide if large payload
        is_large = force_large if force_large is not None else (random.uniform(0, 100) < self.large_payload_pct)
        # Decide if corrupted
        is_corrupt = force_corrupt if force_corrupt is not None else (random.uniform(0, 100) < self.corrupt_pct)

        if is_large:
            # Multimodal embedding or large image payload
            # If offload threshold is small (e.g. 5KB in tests), generate > threshold
            target_size = max(self.publisher.offload_threshold_bytes + 2048, 12 * 1024)
            # Use random bytes to ensure high entropy / incompressible
            payload = os.urandom(target_size)
            payload_type = "application/octet-stream"
        else:
            prompt_text = random.choice(SAMPLE_PROMPTS)
            payload = (f'{{"prompt": "{prompt_text}", "seq": {self._seq}, "model": "claude-3-7-sonnet"}}\n' * 5).encode()
            payload_type = "application/json"

        return self.publisher.publish_event(
            event_id=event_id,
            source=source,
            payload=payload,
            payload_type=payload_type,
            is_corrupted=is_corrupt,
            custom_attributes=extra_attributes,
        )

    def generate_batch(self, count: int = 10) -> list[PublishResult]:
        """Generate and publish a batch of events."""
        results = []
        for _ in range(count):
            results.append(self.generate_single_event())
        return results
