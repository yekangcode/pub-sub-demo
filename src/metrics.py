"""Metrics aggregation and latency analysis engine."""

import threading
from collections import defaultdict

import numpy as np


class MetricsCollector:
    """Thread-safe collector for latency measurements and Dual-Path statistics."""

    def __init__(self):
        self._lock = threading.Lock()
        self._latencies: dict[str, list[float]] = defaultdict(list)
        self._fast_count = 0
        self._offload_count = 0
        self._total_uncompressed_bytes = 0
        self._total_compressed_bytes = 0

    def record_latency(self, label: str, latency_ms: float) -> None:
        """Record an individual latency metric under a label."""
        with self._lock:
            self._latencies[label].append(float(latency_ms))

    def record_path(self, path: str, uncompressed: int, compressed: int) -> None:
        """Record Dual-Path event metrics."""
        with self._lock:
            if path in ("fast", "Fast"):
                self._fast_count += 1
            else:
                self._offload_count += 1
            self._total_uncompressed_bytes += uncompressed
            self._total_compressed_bytes += compressed

    def get_stats(self, label: str) -> dict[str, float]:
        """Calculate statistical distribution for a given label."""
        with self._lock:
            samples = list(self._latencies.get(label, []))

        if not samples:
            return {
                "count": 0.0,
                "p50": 0.0,
                "p90": 0.0,
                "p95": 0.0,
                "p99": 0.0,
                "min": 0.0,
                "max": 0.0,
                "mean": 0.0,
            }

        arr = np.array(samples)
        return {
            "count": float(len(samples)),
            "p50": float(np.percentile(arr, 50)),
            "p90": float(np.percentile(arr, 90)),
            "p95": float(np.percentile(arr, 95)),
            "p99": float(np.percentile(arr, 99)),
            "min": float(np.min(arr)),
            "max": float(np.max(arr)),
            "mean": float(np.mean(arr)),
        }

    def compare(self, baseline_label: str, optimized_label: str) -> dict[str, float]:
        """Compute comparison metrics between baseline and optimized workers."""
        b_stats = self.get_stats(baseline_label)
        o_stats = self.get_stats(optimized_label)

        b_p50 = b_stats["p50"]
        o_p50 = o_stats["p50"]

        if b_p50 > 0:
            reduction = ((b_p50 - o_p50) / b_p50) * 100.0
        else:
            reduction = 0.0

        return {
            "baseline_p50": round(b_p50, 2),
            "optimized_p50": round(o_p50, 2),
            "reduction_percent": round(reduction, 2),
            "baseline_count": b_stats["count"],
            "optimized_count": o_stats["count"],
        }

    def get_path_counters(self) -> dict[str, float]:
        """Retrieve aggregated Dual-Path counters and bandwidth savings."""
        with self._lock:
            fast = self._fast_count
            offload = self._offload_count
            uncompressed = self._total_uncompressed_bytes
            compressed = self._total_compressed_bytes

        saved = max(0, uncompressed - compressed)
        savings_percent = (saved / uncompressed * 100.0) if uncompressed > 0 else 0.0

        return {
            "fast_count": fast,
            "offload_count": offload,
            "total_uncompressed_bytes": uncompressed,
            "total_compressed_bytes": compressed,
            "bytes_saved": saved,
            "overall_savings_percent": round(savings_percent, 2),
        }

    def reset(self) -> None:
        """Reset all metrics."""
        with self._lock:
            self._latencies.clear()
            self._fast_count = 0
            self._offload_count = 0
            self._total_uncompressed_bytes = 0
            self._total_compressed_bytes = 0
