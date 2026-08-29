from src.metrics import MetricsCollector


def test_metrics_collector_record_and_percentiles():
    collector = MetricsCollector()

    # Record 100 sample latencies for sync pull (around 100ms)
    for i in range(1, 101):
        collector.record_latency("sync_pull", float(i))

    stats = collector.get_stats("sync_pull")
    assert stats["count"] == 100
    assert stats["p50"] == 50.5 or stats["p50"] == 50.0 or stats["p50"] == 51.0
    assert stats["p95"] >= 95.0
    assert stats["p99"] >= 99.0
    assert stats["min"] == 1.0
    assert stats["max"] == 100.0


def test_metrics_collector_88_percent_reduction_calculation():
    collector = MetricsCollector()

    # Sync pull ~100ms
    for _ in range(50):
        collector.record_latency("sync_pull", 100.0)

    # Streaming pull ~12ms (88% reduction)
    for _ in range(50):
        collector.record_latency("streaming_pull", 12.0)

    comparison = collector.compare("sync_pull", "streaming_pull")
    assert comparison["baseline_p50"] == 100.0
    assert comparison["optimized_p50"] == 12.0
    assert comparison["reduction_percent"] == 88.0


def test_metrics_collector_dual_path_counters():
    collector = MetricsCollector()

    collector.record_path("fast", uncompressed=1000, compressed=200)
    collector.record_path("fast", uncompressed=2000, compressed=400)
    collector.record_path("gcs_offload", uncompressed=10000, compressed=3000)

    counters = collector.get_path_counters()
    assert counters["fast_count"] == 2
    assert counters["offload_count"] == 1
    assert counters["total_uncompressed_bytes"] == 13000
    assert counters["total_compressed_bytes"] == 3600
    assert counters["bytes_saved"] == 9400
    assert counters["overall_savings_percent"] > 70.0
