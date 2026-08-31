"""Unit tests for DataFormatBenchmark (JSON vs REST Base64 vs Protobuf vs gRPC + Zstd)."""

from src.format_benchmark import DataFormatBenchmark


def test_format_benchmark_reductions():
    bench = DataFormatBenchmark(compression_level=3)
    sample_text = (
        "Explain the internal architecture of Google Cloud Pub/Sub and StreamingPull RPCs. "
        "Anthropic optimizes real-time LLM inference telemetry with Protocol Buffers and Zstandard compression. "
    ) * 5

    res = bench.benchmark_event(
        event_id="evt-bench-001",
        source="serving-claude",
        prompt_text=sample_text,
    )

    results = {r.format_name: r for r in res["results"]}

    json_plain = results["1. Plain JSON (텍스트)"]
    json_rest = results["2. JSON over REST (Base64)"]
    proto_rest = results["3. Protobuf over REST (Base64)"]
    proto_grpc = results["4. Protobuf over gRPC (순수 바이너리)"]
    proto_zstd_grpc = results["5. Protobuf + Zstd over gRPC (Anthropic)"]

    # 1. JSON over REST must be strictly larger than Plain JSON due to Base64 inflation penalty
    assert json_rest.wire_bytes > json_plain.wire_bytes
    assert json_rest.base64_overhead_bytes > 0

    # 2. Protobuf over gRPC must be strictly smaller than Plain JSON and Protobuf over REST
    assert proto_grpc.wire_bytes < json_plain.wire_bytes
    assert proto_grpc.wire_bytes < proto_rest.wire_bytes
    assert proto_grpc.base64_overhead_bytes == 0

    # 3. Protobuf + Zstd over gRPC must achieve the minimum wire bytes
    assert proto_zstd_grpc.wire_bytes < proto_grpc.wire_bytes
    assert proto_zstd_grpc.reduction_vs_json_pct > 30.0

    # 4. Check 1 Billion events savings calculation
    savings = res["savings_summary"]
    assert savings["saved_tb_per_1b"] > 0
    assert savings["overall_reduction_pct"] > 30.0
