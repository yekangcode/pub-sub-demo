import pytest
import zstandard as zstd

from src.compression import CompressionManager


def test_compression_round_trip():
    manager = CompressionManager(level=3)
    raw_data = b"Hello Google Cloud Pub/Sub with Anthropic architecture patterns!" * 100

    compressed = manager.compress(raw_data)
    assert isinstance(compressed, bytes)
    assert len(compressed) < len(raw_data)
    assert manager.is_compressed(compressed) is True
    assert manager.is_compressed(raw_data) is False

    decompressed = manager.decompress(compressed)
    assert decompressed == raw_data


def test_compression_ratio_reduction():
    manager = CompressionManager()
    sample_text = """
    Claude interaction log: user requested assistance in building high throughput
    streaming event platform with Pub/Sub, BigQuery, and Cloud Storage.
    """ * 50
    raw_bytes = sample_text.encode("utf-8")

    compressed = manager.compress(raw_bytes)
    ratio = manager.reduction_percentage(len(raw_bytes), len(compressed))

    # Expecting typical LLM text compression reduction of 60% to 80%
    assert ratio >= 60.0


def test_decompress_uncompressed_raises_or_handles():
    manager = CompressionManager()
    uncompressed_data = b"not a zstd compressed stream"

    with pytest.raises(zstd.ZstdError):
        manager.decompress(uncompressed_data)


def test_wrap_and_unwrap_attributes():
    manager = CompressionManager()
    attrs = {"source": "generator", "format": "proto"}

    wrapped_attrs = manager.enrich_attributes_with_encoding(attrs)
    assert wrapped_attrs["content-encoding"] == "zstd"

    assert manager.has_zstd_encoding(wrapped_attrs) is True
    assert manager.has_zstd_encoding({"format": "proto"}) is False


def test_invalid_types_raise():
    manager = CompressionManager()
    with pytest.raises(TypeError):
        manager.compress("string is not bytes")  # type: ignore

    with pytest.raises(TypeError):
        manager.decompress("string is not bytes")  # type: ignore


def test_reduction_percentage_zero():
    manager = CompressionManager()
    assert manager.reduction_percentage(0, 0) == 0.0
