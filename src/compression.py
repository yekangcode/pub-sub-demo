"""Zstandard compression layer for streaming payloads."""

from typing import Any

import zstandard as zstd

# Zstandard frame magic number: 0xFD2FB528 (little-endian: 0x28, 0xB5, 0x2F, 0xFD)
ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"


class CompressionManager:
    """Manages payload compression and decompression using Zstandard."""

    def __init__(self, level: int = 3):
        """Initialize with specified zstd compression level (1-22)."""
        self.level = level
        self._cctx = zstd.ZstdCompressor(level=self.level)
        self._dctx = zstd.ZstdDecompressor()

    def compress(self, data: bytes) -> bytes:
        """Compress raw bytes with zstd."""
        if not isinstance(data, bytes):
            raise TypeError("Data must be bytes")
        return self._cctx.compress(data)

    def decompress(self, compressed_data: bytes) -> bytes:
        """Decompress zstd-compressed bytes."""
        if not isinstance(compressed_data, bytes):
            raise TypeError("Compressed data must be bytes")
        return self._dctx.decompress(compressed_data)

    def is_compressed(self, data: bytes) -> bool:
        """Check if data starts with the zstd frame magic number."""
        return isinstance(data, bytes) and data.startswith(ZSTD_MAGIC)

    @staticmethod
    def reduction_percentage(original_size: int, compressed_size: int) -> float:
        """Calculate the payload reduction percentage (0% to 100%)."""
        if original_size <= 0:
            return 0.0
        reduced = max(0, original_size - compressed_size)
        return (reduced / original_size) * 100.0

    @staticmethod
    def enrich_attributes_with_encoding(
        attributes: dict[str, Any] | None = None,
    ) -> dict[str, str]:
        """Inject content-encoding: zstd into Pub/Sub message attributes."""
        result = {k: str(v) for k, v in (attributes or {}).items()}
        result["content-encoding"] = "zstd"
        return result

    @staticmethod
    def has_zstd_encoding(attributes: dict[str, str] | None = None) -> bool:
        """Check if message attributes indicate zstd content encoding."""
        if not attributes:
            return False
        return attributes.get("content-encoding") == "zstd"
