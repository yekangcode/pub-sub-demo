"""BigQuery에 저장된 Zstd 압축 바이너리 및 Protocol Buffers 페이로드 역직렬화/분석 모듈.

[Anthropic 아키텍처 배경]
BigQuery Zero-ETL 직접 구독을 통해 수집된 데이터는 네트워크 대역폭과 저장 비용을
극대화하기 위해 Protocol Buffers 바이너리 형식과 Zstandard(zstd) 알고리즘으로 이중 압축되어 있습니다.
본 모듈은 BigQuery의 `data` 컬럼(Bytes)을 디코딩하여:
1. Protocol Buffers `StreamingEvent` 스키마 역직렬화
2. 내부 페이로드의 Zstd 매직 넘버(0x28 0xB5 0x2F 0xFD) 검사 및 압축 해제
3. 압축 전/후 용량, 절감률, 원본 텍스트 및 메타데이터를 정밀하게 분석합니다.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field
from typing import Any

from src.compression import ZSTD_MAGIC, CompressionManager
from src.proto_gen import streaming_event_pb2


@dataclass
class InspectedPayload:
    """BigQuery 단일 레코드의 압축 해제 및 역직렬화 분석 결과 모델."""

    message_id: str
    publish_time: str
    raw_bytes_len: int
    base64_preview: str
    hex_preview: str
    is_valid_proto: bool
    event_id: str
    source: str
    payload_type: str
    schema_fingerprint: str
    timestamp_ms: int
    pod_env_vars: dict[str, str] = field(default_factory=dict)
    is_zstd_compressed: bool = False
    compressed_payload_bytes: int = 0
    uncompressed_payload_bytes: int = 0
    reduction_percent: float = 0.0
    decompressed_text: str = ""
    is_gcs_claim_check: bool = False
    payload_uri: str = ""
    raw_attributes: dict[str, Any] = field(default_factory=dict)
    error_message: str = ""


class PayloadInspector:
    """BigQuery 바이트 데이터를 Zstd 압축 해제 및 Protobuf 역직렬화하는 인스펙터."""

    def __init__(self, compression_level: int = 3):
        self.cm = CompressionManager(level=compression_level)

    def inspect_raw(
        self,
        raw_data: bytes | str | None,
        attributes: dict[str, Any] | str | None = None,
        message_id: str = "",
        publish_time: str = "",
    ) -> InspectedPayload:
        """BigQuery의 `data` 필드(바이너리/Base64)와 `attributes`를 분석합니다."""
        # 1. 속성(Attributes) 정규화
        parsed_attrs: dict[str, Any] = {}
        if isinstance(attributes, str):
            try:
                parsed_attrs = json.loads(attributes)
            except Exception:
                parsed_attrs = {"raw": attributes}
        elif isinstance(attributes, dict):
            parsed_attrs = attributes

        # 2. 바이트 데이터 정규화 (Base64 문자열 또는 bytes)
        raw_bytes = b""
        if raw_data is not None:
            if isinstance(raw_data, bytes):
                raw_bytes = raw_data
            elif isinstance(raw_data, str):
                try:
                    raw_bytes = base64.b64decode(raw_data)
                except Exception:
                    raw_bytes = raw_data.encode("utf-8")

        raw_len = len(raw_bytes)
        b64_str = base64.b64encode(raw_bytes).decode("ascii") if raw_bytes else ""
        hex_dump = raw_bytes[:32].hex(" ") if raw_bytes else ""

        if not raw_bytes:
            return InspectedPayload(
                message_id=message_id,
                publish_time=publish_time,
                raw_bytes_len=0,
                base64_preview="",
                hex_preview="",
                is_valid_proto=False,
                event_id=parsed_attrs.get("event-id", parsed_attrs.get("event_id", "")),
                source=parsed_attrs.get("source", ""),
                payload_type=parsed_attrs.get("payload-type", ""),
                schema_fingerprint="",
                timestamp_ms=0,
                raw_attributes=parsed_attrs,
                error_message="페이로드 데이터가 비어 있습니다.",
            )

        # 3. Protocol Buffers 역직렬화 시도
        event = streaming_event_pb2.StreamingEvent()
        try:
            event.ParseFromString(raw_bytes)
            is_proto = True
        except Exception:
            is_proto = False

        if is_proto:
            event_id = event.event_id or parsed_attrs.get("event-id", parsed_attrs.get("event_id", ""))
            source = event.source or parsed_attrs.get("source", "")
            payload_type = event.payload_type or parsed_attrs.get("payload-type", "text/plain")
            schema_fp = event.schema_fingerprint
            timestamp_ms = event.timestamp_ms
            pod_vars = dict(event.pod_env_vars)
            payload_uri = event.payload_uri
            is_gcs = bool(payload_uri)

            inner_payload = event.payload
            compressed_len = len(inner_payload)

            # 4. 내부 페이로드 Zstd 압축 해제 검사
            if self.cm.is_compressed(inner_payload):
                try:
                    decompressed = self.cm.decompress(inner_payload)
                    is_zstd = True
                    uncompressed_len = len(decompressed)
                    red_pct = self.cm.reduction_percentage(uncompressed_len, compressed_len)
                    text_preview = decompressed.decode("utf-8", errors="replace")
                except Exception as e:
                    is_zstd = True
                    uncompressed_len = compressed_len
                    red_pct = 0.0
                    text_preview = f"<Zstd 해제 에러: {e}>"
            else:
                is_zstd = False
                uncompressed_len = compressed_len
                red_pct = 0.0
                text_preview = inner_payload.decode("utf-8", errors="replace") if inner_payload else ""

            return InspectedPayload(
                message_id=message_id,
                publish_time=publish_time,
                raw_bytes_len=raw_len,
                base64_preview=b64_str[:80] + ("..." if len(b64_str) > 80 else ""),
                hex_preview=hex_dump,
                is_valid_proto=True,
                event_id=event_id,
                source=source,
                payload_type=payload_type,
                schema_fingerprint=schema_fp,
                timestamp_ms=timestamp_ms,
                pod_env_vars=pod_vars,
                is_zstd_compressed=is_zstd,
                compressed_payload_bytes=compressed_len,
                uncompressed_payload_bytes=uncompressed_len,
                reduction_percent=red_pct,
                decompressed_text=text_preview,
                is_gcs_claim_check=is_gcs,
                payload_uri=payload_uri,
                raw_attributes=parsed_attrs,
            )

        # 5. Protobuf가 아닌 직접 전달된 Zstd 또는 일반 바이트인 경우의 폴백
        if self.cm.is_compressed(raw_bytes):
            try:
                decompressed = self.cm.decompress(raw_bytes)
                is_zstd = True
                uncompressed_len = len(decompressed)
                red_pct = self.cm.reduction_percentage(uncompressed_len, raw_len)
                text_preview = decompressed.decode("utf-8", errors="replace")
            except Exception as e:
                is_zstd = True
                uncompressed_len = raw_len
                red_pct = 0.0
                text_preview = f"<Zstd 해제 에러: {e}>"
        else:
            is_zstd = False
            uncompressed_len = raw_len
            red_pct = 0.0
            text_preview = raw_bytes.decode("utf-8", errors="replace")

        return InspectedPayload(
            message_id=message_id,
            publish_time=publish_time,
            raw_bytes_len=raw_len,
            base64_preview=b64_str[:80] + ("..." if len(b64_str) > 80 else ""),
            hex_preview=hex_dump,
            is_valid_proto=False,
            event_id=parsed_attrs.get("event-id", parsed_attrs.get("event_id", "raw-event")),
            source=parsed_attrs.get("source", "unknown"),
            payload_type=parsed_attrs.get("payload-type", "application/octet-stream"),
            schema_fingerprint="",
            timestamp_ms=0,
            is_zstd_compressed=is_zstd,
            compressed_payload_bytes=raw_len,
            uncompressed_payload_bytes=uncompressed_len,
            reduction_percent=red_pct,
            decompressed_text=text_preview,
            raw_attributes=parsed_attrs,
        )
