"""데이터 전송 포맷 및 프로토콜 최적화 벤치마크 모듈 (Format & Protocol Benchmark).

[문제 배경 및 아키텍처 원리]
1. JSON 포맷의 비효율성:
   - JSON은 직관적이지만 필드명("event_id", "timestamp_ms", "pod_env_vars" 등)이 모든 메시지마다
     문자열로 중복 전송되어 페이로드 크기가 불필요하게 커지고 압축률이 저하됩니다.
2. 프로토콜 선택의 치명적 영향 (REST vs gRPC):
   - Google Cloud Pub/Sub의 HTTP REST API(`topics.publish`)를 사용하면, HTTP 본문 규격(JSON)에 맞춰
     메시지 데이터(`data` 필드)가 반드시 **Base64로 강제 인코딩**되어야 합니다.
   - Base64 인코딩은 3바이트를 4문자로 변환하므로 **정확히 33.3%의 용량 증가(Inflation Penalty)**가 발생합니다.
   - 반면 gRPC는 HTTP/2 바이너리 프레이밍 위에서 순수 raw 바이너리를 직접 전송하므로 Base64 패널티가 전혀 없습니다 (0% 오버헤드).
3. 바이너리 스키마(Protobuf) + Zstd + gRPC의 결합 (Anthropic 프로덕션 패턴):
   - 필드명을 태그 번호(1바이트 Varint)로 대체하고, 타입 기반 바이너리 인코딩을 적용.
   - 클라이언트단 Zstandard 압축을 결합하고 순수 gRPC 파이프라인으로 전송하여
     단일 페이로드당 최적화할 수 있는 가장 압축된 바이트 볼륨을 달성합니다.
"""

import base64
import json
from dataclasses import dataclass
from typing import Any

from src.compression import CompressionManager
from src.proto import streaming_event_pb2


@dataclass
class FormatComparisonResult:
    """단일 전송 포맷/프로토콜별 직렬화 결과 메트릭 모델."""

    format_name: str           # 포맷 명칭 (예: JSON (REST Base64), Protobuf (gRPC))
    protocol: str              # 전송 프로토콜 (REST vs gRPC)
    wire_bytes: int            # 실제 네트워크 통신망에 전송되는 총 바이트 수
    base64_overhead_bytes: int # Base64로 인해 추가 낭비된 바이트 수
    reduction_vs_json_pct: float # 기본 JSON 대비 데이터 절감률 (%)
    monthly_cost_saving_pct: float # 네트워크 Egress 및 수집 비용 절감률 추정치
    description: str           # 포맷별 아키텍처 설명


class DataFormatBenchmark:
    """JSON, REST Base64, Protobuf, gRPC, Zstd 간의 페이로드 크기 및 비용 절감량을 정량 비교하는 엔진."""

    def __init__(self, compression_level: int = 3):
        self.compression = CompressionManager(level=compression_level)

    def benchmark_event(
        self,
        event_id: str,
        source: str,
        prompt_text: str,
        pod_env: dict[str, str] | None = None,
        schema_fingerprint: str = "sha256-a1b2c3d4e5f60718",
    ) -> dict[str, Any]:
        """동일한 비즈니스 이벤트 데이터에 대해 5대 전송 방식의 바이트 크기를 실측 비교합니다."""
        env = pod_env or {
            "node": "gke-claude-pool-01",
            "namespace": "serving",
            "job": "streaming-publisher",
            "csp": "gcp",
        }
        timestamp_ms = 1772410800000
        raw_payload_bytes = prompt_text.encode("utf-8")
        uncompressed_size = len(raw_payload_bytes)

        # 1. 원본 JSON 딕셔너리 구조 생성
        json_dict = {
            "event_id": event_id,
            "source": source,
            "payload": prompt_text,
            "payload_type": "application/json",
            "timestamp_ms": timestamp_ms,
            "pod_env_vars": env,
            "is_corrupted": False,
            "schema_fingerprint": schema_fingerprint,
            "payload_uri": "",
            "uncompressed_bytes": uncompressed_size,
            "compressed_bytes": uncompressed_size,
        }
        json_str = json.dumps(json_dict, separators=(",", ":"))
        json_bytes = json_str.encode("utf-8")
        len_json_plain = len(json_bytes)

        # 2. [Case A] JSON over HTTP REST API (Base64 인코딩 강제)
        # Google Cloud Pub/Sub REST publish 규격: {"messages": [{"data": "<base64_encoded_data>"}]}
        json_b64 = base64.b64encode(json_bytes)
        rest_envelope_json = json.dumps({"messages": [{"data": json_b64.decode("ascii")}]})
        len_json_rest = len(rest_envelope_json.encode("utf-8"))
        json_rest_penalty = len_json_rest - len_json_plain

        # 3. [Case B] Protobuf 바이너리 생성
        proto_msg = streaming_event_pb2.StreamingEvent(
            event_id=event_id,
            source=source,
            payload=raw_payload_bytes,
            payload_type="application/json",
            timestamp_ms=timestamp_ms,
            pod_env_vars=env,
            is_corrupted=False,
            schema_fingerprint=schema_fingerprint,
            payload_uri="",
            uncompressed_bytes=uncompressed_size,
            compressed_bytes=uncompressed_size,
        )
        proto_bytes = proto_msg.SerializeToString()
        len_proto_grpc = len(proto_bytes)

        # 4. [Case C] Protobuf over HTTP REST API (바이너리를 REST로 전송 시 Base64 패널티)
        proto_b64 = base64.b64encode(proto_bytes)
        rest_envelope_proto = json.dumps({"messages": [{"data": proto_b64.decode("ascii")}]})
        len_proto_rest = len(rest_envelope_proto.encode("utf-8"))
        proto_rest_penalty = len_proto_rest - len_proto_grpc

        # 5. [Case D] Protobuf + Zstandard 압축 over gRPC (Anthropic 최적화 패턴)
        compressed_payload = self.compression.compress(raw_payload_bytes)
        proto_zstd_msg = streaming_event_pb2.StreamingEvent(
            event_id=event_id,
            source=source,
            payload=compressed_payload,
            payload_type="application/json",
            timestamp_ms=timestamp_ms,
            pod_env_vars=env,
            is_corrupted=False,
            schema_fingerprint=schema_fingerprint,
            payload_uri="",
            uncompressed_bytes=uncompressed_size,
            compressed_bytes=len(compressed_payload),
        )
        proto_zstd_bytes = proto_zstd_msg.SerializeToString()
        len_proto_zstd_grpc = len(proto_zstd_bytes)

        # 기준(Baseline): Plain JSON
        baseline_bytes = len_json_plain

        results = [
            FormatComparisonResult(
                format_name="1. Plain JSON (텍스트)",
                protocol="HTTP / Internal",
                wire_bytes=len_json_plain,
                base64_overhead_bytes=0,
                reduction_vs_json_pct=0.0,
                monthly_cost_saving_pct=0.0,
                description="모든 필드명이 문자열로 반복 전송되어 불필요한 바이트 낭비 발생",
            ),
            FormatComparisonResult(
                format_name="2. JSON over REST (Base64)",
                protocol="HTTP REST API",
                wire_bytes=len_json_rest,
                base64_overhead_bytes=json_rest_penalty,
                reduction_vs_json_pct=-round(((len_json_rest - baseline_bytes) / baseline_bytes) * 100, 1),
                monthly_cost_saving_pct=-round(((len_json_rest - baseline_bytes) / baseline_bytes) * 100, 1),
                description="Pub/Sub REST 규격으로 인해 데이터가 Base64로 인코딩되어 약 33% 크기 팽창(+33% 패널티)",
            ),
            FormatComparisonResult(
                format_name="3. Protobuf over REST (Base64)",
                protocol="HTTP REST API",
                wire_bytes=len_proto_rest,
                base64_overhead_bytes=proto_rest_penalty,
                reduction_vs_json_pct=round(((baseline_bytes - len_proto_rest) / baseline_bytes) * 100, 1),
                monthly_cost_saving_pct=round(((baseline_bytes - len_proto_rest) / baseline_bytes) * 100, 1),
                description="바이너리 스키마를 적용했으나 REST API의 Base64 강제 규격으로 인해 33% 오버헤드 잔존",
            ),
            FormatComparisonResult(
                format_name="4. Protobuf over gRPC (순수 바이너리)",
                protocol="gRPC (HTTP/2)",
                wire_bytes=len_proto_grpc,
                base64_overhead_bytes=0,
                reduction_vs_json_pct=round(((baseline_bytes - len_proto_grpc) / baseline_bytes) * 100, 1),
                monthly_cost_saving_pct=round(((baseline_bytes - len_proto_grpc) / baseline_bytes) * 100, 1),
                description="필드명 완전 제거 + Varint 압축 + gRPC 순수 바이너리 전송 (Base64 패널티 0%)",
            ),
            FormatComparisonResult(
                format_name="5. Protobuf + Zstd over gRPC (Anthropic)",
                protocol="gRPC (HTTP/2)",
                wire_bytes=len_proto_zstd_grpc,
                base64_overhead_bytes=0,
                reduction_vs_json_pct=round(((baseline_bytes - len_proto_zstd_grpc) / baseline_bytes) * 100, 1),
                monthly_cost_saving_pct=round(((baseline_bytes - len_proto_zstd_grpc) / baseline_bytes) * 100, 1),
                description="바이너리 스키마 + 클라이언트 Zstd 압축 + gRPC 결합: 단일 페이로드 최극소화 바이트 볼륨 달성",
            ),
        ]

        # 10억 건(1 Billion Events) 전송 시 예상 트래픽(TB) 및 절감량 산출
        billion = 1_000_000_000
        baseline_tb = (len_json_plain * billion) / (1024**4)
        zstd_grpc_tb = (len_proto_zstd_grpc * billion) / (1024**4)
        saved_tb = max(0.0, baseline_tb - zstd_grpc_tb)

        return {
            "json_sample_snippet": json_str[:200] + "...",
            "uncompressed_bytes": uncompressed_size,
            "baseline_json_bytes": len_json_plain,
            "results": results,
            "savings_summary": {
                "baseline_tb_per_1b": round(baseline_tb, 2),
                "optimized_tb_per_1b": round(zstd_grpc_tb, 2),
                "saved_tb_per_1b": round(saved_tb, 2),
                "overall_reduction_pct": round(((baseline_bytes - len_proto_zstd_grpc) / baseline_bytes) * 100, 1),
            },
        }
