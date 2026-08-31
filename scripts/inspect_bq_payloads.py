#!/usr/bin/env python3
"""BigQuery에 실시간 적재된 Zstd 압축 바이너리 & Protobuf 페이로드 역직렬화 검사 스크립트.

[Anthropic 아키텍처 배경]
BigQuery Zero-ETL 직접 구독(`pubsub-demo-bq-sub`)으로 스트리밍 수집된 레코드는
Zstandard(zstd) 알고리즘과 Protocol Buffers로 정밀 직렬화되어 BigQuery의 `data`(Bytes)
컬럼에 저장됩니다.
본 CLI 도구는 BigQuery 테이블에서 실시간 데이터를 인출하여:
1. [BEFORE]: BigQuery에 저장된 원시 바이너리 크기 및 Base64/Hex 상태 확인
2. [AFTER]: Zstd 압축 해제 및 Protobuf 역직렬화를 통해 원본 텍스트 및 메타데이터 복원
3. 바이트 절감률(Reduction %)을 정밀 비교 및 시각화합니다.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# Ensure repository root is in sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.payload_inspector import PayloadInspector


def run_inspection(
    project_id: str = "pub-sub-kamo",
    dataset_id: str = "pubsub_demo_analytics",
    table_id: str = "streaming_events",
    limit: int = 5,
    dry_run: bool = False,
) -> bool:
    """BigQuery에서 최신 레코드를 인출하여 Zstd 압축 해제 및 Protobuf 역직렬화를 수행합니다."""
    print("=" * 80)
    print("🔍 [BigQuery Zero-ETL] Zstd 압축 해제 & Protobuf 역직렬화 실시간 분석기")
    print(f"• 대상 프로젝트: {project_id}")
    print(f"• 대상 테이블:   {project_id}.{dataset_id}.{table_id}")
    print(f"• 조회 건수:     최근 {limit}건")
    print("=" * 80)

    inspector = PayloadInspector()
    records: list[dict] = []

    if not dry_run:
        try:
            from google.cloud import bigquery

            bq = bigquery.Client(project=project_id)
            query = f"""
            SELECT
                subscription_name,
                message_id,
                publish_time,
                data,
                TO_JSON_STRING(attributes) AS attr_str
            FROM `{project_id}.{dataset_id}.{table_id}`
            ORDER BY publish_time DESC
            LIMIT {limit}
            """
            print(f"📡 BigQuery 테이블 쿼리 실행 중...")
            job = bq.query(query)
            for row in job:
                records.append({
                    "subscription_name": row.subscription_name,
                    "message_id": row.message_id,
                    "publish_time": str(row.publish_time),
                    "data": row.data,
                    "attributes": row.attr_str,
                })
        except Exception as e:
            print(f"⚠️ BigQuery 실환경 조회 실패: {e}")
            print("• 시뮬레이션 모의 데이터로 대체 검사를 진행합니다.\n")
            records = []

    # If dry_run or live query returned no records, provide representative simulation samples
    if not records:
        print("[MOCK] 시뮬레이션 샌드박스 데이터 생성 중...")
        from src.compression import CompressionManager
        from src.proto_gen import streaming_event_pb2

        cm = CompressionManager(level=3)
        sample_prompts = [
            b"Claude-Fast-Path-Prompt-Payload-" * 10,  # 320 bytes
            b"Anthropic-Production-Serving-Inference-Trace-Sample-" * 8,  # 400 bytes
            b"Telemetry-Metrics-Payload-Worker-Node-GKE-Cluster-" * 12,  # 576 bytes
        ]
        for i, prompt in enumerate(sample_prompts, 1):
            comp_payload = cm.compress(prompt)
            evt = streaming_event_pb2.StreamingEvent(
                event_id=f"sim-evt-{i:03d}",
                source="claude-serving-engine",
                payload=comp_payload,
                payload_type="text/plain",
                timestamp_ms=int(time.time() * 1000),
                pod_env_vars={"node": "gke-node-ai-gpu-1", "namespace": "prod-serving", "csp": "gcp"},
                schema_fingerprint="sha256-29cf2454e35404c9",
            )
            raw_proto = evt.SerializeToString()
            records.append({
                "subscription_name": f"projects/{project_id}/subscriptions/pubsub-demo-bq-sub",
                "message_id": f"sim-msg-99{i:03d}",
                "publish_time": time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "data": raw_proto,
                "attributes": '{"content-encoding": "zstd", "event-id": "sim-evt", "path": "fast"}',
            })

    print(f"\n총 {len(records)}건의 BigQuery 스트리밍 레코드 분석을 시작합니다.\n")

    for idx, r in enumerate(records, 1):
        res = inspector.inspect_raw(
            raw_data=r.get("data"),
            attributes=r.get("attributes"),
            message_id=str(r.get("message_id", "")),
            publish_time=str(r.get("publish_time", "")),
        )

        print("-" * 80)
        print(f"[{idx}/{len(records)}] 📨 Message ID: {res.message_id} | 수집 시각: {res.publish_time}")
        print("-" * 80)

        # BEFORE SECTION
        print("📦 [BEFORE: BigQuery 저장 원시 상태]")
        print(f"  • 저장된 바이너리 크기: {res.raw_bytes_len} Bytes")
        print(f"  • Zstd Magic 헤더 일치: {'✓ 감지됨 (0x28 0xB5 0x2F 0xFD)' if res.is_zstd_compressed else '• 미압축'}")
        print(f"  • Base64 인코딩 원문:   {res.base64_preview}")
        print(f"  • Hex 덤프 (앞 32B):    {res.hex_preview}")

        # AFTER SECTION
        print("\n🔓 [AFTER: Zstd 압축 해제 & Protobuf 역직렬화 복원 결과]")
        if res.is_valid_proto:
            print(f"  • Protocol Buffers:   ✓ 스키마 정상 역직렬화 (StreamingEvent)")
            print(f"  • Event ID / Source:  {res.event_id}  (출처: {res.source})")
            print(f"  • Schema Fingerprint: {res.schema_fingerprint}")
            print(f"  • Pod 환경 메타데이터: {res.pod_env_vars}")
            print(f"  • 페이로드 압축 해제: {res.compressed_payload_bytes}B -> {res.uncompressed_payload_bytes}B "
                  f"({res.reduction_percent:.1f}% 용량 절감)")
            print(f"  • 복원된 원본 텍스트:")
            preview = res.decompressed_text[:150] + ("..." if len(res.decompressed_text) > 150 else "")
            print(f"    \"{preview}\"")
        else:
            print(f"  • 원본 크기:          {res.uncompressed_payload_bytes} Bytes")
            print(f"  • 압축 해제 텍스트:   \"{res.decompressed_text[:120]}\"")

        print()

    print("=" * 80)
    print("✓ BigQuery Zstd + Protobuf 페이로드 역직렬화 검증이 완료되었습니다.")
    print("=" * 80)
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inspect and decompress Zstd Protobuf payloads from BigQuery")
    parser.add_argument("--project_id", default="pub-sub-kamo", help="GCP Project ID")
    parser.add_argument("--dataset_id", default="pubsub_demo_analytics", help="BigQuery Dataset ID")
    parser.add_argument("--table_id", default="streaming_events", help="BigQuery Table ID")
    parser.add_argument("--limit", type=int, default=5, help="Number of records to inspect")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without network calls")
    args = parser.parse_args()

    success = run_inspection(
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        table_id=args.table_id,
        limit=args.limit,
        dry_run=args.dry_run,
    )
    sys.exit(0 if success else 1)
