#!/usr/bin/env python3
"""Cloud Monitoring 그래프 시각화를 위한 지속형 트래픽 생성 및 계측 스크립트.

Google Cloud Monitoring의 Pub/Sub 메트릭(pull_request_count, streaming_pull_response_count,
oldest_unacked_message_age, ack_latencies)은 약 2~3분의 집계 지연(Ingestion Latency)이 있으며,
단발성 5~10건의 트래픽은 차트에서 점 하나로만 나타나 'No data'로 오인될 수 있습니다.
본 스크립트는 30초~60초 동안 지속적인 스트림/폴링 트래픽을 발생시켜 Cloud Monitoring 차트에
선명한 시계열 그래프를 생성합니다.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.gcp_client import GCPClientFactory, GCPMode
from src.publisher import DualPathPublisher
from src.workers.streaming_worker import StreamingPullWorker
from src.workers.sync_worker import SyncPullWorker


def generate_traffic(
    project_id: str = "pub-sub-kamo",
    topic_id: str = "pubsub-demo-events",
    duration_seconds: int = 30,
) -> None:
    print("=" * 80)
    print("🚀 [Cloud Monitoring] 지속형 스트리밍 vs 동기식 풀 트래픽 발생기 시작")
    print(f"• 프로젝트 ID:   {project_id}")
    print(f"• 대상 토픽:     {topic_id}")
    print(f"• 실행 지속 시간: {duration_seconds}초")
    print("=" * 80)

    client = GCPClientFactory.get_client(mode=GCPMode.LIVE, project_id=project_id)
    publisher = DualPathPublisher(
        client=client,
        topic_id=topic_id,
        bucket_name=f"{project_id}-payloads",
    )

    sync_worker = SyncPullWorker(
        client=client,
        project_id=project_id,
        subscription_id="pubsub-demo-sync-sub",
        topic_id=topic_id,
    )

    stream_received_count = 0

    def stream_cb(msg):
        nonlocal stream_received_count
        stream_received_count += 1

    stream_worker = StreamingPullWorker(
        client=client,
        project_id=project_id,
        subscription_id="pubsub-demo-stream-sub",
        topic_id=topic_id,
        callback=stream_cb,
    )

    # 1. Start persistent gRPC streaming pull
    print("⚡ [StreamingPull] 양방향 gRPC 스트림 채널 개방 중...")
    stream_worker.start()

    start_time = time.time()
    batch_idx = 0
    total_published = 0
    total_synced = 0

    print("📡 지속형 트래픽 전송 및 풀링 진행 중 (2초 간격)...")
    payload = b"Anthropic-Claude-Serving-Cloud-Monitoring-Continuous-Telemetry-Payload-" * 4

    try:
        while time.time() - start_time < duration_seconds:
            batch_idx += 1
            now_str = time.strftime("%H:%M:%S")

            # Publish 3 messages per interval
            for i in range(3):
                publisher.publish_event(
                    event_id=f"mon-{batch_idx}-{i}",
                    source="continuous-traffic-gen",
                    payload=payload,
                )
                total_published += 1

            # Sync Pull: Unary RPC
            pulled = sync_worker.pull_batch(max_messages=3)
            total_synced += len(pulled)

            print(f"[{now_str}] ⏱️ 경과: {int(time.time() - start_time):02d}s | "
                  f"발행 누적: {total_published}건 | "
                  f"Sync Pull 수신: {total_synced}건 | "
                  f"StreamingPull 수신: {stream_received_count}건")

            time.sleep(2.0)
    finally:
        print("\n🛑 스트리밍 워커 정리 중...")
        stream_worker.stop()

    print("=" * 80)
    print("🎉 지속형 트래픽 전송 완료!")
    print(f"• 총 발행 메시지 수:   {total_published}건")
    print(f"• Sync Pull 수신 및 Ack: {total_synced}건 (Unary RPC 호출)")
    print(f"• StreamingPull 실시간 수신: {stream_received_count}건 (gRPC Push)")
    print("\n💡 [Cloud Monitoring 확인 안내]")
    print("Google Cloud Pub/Sub의 시스템 메트릭은 약 2~3분의 집계 주기 후 Cloud Monitoring에 완벽히 반영됩니다.")
    print("약 2분 뒤 아래 콘솔 링크를 새로고침하시면 선명한 그래프를 확인하실 수 있습니다:")
    print(f"• Sync Pull 콘솔: https://console.cloud.google.com/cloudpubsub/subscription/detail/pubsub-demo-sync-sub?project={project_id}&tab=metrics")
    print(f"• StreamingPull 콘솔: https://console.cloud.google.com/cloudpubsub/subscription/detail/pubsub-demo-stream-sub?project={project_id}&tab=metrics")
    print("=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate continuous traffic for Cloud Monitoring")
    parser.add_argument("--project_id", default="pub-sub-kamo", help="GCP Project ID")
    parser.add_argument("--duration", type=int, default=30, help="Duration in seconds")
    args = parser.parse_args()
    generate_traffic(project_id=args.project_id, duration_seconds=args.duration)
