"""동기식 배치 풀 워커 모듈 (Synchronous Pull Worker).

[Anthropic 아키텍처 배경]
초기 이벤트 아키텍처에서는 일반적인 마이크로서비스 관행대로 주기적 폴링 루프(Periodic Polling Loop)를
통해 Pub/Sub 메시지를 배치 단위로 동기식 풀링(Unary Pull)했습니다.
그러나 이 방식은 매 배치마다 발생하는 네트워크 왕복 핸드셰이크와 유휴 대기 시간(Idle Wait Time)으로 인해
평균 80ms~150ms 수준의 높은 전달 지연이 발생하여, 실시간 모델 서빙 및 에이전트 로그 수집에 병목이 되었습니다.
본 워커는 이러한 레거시 폴링 방식을 시뮬레이션 및 계측하여 StreamingPull과 비교하는 역할을 합니다.
"""

import time
from dataclasses import dataclass, field

from src.gcp_client import GCPClientInterface, GCPMode


@dataclass
class PullMessageResult:
    """동기식 Pull을 통해 수신된 개별 메시지 결과 모델."""

    data: bytes                              # 수신된 메시지 페이로드
    attributes: dict[str, str] = field(default_factory=dict)  # 메시지 속성
    message_id: str = ""                     # 메시지 ID
    ack_id: str = ""                         # 확인응답용 Ack ID
    latency_ms: float = 0.0                  # 수신 지연 시간 (밀리초)


class SyncPullWorker:
    """HTTP/gRPC 기반의 동기식 배치 풀(Unary Pull RPC)을 수행하는 워커 클래스."""

    def __init__(
        self,
        client: GCPClientInterface,
        project_id: str,
        subscription_id: str,
        topic_id: str,
        batch_size: int = 10,
        simulated_poll_delay_ms: float = 45.0,  # 폴링 주기 및 연결 핸드셰이크 오버헤드 시뮬레이션 값
    ):
        """동기식 풀 워커를 초기화합니다."""
        self.client = client
        self.project_id = project_id
        self.subscription_id = subscription_id
        self.topic_id = topic_id
        self.batch_size = batch_size
        self.simulated_poll_delay_ms = simulated_poll_delay_ms
        self._subscriber = None

    @property
    def subscriber(self):
        """실제 GCP 환경 연결 시 SubscriberClient를 지연 로딩합니다."""
        if self._subscriber is None and self.client.mode == GCPMode.LIVE:
            from google.cloud import pubsub_v1

            self._subscriber = pubsub_v1.SubscriberClient()
        return self._subscriber

    def pull_batch(self, max_messages: int | None = None) -> list[PullMessageResult]:
        """동기식 Pull 요청을 전송하여 최대 batch_limit개의 메시지를 수신하고 Ack 처리합니다."""
        batch_limit = max_messages or self.batch_size
        start_time = time.perf_counter()
        results: list[PullMessageResult] = []

        if self.client.mode == GCPMode.MOCK:
            # [모의 샌드박스] 메모리 토픽에서 메시지 배치 인출
            messages = self.client.get_published_messages(self.topic_id)
            if not messages:
                return []

            # 동기식 풀의 네트워크 왕복 오버헤드 및 폴링 대기 시간 시뮬레이션
            if self.simulated_poll_delay_ms > 0:
                time.sleep(self.simulated_poll_delay_ms / 1000.0)

            to_process = messages[:batch_limit]
            # 인출된 메시지는 큐에서 제거 (Ack 시뮬레이션)
            self.client.topics[self.topic_id] = messages[batch_limit:]

            elapsed_ms = (time.perf_counter() - start_time) * 1000.0
            per_msg_latency = elapsed_ms

            for msg in to_process:
                results.append(
                    PullMessageResult(
                        data=msg.data,
                        attributes=msg.attributes,
                        message_id=msg.message_id,
                        ack_id=f"ack-{msg.message_id}",
                        latency_ms=per_msg_latency,
                    )
                )
        else:
            # [실제 GCP 환경] Google Cloud Pub/Sub pull API 직접 호출
            sub_path = self.subscriber.subscription_path(self.project_id, self.subscription_id)
            try:
                response = self.subscriber.pull(
                    request={"subscription": sub_path, "max_messages": batch_limit},
                    timeout=2.0,
                )
            except Exception:
                # 대기 중인 메시지가 없거나 폴링 타임아웃 시 빈 리스트 반환
                return []

            ack_ids = []
            now_ms = time.time() * 1000.0
            elapsed_ms = (time.perf_counter() - start_time) * 1000.0

            for received in response.received_messages:
                if hasattr(received.message, "publish_time") and received.message.publish_time:
                    pub_ms = received.message.publish_time.timestamp() * 1000.0
                    msg_latency = max(elapsed_ms, now_ms - pub_ms)
                else:
                    msg_latency = elapsed_ms

                results.append(
                    PullMessageResult(
                        data=received.message.data,
                        attributes=dict(received.message.attributes),
                        message_id=received.message.message_id,
                        ack_id=received.ack_id,
                        latency_ms=msg_latency,
                    )
                )
                ack_ids.append(received.ack_id)

            # 수신 완료 후 즉시 일괄 확인응답(Acknowledge) 전송
            if ack_ids:
                self.subscriber.acknowledge(request={"subscription": sub_path, "ack_ids": ack_ids})

        return results
