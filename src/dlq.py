"""Dead Letter Queue (DLQ) 격리 및 장애 복원력 관리자 모듈 (DLQ Manager).

[Anthropic 아키텍처 배경]
수많은 개발팀과 자율 에이전트들이 이벤트를 발행할 때, 단 하나의 잘못된 스키마 변경이나
손상된 데이터(Poison Pill)가 유입되면 컨슈머가 무한 재시도를 반복하며 정상 메시지 처리가 멈추는
"Head-of-Line Blocking" 현상이 발생할 수 있습니다.
Anthropic은 이를 방지하기 위해 엄격한 Protobuf 스키마 검증과 함께,
최대 5회 재시도 실패 시 해당 메시지를 별도의 Dead Letter Topic(`pubsub-demo-dlq-topic`)으로
즉시 격리하는 서킷 브레이커(Circuit Breaker)를 구축하여 메인 파이프라인의 가용성(SLO 99.99%)을 보장합니다.
"""

from collections import defaultdict
from collections.abc import Callable
from typing import Any

from src.gcp_client import GCPClientInterface, PublishedMessage


class DLQManager:
    """메시지 배달 시도 횟수를 추적하고 한도 초과 시 DLQ로 격리하는 관리자 클래스."""

    def __init__(
        self,
        client: GCPClientInterface,
        main_topic_id: str,
        dlq_topic_id: str,
        max_delivery_attempts: int = 5,
    ):
        """DLQ 관리자를 초기화합니다.
        
        Args:
            client: Pub/Sub 통신용 클라이언트
            main_topic_id: 원본 메인 토픽 ID
            dlq_topic_id: 격리 목적지 Dead Letter 토픽 ID
            max_delivery_attempts: 최대 허용 배달 시도 횟수 (Google Cloud 기본 권장값: 5회)
        """
        self.client = client
        self.main_topic_id = main_topic_id
        self.dlq_topic_id = dlq_topic_id
        self.max_delivery_attempts = max_delivery_attempts
        # 메시지별 배달 시도 횟수 카운터
        self._delivery_attempts: dict[str, int] = defaultdict(int)

    def process_with_dlq(
        self,
        message: PublishedMessage,
        consumer_func: Callable[[bytes, dict[str, str]], Any],
    ) -> dict[str, Any]:
        """소비자 함수 실행을 감싸서, 5회 실패 시 자동으로 DLQ 토픽으로 우회 격리합니다."""
        msg_id = message.message_id
        self._delivery_attempts[msg_id] += 1
        attempts = self._delivery_attempts[msg_id]

        try:
            # 정상 처리 시도 (스키마 파싱 및 비즈니스 로직 실행)
            result = consumer_func(message.data, message.attributes)
            return {
                "status": "success",
                "attempts": attempts,
                "result": result,
            }
        except Exception as exc:  # noqa: BLE001 - DLQ 경계에서는 모든 애플리케이션 예외를 안전하게 포착
            error_msg = str(exc)
            
            # [단계 1] 최대 허용 시도 횟수(5회) 미만인 경우: 일시적 재시도(Retry) 허용
            if attempts < self.max_delivery_attempts:
                return {
                    "status": "retry",
                    "attempts": attempts,
                    "error": error_msg,
                }

            # [단계 2] 5회 재시도 모두 실패: 포이즌 필로 판정하고 Dead Letter Queue로 영구 격리
            dlq_attributes = dict(message.attributes)
            dlq_attributes["quarantine-reason"] = error_msg
            dlq_attributes["delivery-attempts"] = str(attempts)

            self.client.publish(
                topic_id=self.dlq_topic_id,
                data=message.data,
                attributes=dlq_attributes,
            )

            return {
                "status": "dead_lettered",
                "attempts": attempts,
                "error": error_msg,
            }

    def get_quarantined_messages(self) -> list[PublishedMessage]:
        """DLQ 토픽에 현재 격리되어 보관 중인 손상 메시지 목록을 조회합니다 (모의 샌드박스 모드)."""
        if hasattr(self.client, "get_published_messages"):
            return self.client.get_published_messages(self.dlq_topic_id)
        return []
