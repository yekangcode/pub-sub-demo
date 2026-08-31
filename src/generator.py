"""합성 LLM 워크로드 트래픽 생성기 모듈 (Synthetic Workload Generator).

[Anthropic 아키텍처 배경]
Anthropic의 프로덕션 환경에서는 초당 수만 건의 텍스트 프롬프트/응답 이벤트와 함께,
대용량 멀티모달 이미지 및 고차원 임베딩 텐서(8MB 이상)가 불규칙하게 섞여서 유입됩니다.
본 모듈은 이러한 실제 LLM 서빙 환경의 트래픽을 현실감 있게 모사하여,
소형 인라인 메시지와 대형 GCS 오프로드 메시지, 그리고 의도적 포이즌 필(DLQ 격리용)을
사용자 지정 비율에 따라 대량 발행합니다.
"""

import os
import random
import uuid
from typing import Any

from src.publisher import DualPathPublisher, PublishResult

# 실제 Claude 서빙 환경을 모사한 샘플 프롬프트 목록
SAMPLE_PROMPTS = [
    "Google Cloud Pub/Sub과 gRPC StreamingPull의 내부 아키텍처 동작 원리를 설명해주세요.",
    "백프레셔(Backpressure)와 플로우 제어(Flow Control)를 구현한 고성능 Python 컨슈머를 작성하세요.",
    "Anthropic은 Protocol Buffers와 SHA-256 핑거프린트로 어떻게 실시간 LLM 텔레메트리를 거버넌스하나요?",
    "별도 Dataflow 없이 구현되는 BigQuery Zero-ETL 직접 구독 스트리밍 파이프라인의 장점을 비교 분석하세요.",
    "반복적인 JSON 로그 이벤트에 대한 Zstandard 압축 벤치마크 및 대역폭 절감 효과를 요약해주세요.",
]

# 발행 소스 (GKE 서빙 Pod 및 워커 인스턴스)
SAMPLE_SOURCES = [
    "claude-opus-serving-pod-01",
    "claude-sonnet-batch-runner",
    "anthropic-eval-worker-4",
    "agentic-workflow-router",
]


class SyntheticWorkloadGenerator:
    """소형 텍스트 프롬프트 및 대형 멀티모달 텐서를 지정 비율로 자동 생성하여 발행하는 클래스."""

    def __init__(
        self,
        publisher: DualPathPublisher,
        large_payload_pct: float = 10.0,  # 대형 페이로드(>=8MB) 발생 비율 (기본: 10%)
        corrupt_pct: float = 0.0,         # 손상된 포이즌 필 발생 비율 (기본: 0%)
    ):
        """합성 트래픽 생성기를 초기화합니다."""
        self.publisher = publisher
        self.large_payload_pct = max(0.0, min(100.0, large_payload_pct))
        self.corrupt_pct = max(0.0, min(100.0, corrupt_pct))
        self._seq = 0

    def generate_single_event(
        self,
        force_large: bool | None = None,
        force_corrupt: bool | None = None,
        extra_attributes: dict[str, Any] | None = None,
    ) -> PublishResult:
        """단일 합성 이벤트를 생성하고 DualPathPublisher를 통해 발행합니다."""
        self._seq += 1
        event_id = f"evt-{uuid.uuid4().hex[:8]}-{self._seq:06d}"
        source = random.choice(SAMPLE_SOURCES)

        # 대용량 이벤트 여부 결정
        is_large = force_large if force_large is not None else (random.uniform(0, 100) < self.large_payload_pct)
        # 포이즌 필 여부 결정
        is_corrupt = force_corrupt if force_corrupt is not None else (random.uniform(0, 100) < self.corrupt_pct)

        if is_large:
            # [대형 페이로드] 멀티모달 텐서 또는 이미지 바이너리 시뮬레이션
            # Zstd 압축 후에도 임계값을 넘을 수 있도록 고엔트로피(난수) 바이트 사용
            target_size = max(self.publisher.offload_threshold_bytes + 2048, 12 * 1024)
            payload = os.urandom(target_size)
            payload_type = "application/octet-stream"
        else:
            # [소형 페이로드] 일반 JSON 텍스트 프롬프트/응답 (Fast Path)
            prompt_text = random.choice(SAMPLE_PROMPTS)
            payload = (f'{{"prompt": "{prompt_text}", "seq": {self._seq}, "model": "claude-3-7-sonnet"}}\n' * 5).encode()
            payload_type = "application/json"

        return self.publisher.publish_event(
            event_id=event_id,
            source=source,
            payload=payload,
            payload_type=payload_type,
            is_corrupted=is_corrupt,
            custom_attributes=extra_attributes,
        )

    def generate_batch(self, count: int = 10) -> list[PublishResult]:
        """지정된 개수(count)만큼의 이벤트를 일괄 생성하여 연속 발행합니다."""
        results = []
        for _ in range(count):
            results.append(self.generate_single_event())
        return results
