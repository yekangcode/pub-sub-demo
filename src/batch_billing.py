"""Google Cloud Pub/Sub 1KB 최소 과금 단위(Billing Unit) 분석 및 배치 최적화 모듈.

[문제 배경: Pub/Sub 1KB 최소 과금 단위 (Billing Unit)]
Google Cloud Pub/Sub의 공식 과금 정책(https://cloud.google.com/pubsub/pricing)에 따르면:
  - 메시지 크기가 1,000바이트(1KB) 미만인 경우에도, 최소 1,000바이트(1KB)로 올림(Rounding up)되어 과금됩니다.
  - 예: 100바이트짜리 메시지 10개를 각각 단일 요청으로 발행하면:
    * 실제 전송 데이터: 100B * 10 = 1,000B (1KB)
    * Pub/Sub 과금 볼륨: 1,000B * 10 = 10,000B (10KB) -> **무려 10배(1,000%) 비용 발생!**

[최적화 방안: 클라이언트 측 배치(BatchSettings) 적용]
Google Cloud Pub/Sub 클라이언트 라이브러리의 `BatchSettings`를 구성하여 여러 메시지를 하나의
PublishRequest(배치)로 묶어서 전송합니다:
  - BatchSettings(max_messages=100, max_bytes=1024*1024, max_latency=0.05)
  - 메시지 10개를 1개 배치로 묶어 발행하면 총 데이터 크기가 1,000바이트(1KB)로 합산되어 과금 볼륨 역시 1KB로 축소됩니다.
  - 비즈니스 허용 지연 시간(Latency Budget: 예 50ms) 내에서 max_messages와 max_latency를 튜닝하면
    지연 시간 영향 없이 최대 10배의 클라우드 비용을 절감할 수 있습니다.
"""

from dataclasses import dataclass


@dataclass
class BatchBillingComparison:
    """1KB 최소 과금 단위 배치 최적화 전후 정량 비교 결과."""

    message_size_bytes: int        # 개별 메시지 크기 (바이트)
    message_count: int             # 총 발행 메시지 건수
    batch_size: int                # 배치당 묶음 메시지 수 (1이면 비배치)
    actual_data_bytes: int         # 실제 전송된 순수 데이터 크기 (바이트)
    unbatched_billed_bytes: int    # 비배치 시 1KB 최소 과금 올림으로 청구되는 바이트
    batched_billed_bytes: int      # 배치 적용 시 청구되는 바이트
    cost_inflation_ratio: float    # 비배치 시 발생하는 과금 팽창 배수 (예: 10.0x)
    billed_bytes_saved: int        # 배치 적용으로 절감된 과금 바이트
    savings_percentage: float      # 과금 절감률 (%)
    unbatched_cost_usd: float      # 월간 예상 비용 (비배치)
    batched_cost_usd: float        # 월간 예상 비용 (배치 적용)


class BatchBillingOptimizer:
    """Pub/Sub 1KB 최소 과금 규칙에 따른 비용 팽창 및 BatchSettings 최적화 분석기."""

    # Google Cloud Pub/Sub 수집/배달 요금: TiB당 $40 (미국 리전 기준, 첫 10GB 무료 제외)
    PRICE_PER_TIB_USD = 40.0
    BYTES_PER_TIB = 1024**4

    @classmethod
    def calculate_billing(
        cls,
        message_size_bytes: int = 100,
        message_count: int = 10_000_000,
        batch_size: int = 10,
    ) -> BatchBillingComparison:
        """개별 메시지 크기와 배치 크기에 따른 Pub/Sub 과금 바이트 및 비용을 계산합니다."""
        actual_total_bytes = message_size_bytes * message_count

        # 1. 비배치(Unbatched): 모든 메시지가 최소 1,000바이트(1KB)로 반올림
        billed_per_msg_unbatched = max(message_size_bytes, 1000)
        unbatched_billed_bytes = billed_per_msg_unbatched * message_count

        # 2. 배치(Batched): 배치 단위로 묶여 전송되므로 배치 총합 크기가 1,000바이트 기준 적용
        batch_count = (message_count + batch_size - 1) // batch_size
        bytes_per_batch = message_size_bytes * batch_size
        billed_per_batch = max(bytes_per_batch, 1000)
        batched_billed_bytes = billed_per_batch * batch_count

        # 과금 팽창 배수 및 절감량
        cost_inflation = (
            round(unbatched_billed_bytes / batched_billed_bytes, 2)
            if batched_billed_bytes > 0
            else 1.0
        )
        bytes_saved = max(0, unbatched_billed_bytes - batched_billed_bytes)
        savings_pct = (
            round((bytes_saved / unbatched_billed_bytes) * 100.0, 1)
            if unbatched_billed_bytes > 0
            else 0.0
        )

        # 비용 계산 (USD)
        unbatched_cost = (unbatched_billed_bytes / cls.BYTES_PER_TIB) * cls.PRICE_PER_TIB_USD
        batched_cost = (batched_billed_bytes / cls.BYTES_PER_TIB) * cls.PRICE_PER_TIB_USD

        return BatchBillingComparison(
            message_size_bytes=message_size_bytes,
            message_count=message_count,
            batch_size=batch_size,
            actual_data_bytes=actual_total_bytes,
            unbatched_billed_bytes=unbatched_billed_bytes,
            batched_billed_bytes=batched_billed_bytes,
            cost_inflation_ratio=cost_inflation,
            billed_bytes_saved=bytes_saved,
            savings_percentage=savings_pct,
            unbatched_cost_usd=round(unbatched_cost, 2),
            batched_cost_usd=round(batched_cost, 2),
        )

    @classmethod
    def get_batch_settings_code_snippet(
        cls, max_messages: int = 100, max_bytes_mb: int = 1, max_latency_ms: int = 50
    ) -> str:
        """Google Cloud Pub/Sub 클라이언트 라이브러리에 권장되는 BatchSettings 및 FlowControl 파이썬 코드 스니펫."""
        return f"""import atexit
from concurrent import futures
from google.cloud import pubsub_v1

# 1. BatchSettings: 처리량 극대화 및 1KB 최소 과금 우회
batch_settings = pubsub_v1.types.BatchSettings(
    max_messages={max_messages},           # 배치당 최대 메시지 수 (권장 500~1000)
    max_bytes={max_bytes_mb} * 1024 * 1024,      # 배치당 최대 바이트 ({max_bytes_mb}MB, 10MB 한도 내 안전 마진)
    max_latency={max_latency_ms / 1000.0:.3f},         # 허용 레이턴시 버퍼: {max_latency_ms}ms 지연 시 자동 플러시
)

# 2. PublishFlowControl: OOM 방지 및 클라이언트 백프레셔 제어 (프로덕션 필수!)
flow_control = pubsub_v1.types.PublishFlowControl(
    message_limit=5000,
    byte_limit=50 * 1024 * 1024,    # 최대 50MB 미완료 버퍼 허용
    limit_exceeded_behavior=pubsub_v1.types.LimitExceededBehavior.BLOCK,  # 메모리 보호를 위해 대기
)

# 3. PublisherOptions 바인딩 및 클라이언트 인스턴스화
publisher = pubsub_v1.PublisherClient(
    batch_settings=batch_settings,
    publisher_options=pubsub_v1.types.PublisherOptions(
        enable_message_ordering=False,
        flow_control=flow_control,
    ),
)
topic_path = publisher.topic_path("your-project-id", "your-topic-id")

# 4. Graceful Shutdown: 프로세스 종료 시 잔여 버퍼 플러시
pending_futures = []
atexit.register(lambda: futures.wait(pending_futures, timeout=30.0))
"""
