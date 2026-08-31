# 🚀 Google Cloud Pub/Sub Batch Messaging 프로덕션 권장 튜닝 가이드

> **참고 공식 문서**:
> - [Google Cloud Pub/Sub Batch Messaging 공식 문서](https://docs.cloud.google.com/pubsub/docs/batch-messaging)
> - [Publisher Batch Settings 코드 샘플](https://docs.cloud.google.com/pubsub/docs/samples/pubsub-publisher-batch-settings)

Google Cloud Pub/Sub에서 프로덕션 환경의 **`BatchSettings` (일괄 처리 설정)**는 **처리량(Throughput), 지연 시간(Latency), 비용(API 호출 수 및 1KB 과금 단위), 그리고 시스템 안정성(OOM 방지 및 소켓 고갈 방지)** 간의 최적 균형(Trade-off)을 결정짓는 핵심 아키텍처 설정입니다.

---

## 1. 핵심 원리 및 물리 한도 (Hard Limits)

Pub/Sub 클라이언트 라이브러리는 메시지를 메모리 내 내부 버퍼에 적재해 두었다가, 아래 **3가지 조건 중 하나라도 먼저 도달(OR 조건)**하면 즉시 단일 `PublishRequest` RPC로 묶어 브로커로 전송합니다.

| 조건 파라미터 (Parameter) | 기본값 (Default) | 프로덕션 권장치 (High Throughput) | Pub/Sub 서버 물리 한계 (Hard Limit) |
| :--- | :--- | :--- | :--- |
| **메시지 개수 (`max_messages` / `ElementCount`)** | **100개** | **1,000개** | **단일 요청당 최대 1,000개** |
| **바이트 크기 (`max_bytes` / `RequestByte`)** | **1KB (Java) / 1MB (Py)** | **8MB ~ 9.5MB** | **단일 요청당 최대 10MB (10,000,000 B)** |
| **지연 시간 (`max_latency` / `DelayThreshold`)** | **1ms (Java) / 10ms (Py)** | **50ms ~ 100ms** | **제한 없음** |

> [!WARNING]
> **클라이언트 라이브러리 기본값(Default)의 위험성**:
> 라이브러리 기본 설정(`1ms`, `1KB` / `10ms`, `1MB`)은 지연 시간 단축에 극단적으로 치우쳐 있어, 실제 트래픽 인입 시 메시지마다 개별 RPC가 폭증합니다. 이로 인해 다음과 같은 심각한 프로덕션 문제가 발생합니다:
> 1. **CPU 과점유 및 네트워크 소켓(File Descriptor) 고갈**
> 2. **네트워크 왕복(RTT) 핸드셰이크 누적으로 인한 전체 시스템 처리량 저하**
> 3. **1KB 미만 메시지의 10배 과금 폭탄 (개별 전송 시 100B 메시지도 무조건 1,000B로 올림 과금)**
> 
> 따라서 고부하 프로덕션 환경에서는 반드시 워크로드에 맞게 배치 파라미터를 튜닝해야 합니다.

---

## 2. 워크로드 성격별 권장 프리셋 (Presets)

비즈니스 요구사항에 따라 아래 3가지 표준 프리셋 중 하나를 선택하여 적용합니다.

### 🔴 패턴 A: 초저지연 프리셋 (Ultra-Low Latency)
- **적용 대상**: 실시간 이상 거래 탐지(FDS), 결제 승인, 사용자 인터랙티브 알림, 긴급 시스템 제어
- **핵심 목표**: 지연 시간 극소화(Sub-15ms) 유지, 순간적인 버스트 트래픽에만 자연스러운 배치 형성
- **권장 설정치**:
  - `max_messages`: **50 ~ 100개**
  - `max_bytes`: **256KB ~ 1MB**
  - `max_latency`: **1ms ~ 10ms** (0.001s ~ 0.010s)

### 🟡 패턴 B: 범용 고처리량 프리셋 (General High-Throughput) — ⭐ 프로덕션 표준 권장
- **적용 대상**: 일반 웹/앱 백엔드 API, 마이크로서비스 간 도메인 이벤트 발행, 사용자 행동 로그
- **핵심 목표**: 사용자 체감 성능(50ms 이내)에 영향을 주지 않으면서 네트워크 RPC 호출 수를 90% 이상 절감하고 소켓 리소스 안정화
- **권장 설정치**:
  - `max_messages`: **500 ~ 1,000개**
  - `max_bytes`: **4MB ~ 8MB**
  - `max_latency`: **30ms ~ 50ms** (0.030s ~ 0.050s)

### 🟢 패턴 C: 벌크 데이터 파이프라인 프리셋 (Bulk Ingestion & ETL)
- **적용 대상**: 대규모 분산 모델 학습 텔레메트리, 인프라 메트릭 수집, 데이터 웨어하우스(BigQuery) 적재용 대용량 로그
- **핵심 목표**: 브로커 전송 바이트 극대화(배치당 ~9MB), 1KB 최소 과금 완벽 무력화, 클라이언트 Zstd 압축 효율 극대화
- **권장 설정치**:
  - `max_messages`: **1,000개** (서버 하드 리밋)
  - `max_bytes`: **9.0MB ~ 9.5MB** (gRPC 메타데이터 오버헤드 마진)
  - `max_latency`: **100ms ~ 500ms** (0.100s ~ 0.500s)

---

## 3. 프로덕션 운영 시 필수 체크리스트 (Top 4 Gotchas)

### ① 10MB 물리 한도와 gRPC 프레임 오버헤드 (RequestByteThreshold)
* **문제 배경**: Google Cloud Pub/Sub 서버의 단일 요청 물리 상한은 정확히 **10,000,000 바이트**입니다.
* **위험 요인**: `max_bytes`를 정확히 10MB(`10 * 1024 * 1024 = 10,485,760 B`) 또는 `10,000,000 B`로 설정하면, gRPC 프로토콜 프레이밍, TLS 레코드 헤더, 메시지 속성(Attributes) 메타데이터가 추가되면서 서버에서 간헐적으로 `INVALID_ARGUMENT: Request payload size exceeds the limit: 10000000 bytes` 오류를 반환하며 배치가 통째로 드롭됩니다.
* **해결책**: 반드시 **8MB ~ 9.5MB(9,000,000 ~ 9,500,000 바이트)** 수준으로 **500KB~1MB의 안전 마진(Safety Margin)**을 두어야 합니다.

### ② FlowControl 누락 시 OOM 발생 (가장 흔한 프로덕션 장애 요인)
* **문제 배경**: Pub/Sub 클라이언트는 비동기 논블로킹(Non-blocking) 방식으로 작동하며, `publish()` 호출 시 즉시 `Future` 객체를 반환합니다.
* **기본값의 함정**: 기본 설정인 `LimitExceededBehavior.IGNORE` 상태에서는 네트워크 일시 단절이나 다운스트림 브로커 지연이 발생할 때, 백그라운드 메모리 버퍼에 미완료 `Future` 객체와 페이로드 바이트가 무한정 적재됩니다.
* **결과**: **JVM 힙 메모리 고갈 또는 Python 메모리 스파이크로 인한 프로세스 OOM Crash** 발생!
* **해결책**: 클라이언트 수준의 **발행 유량 제어(`PublishFlowControl`)**를 반드시 구성하고, 한도 초과 시 발행 호출자를 일시 대기시키는 `LimitExceededBehavior.BLOCK`을 활성화해야 합니다.

### ③ 프로세스 종료 시 Graceful Shutdown 미구현으로 인한 데이터 유실
* **문제 배경**: 메시지들이 메모리 큐에 배치 대기 상태(`max_latency` 동안 체류)에 있는 도중 배포, 재기동, 컨테이너 Scale-in 등으로 인해 프로세스가 종료되면 버퍼의 메시지가 공중으로 증발합니다.
* **해결책**:
  - Python: `atexit` 모듈 또는 SIGTERM 핸들러에서 대기 중인 모든 Future 완료를 기다립니다.
  - Java: `Runtime.getRuntime().addShutdownHook`에서 `publisher.shutdown()` 후 `publisher.awaitTermination(60, TimeUnit.SECONDS)`를 반드시 호출합니다.

### ④ Ordering Key 사용 시 단일 실패로 인한 연쇄 블록 현상
* **문제 배경**: 메시지 순서 보장을 위해 `enable_message_ordering = True` 및 `ordering_key`를 부여한 경우, Pub/Sub은 동일 키를 가진 메시지의 선후 관계를 엄격히 유지합니다.
* **위험 요인**: 만약 특정 배치의 메시지 하나가 유효성 검증 실패(예: 스키마 불일치, 권한 부족) 등으로 영구 실패하면, **동일한 Ordering Key를 가진 후속 메시지들이 전부 전송 중단된 채 버퍼에 블록**됩니다.
* **해결책**: 실패 콜백에서 에러를 로깅하고, 필요 시 `publisher.resume_publish(ordering_key)`를 호출하여 후속 메시지의 파이프라인을 재개하거나 Dead Letter 처리를 연계해야 합니다.

---

## 4. 언어별 프로덕션 표준 구현 코드

### 🐍 Python 프로덕션 표준 코드 (`google-cloud-pubsub`)

```python
import atexit
import logging
from concurrent import futures
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import (
    BatchSettings,
    LimitExceededBehavior,
    PublishFlowControl,
    PublisherOptions,
)

logger = logging.getLogger("PubSubPublisher")

def create_production_publisher(project_id: str, topic_id: str):
    # 1. BatchSettings 구성 (범용 고처리량 패턴 B 기준)
    batch_settings = BatchSettings(
        max_messages=1000,              # 서버 상한 1,000개
        max_bytes=9 * 1024 * 1024,      # 9MB (10MB 서버 하드 리밋 내 안전 마진 확보)
        max_latency=0.05,               # 50ms (0.05초)
    )

    # 2. FlowControl 구성 (OOM 방지 핵심: 50MB 또는 5,000건 초과 시 블로킹 백프레셔)
    flow_control = PublishFlowControl(
        message_limit=5000,
        byte_limit=50 * 1024 * 1024,    # 최대 50MB 미완료 버퍼 허용
        limit_exceeded_behavior=LimitExceededBehavior.BLOCK,  # 메모리 보호를 위해 호출자 스레드 일시 대기
    )

    # 3. PublisherOptions 바인딩
    publisher_options = PublisherOptions(
        enable_message_ordering=False,  # 순서 보장 필요 시 True 및 resume_publish 핸들링
        flow_control=flow_control,
    )

    # 4. 클라이언트 인스턴스화
    publisher = pubsub_v1.PublisherClient(
        batch_settings=batch_settings,
        publisher_options=publisher_options,
    )
    topic_path = publisher.topic_path(project_id, topic_id)

    # 5. Graceful Shutdown 훅 등록 (프로세스 종료 시 잔여 메시지 플러시 보장)
    pending_futures = []

    def cleanup():
        logger.info("Graceful shutdown initiated: Flushing pending Pub/Sub batches...")
        # 남은 모든 publish future 완료 대기 (최대 30초)
        done, not_done = futures.wait(pending_futures, timeout=30.0)
        if not_done:
            logger.error(f"Shutdown timeout: {len(not_done)} messages were not published!")
        else:
            logger.info("All pending batches successfully published to Pub/Sub.")

    atexit.register(cleanup)

    return publisher, topic_path, pending_futures

# [발행 및 비동기 콜백 패턴]
def publish_event(publisher, topic_path, pending_futures, payload_bytes: bytes, attributes: dict = None):
    def on_publish_done(future):
        try:
            message_id = future.result()
            # 발행 성공 처리
        except Exception as exc:
            logger.error(f"Failed to publish message: {exc}", exc_info=True)
        finally:
            if future in pending_futures:
                pending_futures.remove(future)

    # 논블로킹 발행 호출 (BatchSettings에 의해 자동 버퍼링 및 일괄 발송)
    future = publisher.publish(topic_path, payload_bytes, **(attributes or {}))
    pending_futures.append(future)
    future.add_done_callback(on_publish_done)
```

---

### ☕ Java 프로덕션 표준 코드 (`google-cloud-pubsub`)

```java
package com.example.pubsub;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController.LimitExceededBehavior;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ProductionPublisherManager {
    private static final Logger logger = Logger.getLogger(ProductionPublisherManager.class.getName());
    private final Publisher publisher;

    public ProductionPublisherManager(String projectId, String topicId) throws IOException {
        TopicName topicName = TopicName.of(projectId, topicId);

        // 1. BatchingSettings (처리량 및 지연 시간 튜닝)
        BatchingSettings batchingSettings = BatchingSettings.newBuilder()
            .setElementCountThreshold(1000L)              // 최대 1,000건
            .setRequestByteThreshold(9_500_000L)           // 9.5MB (10MB 한도 내 안전 마진)
            .setDelayThreshold(Duration.ofMillis(50))      // 50ms 지연 허용
            // 2. FlowControlSettings (OOM 방지 백프레셔 핵심)
            .setFlowControlSettings(
                FlowControlSettings.newBuilder()
                    .setMaxOutstandingElementCount(10_000L)
                    .setMaxOutstandingRequestBytes(100_000_000L) // 100MB 힙 메모리 상한
                    .setLimitExceededBehavior(LimitExceededBehavior.Block) // 메모리 보호
                    .build()
            )
            .build();

        // 3. Publisher 인스턴스 빌드
        this.publisher = Publisher.newBuilder(topicName)
            .setBatchingSettings(batchingSettings)
            .setEnableMessageOrdering(false)
            .build();

        // 4. JVM Graceful Shutdown Hook 등록
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("JVM Shutdown: Flushing remaining Pub/Sub batches...");
            try {
                this.publisher.shutdown();
                if (!this.publisher.awaitTermination(60, TimeUnit.SECONDS)) {
                    logger.warning("Pub/Sub publisher failed to terminate cleanly within 60s.");
                } else {
                    logger.info("Pub/Sub publisher cleanly terminated.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.log(Level.SEVERE, "Shutdown interrupted", e);
            }
        }));
    }

    public void publish(byte[] data) {
        PubsubMessage message = PubsubMessage.newBuilder()
            .setData(ByteString.copyFrom(data))
            .build();

        ApiFuture<String> future = publisher.publish(message);

        // 비동기 완료 콜백 등록
        ApiFutures.addCallback(future, new ApiFutureCallback<String>() {
            @Override
            public void onSuccess(String messageId) {
                // 정상 발행 완료
            }

            @Override
            public void onFailure(Throwable t) {
                logger.log(Level.SEVERE, "Publish failed for message", t);
            }
        }, MoreExecutors.directExecutor());
    }
}
```

---

## 5. 결론: 프로덕션 체크 요약표

| 검증 항목 | 미적용 시 위험 | 권장 대응책 |
| :--- | :--- | :--- |
| **Batching 활성화** | RPC 폭증, 소켓 고갈, 1KB 올림 10배 과금 | 워크로드별 `max_messages(500~1000)`, `max_latency(30~50ms)` 적용 |
| **10MB 마진 확보** | `INVALID_ARGUMENT` 에러 및 배치 유실 | `max_bytes`를 8MB ~ 9.5MB로 제한하여 헤더 오버헤드 방어 |
| **FlowControl 설정** | 지연/장애 시 미완료 Future 누적으로 OOM | `LimitExceededBehavior.BLOCK` 및 `byte_limit` 명시 |
| **Graceful Shutdown** | 배포/재기동 시 버퍼 내 데이터 유실 | 종료 훅에서 `shutdown()` 및 완료 대기(`wait`/`awaitTermination`) 필수 |
