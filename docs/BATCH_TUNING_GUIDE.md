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

## 2. 워크로드별 권장 설정 가이드

| 설정 항목 | 표준/일반 프로덕션 (Balanced) | 대용량 처리/데이터 파이프라인 (High Throughput) | 저지연 실시간 처리 (Low Latency) |
| :--- | :--- | :--- | :--- |
| **메시지 수 (`max_messages`)** | **100 ~ 500개** | **1,000개** (서버 한계) | **10 ~ 50개** |
| **요청 바이트 (`max_bytes`)** | **1 MB (1,000,000 B)** | **8 MB (8,000,000 B)** | **256 KB** |
| **대기 시간 (`max_latency`)** | **10 ms (0.01초)** | **50 ms ~ 100 ms** | **1 ms ~ 5 ms** |
| **Flow Control 정책** | **BLOCK (메모리 보호)** | **BLOCK (메모리 보호)** | **BLOCK / THROW_EXCEPTION** |

---

## 3. 프로덕션 운영 시 주의사항 (Watch-out)

| 항목 | 점검 포인트 및 권장 조치 |
| :--- | :--- |
| **비동기 Future 처리** | `publish()` 호출 시 즉시 전송되지 않고 Future를 반환하므로, 반드시 콜백(Callback)을 등록하여 전송 실패/에러를 핸들링해야 합니다. |
| **Graceful Shutdown** | 애플리케이션 종료 시 Publisher 클라이언트를 반드시 `shutdown()` 또는 버퍼 플러시(`awaitTermination`)하여 메모리에 남아있는 배치가 유실되지 않도록 해야 합니다. |
| **Ordering Key 사용 시** | 메시지 순서 보장(`OrderingKey`)을 활성화한 경우, 특정 키에 장애가 발생하면 해당 키의 후속 배치가 블로킹되므로 재시도 정책과 에러 핸들링을 별도로 분리해야 합니다. |

### 세부 점검 가이드 (Deep Dive)
* **10MB 물리 한도 마진**: 서버 한계(10,000,000 B)에 임계치를 맞추면 gRPC 헤더 오버헤드로 `INVALID_ARGUMENT` 오류가 발생하므로 반드시 **8MB ~ 9.5MB**로 설정합니다.
* **FlowControl 누락 방지**: 기본값 `IGNORE`는 백그라운드 큐에 Future가 무한정 쌓여 프로세스 OOM Crash를 초래합니다. `LimitExceededBehavior.BLOCK`을 필수로 적용하세요.
* **종료 훅 등록**: 컨테이너 Scale-in이나 SIGTERM 수신 시 버퍼에 남은 메시지가 버려지지 않도록 `atexit` 또는 Java ShutdownHook에서 잔여 Future 완료 대기를 강제합니다.

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
