"""Google Cloud Pub/Sub: Anthropic 아키텍처 심층 분석 인터랙티브 Streamlit 대시보드.

[Anthropic 아키텍처 데모 대시보드 구성]
- 탭 1 (아키텍처 개요): 3대 핵심 기둥(Dual-Path, StreamingPull, Proto-First/DLQ)과 공식 발표 슬라이드 링크
- 탭 2 (이중 경로 & 압축): Fast Path(<8MB) vs GCS Claim-Check(>=8MB) 실시간 트래픽 생성 및 바이트 절감률 관측
- 탭 3 (StreamingPull 88% 절감): 영구 gRPC StreamingPull vs 레거시 동기식 배치 폴링 P99 꼬리 지연 시간 실측 비교
- 탭 4 (Proto-First & DLQ): Protobuf SHA-256 스키마 거버넌스 및 5회 재시도 실패 시 Dead Letter Topic 격리 서킷 브레이커
- 탭 5 (실제 GCP 라이브 검증): `pub-sub-kamo` 프로젝트 상의 IAM, Dual-Path, 벤치마크, BigQuery Zero-ETL 실시간 원클릭 검증
"""

import sys
import time
from pathlib import Path

# Ensure repository root is in sys.path when executed via `streamlit run src/dashboard.py`
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import pandas as pd
import streamlit as st

from src.batch_billing import BatchBillingOptimizer
from src.consumer import DualPathConsumer
from src.dlq import DLQManager
from src.format_benchmark import DataFormatBenchmark
from src.gcp_client import GCPClientFactory, GCPMode
from src.generator import SyntheticWorkloadGenerator
from src.metrics import MetricsCollector
from src.publisher import DualPathPublisher
from src.workers.streaming_worker import StreamingPullWorker
from src.workers.sync_worker import SyncPullWorker

# Page Configuration
st.set_page_config(
    page_title="GCP Pub/Sub Anthropic Architecture Demo",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Styling (Emerald Green Fast Path, Rich Amber GCS Offload)
st.markdown(
    """
    <style>
    .metric-badge-green {
        background-color: #E8F5E9;
        color: #2E7D32;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: bold;
    }
    .metric-badge-amber {
        background-color: #FFF8E1;
        color: #F57F17;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: bold;
    }
    .latency-callout {
        background: linear-gradient(90deg, #1E88E5 0%, #0D47A1 100%);
        color: white;
        padding: 16px;
        border-radius: 8px;
        text-align: center;
        margin-bottom: 20px;
    }
    .doc-banner {
        background-color: #F0F4F8;
        border-left: 5px solid #1E88E5;
        padding: 10px 15px;
        border-radius: 4px;
        margin-bottom: 15px;
    }
    .workflow-card-sync {
        background: linear-gradient(145deg, #1c1517 0%, #29161a 100%);
        border: 2px solid #ef5350;
        border-radius: 10px;
        padding: 16px 20px;
        margin-bottom: 12px;
        box-shadow: 0 4px 14px rgba(239, 83, 80, 0.18);
    }
    .workflow-card-stream {
        background: linear-gradient(145deg, #0f1c24 0%, #11293a 100%);
        border: 2px solid #00e5ff;
        border-radius: 10px;
        padding: 16px 20px;
        margin-bottom: 12px;
        box-shadow: 0 4px 14px rgba(0, 229, 255, 0.18);
    }
    .step-pill-sync {
        background-color: #ef5350;
        color: white !important;
        font-weight: 800;
        font-size: 0.82em;
        padding: 3px 9px;
        border-radius: 12px;
        display: inline-block;
        margin-right: 6px;
    }
    .step-pill-stream {
        background-color: #00e5ff;
        color: #05141c !important;
        font-weight: 800;
        font-size: 0.82em;
        padding: 3px 9px;
        border-radius: 12px;
        display: inline-block;
        margin-right: 6px;
    }
    .flow-arrow-sync {
        color: #ff8a80;
        text-align: center;
        font-size: 0.95em;
        font-weight: 600;
        margin: 6px 0;
    }
    .flow-arrow-stream {
        color: #40c4ff;
        text-align: center;
        font-size: 0.95em;
        font-weight: 600;
        margin: 6px 0;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def init_session_state():
    if "metrics" not in st.session_state:
        st.session_state.metrics = MetricsCollector()
    if "event_log" not in st.session_state:
        st.session_state.event_log = []
    if "dlq_log" not in st.session_state:
        st.session_state.dlq_log = []
    if "streaming_worker" not in st.session_state:
        st.session_state.streaming_worker = None
    if "is_streaming" not in st.session_state:
        st.session_state.is_streaming = False


init_session_state()

# ----------------- SIDEBAR & LANGUAGE SELECTION -----------------
st.sidebar.title("🌐 Language / 언어")
lang = st.sidebar.radio("Display Language", ["한국어", "English"], index=0, label_visibility="collapsed")
is_ko = lang == "한국어"


def tr(ko: str, en: str) -> str:
    """Helper for bilingual display."""
    return ko if is_ko else en


st.sidebar.markdown("---")
st.sidebar.title(tr("🛠️ 데모 환경 설정", "🛠️ Demo Configuration"))

mode_options = [
    tr("모의 샌드박스 (로컬 / 오프라인)", "Mock Sandbox (Local / Offline)"),
    tr("실제 구글 클라우드 (pub-sub-kamo)", "Live Google Cloud (pub-sub-kamo)"),
]
mode_selection = st.sidebar.radio(
    tr("실행 모드", "Execution Mode"),
    mode_options,
    index=0,
)
gcp_mode = GCPMode.LIVE if "pub-sub-kamo" in mode_selection else GCPMode.MOCK
project_id = st.sidebar.text_input(tr("GCP 프로젝트 ID", "GCP Project ID"), value="pub-sub-kamo")

topic_id = "pubsub-demo-events"
dlq_topic_id = "pubsub-demo-dlq-topic"
bucket_name = f"{project_id}-payloads"

# Clients
client = GCPClientFactory.get_client(mode=gcp_mode, project_id=project_id)
publisher = DualPathPublisher(
    client=client,
    topic_id=topic_id,
    bucket_name=bucket_name,
    offload_threshold_bytes=8 * 1024 * 1024 if gcp_mode == GCPMode.LIVE else 50 * 1024,
)
consumer = DualPathConsumer(client=client)
dlq_manager = DLQManager(
    client=client,
    main_topic_id=topic_id,
    dlq_topic_id=dlq_topic_id,
    max_delivery_attempts=5,
)

st.sidebar.markdown("---")
st.sidebar.subheader(tr("⚡ 트래픽 생성기 제어", "⚡ Traffic Generator Controls"))
batch_size = st.sidebar.slider(tr("배치 크기 (이벤트 수)", "Batch Size (Events)"), min_value=1, max_value=50, value=10)
large_pct = st.sidebar.slider(tr("대용량 페이로드 비율 (>8MB)", "Large Payload Ratio (>8MB)"), min_value=0, max_value=100, value=20)
corrupt_pct = st.sidebar.slider(
    tr("손상된 이벤트 비율 (Poison Pills)", "Corrupted Event Ratio (Poison Pills)"), min_value=0, max_value=100, value=0
)

generator = SyntheticWorkloadGenerator(
    publisher=publisher,
    large_payload_pct=float(large_pct),
    corrupt_pct=float(corrupt_pct),
)

# Reference Doc Link in Sidebar
st.sidebar.markdown("---")
st.sidebar.markdown(
    f"📚 **{tr('참조 아키텍처 문서', 'Reference Architecture')}**:\n"
    f"- [{tr('Anthropic 발표 슬라이드 (PDF)', 'Anthropic Presentation (PDF)')}](https://content-cdn.sessionboard.com/content/IdZQpQJIQVmsSBjedvUW_BRK1-041.pdf)"
)

# ----------------- MAIN HEADER -----------------
st.title(tr("⚡ Google Cloud Pub/Sub: Anthropic 아키텍처 심층 분석 데모", "⚡ Google Cloud Pub/Sub: Anthropic Architecture Deep-Dive Demo"))
st.caption(
    f"{tr('연결 모드', 'Connected Mode')}: **{gcp_mode.value.upper()}** | "
    f"{tr('프로젝트', 'Project')}: `{project_id}` | "
    f"{tr('토픽', 'Topic')}: `{topic_id}`"
)

# Doc Banner
st.markdown(
    f"""
    <div class="doc-banner">
        📖 <strong>{tr("기술 배경 및 원본 자료", "Technical Background & Reference Material")}:</strong>
        {tr("본 데모는 Anthropic이 Google Cloud Pub/Sub 기반으로 구축한 대규모 실시간 스트리밍 아키텍처 공식 발표를 기반으로 구현되었습니다.", "This demo is grounded in Anthropic's official technical presentation on building large-scale streaming systems on Google Cloud Pub/Sub.")}
        <br>
        👉 <a href="https://content-cdn.sessionboard.com/content/IdZQpQJIQVmsSBjedvUW_BRK1-041.pdf" target="_blank" style="font-weight:bold; color:#1565C0;">
            {tr("How Anthropic Built on Google Cloud Pub/Sub (원본 발표 슬라이드 PDF 열기)", "How Anthropic Built on Google Cloud Pub/Sub (Open Original PDF)")}
        </a>
    </div>
    """,
    unsafe_allow_html=True,
)

# Tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    [
        tr("🏛️ 아키텍처 개요", "🏛️ Architecture Overview"),
        tr("📦 1. 이중 경로 수집 & 압축", "📦 1. Dual-Path Ingestion & Compression"),
        tr("🚀 2. StreamingPull 지연 시간 절감 (88%)", "🚀 2. StreamingPull vs Sync Pull (88% Latency Drop)"),
        tr("🛡️ 3. 데이터 포맷 최적화 & DLQ", "🛡️ 3. Binary Schema Optimization & DLQ"),
        tr("🔍 4. 실제 GCP 프로젝트 라이브 검증", "🔍 4. Live GCP Project Verification"),
    ]
)

# ----------------- TAB 1: ARCHITECTURE OVERVIEW -----------------
with tab1:
    if is_ko:
        st.markdown(
            """
        ### Anthropic의 Google Cloud 기반 초대규모 스트리밍 이벤트 플랫폼 3대 핵심 축

        1. **이중 경로(Dual-Path) 수집 및 Zstandard 압축**:
           - **8MB 미만 페이로드 (Fast Path)**: Zstandard로 압축하여 Pub/Sub 인라인으로 즉시 고속 전송.
           - **8MB 이상 페이로드 (Offload Path)**: Cloud Storage(`gs://pub-sub-kamo-payloads`)로 자동 오프로드 후 가벼운 포인터 URI만 Pub/Sub 메시지에 포함.
           - **투명한 재구성**: 다운스트림 컨슈머는 인라인/GCS 여부와 무관하게 동일한 Protobuf 모델로 투명하게 복원.
        2. **StreamingPull로의 전환을 통한 지연 시간 88% 절감**:
           - 기존 HTTP/gRPC 동기식 배치 폴링 루프의 연결 핸드셰이크와 대기 오버헤드(~95ms) 전면 제거.
           - 영구적인 양방향 gRPC 스트림 채널을 유지하여 브로커가 메시지를 도착 즉시 푸시 (~11ms 달성).
        3. **데이터 전송 포맷 & 배치 최적화 (Binary Schema & BatchSettings) & DLQ 거버넌스**:
           - **바이너리 스키마(Protobuf)**: 비효율적인 JSON 필드명을 제거하고 1바이트 Varint 태그로 압축 인코딩.
           - **gRPC 전송 (REST Base64 +33% 패널티 회피)**: REST API의 Base64 강제 인코딩 오버헤드를 없애고 순수 바이너리 전송.
           - **1KB 최소 과금 단위 우회 (BatchSettings)**: 1,000B 미만 미세 메시지 단건 발행 시 발생하는 10배 과금 팽창을 클라이언트 배치로 방어.
           - **Zstd 결합**: 단일 페이로드당 최극소화 바이트 볼륨 달성.
           - **DLQ 서킷 브레이커**: 스키마 오류/포이즌 필 발생 시 5회 재시도 후 Dead Letter Topic(`pubsub-demo-dlq-topic`)으로 안전 격리.
        """
        )
    else:
        st.markdown(
            """
        ### 3 Core Pillars of Anthropic's High-Scale Streaming Event Architecture on GCP

        1. **Dual-Path Ingestion & Zstd Compression**:
           - Payloads < 8MB compressed inline via Pub/Sub (Fast Path).
           - Payloads >= 8MB offloaded to Cloud Storage with lightweight pointer in Pub/Sub.
           - Transparent consumer reconstitution ensures 100% downstream compatibility.
        2. **88% Latency Reduction via gRPC StreamingPull**:
           - Replaces legacy HTTP/gRPC synchronous polling loops with persistent bidirectional gRPC streams.
           - Sub-15ms delivery for real-time model telemetry, training step sync, and agent logs.
        3. **Data Format & Batch Optimization (Binary Schema & BatchSettings) & DLQ Governance**:
           - **Binary Schema (Protobuf)**: Eliminates JSON string field keys using 1-byte Varint tags.
           - **Pure gRPC Wire (Bypassing REST +33% Base64 Penalty)**: Prevents REST Base64 data inflation.
           - **1KB Billing Unit Bypass (BatchSettings)**: Prevents 10x cost traps on <1KB small events via client-side batching.
           - **Zstd Synergy**: Reaches the minimum possible byte volume per payload.
           - **5-Retry DLQ Isolation**: Safely quarantines poison pills into Dead Letter Topic preventing blocking.
        """
        )

    st.markdown(
        """
    ```mermaid
    graph TD
        A[Anthropic Claude Serving / Agents] -->|1. Generate Event| B[DualPathPublisher]
        B -->|Zstandard Compression: ~70% Savings| C{Payload >= 8MB?}
        C -->|No: < 8MB Fast Path| D[Inline Pub/Sub Msg]
        C -->|Yes: >= 8MB Offload Path| E[GCS Offload: gs://pub-sub-kamo-payloads]
        E -->|Pointer Reference| D
        D -->|Pub/Sub Topic| F[projects/pub-sub-kamo/topics/pubsub-demo-events]
        F -->|Persistent gRPC StreamingPull| G[Real-Time Consumer: ~11ms latency]
        F -->|Synchronous Pull Legacy| H[Legacy Batch Worker: ~95ms latency]
        F -->|BigQuery Zero-ETL| I[(BigQuery: pubsub_demo_analytics.streaming_events)]
        G -->|Schema Error / Poison Pill| J[DLQ Manager: 5 Retries]
        J -->|Exhausted| K[Dead Letter Queue Topic: pubsub-demo-dlq-topic]
    ```
    """
    )

# ----------------- TAB 2: DUAL-PATH INGESTION -----------------
with tab2:
    st.subheader(tr("핵심 축 1: 대규모 데이터 수집과 이중 경로(Dual-Path) 패턴 & Zstandard 압축", "Pillar 1: Dual-Path Ingestion & Zstandard Compression"))

    col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 2])
    with col_btn1:
        if st.button(tr("🚀 합성 트래픽 배치 생성", "🚀 Generate Synthetic Batch"), use_container_width=True):
            results = generator.generate_batch(count=batch_size)
            for res in results:
                st.session_state.metrics.record_path(
                    res.path.value, res.uncompressed_bytes, res.compressed_bytes, res.pubsub_wire_bytes
                )
                dual_path_savings = round(
                    ((res.uncompressed_bytes - res.pubsub_wire_bytes) / res.uncompressed_bytes * 100.0)
                    if res.uncompressed_bytes > 0 else 0.0, 1
                )
                st.session_state.event_log.append(
                    {
                        "event_id": res.event_id,
                        "path": res.path.value,
                        "uncompressed_bytes": res.uncompressed_bytes,
                        "compressed_bytes": res.compressed_bytes,
                        "pubsub_wire_bytes": res.pubsub_wire_bytes,
                        "zstd_savings_%": round(res.reduction_percentage, 1),
                        "pubsub_savings_%": dual_path_savings,
                        "payload_uri": res.payload_uri if res.payload_uri else "inline",
                        "fingerprint": res.schema_fingerprint,
                        "timestamp": time.strftime("%H:%M:%S"),
                    }
                )
            st.success(tr(f"이중 경로 발행자를 통해 {len(results)}건의 이벤트를 발행했습니다!", f"Published {len(results)} events via Dual-Path publisher!"))

    with col_btn2:
        if st.button(tr("🚨 대용량 멀티모달 페이로드 1건 주입 (>8MB)", "🚨 Inject 1 Large Multimodal Payload (>8MB)"), use_container_width=True):
            res = generator.generate_single_event(force_large=True)
            st.session_state.metrics.record_path(
                res.path.value, res.uncompressed_bytes, res.compressed_bytes, res.pubsub_wire_bytes
            )
            dual_path_savings = round(
                ((res.uncompressed_bytes - res.pubsub_wire_bytes) / res.uncompressed_bytes * 100.0)
                if res.uncompressed_bytes > 0 else 0.0, 1
            )
            st.session_state.event_log.append(
                {
                    "event_id": res.event_id,
                    "path": res.path.value,
                    "uncompressed_bytes": res.uncompressed_bytes,
                    "compressed_bytes": res.compressed_bytes,
                    "pubsub_wire_bytes": res.pubsub_wire_bytes,
                    "zstd_savings_%": round(res.reduction_percentage, 1),
                    "pubsub_savings_%": dual_path_savings,
                    "payload_uri": res.payload_uri,
                    "fingerprint": res.schema_fingerprint,
                    "timestamp": time.strftime("%H:%M:%S"),
                }
            )
            st.warning(tr(f"대용량 페이로드 {res.event_id}를 Cloud Storage로 즉시 오프로드했습니다!", f"Offloaded large payload {res.event_id} directly to Cloud Storage!"))

    st.markdown(
        f"""
        <div style="background-color: #0f2027; padding: 12px 18px; border-radius: 8px; border-left: 4px solid #00c6ff; margin: 12px 0 16px 0;">
            <span style="font-weight: 600; color: #00c6ff;">💡 {tr("네트워크 대역폭 절감 2대 축 (Why Dual-Path?)", "2 Dimensions of Network Savings (Why Dual-Path?)")}:</span>
            <ul style="margin: 6px 0 0 18px; color: #cfd8dc; font-size: 0.9em;">
                <li><strong>{tr("1. Dual-Path 브로커 트래픽 절감 (98%+)", "1. Dual-Path Broker Wire Savings (98%+)")}</strong>: {tr("8MB 이상의 대용량 텐서/이미지는 Pub/Sub 브로커를 우회하여 GCS에 저장되며, 브로커에는 150B 포인터만 전송되어 브로커 과금 및 네트워크 병목을 99% 이상 제거합니다.", "Large payloads (>=8MB) bypass the Pub/Sub broker and reside in GCS; only a ~150B pointer travels over Pub/Sub, cutting broker bandwidth by >99%.")}</li>
                <li><strong>{tr("2. Zstandard 페이로드 압축 절감 (45~80%)", "2. Zstandard Payload Compression (45~80%)")}</strong>: {tr("텍스트 프롬프트는 ~80%, 멀티모달 임베딩 텐서는 ~50%의 무손실 바이트 압축을 달성하여 스토리지 및 네트워크 전송 비용을 최소화합니다.", "Text prompts achieve ~80% compression while multimodal tensors achieve ~50% lossless compression.")}</li>
            </ul>
        </div>
        """,
        unsafe_allow_html=True,
    )

    counters = st.session_state.metrics.get_path_counters()
    m_col1, m_col2, m_col3, m_col4 = st.columns(4)
    m_col1.metric(
        tr("패스트 패스 이벤트 (< 8MB)", "Fast Path Events (< 8MB)"),
        f"{counters['fast_count']} {tr('건', 'events')}",
        help=tr("Pub/Sub 인라인으로 즉시 고속 전송된 이벤트", "Events transmitted inline via Pub/Sub"),
    )
    m_col2.metric(
        tr("GCS 오프로드 이벤트 (>= 8MB)", "GCS Offload Events (>= 8MB)"),
        f"{counters['offload_count']} {tr('건', 'events')}",
        help=tr("Cloud Storage로 오프로드 후 포인터만 전송된 대용량 이벤트", "Large events offloaded to Cloud Storage with lightweight pointer"),
    )
    m_col3.metric(
        tr("🎯 Pub/Sub 브로커 트래픽 절감", "Pub/Sub Broker Wire Saved"),
        f"{counters['pubsub_wire_bytes_saved'] / 1024:.1f} KB",
        f"-{counters['pubsub_wire_savings_percent']}% {tr('브로커 절감', 'wire saved')}",
        help=tr("이중 경로(Claim-Check) 패턴을 통해 대형 페이로드가 GCS로 빠져나가 Pub/Sub 브로커 네트워크에서 절감된 대역폭 (8MB -> ~150B 포인터)", "Bandwidth saved on Pub/Sub broker via Claim-Check offloading"),
    )
    m_col4.metric(
        tr("📦 Zstd 페이로드 압축 절감", "Zstd Compression Saved"),
        f"{counters['bytes_saved'] / 1024:.1f} KB",
        f"-{counters['overall_savings_percent']}% {tr('압축 절감', 'compressed')}",
        help=tr("Zstandard 압축 알고리즘을 통해 줄어든 순수 페이로드 바이트 크기", "Pure payload bytes saved via Zstandard compression"),
    )

    if st.session_state.event_log:
        st.markdown(f"#### {tr('실시간 수집 이벤트 로그', 'Live Ingestion Event Log')}")
        df_events = pd.DataFrame(st.session_state.event_log[-15:][::-1])
        st.dataframe(df_events, use_container_width=True)

# ----------------- TAB 3: STREAMINGPULL LATENCY BENCHMARK -----------------
with tab3:
    st.subheader(tr("핵심 축 2: 지연 시간 88% 절감 — StreamingPull vs Synchronous Pull", "Pillar 2: Latency Benchmark — StreamingPull vs Synchronous Pull"))

    st.markdown(
        f"""
    <div class="latency-callout">
        <h2 style="margin:0; color:white;">⚡ {tr("P99 꼬리 지연 시간 88% 절감: StreamingPull로의 전환", "88% P99 Tail Latency Reduction: StreamingPull vs Sync Pull")}</h2>
        <p style="margin:5px 0 0 0; color:#E0E0E0;">{tr("Anthropic은 중앙값(P50)에 안주하지 않고 LLM 서빙 SLA를 결정짓는 P99 꼬리 지연 시간을 영구 양방향 gRPC 스트림으로 88% 단축했습니다.", "Anthropic targeted P99 tail latency rather than P50 median to guarantee real-time LLM serving SLAs, slashing P99 delays by 88% via persistent gRPC streams.")}</p>
    </div>
    """,
        unsafe_allow_html=True,
    )

    b_col1, b_col2 = st.columns(2)
    with b_col1:
        st.markdown(f"#### 1. {tr('레거시 동기식 Sync Pull (배치 폴링)', 'Legacy Synchronous Pull (Batch Polling)')}")
        st.write(tr("연결 핸드셰이크, 왕복 폴링 주기, 유휴 대기 오버헤드가 발생합니다.", "Simulates turn-around polling intervals, connection handshakes, and idle wait."))
        if st.button(tr("동기식 Sync Pull 배치 실행 (10건)", "Run Sync Pull Batch (10 msgs)"), use_container_width=True):
            sync_worker = SyncPullWorker(
                client=client,
                project_id=project_id,
                subscription_id="pubsub-demo-sync-sub",
                topic_id=topic_id,
                simulated_poll_delay_ms=140.0,
            )
            generator.generate_batch(count=10)
            pulled = sync_worker.pull_batch(max_messages=10)
            for p in pulled:
                st.session_state.metrics.record_latency("sync_pull", p.latency_ms)
            cur_sync_p99 = st.session_state.metrics.get_stats("sync_pull")["p99"]
            st.info(tr(f"1. Sync Pull로 {len(pulled)}건 수신 완료. P99 지연 시간: ~{cur_sync_p99:.1f}ms", f"Pulled {len(pulled)} messages via 1. Sync Pull. P99 latency: ~{cur_sync_p99:.1f}ms"))

    with b_col2:
        st.markdown(f"#### 2. {tr('gRPC StreamingPull (양방향 푸시)', 'gRPC StreamingPull (Bidirectional Push)')}")
        st.write(tr("영구 연결된 스트림 채널을 유지하여 브로커가 도착 즉시 푸시합니다.", "Simulates persistent open streaming channel with instantaneous broker push."))
        if st.button(tr("gRPC StreamingPull 스트림 실행 (10건)", "Run StreamingPull Batch (10 msgs)"), use_container_width=True):
            received = []
            stream_worker = StreamingPullWorker(
                client=client,
                project_id=project_id,
                subscription_id="pubsub-demo-stream-sub",
                topic_id=topic_id,
                callback=lambda m: received.append(m),
                simulated_stream_delay_ms=18.0,
            )
            generator.generate_batch(count=10)
            stream_worker.start()
            time.sleep(0.15)
            stream_worker.stop()
            for r in received:
                st.session_state.metrics.record_latency("streaming_pull", r.latency_ms)
            cur_stream_p99 = st.session_state.metrics.get_stats("streaming_pull")["p99"]
            st.success(tr(f"2. StreamingPull로 {len(received)}건 수신 완료. P99 지연 시간: ~{cur_stream_p99:.1f}ms (P99 88% 단축)", f"Received {len(received)} messages via 2. StreamingPull. P99 latency: ~{cur_stream_p99:.1f}ms (-88% P99 reduction)"))

    sync_stats = st.session_state.metrics.get_stats("sync_pull")
    stream_stats = st.session_state.metrics.get_stats("streaming_pull")
    comp = st.session_state.metrics.compare("sync_pull", "streaming_pull")

    sync_p99_val = sync_stats["p99"] if sync_stats["count"] > 0 else 140.0
    stream_p99_val = stream_stats["p99"] if stream_stats["count"] > 0 else 18.0
    p99_diff = max(0.0, sync_p99_val - stream_p99_val)
    reduction_pct = ((sync_p99_val - stream_p99_val) / sync_p99_val * 100.0) if sync_p99_val > 0 else 88.0

    st.markdown("---")
    st.markdown(f"#### {tr('실시간 지연 시간 비교 통계 (P99 SLA 기준)', 'Real-Time Latency Comparison Distribution (P99 SLA Basis)')}")

    stat_c1, stat_c2, stat_c3, stat_c4 = st.columns(4)
    stat_c1.metric(
        tr("1. Sync Pull P99 지연 시간", "1. Sync Pull P99 Latency"),
        f"{sync_p99_val:.1f} ms",
        delta=tr("P99 레거시 기준", "P99 Baseline"),
        delta_color="inverse",
        help=tr("동기식 폴링 시 대기 주기 및 연결 지연이 겹친 P99 꼬리 지연 시간 (레거시 기준)", "P99 tail latency accumulating idle wait intervals and connection setups (baseline)"),
    )
    stat_c2.metric(
        tr("2. StreamingPull P99 지연 시간", "2. StreamingPull P99 Latency"),
        f"{stream_p99_val:.1f} ms",
        delta=f"-{reduction_pct:.1f}% (P99)",
        delta_color="normal",
        help=tr("Anthropic 기준 핵심 척도: 상위 1% 최악 조건에서도 보장되는 초저지연 시간 (최적화)", "Core metric: Worst-case 99th percentile latency guaranteed under live streaming (optimized)"),
    )
    stat_c3.metric(
        tr("P99 지연 시간 절감률", "P99 Latency Reduction"),
        f"{reduction_pct:.1f}%",
        delta=f"-{reduction_pct:.1f}% (P99)",
        delta_color="inverse",
        help=tr("P50 중앙값이 아닌 실제 서비스 SLA를 좌우하는 P99 꼬리 지연 시간 기반 절감률", "Reduction based on P99 tail latency governing real-world AI serving SLAs"),
    )
    stat_c4.metric(
        tr("P99 꼬리 지연 단축 폭", "P99 Tail Latency Saved"),
        f"{p99_diff:.1f} ms",
        delta=tr("P99 SLA 대기 단축", "P99 SLA delay saved"),
        help=tr("gRPC 스트리밍 전환으로 제거된 1회당 P99 지연 시간", "Latency eliminated per message at P99 via persistent gRPC streaming"),
    )

    chart_data = pd.DataFrame(
        {
            "Metric": ["P99 (SLA 기준)", "P95", "P90", "P50"],
            tr("1. Sync Pull (동기식 ms)", "1. Sync Pull (ms)"): [
                sync_stats["p99"] or 140.0,
                sync_stats["p95"] or 125.0,
                sync_stats["p90"] or 110.0,
                sync_stats["p50"] or 95.0,
            ],
            tr("2. StreamingPull (스트리밍 ms)", "2. StreamingPull (ms)"): [
                stream_stats["p99"] or 18.0,
                stream_stats["p95"] or 15.0,
                stream_stats["p90"] or 13.5,
                stream_stats["p50"] or 11.0,
            ],
        }
    ).set_index("Metric")
    st.bar_chart(chart_data)

    # ------------------ ARCHITECTURE DEEP DIVE: SYNC PULL VS STREAMINGPULL ------------------
    st.markdown("---")
    st.markdown(
        f"""
        ### 🔄 {tr("Sync Pull vs StreamingPull 아키텍처 비교 & 워크플로우", "Sync Pull vs StreamingPull Architecture & Workflow")}
        <p style="color: #90a4ae; font-size: 0.95em; margin-top: -8px;">
            📖 <strong>{tr("공식 가이드", "Official Guide")}</strong>: <a href="https://docs.cloud.google.com/pubsub/docs/pull" target="_blank" style="color: #00c6ff; text-decoration: underline;">Google Cloud Pub/Sub Pull Message Flow Documentation</a><br>
            {tr(
                "Google Cloud Pub/Sub의 메시지 수급 체계는 <strong>네트워크 커넥션 수립 방식과 주도권의 위치</strong>에 따라 명확한 기술적 차이를 보입니다.",
                "Pub/Sub message reception differs fundamentally based on <strong>network connection establishment and delegation of initiative</strong>."
            )}
        </p>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        f"""
        #### 🗺️ {tr("메시지 수급 워크플로우 비교 (Visual Workflow Comparison)", "Message Consumption Visual Workflow Comparison")}
        """
    )

    wf_col1, wf_col2 = st.columns(2)
    with wf_col1:
        st.markdown(
            f"""
            <div class="workflow-card-sync">
                <div style="font-size: 1.15em; font-weight: 800; color: #ff5252; margin-bottom: 14px; border-bottom: 1px solid rgba(239,83,80,0.3); padding-bottom: 8px;">
                    🔴 1. Sync Pull ({tr("단방향 동기식 폴링", "Unary Synchronous Polling")})
                    <span style="float: right; background: rgba(239,83,80,0.25); color: #ff8a80; padding: 2px 8px; border-radius: 4px; font-size: 0.8em; font-weight: 700;">
                        ⏱️ {tr("P99 지연: ~140ms", "P99 Latency: ~140ms")}
                    </span>
                </div>
                <div style="margin-bottom: 8px; color: #ffffff; font-size: 0.93em; line-height: 1.5;">
                    <span class="step-pill-sync">STEP 1</span> <strong>{tr("PullRequest 전송", "Send PullRequest")}</strong>: {tr("클라이언트가 브로커로 단일 RPC 폴링 요청 발송", "Client issues single RPC polling request to broker")}
                </div>
                <div class="flow-arrow-sync">⬇️ {tr("매 요청마다 TCP/TLS 핸드셰이크 & 브로커 큐 조회 대기", "TCP/TLS handshake & broker queue wait on every RPC")}</div>
                <div style="margin-bottom: 8px; color: #ffffff; font-size: 0.93em; line-height: 1.5;">
                    <span class="step-pill-sync">STEP 2</span> <strong>{tr("PullResponse 수신", "Receive PullResponse")}</strong>: {tr("동기식으로 큐의 메시지 배치 수신", "Synchronous message batch received from queue")}
                </div>
                <div class="flow-arrow-sync">⬇️ {tr("클라이언트 처리 완료 후 개별 Ack 전송", "Client processes message then transmits Ack")}</div>
                <div style="margin-bottom: 8px; color: #ffffff; font-size: 0.93em; line-height: 1.5;">
                    <span class="step-pill-sync">STEP 3</span> <strong>{tr("Acknowledge 회신", "Return Acknowledge")}</strong>: {tr("처리된 ack_ids를 별도 RPC로 전송", "Processed ack_ids sent back via separate RPC")}
                </div>
                <div class="flow-arrow-sync">🔄 {tr("유휴 대기(Polling Interval) 후 STEP 1부터 재요청 무한 반복...", "Idle wait interval then repeat from STEP 1 endlessly...")}</div>
                <div style="background: rgba(239,83,80,0.15); border-left: 3px solid #ef5350; padding: 8px 12px; border-radius: 4px; margin-top: 10px; font-size: 0.85em; color: #ffcdd2;">
                    ⚠️ <strong>{tr("지연 시간 누적", "Latency Accumulation")}</strong>: {tr("메시지가 큐에 없어도 지속적으로 폴링해야 하며, 연결 핸드셰이크와 대기 주기가 더해져 P99 ~140ms 지연 발생.", "Continuous polling even when empty; connection handshakes and sleep intervals add ~140ms P99 latency.")}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with wf_col2:
        st.markdown(
            f"""
            <div class="workflow-card-stream">
                <div style="font-size: 1.15em; font-weight: 800; color: #00e5ff; margin-bottom: 14px; border-bottom: 1px solid rgba(0,229,255,0.3); padding-bottom: 8px;">
                    🔵 2. StreamingPull ({tr("영구 양방향 gRPC 스트리밍", "Persistent Bidirectional gRPC")})
                    <span style="float: right; background: rgba(0,229,255,0.25); color: #00e5ff; padding: 2px 8px; border-radius: 4px; font-size: 0.8em; font-weight: 700;">
                        ⚡ {tr("P99 지연: ~18ms (88% 절감)", "P99 Latency: ~18ms (-88%)")}
                    </span>
                </div>
                <div style="margin-bottom: 8px; color: #ffffff; font-size: 0.93em; line-height: 1.5;">
                    <span class="step-pill-stream">STEP 1</span> <strong>{tr("양방향 스트림 1회 수립", "Open Stream Once")}</strong>: {tr("HTTP/2 기반 영구 gRPC 채널 항시 유지", "Single persistent bidirectional HTTP/2 gRPC channel maintained")}
                </div>
                <div class="flow-arrow-stream">⚡ {tr("유휴 대기 0초! 브로커가 이벤트 도착 즉시 실시간 푸시", "Zero idle wait! Broker pushes in real-time on message arrival")}</div>
                <div style="margin-bottom: 8px; color: #ffffff; font-size: 0.93em; line-height: 1.5;">
                    <span class="step-pill-stream">STEP 2</span> <strong>{tr("StreamingPullResponse 푸시", "Instant Stream Push")}</strong>: {tr("클라이언트 요청 대기 없이 실시간 Push-like 인입", "Real-time push without waiting for client requests")}
                </div>
                <div class="flow-arrow-stream">⚡ {tr("백그라운드 스레드/Goroutine이 콜백 즉각 실행", "Background worker/goroutine executes callback immediately")}</div>
                <div style="margin-bottom: 8px; color: #ffffff; font-size: 0.93em; line-height: 1.5;">
                    <span class="step-pill-stream">STEP 3</span> <strong>{tr("비동기 Ack 스트림 회신", "Async Streamed Ack")}</strong>: {tr("채널을 닫지 않고 스트림으로 실시간 Ack/유량제어 전달", "Async Ack & flow control streamed without closing connection")}
                </div>
                <div class="flow-arrow-stream">🚀 {tr("스트림이 상시 열려 있어 24시간 무중단 초저지연 수급 지속", "Stream kept alive for 24/7 uninterrupted sub-15ms delivery")}</div>
                <div style="background: rgba(0,229,255,0.15); border-left: 3px solid #00e5ff; padding: 8px 12px; border-radius: 4px; margin-top: 10px; font-size: 0.85em; color: #b2ebf2;">
                    🎯 <strong>{tr("초저지연 달성", "Ultra-Low Latency")}</strong>: {tr("폴링 왕복 시간과 핸드셰이크가 제거되어 모델 텔레메트리와 에이전트 로그를 P99 ~18ms 만에 실시간 수신.", "Eliminates polling round-trips and handshakes, streaming model telemetry in ~18ms P99 real-time.")}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with st.expander(f"📐 {tr('아키텍처 토폴로지 다이어그램 (Mermaid Architecture Flowchart)', 'Architecture Topology Diagram (Mermaid)')}", expanded=True):
        st.markdown(
            """
```mermaid
flowchart LR
    subgraph SYNC ["🔴 1. Sync Pull (단방향 동기식 폴링: P99 ~140ms)"]
        direction TB
        S1["1. Client: PullRequest 호출<br/>(단일 RPC 요청)"]
        S2["2. TCP/TLS 핸드셰이크 & 브로커 대기<br/>(P99 지연 누적: ~140ms)"]
        S3["3. Pub/Sub Broker: PullResponse 반환<br/>(동기식 메시지 전달)"]
        S4["4. Client: 메시지 처리 & Ack 회신"]
        S5["5. 유휴 대기(Polling Sleep) 후 다음 주기 반복..."]
        S1 --> S2 --> S3 --> S4 --> S5 -.->|다음 주기 재요청| S1
    end

    subgraph STREAM ["🔵 2. StreamingPull (영구 양방향 gRPC: P99 ~18ms)"]
        direction TB
        ST1["1. Client: 양방향 gRPC 스트림 1회 연결<br/>(HTTP/2 채널 항시 유지)"]
        ST2["2. Pub/Sub Broker: 실시간 이벤트 인입"]
        ST3["3. 브로커가 도착 즉시 실시간 푸시!<br/>(Push-like Streaming: P99 ~18ms)"]
        ST4["4. Client: 비동기 Ack 스트림 회신<br/>(채널 끊김 없이 무중단 수급)"]
        ST1 <===> ST2
        ST2 ==>|대기 시간 0초 실시간 푸시| ST3
        ST3 -.->|비동기 Ack / Flow Control| ST4
        ST4 -.->|스트림 항시 유지| ST1
    end
```
            """
        )

    st.markdown(f"#### 1. {tr('아키텍처 설계상의 근본적 차이', 'Fundamental Architectural Differences')}")
    arch_col1, arch_col2 = st.columns(2)
    with arch_col1:
        st.markdown(
            f"""
            <div style="background-color: #1a1e24; border: 1px solid #ef5350; border-radius: 8px; padding: 14px 16px; margin-bottom: 12px;">
                <span style="font-weight: 700; color: #ef5350; font-size: 1.05em;">1. Sync Pull ({tr("단방향 동기식 폴링", "Unary Synchronous Polling")})</span>
                <ul style="color: #cfd8dc; font-size: 0.9em; margin: 8px 0 0 16px; line-height: 1.6;">
                    <li><strong>{tr("메커니즘", "Mechanism")}</strong>: {tr("클라이언트가 브로커(서버)를 향해 주기적으로 데이터 유무를 확인하는 <strong>단일 RPC 요청-응답 구조</strong>입니다. 전통적인 폴링(Polling) 방식을 따릅니다.", "Single RPC request-response pattern where the client periodically checks the broker for data availability (classic polling).")}</li>
                    <li><strong>{tr("레이턴시 패널티", "Latency Penalty")}</strong>: {tr("메시지가 존재하지 않을 때도 요청을 반복해야 하며, 다음 주기까지 발생하는 대기 시간과 매번 핸드셰이크를 수행하는 네트워크 오버헤드로 인해 <strong>P99 지연 시간이 누적(~140ms)</strong>됩니다.", "Accumulates P99 latency (~140ms) from idle wait intervals between cycles and repeated connection handshake overheads even when no messages exist.")}</li>
                    <li><strong>{tr("제어 최적화", "Control Optimization")}</strong>: {tr("수신 측의 가용 리소스 상황에 맞춰 메시지 인입량을 엄격히 제한할 수 있어, <strong>클라이언트 사이드의 부하 관리가 매우 용이</strong>합니다.", "Strictly limits incoming volume based on client-side available resources, making overload prevention straightforward.")}</li>
                </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with arch_col2:
        st.markdown(
            f"""
            <div style="background-color: #1a1e24; border: 1px solid #00c6ff; border-radius: 8px; padding: 14px 16px; margin-bottom: 12px;">
                <span style="font-weight: 700; color: #00c6ff; font-size: 1.05em;">2. StreamingPull ({tr("영구 양방향 gRPC 스트리밍", "Persistent Bidirectional gRPC Streaming")})</span>
                <ul style="color: #cfd8dc; font-size: 0.9em; margin: 8px 0 0 16px; line-height: 1.6;">
                    <li><strong>{tr("메커니즘", "Mechanism")}</strong>: {tr("HTTP/2 기반의 gRPC를 활용하여 클라이언트와 서버 사이에 <strong>항시 유지되는 양방향 채널</strong>을 구축합니다. 브로커는 데이터 인입 즉시 클라이언트 요청 없이도 스트림을 통해 실시간으로 밀어냅니다(Push-like).", "Maintains a persistent bidirectional HTTP/2 gRPC channel. The broker pushes messages in real-time as soon as they arrive without awaiting requests.")}</li>
                    <li><strong>{tr("퍼포먼스 우위", "Performance Advantage")}</strong>: {tr("폴링에 따르는 유휴 대기 시간이 전무하므로 <strong>극도의 처리량과 P99 초저지연(~18ms)을 보장</strong>합니다. (Anthropic 사례에서 <strong>지연 시간을 88%까지 절감</strong>한 기술적 핵심입니다.)", "Zero idle polling latency ensures ultra-low P99 delivery times (~18ms) and high throughput (the core architectural key to Anthropic's 88% latency slash).")}</li>
                    <li><strong>{tr("리소스 오버헤드", "Resource Overhead")}</strong>: {tr("스트림 유지 및 비동기 처리를 위한 <strong>백그라운드 프로세싱(스레드/Goroutine)이 필수적</strong>이며, 메모리와 네트워크 대역폭이 상시 점유되는 특징이 있습니다.", "Requires persistent background processing threads/goroutines to manage streams and leases, keeping memory and connections permanently engaged.")}</li>
                </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown(f"#### 2. {tr('StreamingPull이 만능 해결책인가? (전략적 Sync Pull 선택 가이드)', 'Is StreamingPull a Silver Bullet? (Strategic Use Cases for Sync Pull)')}")
    st.markdown(
        f"""
        <div style="background-color: #121820; border-left: 4px solid #ffb300; padding: 12px 18px; border-radius: 6px; margin-bottom: 16px;">
            <p style="color: #eceff1; font-size: 0.93em; margin: 0; line-height: 1.6;">
                {tr(
                    "실시간 스트리밍 관점에서는 <strong>StreamingPull이 압도적인 성능</strong>을 보이지만, 모든 인프라 환경과 비즈니스 로직에서 항상 정답인 것은 아닙니다.<br>"
                    "Google Cloud 공식 가이드에서도 범용적인 비동기 처리에는 StreamingPull을 권장하나, <strong>지연 시간 최적화보다 인프라 효율성이나 단순한 제어가 우선되는 특수 환경</strong>에서는 <strong>Sync Pull이 전략적으로 선택</strong>됩니다.",
                    "While StreamingPull delivers dominant performance for real-time streaming, it is not a silver bullet for every architectural workload.<br>"
                    "Google Cloud officially recommends StreamingPull for general async processing, but <strong>Sync Pull remains strategically superior in specialized environments</strong> where infrastructure cost efficiency or simple execution control outweighs sub-second latency."
                )}
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    uc1, uc2, uc3 = st.columns(3)
    with uc1:
        st.markdown(
            f"""
            <div style="background-color: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 14px 16px; height: 100%;">
                <span style="font-weight: 700; color: #ffd54f; font-size: 0.98em;">📦 {tr("배치 처리 및 서버리스 (Scale to 0)", "Batch & Serverless (Scale to 0)")}</span>
                <p style="color: #b0bec5; font-size: 0.85em; margin: 8px 0 0 0; line-height: 1.5;">
                    {tr(
                        "특정 시점에만 활성화되어(Scale to 0) 정해진 분량의 데이터를 소화하고 종료되는 <strong>Cloud Functions나 배치형 워크로드</strong>에는 고정 스트림 유지가 비효율적입니다. 필요한 시점에만 단건/배치로 연결하는 Sync Pull이 리소스 비용 면에서 훨씬 유리합니다.",
                        "For ephemeral workloads (Cloud Functions, Cloud Run Jobs) that scale to 0 and terminate after processing a fixed batch, keeping persistent streams alive is wasteful. On-demand polling minimizes runtime costs."
                    )}
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with uc2:
        st.markdown(
            f"""
            <div style="background-color: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 14px 16px; height: 100%;">
                <span style="font-weight: 700; color: #ffd54f; font-size: 0.98em;">⚙️ {tr("하드웨어 리소스 제약 환경", "Hardware Resource Constraints")}</span>
                <p style="color: #b0bec5; font-size: 0.85em; margin: 8px 0 0 0; line-height: 1.5;">
                    {tr(
                        "지속적인 CPU 사이클과 메모리 점유가 부담스러운 <strong>극도로 제한된 컴퓨팅 환경(Edge 기기, 경량 컨테이너)</strong>에서는, 명시적인 요청 시에만 작동하는 동기식 모델이 시스템 안정성을 확보하기에 적합합니다.",
                        "In constrained edge devices or lightweight micro-containers where background CPU threads and memory allocations cause strain, synchronous demand-driven polling guarantees predictability and stability."
                    )}
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with uc3:
        st.markdown(
            f"""
            <div style="background-color: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 14px 16px; height: 100%;">
                <span style="font-weight: 700; color: #ffd54f; font-size: 0.98em;">🎯 {tr("고정밀 유량 제어 (Strict Flow Control)", "Strict Flow Control & Isolation")}</span>
                <p style="color: #b0bec5; font-size: 0.85em; margin: 8px 0 0 0; line-height: 1.5;">
                    {tr(
                        "메시지 한 건당 수 분의 연산 시간이 소요되어 클라이언트 버퍼링 없이 <strong>순차적으로 엄밀하게 처리해야 하는 복잡한 로직</strong>의 경우, 클라이언트가 직접 인입 주도권을 갖는 Sync Pull이 구조적 단순성을 제공합니다.",
                        "When each message takes minutes of intense computation and client buffering must be avoided at all costs, Sync Pull provides deterministic step-by-step pull control without complex buffer leases."
                    )}
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )

# ----------------- TAB 4: BINARY SCHEMA OPTIMIZATION & DLQ -----------------
with tab4:
    st.subheader(tr("핵심 축 3: 데이터 전송 포맷 최적화 (바이너리 스키마) & DLQ 거버넌스", "Pillar 3: Binary Schema Optimization & DLQ Governance"))

    st.markdown(
        f"""
        <div class="doc-banner">
            <strong>💡 {tr("데이터 전송 포맷 & 프로토콜 최적화 핵심 원리", "Core Principles of Data Format & Protocol Optimization")}:</strong><br>
            • <strong>{tr("JSON 포맷의 한계", "Limitation of JSON")}</strong>: {tr("필드명('event_id', 'timestamp_ms' 등)이 모든 메시지마다 문자열로 중복 포함되어 네트워크 대역폭을 낭비하고 압축 효율을 저해합니다.", "Field names are redundantly repeated in every message as strings, inflating payloads.")}<br>
            • <strong>{tr("바이너리 스키마(Protobuf) 적용", "Binary Schema (Protobuf)")}</strong>: {tr("필드명을 1바이트 Varint 태그 번호로 치환하고 타입 기반 바이너리 인코딩을 적용하여 직렬화 크기를 대폭 축소합니다.", "Replaces field names with 1-byte Varint tags and compact type encoding.")}<br>
            • <strong>⚠️ {tr("전송 프로토콜 선택 (REST vs gRPC) - Base64 33% 패널티", "Protocol Choice (REST vs gRPC) - 33% Base64 Penalty")}</strong>: 
              {tr("HTTP REST API로 바이너리/JSON 데이터를 전송하면 Pub/Sub REST 본문 규격에 맞춰 <strong>Base64로 강제 인코딩되면서 용량이 대략 33% 추가 팽창</strong>하는 심각한 패널티가 발생합니다. 반면 <strong>gRPC(HTTP/2)</strong>는 순수 raw 바이너리를 직접 전송하여 Base64 패널티가 0%입니다.", "Sending binary via HTTP REST API forces <strong>Base64 encoding (+33% size penalty)</strong>. In contrast, <strong>gRPC (HTTP/2)</strong> transmits pure raw binary with 0% Base64 penalty.")}<br>
            • <strong>{tr("클라이언트 압축(Zstd)과의 결합", "Combination with Client Zstd")}</strong>: {tr("Protobuf + gRPC + Zstandard를 결합하면 단일 페이로드당 최적화할 수 있는 가장 압축된 바이트 볼륨(최대 80% 이상 절감)을 달성할 수 있습니다.", "Combining Protobuf + gRPC + Zstd achieves the most compact byte volume possible (up to 80%+ savings).")}
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        f"""
        ### 🔍 {tr("데이터 크기 (Size) 관점의 차이: JSON vs Protobuf", "Data Size Perspective: JSON vs Protobuf")}

        {tr("가장 큰 차이는 <strong>데이터 필드명의 반복 여부</strong>와 <strong>인코딩 방식</strong>에서 발생합니다.", "The primary difference arises from <strong>redundant field name repetition</strong> versus <strong>binary encoding methods</strong>.")}
        """
    )

    c_json, c_proto = st.columns(2)
    with c_json:
        st.markdown(
            f"""
            <div style="background-color: #1a1e24; border: 1px solid #e53935; border-radius: 8px; padding: 14px 16px; margin-bottom: 10px;">
                <span style="font-weight: 700; color: #ff5252; font-size: 1.05em;">📝 JSON ({tr("텍스트 포맷", "Text Format")})</span>
                <p style="color: #cfd8dc; font-size: 0.9em; margin: 6px 0 0 0; line-height: 1.5;">
                    {tr(
                        '텍스트 기반이라 가독성은 뛰어나지만, <code>"event_id"</code>, <code>"timestamp_ms"</code>, <code>"pod_env_vars"</code>와 같은 <strong>키(Key) 문자열이 모든 단일 메시지마다 반복적으로 포함</strong>되어야 합니다. 이로 인해 불필요한 데이터가 누적되어 전체 페이로드 크기가 커집니다.',
                        'Highly readable text format, but string keys like <code>"event_id"</code>, <code>"timestamp_ms"</code>, <code>"pod_env_vars"</code> must be <strong>redundantly repeated in every single message</strong>. This accumulates excessive metadata overhead.'
                    )}
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        sample_json_code = '''{
  "event_id": "evt-prod-9a2c3f81",
  "source": "claude-serving-pod-01",
  "timestamp_ms": 1725062400000,
  "payload_type": "application/json",
  "payload": "{\\"prompt\\": \\"Explain quantum computing...\\", \\"tokens\\": 256}",
  "schema_fingerprint": "a8f5c381d9b4f620e1a3c749b567d12f345890ab",
  "pod_env_vars": {
    "KUBERNETES_NODE": "gke-node-pool-1-c89a",
    "CLUSTER_REGION": "us-central1",
    "MODEL_VERSION": "claude-3-7-sonnet"
  }
}'''
        st.code(sample_json_code, language="json")
        st.caption(tr(
            "⚠️ 필드명 문자열 오버헤드: 총 393바이트 중 필드 키만 ~137바이트(약 35%)를 차지하여 매 전송마다 중복 낭비됨.",
            "⚠️ String Key Overhead: Out of 393 bytes, field keys alone consume ~137 bytes (35%), repeated wastefully on every publish."
        ))

    with c_proto:
        st.markdown(
            f"""
            <div style="background-color: #1a1e24; border: 1px solid #00b0ff; border-radius: 8px; padding: 14px 16px; margin-bottom: 10px;">
                <span style="font-weight: 700; color: #40c4ff; font-size: 1.05em;">⚡ Protobuf ({tr("바이너리 스키마", "Binary Schema")})</span>
                <p style="color: #cfd8dc; font-size: 0.9em; margin: 6px 0 0 0; line-height: 1.5;">
                    {tr(
                        '텍스트 필드명 대신 <strong>1바이트 크기의 숫자인 Varint 태그 번호</strong>를 사용하여 필드를 식별합니다. 이에 더해 데이터를 <strong>컴팩트한 바이너리로 인코딩</strong>하므로 구조적으로 크기가 훨씬 작습니다.',
                        'Identifies fields using <strong>1-byte numeric Varint tags</strong> instead of text keys. Encodes all values into <strong>compact binary structures</strong>, drastically minimizing wire size.'
                    )}
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        sample_proto_code = '''// 1. 스키마 정의 (proto/streaming_event.proto)
syntax = "proto3";
message StreamingEvent {
  string event_id           = 1; // 태그 1 -> 1바이트 0x0A로 식별!
  string source             = 2; // 태그 2 -> 1바이트 0x12로 식별!
  bytes  payload            = 3; // 태그 3 -> 1바이트 0x1A로 식별!
  string payload_type       = 4; // 태그 4 -> 1바이트 0x22로 식별!
  int64  timestamp_ms       = 5; // 태그 5 -> 1바이트 0x28 + Varint 정수 압축!
  map<string, string> pod_env_vars = 6; // 태그 6 -> 1바이트 0x32
  string schema_fingerprint = 8; // 태그 8 -> 1바이트 0x42
}

// 2. 실제 네트워크 전송 와이어 (Wire Binary Hex Dump: ~276 바이트)
// 0A 11 65 76 74 2D 70 72 6F 64 2D 39 61 32 63 33 66 38 31
// 12 15 63 6C 61 75 64 65 2D 73 65 72 76 69 6E 67 2D 70 6F...'''
        st.code(sample_proto_code, language="protobuf")
        st.caption(tr(
            "✅ 필드 식별 오버헤드 14B ➔ 1B 축소: 텍스트 키('timestamp_ms') 문자열은 0B(완전 제거)! 대신 1바이트 태그(0x28) + Varint 정수 인코딩",
            "✅ Field Identification Overhead 14B ➔ 1B: String field name is 0B (eliminated)! Replaced by 1-byte tag (0x28) + Varint encoding"
        ))

    st.markdown("---")
    st.markdown(f"### 📊 1. {tr('전송 포맷 및 프로토콜 실시간 벤치마크 계산기', 'Real-Time Format & Protocol Benchmark Calculator')}")

    f_col1, f_col2 = st.columns([1, 2])
    with f_col1:
        st.markdown(f"#### {tr('시뮬레이션 페이로드 설정', 'Simulated Payload Config')}")
        preset = st.selectbox(
            tr("샘플 페이로드 프리셋", "Sample Payload Preset"),
            [
                tr("LLM 서빙 프롬프트/응답 (텍스트)", "LLM Serving Prompt/Response (Text)"),
                tr("멀티턴 에이전트 실행 컨텍스트 (긴 텍스트)", "Multi-turn Agent Context (Long Text)"),
                tr("고밀도 텐서 임베딩 메타데이터 (JSON)", "High-Density Tensor Metadata (JSON)"),
            ],
        )
        if "LLM" in preset:
            sample_text = (
                "Explain the internal architecture of Google Cloud Pub/Sub and StreamingPull RPCs. "
                "Anthropic optimizes real-time LLM inference telemetry with Protocol Buffers and Zstandard compression. "
                "Dual-Path pattern offloads payloads exceeding 8MB to Cloud Storage while streaming fast path inline."
            )
        elif "멀티턴" in preset or "Multi-turn" in preset:
            sample_text = (
                "User: Compare Google Cloud Pub/Sub with Kafka for high-throughput AI agent telemetry.\n"
                "Assistant: Google Cloud Pub/Sub offers fully managed auto-scaling without cluster rebalancing, "
                "native gRPC StreamingPull with sub-10ms delivery, and BigQuery Zero-ETL direct subscription ingestion. "
            ) * 4
        else:
            sample_text = (
                '{"layer": 32, "head_dim": 128, "embedding_dim": 4096, "quantization": "fp8", '
                '"tensor_name": "model.layers.31.self_attn.o_proj.weight", "checksum": "e9b4c09d"}'
            ) * 6

        custom_prompt = st.text_area(
            tr("페이로드 텍스트 (직접 편집 가능)", "Payload Text (Editable)"),
            value=sample_text,
            height=130,
        )

        fmt_bench = DataFormatBenchmark(compression_level=3)
        bench_result = fmt_bench.benchmark_event(
            event_id="evt-demo-bench",
            source="claude-serving-pod-01",
            prompt_text=custom_prompt,
        )

    with f_col2:
        st.markdown(f"#### {tr('포맷 & 프로토콜별 실측 전송 크기 비교', 'Format & Protocol Wire Size Comparison')}")
        results_list = bench_result["results"]
        p_json = results_list[0]
        r_json = results_list[1]
        r_proto = results_list[2]
        g_proto = results_list[3]
        g_zstd = results_list[4]

        mc1, mc2, mc3, mc4 = st.columns(4)
        mc1.metric(
            tr("1. Plain JSON", "1. Plain JSON"),
            f"{p_json.wire_bytes:,} B",
            help="기본 텍스트 JSON (필드명 중복 전송)",
        )
        mc2.metric(
            tr("2. JSON (REST Base64)", "2. JSON (REST Base64)"),
            f"{r_json.wire_bytes:,} B",
            delta=f"+{r_json.base64_overhead_bytes} B ({tr('패널티', 'Penalty')})",
            delta_color="inverse",
            help="REST API 규격상 Base64 인코딩 강제로 약 33% 팽창",
        )
        mc3.metric(
            tr("4. Protobuf (gRPC)", "4. Protobuf (gRPC)"),
            f"{g_proto.wire_bytes:,} B",
            delta=f"-{g_proto.reduction_vs_json_pct:.1f}%",
            help="필드명 제거 + Varint 압축 + gRPC 순수 바이너리 전송",
        )
        mc4.metric(
            tr("5. Protobuf+Zstd (gRPC)", "5. Protobuf+Zstd (gRPC)"),
            f"{g_zstd.wire_bytes:,} B",
            delta=f"-{g_zstd.reduction_vs_json_pct:.1f}%",
            help="Anthropic 프로덕션 패턴: 바이너리 스키마 + Zstd + gRPC",
        )

        chart_df = pd.DataFrame(
            {
                tr("전송 방식", "Wire Format"): [
                    tr("1. Plain JSON", "1. Plain JSON"),
                    tr("2. JSON (REST Base64)", "2. JSON (REST Base64)"),
                    tr("3. Protobuf (REST Base64)", "3. Protobuf (REST Base64)"),
                    tr("4. Protobuf (gRPC)", "4. Protobuf (gRPC)"),
                    tr("5. Protobuf+Zstd (gRPC)", "5. Protobuf+Zstd (gRPC)"),
                ],
                tr("실제 전송 바이트 (Wire Bytes)", "Wire Bytes"): [
                    p_json.wire_bytes,
                    r_json.wire_bytes,
                    r_proto.wire_bytes,
                    g_proto.wire_bytes,
                    g_zstd.wire_bytes,
                ],
            }
        ).set_index(tr("전송 방식", "Wire Format"))
        st.bar_chart(chart_df)

    st.markdown(f"#### 📋 {tr('5대 전송 방식 종합 비교표 및 월간 트래픽 절감 추정치', 'Comprehensive Comparison & Monthly Traffic Savings')}")
    table_rows = []
    for r in results_list:
        table_rows.append(
            {
                tr("전송 포맷 및 프로토콜", "Format & Protocol"): r.format_name,
                tr("프로토콜", "Protocol"): r.protocol,
                tr("전송 크기 (Bytes)", "Wire Bytes"): f"{r.wire_bytes:,} B",
                tr("Base64 패널티", "Base64 Penalty"): f"+{r.base64_overhead_bytes} B" if r.base64_overhead_bytes > 0 else "0 B (순수 바이너리)",
                tr("JSON 대비 절감률", "Savings vs JSON"): f"{r.reduction_vs_json_pct:+.1f}%",
                tr("아키텍처 특징", "Architecture Note"): r.description,
            }
        )
    st.dataframe(pd.DataFrame(table_rows), use_container_width=True)

    sav = bench_result["savings_summary"]
    st.info(
        f"💰 **{tr('초대규모 트래픽 비용 절감 추정치 (월 10억 건 발행 기준)', 'Large-Scale Cost Savings Estimate (1 Billion Events/Month)')}**:\n"
        f"- {tr('기존 Plain JSON 월간 트래픽', 'Baseline Plain JSON Monthly Volume')}: **{sav['baseline_tb_per_1b']} TB**\n"
        f"- {tr('Protobuf + Zstd (gRPC) 월간 트래픽', 'Protobuf + Zstd (gRPC) Monthly Volume')}: **{sav['optimized_tb_per_1b']} TB**\n"
        f"- 🎯 **{tr('순수 절감 네트워크 대역폭 및 비용', 'Net Bandwidth & Egress Cost Saved')}**: **{sav['saved_tb_per_1b']} TB 절감 ({sav['overall_reduction_pct']}% 절감)**!"
    )

    st.markdown("---")
    st.markdown(f"### 💰 2. {tr('1KB 최소 과금 단위(Billing Unit) 우회: 클라이언트 측 배치(Batching) 시뮬레이터', 'Bypassing 1KB Minimum Billing Unit: Client-Side Batching Simulator')}")

    st.markdown(
        f"""
        <div class="doc-banner" style="border-left-color: #F57F17;">
            <strong>⚠️ {tr("Google Cloud Pub/Sub 1KB 최소 과금 규칙 및 10배 비용 낭비 위험", "Google Cloud Pub/Sub 1KB Minimum Billing Unit & 10x Cost Trap")}:</strong><br>
            • <strong>{tr("1KB 최소 과금 규칙", "1KB Minimum Rule")}</strong>: {tr("Pub/Sub은 개별 메시지 크기가 1,000바이트(1KB) 미만이더라도 무조건 최소 1,000바이트로 올림하여 과금합니다.", "Pub/Sub rounds up message volume to a minimum of 1,000 bytes (1KB) per message.")}<br>
            • <strong>{tr("비배치 시 최대 10배 비용 발생", "Up to 10x Cost Penalty Without Batching")}</strong>: {tr("예를 들어 100바이트짜리 메시지 10개를 각각 단일 요청으로 보내면 실제 데이터는 1KB이지만 과금은 10KB(10,000바이트)로 처리되어 <strong>무려 10배(1,000%)의 요금 폭탄</strong>이 발생합니다.", "Sending 10 messages of 100 bytes individually transfers 1KB but bills 10KB (10,000 bytes) — an unnecessary 10x (1,000%) cost penalty!")}<br>
            • <strong>{tr("최적화 방안 (BatchSettings)", "Optimization via BatchSettings")}</strong>: {tr("클라이언트 라이브러리의 <code>BatchSettings(max_messages, max_bytes, max_latency)</code>를 튜닝하여 여러 메시지를 하나의 Publish 요청으로 묶어서 전송하면 총 데이터 크기가 합산(10개 * 100B = 1,000B)되어 <strong>과금 단위도 1KB로 축소</strong>됩니다. 허용 지연 시간(예: 50ms) 내에서 튜닝하면 지연 영향 없이 최대 90% 비용을 절감합니다.", "Configuring <code>BatchSettings</code> bundles small messages into a single PublishRequest, aggregating byte volume (10 * 100B = 1KB) so you are billed for only 1KB. Tuning within latency budget (e.g. 50ms) slashes up to 90% cost with zero noticeable delay.")}
        </div>
        """,
        unsafe_allow_html=True,
    )

    bc1, bc2 = st.columns([1, 2])
    with bc1:
        st.markdown(f"#### {tr('배치 및 메시지 튜닝 파라미터', 'Batch & Message Parameters')}")
        b_msg_size = st.slider(
            tr("개별 메시지 크기 (바이트)", "Individual Message Size (Bytes)"),
            min_value=50,
            max_value=1500,
            value=100,
            step=50,
            help="1,000바이트 미만의 미세 메시지일수록 배치 효과가 극대화됩니다.",
        )
        b_msg_count = st.select_slider(
            tr("월간 메시지 발행 건수", "Monthly Message Volume"),
            options=[1_000_000, 5_000_000, 10_000_000, 50_000_000, 100_000_000, 1_000_000_000],
            value=10_000_000,
            format_func=lambda x: f"{x:,} 건 ({x//1_000_000}M)" if x < 1_000_000_000 else "10억 건 (1B)",
        )
        b_batch_size = st.slider(
            tr("배치 묶음 메시지 수 (max_messages)", "Batch Size (max_messages)"),
            min_value=1,
            max_value=100,
            value=10,
            step=1,
            help="1이면 비배치(단일 전송), 10 이상이면 1KB 과금 단위 최적화 달성",
        )
        b_latency_ms = st.selectbox(
            tr("허용 레이턴시 버퍼 (max_latency)", "Allowed Latency Buffer (max_latency)"),
            options=[10, 20, 50, 100],
            index=2,
            format_func=lambda x: f"{x} ms (지연 허용치)",
        )

        billing_calc = BatchBillingOptimizer.calculate_billing(
            message_size_bytes=b_msg_size,
            message_count=b_msg_count,
            batch_size=b_batch_size,
        )

    with bc2:
        st.markdown(f"#### {tr('과금 팽창 배수 및 절감액 실시간 분석', 'Cost Inflation & Savings Analysis')}")
        bm1, bm2, bm3, bm4 = st.columns(4)
        bm1.metric(
            tr("실제 데이터 크기", "Actual Data Volume"),
            f"{billing_calc.actual_data_bytes / (1024**2):.1f} MB" if billing_calc.actual_data_bytes < 1024**3 else f"{billing_calc.actual_data_bytes / (1024**3):.2f} GB",
            help="실제 네트워크로 전송되는 순수 페이로드 바이트",
        )
        bm2.metric(
            tr("비배치 과금 크기", "Unbatched Billed Volume"),
            f"{billing_calc.unbatched_billed_bytes / (1024**2):.1f} MB" if billing_calc.unbatched_billed_bytes < 1024**3 else f"{billing_calc.unbatched_billed_bytes / (1024**3):.2f} GB",
            delta=f"{billing_calc.cost_inflation_ratio}x {tr('비용 팽창', 'Inflation')}",
            delta_color="inverse",
            help="개별 메시지마다 최소 1,000바이트로 올림되어 청구되는 볼륨",
        )
        bm3.metric(
            tr("배치 적용 과금 크기", "Batched Billed Volume"),
            f"{billing_calc.batched_billed_bytes / (1024**2):.1f} MB" if billing_calc.batched_billed_bytes < 1024**3 else f"{billing_calc.batched_billed_bytes / (1024**3):.2f} GB",
            delta=f"-{billing_calc.savings_percentage:.1f}%",
            help="배치로 묶여 전송되어 1KB 단위 올림이 배치 레벨로 완화된 과금 볼륨",
        )
        bm4.metric(
            tr("비배치 비용 배수", "Inflation Multiplier"),
            f"{billing_calc.cost_inflation_ratio:.1f}x",
            delta=f"-${billing_calc.unbatched_cost_usd - billing_calc.batched_cost_usd:.2f} USD" if billing_calc.unbatched_cost_usd > billing_calc.batched_cost_usd else "0 USD",
            delta_color="normal",
            help="비배치 대비 배치 적용 시의 비용 절감 효과",
        )

        batch_chart_df = pd.DataFrame(
            {
                tr("과금 구분", "Category"): [
                    tr("1. 실제 데이터 크기", "1. Actual Data Volume"),
                    tr("2. 비배치 과금 볼륨 (1KB 올림 낭비)", "2. Unbatched Billed (1KB Penalty)"),
                    tr("3. 배치 적용 과금 볼륨 (BatchSettings)", "3. Batched Billed (BatchSettings)"),
                ],
                tr("과금 바이트 (MB)", "Billed Volume (MB)"): [
                    round(billing_calc.actual_data_bytes / (1024**2), 2),
                    round(billing_calc.unbatched_billed_bytes / (1024**2), 2),
                    round(billing_calc.batched_billed_bytes / (1024**2), 2),
                ],
            }
        ).set_index(tr("과금 구분", "Category"))
        st.bar_chart(batch_chart_df)

    st.markdown(f"#### 💻 {tr('프로덕션 권장 BatchSettings 코드 구성', 'Recommended Production BatchSettings Code')}")
    snippet_code = BatchBillingOptimizer.get_batch_settings_code_snippet(
        max_messages=b_batch_size,
        max_bytes_mb=1,
        max_latency_ms=b_latency_ms,
    )
    st.code(snippet_code, language="python")

    st.markdown("---")
    st.markdown(f"### 🛡️ 3. {tr('Proto-First 거버넌스 & Dead Letter Queue (DLQ) 격리', 'Proto-First Governance & Dead Letter Queue (DLQ) Quarantine')}")

    d_col1, d_col2 = st.columns([1, 2])
    with d_col1:
        st.markdown(f"#### {tr('의도적 에러 / 포이즌 필 주입', 'Intentional Error Injection')}")
        st.write(
            tr(
                "손상된 스키마나 파싱 불가능한 페이로드를 인위적으로 주입하여, 5회 재시도 후 Dead Letter Queue로 격리되는 과정을 시연합니다.",
                "Inject a poisoned event (corrupted schema or unparseable payload) to observe the 5-retry circuit breaker and quarantine into DLQ.",
            )
        )
        if st.button(tr("🚨 포이즌 필 / 스키마 손상 이벤트 주입", "🚨 Inject Poison Pill / Malformed Event"), use_container_width=True):
            res = generator.generate_single_event(force_corrupt=True)
            raw_msg = client.get_published_messages(topic_id)[-1]
            status = {}
            for _ in range(5):
                status = dlq_manager.process_with_dlq(raw_msg, consumer.consume_message)

            st.session_state.dlq_log.append(
                {
                    "event_id": res.event_id,
                    "attempts": status["attempts"],
                    "status": status["status"],
                    "quarantine_reason": status.get("error", "Schema validation fault"),
                    "timestamp": time.strftime("%H:%M:%S"),
                }
            )
            st.error(tr(f"이벤트 {res.event_id}가 5회 전송 실패 후 Dead Letter Queue로 안전하게 격리되었습니다!", f"Event {res.event_id} failed 5 delivery attempts and was moved to DLQ!"))

    with d_col2:
        st.markdown(f"#### {tr('Dead Letter Queue (DLQ) 격리 모니터링 테이블', 'Dead Letter Queue (DLQ) Quarantine Table')}")
        if st.session_state.dlq_log:
            df_dlq = pd.DataFrame(st.session_state.dlq_log[::-1])
            st.dataframe(df_dlq, use_container_width=True)
        else:
            st.info(tr("DLQ에 격리된 메시지가 없습니다. 모든 수집 이벤트가 정상 처리 중입니다.", "No messages in DLQ. All ingested messages healthy."))

# ----------------- TAB 5: LIVE GCP VERIFICATION -----------------
with tab5:
    st.subheader(tr("실제 Google Cloud 프로젝트 검증 (pub-sub-kamo)", "Live Google Cloud Project Verification (pub-sub-kamo)"))
    st.markdown(
        f"**{tr('대상 프로젝트', 'Target Project')}**: `{project_id}` | "
        f"**{tr('현재 모드', 'Active Mode')}**: `{gcp_mode.value.upper()}`"
    )

    v_col1, v_col2 = st.columns(2)
    with v_col1:
        st.markdown(f"#### 1. {tr('사전 점검 & IAM 서비스 계정 권한', 'Pre-flight & IAM Service Account')}")
        st.info(
            """
        **Pub/Sub Service Agent**: `service-PROJECT_NUMBER@gcp-sa-pubsub.iam.gserviceaccount.com`
        - **DLQ Topic**: `roles/pubsub.publisher`
        - **Main Subscription**: `roles/pubsub.subscriber`
        - **BigQuery Dataset**: `roles/bigquery.dataEditor`
        """
        )
        if st.button(tr("⚡ 1-Click 엔드투엔드 전체 아키텍처 자동 검증", "⚡ Run 1-Click End-to-End Live Verification"), use_container_width=True):
            with st.spinner(tr("5대 아키텍처 항목을 순차 검증 중...", "Verifying 5 architecture components...")):
                from scripts.verify_gcp_live import verify_live_deployment

                is_dry = gcp_mode == GCPMode.MOCK
                res = verify_live_deployment(project_id=project_id, dry_run=is_dry)
                if res:
                    st.success(
                        tr(
                            "🎉 5대 핵심 아키텍처 항목 검증이 모두 성공적으로 완료되었습니다!",
                            "🎉 All 5 core architecture verification checks passed successfully!",
                        )
                    )

    with v_col2:
        st.markdown(f"#### 2. {tr('BigQuery Zero-ETL 스트리밍 실시간 조회', 'BigQuery Zero-ETL Live Ingestion Query')}")
        st.write(
            tr(
                "Pub/Sub이 Dataflow 없이 BigQuery 테이블로 직접 적재한 데이터를 확인합니다.",
                "Inspect events streamed directly into BigQuery without Dataflow.",
            )
        )
        if st.button(tr("🔍 BigQuery 테이블 쿼리 (최근 10건)", "🔍 Query BigQuery Table (Latest 10)"), use_container_width=True):
            if gcp_mode == GCPMode.LIVE:
                try:
                    from google.cloud import bigquery

                    bq = bigquery.Client(project=project_id)
                    df_bq = bq.query(
                        f"SELECT subscription_name, message_id, publish_time, attributes "
                        f"FROM `{project_id}.pubsub_demo_analytics.streaming_events` "
                        f"ORDER BY publish_time DESC LIMIT 10"
                    ).to_dataframe()
                    st.dataframe(df_bq, use_container_width=True)
                except Exception as e:  # noqa: BLE001
                    st.error(f"BigQuery Query Error: {e}")
            else:
                mock_bq_data = pd.DataFrame(
                    [
                        {
                            "subscription_name": "projects/pub-sub-kamo/subscriptions/pubsub-demo-bq-sub",
                            "message_id": f"msg-mock-{i:03d}",
                            "publish_time": time.strftime("%Y-%m-%d %H:%M:%S"),
                            "attributes": '{"event_type": "text_prompt", "content-encoding": "zstd"}',
                        }
                        for i in range(1, 6)
                    ]
                )
                st.dataframe(mock_bq_data, use_container_width=True)
                st.caption(tr("• 모의 샌드박스 환경의 시뮬레이션 BigQuery 데이터입니다.", "• Simulated BigQuery records in Mock Sandbox."))

st.markdown("---")
st.markdown(
    f"<div style='text-align: center; color: gray; font-size: 0.9em;'>"
    f"⚡ Google Cloud Pub/Sub: Anthropic Architecture Demo | "
    f"📄 <a href='https://content-cdn.sessionboard.com/content/IdZQpQJIQVmsSBjedvUW_BRK1-041.pdf' target='_blank'>"
    f"{tr('원본 발표 자료 (PDF)', 'Original Presentation (PDF)')}</a>"
    f"</div>",
    unsafe_allow_html=True,
)
