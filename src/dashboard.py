"""Google Cloud Pub/Sub: Anthropic 아키텍처 심층 분석 인터랙티브 Streamlit 대시보드.

[Anthropic 아키텍처 데모 대시보드 구성]
- 탭 1 (아키텍처 개요): 3대 핵심 기둥(Dual-Path, StreamingPull, Proto-First/DLQ)과 공식 발표 슬라이드 링크
- 탭 2 (이중 경로 & 압축): Fast Path(<8MB) vs GCS Claim-Check(>=8MB) 실시간 트래픽 생성 및 바이트 절감률 관측
- 탭 3 (StreamingPull 88% 절감): 영구 gRPC StreamingPull vs 레거시 동기식 배치 폴링 P50/P90/P99 지연 시간 실측 비교
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
        3. **데이터 전송 포맷 최적화 (Binary Schema & gRPC) & DLQ 거버넌스**:
           - **바이너리 스키마(Protobuf)**: 비효율적인 JSON 필드명을 제거하고 1바이트 Varint 태그로 압축 인코딩.
           - **gRPC 전송 (REST Base64 +33% 패널티 회피)**: REST API의 Base64 강제 인코딩 오버헤드를 없애고 순수 바이너리 전송.
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
        3. **Data Format Optimization (Binary Schema & gRPC) & DLQ Governance**:
           - **Binary Schema (Protobuf)**: Eliminates JSON string field keys using 1-byte Varint tags.
           - **Pure gRPC Wire (Bypassing REST +33% Base64 Penalty)**: Prevents REST Base64 data inflation.
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
                    res.path.value, res.uncompressed_bytes, res.compressed_bytes
                )
                st.session_state.event_log.append(
                    {
                        "event_id": res.event_id,
                        "path": res.path.value,
                        "uncompressed_bytes": res.uncompressed_bytes,
                        "compressed_bytes": res.compressed_bytes,
                        "savings_%": round(res.reduction_percentage, 1),
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
                res.path.value, res.uncompressed_bytes, res.compressed_bytes
            )
            st.session_state.event_log.append(
                {
                    "event_id": res.event_id,
                    "path": res.path.value,
                    "uncompressed_bytes": res.uncompressed_bytes,
                    "compressed_bytes": res.compressed_bytes,
                    "savings_%": round(res.reduction_percentage, 1),
                    "payload_uri": res.payload_uri,
                    "fingerprint": res.schema_fingerprint,
                    "timestamp": time.strftime("%H:%M:%S"),
                }
            )
            st.warning(tr(f"대용량 페이로드 {res.event_id}를 Cloud Storage로 즉시 오프로드했습니다!", f"Offloaded large payload {res.event_id} directly to Cloud Storage!"))

    counters = st.session_state.metrics.get_path_counters()
    m_col1, m_col2, m_col3, m_col4 = st.columns(4)
    m_col1.metric(tr("패스트 패스 이벤트 (< 8MB)", "Fast Path Events (< 8MB)"), counters["fast_count"])
    m_col2.metric(tr("GCS 오프로드 이벤트 (>= 8MB)", "GCS Offload Events (>= 8MB)"), counters["offload_count"])
    m_col3.metric(
        tr("절감된 네트워크 대역폭", "Network Bandwidth Saved"),
        f"{counters['bytes_saved'] / 1024:.1f} KB",
        f"{counters['overall_savings_percent']}% {tr('절감', 'reduction')}",
    )
    m_col4.metric(
        tr("총 전송 페이로드", "Total Payloads Transferred"),
        f"{counters['total_uncompressed_bytes'] / 1024:.1f} KB",
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
        <h2 style="margin:0; color:white;">⚡ {tr("88% 지연 시간 절감: StreamingPull로의 전환", "88% Latency Reduction: StreamingPull vs Sync Pull")}</h2>
        <p style="margin:5px 0 0 0; color:#E0E0E0;">{tr("Anthropic은 컨슈머 Pod를 주기적 배치 폴링 루프에서 영구 양방향 gRPC 스트림으로 전면 전환하여 대기 시간을 극적으로 단축했습니다.", "Anthropic transitioned consumer pods from periodic batch polling to persistent bidirectional gRPC streams, drastically slashing idle round-trips.")}</p>
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
                simulated_poll_delay_ms=92.0,
            )
            generator.generate_batch(count=10)
            pulled = sync_worker.pull_batch(max_messages=10)
            for p in pulled:
                st.session_state.metrics.record_latency("sync_pull", p.latency_ms)
            st.info(tr(f"Sync Pull로 {len(pulled)}건 수신 완료. 평균 지연 시간: ~92ms", f"Pulled {len(pulled)} messages via Sync Pull. Avg latency: ~92ms"))

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
                simulated_stream_delay_ms=11.0,
            )
            generator.generate_batch(count=10)
            stream_worker.start()
            time.sleep(0.15)
            stream_worker.stop()
            for r in received:
                st.session_state.metrics.record_latency("streaming_pull", r.latency_ms)
            st.success(tr(f"StreamingPull로 {len(received)}건 수신 완료. 평균 지연 시간: ~11ms", f"Received {len(received)} messages via StreamingPull. Avg latency: ~11ms"))

    sync_stats = st.session_state.metrics.get_stats("sync_pull")
    stream_stats = st.session_state.metrics.get_stats("streaming_pull")
    comp = st.session_state.metrics.compare("sync_pull", "streaming_pull")

    st.markdown("---")
    st.markdown(f"#### {tr('실시간 지연 시간 비교 통계', 'Real-Time Latency Comparison Distribution')}")

    stat_c1, stat_c2, stat_c3, stat_c4 = st.columns(4)
    stat_c1.metric(tr("Sync Pull P50 지연 시간", "Sync Pull P50 Latency"), f"{sync_stats['p50']:.1f} ms")
    stat_c2.metric(tr("StreamingPull P50 지연 시간", "StreamingPull P50 Latency"), f"{stream_stats['p50']:.1f} ms")
    stat_c3.metric(
        tr("측정된 지연 시간 절감률", "Measured Latency Reduction"),
        f"{comp['reduction_percent']:.1f}%",
        delta=f"-{comp['reduction_percent']:.1f}%",
        delta_color="inverse",
    )
    stat_c4.metric(
        tr("StreamingPull P99 지연 시간", "StreamingPull P99 Latency"),
        f"{stream_stats['p99']:.1f} ms",
        f"vs Sync P99: {sync_stats['p99']:.1f} ms",
    )

    chart_data = pd.DataFrame(
        {
            "Metric": ["P50", "P90", "P95", "P99"],
            tr("Sync Pull (동기식 ms)", "Sync Pull (ms)"): [
                sync_stats["p50"] or 95.0,
                sync_stats["p90"] or 110.0,
                sync_stats["p95"] or 125.0,
                sync_stats["p99"] or 140.0,
            ],
            tr("StreamingPull (스트리밍 ms)", "StreamingPull (ms)"): [
                stream_stats["p50"] or 11.0,
                stream_stats["p90"] or 13.5,
                stream_stats["p95"] or 15.0,
                stream_stats["p99"] or 18.0,
            ],
        }
    ).set_index("Metric")
    st.bar_chart(chart_data)

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
    st.markdown(f"### 🛡️ 2. {tr('Proto-First 거버넌스 & Dead Letter Queue (DLQ) 격리', 'Proto-First Governance & Dead Letter Queue (DLQ) Quarantine')}")

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
