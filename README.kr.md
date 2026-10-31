# langgraph-artifact-memory

[English](README.md) · [만들게 된 배경과 성능 실험](docs/blog/introducing-langgraph-artifact-memory-ko.md)

LangGraph 체크포인트에는 작은 `ArtifactRef`만 남기고, 문서·Tool 결과 같은 무거운 값은
별도 저장소에서 관리하는 라이브러리입니다.

설정한 state 채널은 최초 체크포인트가 기록되기 전에 외부화하고, 실제 값이 필요한
노드에서만 제한적으로 복원합니다. 오래된 메시지는 삭제하지 않고 별도 archive로 옮기면서
체크포인트에는 정해진 개수만 유지할 수도 있습니다.

## 왜 필요한가

대화와 워크플로우가 길어지면 문서 목록, Tool 결과, 메시지가 state에 계속 누적됩니다.
LangGraph는 이 state를 체크포인트마다 읽고 직렬화하고 저장하므로, 데이터가 커질수록 다음
비용이 함께 증가할 수 있습니다.

- 체크포인트 크기와 Redis 메모리 사용량
- 체크포인트 읽기·쓰기 지연
- reducer의 리스트 병합 및 복사 비용
- 네트워크 전송량과 Pod 메모리 사용량

```text
무거운 값 -> ArtifactStore -> 그래프 state에는 ArtifactRef
ArtifactRef -> 제한적 hydrate -> 필요한 노드 안에서만 실제 값
```

## 측정된 개선 결과

아래 결과는 동일한 Redis, `durability="sync"`, 동시 사용자 10명, 사용자당 100턴으로
실행한 로컬 end-to-end 벤치마크입니다. 각 turn마다 4 KB artifact 5개와 1 KB 사용자
메시지를 추가했고, answer 노드는 artifact 5개를 실제로 복원하여 digest를 계산한 뒤
1 KB AI 메시지를 생성했습니다.

| 지표 | 기존 inline state | `ArtifactRef` + 메시지 제한 | 개선 |
|---|---:|---:|---:|
| 전체 1,000턴 실행시간 | 164.2초 | 49.2초 | 70.1% 감소 |
| 처리량 | 6.09 turn/s | 20.33 turn/s | 3.34배 |
| 평균 turn 지연 | 1,634 ms | 490 ms | 70.0% 감소 |
| turn p95 | 3,211 ms | 722 ms | 77.5% 감소 |
| 사용자별 마지막 10턴 평균 | 3,205 ms | 698 ms | 78.2% 감소 |
| 최대 체크포인트 크기 | 2,346 KB | 112 KB | 95.2% 감소 |
| artifact 채널 크기 | 2,104 KB | 85 KB | 96.0% 감소 |
| messages 채널 크기 | 242 KB | 25 KB | 89.5% 감소 |
| 체크포인트 `aget_tuple` 평균 | 926 ms | 89 ms | 90.4% 감소 |
| 체크포인트 `aput` 평균 | 118 ms | 40 ms | 65.7% 감소 |
| Redis peak 메모리 | 507 MB | 382 MB | 24.6% 감소 |
| 워크로드 실행 중 Redis 메모리 증가분 | 404 MB | 279 MB | 30.9% 감소 |

이 수치는 로컬 측정 결과이며 운영 환경의 성능을 보장하지 않습니다. 개선된 방식의 Redis
peak에는 별도로 저장된 artifact 본문 5,000개와 archive 메시지 1,800개도 포함되어
있습니다. 즉 외부 저장 비용을 제외하고 계산한 결과가 아닙니다. 실제 결과는 payload 크기,
그래프 구조, checkpointer, durability, 네트워크 지연, Redis 설정, hydrate 정책에 따라
달라집니다. 또한 이 비교는 artifact 외부화와 bounded message window를 함께 적용한 권장
구성을 측정한 것이므로, 모든 개선분을 artifact 외부화 하나의 효과로 해석해서는 안 됩니다.

### 어떤 경우에 도움이 되는가

| 워크로드 특성 | 기존 inline state | 이 라이브러리 적용 후 |
|---|---|---|
| 문서나 Tool 결과가 리스트에 계속 누적됨 | 커지는 본문을 체크포인트마다 반복 직렬화 | 체크포인트에는 작은 참조만 저장하고 본문은 한 번 저장 |
| 대화가 오랫동안 이어짐 | 메시지 state와 체크포인트 읽기 비용이 계속 증가 | 최근 메시지만 state에 유지하고 오래된 메시지는 조회 가능한 archive로 이동 |
| 여러 worker 또는 Pod가 같은 thread를 처리함 | 큰 체크포인트가 Redis와 네트워크 부하를 증폭 | Redis 기반 참조를 공유하고 노드별 복원량은 제한 |
| 무거운 값이 특정 노드에서만 필요함 | 모든 노드가 무거운 체크포인트 state를 전달받음 | 해당 노드 wrapper에서 설정한 범위만 복원 |
| reducer가 큰 리스트를 반복 병합함 | 원본 본문의 복사·병합·직렬화 비용이 함께 증가 | 원본 대신 작은 `ArtifactRef` 딕셔너리를 병합 |

### 효과가 작거나 주의가 필요한 경우

| 상황 | 고려할 점 |
|---|---|
| state가 작은 짧은 대화 | artifact put/get 비용이 체크포인트 절감 효과보다 클 수 있음 |
| 모든 노드가 모든 artifact를 사용 | 반복적인 전체 hydrate로 병목이 artifact store로 이동할 수 있음 |
| 수백 MB를 한 번에 hydrate | Pod 메모리는 다시 커질 수 있으므로 `last_n`, `max_bytes` 또는 별도 검색 필요 |
| artifact와 체크포인트 보존 기간을 따로 관리 | 삭제된 artifact를 체크포인트가 참조하거나 미사용 artifact가 계속 남을 수 있음 |
| 여러 Pod에서 InMemory/Local store 사용 | 참조를 공유할 수 없으므로 Redis 같은 공용 backend 필요 |

## 설치

InMemory와 로컬 파일 저장소만 사용하는 기본 패키지:

```bash
pip install langgraph-artifact-memory
```

공용 Redis backend도 사용하는 경우:

```bash
pip install "langgraph-artifact-memory[redis]"
```

## 저장소 선택

| Backend | 영속성 | 여러 프로세스/Pod 공유 | 추가 의존성 | 권장 용도 |
|---|---:|---:|---:|---|
| `InMemoryArtifactStore` | 없음 | 불가능 | 없음 | 단위 테스트, 짧은 로컬 실행 |
| `LocalArtifactStore` | 있음 | 불가능 | 없음 | 로컬 개발, 단일 서버 |
| `RedisArtifactStore` | 있음 | 가능 | `redis` | 여러 worker 또는 Pod를 사용하는 서비스 |

`LocalArtifactStore`는 payload를 content-addressed 파일로 저장하고 archive 순서는 SQLite
WAL 데이터베이스로 관리합니다. 컨테이너가 교체된 후에도 데이터를 유지해야 한다면 저장
경로에 persistent volume을 연결해야 합니다.

## 기본 사용법

```python
import operator
from typing import Annotated, Any

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from langgraph_artifact_memory import (
    ArtifactIOPolicy,
    ArtifactMemory,
    ArtifactRef,
    Externalize,
    Hydrate,
    InMemoryArtifactStore,
)


class State(TypedDict, total=False):
    artifacts: Annotated[list[dict[str, Any] | ArtifactRef], operator.add]
    answer: str


memory = ArtifactMemory(
    InMemoryArtifactStore(),
    externalize={"artifacts": Externalize(itemized=True)},
    io_policy=ArtifactIOPolicy(max_concurrency=16),
    state_schema=State,
)


def answer_node(state: State) -> dict[str, str]:
    # wrapper가 실행되기 전에 선택한 ArtifactRef가 실제 값으로 복원됩니다.
    artifacts = state["artifacts"]
    return {"answer": f"retrieved={len(artifacts)}"}


graph = (
    StateGraph(State)
    .add_node(
        "answer",
        memory.wrap(
            answer_node,
            hydrate={"artifacts": Hydrate(last_n=5, max_bytes=1_000_000)},
        ),
    )
    .add_edge(START, "answer")
    .add_edge("answer", END)
    .compile(checkpointer=InMemorySaver())
)

config = {"configurable": {"thread_id": "conversation-123"}}
result = await memory.ainvoke(
    graph,
    {"artifacts": new_artifacts},
    config,
)
```

외부화 대상 채널이 입력에 포함될 때는 `graph.ainvoke()` 대신 `memory.ainvoke()`를
사용해야 합니다. 그래프의 최초 체크포인트가 기록되기 전에 무거운 입력값을 참조로
교체하기 위해서입니다.

## State schema 검증

`StateGraph`에 전달한 것과 같은 state schema를 지정하면 그래프 실행 전에 설정 실수를
발견할 수 있습니다.

```python
memory = ArtifactMemory(
    store,
    externalize={"artifacts": Externalize(itemized=True)},
    state_schema=State,
    validation="strict",
)
```

strict 모드는 다음 항목을 확인합니다.

- `externalize`, `hydrate`, `message_window`의 key가 state schema에 존재하는지
- `Externalize(itemized=True)` 대상이 `list[...]`이며 원소 타입에 `ArtifactRef`를 담을 수 있는지
- `Externalize(itemized=False)` 대상 타입에 `ArtifactRef`를 담을 수 있는지
- message window 대상이 리스트 채널인지
- 입력값과 wrapped node 반환값의 itemized 채널이 실제 리스트인지

`"artfacts"` 같은 오타는 `ArtifactConfigurationError`와 함께
`Did you mean 'artifacts'?` 제안을 출력합니다. 노드가 잘못된 runtime 형태를 반환하면
artifact store나 체크포인트에 기록되기 전에 `ArtifactStateTypeError`가 발생합니다.

기존 코드에 점진적으로 적용하려면 `validation="warn"`을 사용하여
`ArtifactValidationWarning`만 발생시킬 수 있습니다. 검증을 완전히 끄려면
`validation="off"`를 사용합니다. 정적 key와 annotation 검증에는 `state_schema`가
필요하지만, strict 모드의 runtime itemized 검증은 schema 없이도 동작합니다.

## 실행 가능한 예제

모든 예제에는 assertion이 포함되어 있고 테스트에서 독립 스크립트로 실행됩니다.

| 예제 | 확인할 수 있는 내용 |
|---|---|
| `examples/basic.py` | 입력 외부화와 제한적 hydrate 기본 흐름 |
| `examples/document_pipeline.py` | 문서 수집, 외부화, 분석, 답변 pipeline |
| `examples/parallel_tool_workflow.py` | 병렬 `Send` worker와 외부화된 Tool 결과 |
| `examples/conditional_hydration.py` | quick/deep route별로 다른 hydrate 범위 |
| `examples/multi_thread_isolation.py` | thread ID별 체크포인트와 archive 분리 |
| `examples/multiple_channels.py` | documents와 tool-results 채널의 개별 정책 |
| `examples/message_keep.py` | Human, AI, System, Tool 메시지 keep/archive |
| `examples/long_conversation.py` | 100턴 대화에서 recent/kept 메시지 수 제한 |
| `examples/local_persistence.py` | 프로세스 재시작을 가정한 Local store 재연결 |
| `examples/resilient_io.py` | 일시적 장애, 재시도, resilience metric |
| `examples/redis_shared.py` | 두 worker가 Redis artifact를 공유하는 방식 |
| `examples/state_validation.py` | schema key 오타와 잘못된 입력/반환값 조기 발견 |

저장소 루트에서 다음과 같이 실행할 수 있습니다.

```bash
python examples/basic.py
python examples/document_pipeline.py
python examples/parallel_tool_workflow.py
python examples/conditional_hydration.py
python examples/multi_thread_isolation.py
python examples/message_keep.py
python examples/long_conversation.py
python examples/local_persistence.py
python examples/multiple_channels.py
python examples/resilient_io.py
python examples/state_validation.py
```

Redis 예제에는 optional dependency와 Redis URL이 필요합니다.

```bash
pip install -e ".[redis]"
REDIS_URL=redis://localhost:6379/0 python examples/redis_shared.py
```

PowerShell에서는 먼저 `$env:REDIS_URL = "redis://localhost:6379/0"`를 설정한 뒤 같은
Python 명령을 실행합니다.

## 제한된 비동기 I/O와 장애 대응

artifact 리스트는 원래 순서를 유지하면서 병렬 저장·복원합니다. 하나의
`ArtifactMemory` 인스턴스가 수행하는 모든 store I/O는 공용 semaphore로 제한되므로,
여러 graph thread가 동시에 실행되어도 무제한 task가 만들어지지 않습니다.

```python
from langgraph_artifact_memory import ArtifactIOPolicy, ArtifactMemory

memory = ArtifactMemory(
    store,
    externalize={"artifacts": Externalize(itemized=True)},
    io_policy=ArtifactIOPolicy(
        max_concurrency=16,
        get_timeout=2.0,
        put_timeout=5.0,
        archive_timeout=3.0,
        acquire_timeout=1.0,
        max_retries=2,
        initial_backoff=0.1,
        max_backoff=2.0,
        jitter=0.1,
    ),
)
```

- timeout과 일시적 연결 오류는 제한된 exponential-backoff retry를 사용합니다.
- 누락된 artifact, 직렬화 오류, validation 오류는 재시도하지 않습니다.
- 제공되는 content-addressed store의 `put`, `get`, archive read는 안전하게 재시도합니다.
- archive append는 timeout을 적용하지만, 결과가 불명확한 쓰기를 재시도하면 중복될 수 있어 자동 재시도하지 않습니다.
- `acquire_timeout=None`이면 I/O 여유가 생길 때까지 기다립니다. 양수를 지정하면 무제한 대기 대신 `ArtifactCapacityError`가 발생합니다.

최종 timeout은 `ArtifactTimeoutError`, 재시도 후에도 남은 일시적 장애는
`ArtifactUnavailableError`를 발생시킵니다. 오류는 노드 밖으로 전달되므로 LangGraph의
노드 단위 retry/failure 정책과 함께 사용할 수 있습니다. LangGraph `RetryPolicy`도
설정한다면 store retry와 node retry가 곱해지므로 두 횟수를 모두 작게 유지하는 것이
좋습니다.

## Backend별 사용법

### InMemory

```python
from langgraph_artifact_memory import InMemoryArtifactStore

store = InMemoryArtifactStore()
```

단위 테스트와 짧은 프로세스에 적합하며 프로세스 종료 시 값이 사라집니다.

### 로컬 파일

```python
from langgraph_artifact_memory import LocalArtifactStore

store = LocalArtifactStore("./.artifacts")
```

로컬 개발이나 단일 서버에 적합합니다. 영속성은 있지만 여러 서버가 공유하는 분산
저장소는 아닙니다. Kubernetes에서 데이터 보존이 필요하면 persistent volume을
연결해야 합니다.

### Redis

```python
from langgraph_artifact_memory.redis import RedisArtifactStore

store = RedisArtifactStore(
    "redis://localhost:6379/0",
    prefix="my-artifacts",
)

# 애플리케이션 종료 시
await store.aclose()
```

여러 worker나 Pod가 같은 artifact를 읽어야 하는 서비스에 적합합니다.

## 메시지 window와 archive

```python
from langgraph_artifact_memory import MessageWindow, mark_message_kept

memory = ArtifactMemory(
    store,
    externalize={"artifacts": Externalize(itemized=True)},
    message_window=MessageWindow(
        max_messages=20,
        keep_message_types=("system",),
        keep_metadata_key="keep",
        max_kept_messages=50,
    ),
)

old_messages = await memory.aget_archive("conversation-123", limit=20)
```

wrapped node가 `messages`를 반환하면 최신 메시지만 LangGraph의 `Overwrite`를 이용해
graph state에 남기고 초과 메시지는 `ArtifactStore` archive로 이동합니다.

개별 메시지는 일반 LangChain metadata로 제한적 keep 대상에 지정할 수 있습니다.

```python
from langchain_core.messages import HumanMessage

preference = HumanMessage(
    content="항상 한국어로 답변해 주세요.",
    additional_kwargs={"keep": True},
)

kept_preference = mark_message_kept(preference)
ordinary_preference = mark_message_kept(kept_preference, kept=False)
```

`keep_message_types`를 사용하면 system 같은 특정 타입의 메시지를 같은 방식으로
유지합니다. `max_messages`는 일반 최신 메시지 수, `max_kept_messages`는 keep 메시지 수를
각각 제한하므로 state 최대 메시지 수는 두 값의 합입니다. 두 그룹 모두 시간순을
유지하며 제한을 넘은 메시지는 삭제하지 않고 archive로 이동합니다.

keep 대상은 작은 지침, 사용자 선호, 중요한 결정에 사용하는 것이 좋습니다. 큰 Tool
payload를 keep하면 체크포인트가 다시 커집니다. 큰 본문은 artifact 채널로 외부화하고
메시지에는 참조나 요약만 남기는 방식을 권장합니다.

## Metric

```python
print(memory.metrics.snapshot())
```

snapshot에는 put/get/archive 횟수, 논리적 byte 크기, 프로세스 내부 실행시간이 포함됩니다.
retry, failure, timeout, capacity rejection, queue wait, 최대 in-flight I/O도 확인할 수
있습니다. 실행시간은 모든 샘플을 메모리에 쌓지 않고 누적 통계로 계산합니다.

## 호환성

| 구성요소 | 0.4.x 지원 범위 |
|---|---|
| Python | CPython 3.10-3.13 |
| LangGraph | `>=1.2,<2` |
| 실행 방식 | Async |
| 노드 반환 타입 | Mapping / partial state update |
| wrapped node signature | `node(state)` 또는 `node(state, config)` |
| Artifact 값 | LangGraph `JsonPlusSerializer`가 지원하는 값 |
| Checkpointer | 특정 구현에 종속되지 않으며 InMemory와 Redis에서 테스트 |

message window 교체에 `langgraph.types.Overwrite`를 사용하므로 LangGraph 1.2 이상이
필요합니다.

## Wheel 빌드와 검증

```bash
python -m build
python -m twine check dist/*
python -m pip install --force-reinstall dist/langgraph_artifact_memory-0.4.0-py3-none-any.whl
python -c "from langgraph_artifact_memory import ArtifactMemory, InMemoryArtifactStore"
```

## Backend benchmark

```bash
python benchmarks/backend_benchmark.py \
  --operations 1000 \
  --payload-bytes 4096 \
  --concurrency 20
```

Redis도 포함하려면 URL을 전달합니다.

```bash
python benchmarks/backend_benchmark.py \
  --redis-url redis://localhost:6379/0
```

backend microbenchmark는 직렬화와 저장소 작업을 측정합니다. 앞의 체크포인트 절감 결과는
단순 store 처리량이 아니라 실제 LangGraph를 사용한 end-to-end 벤치마크 결과입니다.

비동기 I/O 동시성별 차이는 다음과 같이 비교합니다.

```bash
python benchmarks/io_policy_benchmark.py \
  --operations 1000 \
  --concurrency 1,4,16 \
  --redis-url redis://localhost:6379/0
```

Windows/Python 3.12에서 4 KB 값 1,000개를 측정한 한 사례에서는 Redis concurrency 4가
put 처리량을 초당 658개에서 3,529개로 높였고, concurrency 16은 get 처리량을 초당
756개에서 3,828개로 높였습니다. 운영 용량을 의미하는 수치는 아니며 실제 Redis 연결
pool과 지연 분포에 맞게 조정해야 합니다.

## 데이터 수명주기

artifact 데이터와 체크포인트는 서로 다른 수명주기를 가집니다. 애플리케이션은
체크포인트 보존 정책과 `store.adelete_scope(thread_id)` 호출을 함께 관리해야 합니다.
replay, time travel, 장애 복구에서 아직 참조하는 artifact를 먼저 삭제하면 안 됩니다.

## Roadmap

S3와 MinIO 지원은 MVP 이후 항목입니다. 자세한 내용은 [ROADMAP.md](ROADMAP.md)를
참고하세요. 현재 릴리스에는 구현과 테스트가 완료된 InMemory, 로컬 파일, Redis backend만
포함됩니다.

## 라이선스

MIT
