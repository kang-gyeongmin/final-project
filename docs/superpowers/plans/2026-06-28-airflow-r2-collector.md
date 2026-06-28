# Airflow + R2 인구혼잡도 수집 파이프라인 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** citydata_ppltn 121개 장소 수집을 Airflow로 5분마다 자동 실행하고, 원본 JSON을 로컬 디스크와 Cloudflare R2에 함께 적재한다.

**Architecture:** 기존 `collectors/seoul_citydata.py`의 `fetch`/`save_raw`/`load_areas`를 그대로 재사용하고, 신규 `collectors/r2_storage.py`(R2 업로드)와 신규 `collect_and_store`(장소별 fetch→로컬저장→R2업로드, 단위 격리) 오케스트레이션 함수를 추가한다. Airflow는 Docker Compose(LocalExecutor+Postgres)로 로컬에 띄우고, DAG 하나가 PythonOperator로 `collect_and_store`를 호출한다.

**Tech Stack:** Python 3.13, boto3(R2는 S3 호환 API), Apache Airflow(Docker Compose, LocalExecutor), pytest, uv

## Global Constraints

- R2 자격증명(`R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET_NAME`)은 현재 비어 있음 — R2 업로드 실패가 다른 장소 수집이나 로컬 저장을 막아서는 안 됨
- 기존 `collectors/seoul_citydata.py`의 `build_url`, `load_areas`, `save_raw`, `fetch`, `collect_all` 함수의 시그니처는 변경하지 않음 (기존 테스트 5개가 계속 통과해야 함)
- DAG 스케줄: `*/5 * * * *` (5분마다)
- Airflow는 CeleryExecutor/Redis 없이 LocalExecutor + Postgres만 사용

---

### Task 1: R2 업로드 모듈 (`collectors/r2_storage.py`)

**Files:**
- Create: `collectors/r2_storage.py`
- Test: `tests/test_r2_storage.py`

**Interfaces:**
- Produces: `get_r2_client(boto3_client_factory=boto3.client) -> Any`
- Produces: `upload_json(client, bucket: str, key: str, payload: dict) -> None`

- [ ] **Step 1: Write the failing test for `upload_json`**

```python
# tests/test_r2_storage.py
import json

from collectors.r2_storage import upload_json


class FakeR2Client:
    def __init__(self):
        self.calls = []

    def put_object(self, **kwargs):
        self.calls.append(kwargs)


def test_upload_json_puts_payload_as_utf8_json_body():
    client = FakeR2Client()

    upload_json(client, bucket="my-bucket", key="raw/2026-06-28/강남역_151800.json", payload={"foo": "bar"})

    assert len(client.calls) == 1
    call = client.calls[0]
    assert call["Bucket"] == "my-bucket"
    assert call["Key"] == "raw/2026-06-28/강남역_151800.json"
    assert json.loads(call["Body"].decode("utf-8")) == {"foo": "bar"}
    assert call["ContentType"] == "application/json"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_r2_storage.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'collectors.r2_storage'`

- [ ] **Step 3: Write minimal implementation**

```python
# collectors/r2_storage.py
import json
import os
from typing import Any, Callable

import boto3


def get_r2_client(boto3_client_factory: Callable[..., Any] = boto3.client) -> Any:
    endpoint_url = f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    return boto3_client_factory(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    )


def upload_json(client: Any, bucket: str, key: str, payload: dict) -> None:
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_r2_storage.py -v`
Expected: PASS (1 passed)

- [ ] **Step 5: Write the failing test for `get_r2_client`**

```python
# tests/test_r2_storage.py (append)
from collectors.r2_storage import get_r2_client


def test_get_r2_client_builds_endpoint_url_from_account_id(monkeypatch):
    monkeypatch.setenv("R2_ACCOUNT_ID", "abc123")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "key-id")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "secret")
    captured = {}

    def fake_factory(service_name, **kwargs):
        captured["service_name"] = service_name
        captured["kwargs"] = kwargs
        return "fake-client"

    result = get_r2_client(boto3_client_factory=fake_factory)

    assert result == "fake-client"
    assert captured["service_name"] == "s3"
    assert captured["kwargs"]["endpoint_url"] == "https://abc123.r2.cloudflarestorage.com"
    assert captured["kwargs"]["aws_access_key_id"] == "key-id"
    assert captured["kwargs"]["aws_secret_access_key"] == "secret"
```

- [ ] **Step 6: Run test to verify it fails for the right reason**

Run: `uv run pytest tests/test_r2_storage.py::test_get_r2_client_builds_endpoint_url_from_account_id -v`
Expected: Should already PASS since Step 3 implemented `get_r2_client` too — if it fails, re-check Step 3's code was saved correctly. If it fails with an `AssertionError`, fix `get_r2_client` to match; if it errors before assertions run, fix the bug it surfaces.

- [ ] **Step 7: Run full test file and commit**

Run: `uv run pytest tests/test_r2_storage.py -v`
Expected: PASS (2 passed)

```bash
git add collectors/r2_storage.py tests/test_r2_storage.py
git commit -m "feat: add R2 upload helper for raw citydata payloads"
```

---

### Task 2: 장소별 격리 오케스트레이션 (`collect_and_store`)

**Files:**
- Modify: `collectors/seoul_citydata.py`
- Test: `tests/test_seoul_citydata.py`

**Interfaces:**
- Consumes: `fetch(area_name, api_key, http_get) -> dict`, `save_raw(base_dir, area_name, fetched_at, payload) -> Path` (existing, unchanged)
- Consumes: `upload_json(client, bucket, key, payload) -> None` (from Task 1, via injected `r2_client`/`r2_bucket` — this function imports `collectors.r2_storage.upload_json` directly)
- Produces: `collect_and_store(area_names: list[str], api_key: str, base_dir: Path, r2_client: Any, r2_bucket: str, http_get=requests.get, fetched_at: datetime | None = None) -> list[dict]`
  - Each result dict: `{"area_name": str, "local_path": Path | None, "r2_key": str | None, "error": str | None}`
  - `error` is set when `fetch` itself fails for that area (local_path/r2_key stay `None`)
  - If `fetch` succeeds but R2 upload fails, `local_path` is set, `r2_key` is `None`, `error` holds the R2 failure message (local save is not rolled back)

- [ ] **Step 1: Write the failing test for the all-success path**

```python
# tests/test_seoul_citydata.py (append, near existing collect_all tests)
from collectors.seoul_citydata import collect_and_store


class FakeR2Client:
    def __init__(self, fail_keys=None):
        self.put_calls = []
        self.fail_keys = fail_keys or set()

    def put_object(self, **kwargs):
        if kwargs["Key"] in self.fail_keys:
            raise RuntimeError(f"r2 upload failed for {kwargs['Key']}")
        self.put_calls.append(kwargs)


def test_collect_and_store_saves_locally_and_uploads_to_r2(tmp_path: Path):
    def fake_get(url, timeout):
        return FakeResponse({"url": url})

    fetched_at = datetime(2026, 6, 26, 15, 18, 0)
    r2_client = FakeR2Client()

    results = collect_and_store(
        area_names=["강남역", "광화문·덕수궁"],
        api_key="sample",
        base_dir=tmp_path,
        r2_client=r2_client,
        r2_bucket="my-bucket",
        http_get=fake_get,
        fetched_at=fetched_at,
    )

    assert results == [
        {
            "area_name": "강남역",
            "local_path": tmp_path / "2026-06-26" / "강남역_151800.json",
            "r2_key": "raw/2026-06-26/강남역_151800.json",
            "error": None,
        },
        {
            "area_name": "광화문·덕수궁",
            "local_path": tmp_path / "2026-06-26" / "광화문·덕수궁_151800.json",
            "r2_key": "raw/2026-06-26/광화문·덕수궁_151800.json",
            "error": None,
        },
    ]
    assert len(r2_client.put_calls) == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_seoul_citydata.py::test_collect_and_store_saves_locally_and_uploads_to_r2 -v`
Expected: FAIL with `ImportError: cannot import name 'collect_and_store'`

- [ ] **Step 3: Write minimal implementation**

```python
# collectors/seoul_citydata.py — add import and function
from collectors.r2_storage import upload_json


def collect_and_store(
    area_names: list[str],
    api_key: str,
    base_dir: Path,
    r2_client,
    r2_bucket: str,
    http_get=requests.get,
    fetched_at: datetime | None = None,
) -> list[dict]:
    timestamp = fetched_at or datetime.now()
    results = []
    for area_name in area_names:
        try:
            payload = fetch(area_name=area_name, api_key=api_key, http_get=http_get)
        except Exception as exc:
            results.append(
                {"area_name": area_name, "local_path": None, "r2_key": None, "error": str(exc)}
            )
            continue

        local_path = save_raw(base_dir=base_dir, area_name=area_name, fetched_at=timestamp, payload=payload)
        r2_key = f"raw/{timestamp.strftime('%Y-%m-%d')}/{area_name}_{timestamp.strftime('%H%M%S')}.json"

        try:
            upload_json(r2_client, bucket=r2_bucket, key=r2_key, payload=payload)
        except Exception as exc:
            results.append(
                {"area_name": area_name, "local_path": local_path, "r2_key": None, "error": str(exc)}
            )
            continue

        results.append(
            {"area_name": area_name, "local_path": local_path, "r2_key": r2_key, "error": None}
        )
    return results
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_seoul_citydata.py::test_collect_and_store_saves_locally_and_uploads_to_r2 -v`
Expected: PASS

- [ ] **Step 5: Write the failing test for R2-failure isolation**

```python
# tests/test_seoul_citydata.py (append)
def test_collect_and_store_keeps_local_save_when_r2_upload_fails(tmp_path: Path):
    def fake_get(url, timeout):
        return FakeResponse({"url": url})

    fetched_at = datetime(2026, 6, 26, 15, 18, 0)
    r2_client = FakeR2Client(fail_keys={"raw/2026-06-26/강남역_151800.json"})

    results = collect_and_store(
        area_names=["강남역"],
        api_key="sample",
        base_dir=tmp_path,
        r2_client=r2_client,
        r2_bucket="my-bucket",
        http_get=fake_get,
        fetched_at=fetched_at,
    )

    assert results[0]["local_path"] == tmp_path / "2026-06-26" / "강남역_151800.json"
    assert results[0]["local_path"].exists()
    assert results[0]["r2_key"] is None
    assert "r2 upload failed" in results[0]["error"]
```

- [ ] **Step 6: Run test to verify it fails first, then passes**

Run: `uv run pytest tests/test_seoul_citydata.py::test_collect_and_store_keeps_local_save_when_r2_upload_fails -v`
Expected before Step 3 logic existed: would already PASS since Step 3 handles this — if it FAILs, the `except Exception as exc` block around `upload_json` is missing or misordered; fix `collect_and_store` so the try/except wraps only the `upload_json` call (not `save_raw`), then re-run until PASS.

- [ ] **Step 7: Run fetch-failure isolation test**

```python
# tests/test_seoul_citydata.py (append)
def test_collect_and_store_skips_area_when_fetch_fails(tmp_path: Path):
    def fake_get(url, timeout):
        raise RuntimeError("network down")

    r2_client = FakeR2Client()

    results = collect_and_store(
        area_names=["강남역"],
        api_key="sample",
        base_dir=tmp_path,
        r2_client=r2_client,
        r2_bucket="my-bucket",
        http_get=fake_get,
        fetched_at=datetime(2026, 6, 26, 15, 18, 0),
    )

    assert results == [
        {"area_name": "강남역", "local_path": None, "r2_key": None, "error": "network down"}
    ]
    assert r2_client.put_calls == []
```

Run: `uv run pytest tests/test_seoul_citydata.py -v`
Expected: all tests in the file PASS (existing 5 + 3 new = 8 passed)

- [ ] **Step 8: Commit**

```bash
git add collectors/seoul_citydata.py tests/test_seoul_citydata.py
git commit -m "feat: add collect_and_store with per-area fetch/R2 failure isolation"
```

---

### Task 3: Airflow DAG (`dags/seoul_ppltn_collect.py`)

**Files:**
- Create: `dags/seoul_ppltn_collect.py`

**Interfaces:**
- Consumes: `collectors.seoul_citydata.load_areas(path) -> list[str]`, `collectors.seoul_citydata.collect_and_store(...) -> list[dict]` (Task 1+2), `collectors.r2_storage.get_r2_client(...) -> Any` (Task 1)
- Produces: module-level `run_collect_and_store() -> None` (used as the PythonOperator callable) and `dag` (the Airflow DAG object)

- [ ] **Step 1: Write the DAG file**

```python
# dags/seoul_ppltn_collect.py
import logging
import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

from collectors.r2_storage import get_r2_client
from collectors.seoul_citydata import collect_and_store, load_areas

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path("/opt/airflow/project")
AREAS_FILE = PROJECT_ROOT / "collectors" / "areas.txt"
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


def run_collect_and_store() -> None:
    api_key = os.environ["SEOUL_API_KEY"]
    r2_bucket = os.environ.get("R2_BUCKET_NAME", "")
    area_names = load_areas(AREAS_FILE)
    r2_client = get_r2_client()

    results = collect_and_store(
        area_names=area_names,
        api_key=api_key,
        base_dir=RAW_DATA_DIR,
        r2_client=r2_client,
        r2_bucket=r2_bucket,
    )

    succeeded = [r for r in results if r["error"] is None]
    failed = [r for r in results if r["error"] is not None]
    for r in failed:
        logger.warning("collect failed for %s: %s", r["area_name"], r["error"])
    logger.info("collected %d/%d areas successfully", len(succeeded), len(results))

    if not succeeded:
        raise RuntimeError(f"all {len(results)} areas failed to collect")


with DAG(
    dag_id="seoul_ppltn_collect",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2026, 6, 28),
    catchup=False,
) as dag:
    collect_task = PythonOperator(
        task_id="collect_and_store",
        python_callable=run_collect_and_store,
    )
```

- [ ] **Step 2: Commit**

```bash
git add dags/seoul_ppltn_collect.py
git commit -m "feat: add Airflow DAG for 5-minute citydata_ppltn collection"
```

(No automated test here — DAG import/runtime correctness is verified manually in Task 5 once Airflow is running.)

---

### Task 4: Docker Compose for Airflow (LocalExecutor + Postgres)

**Files:**
- Create: `docker-compose.yml`
- Modify: `.env.example`

**Interfaces:**
- Consumes: `dags/seoul_ppltn_collect.py` (Task 3), `collectors/` package (Tasks 1-2), `.env` (`SEOUL_API_KEY`, `R2_*`)
- Produces: running Airflow webserver on `localhost:8080`, scheduler picking up `dags/seoul_ppltn_collect.py`

- [ ] **Step 1: Write `docker-compose.yml`**

```yaml
# docker-compose.yml
x-airflow-common: &airflow-common
  image: apache/airflow:2.10.5
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    _PIP_ADDITIONAL_REQUIREMENTS: "requests boto3 python-dotenv"
    SEOUL_API_KEY: ${SEOUL_API_KEY}
    R2_ACCOUNT_ID: ${R2_ACCOUNT_ID}
    R2_ACCESS_KEY_ID: ${R2_ACCESS_KEY_ID}
    R2_SECRET_ACCESS_KEY: ${R2_SECRET_ACCESS_KEY}
    R2_BUCKET_NAME: ${R2_BUCKET_NAME}
    PYTHONPATH: /opt/airflow/project
  volumes:
    - ./dags:/opt/airflow/dags
    - ./collectors:/opt/airflow/project/collectors
    - ./data:/opt/airflow/project/data
    - airflow-logs:/opt/airflow/logs
  depends_on:
    - postgres

services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db:/var/lib/postgresql/data

  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    command: -c "airflow db migrate && airflow users create --username admin --password admin --firstname admin --lastname admin --role Admin --email admin@example.com"

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler

volumes:
  postgres-db:
  airflow-logs:
```

- [ ] **Step 2: Update `.env.example` with R2 variables**

```
# .env.example
SEOUL_API_KEY=sample
R2_ACCOUNT_ID=
R2_ACCESS_KEY_ID=
R2_SECRET_ACCESS_KEY=
R2_BUCKET_NAME=
```

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml .env.example
git commit -m "feat: add docker-compose for Airflow LocalExecutor running the collect DAG"
```

---

### Task 5: 수동 스모크 테스트 및 운영 문서화

**Files:**
- Modify: `CLAUDE.md` (수집 파이프라인 섹션을 Airflow+R2 구조로 갱신)

**Interfaces:**
- Consumes: 전체 스택 (Tasks 1-4)
- Produces: 없음 — 검증 + 문서 갱신만

- [ ] **Step 1: `.env` 준비**

`.env`(실제 키 보유, git에 커밋되지 않음)에 `SEOUL_API_KEY`를 실제 키로 채우고, R2 변수는 아직 비워둔다.

- [ ] **Step 2: Airflow 기동**

Run: `docker compose up airflow-init`
Expected: `Admin user admin created` 로그와 함께 종료 코드 0

Run: `docker compose up -d postgres airflow-webserver airflow-scheduler`
Expected: 세 컨테이너 모두 `Up` 상태

- [ ] **Step 3: DAG 인식 확인**

Run: `docker compose exec airflow-scheduler airflow dags list`
Expected: 출력에 `seoul_ppltn_collect` 포함 (import 에러 없음)

- [ ] **Step 4: 수동 트리거로 1회 실행 확인**

Run: `docker compose exec airflow-scheduler airflow dags trigger seoul_ppltn_collect`

`http://localhost:8080`에서 해당 DAG run의 `collect_and_store` task 로그를 확인.
Expected: 로그에 `collected <N>/121 areas successfully` (R2 키가 비어있으므로 R2 업로드는 실패 로그가 남지만 task 자체는 성공 — `succeeded`가 1개 이상이면 통과)

- [ ] **Step 5: 로컬 저장 확인**

Run: `ls data/raw/$(date +%Y-%m-%d) | head`
Expected: 방금 트리거한 시각의 `<장소명>_HHMMSS.json` 파일들이 존재

- [ ] **Step 6: `CLAUDE.md` 갱신**

`CLAUDE.md`의 "아키텍처" 섹션 중 Collector(Worker) 설명을 다음으로 교체:

```markdown
- **Collector**: Airflow(Docker Compose, LocalExecutor)가 5분마다(`*/5 * * * *`) `dags/seoul_ppltn_collect.py` DAG를 실행. 121개 장소를 순회하며 원본 응답을 로컬(`data/raw/`)과 Cloudflare R2(`raw/`)에 함께 적재. R2 업로드 실패는 장소 단위로 격리되어 다른 장소 수집을 막지 않음.
```

"미정 사항" 섹션에서 "수집 스케줄러 구현 방식"을 제거 (Airflow로 확정됨).

- [ ] **Step 7: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: update CLAUDE.md collector section for Airflow+R2 pipeline"
```
