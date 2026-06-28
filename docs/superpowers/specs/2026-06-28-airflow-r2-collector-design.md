# Airflow + R2 인구혼잡도 수집 파이프라인 설계

## 배경

현재 `collectors/run_collect.py`는 사람이 직접 한 번 실행해서 121개 장소(citydata_ppltn)
데이터를 `data/raw/<날짜>/<장소>_<HHMMSS>.json`에 저장하는 스크립트다. 이를 5분 간격으로
자동 반복 실행하고, 원본 데이터를 Cloudflare R2에도 적재하도록 확장한다.

## 목표

- Airflow가 5분마다 자동으로 121개 장소를 수집
- 수집한 원본 JSON을 Cloudflare R2에 적재 (CLAUDE.md에 명시된 목표 저장소)
- R2 자격증명이 아직 없는 현재 상태에서도 파이프라인이 죽지 않고 동작 (R2 실패는 격리, 로컬 저장은 계속됨)
- 기존 `collectors/seoul_citydata.py`의 `fetch`/`save_raw`/`load_areas`는 변경 없이 재사용

## 비목표

- R2 자격증명 발급/설정 자체 (사용자가 추후 `.env`에 채워 넣음)
- DuckDB 가공(processor) 단계 — 이번 스펙은 수집·적재까지만
- Airflow 운영 환경(원격 배포, HA 등) — 로컬 Docker Compose로 한정

## 아키텍처

```
[Airflow Scheduler] --(*/5 * * * * cron)--> DAG: seoul_ppltn_collect
                                                   |
                                          PythonOperator (1개 task)
                                                   |
                                    collect_and_store(area_names, ...)
                                                   |
                              각 장소마다: fetch() -> save_raw(로컬) -> upload_json(R2)
                                          (장소 단위로 예외 격리, 한 곳 실패해도 나머지 계속)
```

- **Airflow 실행 환경**: Docker Compose, `LocalExecutor` + Postgres (Celery/Redis 워커 없음 — 단일 경량 task만 도는 워크로드라 과한 구성 불필요)
- **DAG**: `dags/seoul_ppltn_collect.py`, `schedule_interval="*/5 * * * *"`, task 1개(`PythonOperator`)가 오케스트레이션 함수를 호출
- **R2 적재 모듈**: `collectors/r2_storage.py` — boto3 S3 클라이언트로 Cloudflare R2(S3 호환 엔드포인트)에 업로드하는 순수 함수
- **오케스트레이션**: `collectors/seoul_citydata.py`에 `collect_and_store(area_names, api_key, base_dir, r2_client, r2_bucket, http_get=..., fetched_at=...)` 추가
  - 장소별로 `try/except`로 감싸 R2 업로드 실패가 다른 장소 수집을 막지 않게 함
  - 로컬 저장(`save_raw`)은 항상 수행 (디버그/백업 용도로 유지)

## 데이터 흐름 및 키 규칙

- 로컬: `data/raw/<YYYY-MM-DD>/<AREA_NM>_<HHMMSS>.json` (기존과 동일)
- R2: `raw/<YYYY-MM-DD>/<AREA_NM>_<HHMMSS>.json` (로컬과 동일한 상대 경로 패턴, prefix만 `raw/`)

## 환경 변수 (.env)

```
SEOUL_API_KEY=...      # 기존
R2_ACCOUNT_ID=         # 비워둠, 추후 입력
R2_ACCESS_KEY_ID=
R2_SECRET_ACCESS_KEY=
R2_BUCKET_NAME=
```

`R2_ACCOUNT_ID` 등이 비어 있으면 R2 클라이언트 생성 시점에 막연히 죽지 않고, R2 업로드를
시도하는 시점에서 장소 단위로 실패 처리되어 로그만 남기고 다음 장소로 진행한다.

## 에러 처리

- 장소 단위 `fetch` 실패: 해당 장소만 건너뛰고 로그, 나머지 장소는 계속 진행 (기존 동작과 동일하게 유지하되 명시적으로 try/except 추가)
- 장소 단위 R2 업로드 실패: 로그만 남기고 로컬 저장은 이미 끝난 상태이므로 데이터 손실 없음
- DAG task 자체는 "전체 중 일부라도 처리됨"이면 성공으로 간주 (전부 실패하면 task 실패로 Airflow가 표시)

## 테스트 계획

- `collectors/r2_storage.py`: `upload_json(client, bucket, key, payload)`을 가짜 클라이언트(`put_object` 호출 캡처)로 TDD
- `collect_and_store`: 기존 `collect_all` 테스트와 동일한 패턴으로 fetch/save/upload를 스텁 처리해 오케스트레이션 로직(장소별 격리 포함) 검증
- DAG 파일 자체는 별도 유닛 테스트 없음 — `docker compose`로 띄운 뒤 Airflow UI/`airflow dags list`로 import 성공 여부만 수동 확인

## 디렉토리 구조 변경

```
docker-compose.yml          # 신규 — Airflow(LocalExecutor)+Postgres
dags/
  seoul_ppltn_collect.py    # 신규
collectors/
  seoul_citydata.py         # collect_and_store 함수 추가
  r2_storage.py             # 신규
```
