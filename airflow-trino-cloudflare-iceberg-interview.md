# Airflow + Trino + dbt + Cloudflare Iceberg 인터뷰 정리

- 작성일: 2026-06-29
- 목적: 서울시 실시간 인구혼잡도 수집 파이프라인을 위해 Airflow, Trino, dbt, Cloudflare R2 Data Catalog/Iceberg 기반 실행 환경을 만들기 위한 요구사항/결정 정리
- 현재 상태: end-to-end로 동작 중 (수집 → R2 적재 → Trino Bronze → dbt Silver/Gold)

## 1. 목표

Airflow가 DAG 안에서 직접 데이터를 수집·R2에 적재하고, 그 직후 dbt를 실행한다. dbt는 Trino를 실행 엔진으로 사용한다.

Trino는 Cloudflare R2 Data Catalog의 Iceberg REST catalog에 연결해서, dbt 작업에서 Iceberg 테이블을 조회·생성·갱신할 수 있어야 한다.

이 프로젝트의 목표는 "서울시 실시간 인구혼잡도" 1개 도메인에 대해, 수집부터 사용자 질의응답에 쓸 수 있는 정제 테이블까지 끝까지 도는 파이프라인을 갖추는 것이다. 멀티 도메인/멀티 멘티 운영 모델은 고려하지 않는다(1인 프로젝트).

## 2. 확정된 아키텍처

### 실행 구조

- Docker Compose로 Airflow와 Trino를 띄운다 (LocalExecutor + Postgres, Celery/Redis 없음).
- Airflow DAG(`seoul_ppltn_collect`)가 5분마다(`*/5 * * * *`) 실행되며, 다음 순서로 동작:
  1. `collect_and_store`: 서울 열린데이터광장 citydata_ppltn API에서 118개 장소를 수집 → 로컬(`data/raw/`)과 Cloudflare R2에 함께 적재 → Trino Bronze 테이블에 배치 INSERT
  2. `dbt_run`: Bronze → Silver → Gold 모델 빌드
  3. `dbt_test`: Silver 모델 검증
- dbt는 `dbt-trino` adapter로 Trino에 연결한다.
- Airflow `3.2.2`, dbt-core/`dbt-trino` 버전은 `Dockerfile.airflow`에서 고정.
- dbt는 Airflow의 Python 의존성과 충돌하지 않도록 별도 가상환경에 설치한다 (참조 인프라와 동일한 방식).

### 메달리언 구조

- `bronze_seoul_ppltn`: 원본 그대로(varchar 위주) 적재. `collect_and_store`가 Trino에 직접 INSERT.
- `silver_seoul_ppltn`: 타입 정제 + 같은 장소·같은 시각 중복 제거(`row_number() over (partition by area_nm, ppltn_time)`).
- `gold_seoul_ppltn_by_time`: 시간대별 장소 인구혼잡도 분석용, `avg_ppltn` 등 파생 컬럼 추가.
- 레이어 구분은 schema가 아니라 테이블 이름(`bronze_`/`silver_`/`gold_`)으로 표현한다 (참조 인프라와 동일).

### Trino catalog/bucket 구조

이 프로젝트는 R2 버킷을 두 개 쓴다.

| 버킷 | Trino catalog | 용도 |
| --- | --- | --- |
| `seoul` | `iceberg` | Iceberg 테이블(Bronze/Silver/Gold)이 실제로 저장되는 prod 위치 |
| `seoul-dev` | `iceberg_dev` (카탈로그는 추가했으나 아직 dbt에서 사용 안 함) | 수집기가 원본 JSON 스냅샷을 적재하는 위치 (`bronze/population/<날짜>/<시>/<분>/<장소>.json`) |

즉 **원본(raw object) 스냅샷**과 **Iceberg 테이블 데이터**가 서로 다른 버킷에 있다 — 의도적인 분리이며, raw 스냅샷은 디버깅/재처리용 백업, Iceberg 테이블이 실질적인 분석/질의 대상이다.

## 3. 진행하면서 발견한 이슈와 해결

- **Airflow 2.10.5 → 3.2.2 업그레이드**: `schedule_interval` → `schedule` API 변경 대응 필요.
- **R2 클라이언트 생성 실패가 전체 task를 죽이던 문제**: `R2_ACCOUNT_ID`가 비어 있을 때 `boto3.client()`가 즉시 예외를 던져서 121개 장소 전체가 시도조차 안 됨 → `_BrokenR2Client`로 감싸서 실패를 장소 단위 업로드 시점으로 미룸.
- **DAG 성공 판정 버그**: R2 업로드 실패도 `error` 필드에 기록되는데, 성공 판정을 `error is None`으로 했더니 R2가 비어 있으면 로컬 수집이 100% 성공해도 항상 실패로 표시됨 → `local_path is not None` 기준으로 변경.
- **타임존**: 컨테이너가 UTC로 동작해서 로컬/R2 경로가 UTC 시각으로 찍히던 문제 → DAG에서 `Asia/Seoul` 타임존을 명시한 `fetched_at`을 넘기도록 수정.
- **로컬 raw 파일 포맷**: 장소+날짜당 파일이 계속 쪼개지던 것 → `.jsonl`(append-only)로 통일.

## 4. 추후 결정 필요

- `iceberg_dev` 카탈로그(seoul-dev 버킷)를 dbt에서 실제로 쓸지, 아니면 raw 스냅샷 백업 용도로만 둘지
- `data/raw/` 로컬 보관 기간 정책 (현재는 무제한 누적)
- 인구혼잡도 외 도메인(교통/행사/기상) 추가 시 동일한 DAG에 합칠지, DAG를 도메인별로 분리할지
