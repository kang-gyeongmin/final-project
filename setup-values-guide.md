# Setup Values Guide

이 문서는 Airflow + Trino + dbt + Cloudflare R2 Data Catalog/Iceberg 환경을 띄우기 전에 준비해야 하는 값과 생성 방법을 정리한다.

기준 설계:

- R2 bucket (Iceberg 테이블): `seoul`
- R2 bucket (원본 JSON 스냅샷): `seoul-dev`
- Trino catalog: `iceberg` (prod), `iceberg_dev` (seoul-dev, 현재는 raw 스냅샷 전용)
- 도메인 schema: `seoul_ppltn`
- Airflow web UI host port: `8080`

실제 secret 값은 저장소에 커밋하지 않는다. repo에는 `.env.example`만 두고, 실제 값은 로컬 `.env`로 관리한다.

## 0. 현재 진행 상태

기준일: 2026-06-29

Cloudflare/R2 준비 상태:

- R2 bucket 준비 완료: `seoul`(prod, Iceberg 테이블), `seoul-dev`(원본 JSON 스냅샷)
- R2 Data Catalog 상태: `seoul` 활성화 완료, 인증 확인 완료
- 원본 JSON 스냅샷을 `seoul/raw/...`에서 `seoul-dev/bronze/population/...`로 이동 완료

`.env` 준비 상태:

- `SEOUL_API_KEY`(서울 열린데이터광장), R2/Data Catalog runtime 값 입력 완료
- Airflow local runtime 필수값(Fernet key, JWT secret, admin 계정) 생성 및 입력 완료
- Postgres 비밀번호 생성 및 입력 완료
- `CLOUDFLARE_API_TOKEN`, `WRANGLER_R2_SQL_AUTH_TOKEN`은 필요 시에만 채움

구현/검증 상태:

- `.env.example`, `.gitignore`, `.dockerignore` 작성 완료
- Docker Compose 작성 완료 (Airflow 3.2.2 LocalExecutor + Postgres + Trino)
- Trino Iceberg REST catalog 설정 작성 완료 (`iceberg`, `iceberg_dev`)
- end-to-end DAG(`seoul_ppltn_collect`: 수집 → R2 → Trino Bronze → dbt Silver/Gold/Test) 성공 확인 완료

## 1. 필요한 값 요약

### Cloudflare/R2 필수값

| 값 | 어디에 쓰나 | 만드는 방법 |
| --- | --- | --- |
| `CLOUDFLARE_ACCOUNT_ID` | R2 endpoint 구성 | Cloudflare dashboard 또는 `wrangler whoami` |
| `R2_ACCOUNT_ID` | 수집기(`get_r2_client`)의 R2 endpoint 구성 — **`CLOUDFLARE_ACCOUNT_ID`와 동일한 값을 넣어야 함** | 위와 동일 |
| `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` | R2 S3 호환 API 접근 (수집기, Trino 둘 다) | Cloudflare R2 > API Tokens에서 생성 |
| `R2_BUCKET_NAME` | 수집기가 원본 JSON을 적재할 버킷 (`seoul-dev`) | 이미 결정된 버킷 이름 |
| `R2_ENDPOINT` | Trino S3 호환 R2 접근(prod) | `https://<account_id>.r2.cloudflarestorage.com` |
| `R2_DATA_CATALOG_URI` / `WAREHOUSE` / `TOKEN` | Trino `iceberg`(prod) catalog | `npx wrangler r2 bucket catalog get seoul`, API 토큰은 Cloudflare 대시보드 |
| `R2_DEV_ENDPOINT`, `R2_DEV_ACCESS_KEY_ID`, `R2_DEV_SECRET_ACCESS_KEY` | Trino `iceberg_dev` catalog (seoul-dev) | 위와 동일, `seoul-dev` 버킷 기준 |
| `R2_DEV_DATA_CATALOG_URI` / `WAREHOUSE` / `TOKEN` | Trino `iceberg_dev` catalog | `npx wrangler r2 bucket catalog get seoul-dev` |

### 이 프로젝트 고유 값

| 값 | 어디에 쓰나 | 만드는 방법 |
| --- | --- | --- |
| `SEOUL_API_KEY` | 서울 열린데이터광장 citydata_ppltn API 호출 | data.seoul.go.kr에서 인증키 발급 |

### Airflow/Postgres 로컬 런타임 값

| 값 | 만드는 방법 |
| --- | --- |
| `AIRFLOW_UID` | Linux는 `id -u`, Windows/Mac Docker Desktop은 `50000` 권장 |
| `AIRFLOW_ADMIN_USERNAME` / `AIRFLOW_ADMIN_PASSWORD` / `AIRFLOW_ADMIN_EMAIL` | 임의 지정 (UI 로그인용) |
| `AIRFLOW_FERNET_KEY` | `python -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"` |
| `AIRFLOW_SECRET_KEY` | `python -c "import secrets; print(secrets.token_hex(32))"` |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | 임의 지정. **비밀번호를 바꾸면 기존 Postgres 볼륨을 `docker compose down -v`로 재생성해야 함** (볼륨 생성 시 비밀번호가 고정되기 때문) |

## 2. 빠른 점검 명령

```bash
# R2 Data Catalog(prod) 인증 확인
./scripts/check-r2-catalog-auth.sh

# R2 버킷/Data Catalog 생성 또는 확인
./scripts/bootstrap-cloudflare.sh

# 빌드 + 기동
./scripts/deploy.sh
```

## 3. 자주 막히는 부분

- `R2_ACCOUNT_ID`를 안 채우고 `CLOUDFLARE_ACCOUNT_ID`나 `R2_ENDPOINT`만 채우면 수집기의 R2 업로드가 계속 실패한다. 코드가 정확히 `R2_ACCOUNT_ID`라는 변수명을 읽기 때문.
- `.env`에 같은 키를 두 번 적으면 보통 **마지막 줄이 이긴다**. Postgres가 이미 떠 있는 상태에서 `POSTGRES_PASSWORD`만 바꾸면 다음 기동 때 인증이 깨진다 — 볼륨을 새로 만들어야 한다.
- Trino `catalog/*.properties` 파일을 추가/수정한 뒤에는 Trino 컨테이너를 재기동해야 새 catalog가 인식된다.
