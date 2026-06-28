# 서울시 실시간 인구혼잡도 데이터 수집 및 분석 시스템

서울시 공공 API에서 실시간 인구혼잡도 데이터를 수집·적재·가공하여 분석 가능한 형태로 제공하는 데이터 파이프라인입니다.

## 🏗️ 아키텍처

```
[Airflow DAG] → [API 수집] → [R2 저장] → [Trino Bronze]
                                              ↓
                                        [DBT 변환]
                                              ↓
                                    [Silver/Gold 생성]
```

**3단계 Medallion 아키텍처:**
- **Bronze**: 원본 데이터 (673개 행)
- **Silver**: 정제된 데이터 (중복 제거, 351개 행)
- **Gold**: 분석용 데이터 (시간순 정렬, 351개 행)

## 📊 데이터 도메인

- 🎯 **1순위**: 인구혼잡도 (현재 구현)
- 🚗 **2순위**: 교통
- 🎪 **3순위**: 행사
- ☀️ **4순위**: 기상

## 🛠️ 기술 스택

| 구성요소 | 기술 |
|---------|------|
| **오케스트레이션** | Apache Airflow 3.2.2 (Docker) |
| **데이터 수집** | Python + 서울시 공공 API |
| **원본 저장** | Cloudflare R2 (S3 호환) |
| **데이터 웨어하우스** | Trino + Iceberg |
| **데이터 변환** | DBT (dbt-trino) |
| **로컬 저장** | JSONL 포맷 |

## 📁 디렉토리 구조

```
├── dags/                          # Airflow DAG 정의
│   └── seoul_ppltn_collect.py    # 메인 파이프라인 (5분 주기)
├── collectors/                    # 데이터 수집 로직
│   ├── seoul_citydata.py         # API 호출 + 배치 Trino INSERT
│   ├── r2_storage.py             # R2 업로드
│   ├── areas.txt                 # 118개 모니터링 장소 목록
│   └── trino_loader.py           # Trino 배치 INSERT 유틸
├── dbt/seoul_ppltn/              # DBT 프로젝트
│   ├── dbt_project.yml           # DBT 설정
│   ├── profiles.yml              # Trino 연결
│   ├── models/
│   │   ├── schema.yml            # 데이터 스키마
│   │   ├── silver/               # 정제 모델
│   │   └── gold/                 # 분석 모델
│   └── tests/                    # 데이터 품질 테스트
├── docker-compose.yml            # 서비스 정의
├── Dockerfile.airflow            # Airflow 이미지
├── trino/catalog/                # Trino 카탈로그 설정
└── data/raw/                     # 로컬 raw 데이터 (JSONL)
```

## 🚀 빠른 시작

### 1. 환경 설정

```bash
# .env 파일 수정
cp .env.example .env
# 다음 항목 입력:
# - SEOUL_API_KEY: 서울 열린데이터광장 API 키
# - R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME
# - DBT_TARGET: dev or prod
```

### 2. Docker 실행

```bash
docker-compose down
docker-compose up -d --build
```

모든 서비스가 `healthy` 상태가 될 때까지 대기 (1-2분)

### 3. Airflow UI 접속

```
http://localhost:8080
```

- **사용자명**: admin
- **비밀번호**: .env의 `AIRFLOW_ADMIN_PASSWORD`

### 4. DAG 실행

UI에서 `seoul_ppltn_collect` DAG의 ▶️ 버튼 클릭

**또는 CLI:**

```bash
docker-compose exec airflow-scheduler airflow dags test seoul_ppltn_collect 2026-06-28
```

### 5. Trino UI 접속 (선택)

```
http://localhost:8081
```

테이블 조회:
- Catalog: `iceberg`
- Schema: `seoul_ppltn`
- Tables: `bronze_seoul_ppltn`, `silver_seoul_ppltn`, `gold_seoul_ppltn_by_time`

## 📊 데이터 조회

### Bronze (원본 데이터)

```bash
docker-compose exec trino trino --execute \
  "SELECT area_nm, ppltn_time, area_ppltn_min, area_ppltn_max \
   FROM iceberg.seoul_ppltn.bronze_seoul_ppltn \
   LIMIT 10"
```

### Silver (정제 데이터)

```bash
docker-compose exec trino trino --execute \
  "SELECT area_nm, area_congest_lvl, ppltn_time \
   FROM iceberg.seoul_ppltn.silver_seoul_ppltn \
   ORDER BY ppltn_time DESC LIMIT 10"
```

### Gold (분석용 데이터)

```bash
docker-compose exec trino trino --execute \
  "SELECT ppltn_time, area_nm, area_congest_lvl, avg_ppltn \
   FROM iceberg.seoul_ppltn.gold_seoul_ppltn_by_time \
   ORDER BY ppltn_time DESC LIMIT 10"
```

## 📈 파이프라인 상세

### collect_and_store (DAG Task 1)

**실행 시간**: ~1분 (118개 장소)

1. **API 호출**: 서울시 citydata API에서 각 장소의 인구혼잡도 수집
2. **로컬 저장**: JSONL 포맷으로 `data/raw/{날짜}/{장소명}.jsonl` 저장
3. **R2 업로드**: `raw/{날짜}/{시}/{분}/{장소명}.json` 형식으로 업로드
4. **Trino 배치 INSERT**: 118개를 한 번에 Bronze 테이블에 로드

### dbt_run (DAG Task 2)

**실행 시간**: ~10초

Bronze → Silver → Gold 변환:

**Silver 모델** (`models/silver/silver_seoul_ppltn.sql`):
- 타입 정규화 (문자열 → INT, DECIMAL)
- 공백 처리 (빈 문자열 → NULL)
- 중복 제거 (장소+시간 기준 최신만)

**Gold 모델** (`models/gold/gold_seoul_ppltn_by_time.sql`):
- 평균 인구 계산: `(area_ppltn_min + area_ppltn_max) / 2`
- 시간순 정렬
- 분석에 필요한 컬럼만 선택

### dbt_test (DAG Task 3)

**실행 시간**: ~5초

데이터 품질 검증:
- Silver 테이블 비어있음 확인
- 스키마 정합성 검증

## 🔄 스케줄

**기본**: 5분마다 자동 실행 (`*/5 * * * *`)

**DAG 활성화/비활성화:**

```bash
# 활성화
docker-compose exec airflow-scheduler airflow dags unpause seoul_ppltn_collect

# 비활성화
docker-compose exec airflow-scheduler airflow dags pause seoul_ppltn_collect
```

## 📝 API 명세

### 서울시 공공 API (citydata_ppltn)

**엔드포인트**: `http://openapi.seoul.go.kr:8088/{API_KEY}/json/citydata_ppltn/1/5/{장소명}`

**응답 스키마**:

```json
{
  "SeoulRtd.citydata_ppltn": [
    {
      "AREA_NM": "강남역",
      "AREA_CD": "POI014",
      "AREA_CONGEST_LVL": "보통",
      "AREA_PPLTN_MIN": "46000",
      "AREA_PPLTN_MAX": "48000",
      "MALE_PPLTN_RATE": "46.4",
      "FEMALE_PPLTN_RATE": "53.6",
      "PPLTN_RATE_0": "1.5",
      "PPLTN_RATE_10": "7.9",
      "PPLTN_RATE_20": "31.7",
      "PPLTN_RATE_30": "22.0",
      "PPLTN_RATE_40": "15.5",
      "PPLTN_RATE_50": "11.2",
      "PPLTN_RATE_60": "5.9",
      "PPLTN_RATE_70": "4.4",
      "RESNT_PPLTN_RATE": "31.7",
      "NON_RESNT_PPLTN_RATE": "68.3",
      "PPLTN_TIME": "2026-06-28 18:30",
      "FCST_YN": "Y",
      "FCST_PPLTN": [...]
    }
  ]
}
```

**주요 필드**:
- `AREA_NM`: 장소명
- `AREA_CONGEST_LVL`: 혼잡도 ("여유", "보통", "혼잡", "매우혼잡")
- `AREA_PPLTN_MIN/MAX`: 추정 인구 범위
- `MALE/FEMALE_PPLTN_RATE`: 성별 비율
- `PPLTN_RATE_X`: 연령대별 비율 (0-9세, 10-19세, ...)
- `RESNT/NON_RESNT_PPLTN_RATE`: 상주/비상주 인구 비율
- `PPLTN_TIME`: 데이터 기준 시간 (약 20-25분 지연)

## ⚠️ 알려진 제약사항

- **API 지연**: 서울시 API는 약 20-25분 지연된 데이터 제공
- **모니터링 장소**: 118개 장소만 수집 (확장 가능)
- **Trino 메모리**: 로컬 개발용이므로 대용량 데이터는 프로덕션 환경 권장

## 🔧 트러블슈팅

### 1. Trino 연결 실패

```bash
docker-compose logs trino --tail 50
docker-compose ps | grep trino
```

Trino가 `healthy` 상태인지 확인

### 2. DAG가 자동 실행 안 됨

```bash
# 스케줄러 상태 확인
docker-compose ps | grep scheduler

# DAG 활성화
docker-compose exec airflow-scheduler airflow dags unpause seoul_ppltn_collect
```

### 3. DBT 실행 실패

```bash
docker-compose logs airflow-scheduler --tail 100 | grep dbt
```

profiles.yml 설정과 Trino 연결 확인

## 📚 참고 자료

- [Airflow 문서](https://airflow.apache.org/)
- [DBT 문서](https://docs.getdbt.com/)
- [Trino 문서](https://trino.io/docs/)
- [서울 열린데이터광장](https://data.seoul.go.kr/)

## 📝 라이선스

MIT License

## 👤 작성자

kang-gyeongmin (rkdrudals112@gmail.com)

---

**마지막 업데이트**: 2026-06-28
