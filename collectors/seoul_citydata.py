import json
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import requests

from collectors.r2_storage import upload_json

BASE_URL = "http://openapi.seoul.go.kr:8088"
SERVICE_NAME = "citydata_ppltn"


def build_url(api_key: str, area_name: str, fmt: str = "json", start: int = 1, end: int = 5) -> str:
    encoded_area = quote(area_name, safe="")
    return f"{BASE_URL}/{api_key}/{fmt}/{SERVICE_NAME}/{start}/{end}/{encoded_area}"


def load_areas(path: Path) -> list[str]:
    lines = Path(path).read_text(encoding="utf-8").splitlines()
    return [line.strip() for line in lines if line.strip() and not line.startswith("#")]


def save_raw(base_dir: Path, area_name: str, fetched_at: datetime, payload: dict) -> Path:
    day_dir = Path(base_dir) / fetched_at.strftime("%Y-%m-%d")
    day_dir.mkdir(parents=True, exist_ok=True)
    file_path = day_dir / f"{area_name}.json"

    if file_path.exists():
        entries = json.loads(file_path.read_text(encoding="utf-8"))
    else:
        entries = []

    entries.append({"fetched_at": fetched_at.isoformat(), "payload": payload})
    file_path.write_text(json.dumps(entries, ensure_ascii=False, indent=2), encoding="utf-8")
    return file_path


def fetch(area_name: str, api_key: str, http_get=requests.get) -> dict:
    url = build_url(api_key=api_key, area_name=area_name)
    response = http_get(url, timeout=10)
    response.raise_for_status()
    return response.json()


def collect_all(
    area_names: list[str],
    api_key: str,
    base_dir: Path,
    http_get=requests.get,
    fetched_at: datetime | None = None,
) -> list[Path]:
    timestamp = fetched_at or datetime.now()
    saved_paths = []
    for area_name in area_names:
        payload = fetch(area_name=area_name, api_key=api_key, http_get=http_get)
        saved_paths.append(
            save_raw(base_dir=base_dir, area_name=area_name, fetched_at=timestamp, payload=payload)
        )
    return saved_paths


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
        r2_key = (
            f"raw/{timestamp.strftime('%Y-%m-%d')}/{timestamp.strftime('%H')}"
            f"/{timestamp.strftime('%M')}/{area_name}.json"
        )

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
