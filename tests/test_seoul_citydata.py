import json
from datetime import datetime
from pathlib import Path

from collectors.seoul_citydata import build_url, collect_all, fetch, load_areas, save_raw


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def test_fetch_calls_http_get_with_built_url_and_returns_json():
    captured_urls = []

    def fake_get(url, timeout):
        captured_urls.append(url)
        return FakeResponse({"hello": "world"})

    result = fetch(area_name="강남역", api_key="sample", http_get=fake_get)

    assert result == {"hello": "world"}
    assert captured_urls == [build_url(api_key="sample", area_name="강남역")]


def test_collect_all_fetches_and_saves_each_area(tmp_path: Path):
    def fake_get(url, timeout):
        return FakeResponse({"url": url})

    fetched_at = datetime(2026, 6, 26, 15, 18, 0)

    result_paths = collect_all(
        area_names=["강남역", "광화문·덕수궁"],
        api_key="sample",
        base_dir=tmp_path,
        http_get=fake_get,
        fetched_at=fetched_at,
    )

    assert result_paths == [
        tmp_path / "2026-06-26" / "강남역_151800.json",
        tmp_path / "2026-06-26" / "광화문·덕수궁_151800.json",
    ]
    for path in result_paths:
        assert path.exists()


def test_save_raw_writes_json_under_date_and_area_named_file(tmp_path: Path):
    payload = {"foo": "bar"}
    fetched_at = datetime(2026, 6, 26, 15, 18, 0)

    result_path = save_raw(
        base_dir=tmp_path,
        area_name="광화문·덕수궁",
        fetched_at=fetched_at,
        payload=payload,
    )

    assert result_path == tmp_path / "2026-06-26" / "광화문·덕수궁_151800.json"
    assert json.loads(result_path.read_text(encoding="utf-8")) == payload


def test_build_url_url_encodes_korean_area_name():
    url = build_url(api_key="sample", area_name="보신각", fmt="json")

    assert url == (
        "http://openapi.seoul.go.kr:8088/sample/json/citydata_ppltn/1/5/"
        "%EB%B3%B4%EC%8B%A0%EA%B0%81"
    )


def test_load_areas_skips_comments_and_blank_lines(tmp_path: Path):
    areas_file = tmp_path / "areas.txt"
    areas_file.write_text(
        "# comment line\n"
        "광화문·덕수궁\n"
        "\n"
        "강남역\n",
        encoding="utf-8",
    )

    result = load_areas(areas_file)

    assert result == ["광화문·덕수궁", "강남역"]
