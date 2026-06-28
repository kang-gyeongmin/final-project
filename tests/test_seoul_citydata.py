import json
from datetime import datetime
from pathlib import Path

from collectors.seoul_citydata import build_url, collect_all, collect_and_store, fetch, load_areas, save_raw


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
        tmp_path / "2026-06-26" / "강남역.json",
        tmp_path / "2026-06-26" / "광화문·덕수궁.json",
    ]
    for path in result_paths:
        assert path.exists()


def test_save_raw_creates_file_with_single_entry_when_new(tmp_path: Path):
    payload = {"foo": "bar"}
    fetched_at = datetime(2026, 6, 26, 15, 18, 0)

    result_path = save_raw(
        base_dir=tmp_path,
        area_name="광화문·덕수궁",
        fetched_at=fetched_at,
        payload=payload,
    )

    assert result_path == tmp_path / "2026-06-26" / "광화문·덕수궁.json"
    written_text = result_path.read_text(encoding="utf-8")
    assert json.loads(written_text) == [
        {"fetched_at": "2026-06-26T15:18:00", "payload": payload}
    ]
    assert "\n" in written_text


def test_save_raw_appends_entry_when_file_already_exists(tmp_path: Path):
    area_name = "광화문·덕수궁"
    first_at = datetime(2026, 6, 26, 15, 18, 0)
    second_at = datetime(2026, 6, 26, 15, 23, 0)

    save_raw(base_dir=tmp_path, area_name=area_name, fetched_at=first_at, payload={"n": 1})
    result_path = save_raw(base_dir=tmp_path, area_name=area_name, fetched_at=second_at, payload={"n": 2})

    assert result_path == tmp_path / "2026-06-26" / "광화문·덕수궁.json"
    assert json.loads(result_path.read_text(encoding="utf-8")) == [
        {"fetched_at": "2026-06-26T15:18:00", "payload": {"n": 1}},
        {"fetched_at": "2026-06-26T15:23:00", "payload": {"n": 2}},
    ]


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
            "local_path": tmp_path / "2026-06-26" / "강남역.json",
            "r2_key": "raw/2026-06-26/15/18/강남역.json",
            "error": None,
        },
        {
            "area_name": "광화문·덕수궁",
            "local_path": tmp_path / "2026-06-26" / "광화문·덕수궁.json",
            "r2_key": "raw/2026-06-26/15/18/광화문·덕수궁.json",
            "error": None,
        },
    ]
    assert len(r2_client.put_calls) == 2


def test_collect_and_store_keeps_local_save_when_r2_upload_fails(tmp_path: Path):
    def fake_get(url, timeout):
        return FakeResponse({"url": url})

    fetched_at = datetime(2026, 6, 26, 15, 18, 0)
    r2_client = FakeR2Client(fail_keys={"raw/2026-06-26/15/18/강남역.json"})

    results = collect_and_store(
        area_names=["강남역"],
        api_key="sample",
        base_dir=tmp_path,
        r2_client=r2_client,
        r2_bucket="my-bucket",
        http_get=fake_get,
        fetched_at=fetched_at,
    )

    assert results[0]["local_path"] == tmp_path / "2026-06-26" / "강남역.json"
    assert results[0]["local_path"].exists()
    assert results[0]["r2_key"] is None
    assert "r2 upload failed" in results[0]["error"]


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
