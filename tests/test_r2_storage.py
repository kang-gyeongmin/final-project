import json

from collectors.r2_storage import get_r2_client, upload_json


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
