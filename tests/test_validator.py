import io
import json
import os
import sys

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ["STATE_MACHINE_ARN"] = "arn:aws:states:us-east-1:123456789012:stateMachine:test"
os.environ["ALERT_TOPIC_ARN"] = "arn:aws:sns:us-east-1:123456789012:alerts"
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "validator"))

import app  # noqa: E402

GOOD = list(app.EXPECTED_COLUMNS)


# ---------------------------------------------------------------- unit
def test_valid_header():
    assert app.check_schema(GOOD) == []


def test_reordered_header_is_accepted():
    assert app.check_schema(list(reversed(GOOD))) == []


def test_header_is_case_and_space_insensitive():
    assert app.check_schema([f" {c.upper()} " for c in GOOD]) == []


def test_new_column_detected():
    problems = app.check_schema(GOOD + ["coupon_code"])
    assert len(problems) == 1 and "NEW column" in problems[0] and "coupon_code" in problems[0]


def test_missing_column_detected():
    problems = app.check_schema([c for c in GOOD if c != "email"])
    assert len(problems) == 1 and "MISSING" in problems[0] and "email" in problems[0]


def test_duplicate_and_blank_columns():
    problems = app.check_schema(GOOD + ["order_id", ""])
    assert any("Duplicate" in p for p in problems)
    assert any("blank" in p for p in problems)


def test_empty_header():
    assert app.check_schema([]) == ["Header row is empty or missing"]


def test_file_name_checks():
    assert app.check_name("incoming/orders_20261106.csv", 100) == []
    assert app.check_name("incoming/sales_report.csv", 100)
    assert app.check_name("incoming/orders_20261106.csv", 0)


def test_sns_envelope_unwrapped():
    inner = {"Records": [{"eventName": "ObjectCreated:Put"}]}
    assert app.s3_records(json.dumps(inner)) == inner["Records"]
    assert app.s3_records(json.dumps({"Message": json.dumps(inner)})) == inner["Records"]
    assert app.s3_records(json.dumps({"Event": "s3:TestEvent"})) == []


# --------------------------------------------------------- flow (faked AWS)
class FakeS3:
    def __init__(self, header_line):
        self.header_line, self.calls = header_line, []

    def get_object(self, **kw):
        return {"Body": io.BytesIO((self.header_line + "\n1,2,3\n").encode())}

    def copy_object(self, **kw):
        self.calls.append(("copy", kw["Key"]))

    def delete_object(self, **kw):
        self.calls.append(("delete", kw["Key"]))


class Recorder:
    def __init__(self):
        self.items, self.published, self.executions = [], [], []

    def put_item(self, **kw):
        self.items.append(kw["Item"])

    def publish(self, **kw):
        self.published.append(kw)

    def start_execution(self, **kw):
        self.executions.append(kw)


def event_for(key="incoming/orders_20261106.csv"):
    rec = {"eventName": "ObjectCreated:Put",
           "s3": {"bucket": {"name": "b"}, "object": {"key": key, "size": 123, "eTag": "abc"}}}
    return {"Records": [{"messageId": "m1", "body": json.dumps({"Records": [rec]})}]}


def install(monkeypatch, header_line):
    fake_s3, rec = FakeS3(header_line), Recorder()
    monkeypatch.setattr(app, "s3", fake_s3)
    monkeypatch.setattr(app, "sns", rec)
    monkeypatch.setattr(app, "sfn", rec)
    monkeypatch.setattr(app, "table", rec)
    return fake_s3, rec


def test_flow_valid_file_starts_execution(monkeypatch):
    fake_s3, rec = install(monkeypatch, ",".join(GOOD))
    assert app.handler(event_for(), None) == {"batchItemFailures": []}
    assert len(rec.executions) == 1 and not rec.published and not fake_s3.calls
    payload = json.loads(rec.executions[0]["input"])
    assert payload["filename"] == "orders_20261106.csv" and payload["bucket"] == "b"


def test_flow_new_column_alerts_and_moves(monkeypatch):
    fake_s3, rec = install(monkeypatch, ",".join(GOOD + ["coupon_code"]))
    assert app.handler(event_for(), None) == {"batchItemFailures": []}
    assert not rec.executions
    assert "Schema change detected" in rec.published[0]["Subject"]
    assert ("copy", "rejected/orders_20261106.csv") in fake_s3.calls
    assert ("delete", "incoming/orders_20261106.csv") in fake_s3.calls
    assert rec.items[0]["status"] == "REJECTED"


def test_flow_wrong_file_name_rejected_without_reading(monkeypatch):
    fake_s3, rec = install(monkeypatch, ",".join(GOOD))
    app.handler(event_for("incoming/sales_report.csv"), None)
    assert not rec.executions and rec.published
    assert "File rejected" in rec.published[0]["Subject"]
