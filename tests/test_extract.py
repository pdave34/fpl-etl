import os
from unittest import mock

import polars as pl
import pytest

from etl.extractors import APIExtractor


@pytest.fixture
def mock_response_success():
    """Mock a successful ``requests.Response`` with JSON payload."""
    mock_resp = mock.Mock()
    mock_resp.status_code = 200
    mock_resp.elapsed = mock.Mock()
    mock_resp.elapsed.total_seconds.return_value = 0.123
    mock_resp.json.return_value = {
        "events": [
            {"id": 1, "name": "Event 1", "active": True},
            {"id": 2, "name": "Event 2", "active": False},
        ]
    }
    return mock_resp


@pytest.fixture
def mock_response_failure():
    """Mock a failed ``requests.Response`` (non‑200)."""
    mock_resp = mock.Mock()
    mock_resp.status_code = 500
    mock_resp.elapsed = mock.Mock()
    mock_resp.elapsed.total_seconds.return_value = 0.456
    mock_resp.json.return_value = {}
    return mock_resp


def test_api_extractor_get_success(monkeypatch, mock_response_success):
    """Ensure ``APIExtractor.get`` records timing and response info."""
    monkeypatch.setattr("requests.get", lambda *a, **kw: mock_response_success)
    extractor = APIExtractor()
    stamp = extractor.get("http://example.com")
    # Basic keys exist
    assert "request_id" in stamp
    assert "request_time" in stamp
    assert stamp["response_code"] == 200
    assert stamp["response_elapsed_ms"] == mock_response_success.elapsed.total_seconds() * 1000
    # Response object should be removed after parse
    df = extractor.parse(stamp, key="events")
    # DataFrame should contain enriched columns
    for col in ["request_id", "request_time", "response_code", "response_elapsed_ms"]:
        assert col in df.columns
    # Boolean column should be cast to Int8 (1/0)
    assert df["active"].dtype == pl.Int8
    # Values should be numeric
    assert df["active"].to_list() == [1, 0]


def test_api_extractor_parse_failure(monkeypatch, mock_response_failure):
    """When response code is not 200, ``parse`` should return an empty DataFrame."""
    monkeypatch.setattr("requests.get", lambda *a, **kw: mock_response_failure)
    extractor = APIExtractor()
    stamp = extractor.get("http://example.com")
    df = extractor.parse(stamp, key="events")
    # Empty DataFrame (no rows)
    assert df.height == 0
    # Enriched columns should still be present
    for col in ["request_id", "request_time", "response_code", "response_elapsed_ms"]:
        assert col in df.columns


def test_api_extractor_save(tmp_path):
    """``save`` should write a Parquet file that can be read back."""
    df = pl.DataFrame({"a": [1, 2], "b": [True, False]})
    # clean_df will convert booleans to Int8
    from etl.utils import clean_df

    df = clean_df(df)
    extractor = APIExtractor()
    file_path = os.path.join(tmp_path, "out.parquet")
    extractor.save(df, file_path)
    # Read back and verify content
    df2 = pl.read_parquet(file_path)
    assert df2.equals(df)
