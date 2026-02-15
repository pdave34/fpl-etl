from pathlib import Path

import pandas as pd
import pyarrow as pa
from pytest import fixture, mark

from etl.loaders import BaseLoader, OracleDBLoader

sample = pd.DataFrame(
    {
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
        "city": ["New York", "Los Angeles", "Chicago"],
    }
)
sample_table = pa.table(sample)
for suffix in ["csv", "xlsx", "json", "txt"]:
    sample_data_path = Path(f"tests/data/sample_data.{suffix}")
    sample_data_path.parent.mkdir(parents=True, exist_ok=True)
    if suffix == "csv":
        sample.to_csv(sample_data_path, index=False)
    elif suffix == "xlsx":
        sample.to_excel(sample_data_path, index=False)
    elif suffix == "txt":
        sample.to_csv(sample_data_path, index=False, sep="\t")
    elif suffix == "json":
        sample.to_json(sample_data_path, orient="records", lines=False)


@fixture
def loader():
    return BaseLoader()


@fixture
def oracle_loader():
    return OracleDBLoader()


def test_read_csv(loader):
    data_path = Path("tests/data/sample_data.csv")
    df = loader.read_data(data_path)
    assert df.equals(pa.table(sample))


def test_read_excel(loader):
    data_path = Path("tests/data/sample_data.xlsx")
    df = loader.read_data(data_path)
    assert df.equals(pa.table(sample))


def test_read_json(loader):
    data_path = Path("tests/data/sample_data.json")
    df = loader.read_data(data_path)
    assert df.equals(pa.table(sample))


def test_read_unsupported_format(loader):
    data_path = Path("tests/data/sample_data.txt")
    try:
        loader.read_data(data_path)
        assert False, "Unsupported format did not raise an exception"
    except ValueError as e:
        assert "Unsupported file format" in str(e)


def test_oracle_connection(oracle_loader):
    assert oracle_loader.test_connection() is True


def test_connection_failure():
    try:
        faulty_loader = OracleDBLoader(dsn="INVALID_DSN", user="WRONG_USER", password="WRONG_PASS")
        assert faulty_loader.test_connection() is False
    except Exception as e:
        assert True, f"Connection failure test failed unexpectedly: {e}"


def test_table_exists(oracle_loader):
    try:
        assert oracle_loader.check_table_exists("SAMPLE_DATA") is True
    except Exception as e:
        assert False, f"Table existence check failed with exception: {e}"


def test_table_does_not_exist(oracle_loader):
    try:
        assert oracle_loader.check_table_exists("DUMDUM") is False
    except Exception as e:
        assert False, f"Table existence check failed with exception: {e}"


def test_insert_data(oracle_loader):
    try:
        oracle_loader.insert_data(sample_table, table_name="SAMPLE_DATA")
    except Exception as e:
        assert False, f"Data insertion failed with exception: {e}"


def test_insert_full_data(oracle_loader):
    try:
        oracle_loader.insert_full_data(sample_table, table_name="SAMPLE_DATA")
    except Exception as e:
        assert False, f"Full data insertion failed with exception: {e}"


@mark.order(1)
def test_create_table(oracle_loader):
    try:
        oracle_loader.create_table("TEST_CREATE_TABLE", sample_table)
    except Exception as e:
        assert False, f"Table creation failed with exception: {e}"
    assert oracle_loader.check_table_exists("TEST_CREATE_TABLE") is True


@mark.order(2)
def test_count_rows(oracle_loader):
    try:
        row_count = oracle_loader.count_rows("TEST_CREATE_TABLE")
    except Exception as e:
        assert False, f"Row count failed with exception: {e}"
    assert row_count == 0
    try:
        oracle_loader.insert_data(sample_table, table_name="TEST_CREATE_TABLE")
        row_count = oracle_loader.count_rows("TEST_CREATE_TABLE")
    except Exception as e:
        assert False, f"Row count after insertion failed with exception: {e}"
    assert row_count == 3


@mark.order(3)
def test_create_existing_table(oracle_loader):
    try:
        oracle_loader.create_table("SAMPLE_DATA", sample_table)
    except Exception as e:
        assert True, f"Existing table creation check failed with exception: {e}"
    assert oracle_loader.check_table_exists("SAMPLE_DATA") is True


@mark.order(4)
def test_drop_table(oracle_loader):
    try:
        oracle_loader.drop_table("TEST_CREATE_TABLE")
    except Exception as e:
        assert False, f"Table drop failed with exception: {e}"
    assert oracle_loader.check_table_exists("TEST_CREATE_TABLE") is False


@mark.order(5)
def test_close_connection(oracle_loader):
    oracle_loader.close()
    try:
        oracle_loader.test_connection()
        assert False, "Connection should be closed but test_connection did not raise an exception"
    except Exception as e:
        assert True, f"Connection closure test passed with exception: {e}"
