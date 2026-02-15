import pytest

from etl.pipeline import Pipeline


# Simple mock DataFrame with the required to_arrow method
class MockDF:
    def __init__(self, name):
        self.name = name

    def to_arrow(self):
        # Return a simple placeholder; the loader mock will just receive this
        return f"arrow-{self.name}"


# Mock extractor that yields a fixed set of (key, df) pairs
class MockExtractor:
    def __init__(self, data):
        self._data = data

    def generate(self):
        for item in self._data:
            yield item


# Mock loader that records calls to rebuild_table and reload_table
class MockLoader:
    def __init__(self):
        self.rebuild_calls = []
        self.reload_calls = []

    def rebuild_table(self, table_name, data):
        self.rebuild_calls.append((table_name, data))

    def reload_table(self, table_name, data):
        self.reload_calls.append((table_name, data))


@pytest.fixture
def pipeline_with_mocks():
    # Prepare mock data: two keys with simple MockDF objects
    mock_data = [("events", MockDF("events")), ("players", MockDF("players"))]
    extractor = MockExtractor(mock_data)
    loader = MockLoader()
    # Inject mocks into a Pipeline instance
    pipeline = Pipeline()
    pipeline.extractor = extractor
    pipeline.loader = loader
    return pipeline, loader


def test_rebuild_calls(pipeline_with_mocks):
    pipeline, loader = pipeline_with_mocks
    pipeline.rebuild()  # default prefix "FPL"
    # Expect two rebuild_table calls with correct table names and data
    assert len(loader.rebuild_calls) == 2
    expected_names = ["FPL_EVENTS", "FPL_PLAYERS"]
    for (tbl, data), exp_name in zip(loader.rebuild_calls, expected_names):
        assert tbl == exp_name
        assert data.startswith("arrow-")


def test_reload_calls_custom_prefix(pipeline_with_mocks):
    pipeline, loader = pipeline_with_mocks
    pipeline.reload(table_prefix="CUSTOM")
    assert len(loader.reload_calls) == 2
    expected_names = ["CUSTOM_EVENTS", "CUSTOM_PLAYERS"]
    for (tbl, data), exp_name in zip(loader.reload_calls, expected_names):
        assert tbl == exp_name
        assert data.startswith("arrow-")


def test_no_calls_when_no_data(monkeypatch):
    # Replace the extractor with one that yields nothing
    empty_extractor = MockExtractor([])
    mock_loader = MockLoader()
    pipeline = Pipeline()
    pipeline.extractor = empty_extractor
    pipeline.loader = mock_loader
    pipeline.rebuild()
    pipeline.reload()
    assert mock_loader.rebuild_calls == []
    assert mock_loader.reload_calls == []
