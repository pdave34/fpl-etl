"""ETL extractors for reading data from APIs.

This module defines an `APIMetrics` dataclass for recording request and
response timing information (timestamps and elapsed time in milliseconds),
and an `APIExtractor` that returns both the parsed JSON payload and the
associated metrics.
"""

import logging
import uuid
from datetime import datetime, timezone
from logging import Logger
from typing import Generator, Optional
from urllib.parse import urljoin

import polars as pl
import requests
from polars import DataFrame
from requests import Response

from etl.utils import clean_df


class APIExtractor:
    """Extractor for fetching data from APIs and recording metrics.

    The `fetch_data` method returns a tuple of the parsed JSON response and
    an `APIMetrics` instance with timing and status information.
    """

    def __init__(self) -> None:
        """Initialize the BaseLoader with logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )
        self.logger: Logger = logging.getLogger(name=__name__)

    def stamp(self) -> dict:
        return {
            "request_id": str(uuid.uuid4()),
            "request_time": datetime.now(timezone.utc).isoformat(sep=" ", timespec="milliseconds"),
        }

    def get(self, url: str, *args, **kwargs) -> dict:
        stamp: dict = self.stamp()
        response: Response = requests.get(url, *args, **kwargs)
        stamp["response"] = response
        stamp["response_code"] = response.status_code
        stamp["response_elapsed_ms"] = response.elapsed.total_seconds() * 1000
        return stamp

    def parse(self, stamp: dict, key: Optional[str]) -> DataFrame:
        if stamp["response_code"] == 200:
            if key is None:
                stamp["data"] = stamp["response"].json()
            else:
                stamp[key] = stamp["response"].json().get(key)
        else:
            stamp["data"] = None
        del stamp["response"]
        # Build DataFrame based on response success and key presence
        if stamp["response_code"] == 200 and key is not None:
            data = stamp.get(key) or []
            df = pl.DataFrame(data)
        else:
            # Empty DataFrame for failure or missing key, but include metric columns (no rows)
            df = pl.DataFrame(
                {
                    "request_id": [],
                    "request_time": [],
                    "response_code": [],
                    "response_elapsed_ms": [],
                }
            )
        df = clean_df(df)
        # Enrich with metric columns; works for empty DataFrames as well
        df = (
            df.with_columns(pl.lit(stamp["request_id"]).alias("request_id"))
            .with_columns(pl.lit(stamp["request_time"]).alias("request_time"))
            .with_columns(pl.lit(stamp["response_code"]).alias("response_code"))
            .with_columns(pl.lit(stamp["response_elapsed_ms"]).alias("response_elapsed_ms"))
        )
        return df

    def save(self, df: DataFrame, filename: str) -> None:
        df.write_parquet(file=filename)


class FPL(APIExtractor):
    def __init__(self) -> None:
        super().__init__()
        self.base = "https://fantasy.premierleague.com/api/"
        self.endpoints: dict = {
            "bootstrap-static": [
                "events",
                # "game_settings",
                "phases",
                "teams",
                # "total_players",
                "elements",
            ],
            "fixtures": [],
        }

    def generate(self) -> Generator[tuple[str, DataFrame], None, None]:
        for e, keys in self.endpoints.items():
            url: str = urljoin(self.base, e)
            if len(keys) == 0:
                yield e, self.parse(self.get(url), key=None)
            for k in keys:
                yield k, self.parse(self.get(url), key=k)
