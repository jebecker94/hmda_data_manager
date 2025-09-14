import pytest
from pathlib import Path

pytest.importorskip("polars")
import polars as pl
from hmda_data_manager import get_file_schema


def test_polars_schema_returns_expected_types() -> None:
    schema = get_file_schema(Path("schemas/hmda_lar_schema_post2018.html"), "polars")
    assert schema["activity_year"] == pl.Int16
    assert schema["lei"] == pl.String
