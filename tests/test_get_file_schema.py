import pytest
from pathlib import Path


def test_polars_schema_returns_expected_types() -> None:
    pytest.importorskip("pandas")
    pl = pytest.importorskip("polars")
    from hmda_data_manager import get_file_schema

    schema = get_file_schema(Path("schemas/hmda_lar_schema_post2018.html"), "polars")
    assert schema["activity_year"] == pl.Int16
    assert schema["lei"] == pl.String
