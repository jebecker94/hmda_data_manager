"""Tests for the :func:`save_file_to_stata` utility."""

import pytest


def test_save_file_to_stata_creates_dta(tmp_path, monkeypatch) -> None:
    """Ensure the converter writes a ``.dta`` file when given a ``Path``."""

    pd = pytest.importorskip("pandas")
    from hmda_data_manager import save_file_to_stata, import_support_functions

    # Create a simple Parquet file
    df = pd.DataFrame({"a": [1, 2]})
    parquet_path = tmp_path / "sample.parquet"
    df.to_parquet(parquet_path)

    # Stub out label preparation to avoid missing resource files
    def fake_prepare(df):
        return df, {}, {}

    monkeypatch.setattr(
        import_support_functions, "prepare_hmda_for_stata", fake_prepare
    )

    save_file_to_stata(parquet_path)

    assert (tmp_path / "sample.dta").is_file()
