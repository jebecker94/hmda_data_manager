"""Tests for utilities in :mod:`hmda_data_manager.core.import_data.common`."""

import polars as pl

from hmda_data_manager.core.import_data.common import cast_integer_like_floats


def test_cast_integer_like_floats_promotes_integral_columns():
    lf = pl.DataFrame(
        {
            "float_ints": [1.0, 2.0, None],
            "float_mix": [0.0, 1.5, 2.0],
        }
    ).lazy()

    schema = {"float_ints": pl.Int32, "float_mix": pl.Float64}

    result = cast_integer_like_floats(lf, schema).collect()

    assert result.schema["float_ints"] == pl.Int32
    assert result.schema["float_mix"] == pl.Float64


def test_cast_integer_like_floats_defaults_to_int64_when_missing_schema():
    lf = pl.DataFrame({"only_ints": [0.0, 1.0]}).lazy()

    result = cast_integer_like_floats(lf, {}).collect()

    assert result.schema["only_ints"] == pl.Int64
