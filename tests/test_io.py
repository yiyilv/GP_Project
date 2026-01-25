from pathlib import Path
import pandas as pd
import pytest

from atl08kit.io import read_table, write_table


def test_io_read_write_csv(tmp_path: Path):
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    out = tmp_path / "x.csv"
    write_table(df, out)

    df2 = read_table(out)
    assert df2.shape == (2, 2)
    assert df2["a"].tolist() == [1, 2]


def test_io_unsupported_format(tmp_path: Path):
    df = pd.DataFrame({"a": [1]})
    with pytest.raises(ValueError):
        write_table(df, tmp_path / "x.parquet")