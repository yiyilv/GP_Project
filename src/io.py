from __future__ import annotations

from pathlib import Path
from typing import Union

import pandas as pd


PathLike = Union[str, Path]


def read_table(path: PathLike) -> pd.DataFrame:
    # Read a CSV file into a DataFrame
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Input file not found: {p}")

    suffix = p.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(p)

    raise ValueError(f"Unsupported input format: {suffix}. Only .csv is supported for now.")


def write_table(df: pd.DataFrame, path: PathLike) -> None:
    # Write a DataFrame to CSV
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    suffix = p.suffix.lower()
    if suffix == ".csv":
        df.to_csv(p, index=False)
        return

    raise ValueError(f"Unsupported output format: {suffix}. Only .csv is supported for now.")