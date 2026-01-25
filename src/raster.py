from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence, Union

import numpy as np
import pandas as pd

try:
    import rasterio
except Exception:  # pragma: no cover
    rasterio = None  # type: ignore

PathLike = Union[str, Path]


def _require_rasterio() -> None:
    # Hard dependency for raster sampling / filtering
    if rasterio is None:
        raise ImportError(
            "rasterio is required for raster sampling. Install it first, e.g.\n"
            "  conda install -c conda-forge rasterio\n"
            "or\n"
            "  pip install rasterio"
        )


@dataclass(frozen=True)
class RasterSampleSummary:
    raster_path: str
    N_total: int
    N_nodata: int
    nodata_value: Optional[float]


def sample_raster(
    df: pd.DataFrame,
    raster_path: PathLike,
    *,
    lon: str = "lon",
    lat: str = "lat",
    band: int = 1,
) -> tuple[np.ndarray, RasterSampleSummary]:
    # Sample raster at (lon, lat) points. Returns (values, summary).
    # Note: assumes point coords are in the raster CRS (no reprojection here).
    _require_rasterio()

    rp = Path(raster_path)
    if not rp.exists():
        raise FileNotFoundError(f"Raster not found: {rp}")

    missing = [c for c in (lon, lat) if c not in df.columns]
    if missing:
        raise ValueError(f"Missing coordinate columns: {missing}")

    if df.empty:
        vals = np.array([], dtype=float)
        return vals, RasterSampleSummary(str(rp), 0, 0, None)

    coords = list(zip(df[lon].astype(float).values, df[lat].astype(float).values))

    with rasterio.open(rp) as src:
        nodata = src.nodata
        samples = list(src.sample(coords, indexes=band))
        vals = np.array([s[0] if s is not None and len(s) else np.nan for s in samples], dtype=float)

    # Replace explicit nodata with NaN so downstream logic is consistent
    if nodata is not None:
        vals = np.where(vals == nodata, np.nan, vals)

    n_nodata = int(np.isnan(vals).sum())
    summary = RasterSampleSummary(
        raster_path=str(rp),
        N_total=int(len(vals)),
        N_nodata=n_nodata,
        nodata_value=None if nodata is None else float(nodata),
    )
    return vals, summary


def filter_by_raster(
    df: pd.DataFrame,
    raster_path: PathLike,
    *,
    keep_values: Optional[Iterable[float]] = None,
    drop_values: Optional[Iterable[float]] = None,
    lon: str = "lon",
    lat: str = "lat",
    band: int = 1,
    keep_nodata: bool = False,
) -> tuple[pd.DataFrame, dict]:
    # Filter rows based on sampled raster values.
    # Use either keep_values OR drop_values (not both).
    if keep_values is not None and drop_values is not None:
        raise ValueError("Provide only one of keep_values or drop_values (not both).")

    vals, s = sample_raster(df, raster_path, lon=lon, lat=lat, band=band)

    if df.empty:
        return df.copy(), {"N_total": 0, "N_pass": 0, "pass_rate": 0.0, "N_nodata": 0}

    # Start with nodata policy
    if keep_nodata:
        mask = np.ones(len(df), dtype=bool)
    else:
        mask = ~np.isnan(vals)

    if keep_values is not None:
        kv = set(float(x) for x in keep_values)
        mask = mask & np.array([False if np.isnan(v) else (float(v) in kv) for v in vals], dtype=bool)

    if drop_values is not None:
        dv = set(float(x) for x in drop_values)
        mask = mask & np.array([True if np.isnan(v) else (float(v) not in dv) for v in vals], dtype=bool)

    out = df.loc[mask].copy()

    summary = {
        "raster": str(raster_path),
        "N_total": int(len(df)),
        "N_pass": int(mask.sum()),
        "pass_rate": float(mask.mean()),
        "N_nodata": int(s.N_nodata),
    }
    return out, summary


def add_raster_column(
    df: pd.DataFrame,
    raster_path: PathLike,
    *,
    out_col: str = "raster_val",
    lon: str = "lon",
    lat: str = "lat",
    band: int = 1,
) -> tuple[pd.DataFrame, RasterSampleSummary]:
     # Add sampled raster values as df[out_col]
    vals, s = sample_raster(df, raster_path, lon=lon, lat=lat, band=band)
    out = df.copy()
    out[out_col] = vals
    return out, s