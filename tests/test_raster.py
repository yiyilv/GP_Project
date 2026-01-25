from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from atl08kit.raster import add_raster_column, filter_by_raster


def _write_tiny_tif(path: Path):
    try:
        import rasterio
        from rasterio.transform import from_origin
    except Exception:
        pytest.skip("rasterio not installed")

    # Create a 2x2 raster:
    # top-left=2, top-right=1
    # bottom-left=2, bottom-right=0
    data = np.array([[2, 1],
                     [2, 0]], dtype=np.uint8)

    transform = from_origin(0, 2, 1, 1)  # origin x=0,y=2; pixel size 1x1
    profile = {
        "driver": "GTiff",
        "height": 2,
        "width": 2,
        "count": 1,
        "dtype": "uint8",
        "crs": "EPSG:4326",
        "transform": transform,
        "nodata": 255,
    }

    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data, 1)


def test_add_raster_column_and_filter(tmp_path: Path):
    tif = tmp_path / "mask.tif"
    _write_tiny_tif(tif)

    # Points inside the 2x2 raster footprint:
    # cell centers roughly:
    # (0.5,1.5)->2, (1.5,1.5)->1, (0.5,0.5)->2, (1.5,0.5)->0
    df = pd.DataFrame(
        {
            "lon": [0.5, 1.5, 0.5, 1.5],
            "lat": [1.5, 1.5, 0.5, 0.5],
            "id": [1, 2, 3, 4],
        }
    )

    df2, _ = add_raster_column(df, tif, out_col="mask", lon="lon", lat="lat")
    assert "mask" in df2.columns
    assert df2["mask"].tolist() == [2.0, 1.0, 2.0, 0.0]

    # Keep only value 2
    out, summary = filter_by_raster(df, tif, keep_values=[2], lon="lon", lat="lat")
    assert len(out) == 2
    assert out["id"].tolist() == [1, 3]
    assert summary["N_total"] == 4
    assert summary["N_pass"] == 2