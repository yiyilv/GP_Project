import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


def _write_tiny_tif(path: Path):
    try:
        import rasterio
        from rasterio.transform import from_origin
    except Exception:
        pytest.skip("rasterio not installed")

    data = np.array([[2, 1],
                     [2, 0]], dtype=np.uint8)

    transform = from_origin(0, 2, 1, 1)
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


def test_cli_raster_keep_value(tmp_path: Path):
    tif = tmp_path / "mask.tif"
    _write_tiny_tif(tif)

    df = pd.DataFrame(
        {"lon": [0.5, 1.5, 0.5, 1.5], "lat": [1.5, 1.5, 0.5, 0.5], "id": [1, 2, 3, 4]}
    )
    in_csv = tmp_path / "in.csv"
    out_csv = tmp_path / "out.csv"
    df.to_csv(in_csv, index=False)

    cmd = [
        sys.executable,
        "-m",
        "atl08kit.cli",
        "raster",
        "--in",
        str(in_csv),
        "--out",
        str(out_csv),
        "--raster",
        str(tif),
        "--keep",
        "2",
        "--summary",
    ]
    res = subprocess.run(cmd, cwd=str(tmp_path), text=True, capture_output=True)
    assert res.returncode == 0, f"STDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}"

    out = pd.read_csv(out_csv)
    assert out["id"].tolist() == [1, 3]