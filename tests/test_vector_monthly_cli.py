import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest


def test_cli_vector_monthly_clip(tmp_path: Path):
    try:
        import geopandas as gpd
        from shapely.geometry import Polygon
    except Exception:
        pytest.skip("geopandas/shapely not installed")

    masks = tmp_path / "masks"
    masks.mkdir()

    # AOI for YYYYMM=202201 -> masks/202201.geojson
    poly = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])
    gdf = gpd.GeoDataFrame({"id": [1]}, geometry=[poly], crs="EPSG:4326")
    mask_path = masks / "202201.geojson"
    gdf.to_file(mask_path, driver="GeoJSON")

    df = pd.DataFrame({"lon": [1.0, 3.0], "lat": [1.0, 1.0], "pid": [1, 2]})
    in_csv = tmp_path / "in.csv"
    out_csv = tmp_path / "out.csv"
    df.to_csv(in_csv, index=False)

    cmd = [
        sys.executable, "-m", "atl08kit.cli",
        "vector",
        "--in", str(in_csv),
        "--out", str(out_csv),
        "--monthly-folder", str(masks),
        "--source-file", "ATL08_20220102113835_01711406_007_01.h5",
        "--ext", ".geojson",
        "--mode", "clip",
        "--summary",
    ]
    res = subprocess.run(cmd, cwd=str(tmp_path), text=True, capture_output=True)
    assert res.returncode == 0, f"STDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}"

    out = pd.read_csv(out_csv)
    assert out["pid"].tolist() == [1]