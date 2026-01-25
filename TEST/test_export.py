from pathlib import Path

import pandas as pd
import pytest

from atl08kit.export import export_points


def test_export_geojson(tmp_path: Path):
    # skip if geopandas is missing
    try:
        import geopandas
    except Exception:
        pytest.skip("geopandas not installed")

    df = pd.DataFrame({"lon": [0.0, 1.0], "lat": [0.0, 1.0], "v": [10, 20]})
    out = tmp_path / "points.geojson"

    export_points(df, out, lon="lon", lat="lat", crs="EPSG:4326")
    assert out.exists()
    assert out.stat().st_size > 0