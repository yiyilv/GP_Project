from pathlib import Path

import pandas as pd
import pytest

from atl08kit.vector_mask import filter_by_polygon


def test_filter_by_polygon_geojson(tmp_path: Path):
    try:
        import geopandas as gpd
        from shapely.geometry import Polygon
    except Exception:
        pytest.skip("geopandas/shapely not installed")

    # AOI square: [0,2] x [0,2]
    poly = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])
    gdf = gpd.GeoDataFrame({"id": [1]}, geometry=[poly], crs="EPSG:4326")
    mask_path = tmp_path / "mask.geojson"
    gdf.to_file(mask_path, driver="GeoJSON")

    df = pd.DataFrame(
        {"lon": [1.0, 3.0], "lat": [1.0, 1.0], "name": ["inside", "outside"]}
    )

    out, summary = filter_by_polygon(df, mask_path, lon="lon", lat="lat", predicate="within")
    assert out["name"].tolist() == ["inside"]
    assert summary.N_total == 2
    assert summary.N_pass == 1