from __future__ import annotations

from pathlib import Path
from typing import Optional, Union

import pandas as pd

try:
    import geopandas as gpd
except Exception as e:  # pragma: no cover
    gpd = None  # type: ignore

PathLike = Union[str, Path]


def _require_geopandas():
    if gpd is None:
        raise ImportError(
            "geopandas is required for vector export. Install it first, e.g.\n"
            "  conda install -c conda-forge geopandas\n"
            "or\n"
            "  pip install geopandas"
        )


def to_geodataframe(
    df: pd.DataFrame,
    *,
    lon: str = "lon",
    lat: str = "lat",
    crs: str = "EPSG:4326",
) -> "gpd.GeoDataFrame":
    # DataFrame (lon/lat) -> GeoDataFrame (Point geometry)
    _require_geopandas()

    missing = [c for c in (lon, lat) if c not in df.columns]
    if missing:
        raise ValueError(f"Missing coordinate columns: {missing}")

    gdf = gpd.GeoDataFrame(
        df.copy(),
        geometry=gpd.points_from_xy(df[lon], df[lat]),
        crs=crs,
    )
    return gdf


def export_points(
    df: pd.DataFrame,
    out_path: PathLike,
    *,
    lon: str = "lon",
    lat: str = "lat",
    crs: str = "EPSG:4326",
    driver: Optional[str] = None,
) -> Path:
     # Export point table to .geojson / .gpkg / .shp
    _require_geopandas()

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    ext = out_path.suffix.lower()

    # choose driver based on extension if not provided
    if driver is None:
        if ext in [".geojson", ".json"]:
            driver = "GeoJSON"
        elif ext == ".gpkg":
            driver = "GPKG"
        elif ext == ".shp":
            driver = "ESRI Shapefile"
        else:
            raise ValueError(
                f"Unsupported export format: {ext}. "
                "Supported: .geojson, .gpkg, .shp"
            )

    gdf = to_geodataframe(df, lon=lon, lat=lat, crs=crs)

    # For GeoPackage, geopandas needs a layer name; default is fine but explicit is clearer.
    if driver == "GPKG":
        gdf.to_file(out_path, driver=driver, layer="points")
    else:
        gdf.to_file(out_path, driver=driver)

    return out_path