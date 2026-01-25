from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

import pandas as pd

try:
    import geopandas as gpd
except Exception:  # pragma: no cover
    gpd = None  # type: ignore

PathLike = Union[str, Path]


def _require_geopandas() -> None:
    if gpd is None:
        raise ImportError(
            "geopandas is required for vector mask filtering. Install it first, e.g.\n"
            "  conda install -c conda-forge geopandas\n"
            "or\n"
            "  pip install geopandas"
        )


@dataclass(frozen=True)
class VectorMaskSummary:
    mask_path: str
    N_total: int
    N_pass: int
    pass_rate: float


def filter_by_polygon(
    df: pd.DataFrame,
    polygon_path: PathLike,
    *,
    lon: str = "lon",
    lat: str = "lat",
    points_crs: str = "EPSG:4326",
    predicate: str = "within",
    invert: bool = False,
) -> tuple[pd.DataFrame, VectorMaskSummary]:
    # filter lon/lat point table by a polygon mask (.shp/.geojson/.gpkg)
    # predicate: "within" or "intersects"
    # invert=True => keep points outside the polygon
    _require_geopandas()

    p = Path(polygon_path)
    if not p.exists():
        raise FileNotFoundError(f"Polygon mask not found: {p}")

    missing = [c for c in (lon, lat) if c not in df.columns]
    if missing:
        raise ValueError(f"Missing coordinate columns: {missing}")

    if df.empty:
        summary = VectorMaskSummary(str(p), 0, 0, 0.0)
        return df.copy(), summary

    # points -> GeoDataFrame
    gdf_pts = gpd.GeoDataFrame(
        df.copy(),
        geometry=gpd.points_from_xy(df[lon], df[lat]),
        crs=points_crs,
    )

    # polygon -> GeoDataFrame
    gdf_poly = gpd.read_file(p)

    # empty polygon: nothing passes (unless invert=True)
    if gdf_poly.empty:
        mask = [True] * len(gdf_pts) if invert else [False] * len(gdf_pts)
    else:
        # combine multi-features into one geometry
        if hasattr(gdf_poly, "union_all"):
            poly = gdf_poly.union_all()
        else: 
            poly = gdf_poly.unary_union

        # align CRS if needed
        if gdf_poly.crs is not None and gdf_pts.crs is not None and gdf_poly.crs != gdf_pts.crs:
            gdf_pts = gdf_pts.to_crs(gdf_poly.crs)

        if predicate == "within":
            m = gdf_pts.geometry.within(poly)
        elif predicate == "intersects":
            m = gdf_pts.geometry.intersects(poly)
        else:
            raise ValueError("predicate must be 'within' or 'intersects'")

        mask = (~m) if invert else m

    out = gdf_pts.loc[mask].drop(columns=["geometry"]).copy()

    N_total = int(len(df))
    N_pass = int(len(out))
    summary = VectorMaskSummary(
        mask_path=str(p),
        N_total=N_total,
        N_pass=N_pass,
        pass_rate=(N_pass / N_total) if N_total else 0.0,
    )
    return out, summary


def drop_points_in_polygon(
    df: pd.DataFrame,
    polygon_path: PathLike,
    *,
    lon: str = "lon",
    lat: str = "lat",
    points_crs: str = "EPSG:4326",
    predicate: str = "within",
) -> tuple[pd.DataFrame, VectorMaskSummary]:
     # drop points inside polygon
    return filter_by_polygon(
        df,
        polygon_path,
        lon=lon,
        lat=lat,
        points_crs=points_crs,
        predicate=predicate,
        invert=True,
    )

def clip_points(
    df: pd.DataFrame,
    polygon_path: PathLike,
    *,
    lon: str = "lon",
    lat: str = "lat",
    points_crs: str = "EPSG:4326",
    predicate: str = "within",
) -> tuple[pd.DataFrame, VectorMaskSummary]:
    # keep only points inside polygon
    return filter_by_polygon(
        df,
        polygon_path,
        lon=lon,
        lat=lat,
        points_crs=points_crs,
        predicate=predicate,
        invert=False,
    )


def exclude_points(
    df: pd.DataFrame,
    polygon_path: PathLike,
    *,
    lon: str = "lon",
    lat: str = "lat",
    points_crs: str = "EPSG:4326",
    predicate: str = "within",
) -> tuple[pd.DataFrame, VectorMaskSummary]:
    # keep only points outside polygon
    return filter_by_polygon(
        df,
        polygon_path,
        lon=lon,
        lat=lat,
        points_crs=points_crs,
        predicate=predicate,
        invert=True,
    )