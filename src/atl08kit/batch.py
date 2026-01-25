from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd

from atl08kit.io import read_table, write_table
from atl08kit.filters import filter_by_expr
from atl08kit.beams import build_strong_beam_map, filter_strong_beams
from atl08kit.raster import filter_by_raster
from atl08kit.vector_mask import clip_points, exclude_points
from atl08kit.export import export_points


@dataclass
class BatchRow:
    file: str
    N_in: int
    N_out: int
    pass_rate: float

    vector_used: bool = False
    vector_mask: str = ""
    vector_mode: str = ""
    vector_pass_rate: Optional[float] = None

    raster_used: bool = False
    raster_mask: str = ""
    raster_pass_rate: Optional[float] = None
    raster_nodata: Optional[int] = None

    beams_used: bool = False
    beams_pass_rate: Optional[float] = None

    expr_used: bool = False
    expr_pass_rate: Optional[float] = None

    out_csv: str = ""
    out_export: str = ""


def _extract_yyyymm_from_atl08_name(source_file: str) -> str:
    # Extract YYYYMM from ATL08 filename
    s = Path(str(source_file)).name  # drop directories
    s = s.replace(".h5", "").replace(".csv", "")

    if "ATL08_" not in s:
        raise ValueError(f"Cannot parse YYYYMM: not an ATL08 name: {source_file}")

    try:
        after = s.split("ATL08_", 1)[1]
        yyyymmdd = after[:8]
        yyyymm = yyyymmdd[:6]
        if not (yyyymm.isdigit() and len(yyyymm) == 6):
            raise ValueError
        return yyyymm
    except Exception as e:
        raise ValueError(f"Cannot parse YYYYMM from ATL08 name: {source_file}") from e


def _resolve_monthly_mask(monthly_folder: str, source_file: str, ext: str) -> str:
    yyyymm = _extract_yyyymm_from_atl08_name(source_file)
    return str(Path(monthly_folder) / f"{yyyymm}{ext}")


def batch_process_one(
    in_csv: Path,
    out_dir: Path,
    *,
    expr: Optional[str] = None,
    # vector
    vector_mask: Optional[str] = None,
    vector_mode: str = "clip",
    vector_lon: str = "lon",
    vector_lat: str = "lat",
    vector_crs: str = "EPSG:4326",
    vector_predicate: str = "within",
    vector_monthly_folder: Optional[str] = None,
    vector_source_file: Optional[str] = None,
    vector_ext: str = ".shp",
    # raster
    raster_mask: Optional[str] = None,
    raster_keep: Optional[Sequence[float]] = None,
    raster_drop: Optional[Sequence[float]] = None,
    raster_lon: str = "lon",
    raster_lat: str = "lat",
    raster_band: int = 1,
    raster_keep_nodata: bool = False,
    # beams
    strong_map: Optional[dict] = None,
    use_strong_beams: bool = False,
    # export
    export_ext: Optional[str] = None,
) -> BatchRow:
    df = read_table(in_csv)
    N0 = len(df)
    row = BatchRow(file=in_csv.name, N_in=N0, N_out=N0, pass_rate=1.0)

    # --- vector mask ---
    if vector_mask or vector_monthly_folder:
        row.vector_used = True

        if vector_mask:
            mask_path = vector_mask
        else:
            if not vector_monthly_folder:
                raise ValueError("vector_monthly_folder is required for monthly mode")
            if not vector_source_file:
                raise ValueError("vector_source_file is required for vector monthly mode")
            mask_path = _resolve_monthly_mask(vector_monthly_folder, vector_source_file, vector_ext)

        row.vector_mask = mask_path
        row.vector_mode = vector_mode

        if vector_mode == "clip":
            df, s = clip_points(
                df,
                mask_path,
                lon=vector_lon,
                lat=vector_lat,
                points_crs=vector_crs,
                predicate=vector_predicate,
            )
        elif vector_mode == "exclude":
            df, s = exclude_points(
                df,
                mask_path,
                lon=vector_lon,
                lat=vector_lat,
                points_crs=vector_crs,
                predicate=vector_predicate,
            )
        else:
            raise ValueError("vector_mode must be clip or exclude")

        row.vector_pass_rate = s.pass_rate

    # --- raster mask ---
    if raster_mask:
        row.raster_used = True
        row.raster_mask = raster_mask

        df, s = filter_by_raster(
            df,
            raster_mask,
            keep_values=raster_keep,
            drop_values=raster_drop,
            lon=raster_lon,
            lat=raster_lat,
            band=raster_band,
            keep_nodata=raster_keep_nodata,
        )
        row.raster_pass_rate = float(s["pass_rate"])
        row.raster_nodata = int(s["N_nodata"])

    # --- strong beams ---
    if use_strong_beams:
        if strong_map is None:
            raise ValueError("strong_map is required when use_strong_beams=True")

        row.beams_used = True
        df, s = filter_strong_beams(df, strong_map)
        row.beams_pass_rate = float(s["pass_rate"])

    # --- expr ---
    if expr:
        row.expr_used = True
        df, s = filter_by_expr(df, expr)
        row.expr_pass_rate = float(s["pass_rate"])

    # --- write output csv ---
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / in_csv.name
    write_table(df, out_csv)
    row.out_csv = str(out_csv)

    # --- export ---
    if export_ext:
        export_ext = export_ext.lower()
        out_vec = out_csv.with_suffix(export_ext)
        export_points(df, out_vec, lon=vector_lon, lat=vector_lat, crs=vector_crs)
        row.out_export = str(out_vec)

    # final summary
    row.N_out = int(len(df))
    row.pass_rate = (row.N_out / row.N_in) if row.N_in else 0.0
    return row


def batch_run(
    in_dir: Path,
    out_dir: Path,
    *,
    pattern: str = "*.csv",
    expr: Optional[str] = None,
    # vector
    vector_mask: Optional[str] = None,
    vector_mode: str = "clip",
    vector_monthly_folder: Optional[str] = None,
    vector_source_file: Optional[str] = None,
    vector_ext: str = ".shp",
    vector_lon: str = "lon",
    vector_lat: str = "lat",
    vector_crs: str = "EPSG:4326",
    vector_predicate: str = "within",
    # raster
    raster_mask: Optional[str] = None,
    raster_keep: Optional[Sequence[float]] = None,
    raster_drop: Optional[Sequence[float]] = None,
    raster_lon: str = "lon",
    raster_lat: str = "lat",
    raster_band: int = 1,
    raster_keep_nodata: bool = False,
    # beams
    h5_folder: Optional[Path] = None,
    strong_beams: bool = False,
    # export
    export_ext: Optional[str] = None,
    summary_csv: Optional[Path] = None,
) -> pd.DataFrame:
    in_dir = Path(in_dir)
    out_dir = Path(out_dir)
    files = sorted(in_dir.glob(pattern))

    strong_map = None
    if strong_beams:
        if h5_folder is None:
            raise ValueError("h5_folder is required when strong_beams=True")
        strong_map = build_strong_beam_map(h5_folder)

    rows: list[BatchRow] = []
    for f in files:
        r = batch_process_one(
            f,
            out_dir,
            expr=expr,
            vector_mask=vector_mask,
            vector_mode=vector_mode,
            vector_lon=vector_lon,
            vector_lat=vector_lat,
            vector_crs=vector_crs,
            vector_predicate=vector_predicate,
            vector_monthly_folder=vector_monthly_folder,
            vector_source_file=vector_source_file,
            vector_ext=vector_ext,
            raster_mask=raster_mask,
            raster_keep=raster_keep,
            raster_drop=raster_drop,
            raster_lon=raster_lon,
            raster_lat=raster_lat,
            raster_band=raster_band,
            raster_keep_nodata=raster_keep_nodata,
            strong_map=strong_map,
            use_strong_beams=strong_beams,
            export_ext=export_ext,
        )
        rows.append(r)

    df_sum = pd.DataFrame([asdict(x) for x in rows])
    if summary_csv:
        summary_csv.parent.mkdir(parents=True, exist_ok=True)
        df_sum.to_csv(summary_csv, index=False)
    return df_sum