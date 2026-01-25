from __future__ import annotations

import argparse
from pathlib import Path
import re
import sys

from atl08kit.expr import ExprError
from atl08kit.filters import filter_by_expr
from atl08kit.beams import build_strong_beam_map
from atl08kit.pipeline import run_pipeline
from atl08kit.io import read_table, write_table
from atl08kit.export import export_points
from atl08kit.raster import filter_by_raster
from atl08kit.vector_mask import clip_points, exclude_points
from atl08kit.water import extract_yyyymm_from_atl08_name
from atl08kit.batch import batch_run
from atl08kit.atl08 import list_land_segment_fields, read_atl08_h5


_ATL08_RE = re.compile(r"(ATL08_\d{14}_\d+_\d+_\d+)", re.IGNORECASE)


def cmd_version(_: argparse.Namespace) -> int:
    print("atl08kit 0.1.0")
    return 0


def cmd_fields(args: argparse.Namespace) -> int:
    try:
        fields = list_land_segment_fields(args.h5, beam=args.beam)
    except Exception as e:
        print(f"[atl08kit] fields error: {e}", file=sys.stderr)
        return 2

    for x in fields:
        print(x)
    return 0


def cmd_extract(args: argparse.Namespace) -> int:
    try:
        df, s = read_atl08_h5(
            args.h5,
            beams=args.beams,
            strong_only=args.strong_only,
            fields=args.fields,
            add_source_file=True,
            source_col="source_file",
        )
        write_table(df, args.output)
    except Exception as e:
        print(f"[atl08kit] extract error: {e}", file=sys.stderr)
        return 2

    if args.summary:
        print(
            f"[atl08kit] extract summary: N_total={s.N_total} N_out={s.N_out} pass_rate={s.pass_rate:.3f} "
            f"beams_used={len(s.beams_used)} fields_used={len(s.fields_used)} fields_skipped={len(s.fields_skipped)}"
        )
    return 0


def cmd_filter(args: argparse.Namespace) -> int:
    try:
        df = read_table(args.input)
        out, summary = filter_by_expr(df, args.expr)
        write_table(out, args.output)
    except FileNotFoundError as e:
        print(f"[atl08kit] filter error: {e}", file=sys.stderr)
        return 2
    except ExprError as e:
        print(f"[atl08kit] expression error: {e}", file=sys.stderr)
        return 2
    except Exception as e:
        print(f"[atl08kit] filter error: {e}", file=sys.stderr)
        return 2

    if args.summary:
        print(
            f"[atl08kit] filter summary: N_total={summary['N_total']} "
            f"N_pass={summary['N_pass']} pass_rate={summary['pass_rate']:.3f}"
        )
    return 0


def _norm_src_to_atl08_stem(src: str) -> str:
     # Extract ATL08 stem from source_file (regex). Fallback: Path(src).stem.
    s = str(src)
    m = _ATL08_RE.search(s)
    if m:
        return m.group(1)
    return Path(s).stem


def cmd_beams(args: argparse.Namespace) -> int:
    # Filter CSV to strong beams using sc_orient read from ATL08 H5 files.
    # Requires columns: beam, source_file.
    try:
        df = read_table(args.input)
    except Exception as e:
        print(f"[atl08kit] beams error: {e}", file=sys.stderr)
        return 2

    needed_cols = {"beam", "source_file"}
    missing = needed_cols - set(df.columns)
    if missing:
        print(f"[atl08kit] beams: input CSV missing columns: {sorted(missing)}", file=sys.stderr)
        return 2

    h5_folder = Path(args.h5_folder)
    if not h5_folder.exists():
        print(f"[atl08kit] beams: h5-folder not found: {h5_folder}", file=sys.stderr)
        return 2

    try:
        strong_map = build_strong_beam_map(h5_folder)
    except Exception as e:
        print(f"[atl08kit] beams error: failed to read H5 folder: {e}", file=sys.stderr)
        return 2

    def is_strong_row(row) -> bool:
        key = _norm_src_to_atl08_stem(row["source_file"])
        beam = str(row["beam"])
        strong_beams = strong_map.get(key, [])
        return beam in strong_beams

    try:
        mask = df.apply(is_strong_row, axis=1)
        out = df.loc[mask].copy()
        write_table(out, args.output)
    except Exception as e:
        print(f"[atl08kit] beams error: {e}", file=sys.stderr)
        return 2

    if args.summary:
        total = len(df)
        kept = int(mask.sum())
        rate = (kept / total) if total else 0.0
        print(f"[atl08kit] beams summary: N_total={total} N_pass={kept} pass_rate={rate:.3f}")
    return 0


def cmd_raster(args: argparse.Namespace) -> int:
    try:
        df = read_table(args.input)

        keep_values = [float(x) for x in args.keep] if args.keep else None
        drop_values = [float(x) for x in args.drop] if args.drop else None

        out, summary = filter_by_raster(
            df,
            args.raster,
            keep_values=keep_values,
            drop_values=drop_values,
            lon=args.lon,
            lat=args.lat,
            band=args.band,
            keep_nodata=args.keep_nodata,
        )
        write_table(out, args.output)
    except Exception as e:
        print(f"[atl08kit] raster error: {e}", file=sys.stderr)
        return 2

    if args.summary:
        print(
            f"[atl08kit] raster summary: N_total={summary['N_total']} "
            f"N_pass={summary['N_pass']} pass_rate={summary['pass_rate']:.3f} "
            f"N_nodata={summary['N_nodata']} raster={summary['raster']}"
        )
    return 0


def cmd_vector(args: argparse.Namespace) -> int:
    try:
        df = read_table(args.input)
    except Exception as e:
        print(f"[atl08kit] vector error: {e}", file=sys.stderr)
        return 2

    if bool(args.mask) == bool(args.monthly_folder):
        print("[atl08kit] vector: provide either --mask OR --monthly-folder + --source-file", file=sys.stderr)
        return 2

    if args.monthly_folder and not args.source_file:
        print("[atl08kit] vector monthly: --source-file is required when using --monthly-folder", file=sys.stderr)
        return 2

    if args.mask:
        mask_path = args.mask
        yyyymm = None
    else:
        yyyymm = extract_yyyymm_from_atl08_name(args.source_file)
        mask_path = str(Path(args.monthly_folder) / f"{yyyymm}{args.ext}")

    try:
        if args.mode == "clip":
            out, summary = clip_points(
                df,
                mask_path,
                lon=args.lon,
                lat=args.lat,
                points_crs=args.crs,
                predicate=args.predicate,
            )
        else:
            out, summary = exclude_points(
                df,
                mask_path,
                lon=args.lon,
                lat=args.lat,
                points_crs=args.crs,
                predicate=args.predicate,
            )
        write_table(out, args.output)
    except Exception as e:
        print(f"[atl08kit] vector error: {e}", file=sys.stderr)
        return 2

    if args.summary:
        if yyyymm is not None:
            print(f"[atl08kit] vector monthly: source_file={args.source_file} yyyymm={yyyymm} mask={mask_path}")
        print(
            f"[atl08kit] vector summary: mode={args.mode} "
            f"N_total={summary.N_total} N_pass={summary.N_pass} "
            f"pass_rate={summary.pass_rate:.3f} mask={summary.mask_path}"
        )
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    try:
        df = read_table(args.input)
        h5_folder = args.h5_folder  # None if not provided
        expr = args.expr or None

        out, summary = run_pipeline(df, h5_folder=h5_folder, expr=expr)
        write_table(out, args.output)
    except Exception as e:
        print(f"[atl08kit] run error: {e}", file=sys.stderr)
        return 2

    if args.summary:
        print(
            "[atl08kit] run summary: "
            f"N_in={summary.N_in} "
            f"N_after_beams={summary.N_after_beams} "
            f"N_after_filter={summary.N_after_filter} "
            f"beam_pass_rate={summary.beam_pass_rate:.3f} "
            f"filter_pass_rate={summary.filter_pass_rate:.3f}"
        )
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    try:
        df = read_table(args.input)
        export_points(
            df,
            args.output,
            lon=args.lon,
            lat=args.lat,
            crs=args.crs,
        )
    except Exception as e:
        print(f"[atl08kit] export error: {e}", file=sys.stderr)
        return 2

    if args.summary:
        print(
            f"[atl08kit] export summary: N_total={len(df)} "
            f"out={args.output} crs={args.crs} lon={args.lon} lat={args.lat}"
        )
    return 0


def cmd_batch(args: argparse.Namespace) -> int:
    raster_keep = [float(x) for x in args.raster_keep] if args.raster_keep else None
    raster_drop = [float(x) for x in args.raster_drop] if args.raster_drop else None

    if args.vector_mask and args.vector_monthly_folder:
        print("[atl08kit] batch: use either --vector-mask OR --vector-monthly-folder (not both)", file=sys.stderr)
        return 2

    if args.vector_monthly_folder and not args.vector_source_file:
        print("[atl08kit] batch: --vector-source-file is required with --vector-monthly-folder", file=sys.stderr)
        return 2

    if args.strong_beams and not args.h5_folder:
        print("[atl08kit] batch: --h5-folder is required with --strong-beams", file=sys.stderr)
        return 2

    try:
        df_sum = batch_run(
            Path(args.in_dir),
            Path(args.out_dir),
            pattern=args.pattern,
            expr=args.expr,
            # vector
            vector_mask=args.vector_mask,
            vector_mode=args.vector_mode,
            vector_monthly_folder=args.vector_monthly_folder,
            vector_source_file=args.vector_source_file,
            vector_ext=args.vector_ext,
            vector_lon=args.lon,
            vector_lat=args.lat,
            vector_crs=args.crs,
            vector_predicate=args.predicate,
            # raster
            raster_mask=args.raster_mask,
            raster_keep=raster_keep,
            raster_drop=raster_drop,
            raster_lon=args.lon,
            raster_lat=args.lat,
            raster_band=args.raster_band,
            raster_keep_nodata=args.raster_keep_nodata,
            # beams
            h5_folder=Path(args.h5_folder) if args.h5_folder else None,
            strong_beams=args.strong_beams,
            # export
            export_ext=args.export,
            summary_csv=Path(args.summary) if args.summary else None,
        )
    except Exception as e:
        print(f"[atl08kit] batch error: {e}", file=sys.stderr)
        return 2

    if args.print_summary:
        print(df_sum.to_string(index=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atl08kit", description="ATL08 toolkit (MVP).")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # extract
    p_ext = sub.add_parser("extract", help="Extract ATL08 land_segments point table from H5 to CSV")
    p_ext.add_argument("--h5", required=True, help="Input ATL08 .h5 file")
    p_ext.add_argument("--out", dest="output", required=True, help="Output CSV file")
    p_ext.add_argument("--beams", nargs="*", default=None, help="Beams to extract, e.g. gt2l gt2r")
    p_ext.add_argument("--strong-only", action="store_true", help="Keep only strong beams (based on sc_orient)")
    p_ext.add_argument(
        "--fields",
        nargs="*",
        default=None,
        help="Fields to extract (default: all aligned 1D).",
    )
    p_ext.add_argument("--summary", action="store_true", help="Print extraction summary")
    p_ext.set_defaults(func=cmd_extract)

    # fields
    p_fields = sub.add_parser("fields", help="List available land_segments fields in an ATL08 H5 file")
    p_fields.add_argument("--h5", required=True, help="Input ATL08 .h5 file")
    p_fields.add_argument("--beam", default="gt2l", help="Beam to inspect (default: gt2l)")
    p_fields.set_defaults(func=cmd_fields)

    # version
    p_ver = sub.add_parser("version", help="Show version")
    p_ver.set_defaults(func=cmd_version)

    # filter
    p_filter = sub.add_parser("filter", help="Filter a table by a safe boolean expression")
    p_filter.add_argument("--in", dest="input", required=True, help="Input CSV file")
    p_filter.add_argument("--out", dest="output", required=True, help="Output CSV file")
    p_filter.add_argument("--expr", required=True, help='Filtering expression, e.g. "abs(dem_h-h_te_best_fit)<=3"')
    p_filter.add_argument("--summary", action="store_true", help="Print filtering summary")
    p_filter.set_defaults(func=cmd_filter)

    # beams
    p_beams = sub.add_parser("beams", help="Keep only strong-beam rows using sc_orient from ATL08 H5 files")
    p_beams.add_argument("--in", dest="input", required=True, help="Input CSV file (must contain beam, source_file)")
    p_beams.add_argument("--out", dest="output", required=True, help="Output CSV file")
    p_beams.add_argument("--h5-folder", required=True, help="Folder containing ATL08_*.h5 files")
    p_beams.add_argument("--summary", action="store_true", help="Print filtering summary")
    p_beams.set_defaults(func=cmd_beams)

    # run (pipeline)
    p_run = sub.add_parser("run", help="Run pipeline: optional strong-beam filter + optional expr filter")
    p_run.add_argument("--in", dest="input", required=True, help="Input CSV file")
    p_run.add_argument("--out", dest="output", required=True, help="Output CSV file")
    p_run.add_argument("--h5-folder", default=None, help="(Optional) ATL08 H5 folder for strong-beam filtering")
    p_run.add_argument("--expr", default="", help="(Optional) Filtering expression")
    p_run.add_argument("--summary", action="store_true", help="Print pipeline summary")
    p_run.set_defaults(func=cmd_run)

    # export
    p_export = sub.add_parser("export", help="Export a CSV point table to GeoJSON / GPKG / Shapefile")
    p_export.add_argument("--in", dest="input", required=True, help="Input CSV file")
    p_export.add_argument("--out", dest="output", required=True, help="Output vector file (.geojson/.gpkg/.shp)")
    p_export.add_argument("--lon", default="lon", help="Longitude column name (default: lon)")
    p_export.add_argument("--lat", default="lat", help="Latitude column name (default: lat)")
    p_export.add_argument("--crs", default="EPSG:4326", help="CRS of input coordinates (default: EPSG:4326)")
    p_export.add_argument("--summary", action="store_true", help="Print export summary")
    p_export.set_defaults(func=cmd_export)

    # raster
    p_ras = sub.add_parser("raster", help="Filter points by sampling a raster mask/value")
    p_ras.add_argument("--in", dest="input", required=True, help="Input CSV file")
    p_ras.add_argument("--out", dest="output", required=True, help="Output CSV file")
    p_ras.add_argument("--raster", required=True, help="Raster path (e.g., GeoTIFF)")
    p_ras.add_argument("--keep", nargs="*", default=[], help="Keep points whose raster value is in this list")
    p_ras.add_argument("--drop", nargs="*", default=[], help="Drop points whose raster value is in this list")
    p_ras.add_argument("--lon", default="lon", help="Longitude column name (default: lon)")
    p_ras.add_argument("--lat", default="lat", help="Latitude column name (default: lat)")
    p_ras.add_argument("--band", type=int, default=1, help="Raster band index (default: 1)")
    p_ras.add_argument("--keep-nodata", action="store_true", help="Keep points with nodata/NaN samples")
    p_ras.add_argument("--summary", action="store_true", help="Print filtering summary")
    p_ras.set_defaults(func=cmd_raster)

    # vector
    p_vec = sub.add_parser("vector", help="Filter points using a vector polygon mask (AOI clip/exclude)")
    p_vec.add_argument("--in", dest="input", required=True, help="Input CSV file")
    p_vec.add_argument("--out", dest="output", required=True, help="Output CSV file")
    p_vec.add_argument("--mask", default=None, help="Polygon mask path (.shp/.geojson/.gpkg)")

    # monthly mode (optional)
    p_vec.add_argument("--monthly-folder", default=None, help="Folder containing monthly masks named YYYYMM + ext")
    p_vec.add_argument("--source-file", default=None, help="ATL08 filename/stem used to infer YYYYMM")
    p_vec.add_argument("--ext", default=".shp", help="Monthly mask extension (default: .shp)")

    p_vec.add_argument("--mode", choices=["clip", "exclude"], default="clip", help="clip=keep inside, exclude=keep outside")
    p_vec.add_argument("--lon", default="lon", help="Longitude column name (default: lon)")
    p_vec.add_argument("--lat", default="lat", help="Latitude column name (default: lat)")
    p_vec.add_argument("--crs", default="EPSG:4326", help="CRS of input points (default: EPSG:4326)")
    p_vec.add_argument("--predicate", choices=["within", "intersects"], default="within", help="Spatial predicate")
    p_vec.add_argument("--summary", action="store_true", help="Print summary")
    p_vec.set_defaults(func=cmd_vector)

    # batch
    p_b = sub.add_parser("batch", help="Batch process a folder of CSV point tables")
    p_b.add_argument("--in-dir", required=True, help="Input folder containing CSV files")
    p_b.add_argument("--out-dir", required=True, help="Output folder")
    p_b.add_argument("--pattern", default="*.csv", help="Glob pattern (default: *.csv)")
    p_b.add_argument("--expr", default=None, help="Optional expression filter")

    # vector mask
    p_b.add_argument("--vector-mask", default=None, help="Vector polygon mask path (single mask)")
    p_b.add_argument("--vector-mode", choices=["clip", "exclude"], default="clip")
    p_b.add_argument("--vector-monthly-folder", default=None, help="Folder with monthly masks named YYYYMM + ext")
    p_b.add_argument("--vector-source-file", default=None, help="ATL08 filename/stem used to infer YYYYMM")
    p_b.add_argument("--vector-ext", default=".shp", help="Monthly mask extension (default: .shp)")
    p_b.add_argument("--predicate", choices=["within", "intersects"], default="within")

    # raster mask
    p_b.add_argument("--raster-mask", default=None, help="Raster mask path")
    p_b.add_argument("--raster-keep", nargs="*", default=None, help="Keep values (space separated)")
    p_b.add_argument("--raster-drop", nargs="*", default=None, help="Drop values (space separated)")
    p_b.add_argument("--raster-band", type=int, default=1)
    p_b.add_argument("--raster-keep-nodata", action="store_true")

    # beams
    p_b.add_argument("--strong-beams", action="store_true", help="Filter strong beams (requires --h5-folder)")
    p_b.add_argument("--h5-folder", default=None, help="Folder containing ATL08_*.h5 files")

    # common coordinates
    p_b.add_argument("--lon", default="lon")
    p_b.add_argument("--lat", default="lat")
    p_b.add_argument("--crs", default="EPSG:4326")

    # export + summary
    p_b.add_argument("--export", default=None, help="Export extension like .geojson/.gpkg/.shp")
    p_b.add_argument("--summary", default=None, help="Write summary CSV to this path")
    p_b.add_argument("--print-summary", action="store_true", help="Print summary table to stdout")
    p_b.set_defaults(func=cmd_batch)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    rc = args.func(args)
    raise SystemExit(rc)


if __name__ == "__main__":
    main()