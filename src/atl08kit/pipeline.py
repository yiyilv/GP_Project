from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd

from atl08kit.beams import build_strong_beam_map
from atl08kit.filters import filter_by_expr
from atl08kit.expr import ExprError


@dataclass(frozen=True)
class RunSummary:
    N_in: int
    N_after_beams: int
    N_after_filter: int
    beam_pass_rate: float
    filter_pass_rate: float


def _norm_src_name(src: str) -> str:
    # Strip CSV suffixes so "source_file" matches ATL08 H5 stem
    base = Path(str(src)).stem  # drop .csv
    for suffix in [
        "_Rule8_full",
        "_Rule7_full",
        "_Rule6_full",
        "_Rule5_full",
        "_Rule4_full",
        "_Rule3_full",
        "_Rule2_full",
        "_Rule1_full",
        "_raw_allpoints",
    ]:
        if base.endswith(suffix):
            base = base[: -len(suffix)]
    return base


def apply_strong_beams(df: pd.DataFrame, h5_folder: str | Path) -> Tuple[pd.DataFrame, Dict[str, float]]:
    # Keep rows where (source_file, beam) is a strong beam for that ATL08 file
    needed_cols = {"beam", "source_file"}
    missing = needed_cols - set(df.columns)
    if missing:
        raise ValueError(f"apply_strong_beams: input missing columns: {sorted(missing)}")

    h5_folder = Path(h5_folder)
    if not h5_folder.exists():
        raise FileNotFoundError(f"apply_strong_beams: h5-folder not found: {h5_folder}")

    strong_map = build_strong_beam_map(h5_folder)

    def is_strong_row(row) -> bool:
        key = _norm_src_name(row["source_file"])
        beam = str(row["beam"])
        strong_beams = strong_map.get(key, [])
        return beam in strong_beams

    mask = df.apply(is_strong_row, axis=1)
    out = df.loc[mask].copy()

    summary = {
        "N_total": int(len(df)),
        "N_pass": int(mask.sum()),
        "pass_rate": float(mask.mean()) if len(df) else 0.0,
    }
    return out, summary


def run_pipeline(
    df: pd.DataFrame,
    *,
    h5_folder: Optional[str | Path] = None,
    expr: Optional[str] = None,
) -> Tuple[pd.DataFrame, RunSummary]:
    # Pipeline: optional strong-beam filter -> optional expr filter
    n_in = len(df)

    if h5_folder is not None:
        df_b, bsum = apply_strong_beams(df, h5_folder)
        n_b = len(df_b)
        beam_rate = bsum["pass_rate"]
    else:
        df_b = df
        n_b = n_in
        beam_rate = 1.0 if n_in else 0.0

    if expr is not None:
        try:
            df_f, fsum = filter_by_expr(df_b, expr)
        except ExprError as e:
            raise ExprError(f"run_pipeline: invalid expr: {e}") from e
        n_f = len(df_f)
        filter_rate = fsum["pass_rate"]
    else:
        df_f = df_b
        n_f = n_b
        filter_rate = 1.0 if n_b else 0.0

    summary = RunSummary(
        N_in=int(n_in),
        N_after_beams=int(n_b),
        N_after_filter=int(n_f),
        beam_pass_rate=float(beam_rate),
        filter_pass_rate=float(filter_rate),
    )
    return df_f, summary