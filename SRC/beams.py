from __future__ import annotations

from pathlib import Path
import h5py
from typing import Dict, List, Tuple, Any
import pandas as pd


def get_strong_beams(sc_orient: int) -> List[str]:
    # Strong beams depend on /orbit_info/sc_orient (0: left, 1: right).
    if sc_orient == 0:
        return ["gt1l", "gt2l", "gt3l"]
    if sc_orient == 1:
        return ["gt1r", "gt2r", "gt3r"]
    return []


def get_strong_beams_from_h5(h5_path: str | Path) -> List[str]:
    # Read sc_orient from an ATL08 H5 and return strong beam ids.
    h5_path = Path(h5_path)
    with h5py.File(h5_path, "r") as f:
        sc_orient = int(f["/orbit_info/sc_orient"][0])
    return get_strong_beams(sc_orient)


def build_strong_beam_map(folder: str | Path) -> Dict[str, List[str]]:
    # Map ATL08 filename stem -> strong beam ids.
    folder = Path(folder)
    strong_map: Dict[str, List[str]] = {}

    for p in sorted(folder.glob("ATL08_*.h5")):
        strong_map[p.stem] = get_strong_beams_from_h5(p)

    return strong_map

def norm_src_name(src: str) -> str:
    # Normalize source_file values to match ATL08 H5 stems.
    base = Path(str(src)).stem  # drop .csv if any
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


def filter_strong_beams(
    df: pd.DataFrame,
    strong_map: Dict[str, List[str]],
    *,
    beam_col: str = "beam",
    source_col: str = "source_file",
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    # Keep only rows whose beam is strong for the corresponding ATL08 file.
    need = {beam_col, source_col}
    missing = need - set(df.columns)
    if missing:
        raise ValueError(f"Strong-beam filtering requires columns: {sorted(missing)}")

    if df.empty:
        summary = {"N_total": 0, "N_pass": 0, "pass_rate": 0.0}
        return df.copy(), summary

    def is_strong_row(row) -> bool:
        key = norm_src_name(row[source_col])
        beam = str(row[beam_col])
        return beam in strong_map.get(key, [])

    mask = df.apply(is_strong_row, axis=1)
    out = df.loc[mask].copy()

    total = int(len(df))
    kept = int(mask.sum())
    summary = {
        "N_total": total,
        "N_pass": kept,
        "pass_rate": (kept / total) if total else 0.0,
    }
    return out, summary