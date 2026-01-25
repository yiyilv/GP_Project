from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import h5py
import numpy as np
import pandas as pd

from atl08kit.beams import get_strong_beams_from_h5


# Supported ATL08 beam identifiers (no leading "/").
_ALL_BEAMS = ["gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r"]


@dataclass(frozen=True)
class ReadSummary:
    N_total: int
    N_out: int
    pass_rate: float
    beams_used: list[str]
    fields_used: list[str]
    fields_skipped: list[str]


# Default ATL08 land_segments fields (col -> relative HDF5 path).
_DEFAULT_FIELDS: dict[str, str] = {
    "dem_h": "dem_h",
    "h_te_interp": "terrain/h_te_interp",
    "h_te_mean": "terrain/h_te_mean",
    "h_te_best_fit": "terrain/h_te_best_fit",
    "terrain_flg": "terrain_flg",
    "urban_flag": "urban_flag",
    "cloud_flag_atm": "cloud_flag_atm",
    "terrain_slope": "terrain/terrain_slope",
    "n_te_photons": "terrain/n_te_photons",
}


def _normalize_beams(beams: Sequence[str] | None) -> list[str]:
    if beams is None:
        return list(_ALL_BEAMS)
    out: list[str] = []
    for b in beams:
        b = str(b).strip()
        if not b:
            continue
        if b.startswith("/"):
            b = b[1:]
        if b not in _ALL_BEAMS:
            raise ValueError(f"Unknown beam name: {b}. Expected one of: {_ALL_BEAMS}")
        out.append(b)
    return out


def _resolve_fields(fields: Sequence[str] | None) -> dict[str, str]:
    """
    Normalize user field selection to a mapping of output column name -> land_segments dataset path.
    """
    if fields is None:
        return dict(_DEFAULT_FIELDS)

    mapping: dict[str, str] = {}
    inv = {v: k for k, v in _DEFAULT_FIELDS.items()}

    for item in fields:
        item = str(item).strip()
        if not item:
            continue

        # default output name
        if item in _DEFAULT_FIELDS:
            mapping[item] = _DEFAULT_FIELDS[item]
            continue

        # default dataset path
        if item in inv:
            mapping[inv[item]] = item
            continue

        # raw dataset path
        col = item.replace("/", "_")
        mapping[col] = item

    if not mapping:
        raise ValueError("fields resolved to empty selection")
    return mapping


def read_atl08_h5(
    h5_path: str | Path,
    *,
    beams: Sequence[str] | None = None,
    strong_only: bool = False,
    fields: Sequence[str] | None = None,
    add_source_file: bool = True,
    source_col: str = "source_file",
) -> tuple[pd.DataFrame, ReadSummary]:
    # Build a flat point table from /{beam}/land_segments.
    # - fields set: strict extraction (missing/misaligned -> error)
    # - fields None: auto-scan aligned 1D datasets (others skipped)
    h5_path = Path(h5_path)
    if not h5_path.exists():
        raise FileNotFoundError(f"H5 not found: {h5_path}")

    selected_beams = _normalize_beams(beams)

    if strong_only:
        strong = set(get_strong_beams_from_h5(h5_path))
        selected_beams = [b for b in selected_beams if b in strong]

    frames: list[pd.DataFrame] = []
    N_total = 0

    # debug info
    used_set: set[str] = set()
    skipped_set: set[str] = set()

    with h5py.File(h5_path, "r") as f:
        for beam in selected_beams:
            ls = f"/{beam}/land_segments"
            if ls not in f:
                continue
            grp = f[ls]

            lat = grp["latitude"][:]
            lon = grp["longitude"][:]
            n = int(len(lat))
            if n == 0:
                continue
            N_total += n

            data: dict[str, np.ndarray] = {
                "lon": lon,
                "lat": lat,
                "beam": np.full(n, beam, dtype=object),
            }

            # Select fields to extract (strict vs auto)
            if fields is not None:
                field_map = _resolve_fields(fields)

                for out_col, rel_path in field_map.items():
                    if rel_path not in grp:
                        raise KeyError(f"Dataset missing in {ls}: '{rel_path}'")

                    arr = grp[rel_path][:]

                    if getattr(arr, "ndim", 0) != 1:
                        raise ValueError(
                            f"Field '{rel_path}' is not 1D (ndim={arr.ndim}). "
                            "Cannot store it in a point table CSV without expansion."
                        )
                    if len(arr) != n:
                        raise ValueError(
                            f"Field '{rel_path}' length mismatch: len(field)={len(arr)} vs len(lat)={n}."
                        )

                    data[out_col] = arr
                    used_set.add(rel_path)

            else:
                # Auto: keep aligned 1D datasets under land_segments
                def walk(g, prefix: str = "") -> None:
                    for k, obj in g.items():
                        p = k if prefix == "" else f"{prefix}/{k}"
                        if isinstance(obj, h5py.Dataset):
                            try:
                                arr = obj[:]
                            except Exception:
                                skipped_set.add(p)
                                continue

                            if getattr(arr, "ndim", 0) != 1 or len(arr) != n:
                                skipped_set.add(p)
                                continue
                            col = p.replace("/", "_")
                            if col in data:
                                col = f"ls_{col}"
                            data[col] = arr
                            used_set.add(p)
                        else:
                            walk(obj, p)

                walk(grp, "")

            df = pd.DataFrame(data)

            if add_source_file:
                df[source_col] = h5_path.stem

            frames.append(df)

    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    N_out = int(len(out))
    pass_rate = (N_out / N_total) if N_total else 0.0
    summary = ReadSummary(
        N_total=N_total,
        N_out=N_out,
        pass_rate=pass_rate,
        beams_used=selected_beams,
        fields_used=sorted(used_set),
        fields_skipped=sorted(skipped_set),
    )
    return out, summary

def list_land_segment_fields(h5_path: str | Path, *, beam: str = "gt2l") -> list[str]:
    # Enumerate datasets under /{beam}/land_segments (relative paths).
    # Used to discover valid values for --fields.
    h5_path = Path(h5_path)
    if not h5_path.exists():
        raise FileNotFoundError(f"H5 not found: {h5_path}")

    beam = beam.lstrip("/")
    base = f"/{beam}/land_segments"

    out: list[str] = []
    with h5py.File(h5_path, "r") as f:
        if base not in f:
            raise KeyError(f"Missing group: {base}")
        grp = f[base]

        def walk(g, prefix: str = "") -> None:
            for k, obj in g.items():
                p = k if prefix == "" else f"{prefix}/{k}"
                if isinstance(obj, h5py.Dataset):
                    out.append(p)
                else:
                    walk(obj, p)

        walk(grp, "")

    return sorted(out)