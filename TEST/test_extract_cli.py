import subprocess
import sys
from pathlib import Path

import h5py
import pandas as pd
import pytest


def _write_minimal_atl08_h5(path: Path):
    # minimal ATL08-like file for extract CLI tests
    with h5py.File(path, "w") as f:
        grp = f.create_group("/gt2l/land_segments")

        grp.create_dataset("latitude", data=[10.0, 11.0, 12.0])
        grp.create_dataset("longitude", data=[100.0, 101.0, 102.0])
        grp.create_dataset("dem_h", data=[50.0, 51.0, 52.0])

        canopy = grp.create_group("canopy")
        canopy.create_dataset(
            "canopy_h_metrics",
            data=[[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]],
        )


def test_cli_extract(tmp_path: Path):
    h5_path = tmp_path / "test_atl08.h5"
    out_csv = tmp_path / "out.csv"

    _write_minimal_atl08_h5(h5_path)

    cmd = [
        sys.executable,
        "-m",
        "atl08kit.cli",
        "extract",
        "--h5",
        str(h5_path),
        "--out",
        str(out_csv),
        "--summary",
    ]

    res = subprocess.run(
        cmd,
        cwd=str(tmp_path),
        text=True,
        capture_output=True,
    )

    assert res.returncode == 0, f"STDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}"

    assert out_csv.exists()

    df = pd.read_csv(out_csv)

    for col in ["lon", "lat", "beam", "dem_h", "source_file"]:
        assert col in df.columns

    # ---- summary mentions skipped fields ----
    combined_output = res.stdout + res.stderr
    assert "fields_skipped" in combined_output