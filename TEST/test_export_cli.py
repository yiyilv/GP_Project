import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest
import h5py


def _write_minimal_atl08_h5(path: Path):
    # minimal ATL08-like file for CLI extract tests
    with h5py.File(path, "w") as f:
        grp = f.create_group("gt2l/land_segments")

        grp.create_dataset("latitude", data=[10.0, 10.1, 10.2])
        grp.create_dataset("longitude", data=[100.0, 100.1, 100.2])
        grp.create_dataset("dem_h", data=[50.0, 51.0, 52.0])

        canopy = grp.create_group("canopy")
        canopy.create_dataset(
            "canopy_h_metrics",
            data=[[1, 2], [3, 4], [5, 6]],  # 2D
        )


def test_cli_extract_minimal_h5(tmp_path: Path):
    h5_path = tmp_path / "test.h5"
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
    assert out_csv.stat().st_size > 0

    df = pd.read_csv(out_csv)

    for col in ["lon", "lat", "beam", "dem_h", "source_file"]:
        assert col in df.columns

    assert len(df) == 3
    assert df["dem_h"].tolist() == [50.0, 51.0, 52.0]

    # auto mode should skip non-1D fields
    assert "fields_skipped" in res.stdout