import subprocess
import sys
from pathlib import Path

import pandas as pd


def test_cli_run_pipeline_expr_only(tmp_path: Path):
    df = pd.DataFrame(
        {
            "dem_h": [100, 100],
            "h_te_best_fit": [98, 50],
            "terrain_flg": [0, 0],
            "cloud_flag_atm": [0, 5],
        }
    )
    in_csv = tmp_path / "in.csv"
    out_csv = tmp_path / "out.csv"
    df.to_csv(in_csv, index=False)

    expr = "abs(dem_h - h_te_best_fit) <= 3 and cloud_flag_atm < 3"

    cmd = [
        sys.executable,
        "-m",
        "atl08kit.cli",
        "run",
        "--in",
        str(in_csv),
        "--out",
        str(out_csv),
        "--expr",
        expr,
        "--summary",
    ]
    res = subprocess.run(cmd, cwd=str(tmp_path), text=True, capture_output=True)
    assert res.returncode == 0, f"STDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}"

    assert out_csv.exists()
    out_df = pd.read_csv(out_csv)
    assert len(out_df) == 1