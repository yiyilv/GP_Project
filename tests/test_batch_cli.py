import subprocess
import sys
from pathlib import Path

import pandas as pd


def test_cli_batch_expr_only(tmp_path: Path):
    in_dir = tmp_path / "in"
    out_dir = tmp_path / "out"
    in_dir.mkdir()

    df1 = pd.DataFrame(
        {"dem_h": [10, 10], "h_te_best_fit": [9, 0], "cloud_flag_atm": [0, 5]}
    )
    df2 = pd.DataFrame(
        {"dem_h": [20, 20], "h_te_best_fit": [19, 17], "cloud_flag_atm": [0, 0]}
    )
    df1.to_csv(in_dir / "a.csv", index=False)
    df2.to_csv(in_dir / "b.csv", index=False)

    summary_csv = tmp_path / "summary.csv"

    cmd = [
        sys.executable,
        "-m",
        "atl08kit.cli",
        "batch",
        "--in-dir",
        str(in_dir),
        "--out-dir",
        str(out_dir),
        "--expr",
        "abs(dem_h - h_te_best_fit) <= 1 and cloud_flag_atm < 3",
        "--summary",
        str(summary_csv),
    ]
    res = subprocess.run(cmd, cwd=str(tmp_path), text=True, capture_output=True)
    assert res.returncode == 0, f"STDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}"

    assert (out_dir / "a.csv").exists()
    assert (out_dir / "b.csv").exists()
    assert summary_csv.exists()

    s = pd.read_csv(summary_csv)
    assert set(s["file"].tolist()) == {"a.csv", "b.csv"}
    assert ((s["pass_rate"] >= 0) & (s["pass_rate"] <= 1)).all()