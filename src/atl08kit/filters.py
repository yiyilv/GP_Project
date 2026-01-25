from __future__ import annotations

from typing import Dict, Tuple
import pandas as pd

from atl08kit.expr import compile_expr, eval_expr


def filter_by_expr(df: pd.DataFrame, expr: str) -> Tuple[pd.DataFrame, Dict[str, float]]:
    # Filter rows using a safe boolean expression on columns
    compiled = compile_expr(expr)
    mask = eval_expr(compiled, df)
    out = df.loc[mask].copy()

    summary = {
        "expr": expr,
        "N_total": int(len(df)),
        "N_pass": int(mask.sum()),
        "pass_rate": float(mask.mean()) if len(df) else 0.0,
    }
    return out, summary