import pandas as pd
import pytest

from atl08kit.filters import filter_by_expr
from atl08kit.expr import ExprError


def make_df():
    return pd.DataFrame(
        {
            "dem_h": [100.0, 100.0, 100.0],
            "h_te_best_fit": [98.0, 97.0, 50.0],
            "terrain_flg": [0, 1, 0],
            "cloud_flag_atm": [0, 0, 5],
        }
    )


def test_filter_by_expr_basic():
    df = make_df()
    expr = "abs(dem_h - h_te_best_fit) <= 3 and terrain_flg == 0 and cloud_flag_atm < 3"
    out, summary = filter_by_expr(df, expr)

    assert len(out) == 1
    assert summary["N_pass"] == 1
    assert 0 < summary["pass_rate"] < 1


def test_expr_missing_column_raises():
    df = make_df()
    with pytest.raises(ExprError):
        filter_by_expr(df, "missing_col > 0")


def test_disallowed_function_raises():
    df = make_df()
    with pytest.raises(ExprError):
        filter_by_expr(df, "__import__('os').system('echo hi')")


def test_expression_must_be_boolean():
    df = make_df()
    with pytest.raises(ExprError):
        filter_by_expr(df, "dem_h - h_te_best_fit")