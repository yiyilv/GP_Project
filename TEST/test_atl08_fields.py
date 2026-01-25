from __future__ import annotations

from pathlib import Path

import h5py
import numpy as np

from atl08kit.atl08 import list_land_segment_fields, read_atl08_h5


def _make_mock_atl08_with_extra_fields(tmp_path: Path) -> Path:
    h5_path = tmp_path / "ATL08_20220102113835_01711406_007_01.h5"

    with h5py.File(h5_path, "w") as f:
        orbit = f.create_group("orbit_info")
        orbit.create_dataset("sc_orient", data=np.array([0], dtype=np.int32))

        g = f.create_group("gt2l")
        ls = g.create_group("land_segments")

        # point-aligned basics
        ls.create_dataset("longitude", data=np.array([116.0, 116.1], dtype=np.float64))
        ls.create_dataset("latitude", data=np.array([29.0, 29.1], dtype=np.float64))
        ls.create_dataset("dem_h", data=np.array([100.0, 101.0], dtype=np.float32))

        terrain = ls.create_group("terrain")
        terrain.create_dataset("h_te_best_fit", data=np.array([99.0, 80.0], dtype=np.float32))
        # invalid shapes for strict field extraction tests
        ls.create_dataset("bad_2d", data=np.array([[1, 2], [3, 4]], dtype=np.int32))
        ls.create_dataset("bad_len", data=np.array([1, 2, 3], dtype=np.int32))

    return h5_path


def test_list_land_segment_fields(tmp_path: Path):
    h5 = _make_mock_atl08_with_extra_fields(tmp_path)
    fields = list_land_segment_fields(h5, beam="gt2l")

    assert "longitude" in fields
    assert "latitude" in fields
    assert "dem_h" in fields
    assert "terrain/h_te_best_fit" in fields
    assert "bad_2d" in fields
    assert "bad_len" in fields


def test_read_atl08_rejects_2d_field(tmp_path: Path):
    h5 = _make_mock_atl08_with_extra_fields(tmp_path)
    try:
        read_atl08_h5(h5, fields=["bad_2d"])
        assert False, "Expected ValueError for 2D field"
    except ValueError as e:
        assert "not 1D" in str(e)


def test_read_atl08_rejects_length_mismatch(tmp_path: Path):
    h5 = _make_mock_atl08_with_extra_fields(tmp_path)
    try:
        read_atl08_h5(h5, fields=["bad_len"])
        assert False, "Expected ValueError for length mismatch"
    except ValueError as e:
        assert "length mismatch" in str(e)