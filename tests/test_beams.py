import numpy as np
import atl08kit.beams as beams


def test_get_strong_beams():
    assert beams.get_strong_beams(0) == ["gt1l", "gt2l", "gt3l"]
    assert beams.get_strong_beams(1) == ["gt1r", "gt2r", "gt3r"]
    assert beams.get_strong_beams(2) == []
    assert beams.get_strong_beams(999) == []


def test_get_strong_beams_from_h5(monkeypatch, tmp_path):
    class FakeFile(dict):
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_h5py_file(path, mode="r"):
        f = FakeFile()
        f["/orbit_info/sc_orient"] = np.array([1])
        return f

    monkeypatch.setattr("h5py.File", fake_h5py_file)

    fake_h5 = tmp_path / "ATL08_fake.h5"
    fake_h5.touch()

    strong = beams.get_strong_beams_from_h5(fake_h5)
    assert strong == ["gt1r", "gt2r", "gt3r"]