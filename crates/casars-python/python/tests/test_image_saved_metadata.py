"""Saved expression metadata persists through the casacore manifest."""

import json

import numpy as np
import pytest

from test_image_removal import _backend


PIXELS = np.array([[3., 4.], [5., 6.]])
EXPRESSION = '"input.im" * 2'


def _fixture(root, api, pixel_type="f"):
    image = api.image()
    try:
        assert image.fromarray(outfile=str(root / "input.im"), pixels=PIXELS, type=pixel_type)
    finally:
        image.done()
    source = root / "expression.im"
    source.mkdir()
    (source / "imageexpr.json").write_text(
        json.dumps({
            "Version": 999, "DataType": "unused", "ImageExpr": EXPRESSION,
            "MiscInfo": {"original": "keep on errors"}, "Extra": "discard on save",
        }),
        encoding="utf-8",
    )
    return source


@pytest.mark.parametrize("writer_name", ["casa", "casars"])
@pytest.mark.parametrize("reader_name", ["casa", "casars"])
def test_saved_miscinfo_round_trips_between_backends(tmp_path, monkeypatch, writer_name, reader_name):
    writer_api = _backend(writer_name)
    reader_api = _backend(reader_name)
    source = _fixture(tmp_path, writer_api)
    monkeypatch.chdir(tmp_path)
    writer = writer_api.image()
    reader = reader_api.image()
    info = {
        "scalar": 1.25 + 2.5j,
        "vector": np.array([3. + 4.j, -1. + 2.j], dtype=np.complex128),
        "nested": {"flags": [True, False], "integers": [3, 7], "names": ["a", "b"]},
        "ordinary": {"r": 7., "i": 9.},
    }
    try:
        assert writer.open("expression.im")
        assert writer.setmiscinfo(info)
        raw = json.loads((source / "imageexpr.json").read_text(encoding="utf-8"))
        assert set(raw) == {"Version", "DataType", "ImageExpr", "MiscInfo"}
        assert (raw["Version"], raw["DataType"], raw["ImageExpr"]) == (1, "float", EXPRESSION)
        assert list(raw["MiscInfo"]["scalar"]) == ["r", "i"]
        assert reader.open("expression.im")
        stored = reader.miscinfo()
        assert stored["scalar"] == info["scalar"]
        np.testing.assert_array_equal(stored["vector"], info["vector"])
        for field, values in info["nested"].items():
            np.testing.assert_array_equal(stored["nested"][field], values)
        assert stored["ordinary"] == info["ordinary"]
        np.testing.assert_array_equal(reader.getchunk(), PIXELS * 2)
        assert reader.history() == []
        assert reader.done()

        assert writer.setmiscinfo({})
        assert reader.open("expression.im")
        assert reader.miscinfo() == {}
    finally:
        reader.done()
        writer.done()


@pytest.mark.parametrize("reader_name", ["casa", "casars"])
def test_saved_miscinfo_keeps_wide_integers(tmp_path, monkeypatch, reader_name):
    api = _backend("casars")
    source = _fixture(tmp_path, api)
    monkeypatch.chdir(tmp_path)
    record = {"scalar": 1 << 50, "vector": np.array([1 << 50, 1 << 51], dtype=np.int64)}
    writer = api.image()
    reader = _backend(reader_name).image()
    try:
        assert writer.open(str(source))
        assert writer.setmiscinfo(record)
        assert reader.open(str(source))
        stored = reader.miscinfo()
        assert stored["scalar"] == record["scalar"]
        np.testing.assert_array_equal(stored["vector"], record["vector"])
    finally:
        reader.done()
        writer.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_saved_double_metadata_can_be_written(tmp_path, monkeypatch, backend):
    api = _backend(backend)
    source = _fixture(tmp_path, api, pixel_type="d")
    monkeypatch.chdir(tmp_path)
    image = api.image()
    try:
        assert image.open(str(source))
        assert image.pixeltype() == "double"
        assert image.setmiscinfo({"stored": "yes"})
        raw = json.loads((source / "imageexpr.json").read_text(encoding="utf-8"))
        assert raw["DataType"] == "double"
        assert raw["MiscInfo"] == {"stored": "yes"}
        with pytest.raises(RuntimeError, match="miscinfo does not support images with double precision"):
            image.miscinfo()
    finally:
        image.done()


@pytest.mark.parametrize("value", [float("nan"), float("inf"), [], [[1., 2.], [3., 4.]]])
def test_saved_miscinfo_refuses_a_record_casa_cannot_read_back(tmp_path, monkeypatch, value):
    api = _backend("casars")
    source = _fixture(tmp_path, api)
    monkeypatch.chdir(tmp_path)
    path = source / "imageexpr.json"
    original = path.read_bytes()
    image = api.image()
    try:
        assert image.open(str(source))
        with pytest.raises(RuntimeError, match="Saved image expression MiscInfo requires"):
            image.setmiscinfo({"never_written": np.asarray(value) if isinstance(value, list) else value})
        assert image.miscinfo() == {"original": "keep on errors"}
        assert image.history() == []
        assert path.read_bytes() == original
        assert len(list(source.iterdir())) == 1
    finally:
        image.done()


@pytest.mark.parametrize("changed", ["directory", "manifest", "file_replacement", "cwd"])
def test_saved_miscinfo_only_writes_the_captured_image(tmp_path, monkeypatch, changed):
    api = _backend("casars")
    source = _fixture(tmp_path, api)
    monkeypatch.chdir(tmp_path)
    path = source / "imageexpr.json"
    original = path.read_bytes()
    decoy = json.dumps({"ImageExpr": '"input.im" + 1', "MiscInfo": {"untouched": True}})
    image = api.image()
    try:
        assert image.open("expression.im")
        if changed == "cwd":
            other = tmp_path / "elsewhere"
            other.mkdir()
            (other / source.name).mkdir()
            other_manifest = other / source.name / path.name
            other_manifest.write_text(decoy, encoding="utf-8")
            monkeypatch.chdir(other)
            assert image.setmiscinfo({"updated": "original"})
            assert other_manifest.read_text(encoding="utf-8") == decoy
            assert json.loads(path.read_text(encoding="utf-8"))["MiscInfo"] == {"updated": "original"}
        else:
            if changed == "directory":
                moved = tmp_path / "moved.im"
                source.rename(moved)
                source.mkdir()
            if changed == "file_replacement":
                decoy = json.dumps({"ImageExpr": EXPRESSION, "MiscInfo": {"untouched": True}})
                replacement = source / "replacement.json"
                replacement.write_text(decoy, encoding="utf-8")
                replacement.replace(path)
            else:
                path.write_text(decoy, encoding="utf-8")
            with pytest.raises(RuntimeError, match="Saved image expression.*has changed"):
                image.setmiscinfo({"must": "not be written"})
            assert image.miscinfo() == {"original": "keep on errors"}
            assert image.history() == []
            assert path.read_text(encoding="utf-8") == decoy
            if changed == "directory":
                assert (moved / path.name).read_bytes() == original
        np.testing.assert_array_equal(image.getchunk(), PIXELS * 2)
    finally:
        image.done()


def test_saved_metadata_peers_can_publish_successive_updates(tmp_path, monkeypatch):
    api = _backend("casars")
    source = _fixture(tmp_path, api)
    monkeypatch.chdir(tmp_path)
    first, second, reopened = api.image(), api.image(), api.image()
    try:
        assert first.open("expression.im")
        assert second.open(str(source))
        assert first.setmiscinfo({"message": "first writer"})
        assert second.setmiscinfo({"message": "second writer"})
        assert first.miscinfo() == {"message": "first writer"}
        assert second.miscinfo() == {"message": "second writer"}
        assert reopened.open("expression.im")
        assert reopened.miscinfo() == {"message": "second writer"}
    finally:
        reopened.done()
        second.done()
        first.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_saved_miscinfo_promotes_a_mixed_complex_vector(tmp_path, monkeypatch, backend):
    api = _backend(backend)
    source = _fixture(tmp_path, api)
    monkeypatch.chdir(tmp_path)
    path = source / "imageexpr.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest["MiscInfo"] = {"mixed": [2, {"r": 3, "i": 4}]}
    path.write_text(json.dumps(manifest), encoding="utf-8")
    image = api.image()
    try:
        assert image.open("expression.im")
        np.testing.assert_array_equal(image.miscinfo()["mixed"], [2 + 0j, 3 + 4j])
    finally:
        image.done()


def test_saved_miscinfo_refuses_a_record_beyond_the_reader_depth(tmp_path, monkeypatch):
    api = _backend("casars")
    source = _fixture(tmp_path, api)
    monkeypatch.chdir(tmp_path)
    path = source / "imageexpr.json"
    original = path.read_bytes()
    record = {"value": 1}
    for _ in range(130):
        record = {"nested": record}
    image = api.image()
    try:
        assert image.open("expression.im")
        with pytest.raises(RuntimeError, match="MiscInfo cannot be reopened"):
            image.setmiscinfo(record)
        assert path.read_bytes() == original
        assert image.miscinfo() == {"original": "keep on errors"}
        assert image.history() == []
    finally:
        image.done()
