"""Persistent component-image masks and public expression consumers."""

import os

import numpy as np
import pytest

from test_image_component_list import _component_record
from test_image_removal import _backend


@pytest.mark.parametrize("writer", ["casa", "casars"])
@pytest.mark.parametrize("reader", ["casa", "casars"])
def test_native_component_mask_lifecycle_and_paged_copy(tmp_path, writer, reader):
    writer_api, reader_api = _backend(writer), _backend(reader)
    path = tmp_path / "source.im"
    destination = tmp_path / "copy.im"
    relocated = tmp_path / "relocated.im"
    source, derived, reopened = writer_api.image(), reader_api.image(), reader_api.image()
    expected = np.zeros((6, 8), dtype=bool)
    expected[3, 4] = True
    try:
        assert source.fromcomplist(str(path), [6, 8], _component_record(writer_api))
        assert source.calcmask(f'"{path}" > 0', "point", True)
        np.testing.assert_array_equal(source.getchunk(getmask=True), expected)
        assert list(source.maskhandler("copy", ["point", "copy"])) == ["T"]
        assert list(source.maskhandler("set", [])) == ["T"]
        assert np.all(source.getchunk(getmask=True))
        assert list(source.maskhandler("set", ["copy"])) == ["T"]
        assert list(source.maskhandler("rename", ["copy", "selection"])) == ["T"]
        assert list(source.maskhandler("default")) == ["selection"]
        assert (path / "selection").is_dir() and not (path / "copy").exists()
        assert source.done()
        assert source.open(str(path))
        np.testing.assert_array_equal(source.getchunk(getmask=True), expected)
        assert source.calcmask("F", "point", False)
        assert list(source.maskhandler("get")) == ["point", "selection"]
        assert source.done()

        assert derived.fromimage(str(destination), str(path))
        np.testing.assert_array_equal(derived.getchunk(getmask=True), expected)
        stats = derived.statistics()
        assert stats["npts"][0] == 1
        assert stats["sum"][0] == 2.
        assert derived.done()

        assert reopened.open(str(path))
        assert list(reopened.maskhandler("default")) == ["selection"]
        np.testing.assert_array_equal(reopened.getchunk(getmask=True), expected)
        assert list(reopened.maskhandler("set", ["point"])) == ["T"]
        assert not np.any(reopened.getchunk(getmask=True))
        assert list(reopened.maskhandler("set", ["selection"])) == ["T"]
        assert list(reopened.maskhandler("delete", ["selection"])) == ["T"]
        assert list(reopened.maskhandler("default")) == [""]
        assert list(reopened.maskhandler("get")) == ["point"]
        assert np.all(reopened.getchunk(getmask=True))
        assert (path / "point").is_dir() and not (path / "selection").exists()
        assert reopened.done()
        assert source.open(str(path))
        assert list(source.maskhandler("default")) == [""]
        assert list(source.maskhandler("get")) == ["point"]
        assert source.rename(str(relocated))
        assert source.done()
        assert reopened.open(str(relocated))
        assert list(reopened.maskhandler("set", ["point"])) == ["T"]
        assert not np.any(reopened.getchunk(getmask=True))
    finally:
        source.done()
        derived.done()
        reopened.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_component_operand_on_a_paged_image(tmp_path, backend):
    api = _backend(backend)
    path = tmp_path / "component.im"
    source, target = api.image(), api.image()
    expected_mask = np.zeros((6, 8), dtype=bool)
    expected_mask[3, 4] = True
    try:
        assert source.fromcomplist(str(path), [6, 8], _component_record(api))
        assert source.done()
        assert target.fromarray(pixels=np.arange(48, dtype=np.float32).reshape(6, 8))
        assert target.calcmask(f'"{path}" > 0')
        np.testing.assert_array_equal(target.getchunk(getmask=True), expected_mask)
        stats = target.statistics(mask=f'"{path}" > 0')
        assert stats["npts"][0] == 1
        assert stats["sum"][0] == 28.
        with pytest.raises(RuntimeError, match="is not a PagedImage"):
            target.calcmask(f'"{path}:nomask" > 0', "rejected")
        assert list(target.maskhandler("get")) == ["mask0"]
    finally:
        source.done()
        target.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_component_operand_in_imagecalc(tmp_path, backend):
    api = _backend(backend)
    path = tmp_path / "component.im"
    source = api.image()
    expression = None
    expected = np.full((6, 8), 3., dtype=np.float64)
    expected[3, 4] = 5.
    try:
        assert source.fromcomplist(str(path), [6, 8], _component_record(api))
        assert source.done()
        expression = source.imagecalc(pixels=f'"{path}" + 3')
        values = expression.getchunk()
        assert expression.pixeltype() == "float"
        assert values.dtype == np.float64
        np.testing.assert_array_equal(values, expected)
    finally:
        if expression is not None:
            expression.done()
        source.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("persistent", [False, True])
def test_component_putregion_mask_state(tmp_path, backend, persistent):
    api = _backend(backend)
    path = tmp_path / "component.im"
    source = api.image()
    try:
        assert source.fromcomplist(str(path) if persistent else "", [6, 8], _component_record(api))
        np.testing.assert_array_equal(source.haslock(), [True, True])
        assert source.unlock()
        assert source.lock(writelock=True, nattempts=1)
        np.testing.assert_array_equal(source.haslock(), [True, True])
        with pytest.raises(RuntimeError, match="pixel values cannot be modified"):
            source.putregion(pixels=np.ones((6, 8), dtype=np.float32))
        mask = np.zeros((6, 8), dtype=bool)
        if not persistent:
            assert source.putregion(pixelmask=mask)
            assert np.all(source.getchunk(getmask=True))
            return
        with pytest.raises(RuntimeError, match="MaskedLattice::pixelMask - no pixelmask available"):
            source.putregion(pixelmask=mask)
        assert list(source.maskhandler("get")) == ["mask0"]
        assert np.all(source.getchunk(getmask=True))
        assert source.done()
        assert source.open(str(path))
        assert list(source.maskhandler("default")) == ["mask0"]
        assert np.all(source.getchunk(getmask=True))
    finally:
        source.done()


def test_mask_file_collision_preserves_unrelated_directory(tmp_path):
    api = _backend("casars")
    path = tmp_path / "component.im"
    source = api.image()
    try:
        assert source.fromcomplist(str(path), [6, 8], _component_record(api))
        assert source.calcmask("T", "original")
        unrelated = path / "occupied"
        unrelated.mkdir()
        sentinel = unrelated / "sentinel"
        sentinel.write_text("keep")
        with pytest.raises(RuntimeError):
            source.calcmask("F", "occupied")
        with pytest.raises(RuntimeError):
            source.maskhandler("rename", ["original", "occupied"])
        assert sentinel.read_text() == "keep"
        assert list(source.maskhandler("get")) == ["original"]
        assert list(source.maskhandler("default")) == ["original"]
        assert np.all(source.getchunk(getmask=True))
    finally:
        source.done()


def test_same_handle_reads_renamed_active_copy(tmp_path):
    api = _backend("casars")
    path = tmp_path / "component.im"
    source = api.image()
    expected = np.zeros((6, 8), dtype=bool)
    expected[3, 4] = True
    try:
        assert source.fromcomplist(str(path), [6, 8], _component_record(api))
        assert source.calcmask(f'"{path}" > 0', "original")
        assert list(source.maskhandler("copy", ["original", "copy"])) == ["T"]
        assert list(source.maskhandler("set", ["copy"])) == ["T"]
        assert list(source.maskhandler("rename", ["copy", "selection"])) == ["T"]
        np.testing.assert_array_equal(source.getchunk(getmask=True), expected)
    finally:
        source.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_component_image_copies_mask_from_another_native_image(tmp_path, backend):
    api = _backend(backend)
    paged, component = api.image(), api.image()
    source, target = tmp_path / "paged.im", tmp_path / "component.im"
    expected = np.arange(48).reshape(6, 8) >= 25
    try:
        assert paged.fromarray(str(source), np.arange(48).reshape(6, 8))
        assert paged.calcmask(f'"{source}" >= 25', "threshold")
        assert paged.done()
        assert component.fromcomplist(str(target), [6, 8], _component_record(api))
        assert list(component.maskhandler("copy", [f"{source}:threshold", "copied"])) == ["T"]
        assert list(component.maskhandler("set", ["copied"])) == ["T"]
        np.testing.assert_array_equal(component.getchunk(getmask=True), expected)
        assert component.done()
        assert component.open(str(target))
        np.testing.assert_array_equal(component.getchunk(getmask=True), expected)
    finally:
        paged.done()
        component.done()


def test_native_component_mask_writes_preserve_other_attachment_edits(tmp_path):
    api = _backend("casars")
    path = tmp_path / "shared.im"
    first, second = api.image(), api.image()
    try:
        assert first.fromcomplist(str(path), [6, 8], _component_record(api))
        assert second.open(str(path))
        assert first.calcmask("F", "first")
        assert second.calcmask("T", "second", False)
        assert second.done()
        assert first.done()
        assert first.open(str(path))
        assert list(first.maskhandler("get")) == ["first", "second"]
        assert list(first.maskhandler("default")) == ["first"]
        assert not np.any(first.getchunk(getmask=True))
    finally:
        first.done()
        second.done()


def test_fresh_component_masks_use_locked_destination_names(tmp_path):
    api = _backend("casars")
    path = tmp_path / "shared.im"
    first, second = api.image(), api.image()
    try:
        assert first.fromcomplist(str(path), [6, 8], _component_record(api))
        assert first.calcmask("T", "original", False)
        assert second.open(str(path))
        assert first.calcmask("F", "claimed")
        with pytest.raises(RuntimeError, match="Mask claimed already exists"):
            second.maskhandler("copy", ["original", "claimed"])
        assert first.calcmask("F", asdefault=False)
        assert second.calcmask("T", asdefault=False)
        assert second.done()
        assert first.done()
        assert first.open(str(path))
        assert list(first.maskhandler("get")) == ["original", "claimed", "mask0", "mask1"]
        assert list(first.maskhandler("default")) == ["claimed"]
        assert not np.any(first.getchunk(getmask=True))
        assert list(first.maskhandler("set", ["mask0"])) == ["T"]
        assert not np.any(first.getchunk(getmask=True))
        assert list(first.maskhandler("set", ["mask1"])) == ["T"]
        assert np.all(first.getchunk(getmask=True))
    finally:
        first.done()
        second.done()


def test_component_mask_update_only_writes_parent_keywords(tmp_path):
    api = _backend("casars")
    path = tmp_path / "component.im"
    image = api.image()
    try:
        assert image.fromcomplist(str(path), [6, 8], _component_record(api))
        info = path / "table.info"
        content = info.read_bytes()
        os.utime(info, (10_000, 10_000))
        info.chmod(0o444)
        assert image.calcmask("F", "selected")
        assert info.stat().st_mtime == 10_000
        assert info.read_bytes() == content
        assert image.done()
        assert image.open(str(path))
        assert list(image.maskhandler("default")) == ["selected"]
        assert not np.any(image.getchunk(getmask=True))
    finally:
        image.done()


@pytest.mark.skipif(not hasattr(os, "geteuid") or os.geteuid() == 0, reason="requires unprivileged POSIX access checks")
def test_failed_control_file_publish_restores_mask_after_reopen(tmp_path):
    api = _backend("casars")
    path = tmp_path / "component.im"
    image = api.image()
    try:
        assert image.fromcomplist(str(path), [6, 8], _component_record(api))
        assert image.calcmask("F", "selected")
        control = path / "table.dat"
        old_keywords = control.read_bytes()
        control.chmod(0o444)
        try:
            with pytest.raises(RuntimeError):
                image.maskhandler("rename", ["selected", "renamed"])
        finally:
            control.chmod(0o644)
        assert control.read_bytes() == old_keywords
        assert (path / "selected").is_dir()
        assert not (path / "renamed").exists()
        assert image.done()
        assert image.open(str(path))
        assert list(image.maskhandler("default")) == ["selected"]
        assert not np.any(image.getchunk(getmask=True))
    finally:
        image.done()


def test_component_mask_edits_protect_shared_backing_and_history_tables(tmp_path):
    api = _backend("casars")
    path = tmp_path / "component.im"
    image, table = api.image(), api.table()
    try:
        assert image.fromcomplist(str(path), [6, 8], _component_record(api))
        assert image.calcmask("F", "selected")
        assert image.sethistory("origin", ["preserved history"])
        assert image.done()
        assert table.open(str(path), nomodify=False)
        region = table.getkeyword("masks")["selected"]
        masks = {
            "selected": dict(region, mask="././selected"),
            "alias": dict(region, mask="././selected"),
            "logtable": dict(region, mask="././logtable"),
        }
        table.putkeyword("masks", masks)
        assert table.done()
        assert image.open(str(path))
        for operation, names, diagnostic in [
            ("delete", ["selected"], "shares its table"),
            ("rename", ["selected", "moved"], "shares its table"),
            ("delete", ["logtable"], "does not own a local mask table"),
        ]:
            with pytest.raises(RuntimeError, match=diagnostic):
                image.maskhandler(operation, names)
        assert (path / "selected").is_dir()
        assert (path / "logtable").is_dir()
        assert "preserved history" in image.history(list=False)
        assert image.maskhandler("set", ["alias"])
        assert not np.any(image.getchunk(getmask=True))
    finally:
        table.done()
        image.done()


def test_lazy_component_expression_mask_survives_rename_and_name_reuse(tmp_path):
    api = _backend("casars")
    path = tmp_path / "component.im"
    source, peer = api.image(), api.image()
    result = None
    try:
        assert source.fromcomplist(str(path), [6, 8], _component_record(api))
        assert source.calcmask("F", "first")
        result = source.imagecalc(pixels=f'"{path}" + 3')
        assert peer.open(str(path))
        assert list(peer.maskhandler("rename", ["first", "renamed"])) == ["T"]
        assert peer.calcmask("T", "first", False)
        assert list(peer.maskhandler("default")) == ["renamed"]
        actual = result.getchunk([1, 0], [5, 7], [2, 2], getmask=True)
        np.testing.assert_array_equal(actual, np.zeros((3, 4), dtype=bool))
    finally:
        if result is not None:
            result.done()
        peer.done()
        source.done()
