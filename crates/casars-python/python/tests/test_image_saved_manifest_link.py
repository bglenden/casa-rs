"""Linked saved image-expression manifests and their captured referents."""

import json
from concurrent.futures import ThreadPoolExecutor
from time import sleep

import numpy as np
import pytest

from test_image_cross_device_rename import other_filesystem
from test_image_removal import _backend
from test_image_saved_metadata import EXPRESSION, PIXELS, _fixture


def _link_manifest(source, kind):
    link = source / "imageexpr.json"
    record_name = "shared-record.json"
    if kind == "internal":
        referent = source / record_name
        link_target = record_name
    else:
        referent = source.parent / record_name
        link_target = "../" + record_name if kind == "relative" else str(referent)
    link.rename(referent)
    link.symlink_to(link_target)
    return link, referent


def _record(path):
    return json.loads(path.read_text(encoding="utf-8"))["MiscInfo"]


@pytest.mark.parametrize("kind", ["absolute", "relative", "internal"])
@pytest.mark.parametrize("writer_name", ["casa", "casars"])
@pytest.mark.parametrize("reader_name", ["casa", "casars"])
def test_linked_manifest_writer_reader(tmp_path, monkeypatch, kind, writer_name, reader_name):
    writer_api, reader_api = _backend(writer_name), _backend(reader_name)
    source = _fixture(tmp_path, writer_api)
    monkeypatch.chdir(tmp_path)
    link, referent = _link_manifest(source, kind)
    contents, identity = link.readlink(), link.lstat().st_ino
    writer, reader = writer_api.image(), reader_api.image()
    record = {"stored": {"origin": kind, "count": 7}}
    try:
        assert writer.open("expression.im")
        assert writer.setmiscinfo(record)
        assert link.readlink() == contents and link.lstat().st_ino == identity
        assert _record(referent) == record
        assert reader.open("expression.im")
        assert reader.miscinfo() == record
        np.testing.assert_array_equal(reader.getchunk(), PIXELS * 2)
    finally:
        writer.done()
        reader.done()


@pytest.mark.parametrize("kind", ["absolute", "relative", "internal"])
@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_linked_manifest_stays_writable_after_rename(tmp_path, monkeypatch, kind, backend):
    api = _backend(backend)
    source = _fixture(tmp_path, api)
    monkeypatch.chdir(tmp_path)
    link, referent = _link_manifest(source, kind)
    contents = link.readlink()
    target = tmp_path / "renamed.im"
    image = api.image()
    try:
        assert image.open(str(source))
        assert image.setmiscinfo({"before": kind})
        assert image.rename(str(target))
        assert image.miscinfo() == {"before": kind}
        assert (target / link.name).readlink() == contents
        assert image.setmiscinfo({"after": kind})
        if kind == "internal":
            referent = target / referent.name
        assert _record(referent) == {"after": kind}
        assert image.done() and image.open(str(target))
        assert image.miscinfo() == {"after": kind}
        np.testing.assert_array_equal(image.getchunk(), PIXELS * 2)
    finally:
        image.done()


@pytest.mark.parametrize("change", [
    "link", "same-link-target", "same-link-recreated", "intermediate-link", "referent", "referent-directory",
])
def test_linked_manifest_write_refuses_changed_identity(tmp_path, monkeypatch, change):
    api = _backend("casars")
    source = _fixture(tmp_path, api)
    monkeypatch.chdir(tmp_path)
    link = source / "imageexpr.json"
    originals = tmp_path / "records"
    originals.mkdir()
    referent = originals / "chosen.json"
    link.rename(referent)
    intermediate = tmp_path / "selected-record.json"
    if change == "intermediate-link":
        intermediate.symlink_to(referent)
        link.symlink_to(intermediate)
    else:
        link.symlink_to(referent)
    original = referent.read_bytes()
    decoy = json.dumps({"ImageExpr": EXPRESSION, "MiscInfo": {"untouched": 7}}).encode()
    replacement = tmp_path / "replacement.json"
    image = api.image()
    try:
        assert image.open(str(source))
        if change == "link":
            replacement.write_bytes(decoy)
            retargeted = source / "other-link"
            retargeted.symlink_to(replacement)
            retargeted.replace(link)
        elif change == "same-link-target":
            retargeted = source / "other-link"
            retargeted.symlink_to(referent)
            retargeted.replace(link)
        elif change == "same-link-recreated":
            link.unlink()
            link.symlink_to(referent)
        elif change == "intermediate-link":
            replacement.write_bytes(decoy)
            swapped = tmp_path / "new-selection"
            swapped.symlink_to(replacement)
            swapped.replace(intermediate)
        elif change == "referent":
            replacement.write_bytes(decoy)
            replacement.replace(referent)
        else:
            moved = tmp_path / "moved-records"
            originals.rename(moved)
            originals.mkdir()
            referent.write_bytes(decoy)
        observed = [(path, path.read_bytes()) for path in (referent, replacement) if path.exists()]
        visible_link = link.readlink()
        with pytest.raises(RuntimeError, match="Saved image expression manifest.*has changed"):
            image.setmiscinfo({"must": "not be written"})
        assert image.isopen()
        assert image.miscinfo() == {"original": "keep on errors"}
        assert image.history() == []
        assert link.readlink() == visible_link
        for path, before in observed:
            assert path.read_bytes() == before
        if change == "referent-directory":
            assert (moved / referent.name).read_bytes() == original
        np.testing.assert_array_equal(image.getchunk(), PIXELS * 2)
    finally:
        image.done()


def test_linked_manifest_attachments_share_referent_updates(tmp_path, monkeypatch):
    api = _backend("casars")
    source = _fixture(tmp_path, api)
    monkeypatch.chdir(tmp_path)
    link, referent = _link_manifest(source, "relative")
    peer_source = tmp_path / "peer.im"
    peer_source.mkdir()
    (peer_source / link.name).symlink_to(referent)
    first, second, reader = api.image(), api.image(), api.image()
    try:
        assert first.open(str(source))
        assert second.open(str(peer_source))
        assert first.setmiscinfo({"latest": 1})
        assert second.setmiscinfo({"latest": 2})
        assert first.setmiscinfo({"latest": 3})
        assert first.miscinfo() == {"latest": 3}
        assert second.miscinfo() == {"latest": 2}
        assert _record(referent) == {"latest": 3}
        assert reader.open(str(peer_source))
        assert reader.miscinfo() == {"latest": 3}
        np.testing.assert_array_equal(reader.getchunk(), PIXELS * 2)
    finally:
        first.done()
        second.done()
        reader.done()


def test_linked_manifest_peer_can_attach_during_publication(tmp_path, monkeypatch):
    api = _backend("casars")
    source = _fixture(tmp_path, api)
    monkeypatch.chdir(tmp_path)
    link, referent = _link_manifest(source, "absolute")
    peer = tmp_path / "peer.im"
    peer.mkdir()
    (peer / link.name).symlink_to(referent)
    writer, reader = api.image(), api.image()
    record = {"payload": "x" * 2_000_000}
    try:
        assert writer.open(str(source))
        with ThreadPoolExecutor(max_workers=1) as worker:
            for attempt in range(10):
                assert writer.setmiscinfo({"before": attempt})
                publish = worker.submit(writer.setmiscinfo, record)
                while not publish.done() and not any(tmp_path.glob(".imageexpr-*")):
                    sleep(0.0001)
                assert reader.open(str(peer))
                assert publish.result(timeout=5)
                assert reader.setmiscinfo({"after": attempt})
                assert _record(referent) == {"after": attempt}
                assert reader.done()
    finally:
        reader.done()
        writer.done()


@pytest.mark.parametrize("kind", ["absolute", "internal"])
@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_linked_manifest_rename_across_filesystems(tmp_path, other_filesystem, monkeypatch, kind, backend):
    api = _backend(backend)
    source = _fixture(tmp_path, api)
    monkeypatch.chdir(tmp_path)
    link, referent = _link_manifest(source, kind)
    link_target = link.readlink()
    destination = other_filesystem / "moved.im"
    image = api.image()
    try:
        assert image.open(str(source))
        assert image.setmiscinfo({"before": kind})
        assert image.rename(str(destination))
        assert not source.exists()
        assert image.miscinfo() == {"before": kind}
        moved_link = destination / link.name
        if kind == "internal":
            referent = destination / referent.name
        if backend == "casa":
            assert moved_link.is_file() and not moved_link.is_symlink()
        else:
            assert moved_link.readlink() == link_target
        assert image.setmiscinfo({"after": kind})
        expected = {"before" if backend == "casa" else "after": kind}
        assert _record(referent) == expected
        assert image.done() and image.open(str(destination))
        assert image.miscinfo() == {"after": kind}
        np.testing.assert_array_equal(image.getchunk(), PIXELS * 2)
    finally:
        image.done()
