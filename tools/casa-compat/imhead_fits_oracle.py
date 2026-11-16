#!/usr/bin/env python3
"""Record CASA imhead returns for a FITS image, including blank pixels."""

import json
import runpy
import shutil
import tempfile
from pathlib import Path

import numpy as np
from casatasks import casalog, exportfits, imhead
from casatools import image, version_string

ROOT = Path("crates/casars-imexplore/tests/data/imhead_fits")
SOURCE = Path("crates/casars-imexplore/tests/data/imhead_crpix/normal.im")
describe = runpy.run_path("tools/casa-compat/record_casa_returns.py")["describe"]


def main():
    casalog.setlogfile("/tmp/casa-977-imhead-fits.log")
    casalog.showconsole(False)
    ROOT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="imhead-fits-oracle-") as scratch:
        source = Path(scratch) / "source.im"
        fits = Path(scratch) / "image.fits"
        shutil.copytree(SOURCE, source)
        ia = image()
        ia.open(str(source))
        pixels = np.arange(32, dtype=np.float32).reshape((4, 4, 1, 2)) - 10
        pixels[0, 0, 0, 0] = np.nan
        ia.putchunk(pixels)
        ia.done()
        exportfits(imagename=str(source), fitsimage=str(fits), overwrite=True)
        integer = Path(scratch) / "integer.fits"
        exportfits(imagename=str(source), fitsimage=str(integer), bitpix=16,
                   minpix=-10.0, maxpix=21.0, overwrite=True)
        shutil.copyfile(integer, ROOT / "integer.fits")
        contents = bytearray(fits.read_bytes())
        end = next(i for i in range(0, len(contents), 80) if contents[i:i + 8] == b"END     ")
        cards = [
            b"MYKEY   =                  123 / observer's value",
            b"STRKEY  = 'tea'",
            b"DBLKEY  =                 1.25",
            b"BOOLKEY =                    T",
            b"HISTORY An external note",
            b"HISTORY > continued",
            b"END",
        ]
        if end % 2880 + 80 * len(cards) > 2880:
            raise ValueError("FITS header needs another block for oracle keywords")
        contents[end:end + 80 * len(cards)] = b"".join(card.ljust(80) for card in cards)
        fits.write_bytes(contents)
        shutil.copyfile(fits, ROOT / "image.fits")
        scaled = Path(scratch) / "scaled.fits"
        scaled_contents = bytearray(contents)
        replacements = {
            "CUNIT3": "CUNIT3  = 'GHz     '",
            "CRVAL3": "CRVAL3  =             1.415",
            "CDELT3": "CDELT3  =             0.000001",
        }
        for offset in range(0, len(scaled_contents), 80):
            key = scaled_contents[offset:offset + 8].decode("ascii", errors="ignore").strip()
            if key in replacements:
                scaled_contents[offset:offset + 80] = replacements.pop(key).encode().ljust(80)
            if key == "END":
                break
        assert not replacements
        scaled.write_bytes(scaled_contents)
        shutil.copyfile(scaled, ROOT / "scaled.fits")
        ia.open(str(fits))
        history = ia.history()
        ia.done()
        cases = {}
        for mode, key in [("summary", ""), ("list", ""), ("history", "")] + [
            ("get", key) for key in ("shape", "bunit", "restfreq", "datamin", "datamax", "minpixpos", "maxpixpos", "mykey", "strkey", "dblkey")
        ]:
            value = imhead(str(fits), mode=mode, hdkey=key)
            cases[f"{mode}:{key}"] = {"structure": describe(value), "value": value}
        for mode in ("put", "add", "del"):
            try:
                imhead(str(fits), mode=mode, hdkey="bunit", hdvalue="Jy/beam")
            except RuntimeError as error:
                cases[mode] = {"error": type(error).__name__, "message": str(error).split("\n", 1)[0]}
            else:
                raise AssertionError(f"CASA allowed {mode} on a FITS image")
        integer_cases = {}
        for mode, key in [("summary", ""), ("list", ""), ("get", "datamin"), ("get", "datamax")]:
            value = imhead(str(integer), mode=mode, hdkey=key)
            integer_cases[f"{mode}:{key}"] = {"structure": describe(value), "value": value}
        scaled_cases = {}
        for mode, key in [("summary", ""), ("list", ""), ("get", "restfreq")]:
            value = imhead(str(scaled), mode=mode, hdkey=key)
            scaled_cases[f"{mode}:{key}"] = {"structure": describe(value), "value": value}
        beam_source = Path(scratch) / "beams.im"
        beam_fits = Path(scratch) / "beams.fits"
        shutil.copytree("crates/casars-imexplore/tests/data/imhead_beams/multiple.im", beam_source)
        exportfits(imagename=str(beam_source), fitsimage=str(beam_fits), overwrite=True)
        shutil.copyfile(beam_fits, ROOT / "beams.fits")
        beam_cases = {}
        for mode in ("summary", "list"):
            value = imhead(str(beam_fits), mode=mode)
            beam_cases[f"{mode}:"] = {"structure": describe(value), "value": value}
        single_source = Path(scratch) / "single.im"
        single_fits = Path(scratch) / "single.fits"
        shutil.copytree("crates/casars-imexplore/tests/data/imhead_beams/degrees.im", single_source)
        exportfits(imagename=str(single_source), fitsimage=str(single_fits), overwrite=True)
        shutil.copyfile(single_fits, ROOT / "single.fits")
        single_cases = {}
        for mode in ("summary", "list"):
            value = imhead(str(single_fits), mode=mode)
            single_cases[f"{mode}:"] = {"structure": describe(value), "value": value}
        velocity_source = Path(scratch) / "velocity.im"
        velocity_fits = Path(scratch) / "velocity.fits"
        shutil.copytree(ROOT.parent / "imhead_crpix/conversion.im", velocity_source)
        imhead(str(velocity_source), mode="put", hdkey="cunit4", hdvalue="GHz")
        exportfits(imagename=str(velocity_source), fitsimage=str(velocity_fits), velocity=True, overwrite=True)
        shutil.copyfile(velocity_fits, ROOT / "velocity.fits")
        velocity_cases = {}
        for mode, key in [("summary", ""), ("list", ""), ("get", "restfreq")]:
            value = imhead(str(velocity_fits), mode=mode, hdkey=key)
            velocity_cases[f"{mode}:{key}"] = {"structure": describe(value), "value": value}
        velocity_no_rest = Path(scratch) / "velocity-no-rest.fits"
        velocity_no_rest.write_bytes(velocity_fits.read_bytes().replace(b"RESTFRQ =", b"COMMENT  ", 1))
        shutil.copyfile(velocity_no_rest, ROOT / "velocity-no-rest.fits")
        no_rest_cases = {}
        for mode, key in [("summary", ""), ("list", ""), ("get", "restfreq")]:
            value = imhead(str(velocity_no_rest), mode=mode, hdkey=key)
            no_rest_cases[f"{mode}:{key}"] = {"structure": describe(value), "value": value}
        (ROOT / "expected.json").write_text(json.dumps(
            {"casa_version": version_string(), "cases": cases, "integer_cases": integer_cases, "scaled_cases": scaled_cases, "velocity_cases": velocity_cases, "no_rest_cases": no_rest_cases, "beam_cases": beam_cases, "single_cases": single_cases, "history": history}, indent=2,
            default=lambda value: value.tolist(),
        ) + "\n")


if __name__ == "__main__":
    main()
