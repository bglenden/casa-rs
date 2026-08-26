"""Small shared helpers for the focused CASA solver oracles."""

from __future__ import annotations

import pathlib
import shutil

import numpy as np


DEFAULT_IMAGE_STORE_SUFFIXES = ("psf", "residual", "model", "sumwt", "mask")


def materialize_solver_seed(
    tclean_task, ms_path: pathlib.Path, prefix: pathlib.Path
) -> pathlib.Path:
    """Materialize the one bounded image-store geometry used by solver gates."""
    tclean_task(
        vis=str(ms_path),
        field="1",
        spw="1",
        imagename=str(prefix),
        imsize=[64, 64],
        cell="0.02arcsec",
        phasecenter=1,
        specmode="mfs",
        datacolumn="data",
        gridder="standard",
        stokes="I",
        weighting="natural",
        deconvolver="clark",
        # One throwaway iteration asks CASA to materialize the complete image
        # store. The controlled gate replaces both model and residual before
        # recording any solver result.
        niter=1,
        cycleniter=1,
        gain=0.1,
        threshold="0Jy",
        usemask="user",
        restoration=False,
        pbcor=False,
        savemodel="none",
        calcpsf=True,
        calcres=True,
        interactive=False,
        verbose=False,
    )
    return prefix


def controlled_point_extended_fixture(psf: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Return the canonical 64x64 point-plus-Gaussian sky and dirty plane."""
    psf = np.asarray(psf, dtype=np.float64)
    if psf.shape != (64, 64):
        raise AssertionError(
            f"controlled solver fixture requires 64x64 PSF, got {psf.shape}"
        )
    axis0, axis1 = np.indices(psf.shape)
    sky = np.zeros(psf.shape, dtype=np.float64)
    sky[22, 21] = 4.0
    sky += 0.2 * np.exp(-((axis1 - 43.0) ** 2 + (axis0 - 42.0) ** 2) / 32.0)
    dirty = np.fft.ifft2(
        np.fft.fft2(sky) * np.fft.fft2(np.fft.ifftshift(psf))
    ).real
    return sky, dirty


def read_plane(path: pathlib.Path) -> np.ndarray:
    """Read one CASA image plane without importing CASA in the caller's parent."""
    from casatools import image

    tool = image()
    tool.open(str(path))
    try:
        return np.asarray(tool.getchunk()).squeeze().astype(np.float64)
    finally:
        tool.close()


def read_validity(path: pathlib.Path) -> np.ndarray:
    """Read one CASA image's pixel-validity mask independently of its values."""
    from casatools import image

    tool = image()
    tool.open(str(path))
    try:
        return np.asarray(tool.getchunk(getmask=True)).squeeze().astype(bool)
    finally:
        tool.close()


def write_plane(path: pathlib.Path, values: np.ndarray) -> None:
    """Write one float32 plane to an existing CASA image."""
    from casatools import image

    tool = image()
    tool.open(str(path))
    try:
        tool.putchunk(np.asarray(values, dtype=np.float32)[:, :, None, None])
    finally:
        tool.close()


def normalize(value):
    """Convert CASA/numpy values into JSON-native values recursively."""
    if isinstance(value, dict):
        return {str(key): normalize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize(item) for item in value]
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    return value


def copy_seed(
    seed: pathlib.Path,
    target: pathlib.Path,
    suffixes: tuple[str, ...] = DEFAULT_IMAGE_STORE_SUFFIXES,
) -> None:
    """Copy the requested CASA image-store members from one prefix to another."""
    for suffix in suffixes:
        source = pathlib.Path(f"{seed}.{suffix}")
        destination = pathlib.Path(f"{target}.{suffix}")
        if destination.exists():
            shutil.rmtree(destination)
        shutil.copytree(source, destination)
