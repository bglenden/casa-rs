"""Small shared helpers for the focused CASA solver oracles."""

from __future__ import annotations

import pathlib
import shutil

import numpy as np


DEFAULT_IMAGE_STORE_SUFFIXES = ("psf", "residual", "model", "sumwt", "mask")


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
