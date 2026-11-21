"""Public image reads select strided pixels with mixed-type LEL operands."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


@pytest.mark.parametrize("kind", [
    "real", "fits", "complex", "dcomplex", "mixed-complex",
])
def test_getregion_and_statistics_accept_mixed_mask_operands(casa, tmp_path, kind):
    x, y = np.ogrid[:5, :6]
    pixels = x + 10 * y
    section = (slice(1, 5, 2), slice(0, 6, 2))
    expected = pixels[section]
    selected = expected[expected > 20]
    manager = casa.regionmanager()
    try:
        region = manager.box([1, 0], [4, 5], [2, 2])
    finally:
        manager.done()

    for writer in (casa, tools):
        paths = {}
        for code in ("f", "d", "c", "cd"):
            path = str(tmp_path / f"{writer.__name__}-{code}.im")
            image = writer.image()
            try:
                assert image.fromshape(path, list(pixels.shape), type=code)
                values = pixels * (1 + 2j) if code.startswith("c") else pixels
                assert image.putchunk(values)
                paths[code] = path
                if code == "f" and kind == "fits":
                    path = str(tmp_path / f"{writer.__name__}.fits")
                    assert image.tofits(path)
                    paths["fits"] = path
            finally:
                image.done()
        f, d, c, cd = (f'"{paths[code]}"' for code in ("f", "d", "c", "cd"))
        if kind == "fits":
            expression = f'("{paths["fits"]}" + {d}) > 40'
        else:
            expression = {
                "real": f"({f} + {d}) > 40",
                "complex": f"real({c}) > 20",
                "dcomplex": f"imag({cd}) > 40",
                "mixed-complex": f"real({c} + {cd}) > 40",
            }[kind]

        for reader in (casa, tools):
            image = reader.image()
            try:
                assert image.open(paths["f"])
                options = dict(region=region, mask=expression)
                np.testing.assert_array_equal(image.getregion(**options), expected)
                np.testing.assert_array_equal(
                    image.getregion(**options, getmask=True), expected > 20,
                )
                stats = image.statistics(**options)
                assert stats["npts"].item() == selected.size
                assert stats["sum"].item() == selected.sum()
            finally:
                image.done()


def test_combined_mask_checks_lattice_operands_after_scalar_reductions(casa, tmp_path):
    operands = {
        "float": ((3, 4), "f", False),
        "linear": ((3, 4), "d", True),
        "column": ((3, 1), "d", False),
        "row": ((1, 4), "f", False),
        "linear_column": ((3, 1), "d", True),
    }
    names = {}
    for key, (shape, kind, linear) in operands.items():
        path = str(tmp_path / f"{key}.im")
        image = casa.image()
        try:
            assert image.fromshape(path, list(shape), type=kind, linear=linear)
            assert image.putchunk(np.ones(shape))
        finally:
            image.done()
        names[key] = f'"{path}"'

    for api in (casa, tools):
        target = api.image()
        try:
            assert target.fromarray(pixels=np.arange(12.0).reshape(3, 4))
            f, lin, column, row, lincol = names.values()
            for expression in (f"({f} + sum({lin})) > 0", f"({f} + {column}) > 0"):
                np.testing.assert_array_equal(target.getregion(mask=expression, getmask=True), True)
                stats = target.statistics(mask=expression)
                assert stats["npts"].item() == 12
                assert stats["sum"].item() == 66

            bad = (
                (f"({column} + {row}) > 0", "shapes of operands mismatch"),
                (f"{column} > {row}", "shapes of operands mismatch"),
                (f"({f} + {lincol}) > 0", "coordinates of operands mismatch"),
                (f"pow({f}, {column}) > 0", "shapes of operands mismatch"),
            )
            for expression, error in bad:
                with pytest.raises(RuntimeError, match=error):
                    target.getregion(mask=expression, stretch=True, getmask=True)
                with pytest.raises(RuntimeError, match=error):
                    target.statistics(mask=expression, stretch=True)
            np.testing.assert_array_equal(target.getregion(), np.arange(12.0).reshape(3, 4))
        finally:
            target.done()
