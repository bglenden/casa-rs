"""Spectral Doppler conversions against CASA and analytic frequency ratios."""

import numpy as np
import pytest

from casars import tools


LIGHT_KM_S = 299792.458


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def _spectral(backend):
    cs = backend.coordsys().newcoordsys(direction=True, spectral=True)
    assert cs.setunits(["GHz"], "spectral")
    assert cs.setrestfrequency(2.0)
    return cs


@pytest.mark.parametrize("convention", ["RADIO", "OPTICAL", "BETA", "RATIO", "GAMMA"])
def test_spectral_velocities_follow_the_analytic_frequency_ratio(casa, convention):
    frequencies = np.array([0.8, 1.5, 1.95])
    ratio = frequencies / 2
    fractions = {
        "RADIO": 1 - ratio,
        "OPTICAL": 1 / ratio - 1,
        "BETA": (1 - ratio**2) / (1 + ratio**2),
        "RATIO": ratio,
        "GAMMA": (ratio + 1 / ratio) / 2,
    }
    expected = LIGHT_KM_S * fractions[convention]
    # The gamma convention discards the sign of beta. Use redshifted inputs
    # for the inverse, as documented by the analytic branch of gamma -> beta.
    for backend in (tools, casa):
        cs = _spectral(backend)
        result = cs.frequencytovelocity(frequencies, doppler=convention)
        assert isinstance(result, np.ndarray) and result.dtype == np.float64
        np.testing.assert_allclose(result, expected, rtol=1e-12, atol=1e-9)
        np.testing.assert_allclose(cs.velocitytofrequency(result, doppler=convention), frequencies, rtol=1e-11)
        np.testing.assert_allclose(
            cs.frequencytovelocity(frequencies * 1e3, "MHz", convention, "m/s"),
            expected * 1e3,
            rtol=1e-12,
        )


def test_shift_accepts_velocity_quantities_and_reciprocal_frequency_units(casa):
    values = np.array([-1.5, 0.0, 1.5, 2.0])
    for backend in (tools, casa):
        cs = _spectral(backend)
        np.testing.assert_allclose(cs.frequencytofrequency(values), values)
        for requested in (0.6, "179875.4748km/s", {"value": 179875474.8, "unit": "m/s"}):
            np.testing.assert_allclose(cs.frequencytofrequency(values, velocity=requested), values / 2)
            np.testing.assert_allclose(cs.frequencytofrequency(values, "m", requested), values * 2)
            np.testing.assert_allclose(cs.frequencytofrequency(values, "s", requested), values * 2)
            np.testing.assert_allclose(cs.frequencytofrequency(values, "rad/s", requested), values / 2)
        assert cs.frequencytofrequency([], "Jy").shape == (0,)
        with pytest.raises(RuntimeError, match="non-conforming unit type 'Jy'"):
            cs.frequencytofrequency(values, "Jy", 0.6)
        with pytest.raises(RuntimeError, match="non-conforming unit type 'm'"):
            cs.frequencytofrequency(values, "Hz", "2m")
        no_spectral = backend.coordsys().newcoordsys(linear=1)
        np.testing.assert_allclose(no_spectral.frequencytofrequency(values, "Hz", 0.6), values / 2)
        with pytest.raises(RuntimeError, match="non-conforming unit type ''"):
            no_spectral.frequencytofrequency(values)


def test_conversion_uses_first_spectral_axis_and_handles_vector_and_unit_inputs(casa):
    samples = np.array([[1.25, 1.5], [1.75, 2.0]], dtype=np.float32)
    expected = LIGHT_KM_S * (1 - samples.ravel(order="F").astype(np.float64) / 2)
    for backend in (tools, casa):
        cs = _spectral(backend)
        for source in (samples, samples.tolist(), samples[::-1, :][::-1, :]):
            np.testing.assert_allclose(cs.frequencytovelocity(source), expected, atol=1e-9)
        np.testing.assert_allclose(cs.frequencytovelocity(1.5), [LIGHT_KM_S / 4])
        np.testing.assert_allclose(cs.frequencytovelocity(np.array(1.5)), [LIGHT_KM_S / 4])
        np.testing.assert_allclose(cs.frequencytovelocity("1.5"), [LIGHT_KM_S / 2])
        np.testing.assert_allclose(cs.frequencytovelocity(["1.5xyz", "x"]), [LIGHT_KM_S / 4, LIGHT_KM_S])
        np.testing.assert_allclose(cs.frequencytovelocity([1.5], doppler="RAT"), [LIGHT_KM_S * 0.75])
        np.testing.assert_allclose(cs.frequencytovelocity([1.5], doppler="z1"), [LIGHT_KM_S / 3])
        np.testing.assert_allclose(cs.frequencytovelocity([1.5], doppler="unknown"), [LIGHT_KM_S / 4])
        for call in (cs.frequencytovelocity, cs.velocitytofrequency):
            with pytest.raises(RuntimeError, match="Unknown unit"):
                call([1.5], frequnit="foo")
            with pytest.raises(RuntimeError, match="Units are not compatible dimensionally"):
                call([1.5], frequnit="m")
            with pytest.raises(RuntimeError, match="Illegal unit string 'foo'"):
                call([1.5], velunit="foo")
            with pytest.raises(SystemError, match="returned a result with an exception set"):
                call((1.0, 2.0))
        record = cs.torecord()
        record["spectral1"]["velUnit"] = "m/s"
        record["spectral1"]["velType"] = 1
        assert cs.fromrecord(record)
        np.testing.assert_allclose(cs.frequencytovelocity([1.5], doppler="RATIO", velunit="Hz"), [LIGHT_KM_S * 1e3 / 3])
        np.testing.assert_allclose(cs.frequencytovelocity([1.5], doppler="radio", velunit=""), [LIGHT_KM_S * 1e3 / 4])
        assert cs.torecord()["spectral1"]["restfreq"] == record["spectral1"]["restfreq"]


def test_nonfinite_inputs_and_zero_rest_frequency(casa):
    for backend in (tools, casa):
        cs = _spectral(backend)
        np.testing.assert_allclose(cs.frequencytovelocity([-1.5, 0.0]), [LIGHT_KM_S / 4, LIGHT_KM_S])
        assert np.isnan(cs.frequencytovelocity([np.inf, np.nan])).all()
        assert np.isnan(cs.velocitytofrequency([np.inf, np.nan])).all()
        np.testing.assert_allclose(cs.velocitytofrequency([400000.], doppler="radio"), [2 * abs(1 - 400000 / LIGHT_KM_S)])
        assert np.isnan(cs.velocitytofrequency([400000.], doppler="beta")).all()
        assert cs.setrestfrequency(0.0)
        assert np.isnan(cs.frequencytovelocity([1.5])).all()
        np.testing.assert_array_equal(cs.velocitytofrequency([0.0]), [0.0])


@pytest.mark.parametrize("legacy,stored,scale,fraction", [(True, "m/s", 1000, 1 / 3), (False, "", 1, 1 / 3), (False, "Hz", 1, 1 / 4)])
def test_restored_velocity_preferences_use_effective_units_and_conventions(casa, legacy, stored, scale, fraction):
    for backend in (tools, casa):
        cs = _spectral(backend)
        record = cs.torecord()
        spectral = record["spectral1"]
        prefix = "vel"
        if legacy:
            del spectral["velUnit"]
            del spectral["velType"]
            prefix = "prefVel"
        spectral[prefix + "Unit"] = stored
        spectral[prefix + "Type"] = 1
        assert cs.fromrecord(record)
        default_radio = scale * LIGHT_KM_S / 4
        fallback = scale * LIGHT_KM_S * fraction
        np.testing.assert_allclose(cs.frequencytovelocity([1.5], velunit=""), [default_radio])
        np.testing.assert_allclose(cs.frequencytovelocity([1.5], doppler="RATIO", velunit="Hz"), [fallback])
        np.testing.assert_allclose(cs.velocitytofrequency([fallback], doppler="RATIO", velunit="Hz"), [1.5])


def test_shift_preserves_individual_vector_input_types(casa):
    for backend in (tools, casa):
        cs = _spectral(backend)
        np.testing.assert_array_equal(cs.frequencytofrequency("1.5"), [-1.0])
        np.testing.assert_array_equal(cs.frequencytofrequency(""), [-1.0])
        np.testing.assert_array_equal(cs.frequencytofrequency([True, "1.5"]), [1.0, 1.5])
        np.testing.assert_array_equal(cs.frequencytofrequency([1.0, None, 2.0]), [1.0, 0.0, 2.0])


def test_extreme_gamma_does_not_round_a_finite_result_to_infinity(casa):
    for backend in (tools, casa):
        cs = _spectral(backend)
        np.testing.assert_allclose(cs.frequencytovelocity([2e-8], doppler="GAMMA"), [2.011873129214771e13])
        assert np.isnan(cs.velocitytofrequency([np.inf], doppler="GAMMA")).all()


def test_spectral_vectors_respect_non_native_array_byte_order():
    cs = _spectral(tools)
    samples = np.array([1.0, 1.5, 2.0], dtype=np.dtype("float64").newbyteorder("S"))
    np.testing.assert_array_equal(cs.frequencytofrequency(samples), samples)
    np.testing.assert_allclose(cs.frequencytovelocity(samples), LIGHT_KM_S * (1 - samples / 2))
    np.testing.assert_allclose(cs.velocitytofrequency(samples), 2 * (1 - samples / LIGHT_KM_S))


def test_doppler_conversions_safely_handle_missing_input():
    cs = _spectral(tools)
    for call in (cs.frequencytovelocity, cs.velocitytofrequency):
        for source in ([], np.array([]), None):
            result = call(source)
            assert result.shape == (0,) and result.dtype == np.float64
    missing = tools.coordsys().newcoordsys(linear=1)
    for call in (missing.frequencytovelocity, missing.velocitytofrequency):
        with pytest.raises(RuntimeError, match="There is no spectral coordinate"):
            call([1.0])


def test_doppler_conversions_after_native_image_round_trip(casa, tmp_path):
    frequencies = np.array([0.8, 1.5, 1.95])
    for index, writer in enumerate((tools, casa)):
        path = str(tmp_path / f"doppler-{index}.im")
        image = writer.image()
        assert image.fromshape(path, [4, 4, 8], csys=_spectral(writer).torecord())
        assert image.done()
        for reader in (tools, casa):
            image = reader.image()
            assert image.open(path)
            cs = image.coordsys()
            velocities = LIGHT_KM_S * (1 - frequencies / 2)
            np.testing.assert_allclose(cs.frequencytovelocity(frequencies), velocities, atol=1e-9)
            np.testing.assert_allclose(cs.velocitytofrequency(velocities), frequencies)
            np.testing.assert_allclose(cs.frequencytofrequency(frequencies, velocity=0.6), frequencies / 2)
            assert image.done()
