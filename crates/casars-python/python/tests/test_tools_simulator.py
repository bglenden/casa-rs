"""`casars.tools.simulator`: CASA's `sm` tool (ADR-0019, #841).

The signatures are compared with the installed CASA binding's, recorded by
`tools/simulation/dump_casa_tool_signatures.py` in
`data/casa_simulator_signatures.json` (and with a live CASA when one is
importable), and with the object-surface bundle built from the same Rust
table. The behaviour tests follow CASA 6.7.6's probed outcomes.
"""

from __future__ import annotations

import gc
import inspect
import json
import logging
from pathlib import Path
import subprocess
import sys
import threading
import time
from typing import Any
import warnings

import numpy as np
import pytest

import casars
from casars.data import Table, schema_bundle
from casars.tools import simulator

DATA = Path(__file__).parent / "data"
LOGGER = "casars.tools.simulator"
CASA = json.loads((DATA / "casa_simulator_signatures.json").read_text(encoding="utf-8"))

# Four VLA pads, ITRF metres.
X = [-1601185.4, -1601155.4, -1601125.4, -1601095.4]
Y = [-5041977.5, -5041977.5, -5041977.5, -5041977.5]
Z = [3554875.9, 3554875.9, 3554875.9, 3554875.9]
# Near transit at the VLA at 2020-01-01 00:00 UTC.
DIRECTION = "J2000 23h30m00 34d00m00"
REFERENCE = "UTC 2020/01/01/00:00:00"

IMPLEMENTED = {
    "open", "close", "done", "name", "type", "summary", "setconfig", "setfeed",
    "setspwindow", "setfield", "settimes", "setlimits", "setauto", "observe",
    "setseed", "setnoise", "corrupt", "reset", "predict", "setoptions", "setvp",
}
NOT_YET_IMPLEMENTED = {
    "openfromms", "observemany", "setmosaicfield", "setdata", "setapply", "setgain",
    "settrop", "setleakage",
}
DISABLED_IN_CASA = {"setbandpass", "setpa", "setpointingerror"}


@pytest.fixture(scope="module")
def measures(tmp_path_factory: pytest.TempPathFactory) -> None:
    """Skip when no measures runtime is installed: `open` loads one.

    `scripts/test-python-package.sh` provides the bundled snapshot.
    """
    sm = simulator()
    try:
        sm.open(str(tmp_path_factory.mktemp("measures") / "probe.ms"))
    except RuntimeError as error:
        if "measures data" not in str(error):
            raise
        pytest.skip(f"no measures runtime: {error}")
    sm.close()


needs_measures = pytest.mark.usefixtures("measures")


def public_methods(cls: type) -> set[str]:
    return {name for name in dir(cls) if not name.startswith("_") and callable(getattr(cls, name))}


def describe(function: Any) -> list[dict[str, str | None]]:
    """Parameters without `self`, as the dump script records them."""
    described = []
    for parameter in inspect.signature(function).parameters.values():
        if parameter.name == "self":
            continue
        default = None if parameter.default is inspect.Parameter.empty else repr(parameter.default)
        described.append({"name": parameter.name, "kind": parameter.kind.name, "default": default})
    return described


def test_the_tool_is_importable_from_casars_tools() -> None:
    assert casars.tools.simulator is simulator
    assert simulator.__module__ == "casars.tools"
    assert simulator().type() == "Simulator"


def test_every_casa_method_exists_with_casas_signature() -> None:
    assert public_methods(simulator) == set(CASA["methods"])
    assert IMPLEMENTED | NOT_YET_IMPLEMENTED | DISABLED_IN_CASA | {"oldsetnoise"} == set(
        CASA["methods"]
    )
    for name, parameters in CASA["methods"].items():
        assert describe(getattr(simulator, name)) == parameters, name


def test_signatures_match_a_live_casa_when_one_is_importable() -> None:
    casatools = pytest.importorskip("casatools")
    live = casatools.simulator
    assert public_methods(simulator) == {
        name for name, _ in inspect.getmembers(live, inspect.isfunction) if not name.startswith("_")
    }
    for name in public_methods(simulator):
        assert describe(getattr(simulator, name)) == describe(getattr(live, name)), name


CONSTRUCTOR = [("memory_budget", None), ("backend", "auto"), ("workers", None)]


def test_the_constructor_adds_only_keyword_only_controls() -> None:
    # CASA's constructor takes (*args, **kwargs) and ignores them.
    assert [entry["kind"] for entry in CASA["constructor"]] == ["VAR_POSITIONAL", "VAR_KEYWORD"]
    parameters = inspect.signature(simulator).parameters.values()
    assert [(p.name, p.kind, p.default) for p in parameters] == [
        (name, inspect.Parameter.KEYWORD_ONLY, default) for name, default in CONSTRUCTOR
    ]
    with pytest.raises(TypeError):
        simulator(1)  # type: ignore[misc]


@pytest.mark.parametrize(
    ("controls", "message"),
    [
        ({"memory_budget": 0}, "memory_budget must be a positive number of bytes, not 0"),
        ({"memory_budget": -1}, "memory_budget must be a positive number of bytes, not -1"),
        ({"workers": 0}, "workers must be a positive number of threads, not 0"),
        ({"workers": -2}, "workers must be a positive number of threads, not -2"),
        ({"backend": "gpu"}, "backend must be 'auto', 'cpu' or 'metal', not 'gpu'"),
        ({"backend": "CPU"}, "backend must be 'auto', 'cpu' or 'metal', not 'CPU'"),
    ],
)
def test_constructor_controls_out_of_range_raise_value_error(
    controls: dict[str, Any], message: str
) -> None:
    with pytest.raises(ValueError) as raised:
        simulator(**controls)
    assert str(raised.value) == message


def test_the_constructor_takes_each_backend() -> None:
    assert simulator(backend="auto", workers=1, memory_budget=1 << 30).type() == "Simulator"
    assert simulator(backend="cpu").type() == "Simulator"
    # 'metal' needs a device: on a machine without one it raises at once.
    try:
        simulator(backend="metal")
    except RuntimeError as error:
        assert "Metal device" in str(error)


def test_the_bundle_describes_the_class_signatures() -> None:
    bundle = schema_bundle()
    objects = {entry["name"]: entry for entry in bundle["semantic"]["objects"]}
    contract = objects["simulator"]
    projection = next(
        entry for entry in bundle["projections"]["python"]["objects"] if entry["name"] == "simulator"
    )
    assert (projection["module"], projection["class_name"]) == ("casars.tools", "simulator")
    methods = {method["name"]: method for method in contract["methods"]}
    assert set(methods) == public_methods(simulator)
    assert set(projection["method_parameters"]) == set(methods)
    for name, method in methods.items():
        properties = method["parameters_schema"].get("properties", {})
        order = projection["method_parameters"][name]
        assert set(order) == set(properties), name
        signature = inspect.signature(getattr(simulator, name)).parameters
        found = [p for p in signature.values() if p.name != "self"]
        assert [p.name for p in found] == order, name
        for parameter in found:
            default = properties[parameter.name]["default"]
            assert parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
            assert parameter.default == default, (name, parameter.name)
            assert type(parameter.default) is type(default), (name, parameter.name)
    constructor = contract["constructors"][0]["parameters_schema"]["properties"]
    # The schema's properties are unordered; the projection carries the order.
    assert projection["constructor_keyword_only"] == [name for name, _ in CONSTRUCTOR]
    assert set(constructor) == {name for name, _ in CONSTRUCTOR}
    for name, default in CONSTRUCTOR:
        assert constructor[name]["default"] == default, name
    status = bundle["annotations"]["python"]["objects"]["simulator"]["method_status"]
    assert {name for name, value in status.items() if value == "implemented"} == IMPLEMENTED
    assert {name for name, value in status.items() if value == "not_yet_implemented"} == (
        NOT_YET_IMPLEMENTED
    )


def configure(sm: simulator, *, autocorrelations: bool = True) -> None:
    assert sm.setconfig(
        telescopename="VLA", x=X, y=Y, z=Z, dishdiameter=[25.0], mount=["ALT-AZ"],
        antname=["A1", "A2", "A3", "A4"], padname=["P1", "P2", "P3", "P4"],
        coordsystem="global",
    )
    assert sm.setspwindow(
        spwname="L", freq="1.4GHz", deltafreq="1MHz", freqresolution="1MHz", nchannels=4,
        stokes="RR LL",
    )
    assert sm.settimes(integrationtime="10s", usehourangle=False, referencetime=REFERENCE)
    assert sm.setfield(sourcename="F", sourcedirection=DIRECTION)
    if not autocorrelations:
        assert sm.setauto(0.0)


@needs_measures
def test_an_sm_script_writes_the_measurement_set(tmp_path: Path) -> None:
    ms = tmp_path / "sm.ms"
    sm = simulator()
    assert sm.open(str(ms))
    assert sm.name() == str(ms)
    configure(sm)
    assert sm.observe("F", "L", starttime="0s", stoptime="30s")
    assert not ms.exists()
    assert sm.close()
    assert sm.name() == ""

    main = Table.open(ms)
    # 4 antennas with autocorrelations (CASA writes them until setauto(0)):
    # 10 baselines in each of 3 integrations.
    assert main.row_count == 30
    assert {"ANTENNA1", "ANTENNA2", "DATA", "FLAG", "TIME", "UVW", "WEIGHT"} <= set(
        main.column_names
    )
    times = main.get_column("TIME")
    reference_s = 58849.0 * 86400.0
    assert np.allclose(np.unique(times), reference_s + np.array([5.0, 15.0, 25.0]))
    assert Table.open(ms / "ANTENNA").get_column("NAME") == ["A1", "A2", "A3", "A4"]
    assert Table.open(ms / "FIELD").get_column("NAME") == ["F"]
    assert np.allclose(
        Table.open(ms / "SPECTRAL_WINDOW").get_column("CHAN_FREQ"),
        1.4e9 + 1e6 * np.arange(4),
    )


@needs_measures
def test_setauto_zero_drops_the_autocorrelations(tmp_path: Path) -> None:
    sm = simulator()
    sm.open(str(tmp_path / "cross.ms"))
    configure(sm, autocorrelations=False)
    assert sm.observe("F", "L", starttime="0s", stoptime="30s")
    sm.done()
    assert Table.open(tmp_path / "cross.ms").row_count == 18


@needs_measures
def test_open_writes_the_previous_session(tmp_path: Path) -> None:
    sm = simulator(memory_budget=256 << 20)
    sm.open(str(tmp_path / "first.ms"))
    configure(sm)
    sm.observe("F", "L", starttime="0s", stoptime="10s")
    sm.open(str(tmp_path / "second.ms"))
    assert Table.open(tmp_path / "first.ms").row_count == 10
    assert sm.name() == str(tmp_path / "second.ms")
    sm.close()


@needs_measures
def test_a_write_failure_at_close_raises(tmp_path: Path) -> None:
    blocker = tmp_path / "not-a-directory"
    blocker.write_text("", encoding="utf-8")
    sm = simulator()
    sm.open(str(blocker / "x.ms"))
    configure(sm)
    sm.observe("F", "L", starttime="0s", stoptime="10s")
    with pytest.raises(RuntimeError):
        sm.close()
    # The session is gone either way.
    assert sm.name() == ""
    assert sm.close() is True


def test_methods_before_open_return_false_and_log_only_setconfig(
    caplog: pytest.LogCaptureFixture,
) -> None:
    sm = simulator()
    with caplog.at_level(logging.INFO, logger=LOGGER):
        assert sm.name() == ""
        assert sm.type() == "Simulator"
        assert sm.summary() is False
        # Arguments are not converted before open, as in CASA.
        assert sm.settimes(integrationtime="10 furlong") is False
        assert sm.setfield(sourcedirection="J2000 10h 20d") is False
        assert sm.setspwindow(refcode="BOGUS") is False
        assert sm.observe("F", "L") is False
        assert sm.setlimits() is False
        assert sm.setauto() is False
        assert sm.setfeed(mode="perfect R L") is False
        assert sm.setbandpass() is False
        assert sm.setseed() is False
        assert sm.setnoise(simplenoise="10 furlong") is False
        assert sm.corrupt() is False
        assert sm.reset() is False
        assert sm.setoptions(ftmachine="mosaic", location="garbage") is False
        assert sm.setvp(vptable="vp.tab", usedefaultvp=False, parangleinc="10 furlong") is False
        assert sm.predict() is False
        assert caplog.messages == []
        assert sm.setconfig() is False
        assert caplog.messages == ["The sm tool must be opened first."]
        assert sm.close() is True
        assert sm.done() is True


def opened(tmp_path: Path) -> simulator:
    sm = simulator()
    sm.open(str(tmp_path / "x.ms"))
    return sm


@needs_measures
def test_casa_refusals_log_an_error_and_return_false(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    sm = opened(tmp_path)
    configure(sm)
    with caplog.at_level(logging.ERROR, logger=LOGGER):
        assert sm.setfeed(mode="") is False
        assert sm.setspwindow(spwname="none", nchannels=0) is False
        assert sm.setfield(sourcename="") is False
        assert sm.observe("F", "X", starttime="0s", stoptime="10s") is False
        assert sm.observe("G", "L", starttime="0s", stoptime="10s") is False
        assert sm.setconfig(x=[0.0], y=[0.0], z=[0.0], coordsystem="Global") is False
        assert sm.setpa() is False
    assert caplog.messages == [
        "Currently, only perfect R L or perfect X Y feeds or list are recognized",
        "must provide nchannels",
        # The default direction `[]` is CASA's unconvertible direction too.
        "Could not convert source direction to a Direction Measure.",
        "must provide a source name",
        "Spectral window named X not yet defined",
        "Field named G not yet defined",
        "Unknown coordinate system type: Global",
        "Caught exception: Corruption by simulated errors temporarily disabled (06Nov20 gmoellen)",
    ]
    sm.close()


@needs_measures
def test_casa_conversion_failures_log_and_continue(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    sm = opened(tmp_path)
    configure(sm)
    with caplog.at_level(logging.ERROR, logger=LOGGER):
        # CASA logs these and continues at the J2000 pole and MJD 0.
        assert sm.setfield(sourcename="pole", sourcedirection="garbage") is True
        assert sm.settimes(integrationtime="10s", referencetime="") is True
        assert sm.setconfig(x=[0.0], y=[0.0], z=[0.0], coordsystem="local", referencelocation=1.0)
    assert caplog.messages == [
        "Could not convert source direction to a Direction Measure.",
        "Could not convert referencetime to an Epoch Measures",
        "Could not convert referencelocation 1 to a Position Measures",
    ]
    sm.close()
    phase = Table.open(tmp_path / "x.ms" / "FIELD").get_column("PHASE_DIR")
    assert np.allclose(phase[1].ravel(), [0.0, np.pi / 2])


@pytest.mark.parametrize(
    ("call", "message"),
    [
        (lambda sm: sm.settimes(integrationtime="-1s"), "Failed AlwaysAssert"),
        (lambda sm: sm.settimes(integrationtime="10 furlong"), "in converting quantity"),
        (lambda sm: sm.settimes(integrationtime="10Jy"), "Cannot convert Jy to s"),
        (lambda sm: sm.settimes(referencetime="12:30:00"), "non-conforming unit type 'deg'"),
        (lambda sm: sm.setfield(sourcedirection="J2000 10h 20d"), "cos illegal unit type 'h'"),
        (lambda sm: sm.setspwindow(refcode="BOGUS"), "Invalid frequency reference 'BOGUS'"),
        (lambda sm: sm.setconfig(x=[0.0, 1.0], y=[0.0], z=[0.0, 1.0]), "Failed AlwaysAssert"),
        (lambda sm: sm.setconfig(referencelocation="NOWHERE"), "Could not understand Position"),
        (lambda sm: sm.oldsetnoise(), "deprecated in CASA"),
        (lambda sm: sm.setnoise(simplenoise="0.1K"), "Cannot convert K to Jy"),
        (lambda sm: sm.setnoise(pground="garbage"), "in converting quantity"),
    ],
)
@needs_measures
def test_casa_exceptions_raise_runtime_error(tmp_path: Path, call: Any, message: str) -> None:
    sm = opened(tmp_path)
    with pytest.raises(RuntimeError, match=message):
        call(sm)
    sm.close()


@pytest.mark.parametrize("method", sorted(NOT_YET_IMPLEMENTED))
def test_unimplemented_methods_raise_not_implemented(method: str) -> None:
    with pytest.raises(NotImplementedError, match=f"{method} is not yet implemented in casa-rs"):
        getattr(simulator(), method)()


@needs_measures
def test_vector_arguments_take_scalars_and_sequences(tmp_path: Path) -> None:
    sm = opened(tmp_path)
    assert sm.setconfig(x=(X[0], X[1]), y=np.array(Y[:2]), z=Z[:2], antname="A", mount="ALT-AZ")
    assert sm.setfeed(mode="perfect X Y", x=0.0, y=[0.0], pol="X")
    with pytest.raises(TypeError):
        sm.setconfig(x="0.0")
    sm.close()
    antennas = Table.open(tmp_path / "x.ms" / "ANTENNA")
    assert antennas.get_column("NAME") == ["A", "A"]


@needs_measures
def test_a_dropped_tool_with_planned_scans_warns_and_writes_nothing(tmp_path: Path) -> None:
    ms = tmp_path / "dropped.ms"
    sm = simulator()
    sm.open(str(ms))
    configure(sm)
    sm.observe("F", "L", starttime="0s", stoptime="10s")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        del sm
        gc.collect()
    assert [w.category for w in caught] == [ResourceWarning]
    assert "nothing was written" in str(caught[0].message)
    assert not ms.exists()


def write_session(path: Path, explicit: bool) -> None:
    """One session through defaults omitted, or passed explicitly as CASA's."""
    sm = simulator()
    sm.open(str(path))
    if explicit:
        sm.setconfig(
            telescopename="VLA", x=X, y=Y, z=Z, dishdiameter=[0.0], offset=[0.0],
            mount=["ALT-AZ"], antname=["A"], padname=["P"], coordsystem="global",
            referencelocation=[],
        )
        sm.setfeed(mode="perfect R L", x=[0.0], y=[0.0], pol=["R"])
        sm.setspwindow(
            spwname="XBAND", freq="1.4GHz", deltafreq="1MHz", freqresolution=[],
            refcode="TOPO", nchannels=1, stokes="RR LL",
        )
        sm.settimes(integrationtime="10s", usehourangle=True, referencetime="2020/01/01")
        sm.setfield(sourcename="SOURCE", sourcedirection=DIRECTION, calcode="", distance=[])
        sm.setlimits(shadowlimit=1e-06, elevationlimit=[])
        sm.setauto(autocorrwt=0.0)
        assert sm.observe(
            sourcename="SOURCE", spwname="XBAND", starttime="0s", stoptime="30s",
            add_observation=False, state_sig=True, state_ref=False, state_cal=0.0,
            state_load=0.0, state_sub_scan=0, state_obs_mode="OBSERVE_TARGET.ON_SOURCE",
            observer="CASA simulator", project="CASA simulation",
        )
    else:
        sm.setconfig(x=X, y=Y, z=Z)
        sm.setfeed(mode="perfect R L")
        sm.setspwindow(freq="1.4GHz", deltafreq="1MHz")
        sm.settimes(integrationtime="10s", referencetime="2020/01/01")
        sm.setfield(sourcedirection=DIRECTION)
        sm.setlimits()
        sm.setauto()
        assert sm.observe("SOURCE", "XBAND", starttime="0s", stoptime="30s")
    sm.close()


@needs_measures
def test_omitted_arguments_behave_as_casas_defaults(tmp_path: Path) -> None:
    write_session(tmp_path / "omitted.ms", explicit=False)
    write_session(tmp_path / "explicit.ms", explicit=True)
    checked = {
        "": ["TIME", "ANTENNA1", "ANTENNA2", "UVW", "STATE_ID", "FLAG"],
        "ANTENNA": ["NAME", "STATION", "MOUNT", "DISH_DIAMETER", "POSITION"],
        "FEED": ["POLARIZATION_TYPE"],
        "SPECTRAL_WINDOW": ["NAME", "CHAN_FREQ", "NUM_CHAN", "MEAS_FREQ_REF"],
        "POLARIZATION": ["CORR_TYPE"],
        "FIELD": ["NAME", "CODE", "PHASE_DIR"],
        "STATE": ["SIG", "REF", "CAL", "LOAD", "SUB_SCAN", "OBS_MODE"],
        "OBSERVATION": ["OBSERVER", "PROJECT", "TELESCOPE_NAME"],
    }
    for subtable, columns in checked.items():
        omitted = Table.open(tmp_path / "omitted.ms" / subtable)
        explicit = Table.open(tmp_path / "explicit.ms" / subtable)
        assert omitted.row_count == explicit.row_count > 0, subtable
        for column in columns:
            left, right = omitted.get_column(column), explicit.get_column(column)
            assert np.array_equal(np.asarray(left), np.asarray(right)), (subtable, column)
    # Hour-angle mode by default: the scan is centred on transit, not on the
    # reference time; 4 antennas without autocorrelations in 3 integrations.
    assert Table.open(tmp_path / "omitted.ms").row_count == 18


def noisy(path: Path, seed: int | None, **controls: Any) -> np.ndarray:
    """DATA of a 300 s session after `setseed(seed)`, 0.1 Jy and one corrupt."""
    sm = simulator(**controls)
    sm.open(str(path))
    configure(sm)
    assert sm.observe("F", "L", starttime="0s", stoptime="300s")
    if seed is not None:
        assert sm.setseed(seed) is True
    assert sm.setnoise(mode="simplenoise", simplenoise="0.1Jy") is True
    assert sm.corrupt() is True
    sm.close()
    return np.asarray(Table.open(path).get_column("DATA"))


@needs_measures
def test_setnoise_and_corrupt_add_casas_thermal_noise_at_close(tmp_path: Path) -> None:
    data = noisy(tmp_path / "noisy.ms", 5)
    main = Table.open(tmp_path / "noisy.ms")
    # 4 antennas with autocorrelations: 10 rows in each of 30 integrations.
    assert data.shape == (300, 2, 4)
    auto = np.asarray(main.get_column("ANTENNA1")) == np.asarray(main.get_column("ANTENNA2"))
    cross = data[~auto]
    # Each real and imaginary part has σ² = 0.01, within five standard errors.
    tolerance = 5 * 0.01 * np.sqrt(2 / cross.size)
    assert abs(np.var(cross.real) - 0.01) < tolerance
    assert abs(np.var(cross.imag) - 0.01) < tolerance
    # Autocorrelations get real noise only.
    assert np.all(data[auto].imag == 0.0)
    assert np.var(data[auto].real) > 0.005
    assert np.array_equal(np.asarray(main.get_column("WEIGHT")), np.ones((300, 2)))


@needs_measures
def test_the_seed_decides_the_draws(tmp_path: Path) -> None:
    first = noisy(tmp_path / "first.ms", 5)
    assert np.array_equal(noisy(tmp_path / "again.ms", 5), first)
    assert not np.array_equal(noisy(tmp_path / "other.ms", 6), first)
    # CASA's binding keeps a seed's low 32 bits.
    assert np.array_equal(noisy(tmp_path / "wrapped.ms", 5 + 2**32), first)
    # Without setseed, CASA's simulator seed 11111, not the binding default.
    assert np.array_equal(noisy(tmp_path / "default.ms", None), noisy(tmp_path / "s.ms", 11111))
    assert not np.array_equal(noisy(tmp_path / "unset.ms", None), noisy(tmp_path / "b.ms", 185349251))


@needs_measures
def test_the_controls_do_not_change_what_close_writes(tmp_path: Path) -> None:
    first = noisy(tmp_path / "default.ms", 5)
    for name, controls in [
        ("one.ms", {"workers": 1}),
        ("four.ms", {"workers": 4, "backend": "cpu"}),
        ("budget.ms", {"memory_budget": 256 << 20, "workers": 3}),
    ]:
        assert np.array_equal(noisy(tmp_path / name, 5, **controls), first), name


@needs_measures
def test_a_budget_too_small_for_close_raises_and_writes_nothing(tmp_path: Path) -> None:
    sm = simulator(memory_budget=1 << 20)
    sm.open(str(tmp_path / "small.ms"))
    configure(sm)
    assert sm.observe("F", "L", starttime="0s", stoptime="30s")
    with pytest.raises(RuntimeError, match="MeasurementSet writer needs .* bytes"):
        sm.close()
    assert not (tmp_path / "small.ms").exists()
    assert sm.name() == ""


@needs_measures
def test_setnoise_outcomes_follow_casa(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    sm = opened(tmp_path)
    configure(sm)
    with caplog.at_level(logging.ERROR, logger=LOGGER):
        # Before observe CASA has no rows to size the term on.
        assert sm.setnoise(simplenoise="0.1Jy") is False
        assert sm.observe("F", "L", starttime="0s", stoptime="30s")
        assert sm.setnoise(mode="bogus") is False
    assert caplog.messages[0].startswith("Caught exception: Error in Simulator::create_corrupt")
    assert caplog.messages[1:] == ["Caught exception: unsupported mode bogus in setnoise()"]
    with pytest.raises(NotImplementedError, match="tsys-manual.*Phase 3"):
        sm.setnoise(mode="tsys-manual")
    with pytest.raises(NotImplementedError, match="tsys-atm.*Phase 4"):
        sm.setnoise(mode="tsys-atm")
    with pytest.raises(NotImplementedError, match="OTF=False"):
        sm.setnoise(simplenoise="0.1Jy", OTF=False)
    # None of those added a term; `[]` is 0 Jy.
    assert sm.setnoise() is True
    assert sm.reset() is True
    assert sm.corrupt() is True
    sm.close()
    data = np.asarray(Table.open(tmp_path / "x.ms").get_column("DATA"))
    assert np.all(data == 0)


@needs_measures
def test_corrupt_raises_for_three_correlations(tmp_path: Path) -> None:
    sm = opened(tmp_path)
    configure(sm)
    assert sm.setspwindow(spwname="T", freq="1.4GHz", deltafreq="1MHz", nchannels=4, stokes="XX XY YY")
    assert sm.observe("F", "T", starttime="0s", stoptime="30s")
    assert sm.setnoise(simplenoise="0.1Jy") is True
    with pytest.raises(RuntimeError, match="No such VisVector type"):
        sm.corrupt()
    sm.reset()
    sm.close()


def test_predict_opens_its_images_first_even_before_open(tmp_path: Path) -> None:
    # CASA's binding opens every named image before it asks the simulator,
    # so a missing or unreadable image raises whether or not a session is
    # open.
    with pytest.raises(RuntimeError, match="does not exist"):
        simulator().predict(imagename=str(tmp_path / "missing.image"))
    plain = tmp_path / "plain.txt"
    plain.write_text("x", encoding="utf-8")
    with pytest.raises(RuntimeError, match="Unable to open image"):
        simulator().predict(imagename=[str(plain)])


@needs_measures
def test_predict_outcomes_follow_casa(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    sm = opened(tmp_path)
    configure(sm)
    assert sm.observe("F", "L", starttime="0s", stoptime="30s")
    missing = tmp_path / "model.cl"
    with caplog.at_level(logging.ERROR, logger=LOGGER):
        assert sm.predict() is False
        assert sm.predict(imagename=[""]) is False
        assert sm.predict(complist=str(missing)) is False
    assert caplog.messages == ["Failed to create SkyEquation: No model images found"] * 2 + [
        f"Failed to create SkyEquation: ComponentList {missing} not readable"
    ]
    with pytest.raises(RuntimeError, match="does not exist"):
        sm.predict(imagename=str(tmp_path / "missing.image"))
    sm.close()
    assert np.all(np.asarray(Table.open(tmp_path / "x.ms").get_column("DATA")) == 0)


REPO_ROOT = Path(__file__).resolve().parents[4]


def sky_model(tmp_path: Path) -> Path:
    """The one-plane Stokes I model around DIRECTION that the fixture
    example writes."""
    root = tmp_path / "fixtures"
    subprocess.run(
        ["cargo", "run", "-q", "-p", "casars-python", "--example", "make_python_fixtures", "--",
         str(root)],
        cwd=REPO_ROOT,
        check=True,
    )
    return root / "simulator_model.image"


@needs_measures
def test_auto_predicts_on_the_metal_device_as_the_cpu_does(tmp_path: Path) -> None:
    # 'auto' predicts on the machine's Metal device when it has one, which
    # agrees with the CPU to 1e-4 of the largest visibility.
    try:
        simulator(backend="metal")
    except RuntimeError as error:
        pytest.skip(f"no Metal device: {error}")
    model = sky_model(tmp_path)

    def predicted(name: str, backend: str) -> np.ndarray:
        sm = simulator(backend=backend)
        sm.open(str(tmp_path / name))
        configure(sm)
        assert sm.observe("F", "L", starttime="0s", stoptime="300s")
        assert sm.predict(imagename=str(model)) is True
        sm.close()
        return np.asarray(Table.open(tmp_path / name).get_column("DATA"))

    cpu = predicted("cpu.ms", "cpu")
    auto = predicted("auto.ms", "auto")
    scale = np.abs(cpu).max()
    assert scale > 0.5
    difference = np.abs(auto - cpu).max()
    assert difference <= 1e-4 * scale
    # The device rounds differently: the CPU's bits would mean it predicted both.
    assert difference > 0


@needs_measures
def test_setoptions_and_setvp_follow_casa(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    sm = opened(tmp_path)
    configure(sm)
    assert sm.setoptions(padding=1.5) is True
    # CASA keeps a padding of at least 1 and ignores the rest.
    assert sm.setoptions(padding=0.5) is True
    with caplog.at_level(logging.ERROR, logger=LOGGER):
        assert sm.setoptions(location=1.0) is True
    assert caplog.messages == ["Could not convert location 1 to a Position Measures"]
    with pytest.raises(NotImplementedError, match="mosaic.*Phase 2"):
        sm.setoptions(ftmachine="mosaic")
    with pytest.raises(NotImplementedError, match="gridfunction"):
        sm.setoptions(gridfunction="BOX")
    assert sm.setvp() is True
    assert sm.setvp(dovp=False, parangleinc="5deg") is True
    with pytest.raises(NotImplementedError, match="vptable"):
        sm.setvp(usedefaultvp=False, vptable="vp.tab")
    with pytest.raises(RuntimeError, match="in converting quantity"):
        sm.setvp(parangleinc="10 furlong")
    sm.close()


@needs_measures
def test_setconfig_lets_other_threads_run_while_the_session_works(tmp_path: Path) -> None:
    """The session's work runs without the GIL (ADR-0019 §4).

    With a switch interval longer than the test, this thread keeps the GIL
    until a call releases it, so the counting thread, which counts only while
    it holds the GIL, counts during `setconfig` only if `setconfig` releases
    it. The tool is dropped without scans, so nothing is written.
    """
    sm = opened(tmp_path)
    antennas = 100_000
    x = np.linspace(-5000.0, 5000.0, antennas)
    y = np.linspace(5000.0, -5000.0, antennas)
    z = np.zeros(antennas)
    counted = 0
    stop = threading.Event()

    def count() -> None:
        nonlocal counted
        while not stop.is_set():
            counted += 1
            time.sleep(0.0002)

    interval = sys.getswitchinterval()
    counter = threading.Thread(target=count)
    sys.setswitchinterval(1000.0)
    try:
        counter.start()
        during = []
        for _ in range(5):
            before = counted
            assert sm.setconfig(
                x=x, y=y, z=z, dishdiameter=[25.0], antname=["A"], padname=["P"],
                coordsystem="local", referencelocation="VLA",
            )
            during.append(counted - before)
            if during[-1]:
                break
    finally:
        stop.set()
        counter.join()
        sys.setswitchinterval(interval)
    assert any(during), f"no other thread ran during setconfig: {during}"
