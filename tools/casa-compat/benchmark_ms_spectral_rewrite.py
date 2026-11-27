"""Compare CASA and casa-rs spectral writes on independent ngc5921_ut.ms copies."""

from argparse import ArgumentParser
from contextlib import closing
from os.path import samestat
from pathlib import Path
from shutil import copytree
from statistics import median
from tempfile import TemporaryDirectory
from time import perf_counter

CASES = (
    ("cvel-default", "cvel", {}, 63),
    ("cvel-noop", "cvel", dict(hanning=False), 63),
    ("cvel-width", "cvel", dict(width=2, hanning=False), 31),
    ("cvel-hanning", "cvel", dict(width=2), 31),
    ("regridspw-width", "regridspw", dict(chanwidth=2, hanning=False), 32),
    ("regridspw-hanning", "regridspw", dict(center=24, chanwidth=2, bandwidth=32), 16),
    ("regridspw-noop", "regridspw", {}, 63),
    ("regridspw-frame", "regridspw", dict(outframe="BARY", mode=""), 63),
    (
        "regridspw-bary-width",
        "regridspw",
        dict(outframe="BARY", center=31, chanwidth=2, hanning=False),
        (32, 32, 32),
    ),
)


def copy_measurement_set(source, destination, identity, table_factory):
    copytree(source, destination)
    assert not samestat((destination / "table.dat").stat(), identity)
    with closing(table_factory()) as table:
        assert table.open(str(destination))
        for name in ("SPECTRAL_WINDOW", "HISTORY", "DATA_DESCRIPTION", "FIELD"):
            reference = Path(table.getkeyword(name).removeprefix("Table: "))
            assert reference.resolve().is_relative_to(destination)


def main():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("measurement_set", type=Path)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--case", choices=[name for name, *_ in CASES])
    options = parser.parse_args()
    if options.repeats < 5:
        parser.error("--repeats must be at least 5")

    import casatools
    from casars import tools

    assert casatools.version_string().replace("-", ".") == "6.7.6.14"
    source = options.measurement_set.resolve(strict=True)
    identity = (source / "table.dat").stat()
    logger = casatools.logsink()
    logger.setlogfile("/dev/null")
    logger.showconsole(False)
    logger.filter("SEVERE")
    apis = (("CASA", casatools), ("native", tools))
    for case, method, args, channels in CASES:
        if options.case and case != options.case:
            continue
        values = {name: [] for name, _ in apis}
        for index in range(options.repeats + 1):
            order = apis if index % 2 == 0 else tuple(reversed(apis))
            with TemporaryDirectory(prefix="ms-spectral-rewrite-") as scratch:
                paths = {name: Path(scratch, f"{name}.ms").resolve() for name, _ in order}
                for path in paths.values():
                    copy_measurement_set(source, path, identity, casatools.table)
                for name, implementation in order:
                    path = paths[name]
                    with closing(implementation.ms()) as tool:
                        assert tool.open(str(path), nomodify=False)
                        start = perf_counter()
                        ok = getattr(tool, method)(**args)
                        elapsed = 1000 * (perf_counter() - start)
                        assert ok and tool.nrow() == 22653 and tool.name() == str(path)
                    with closing(casatools.table()) as table:
                        assert table.open(str(path / "SPECTRAL_WINDOW"))
                        if isinstance(channels, tuple):
                            assert tuple(table.getcol("NUM_CHAN")) == channels
                        else:
                            assert table.getcell("NUM_CHAN", 0) == channels
                    if index:
                        values[name].append(elapsed)
                        print("pair", case, index, name, round(elapsed, 4), flush=True)
        medians = {name: median(timings) for name, timings in values.items()}
        print("median", case, medians, "ratio", medians["CASA"] / medians["native"], flush=True)
    assert (source / "table.dat").stat().st_mtime_ns == identity.st_mtime_ns


if __name__ == "__main__":
    main()
