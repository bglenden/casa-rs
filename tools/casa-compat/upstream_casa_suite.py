#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Run pinned upstream CASA suites against the original or native Python entry points."""

import argparse
from contextlib import redirect_stderr
from functools import wraps
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from time import perf_counter_ns

CASA_VERSION = "6.7.6.14"
IMAGE_TOOL_FIXTURES = {
    "coordsys": "coordsys",
    "image": "image",
    "image_collapse": "ia_collapse",
    "image_deviation": "ia_deviation",
    "image_fitprofile": "ia_fitprofile",
    "image_pbcor": "ia_pbcor",
    "image_pv": "ia_pv",
    "image_rebin": None,
    "image_regrid": "ia_regrid",
    "image_statistics": "ia_statistics",
    "image_subimage": None,
    "image_transpose": "ia_transpose",
}
_fixture_errors = []


def pytest_configure():
    # Use the already installed measures and our staged test data.
    from casaconfig import config
    config.data_auto_update = False
    config.measures_auto_update = False

    import casatasks
    import casatools
    from casatools import ctsys

    if casatasks.version_string() != CASA_VERSION:
        raise RuntimeError(f"the upstream suite requires CASA {CASA_VERSION}")
    ctsys.setpath([os.environ["CASARS_UPSTREAM_TEST_DATA"], *ctsys.getpath()])
    if not casatasks.casalog.setlogfile(str(Path.cwd() / "casapy.log")):
        raise RuntimeError("could not open the upstream suite log")

    if os.environ.get("CASARS_UPSTREAM_EAGER_ASDM_FIXTURES") == "1":
        casatasks.importasdm = _eager_asdm_fixture(casatasks.importasdm)

    task = os.environ["CASARS_UPSTREAM_TEST_TASK"]
    implementation = os.environ["CASARS_UPSTREAM_TEST_IMPLEMENTATION"]
    if tool := os.environ.get("CASARS_UPSTREAM_TEST_TOOL"):
        if task == "image":
            # The general suite calls maketestimage() with the CASA demo FITS.
            sample = Path("demo/Images/imagetestimage.fits")
            copy_fixture(Path(ctsys.resolve(sample.as_posix())), sample)
        if implementation == "casa-rs":
            from casars import tools
            for name in tool.split(","):
                setattr(casatools, name, getattr(tools, name))
        return
    entry_point = getattr(casatasks, task)
    if implementation == "casa-rs":
        from casars import tasks
        from casars.tasks import _runner
        _runner._task_executable(task)
        entry_point = getattr(tasks, task)
        if task == "clearcal":
            # The stock suite keeps a table open while the task changes it.
            from casars import tools
            casatools.table = tools.table
    if timings := os.environ.get("CASARS_UPSTREAM_TEST_TIMINGS"):
        entry_point = _timed_task(entry_point, Path(timings))
    if implementation == "casa-rs":
        entry_point = _native_task(entry_point, casatasks.casalog)
    setattr(casatasks, task, entry_point)


def _eager_asdm_fixture(importasdm):
    @wraps(importasdm)
    def build(*args, **kwargs):
        kwargs["lazy"] = False
        return importasdm(*args, **kwargs)

    return build


def _timed_task(task, output):
    @wraps(task)
    def run(*args, **kwargs):
        started = perf_counter_ns()
        completed = False
        try:
            result = task(*args, **kwargs)
            completed = True
            return result
        finally:
            sample = {
                "test": os.environ.get("PYTEST_CURRENT_TEST"),
                "elapsed_ns": perf_counter_ns() - started,
                "completed": completed,
            }
            with output.open("a", encoding="utf-8") as log:
                print(json.dumps(sample), file=log)
    return run


def _native_task(task, casalog):
    @wraps(task)
    def run(*args, **kwargs):
        try:
            _flush_fixture_writes()
            # Upstream log assertions read the file chosen by casalog.
            log = Path(casalog.logfile()).open("a", encoding="utf-8")
        except Exception as error:
            # Upstream expected-failure assertions may catch this exception.
            _fixture_errors.append(str(error))
            raise
        with log, redirect_stderr(log):
            return task(*args, **kwargs)
    return run


def _flush_fixture_writes():
    """Publish pending writes held by upstream CASA tools to the task process."""
    from casatools import table

    workspace = Path.cwd()
    handle = table()
    for name in handle.showcache():
        if Path(name).resolve().is_relative_to(workspace):
            try:
                if not handle.open(name):
                    raise RuntimeError(f"could not open upstream fixture table {name}")
                handle.flush()
            finally:
                handle.done()


def pytest_sessionfinish(session):
    if _fixture_errors:
        for error in _fixture_errors:
            print(f"upstream CASA fixture failure: {error}", file=sys.stderr)
        session.exitstatus = 1


def run_suite(suite, casatestdata, implementation, output, only, trial, fixtures,
              *, eager_asdm_fixtures=False):
    script_kind, name = suite.stem.removeprefix("test_").split("_", 1)
    task = name.removesuffix("_mms")
    if script_kind == "tool" and name in IMAGE_TOOL_FIXTURES:
        tool, fixture_group = "image,coordsys", IMAGE_TOOL_FIXTURES[name]
    else:
        tool, fixture_group = {
            "ms": ("ms", "mstool"),
            "ms_createmultims": ("ms", "mstool"),
            "ms_statwt": ("ms", "statwt"),
            "calibrater": ("calibrater,table", "calibrater"),
            "getantposalma": (task, None),
            "getcalmodvla": (task, None),
        }.get(name, (task, task))
    source = casatestdata / "unittest" / fixture_group if fixture_group else None
    if source is not None and fixture_group != "exportfits" and not source.is_dir():
        raise FileNotFoundError(f"missing upstream fixtures: {source}")
    with tempfile.TemporaryDirectory(dir=output, prefix=f"{name}-{implementation}-") as directory:
        scratch = Path(directory)
        staged = scratch / "fixtures"
        staged.mkdir()
        if source is not None:
            target = staged / "unittest" / source.name
            if fixtures:
                for fixture in fixtures:
                    copy_fixture(source / fixture, target / fixture)
            elif source.is_dir():
                copy_fixture(source, target)

        work = scratch / "work"
        work.mkdir()
        env = {
            **os.environ,
            "CASARS_UPSTREAM_TEST_DATA": str(staged),
            "CASARS_UPSTREAM_TEST_TASK": task,
            "CASARS_UPSTREAM_TEST_IMPLEMENTATION": implementation,
        }
        if script_kind == "tool":
            env["CASARS_UPSTREAM_TEST_TOOL"] = tool
        if eager_asdm_fixtures:
            env["CASARS_UPSTREAM_EAGER_ASDM_FIXTURES"] = "1"
        env["PYTHONPATH"] = os.pathsep.join(filter(None, (
            str(Path(__file__).resolve().parent), env.get("PYTHONPATH")
        )))
        env.setdefault("MPLCONFIGDIR", str(output / "matplotlib"))
        group = output / f"{name}-{implementation}"
        if trial:
            group = group.with_name(f"{group.name}-{trial}")
            timings = group.with_suffix(".jsonl")
            timings.write_text("", encoding="utf-8")
            env["CASARS_UPSTREAM_TEST_TIMINGS"] = str(timings)
        case_filter = ["-k", only] if only else []
        with group.with_suffix(".log").open("w", encoding="utf-8") as log:
            result = subprocess.run(
                [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
                 "-p", Path(__file__).stem, "-c", os.devnull,
                 *case_filter,
                 "--rootdir", str(suite.parent),
                 "--junitxml", str(group.with_suffix(".xml")), str(suite)],
                cwd=work, env=env, stdout=log, stderr=subprocess.STDOUT,
            )
        print(f"{name} on {implementation}: exit {result.returncode}, {group.with_suffix('.log')}")
        return result.returncode


def copy_fixture(source, target):
    if source.is_dir():
        # CASACORE persists current row counts in table.lock as well as locks.
        shutil.copytree(source, target)
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)


def main(family, description):
    root = Path(__file__).resolve().parents[2] / "resources/casa-compat"
    suites_dir = root / f"upstream-{family}-tests" / CASA_VERSION
    suites = {path.stem.split("_", 2)[2]: path
              for path in sorted(suites_dir.glob("test_*.py"))}
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("task", choices=suites)
    parser.add_argument("--casatestdata", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--implementation", choices=["both", "casa", "casa-rs"], default="both")
    parser.add_argument("--only", help="pytest case-name expression")
    parser.add_argument("--trials", type=int, help="repeat the suite and record public call timings")
    parser.add_argument("--fixture", action="append", type=Path, default=[],
                        help="stage only this path relative to the suite's fixture directory")
    if family == "ms":
        parser.add_argument("--eager-asdm-fixtures", action="store_true",
                            help="materialize visibility columns in CASA-built ASDM fixtures")
    args = parser.parse_args()
    data = args.casatestdata.resolve()
    if not (data / "unittest").is_dir():
        parser.error(f"no unittest directory in {data}")
    if args.trials is not None and args.trials < 1:
        parser.error("--trials must be positive")
    if args.trials and suites[args.task].stem.startswith("test_tool_"):
        parser.error("--trials measures public task calls")
    if args.fixture and args.task in IMAGE_TOOL_FIXTURES and IMAGE_TOOL_FIXTURES[args.task] is None:
        parser.error(f"{args.task} generates its own fixtures")
    if any(path.is_absolute() or ".." in path.parts for path in args.fixture):
        parser.error("--fixture paths must be relative to the suite's fixture directory")
    output = args.out.resolve()
    output.mkdir(parents=True, exist_ok=True)
    implementations = ("casa", "casa-rs") if args.implementation == "both" else (args.implementation,)
    success = True
    for trial in range(args.trials or 1):
        order = implementations if trial % 2 == 0 else implementations[::-1]
        results = [run_suite(suites[args.task], data, mode, output, args.only,
                             trial + 1 if args.trials else None, args.fixture,
                             eager_asdm_fixtures=getattr(args, "eager_asdm_fixtures", False))
                   for mode in order]
        success &= not any(results)
        if not success:
            break
    return int(not success)
