#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Run the imported CASA image task suites with CASA or casa-rs."""

import argparse
from functools import wraps
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

CASA_VERSION = "6.7.6.14"
SUITES = (
    Path(__file__).resolve().parents[2]
    / "resources/casa-compat/upstream-image-tests"
    / CASA_VERSION
)
_fixture_errors = []


def pytest_configure():
    # Use the already installed measures and our staged test data.
    from casaconfig import config
    config.data_auto_update = False
    config.measures_auto_update = False

    import casatasks
    from casatools import ctsys

    if casatasks.version_string() != CASA_VERSION:
        raise RuntimeError(f"the upstream image suite requires CASA {CASA_VERSION}")
    ctsys.setpath([os.environ["CASARS_UPSTREAM_IMAGE_DATA"], *ctsys.getpath()])

    if os.environ["CASARS_UPSTREAM_IMAGE_IMPLEMENTATION"] == "casa-rs":
        from casars import tasks
        from casars.tasks import _runner
        task = os.environ["CASARS_UPSTREAM_IMAGE_TASK"]
        _runner._task_executable(task)
        setattr(casatasks, task, _native_task(getattr(tasks, task)))


def _native_task(task):
    @wraps(task)
    def run(*args, **kwargs):
        try:
            _flush_fixture_writes()
        except Exception as error:
            # Upstream expected-failure assertions may catch this exception.
            _fixture_errors.append(str(error))
            raise
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
            print(f"upstream image fixture failure: {error}", file=sys.stderr)
        session.exitstatus = 1


def run_suite(suite, casatestdata, implementation, output):
    task = suite.stem.removeprefix("test_task_")
    source = casatestdata / "unittest" / task
    if task != "exportfits" and not source.is_dir():
        raise FileNotFoundError(f"missing upstream fixtures: {source}")
    with tempfile.TemporaryDirectory(dir=output, prefix=f"{task}-{implementation}-") as directory:
        scratch = Path(directory)
        staged = scratch / "fixtures"
        staged.mkdir()
        if source.is_dir():
            shutil.copytree(
                source, staged / "unittest" / task, ignore=shutil.ignore_patterns("table.lock")
            )

        work = scratch / "work"
        work.mkdir()
        env = {
            **os.environ,
            "CASARS_UPSTREAM_IMAGE_DATA": str(staged),
            "CASARS_UPSTREAM_IMAGE_TASK": task,
            "CASARS_UPSTREAM_IMAGE_IMPLEMENTATION": implementation,
        }
        env["PYTHONPATH"] = os.pathsep.join(filter(None, (
            str(Path(__file__).resolve().parent), env.get("PYTHONPATH")
        )))
        env.setdefault("MPLCONFIGDIR", str(output / "matplotlib"))
        group = output / f"{task}-{implementation}"
        with group.with_suffix(".log").open("w", encoding="utf-8") as log:
            result = subprocess.run(
                [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
                 "-p", Path(__file__).stem, "-c", os.devnull,
                 "--rootdir", str(suite.parent),
                 "--junitxml", str(group.with_suffix(".xml")), str(suite)],
                cwd=work, env=env, stdout=log, stderr=subprocess.STDOUT,
            )
        print(f"{task} on {implementation}: exit {result.returncode}, {group.with_suffix('.log')}")
        return result.returncode


def main():
    suites = {path.stem.removeprefix("test_task_"): path
              for path in sorted(SUITES.glob("test_task_*.py"))}
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task", choices=suites)
    parser.add_argument("--casatestdata", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--implementation", choices=["both", "casa", "casa-rs"], default="both")
    args = parser.parse_args()
    data = args.casatestdata.resolve()
    if not (data / "unittest").is_dir():
        parser.error(f"no unittest directory in {data}")
    output = args.out.resolve()
    output.mkdir(parents=True, exist_ok=True)
    implementations = ("casa", "casa-rs") if args.implementation == "both" else (args.implementation,)
    results = [run_suite(suites[args.task], data, mode, output) for mode in implementations]
    return int(any(results))


if __name__ == "__main__":
    raise SystemExit(main())
