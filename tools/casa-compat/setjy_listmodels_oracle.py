#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public setjy model discovery and its listing output with CASA."""

import argparse
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import subprocess
import sys

from casars.tasks import setjy


PINNED_CALL = """
import json
import sys
from casatasks import setjy
from casatools import ctsys
options = json.loads(sys.argv[1])
data = sys.argv[2]
if data:
    assert ctsys.setpath([data])
print('setjy return', repr(setjy(listmodels=True, **options)))
"""


def pinned(workspace, options, empty_datapath=''):
    # CASA's ls command writes directly to the process stdout.
    process = subprocess.run([sys.executable, '-I', '-c', PINNED_CALL,
                              json.dumps(options), str(empty_datapath)],
                             cwd=workspace, capture_output=True, text=True, check=True)
    names = process.stdout.splitlines()
    assert names.pop() == 'setjy return {}', (process.stdout, process.stderr)
    return names


def native(workspace, options, empty_datapath=''):
    cwd = Path.cwd()
    prior = os.environ.get('CASA_RS_MEASURESPATH')
    output = io.StringIO()
    try:
        os.chdir(workspace)
        if empty_datapath:
            os.environ['CASA_RS_MEASURESPATH'] = str(empty_datapath)
        with redirect_stdout(output):
            assert setjy(listmodels=True, **options) == {}
    finally:
        os.chdir(cwd)
        if prior is None:
            os.environ.pop('CASA_RS_MEASURESPATH', None)
        else:
            os.environ['CASA_RS_MEASURESPATH'] = prior
    return output.getvalue().splitlines()


def compare(workspace, options, empty_datapath=''):
    expected = pinned(workspace, options, empty_datapath)
    actual = native(workspace, options, empty_datapath)
    assert actual == expected, (options, expected, actual)
    return actual


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('workspace', type=Path)
    args = parser.parse_args()
    workspace = args.workspace.resolve()
    workspace.mkdir()
    for name in ('A.im', 'B.model', 'ordinary.im-note.txt', 'ordinary.txt', '.hidden.im'):
        (workspace / name).touch()
    candidate = workspace / 'fixture.ms'
    candidate.mkdir()
    marker = candidate / 'sentinel'
    marker.write_text('keep this file')
    for options in ({}, {'vis': 'missing.ms'}, {'vis': str(candidate), 'model': '*.ignored'}):
        names = compare(workspace, options)
        assert 'A.im' in names and 'B.model' in names and '3C286_L.im' in names
        assert '.hidden.im' not in names and 'ordinary.txt' not in names
        assert marker.read_text() == 'keep this file'

    empty_data = workspace / 'empty-data'
    empty_data.mkdir()
    fallback = workspace / 'observatory' / 'CalModels'
    fallback.mkdir(parents=True)
    (fallback / 'alternate.im').touch()
    hidden = workspace / '.skip' / 'CalModels'
    hidden.mkdir(parents=True)
    (hidden / 'hidden-alternate.im').touch()
    names = compare(workspace, {}, empty_data)
    assert 'alternate.im' in names and 'hidden-alternate.im' not in names
    (fallback / 'ignored.txt').touch()
    assert compare(fallback, {}, empty_data) == ['alternate.im']

    solar = workspace / 'observatory' / 'SolarSystemModels'
    solar.mkdir()
    for name in ('Mars_Tb_001.dat', 'Ceres_fd_time.dat', 'irrelevant.dat'):
        (solar / name).touch()
    excluded = workspace / 'tables' / 'SolarSystemModels'
    excluded.mkdir(parents=True)
    (excluded / 'excluded_Tb_file.dat').touch()
    assert compare(workspace, {'standard': 'Butler-JPL-Horizons 2012'}, empty_data) == [
        'Mars_Tb_001.dat', 'Ceres_fd_time.dat',
    ]
    expected_2010 = ('Venus Mars Jupiter Uranus Neptune Pluto Io Europa Ganymede Callisto Titan '
                     'Triton Ceres Pallas Vesta Juno Victoria Davida').split()
    assert native(workspace, {'standard': 'Butler-JPL-Horizons 2010'}, empty_data) == expected_2010
    local = workspace / 'nrao' / 'VLA' / 'CalModels'
    local.mkdir(parents=True)
    (local / 'local-3C286.im').touch()
    names = compare(workspace, {})
    assert 'local-3C286.im' in names and '3C286_L.im' not in names


if __name__ == '__main__':
    main()
