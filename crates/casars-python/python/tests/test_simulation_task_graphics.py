# SPDX-License-Identifier: LGPL-3.0-or-later
"""Actual native task processes and renderer against CASA's recorded figures."""
import gzip
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tarfile

import pytest

pytest.importorskip('matplotlib')
import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt
from matplotlib.figure import Figure
from casars import tasks
from casars.tasks import _runner

REPO=Path(__file__).resolve().parents[4]
spec=importlib.util.spec_from_file_location('simulation_figures',REPO/'tools/simulation/simulation_task_graphics_compare.py')
helper=importlib.util.module_from_spec(spec)
sys.path.insert(0,str(REPO/'tools/simulation'))
spec.loader.exec_module(helper)


@pytest.fixture(scope='module')
def rendered_cases(tmp_path_factory):
    for task in ['simalma','simanalyze']:
        try:_runner._task_executable(task)
        except tasks.TaskBinaryNotFoundError as error:
            if os.environ.get('CASARS_REQUIRE_TASK_BINARIES'):raise
            pytest.skip(str(error))
    root=tmp_path_factory.mktemp('simulation-task-figures')
    data=REPO/'crates/casa-simulation/tests/data'
    for filename in ['simalma_inputs.tar.gz','simanalyze_multi_inputs.tar.gz']:
        with tarfile.open(data/filename) as archive:archive.extractall(root,filter="data")
    # The task searches cwd before its configuration repository. Do not
    # require an external CASA installation for the default TP configuration.
    shutil.copy(root/'inputs/alma/simmos/aca.tp.cfg',root/'aca.tp.cfg')
    with gzip.open(data/'simulation_task_graphics_casa.json.gz','rt') as source:reference=json.load(source)
    captured={};original=Figure.savefig;result={}
    def save(figure,filename,*args,**kwargs):
        name=Path(filename).name
        match=re.fullmatch(r'\.(.+\.png)\.[^.]+\.png',name)
        captured[match.group(1) if match else name]=helper.artists(figure)
        return original(figure,filename,*args,**kwargs)
    Figure.savefig=save
    try:
        for case,expected in [('simalma',reference['simalma']),*reference['simanalyze'].items()]:
            p=dict(expected['parameters'])
            if case=='simalma':
                p.update(skymodel=str(root/'inputs/model.image'),antennalist=[str(root/'inputs/alma.test.cfg'),str(root/'inputs/aca.test.cfg')],hourangle='35s')
            completion=tasks.run('simalma' if case=='simalma' else 'simanalyze',workspace=root,**p)
            assert completion.successful and completion.value is None
            pngs=[p for p in completion.products if p.path.endswith('.png')]
            assert len(pngs)==len(expected['figures'])
            assert all(p.exists and Path(p.path).read_bytes().startswith(b'\x89PNG\r\n\x1a\n') for p in pngs)
            result[case]=helper.compare(captured,expected['figures'])
            captured.clear();plt.close('all')
    finally:Figure.savefig=original
    return result


@pytest.mark.parametrize('case',['simalma','multi','tp','seed','feather'])
def test_complete_simulation_figures_match_casa(rendered_cases,case):
    assert not rendered_cases[case],json.dumps(rendered_cases[case],indent=2)
