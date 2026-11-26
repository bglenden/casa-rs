# SPDX-License-Identifier: LGPL-3.0-or-later
"""Actual native task processes and renderer against CASA's recorded figures."""
import gzip
from copy import deepcopy
import importlib.util
import json
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

REPO=Path(__file__).resolve().parents[4]
spec=importlib.util.spec_from_file_location('simulation_figures',REPO/'tools/simulation/simulation_task_graphics_compare.py')
helper=importlib.util.module_from_spec(spec)
sys.path.insert(0,str(REPO/'tools/simulation'))
spec.loader.exec_module(helper)


@pytest.fixture(scope='module')
def rendered_cases(tmp_path_factory):
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


@pytest.mark.parametrize('kind',['circles','anchored_circles','texts'])
def test_figure_comparison_detects_missing_pointings_beams_and_statistics(kind):
    with gzip.open(REPO/'crates/casa-simulation/tests/data/simulation_task_graphics_casa.json.gz','rt') as source:
        figures=json.load(source)['simalma']['figures']
    name=next(name for name,panels in figures.items() if any(panel[kind] for panel in panels))
    expected={name:figures[name]};actual=deepcopy(expected)
    next(panel for panel in actual[name] if panel[kind])[kind].pop()
    assert helper.compare(actual,expected)


def test_statistical_annotations_compare_numbers_and_units():
    panel={'images':[{'pixels':[[1.25]]}]}
    assert helper.annotation_equal('min=0.0e+00\nmax=1.2e+00\nJy/beam','min=-5.0e-08\nmax=1.3e+00\nJy/beam',panel)
    assert not helper.annotation_equal('max=9.9e+01\nJy/beam','max=1.3e+00\nJy/beam',panel)
    assert not helper.annotation_equal('max=1.3e+00\nJy/pixel','max=1.3e+00\nJy/beam',panel)
