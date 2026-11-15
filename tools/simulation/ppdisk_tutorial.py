#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Reproduce CASA Guide 41893 with either native Python tasks or CASA 6.7.6.14.

Use a separate --out for each engine. Timings include task startup and graphics;
use release native binaries. Input downloads are explicit, never default gates.
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
from pathlib import Path
import shutil
import time
from urllib.parse import urlencode
from urllib.request import urlopen

os.environ.setdefault('MPLBACKEND', 'Agg')

MODEL_URL = 'https://bulk.cv.nrao.edu/almadata/public/casaguides/sim_inputs/ppdisk672_GHz_50pc.fits'
GUIDE_PAGE = 'Protoplanetary_Disk_Simulation_CASA_6.6.6'
GUIDE_REVISION = 41893
FIGURES = ['skymodel', 'observe', 'image', 'analysis']


def download(root: Path):
    root.mkdir(parents=True, exist_ok=True)
    model = root / 'ppdisk672_GHz_50pc.fits'
    if not model.exists():
        model.write_bytes(urlopen(MODEL_URL).read())
    api = 'https://casaguides.nrao.edu/api.php?'
    guide = json.load(urlopen(api + urlencode(dict(action='parse', oldid=GUIDE_REVISION,
                                                   prop='text|wikitext|revid', format='json'))))
    assert guide['parse']['revid'] == GUIDE_REVISION
    (root / 'guide.json').write_text(json.dumps(guide, indent=1) + '\n')
    titles = '|'.join('File:psim2-5.4.alma.out20.' + name + '.png' for name in FIGURES)
    sources = json.load(urlopen(api + urlencode(dict(action='query', titles=titles,
                            prop='imageinfo', iiprop='url', format='json'))))
    figures = root / 'guide-figures'
    figures.mkdir(exist_ok=True)
    for page in sources['query']['pages'].values():
        path = figures / page['title'].split(':', 1)[1]
        if not path.exists():
            path.write_bytes(urlopen(page['imageinfo'][0]['url']).read())
    (figures / 'sources.json').write_text(json.dumps(sources, indent=1) + '\n')


def run(engine: str, root: Path, model: Path, config: Path, repeats: int):
    root.mkdir(parents=True, exist_ok=True)
    shutil.copy2(model, root / model.name)
    os.chdir(root)
    # Numerical references capture actual Matplotlib artists. Native preparation
    # retains its complete binary payload so each figure can be replayed.
    captured = {}
    from matplotlib.figure import Figure
    from simobserve_graphics_oracle import artists
    savefig = Figure.savefig

    def capture(figure, filename, *args, **kwargs):
        name = Path(filename).name
        if name.startswith('.'):  # native atomic PNG staging filename
            name = name[1:].rsplit('.', 2)[0]
        captured[name] = artists(figure)
        return savefig(figure, filename, *args, **kwargs)

    Figure.savefig = capture
    if engine == 'casa':
        from casatasks import importfits, simobserve, simanalyze, imhead, version_string
        assert version_string() == '6.7.6.14'
    else:
        from casars.tasks import importfits, simobserve, simanalyze, imhead, _runner
        render = _runner._render_task_plots

        def retain(plots, payload, workspace):
            name = 'observe' if any('observe' in str(p['output_file']) for p in plots) else 'analyze'
            directory = root / 'plots' / name
            directory.mkdir(parents=True, exist_ok=True)
            (directory / 'plots.json').write_text(json.dumps(plots))
            shutil.copy2(payload, directory / 'arrays.bin')
            return render(plots, payload, workspace)

        _runner._render_task_plots = retain
    header = imhead(model.name)
    observe = dict(project='psim2', skymodel=model.name, setpointings=True,
                   direction='J2000 18h00m00.031s -22d59m59.6s', mapsize='0.76arcsec',
                   obsmode='int', totaltime='1200s', antennalist=str(config),
                   thermalnoise='', graphics='file', overwrite=True)
    if engine == 'native':
        # Existing owner-approved UTC comparison procedure: equal stored TIME,
        # not a production task default change (simobserve_parity.casars_params).
        observe['hourangle'] = '35s'
    analyze = dict(project='psim2', image=True, modelimage=model.stem + '.image',
                   vis='psim2.alma.out20.ms', imsize=[192, 192], niter=10000,
                   threshold='1e-7Jy', weighting='natural', analyze=True,
                   showuv=False, showresidual=True, showconvolved=True,
                   graphics='file', verbose=True, overwrite=True)
    elapsed = []
    for _ in range(repeats):
        start = time.perf_counter()
        importfits(fitsimage=model.name, imagename=analyze['modelimage'], overwrite=True)
        imported = time.perf_counter()
        simobserve(**observe)
        observed = time.perf_counter()
        simanalyze(**analyze)
        finished = time.perf_counter()
        elapsed.append(dict(import_s=imported-start, observe_s=observed-imported,
                            analyze_s=finished-observed, total_s=finished-start))
    with gzip.open(root / 'artists.json.gz', 'wt') as output:
        json.dump(captured, output, separators=(',', ':'))
    result = dict(engine=engine, guide_revision=GUIDE_REVISION, oracle='6.7.6.14',
                  import_adjustment='Lossless native/CASA importfits: CASA 6.7.6.14 rejects the Guide FITS startmodel as not a readable Table.',
                  header=header, observe=observe, analyze=analyze, timings=elapsed,
                  products=sorted(p.name for p in Path('psim2').iterdir()))
    (root / 'run.json').write_text(json.dumps(result, indent=1, default=lambda value: value.tolist()) + '\n')
    print(json.dumps(result, indent=1, default=lambda value: value.tolist()))
    if engine == 'casa':
        corrected_control(root, analyze)
        with gzip.open(root / 'corrected-artists.json.gz', 'wt') as output:
            json.dump(captured, output, separators=(',', ':'))


def corrected_control(root: Path, parameters: dict):
    """Retain raw CASA products, plus its documented full-precision control."""
    from simanalyze_task_oracle import corrected_simanalyze
    original = root / 'original-figures'
    original.mkdir(exist_ok=True)
    for path in Path('psim2').glob('*.png'):
        shutil.copy2(path, original / path.name)
    fidelity = root / 'psim2/psim2.alma.out20.fidelity'
    preserved = root / 'raw-fidelity.image'
    shutil.rmtree(preserved, ignore_errors=True)
    shutil.copytree(fidelity, preserved)
    task, defaults = corrected_simanalyze()
    defaults.update(parameters)
    task(**defaults)
    controlled = root / 'corrected-figures'
    controlled.mkdir(exist_ok=True)
    for path in Path('psim2').glob('*.png'):
        shutil.copy2(path, controlled / path.name)
    corrected = root / 'corrected-fidelity.image'
    shutil.rmtree(corrected, ignore_errors=True)
    shutil.copytree(fidelity, corrected)
    shutil.rmtree(fidelity)
    shutil.copytree(preserved, fidelity)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--engine', choices=['download', 'casa', 'native'], required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--model', type=Path)
    parser.add_argument('--config', type=Path)
    parser.add_argument('--repeats', type=int, default=1)
    args = parser.parse_args()
    if args.engine == 'download':
        download(args.out.resolve())
    else:
        if not args.model or not args.config:
            parser.error('--model and --config are required')
        if args.repeats < 1:
            parser.error('--repeats must be positive')
        run(args.engine, args.out.resolve(), args.model.resolve(), args.config.resolve(), args.repeats)


if __name__ == '__main__':
    main()
