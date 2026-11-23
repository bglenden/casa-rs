#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare matched public image and coordinate calls against pinned CASA."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import tempfile
import time

import casatools
from casars import _core
from casars import tools
import numpy as np


CASES = ('toworldmany', 'topixelmany', 'getchunk', 'getchunk_strided',
         'statistics', 'rebin', 'convolve', 'regrid')
WORKER_ENV = ('RAYON_NUM_THREADS', 'OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS',
              'VECLIB_MAXIMUM_THREADS')


def time_call(call):
    cpu = time.process_time_ns()
    wall = time.perf_counter_ns()
    result = call()
    return result, (time.perf_counter_ns() - wall, time.process_time_ns() - cpu)


def product(tool):
    cs = tool.coordsys()
    try:
        points = np.column_stack((cs.referencepixel()['numeric'], np.asarray(tool.shape()) - 1.0))
        return tool.getchunk(), tool.getchunk(getmask=True), cs.toworldmany(points)['numeric']
    finally:
        cs.done()
        tool.done()


def image_case(api, path, case, regrid_record, shape):
    image = api.image()
    try:
        assert image.open(str(path))
        match case:
            case 'getchunk':
                return time_call(image.getchunk)
            case 'getchunk_strided':
                return time_call(lambda: image.getchunk(inc=[3, 2, 1, 2]))
            case 'statistics':
                values, timing = time_call(lambda: image.statistics(axes=[0, 1], list=False, force=True))
                return tuple(values[k] for k in ('npts', 'sum', 'mean', 'sigma', 'min', 'max')), timing
            case 'rebin':
                result, timing = time_call(lambda: image.rebin(outfile='', bin=[2, 2, 1, 1]))
            case 'convolve':
                sigma = np.arange(-3, 4, dtype=np.float64)
                kernel = np.exp(-(sigma[:, None] ** 2 + sigma ** 2) / 4)
                kernel /= kernel.sum()
                result, timing = time_call(lambda: image.convolve(kernel=kernel, scale=1.0))
            case 'regrid':
                result, timing = time_call(lambda: image.regrid(shape=shape, csys=regrid_record,
                                                               axes=[0, 1], method='linear', decimate=0))
            case _:
                raise ValueError(case)
        return product(result), timing
    finally:
        image.done()


def coord_case(api, case, record, pixels, world):
    coords = api.coordsys()
    try:
        assert coords.fromrecord(record)
        if case == 'toworldmany':
            values, timing = time_call(lambda: coords.toworldmany(pixels))
        else:
            values, timing = time_call(lambda: coords.topixelmany(world))
        return values['numeric'], timing
    finally:
        coords.done()


def equivalent(case, reference, actual):
    if case in ('rebin', 'convolve', 'regrid'):
        expected, mask, world = reference
        actual, actual_mask, actual_world = actual
        np.testing.assert_array_equal(actual_mask, mask, strict=True)
        assert actual.dtype == expected.dtype, (actual.dtype, expected.dtype)
        np.testing.assert_allclose(actual_world, world, rtol=1e-9, atol=1e-9)
        np.testing.assert_allclose(actual[mask], expected[mask], rtol=1e-3, atol=1e-4)
    elif case == 'statistics':
        for expected, values in zip(reference, actual):
            np.testing.assert_allclose(values, expected, rtol=3e-6, atol=1e-8)
    elif case in ('toworldmany', 'topixelmany'):
        np.testing.assert_allclose(actual, reference, rtol=1e-9, atol=1e-6)
    else:
        np.testing.assert_array_equal(actual, reference, strict=True)


def benchmark(scratch, shape, points, repetitions, selection):
    rng = np.random.default_rng(61214)
    values = rng.uniform(.1, 5., size=shape).astype(np.float32)
    mask = rng.random(shape) > .07
    source = scratch / 'source.im'
    image = casatools.image()
    assert image.fromarray(str(source), values)
    assert image.putregion(pixelmask=mask)
    cs = image.coordsys()
    coords = cs.torecord()
    pixels = np.asfortranarray(rng.random((4, points)) * (np.asarray(shape)[:, None] - 1))
    world = cs.toworldmany(pixels)['numeric']
    shifted = cs.referencepixel()['numeric']
    shifted[:2] += [.36, .18]
    assert cs.setreferencepixel(shifted)
    regrid_record = cs.torecord()
    cs.done()
    image.done()

    results = {}
    for case in selection:
        print(f'{case}:', flush=True)
        times = {'CASA': [], 'casa-rs': []}
        for repeat in range(repetitions + 1):
            order = list(times) if repeat % 2 == 0 else list(reversed(times))
            observed = {}
            for name in order:
                api = casatools if name == 'CASA' else tools
                if case in ('toworldmany', 'topixelmany'):
                    observed[name], duration = coord_case(api, case, coords, pixels, world)
                else:
                    observed[name], duration = image_case(api, source, case, regrid_record, shape)
                if repeat:
                    times[name].append(duration)
            equivalent(case, observed['CASA'], observed['casa-rs'])
        speedup = (statistics.median(t[0] for t in times['CASA']) /
                   statistics.median(t[0] for t in times['casa-rs']))
        results[case] = {'samples_wall_and_cpu_ns': times, 'wall_speedup': speedup}
        print(f'{speedup:.2f}x', flush=True)
    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pilot', action='store_true')
    parser.add_argument('--repeat', type=int, default=4)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--case', choices=CASES, action='append')
    parser.add_argument('--casa-rs-revision', help='source revision of the installed extension')
    args = parser.parse_args()
    if args.repeat < 1:
        parser.error('--repeat must be positive')
    casa_version = casatools.version_string().replace('-', '.')
    if casa_version != '6.7.6.14':
        parser.error(f'CASA 6.7.6.14 is required, found {casa_version}')
    shape = [40, 48, 1, 8] if args.pilot else [512, 512, 1, 32]
    points = 512 if args.pilot else 262144
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with Path(_core.__file__).open('rb') as binary:
        extension_sha256 = hashlib.file_digest(binary, 'sha256').hexdigest()
    with tempfile.TemporaryDirectory(dir=output.parent) as scratch:
        measurements = benchmark(Path(scratch), shape, points, args.repeat, args.case or CASES)
    report = {
        'casa_version': casa_version,
        'casa_rs_revision': args.casa_rs_revision,
        'casa_rs_extension_sha256': extension_sha256,
        'platform': platform.platform(),
        'worker_environment': {name: os.environ.get(name) for name in WORKER_ENV},
        'cache': 'one warm-up per backend, fresh attachments for each timed call',
        'image_shape': shape,
        'coordinate_count': points,
        'cases': measurements,
    }
    output.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
