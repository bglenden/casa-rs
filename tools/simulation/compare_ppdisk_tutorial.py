#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Read CASA/native tutorial products with C++ casatools and compare all figures.

Run with CASA Python after ppdisk_tutorial.py has run with both engines.
Numerical acceptance is NRMS <= 1e-3; masks, labels, units and artist styles
must agree. The standing CASA fidelity rounding/unit correction is explicit.
"""
from __future__ import annotations
import argparse
import gzip
import json
import os
from pathlib import Path

os.environ.setdefault('MPLBACKEND', 'Agg')
import numpy as np
from ppdisk_tutorial import FIGURES

STEM = 'psim2/psim2.alma.out20'


def nrms(actual, expected):
    a, e = np.asarray(actual), np.asarray(expected)
    assert a.shape == e.shape and np.all(np.isfinite(a)) and np.all(np.isfinite(e))
    return float(np.linalg.norm(a.astype(complex)-e.astype(complex)) /
                 max(np.linalg.norm(e.astype(complex)), np.finfo(float).tiny))


def close(actual, expected, tolerance=1e-3):
    a, e = np.asarray(actual), np.asarray(expected)
    assert a.shape == e.shape and np.all(np.isfinite(a)) and np.all(np.isfinite(e))
    scale = max(float(np.max(np.abs(e), initial=0)), np.finfo(float).tiny)
    assert np.max(np.abs(a-e), initial=0) <= tolerance * scale, (a, e)


def read(path):
    from casatools import image
    tool = image()
    assert tool.open(str(path))
    cs = tool.coordsys()
    result = dict(pixels=tool.getchunk(), mask=tool.getchunk(getmask=True),
                  unit=tool.brightnessunit(), beam=tool.restoringbeam(),
                  reference=cs.referencevalue()['numeric'], increment=cs.increment()['numeric'],
                  reference_pixel=cs.referencepixel()['numeric'], projection=cs.projection(),
                  frames={kind: cs.referencecode(kind) for kind in ['direction', 'spectral']
                          if cs.findcoordinate(kind)['return']})
    cs.done()
    tool.done()
    return result


def compare_metadata(actual, expected):
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys()
        for key in expected:
            compare_metadata(actual[key], expected[key])
    elif isinstance(expected, str):
        assert actual == expected
    else:
        close(actual, expected)


def compare_coordinates(actual, expected):
    # Each axis has its own physical unit. A frequency value must never set
    # the tolerance for an angle, pixel position or angular cell increment.
    for key in ['reference', 'increment', 'reference_pixel']:
        a, e = np.asarray(actual[key]), np.asarray(expected[key])
        assert a.shape == e.shape and np.all(np.isfinite(a)) and np.all(np.isfinite(e))
        np.testing.assert_allclose(a, e, rtol=1e-10, atol=1e-15)
    assert actual['frames'] == expected['frames']
    compare_metadata(actual['projection'], expected['projection'])


def compare_images(casa, native):
    results = {}
    for suffix in ['skymodel', 'skymodel.flat', 'psf', 'model', 'residual', 'image',
                   'pb', 'image.pbcor', 'sumwt', 'mask', 'image.flat',
                   'skymodel.flat.regrid', 'skymodel.flat.regrid.conv', 'diff', 'fidelity']:
        truth, actual = read(str(casa / STEM)+'.'+suffix), read(str(native / STEM)+'.'+suffix)
        error = nrms(actual['pixels'], truth['pixels'])
        assert np.array_equal(actual['mask'], truth['mask']), suffix + ' masks'
        compare_metadata(actual['beam'], truth['beam'])
        compare_coordinates(actual, truth)
        if suffix != 'fidelity':
            assert actual['unit'] == truth['unit'], suffix + ' units'
            assert error <= 1e-3, (suffix, error)
        else:
            assert actual['unit'] == ''
        results[suffix] = dict(nrms=error, mask_disagreements=0, units=[truth['unit'], actual['unit']])
    corrected = read(casa / 'corrected-fidelity.image')
    actual = read(str(native / STEM)+'.fidelity')
    error = nrms(actual['pixels'], corrected['pixels'])
    assert error <= 1e-3, ('corrected fidelity', error)
    results['fidelity_corrected'] = dict(nrms=error)
    return results


def compare_ms(casa, native):
    from casatools import table
    sets = []
    for root in [casa, native]:
        tool = table()
        assert tool.open(str(root / STEM)+'.ms')
        sets.append({key: tool.getcol(key) for key in ['DATA', 'UVW', 'TIME', 'FLAG', 'WEIGHT', 'SIGMA']})
        tool.done()
    truth, actual = sets
    error = nrms(actual['DATA'], truth['DATA'])
    assert error <= 1e-3
    uvw = float(np.max(np.abs(actual['UVW']-truth['UVW'])))
    assert uvw <= 1e-3
    for key in ['TIME', 'FLAG', 'WEIGHT', 'SIGMA']:
        assert np.array_equal(actual[key], truth[key]), key
    return dict(rows=len(truth['TIME']), data_nrms=error, uvw_max_m=uvw,
                time_flags_weights_sigma='exact')


def compare_artists(actual, expected):
    from matplotlib import colors
    # CASA adds each colour scale immediately, the native host adds them after
    # the data axes. This changes creation order, not panel placement/content.
    order = lambda axes: sorted(axes, key=lambda axis: not bool(axis['images']))
    actual, expected = order(actual), order(expected)
    assert len(actual) == len(expected)
    maximum = 0.
    for a, e in zip(actual, expected, strict=True):
        for key in ['title', 'xlabel', 'ylabel', 'aspect', 'frame_on']:
            assert a[key] == e[key], (key, a[key], e[key])
        for key in ['xlim', 'ylim']:
            close(a[key], e[key])
        for key in ['lines', 'images', 'circles', 'anchored_circles', 'texts']:
            assert len(a[key]) == len(e[key]), key
        for a_line, e_line in zip(a['lines'], e['lines'], strict=True):
            for key in ['x', 'y']:
                close(a_line[key], e_line[key])
            for key in ['marker', 'linestyle', 'line_width']:
                assert a_line[key] == e_line[key]
            close(colors.to_rgba(a_line['color']), colors.to_rgba(e_line['color']), 1e-12)
        for a_image, e_image in zip(a['images'], e['images'], strict=True):
            error = nrms(a_image['pixels'], e_image['pixels'])
            assert error <= 1e-3, error
            maximum = max(maximum, error)
            close(a_image['extent'], e_image['extent'], 1e-10)
            close(a_image['clim'], e_image['clim'])
            for key in ['origin', 'interpolation', 'cmap']:
                assert a_image[key] == e_image[key]
        for group in ['circles', 'anchored_circles']:
            for ac, ec in zip(a[group], e[group], strict=True):
                for key in ['center', 'radius', 'edge_color', 'face_color', 'line_width']:
                    close(ac[key], ec[key])
                assert ac['fill'] == ec['fill']
        for at, et in zip(a['texts'], e['texts'], strict=True):
            assert at['text'] == et['text'], (at, et)
            close(at['position'], et['position'])
            for key in ['font_size', 'horizontal_alignment', 'vertical_alignment']:
                assert at[key] == et[key]
        for label in ['title', 'xlabel', 'ylabel']:
            if e[label]:
                assert a['font_sizes'][label] == e['font_sizes'][label]
        assert a['tick_font_sizes'] == e['tick_font_sizes']
    return maximum


def review_figures(casa, native, guides, out):
    from matplotlib import pyplot as plt
    with gzip.open(casa / 'artists.json.gz', 'rt') as source:
        raw = json.load(source)
    with gzip.open(casa / 'corrected-artists.json.gz', 'rt') as source:
        corrected = json.load(source)
    with gzip.open(native / 'artists.json.gz', 'rt') as source:
        actual = json.load(source)
    results = []
    for suffix in FIGURES:
        name = 'psim2.alma.out20.'+suffix+'.png'
        reference = corrected[name] if suffix == 'analysis' else raw[name]
        maximum = compare_artists(actual[name], reference)
        guide = next(guides.glob('*.'+suffix+'.png'))
        # Scientific review artifact, preserving the full original PNGs beside
        # numerical differences. Raster subtraction is not a parity criterion.
        fig, axes = plt.subplots(1, 3, figsize=(18, 6))
        for axis, path, label in zip(axes, [guide, casa/'original-figures'/name, native/'psim2'/name],
                                     ['Guide illustration (CASA 5.4)', 'CASA 6.7.6.14', 'casa-rs'], strict=True):
            axis.imshow(plt.imread(path)); axis.set_title(label); axis.axis('off')
        fig.tight_layout()
        fig.savefig(out / (suffix+'-comparison.png'), dpi=120)
        plt.close(fig)
        results.append(dict(figure=name, axes=len(reference), maximum_plane_nrms=maximum,
                            fidelity_control=suffix == 'analysis'))
    # Numerical difference panels on common physical axes, not raster deltas.
    fig, axes = plt.subplots(1, 3, figsize=(12, 4))
    for axis, suffix in zip(axes, ['image', 'residual', 'diff'], strict=True):
        truth, result = read(str(casa/STEM)+'.'+suffix), read(str(native/STEM)+'.'+suffix)
        delta = np.squeeze(result['pixels']-truth['pixels']).T
        scale = np.max(np.abs(delta))
        artist = axis.imshow(delta, origin='lower', cmap='RdBu_r', vmin=-scale, vmax=scale)
        axis.set_title(suffix+' native − CASA [Jy/beam]')
        axis.set_xlabel('x pixel'); axis.set_ylabel('y pixel')
        fig.colorbar(artist, ax=axis)
    fig.tight_layout(); fig.savefig(out/'numerical-differences.png', dpi=150); plt.close(fig)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['casa', 'native', 'guides', 'out']:
        parser.add_argument('--'+name, required=True, type=Path)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    result = dict(oracle='CASA 6.7.6.14', acceptance_nrms=1e-3,
                  images=compare_images(args.casa, args.native),
                  measurement_set=compare_ms(args.casa, args.native),
                  figures=review_figures(args.casa, args.native, args.guides, args.out))
    (args.out/'comparison.json').write_text(json.dumps(result, indent=1)+'\n')
    print(json.dumps(result, indent=1))


if __name__ == '__main__':
    main()
