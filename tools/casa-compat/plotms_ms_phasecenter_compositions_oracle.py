# SPDX-License-Identifier: LGPL-3.0-or-later
"""Pair public PlotMS phase shifts with the pinned CASA visibility iterator."""

import numpy as np
from casatools import measures, mstransformer

from plotms_ms_columns_oracle import casa, compare_figure, compare_text, copy_ms, fixture, opened, reference, request


def compare_phasecenter_compositions(root, native, phasecenter):
    root.mkdir()
    vis, original = fixture(root)
    with opened(vis, True) as table:
        row = np.arange(table.nrows())
        uvw = np.array([-30 - row / 16, 42 + row / 8, 4 + row / 32])
        table.putcol('UVW', uvw)
    base = request(vis) | dict(xaxis='real', yaxis='imag', spw='0:1~3^2')
    shifted = materialize_shift(root / 'fixed', vis, phasecenter)
    reference_text = root / 'fixed.casa.txt'
    assert reference(**(base | dict(vis=str(shifted))), plotfile=str(reference_text), expformat='txt') is None
    galactic = galactic_direction(phasecenter)
    for name, target in (('j2000', phasecenter), ('galactic', galactic)):
        actual = root / f'{name}.native.txt'
        assert native(**base, phasecenter=target, transform=True, plotfile=str(actual), expformat='txt') is None
        assert compare_text(actual, reference_text) == 12
    options = base | dict(phasecenter=galactic, transform=True, title='Galactic phase center')
    compare_figure(root, native, options, reference_text, ('Galactic phase center', 'Real', 'Imag:corrected'))
    compare_polynomial_shift(root, native, vis, phasecenter)
    with opened(vis) as table:
        for name, values in (original | {'UVW': uvw}).items():
            np.testing.assert_array_equal(table.getcol(name), values)


def galactic_direction(phasecenter):
    me = measures()
    try:
        target = me.measure(me.direction(*phasecenter.split()), 'GALACTIC')
        return 'GALACTIC ' + ' '.join(f"{target[key]['value']:.17g}{target[key]['unit']}" for key in ('m0', 'm1'))
    finally:
        me.done()


def materialize_shift(folder, vis, phasecenter, *, polynomial=False):
    folder.mkdir()
    output = folder / vis.name
    if polynomial:
        # The phaseshift task's final FIELD update reads the entire mixed-shape column.
        # Its iterator can materialize the shifted visibilities without that update.
        tool = mstransformer()
        try:
            tool.config(dict(inputms=str(vis), outputms=str(output), datacolumn='ALL', reindex=False,
                             tviphaseshift=True, tviphaseshiftlib={'phasecenter': phasecenter}))
            tool.open()
            tool.run()
        finally:
            tool.done()
    else:
        assert casa.phaseshift(vis=str(vis), outputvis=str(output), phasecenter=phasecenter,
                               datacolumn='all') is None
    return output


def compare_polynomial_shift(root, native, vis, phasecenter):
    work = root / 'polynomial'
    work.mkdir()
    source = work / vis.name
    copy_ms(vis, source)
    with opened(source / 'FIELD', True) as table:
        coefficients = {}
        table.putcell('NUM_POLY', 0, 1)
        for name in ('PHASE_DIR', 'DELAY_DIR', 'REFERENCE_DIR'):
            constant = table.getcell(name, 0)
            assert constant.shape == (2, 1)
            coefficients[name] = np.concatenate((constant, [[3e-8], [-5e-8]]), axis=1)
            table.putcell(name, 0, coefficients[name])
    shifted = materialize_shift(work / 'shifted', source, phasecenter, polynomial=True)
    base = request(source) | dict(xaxis='real', yaxis='imag', spw='0:1~3^2')
    expected, actual = (work / f'polynomial.{engine}.txt' for engine in ('casa', 'native'))
    assert reference(**(base | dict(vis=str(shifted))), plotfile=str(expected), expformat='txt') is None
    options = base | dict(phasecenter=phasecenter, transform=True, title='Polynomial FIELD')
    assert native(**options, plotfile=str(actual), expformat='txt') is None
    assert compare_text(actual, expected) == 12
    compare_figure(work, native, options, expected, ('Polynomial FIELD', 'Real', 'Imag:corrected'))
    with opened(source / 'FIELD') as table:
        assert table.getcell('NUM_POLY', 0) == 1
        for name, values in coefficients.items():
            np.testing.assert_array_equal(table.getcell(name, 0), values)
