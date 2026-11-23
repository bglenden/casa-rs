#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare calanalysis fits with CASA 6.7.6.14 and NumPy polynomial fits."""

import argparse
from contextlib import chdir
from itertools import product
from pathlib import Path

import casatasks
from casatools import calanalysis as casa_calanalysis
import numpy as np

from calibration_smoothcal_oracle import opened
from record_casa_returns import make_cal_ms


ORDERS = ('AVERAGE', 'LINEAR', 'QUADRATIC')
CHANNELS = 24
OUTLIER = 10
FLAGGED = 3


def make_table(vis, path, maker, complex_values):
    maker(vis=str(vis), caltable=str(path),
          caltype='amp' if complex_values else 'opac', parameter=[1.])
    x = np.arange(1., CHANNELS + 1.)
    with opened(path/'SPECTRAL_WINDOW', True) as spw:
        assert spw.putcell('CHAN_FREQ', 0, x)
        assert spw.putcell('NUM_CHAN', 0, CHANNELS)
        assert spw.putcell('REF_FREQUENCY', 0, x.mean())
        for column in ('CHAN_WIDTH', 'EFFECTIVE_BW', 'RESOLUTION'):
            assert spw.putcell(column, 0, np.ones_like(x))
    with opened(path, True) as cal:
        subtype = 'B Jones' if complex_values else 'B TSYS'
        assert cal.putinfo(cal.info() | {'subType': subtype})
        assert cal.putkeyword('VisCal', subtype)
        for row in range(cal.nrows()):
            assert cal.putcell('FIELD_ID', row, 0)
            assert cal.putcell('TIME', row, 7.)
            amplitude = 2. + .1*x + .004*x**2 + .02*np.sin(x)
            amplitude[OUTLIER] += 100.
            column, values = ('CPARAM', (amplitude*np.exp(1j*(2.6 + .2*x))).astype(np.complex64)) \
                if complex_values else ('FPARAM', amplitude.astype(np.float32))
            errors = (.1 + .006*x).astype(np.float32)
            for name, cell in [(column, values), ('FLAG', x == FLAGGED + 1), ('PARAMERR', errors)]:
                assert cal.putcell(name, row, np.stack((cell, cell)))


def close(actual, expected):
    np.testing.assert_allclose(actual, expected, rtol=2e-5, atol=3e-8)


def compare_records(actual, expected):
    assert type(actual) is type(expected), (type(actual), type(expected))
    if isinstance(expected, dict):
        assert list(actual) == list(expected)
        for key in expected:
            compare_records(actual[key], expected[key])
    elif isinstance(expected, np.ndarray):
        assert actual.dtype == expected.dtype
        close(actual, expected)
    elif isinstance(expected, float):
        close(actual, expected)
    else:
        assert actual == expected, (actual, expected)


def check_polynomial(record):
    order = ORDERS.index(record['order'])
    weighted = record['weight'] == 'YES'
    if record['type'] == 'ROBUST' and order == 2:
        assert record['validFit'] is False
        assert not any(record[key].size for key in ('pars', 'vars', 'covars', 'model', 'res'))
        assert record['redChi2'] == record['resMean'] == record['resVar'] == 0.
        return
    assert record['validFit'] is True
    use = ~record['flag']
    x, y = record[record['abscissa']], record['value']
    basis = np.polynomial.polynomial.polyvander(x, order)
    sigma = record['valueErr'][use] if weighted else np.ones(sum(use))
    design = basis[use]/sigma[:, None]
    params = np.linalg.lstsq(design, y[use]/sigma, rcond=None)[0]
    cov = np.linalg.inv(design.T@design)
    model = basis@params
    residuals = y-model
    metric = np.sum((residuals[use]/sigma)**2)/(sum(use)-order)
    if not weighted:
        cov *= metric
    for key, value in (('pars', params), ('model', model), ('res', residuals),
                       ('vars', np.diag(cov)), ('covars', cov[np.triu_indices(order+1, 1)]),
                       ('redChi2', metric if weighted else 1.), ('resMean', residuals.mean()),
                       ('resVar', residuals.var(ddof=1))):
        close(record[key], value)
    if record['type'] == 'ROBUST':
        np.testing.assert_array_equal(np.flatnonzero(record['flag']), [FLAGGED, OUTLIER])


def exercise(path, readers, complex_values):
    tools = {label: factory() for label, factory in readers.items()}
    for tool in tools.values():
        assert tool.fit() == {} and tool.open(str(path))
    cases = [dict(order=order, type=mode, weight=weight)
             for order, mode, weight in product(ORDERS, ('LSQ', 'ROBUST'), (False, True))]
    if complex_values:
        cases.extend([dict(ap='P', unwrap=True, jumpmax=.7, order='L', type='L', weight=True),
                      dict(norm=True, order='a', type='l', weight=True)])
    cases.extend([dict(order='invalid'), dict(type='invalid'), dict(order='', type='')])
    for options in cases:
        outputs = {label: tool.fit(antenna='0', **options) for label, tool in tools.items()}
        reference = outputs['casa']
        if 'native' in outputs:
            compare_records(outputs['native'], reference)
        if 'invalid' in options.values():
            assert reference == {}
        else:
            assert len(reference) == 2
            for record in reference.values():
                check_polynomial(record)
        print(path.name, options, len(reference), flush=True)
    for tool in tools.values():
        assert tool.close()


def compare(work, reference_only):
    assert casatasks.version_string() == '6.7.6.14'
    readers, writers = {'casa': casa_calanalysis}, {'casa': casatasks.gencal}
    if not reference_only:
        from casars.tasks import gencal
        from casars.tools import calanalysis

        readers['native'], writers['native'] = calanalysis, gencal
    work.mkdir()
    with chdir(work):
        vis = work/'source.ms'
        make_cal_ms(str(vis), channels=CHANNELS)
        for writer, maker in writers.items():
            for complex_values in (True, False):
                kind = 'complex' if complex_values else 'real'
                path = work/f'{writer}-{kind}.cal'
                make_table(vis, path, maker, complex_values)
                exercise(path, readers, complex_values)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=lambda path: Path(path).resolve())
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    compare(args.work, args.reference_only)
