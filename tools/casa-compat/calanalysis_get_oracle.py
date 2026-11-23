#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare selected calanalysis data series against CASA 6.7.6.14."""

import argparse
from contextlib import chdir
from pathlib import Path
import signal
from subprocess import run
from time import perf_counter
import sys

import casatasks
from casatools import calanalysis as casa_calanalysis, table
import numpy as np

from calibration_smoothcal_oracle import opened
from gaincal_delay_oracle import duplicate_rows
from record_casa_returns import make_cal_ms


ROWS = [(1, 3, 1, 7.), (0, 1, 0, 9.), (0, 1, 1, 7.),
        (0, 1, 0, 7.), (0, 1, 0, 7.), (0, 3, 1, 8.)]
CASES = [
    ({}, 8), ({'axis': 'F'}, 34), ({'field': 'FLUX'}, 6), ({'field': [1]}, 2),
    ({'antenna': 'A1'}, 4), ({'antenna': 'A1&A3'}, 4),
    ({'spw': '0:1~7^2,1:0~2'}, 8), ({'spw': '1,0:1;1;3;5'}, 8),
    ({'timerange': [7., 8.]}, 6), ({'timerange': [7., 7.]}, 4),
    ({'timerange': [8., 7.]}, 0), ({'ap': 'p', 'unwrap': True, 'jumpmax': .7}, 8),
    ({'ap': 'PHASE', 'unwrap': True}, 8), ({'ap': 'P', 'axis': 'F', 'unwrap': True}, 34),
    ({'norm': True}, 8), ({'axis': 'garbage'}, 0), ({'jumpmax': -1.}, 0),
    ({'field': 'does-not-exist'}, 0), ({'feed': 'does-not-exist'}, 0),
    ({'field': 1}, 2), ({'spw': [1, 0]}, 8), ({'antenna': '!A1'}, 0),
    ({'antenna': 'A1;!A1'}, 4), ({'feed': [1, 2]}, 8), ({'feed': '2,1'}, 8),
    ({'timerange': [6., 8.]}, 6), ({'timerange': [8.01, 8.99]}, 0),
    ({'timerange': np.array([7., 9.])}, 8), ({'timerange': [7.]}, 0),
    ({'timerange': ['7', '8']}, 6), ({'timerange': [[7., 8.]]}, 6),
    ({'timerange': '7~8'}, 0),
]


def make_table(vis, path, maker, complex_values):
    maker(vis=str(vis), caltable=str(path), caltype='amp' if complex_values else 'opac', parameter=[1.])
    assert duplicate_rows(table(), path/'SPECTRAL_WINDOW') == 1
    with opened(path/'SPECTRAL_WINDOW', True) as spw, opened(vis/'SPECTRAL_WINDOW') as source:
        for row, count in enumerate((8, 3)):
            for name in ('CHAN_FREQ', 'CHAN_WIDTH', 'EFFECTIVE_BW', 'RESOLUTION'):
                column = source.getcell(name, 0)[:count].copy()
                if name == 'CHAN_FREQ':
                    column += row*20e6
                assert spw.putcell(name, row, column)
            assert spw.putcell('NUM_CHAN', row, count)
        assert spw.putcell('NAME', 1, 'FAR')
    with opened(path, True) as cal:
        subtype = 'B Jones' if complex_values else 'B TSYS'
        cal.putinfo(cal.info() | {'subType': subtype})
        assert cal.putkeyword('VisCal', subtype)
        assert cal.nrows() == 5
        prior = {key: cal.getcell(key, 0) for key in cal.colnames() if cal.iscelldefined(key, 0)}
        assert cal.addrows(1)
        for key, value in prior.items():
            assert cal.putcell(key, 5, value)
        for row, (field, antenna, spw, time) in enumerate(ROWS):
            for name, value in (('FIELD_ID', field), ('ANTENNA1', antenna), ('ANTENNA2', -1),
                                ('SPECTRAL_WINDOW_ID', spw), ('TIME', time)):
                assert cal.putcell(name, row, value)
            count = 3 if spw else 8
            channel = np.arange(count, dtype=np.float32)
            amplitude = 1.+row+channel[None, :]*.1+np.array([[0.], [.4]], dtype=np.float32)
            if complex_values:
                data = (amplitude*np.exp(1j*(2.5+.7*channel))).astype(np.complex64)
                assert cal.putcell('CPARAM', row, data)
            else:
                assert cal.putcell('FPARAM', row, amplitude)
            flags = np.zeros((2, count), dtype=bool)
            flags[0, -1] = True
            assert cal.putcell('FLAG', row, flags)
            assert cal.putcell('PARAMERR', row, np.full((2, count), .1, dtype=np.float32))


def equal(actual, expected):
    assert type(actual) is type(expected), (type(actual), type(expected))
    if isinstance(expected, dict):
        assert list(actual) == list(expected)
        for key in expected:
            equal(actual[key], expected[key])
    elif isinstance(expected, np.ndarray):
        assert actual.dtype == expected.dtype
        np.testing.assert_allclose(actual, expected, rtol=0., atol=2e-6)
    else:
        assert actual == expected, (actual, expected)


def restricted_feed(path, readers):
    script = """
from casatools import calanalysis
import sys
t = calanalysis()
assert t.open(sys.argv[1])
print('attached', flush=True)
t.get(feed='1')
"""
    pinned = run([sys.executable, '-c', script, str(path)], capture_output=True, text=True,
                 timeout=30, check=False)
    assert pinned.returncode == -signal.SIGSEGV and 'attached' in pinned.stdout, pinned
    if 'native' in readers:
        tool = readers['native']()
        assert tool.open(str(path))
        complete = tool.get()
        chosen = [record for record in complete.values() if record['feed'] == '1']
        restricted = tool.get(feed='1')
        equal(restricted, {str(index): record for index, record in enumerate(chosen)})
        assert tool.close()


def check_group_delay(path, readers):
    for descending in (False, True):
        order = np.arange(8.)[::-1] if descending else np.arange(8.)
        for carrier, spacing in ((1e9, 1e6), (1e11, 1e3)):
            frequency = carrier + spacing*order
            with opened(path/'SPECTRAL_WINDOW', True) as spw:
                assert spw.putcell('CHAN_FREQ', 0, frequency)
            for delay in (0., .18/spacing):
                expected = 2.7 + 2*np.pi*delay*(frequency-frequency[0])
                gain = np.exp(1j*expected).astype(np.complex64)
                with opened(path, True) as cal:
                    assert cal.putcell('CPARAM', 4, np.stack((gain,gain)))
                    assert cal.putcell('FLAG', 4, np.zeros((2,8),bool))
                options = dict(field='0', antenna='1', spw='0', timerange=[7.,7.], ap='P',unwrap=True)
                for engine, factory in readers.items():
                    # Pinned CASA would evaluate roughly 200 million trial
                    # delays for the fine band. Use the analytic law there.
                    if engine == 'casa' and spacing == 1e3:
                        continue
                    tool = factory()
                    assert tool.open(str(path))
                    start = perf_counter()
                    output = tool.get(**options)
                    duration = perf_counter()-start
                    assert len(output)==2 and tool.close()
                    for record in output.values():
                        np.testing.assert_array_equal(record['frequency'], frequency)
                        if engine == 'casa' and descending:
                            if delay == 0.:
                                assert np.max(np.abs(record['value']-expected)) > 2*np.pi
                        else:
                            np.testing.assert_allclose(record['value'],expected,atol=2e-6,rtol=0.)
                    if engine == 'native' and spacing==1e3:
                        assert duration < 5., duration
                    print(engine, 'group delay', carrier, spacing, descending, delay, duration,flush=True)


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
        make_cal_ms(str(vis), channels=8)
        for writer, maker in writers.items():
            for complex_values in (True, False):
                kind = 'complex' if complex_values else 'real'
                path = work/f'{writer}-{kind}.cal'
                make_table(vis, path, maker, complex_values)
                tools = {reader: factory() for reader, factory in readers.items()}
                for tool in tools.values():
                    assert tool.get() == {} and tool.open(str(path))
                for options, count in CASES:
                    outputs = {label: tool.get(**options) for label, tool in tools.items()}
                    assert len(outputs['casa']) == count, (kind, options, outputs['casa'])
                    if 'native' in outputs:
                        equal(outputs['native'], outputs['casa'])
                    print(writer, kind, options, count, flush=True)
                for tool in tools.values():
                    assert tool.close()
                restricted_feed(path, readers)
                if complex_values:
                    check_group_delay(path, readers)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=lambda path: Path(path).resolve())
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    compare(args.work, args.reference_only)
