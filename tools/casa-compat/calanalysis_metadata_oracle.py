#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check calanalysis attachment and metadata against CASA 6.7.6.14."""

import argparse
from contextlib import chdir
from itertools import product
from pathlib import Path
from shutil import copytree

import casatasks
from casatools import calanalysis as casa_calanalysis, table
import numpy as np

from calibration_smoothcal_oracle import opened
from calibrater_apply_tool_oracle import result, same_record
from gaincal_delay_oracle import duplicate_rows
from record_casa_returns import make_cal_ms


NAMES = ('field', 'antenna', 'antenna1', 'antenna2', 'spw')
TEXT = ('calname', 'msname', 'viscal', 'partype', 'polbasis')
COUNTS = ('numfield', 'numantenna', 'numantenna1', 'numantenna2', 'numfeed', 'numtime', 'numspw')


def metadata(tool):
    methods = (*NAMES, *TEXT, *COUNTS, 'feed', 'time', 'numchannel', 'freq')
    result = {method: getattr(tool, method)() for method in methods}
    for name in NAMES:
        result[f'{name}_ids'] = getattr(tool, name)(name=False)
    assert type(result['time']) is np.ndarray and result['time'].dtype == np.dtype('float64')
    assert type(result['numchannel']) is np.ndarray and result['numchannel'].dtype == np.dtype('int64')
    return result


def prepare_table(vis, maker, kind, label):
    output = Path(f'{kind}-{label}.cal')
    maker(vis=str(vis), caltable=str(output), caltype=kind, parameter=[1.])
    assert duplicate_rows(table(), output/'SPECTRAL_WINDOW') == 1
    with opened(output/'SPECTRAL_WINDOW', True) as window:
        assert window.putcell('NAME', 1, 'SECOND')
        for index, count in enumerate((8, 3)):
            assert window.putcell('NUM_CHAN', index, count)
            assert window.putcell('CHAN_FREQ', index, 100e9 + (np.arange(count) + 15*index)*1e6)
    with opened(output, True) as cal:
        count = cal.nrows()
        values = {
            'FIELD_ID': [1], 'ANTENNA1': [0, 2, 4], 'ANTENNA2': [-1, 4, 1],
            'SPECTRAL_WINDOW_ID': [1], 'TIME': [9., 2., 7.],
        }
        for column, values in values.items():
            vector = np.resize(np.asarray(values, dtype=cal.getcol(column).dtype), count)
            assert cal.putcol(column, vector)
    return output


def read_attachment(factory, path, other, kind):
    tool = factory()
    assert tool.close() is False
    detached = metadata(tool)
    for key in TEXT:
        assert detached[key] == ''
    for key in COUNTS:
        assert detached[key] == 0
    for key in (*NAMES, 'feed'):
        assert detached[key] == []
    assert detached['time'].size == detached['numchannel'].size == 0
    assert detached['freq'] == {}
    assert tool.open() is False and tool.open('missing.cal') is False
    for wrong in (path, None, 17):
        assert result(tool.open, wrong) == (False, ('TypeError', 'argument _caltable must be a string'))
    assert tool.open(caltable=str(path)) is True
    before = metadata(tool)
    assert tool.open(str(other)) is False
    same_record(metadata(tool), before)
    for name, wrong in product(NAMES, (None, 0, np.bool_(False), 'X')):
        assert result(getattr(tool, name), wrong) == (
            False, ('TypeError', f"in method 'calanalysis_{name}', argument 2 of type 'bool'"))
    expected_type = 'Complex' if kind == 'amp' else 'Float'
    assert before['calname'] == str(path) and before['partype'] == expected_type
    assert before['field'] == ['FLUX'] and before['field_ids'] == ['1']
    assert before['antenna_ids'] == ['0', '1', '2', '3', '4']
    assert before['antenna1_ids'] == ['0', '2', '4']
    assert before['antenna2'][0] == 'NoName' and before['antenna2_ids'][0] == '-1'
    assert list(before['freq']) == ['0', '1'] and before['spw'][1] == 'SECOND'
    np.testing.assert_array_equal(before['numchannel'], [8, 3])
    np.testing.assert_array_equal(before['time'], [2., 7., 9.])

    with opened(path/'ANTENNA', True) as antenna:
        original = antenna.getcell('NAME', 2)
        assert antenna.putcell('NAME', 2, 'RENAMED')
    assert tool.antenna1() == ['A0', 'RENAMED', 'A4']
    with opened(path/'ANTENNA', True) as antenna:
        assert antenna.putcell('NAME', 2, original)
    assert tool.close() is True and tool.close() is False
    same_record(metadata(tool), detached)
    assert tool.open(str(other)) is True and tool.calname() == str(other)
    assert tool.close() is True
    return before


def check_window_order(source, readers):
    more = Path('many-windows.cal')
    copytree(source, more)
    with opened(more/'SPECTRAL_WINDOW', True) as window:
        original = {key: window.getcell(key, 0) for key in window.colnames()
                    if window.iscelldefined(key, 0)}
        assert window.addrows(10)
        for row in range(2, 12):
            for key, value in original.items():
                assert window.putcell(key, row, value)
    ids = [str(index) for index in range(12)]
    for label, factory in readers.items():
        tool = factory()
        assert tool.open(str(more))
        assert tool.spw(False) == ids
        assert list(tool.freq()) == sorted(ids)
        assert tool.close()
        print(label, '12 spectral-window keys', flush=True)


def run(work, reference_only):
    assert casatasks.version_string() == '6.7.6.14'
    readers = {'casa': casa_calanalysis}
    writers = {'casa': casatasks.gencal}
    if not reference_only:
        from casars.tasks import gencal
        from casars.tools import calanalysis

        readers['native'], writers['native'] = calanalysis, gencal
    work.mkdir()
    with chdir(work):
        vis = Path('source.ms').resolve()
        make_cal_ms(str(vis), channels=8)
        for label, maker in writers.items():
            outputs = {kind: prepare_table(vis, maker, kind, label) for kind in ('amp', 'opac')}
            for kind, reader in product(outputs, readers):
                other = next(path for name, path in outputs.items() if name != kind)
                record = read_attachment(readers[reader], outputs[kind], other, kind)
                if reader == 'casa':
                    expected = record
                else:
                    same_record(record, expected)
                print(label, kind, reader, record['partype'], record['feed'], flush=True)
        check_window_order(Path('amp-casa.cal'), readers)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=lambda path: Path(path).resolve())
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    run(args.work, args.reference_only)
