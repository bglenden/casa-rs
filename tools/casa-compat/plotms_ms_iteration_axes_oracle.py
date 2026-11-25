#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Pair static MeasurementSet iteration pages, positions and sample order."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path
from unittest.mock import patch

import numpy as np

from plotms_ms_columns_oracle import casa, compare_text, fixture, opened, plotms_launcher, reference, request
from plotms_ms_layout_oracle import occupied_marker_cells
from plotms_ms_spw_average_oracle import make_fixture


GRID = (2, 2)
PAINT = dict(gridrows=2, gridcols=2, expformat='png', width=500, height=360, dpi=100)
STYLE = dict(customsymbol=True, symbolshape='circle', symbolcolor='dd2255',
             symbolsize=5, symbolfill='fill', symboloutline=False, title='Selected visibility')
GRID_CELLS = ((0, 0), (0, 1), (1, 0), (1, 1))


def data_fixture(root):
    vis, _ = fixture(root)
    with opened(vis, True) as table:
        time = table.getcol('TIME')
        epochs = np.unique(time)
        assert len(epochs) == 12
        scans = np.array([1, 1, 1, 2, 2, 2, 3, 3, 3, 5, 5, 4], dtype=np.int32)
        table.putcol('SCAN_NUMBER', scans[np.searchsorted(epochs, time)])
    reversed_vis = root / 'reversed.ms'
    with opened(vis) as table:
        selection = table.selectrows(list(range(table.nrows() - 1, -1, -1)))
        try:
            selection.copy(str(reversed_vis), deep=True, valuecopy=True).done()
        finally:
            selection.done()
    return vis, reversed_vis


def plot_sections(path):
    sections = []
    for line in path.read_text().splitlines():
        if line.startswith('# From plot '):
            sections.append([])
        elif line and not line.startswith('#'):
            sections[-1].append(line.split())
    return sections


def panel_pages(sections, labels, offset):
    first = len(GRID_CELLS) - offset
    entries = list(zip(labels, sections, strict=True))
    remaining = entries[first:]
    return [list(zip(GRID_CELLS[offset:], entries[:first]))] + [
        list(zip(GRID_CELLS, remaining[index:index + len(GRID_CELLS)]))
        for index in range(0, len(remaining), len(GRID_CELLS))
    ]


def compare_case(root, native, name, controls, axis, labels, counts, *, check_current=False):
    from casars.tasks import _plotms_plot

    root = root / name
    root.mkdir()
    controls |= STYLE
    offset = controls.get('rowindex', 0) * GRID[1] + controls.get('colindex', 0)
    # The pinned TXT export can close the plot server when row zero is empty.
    # Read the same selected coordinates using a centered canvas for numeric evidence.
    centered = controls | dict(rowindex=0, colindex=0, exprange='all', expformat='txt')
    expected, actual = (root / f'{engine}.txt' for engine in ('casa', 'native'))
    assert reference(**(centered | dict(gridrows=2, gridcols=2)), plotfile=str(expected)) is None
    assert native(**(controls | dict(gridrows=2, gridcols=2, exprange='all', expformat='txt')),
                  plotfile=str(actual)) is None
    points = compare_text(actual, expected)
    all_points = root / 'all-panels.casa.txt'
    assert reference(**(centered | dict(gridrows=1, gridcols=len(labels))), plotfile=str(all_points)) is None
    sections = plot_sections(all_points)
    assert [len(points) for points in sections] == counts
    pages = panel_pages(sections, labels, offset)

    configure = _plotms_plot.configure
    recorded = []

    def record(figure, axes, document, options):
        configure(figure, axes, document, options)
        page = []
        for panel in figure.axes:
            cell = panel.get_subplotspec()
            assert cell.get_gridspec().get_geometry() == GRID
            page.append(((cell.rowspan.start, cell.colspan.start),
                         panel.get_title(), panel.lines[0].get_xydata()))
        recorded.append(page)

    for export in (('current', 'all') if check_current else ('all',)):
        page_plan = pages if export == 'all' else pages[:1]
        names = []
        recorded.clear()
        for index, page in enumerate(page_plan):
            file_labels = ','.join(label.replace(' & ', '_with_') for _, (label, _) in page)
            suffix = f'_{index + 1}' if index else ''
            names.append(f'{name}_{axis}{file_labels}{suffix}.png')
        options = controls | PAINT | dict(exprange=export)
        for engine, plot in (('casa', reference), ('native', native)):
            out = root / f'{engine}-{export}'
            out.mkdir()
            if engine == 'native':
                with patch.object(_plotms_plot, 'configure', record):
                    assert plot(**options, plotfile=str(out / f'{name}.png')) is None
            else:
                assert plot(**options, plotfile=str(out / f'{name}.png')) is None
            assert {p.name for p in out.glob('*.png')} == set(names)
            for file, page in zip(names, page_plan, strict=True):
                assert occupied_marker_cells(out / file, GRID) == {cell for cell, _ in page}
        for page, expected_page in zip(recorded, page_plan, strict=True):
            for (cell, title, xy), (expected_cell, (label, values)) in zip(page, expected_page, strict=True):
                suffix = f': {label}' if axis == 'Time' else f'{axis}: {label}'
                assert (cell, title) == (expected_cell, f'{STYLE["title"]} {suffix}')
                coords = np.array([point[:2] for point in values], dtype=float)
                np.testing.assert_allclose(xy, coords, rtol=1.2e-5, atol=1e-7)
    print(f'{name}: {points} paired TXT points, {sum(counts)} points across {len(pages)} PNG pages', flush=True)


def compare_axes(root, native):
    vis, reversed_vis = data_fixture(root)
    focus = dict(xaxis='channel', xdatacolumn='', yaxis='amp', ydatacolumn='data', spw='0:1')
    base = request(vis) | focus
    offset = dict(field='', rowindex=1, colindex=0)
    compare_case(root, native, 'scan-offset', base | offset | dict(iteraxis='scan'), 'Scan',
                 ('1', '2', '3', '4', '5'), [3, 3, 3, 1, 2], check_current=True)
    compare_case(root, native, 'field-offset', base | offset | dict(vis=str(reversed_vis), iteraxis='field'),
                 'Field', ('FLUX', 'PHASE'), [6, 6])
    compare_case(root, native, 'corr-offset', base | offset | dict(vis=str(reversed_vis), iteraxis='corr',
                 correlation='XX,YY', spw='0:1~3'), 'Corr', ('XX', 'YY'), [24, 36])
    win = make_fixture(root, integrations=2)
    compare_case(root, native, 'spw', base | dict(vis=str(win), field='', spw='0:1,1:1', iteraxis='spw'),
                 'Spw', ('0', '1'), [2, 2])
    triad = base | dict(antenna='A0&A1;A0&A2;A1&A2')
    compare_case(root, native, 'antenna', triad | dict(iteraxis='antenna'),
                 'Antenna', ('A0@P0', 'A1@P1', 'A2@P2'), [12, 12, 12])
    compare_case(root, native, 'baseline', triad | dict(iteraxis='baseline'),
                 'Baseline', ('A0@P0 & A1@P1_30m', 'A0@P0 & A2@P2_30m', 'A1@P1 & A2@P2_42m'), [6, 6, 6])
    compare_case(root, native, 'time', base | dict(iteraxis='time', correlation='XX,YY', spw='0:1~3'), 'Time',
                 ('21:52:15.2961', '21:52:25.2961', '21:52:35.2961', '21:52:45.2961', '21:52:55.2961',
                  '21:53:05.2961'), [5] * 6)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    parser.add_argument('--app-run', type=Path)
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    assert version('casaplotms') == '2.9.1'
    root = args.work.resolve()
    root.mkdir()
    os.environ['CASARS_MSEXPLORE_BIN'] = str(args.native_bin.resolve())
    from casars.tasks import plotms as native
    with chdir(root), plotms_launcher(args.app_run):
        compare_axes(root, native)


if __name__ == '__main__':
    main()
