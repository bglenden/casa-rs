#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare direct calibrater factory spellings against pinned CASA."""

import argparse
from contextlib import chdir
from itertools import product
from pathlib import Path

import casatasks
from casatools import calibrater as casa_calibrater

from calibrater_apply_tool_oracle import result, same_record
from gaincal_delay_oracle import delay_data


ALIASES = {
    'PREFIXKANTPOSSUFFIX': 'KAntPos', 'KANTPOS ': 'KAntPos', 'kAnTpOs': 'KAntPos',
    'PREFIXFRINGESUFFIX': 'FRINGE', 'FRINGE ': 'FRINGE',
    'PREFIXACCORSUFFIX': 'ACCOR', 'ACCOR ': 'ACCOR',
    'PREFSDSKY_PSSUFFIX': 'SDSKY_PS', 'PREFIXSDSKY_RASTERSUFFIX': 'SDSKY_RASTER',
    'PREFIXSDGAIN_OTFDSUFFIX': 'SDGAIN_OTFD',
}
REJECTED = (False, ('RuntimeError', 'Error in Calibrater::setsolve.'))


def setup(factory, vis, legacy, family):
    tool = factory()
    assert tool.setvi(legacy, True) and tool.open(str(vis), addcorr=False, addmodel=False)
    assert tool.selectvis(field='FLUX') and tool.setptmodel([1, 0, 0, 0])
    returned = result(tool.setsolve, family, 'inf', 'unused.cal',
                      refant='A0', minsnr=0., minblperant=2)
    state = tool.state() if returned[0] else None
    assert tool.close()
    return returned, state


def compare(work, reference_only):
    assert casatasks.version_string() == '6.7.6.14'
    engines = {'casa': casa_calibrater}
    if not reference_only:
        from casars.tools import calibrater

        engines['native'] = calibrater
    work.mkdir()
    with chdir(work):
        vis = work/'source.ms'
        delay_data(vis, 'K')
        for legacy, (alias, canonical) in product((False, True), ALIASES.items()):
            for factory in engines.values():
                outcome, state = setup(factory, vis, legacy, alias)
                expected, canonical_state = setup(factory, vis, legacy, canonical)
                assert outcome == expected == (True, True), (alias, legacy, outcome, expected)
                same_record(state, canonical_state)
            print('alias', legacy, repr(alias), canonical, flush=True)
        for legacy, factory in product((False, True), engines.values()):
            for name in ('SDSKY_OTF', 'PREFSDSKY_OTFSUFFIX', 'SDSKY_OTF ', 'SDSKY_OTFD'):
                assert setup(factory, vis, legacy, name)[0] == (
                    (True, True) if legacy else REJECTED)
            for name in ('XJONES', ' B'):
                assert setup(factory, vis, legacy, name)[0] == REJECTED
            for name in ('DLIN+X', 'D+RANDOM', 'D+QU', 'DF+X', 'X JONES'):
                assert setup(factory, vis, legacy, name)[0] == (True, True)
        assert not (work/'unused.cal').exists()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=lambda path: Path(path).resolve())
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    compare(args.work, args.reference_only)
