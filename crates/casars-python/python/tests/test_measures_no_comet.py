"""CASA measures results when no comet ephemeris is loaded."""
import json
import logging
from pathlib import Path

import numpy as np
import pytest
from casars.tools import measures

DATA = Path(__file__).parent / 'data'
ORACLE = json.loads((DATA / 'casa_measures_no_comet.json').read_text())
LOGGER = 'casars.tools.measures'


def test_comet_getters_without_table_match_casa(caplog):
    me=measures()
    for state in ['fresh','epoch','direction','position','done']:
        if state=='epoch': assert me.doframe(me.epoch('UTC','2020/01/01/00:00:00'))
        if state=='direction': assert me.doframe(me.direction('J2000','10deg','20deg'))
        if state=='position': assert me.doframe(me.position('ITRF','100m','200m','300m'))
        if state=='done': assert me.done()
        for case in ORACLE['cases']:
            if case['state']!=state: continue
            caplog.clear()
            with caplog.at_level(logging.WARNING,logger=LOGGER):
                actual=getattr(me,case['method'])()
            expected=case['result']
            if case['method']=='comettopo':
                assert actual['unit']==expected['unit']
                assert isinstance(actual['value'],np.ndarray)
                np.testing.assert_array_equal(actual['value'],expected['value'])
                assert caplog.messages==['Method comettopo fails!  No Topocentric Comet table present']
            else:
                assert actual==expected
                if case['method']=='cometname':
                    assert caplog.messages==['Method cometname fails! No Comet table present']
                else: assert caplog.messages==[]


def test_framecomet_remains_explicitly_unimplemented():
    with pytest.raises(NotImplementedError,match='#1053'):
        measures().framecomet('missing-comet-table')
