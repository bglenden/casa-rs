#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare CASA and native applycal on the same partitioned MeasurementSet."""
import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import shutil

import casatasks
from casatools import ms, table
import numpy as np

from applycal_oracle import history
from calibration_lifecycle_oracle import contents, equal, snapshot
from record_casa_returns import make_cal_ms


def referenced_tables(path):
    tool = ms()
    tool.open(str(path))
    try:
        assert tool.ismultims()
        return [Path(p) for p in tool.getreferencedtables()]
    finally:
        tool.close()


def history_messages(path, label):
    messages = []
    for message in history(path)['MESSAGE']:
        if message.startswith('version: ') and messages and messages[-1] == 'taskname=applycal':
            message = 'version: <implementation>'
        messages.append(str(message).replace(label, '<engine>'))
    return messages


def compare_case(root, name, source, gain, arguments, native):
    paths = {label: root/f'{name}-{label}.mms' for label in ('casa', 'native')}
    for label, path in paths.items():
        casatasks.partition(vis=str(source), outputvis=str(path), separationaxis='scan',
                            numsubms=2, disableparallel=True, flagbackup=False)
        assert len(referenced_tables(path)) == 2

    records = {}
    for label, apply in [('casa', casatasks.applycal), ('native', native.applycal)]:
        path = paths[label]
        preload = os.environ.pop('LD_PRELOAD', None) if label == 'native' else None
        try:
            assert apply(**(dict(vis=str(path), gaintable=[str(gain)]) | arguments)) is None
        finally:
            if preload is not None:
                os.environ['LD_PRELOAD'] = preload

        backup = Path(str(path) + '.flagversions')
        parts = referenced_tables(path)
        for entry in backup.glob('flags.applycal_*'):
            version_list = (backup/'FLAG_VERSION_LIST').read_text()
            comment = next(line for line in version_list.splitlines()
                           if line.startswith(entry.name.removeprefix('flags.') + ' : '))
            timestamp = datetime.strptime(comment.split(' : Flags autosave on ')[1],
                                          '%Y-%m-%d %H:%M:%S')
            assert abs((datetime.now()-timestamp).total_seconds()) < 60, comment
        records[label] = {
            'backups': sorted(item.name for item in backup.glob('flags.*')),
            'partitions': [{'path': str(part),
                            'columns': sorted(contents(part)),
                            'apply_history': history_messages(part, f'{name}-{label}.mms').count('taskname=applycal'),
                            'local_backup': Path(str(part)+'.flagversions').exists()} for part in parts],
        }

    assert records['casa']['backups'] == records['native']['backups']
    if records['casa']['backups']:
        for filename in records['casa']['backups']:
            left = Path(str(paths['casa'])+'.flagversions')/filename
            right = Path(str(paths['native'])+'.flagversions')/filename
            assert contents(left).keys() == contents(right).keys()
            equal(left, right)
    for left, right in zip(referenced_tables(paths['casa']), referenced_tables(paths['native']), strict=True):
        assert contents(left).keys() == contents(right).keys()
        equal(left, right)
        a, b = history_messages(left, f'{name}-casa.mms'), history_messages(right, f'{name}-native.mms')
        # Compare task provenance, away from the partition operation's clocks
        # and private operation-specific history.
        if 'taskname=applycal' in a or 'taskname=applycal' in b:
            assert 'taskname=applycal' in a and 'taskname=applycal' in b
            assert a[a.index('taskname=applycal'):] == b[b.index('taskname=applycal'):], (a, b)
        assert Path(str(left)+'.flagversions').exists() == Path(str(right)+'.flagversions').exists()
    if name == 'default':
        for label in paths:
            flags = contents(Path(str(paths[label])+'.flagversions')/'flags.applycal_1')
            midpoint = len(flags['FLAG_ROW'])//2
            assert not np.array_equal(flags['FLAG'][..., :midpoint], flags['FLAG'][..., midpoint:])
            assert not np.array_equal(flags['FLAG_ROW'][:midpoint], flags['FLAG_ROW'][midpoint:])
        native_ms = paths['native']
        backup = Path(str(native_ms)+'.flagversions')/'flags.applycal_1'
        flags_before = contents(backup)
        applied = contents(native_ms)
        changed = int(np.count_nonzero(applied['FLAG'] != flags_before['FLAG']))
        assert changed > 0
        casatasks.flagmanager(vis=str(native_ms), mode='restore', versionname='applycal_1')
        restored = contents(native_ms)
        for column in ['FLAG', 'FLAG_ROW']:
            np.testing.assert_array_equal(restored[column], flags_before[column])
        records['native']['restored_with_casa'] = changed
    return records


def compare_missing_partition(root, source, gain, native):
    records = {}
    for label, apply in [('casa', casatasks.applycal), ('native', native.applycal)]:
        path = root/f'missing-partition-{label}.mms'
        casatasks.partition(vis=str(source), outputvis=str(path), separationaxis='scan',
                            numsubms=2, disableparallel=True, flagbackup=False)
        first, missing = referenced_tables(path)
        shutil.rmtree(missing)
        before = snapshot(first)
        preload = os.environ.pop('LD_PRELOAD', None) if label == 'native' else None
        try:
            apply(vis=str(path), gaintable=[str(gain)], flagbackup=False, applymode='calonly')
        except RuntimeError as error:
            record = dict(error=type(error).__name__, message=str(error))
        else:
            raise AssertionError(f'{label} accepted a missing Multi-MS partition')
        finally:
            if preload is not None:
                os.environ['LD_PRELOAD'] = preload
        assert snapshot(first) == before, f'{label} changed the first partition'
        assert not Path(str(path)+'.flagversions').exists()
        records[label] = dict(record, first_partition_unchanged=True)
    return records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    options = parser.parse_args()
    assert casatasks.version_string() == '6.7.6.14'
    os.environ['CASARS_CALIBRATE_BIN'] = str(options.native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    from casars import tasks as native
    root = options.work.resolve()
    root.mkdir(parents=True, exist_ok=True)
    assert not any(root.iterdir()), 'work directory must be empty'
    os.chdir(root)
    source = root/'source.ms'
    gain = root/'amplitude.gcal'
    make_cal_ms(str(source))
    tool = table()
    tool.open(str(source), nomodify=False)
    if 'CORRECTED_DATA' in tool.colnames():
        tool.removecols(['CORRECTED_DATA'])
    # Make backup row order observable. The first scan has a marked full row
    # and the other scan has a different subset of flagged channel samples.
    scans = tool.getcol('SCAN_NUMBER')
    first = np.flatnonzero(scans == scans.min())
    second = np.flatnonzero(scans != scans.min())
    flags = tool.getcol('FLAG')
    flags[..., first[0]] = True
    flags[0, 1, second[::3]] = True
    flag_rows = tool.getcol('FLAG_ROW')
    flag_rows[first[0]] = True
    tool.putcol('FLAG', flags)
    tool.putcol('FLAG_ROW', flag_rows)
    tool.close()
    casatasks.gencal(vis=str(source), caltable=str(gain), caltype='amp', parameter=[2.])
    tool = table()
    tool.open(str(gain), nomodify=False)
    flags = tool.getcol('FLAG')
    flags[:, :, tool.getcol('ANTENNA1') == 0] = True
    tool.putcol('FLAG', flags)
    tool.close()

    library = root/'apply.callib'
    library.write_text(f"caltable='{gain}' field='PHASE'\n")
    result = {}
    for label, arguments in [
        ('default', {}),
        ('field', dict(field='PHASE')),
        ('trial', dict(applymode='trial')),
        ('calonly', dict(applymode='calonly')),
        ('flagonly', dict(applymode='flagonly')),
        ('calflagstrict', dict(applymode='calflagstrict')),
        ('flagonlystrict', dict(applymode='flagonlystrict')),
        ('without-backup', dict(flagbackup=False)),
        ('callibrary', dict(docallib=True, callib='apply.callib')),
        ('missing-caltable', dict(gaintable=[str(root/'missing.gcal')])),
    ]:
        result[label] = compare_case(root, label, source, gain, arguments, native)
        (root/'results.json').write_text(json.dumps(result, indent=2)+'\n')
        print(label, 'passed', flush=True)
    result['missing-partition'] = compare_missing_partition(root, source, gain, native)
    (root/'results.json').write_text(json.dumps(result, indent=2)+'\n')
    print('missing-partition passed', flush=True)


if __name__ == '__main__':
    main()
