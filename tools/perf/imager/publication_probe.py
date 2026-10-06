#!/usr/bin/env python3
"""Bounded production-publication timing and exact native product guard for T55."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys

from perf_harness import t51_pair_guard as resource_guard

TEST = 't55_real_cube::t55_q_band_rebaseline_preflight'
PRODUCTS = {'.image', '.residual', '.model', '.psf', '.pb', '.mask', '.sumwt'}


def read(path):
    return json.loads(Path(path).read_text())


def save(path, value):
    with Path(path).open('x') as stream:
        json.dump(value, stream, indent=2)


def file_hash(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def input_hashes(path):
    return {str(item.relative_to(path)): file_hash(item)
        for item in sorted(path.rglob('*')) if item.is_file() and item.name != 'table.lock'}


def paired_statistics(pairs):
    assert len(pairs) in (4, 6), 'paired runs require a fixed four- or six-pair design'
    ratios = [math.log(pair['candidate']['result']['publication_seconds'] /
                      pair['parent']['result']['publication_seconds']) for pair in pairs]
    assert all(math.isfinite(value) for value in ratios)
    mean = statistics.mean(ratios)
    critical = {4: 3.182446305, 6: 2.570581836}[len(pairs)]
    half = critical * statistics.stdev(ratios) / math.sqrt(len(pairs))
    return {'ratio': math.exp(mean), 'ci95': [math.exp(mean-half), math.exp(mean+half)]}


def scientific_signature(result):
    fingerprints = result['product_fingerprints']
    assert set(fingerprints) == PRODUCTS
    assert all(len(value) == 64 and set(value) <= set('0123456789abcdef')
        for value in fingerprints.values())
    return {key: result[key] for key in (
        'rows', 'image_size', 'major_cycles', 'actual_minor_iterations', 'product_fingerprints')}


def worker(args):
    output = args.output
    output.mkdir()
    shutil.copytree(args.source_ms, output/'input.ms')
    environment = os.environ.copy()
    environment.update(CASA_RS_T55_REAL_MS=str(output/'input.ms'),
        CASA_RS_T55_ARTIFACT_ROOT=str(output/'products'),
        CASA_RS_T55_PREFLIGHT_IMAGE_SIZE='128', CASA_RS_T55_PREFLIGHT_ROWS='351',
        CASA_RS_T55_PUBLICATION_PROBE='1', CASA_RS_MEASURESPATH=str(args.measures),
        CASA_RS_IMAGING_SPILL_READ_BYTES_PER_SECOND='1000000000',
        CASA_RS_IMAGING_SPILL_WRITE_BYTES_PER_SECOND='1000000000',
        OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1', VECLIB_MAXIMUM_THREADS='1')
    command = [str(args.binary), TEST, '--exact', '--ignored', '--nocapture', '--test-threads=1']
    save(output/'command.json', {'command': command, 'environment': {
        key: value for key, value in environment.items() if key.startswith('CASA_RS_')}})
    subprocess.run(command, env=environment, check=True)


def sample(args, binary, output):
    command = [sys.executable, str(Path(__file__).resolve()), 'measure', '--worker',
        '--binary', str(binary), '--source-ms', str(args.source_ms),
        '--measures', str(args.measures), '--output', str(output)]
    resource_guard.RSS_BYTES = 8 << 30
    with output.with_suffix('.log').open('x') as log:
        receipt = resource_guard.run_pair_pipeline(command, cwd=Path.cwd(),
            environment=os.environ.copy(), log=log, wall_seconds=600)
    save(output.with_suffix('.resource.json'), receipt)
    assert receipt['complete'], receipt
    result = read(output/'products/summary.json')
    assert result['rows'] == 351 and result['image_size'] == 128
    assert math.isfinite(result['publication_seconds']) and result['publication_seconds'] > 0
    scientific_signature(result)
    return {'result': result, 'resource': receipt, 'output': str(output)}


def measure(args):
    assert math.isfinite(args.prior_index) and args.prior_index > 0
    assert not args.parent_binary or args.samples in (4, 6)
    args.output.mkdir(parents=True)
    inputs = input_hashes(args.source_ms)
    candidate = args.output/'candidate-application'
    shutil.copyfile(args.binary, candidate)
    candidate.chmod(0o755)
    parent = None
    if args.parent_binary:
        parent = args.output/'parent-application'
        shutil.copyfile(args.parent_binary, parent)
        parent.chmod(0o755)
    expected = read(args.reference_report)['scientific_signature'] if args.reference_report else None
    pairs, candidates = [], []
    for index in range(args.samples):
        pair = {}
        order = ['parent', 'candidate'] if index % 2 == 0 else ['candidate', 'parent']
        for role in order:
            if role == 'parent' and parent is None:
                continue
            measured = sample(args, candidate if role == 'candidate' else parent,
                args.output/f'{index:02}-{role}')
            signature = scientific_signature(measured['result'])
            if expected is None:
                expected = signature
            assert signature == expected, 'exact products or executed scientific work changed'
            pair[role] = measured
        candidates.append(pair['candidate']['result']['publication_seconds'])
        pairs.append(pair)
    statistics_result = paired_statistics(pairs) if parent else None
    assert input_hashes(args.source_ms) == inputs, 'source input changed during measurement'
    result = {'metric': 'publication stage sum, seconds',
        'median_publication_seconds': statistics.median(candidates),
        'publication_time_index': args.prior_index * statistics_result['ratio'] if parent else 1.0,
        'statistics': statistics_result, 'samples': pairs, 'scientific_signature': expected,
        'binary_sha256': file_hash(candidate), 'source_ms': str(args.source_ms),
        'input_files_sha256': inputs,
        'scope': '128-square, all 512 channels, 351 rows; not full-field acceptance'}
    save(args.output/'report.json', result)
    print(json.dumps({key: result[key] for key in (
        'median_publication_seconds', 'publication_time_index', 'statistics')}))


def guard(report):
    result = read(report)
    assert file_hash(Path(report).parent/'candidate-application') == result['binary_sha256']
    assert input_hashes(Path(result['source_ms'])) == result['input_files_sha256']
    expected = result['scientific_signature']
    for pair in result['samples']:
        for measured in pair.values():
            assert measured['resource']['complete']
            assert measured['resource']['rss_cap_bytes'] == 8 << 30
            assert measured['resource']['wall_cap_seconds'] == 600
            assert scientific_signature(measured['result']) == expected
            assert scientific_signature(read(Path(measured['output'])/'products/summary.json')) == expected
    stats = result['statistics']
    assert stats == (paired_statistics(result['samples']) if stats is not None else None)
    passed = stats is None or stats['ci95'][1] < 0.98
    print(json.dumps({'passed': passed, 'statistics': stats}))
    return 0 if passed else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='mode', required=True)
    measure_parser = sub.add_parser('measure')
    for name in ('binary', 'source-ms', 'measures', 'output'):
        measure_parser.add_argument('--'+name, type=lambda value: Path(value).resolve(), required=True)
    measure_parser.add_argument('--parent-binary', type=lambda value: Path(value).resolve())
    measure_parser.add_argument('--reference-report', type=Path)
    measure_parser.add_argument('--samples', type=int, choices=(1, 3, 4, 6), default=3)
    measure_parser.add_argument('--prior-index', type=float, default=1.0)
    measure_parser.add_argument('--worker', action='store_true', help=argparse.SUPPRESS)
    guard_parser = sub.add_parser('guard')
    guard_parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    if args.mode == 'guard':
        return guard(args.report)
    if args.worker:
        worker(args)
    else:
        measure(args)
    return 0


if __name__ == '__main__':
    sys.exit(main())
