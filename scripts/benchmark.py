#!/usr/bin/env python3
"""Offline mock benchmark matrix; JSON results go to stdout, progress to stderr.

Build with `cargo build --release --workspace --locked` first. Source generation
and output validation are streamed and excluded from measured command durations.
Requires Python 3.9+ on macOS/Linux; no Python packages or model services needed.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import itertools
import json
import math
import os
from pathlib import Path
import platform
import resource
import signal
import statistics
import subprocess
import sys
import tempfile
import time

CASES = ('jsonl', 'exact-memory', 'exact-disk', 'minhash-memory', 'minhash-disk',
         'parquet', 'judge')
SCRIPT = Path(__file__).resolve()
REPO = SCRIPT.parent.parent


def positive_list(values, label, maximum=None):
    try:
        result = [int(part) for value in values for part in value.split(',')]
    except ValueError as error:
        raise ValueError(f'{label} must contain positive integers') from error
    if not result or any(value <= 0 or (maximum and value > maximum) for value in result):
        raise ValueError(f'{label} must contain integers in 1..{maximum or "unbounded"}')
    return list(dict.fromkeys(result))


def percentile(values, quantile):
    """Linearly interpolated percentile; a single sample returns that sample."""
    values = sorted(values)
    position = (len(values) - 1) * quantile
    low, high = math.floor(position), math.ceil(position)
    return values[low] + (values[high] - values[low]) * (position - low)


def distribution(values):
    return {'median': statistics.median(values), 'p95': percentile(values, 0.95),
            'max': max(values)}


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def command_text(command):
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=10, check=True)
        return result.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None


def fingerprint(binary):
    cpu_model = platform.processor() or platform.machine()
    memory = None
    if sys.platform == 'darwin':
        cpu_model = command_text(['sysctl', '-n', 'machdep.cpu.brand_string']) or cpu_model
        memory_text = command_text(['sysctl', '-n', 'hw.memsize'])
        memory = int(memory_text) if memory_text else None
    elif Path('/proc/cpuinfo').exists():
        with Path('/proc/cpuinfo').open() as source:
            for line in source:
                if line.startswith('model name'):
                    cpu_model = line.partition(':')[2].strip()
                    break
    if memory is None:
        try:
            memory = os.sysconf('SC_PHYS_PAGES') * os.sysconf('SC_PAGE_SIZE')
        except (AttributeError, OSError, ValueError):
            pass
    return {
        'system': platform.system(), 'os_release': platform.release(),
        'os_version': platform.version(), 'architecture': platform.machine(),
        'cpu_model': cpu_model, 'logical_cpus': os.cpu_count(), 'memory_bytes': memory,
        'python': platform.python_version(),
        'rustc': command_text(['rustc', '--version', '--verbose']),
        'cargo': command_text(['cargo', '--version']),
        'binary': str(binary), 'binary_bytes': binary.stat().st_size,
        'binary_sha256': sha256_file(binary),
        'binary_modified_at_utc': datetime.fromtimestamp(binary.stat().st_mtime, timezone.utc).isoformat(),
        'build_profile_hint': 'release' if 'release' in binary.parts else
                              'debug' if 'debug' in binary.parts else 'unknown',
        'git_revision': command_text(['git', '-C', str(REPO), 'rev-parse', 'HEAD']),
        'git_status': command_text(['git', '-C', str(REPO), 'status', '--porcelain']),
        'cargo_lock_sha256': sha256_file(REPO / 'Cargo.lock'),
        'benchmark_sha256': sha256_file(SCRIPT),
    }


def tail(file, limit=8192):
    file.seek(0, os.SEEK_END)
    length = file.tell()
    file.seek(max(0, length - limit))
    return file.read().decode('utf-8', errors='replace')


def measure_child(command, timeout):
    """Runs in a fresh helper so RUSAGE_CHILDREN contains only this command."""
    started = time.perf_counter()
    with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
        timed_out = False
        try:
            result = subprocess.run(command, stdout=stdout, stderr=stderr, timeout=timeout)
            returncode = result.returncode
        except subprocess.TimeoutExpired:
            timed_out, returncode = True, None
        elapsed = time.perf_counter() - started
        rss = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
        stdout.seek(0)
        raw = stdout.read(8 * 1024 * 1024 + 1)
        report = None
        if len(raw) <= 8 * 1024 * 1024:
            try:
                report = json.loads(raw)
            except (ValueError, UnicodeError):
                pass
        measurement = {
            'seconds': elapsed, 'returncode': returncode, 'timed_out': timed_out,
            'peak_rss_bytes': int(rss if sys.platform == 'darwin' else rss * 1024),
            'report': report,
        }
        if timed_out or returncode or report is None:
            measurement['stderr_tail'] = tail(stderr)
            measurement['stdout_tail'] = raw[-8192:].decode('utf-8', errors='replace')
        return measurement


def measure(binary, command, config, timeout):
    helper = subprocess.run(
        [sys.executable, str(SCRIPT), '--measure', str(binary), command, str(config),
         '--timeout', str(timeout)],
        capture_output=True, text=True, timeout=timeout + 30, check=True,
    )
    return json.loads(helper.stdout)


def topic(position, text_bytes):
    prefix = f'sample-{position:012d}-'
    digest = hashlib.sha256(str(position).encode()).hexdigest()
    return prefix + (digest * ((text_bytes + 63) // 64))[:max(0, text_bytes - len(prefix))]


def write_source(path, records, text_bytes):
    with path.open('w') as output:
        for position in range(1, records + 1):
            output.write(json.dumps({'ordinal': position, 'topic': topic(position, text_bytes)}) + '\n')


def make_spec(source, output, case, interval, concurrency, dedup_bytes):
    spec = {
        'version': 1, 'dataset': {'name': f'benchmark-{case}'},
        'source': {'type': 'jsonl', 'path': str(source)},
        'providers': {'mock': {'type': 'mock', 'concurrency': concurrency,
                               'response': '{"answer": {{ record.topic | tojson }}}'}},
        'generate': {'provider': 'mock', 'prompt': '{{ record.topic }}',
                     'output_schema': {'type': 'object', 'required': ['answer'],
                                       'properties': {'answer': {'type': 'string'}}}},
        'output': {'format': 'parquet' if case == 'parquet' else 'jsonl',
                   'path': str(output), 'commit_every': interval},
    }
    if case.startswith(('exact-', 'minhash-')):
        spec['dedup_storage'] = case.split('-')[1]
        spec['dedup_max_bytes'] = dedup_bytes
        if case.startswith('exact-'):
            spec['dedup'] = {'method': 'exact', 'fields': ['generated.answer']}
        else:
            # Distinct single-token texts intentionally grow the whole index.
            # This measures capacity, not quality on a near-duplicate corpus.
            spec['dedup'] = {'method': 'minhash', 'field': 'generated.answer',
                             'num_perm': 128, 'bands': 16, 'threshold': 0.8}
    if case == 'judge':
        spec['providers']['judge'] = {'type': 'mock', 'concurrency': concurrency,
                                      'response': '{"score": 1}'}
        spec['judge'] = {'provider': 'judge', 'prompt': 'Score {{ generated.answer }}',
                         'min_score': 0.5}
    return spec


def check_report(measured, records):
    report = measured.get('report')
    if measured['timed_out']:
        raise RuntimeError('command exceeded --timeout')
    if measured['returncode'] != 0 or not report or report.get('status') != 'completed':
        errors = report.get('errors', []) if report else measured.get('stderr_tail', '')
        raise RuntimeError(f'command failed (exit {measured["returncode"]}): {errors}')
    state = report['sink_state']
    if (state['committed_source_position'], state['committed_accepted_records'],
            state['committed_rejected_records']) != (records, records, 0):
        raise RuntimeError(f'unexpected durable record counts: {state}')


def verify_jsonl(output, records, text_bytes, judge=False):
    count = 0
    with output.open() as rows:
        for count, line in enumerate(rows, 1):
            row = json.loads(line)
            expected = topic(count, text_bytes)
            if (row['_meta']['source_position'] != count or row['ordinal'] != count
                    or row['topic'] != expected or row['generated']['answer'] != expected):
                raise RuntimeError(f'JSONL row {count} is missing, duplicated, reordered, or changed')
            if judge and row['judge']['score'] != 1:
                raise RuntimeError(f'JSONL row {count} has an incorrect judge result')
    if count != records:
        raise RuntimeError(f'expected {records} JSONL rows, found {count}')
    return {'method': 'streamed_jsonl_rows', 'rows': count, 'order_and_content': True}


def verify_output(binary, output, records, text_bytes, case, timeout):
    if case != 'parquet':
        return verify_jsonl(output, records, text_bytes, case == 'judge')
    # The CLI decodes Parquet incrementally; no million-row Python list or a
    # third-party Python package is needed. This checks aggregates, not order.
    result = subprocess.run([str(binary), 'inspect', str(output), '--json'],
                            capture_output=True, text=True, timeout=timeout, check=True)
    summary = json.loads(result.stdout)
    ordinal = next(column for column in summary['columns'] if column['name'] == 'ordinal')
    if (summary['rows'] != records or ordinal['count'] != records or ordinal['null_count']
            or ordinal['min'] != 1 or ordinal['max'] != records
            or (ordinal['distinct'] is not None and ordinal['distinct'] != records)
            or not math.isclose(ordinal['mean'], (records + 1) / 2, rel_tol=1e-12)):
        raise RuntimeError('Parquet row counts or ordinal aggregates differ from the source')
    return {'method': 'streamed_parquet_inspect', 'rows': records,
            'order_and_content': False, 'ordinal': ordinal}


def recover(binary, config, spec, records, interval, case, args):
    output = Path(spec['output']['path'])
    manifest = Path(str(output) + '.manifest.json')
    target = args.recovery_after or interval
    if target >= records:
        raise RuntimeError('--recovery-after/commit interval must be below --records')
    started = time.perf_counter()
    observed = None
    with tempfile.TemporaryFile() as stderr:
        child = subprocess.Popen([str(binary), 'run', str(config)],
                                 stdout=subprocess.DEVNULL, stderr=stderr)
        try:
            deadline = time.monotonic() + args.recovery_timeout
            while time.monotonic() < deadline:
                if child.poll() is not None:
                    raise RuntimeError('run exited before interruption; increase --records or '
                                       f'reduce --recovery-after; stderr: {tail(stderr)}')
                try:
                    with manifest.open() as source:
                        state = json.load(source)
                    position = state['sink_state']['committed_source_position']
                    if target <= position < records and state['status'] == 'running':
                        observed = position
                        child.send_signal(signal.SIGKILL)
                        child.wait(timeout=args.recovery_timeout)
                        break
                except (FileNotFoundError, json.JSONDecodeError):
                    pass
                time.sleep(0.001)
            else:
                raise TimeoutError('no interruptible checkpoint before --recovery-timeout')
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=args.recovery_timeout)
    interrupted_after = time.perf_counter() - started
    if child.returncode != -signal.SIGKILL:
        raise RuntimeError(f'process exited with {child.returncode}, not SIGKILL')
    with manifest.open() as source:
        checkpoint = json.load(source)
    if checkpoint['status'] != 'running':
        raise RuntimeError('run finalized before SIGKILL; increase --records')
    resume = measure(binary, 'resume', config, args.timeout)
    check_report(resume, records)
    verified = verify_output(binary, output, records, args.text_bytes,
                             case, args.timeout)
    return {
        'signal': 'SIGKILL', 'interrupted_after_seconds': interrupted_after,
        'observed_before_kill': observed, 'checkpoint': checkpoint['sink_state'],
        'resume_from': checkpoint['sink_state']['committed_source_position'],
        'resume_seconds': resume['seconds'], 'resume_peak_rss_bytes': resume['peak_rss_bytes'],
        'verification': verified,
    }


def summarize(observations):
    groups = {}
    for observation in observations:
        key = tuple(observation[field] for field in ('case', 'records', 'commit_every', 'concurrency'))
        groups.setdefault(key, []).append(observation)
    summaries = []
    for key, values in groups.items():
        successful = [value for value in values if value['status'] == 'completed']
        summary = dict(zip(('case', 'records', 'commit_every', 'concurrency'), key))
        summary.update({'samples': len(values), 'successful': len(successful),
                        'failed': len(values) - len(successful)})
        for field in ('run_seconds', 'records_per_second', 'peak_rss_bytes'):
            if successful:
                summary[field] = distribution([value[field] for value in successful])
        recovered = [value['recovery'] for value in successful if value.get('recovery')]
        if recovered:
            for field in ('resume_seconds', 'resume_peak_rss_bytes'):
                summary[field] = distribution([value[field] for value in recovered])
        summaries.append(summary)
    return summaries


def self_test():
    import unittest

    class BenchmarkTests(unittest.TestCase):
        def test_parsing_and_percentiles(self):
            self.assertEqual(positive_list(['100,1000', '100'], 'records'), [100, 1000])
            for value in ('0', '-1', 'nan'):
                with self.assertRaises(ValueError):
                    positive_list([value], 'records')
            self.assertAlmostEqual(percentile([1, 2, 3, 4], .95), 3.85)
            self.assertEqual(distribution([3])['p95'], 3)

        def test_streamed_verification_catches_duplicates_and_gaps(self):
            with tempfile.TemporaryDirectory() as temp:
                path = Path(temp) / 'rows.jsonl'
                write_source(path, 4, 64)
                with path.open() as source:
                    self.assertEqual(sum(1 for _ in source), 4)
                def write(positions):
                    with path.open('w') as output:
                        for position in positions:
                            text = topic(position, 64)
                            row = {'ordinal': position, 'topic': text, 'generated': {'answer': text},
                                   '_meta': {'source_position': position}}
                            output.write(json.dumps(row) + '\n')
                write(range(1, 5))
                self.assertTrue(verify_jsonl(path, 4, 64)['order_and_content'])
                for positions in ([1, 2, 2, 4], [1, 2, 4], [1, 2, 3]):
                    write(positions)
                    with self.assertRaises(RuntimeError):
                        verify_jsonl(path, 4, 64)

        def test_measurement_timeout_and_report_capture(self):
            measured = measure_child([sys.executable, "-c", 'print(\'{"status":"completed"}\')'], 5)
            self.assertEqual(measured['report']['status'], 'completed')
            self.assertFalse(measured['timed_out'])
            timed_out = measure_child([sys.executable, '-c', 'import time; time.sleep(2)'], .05)
            self.assertTrue(timed_out['timed_out'])
            self.assertIsNone(timed_out['returncode'])

        def test_matrix_cases_and_failed_summary(self):
            for case in CASES:
                spec = make_spec(Path('source'), Path('output'), case, 128, 8, 1024)
                self.assertEqual(spec['providers']['mock']['concurrency'], 8)
                if case.endswith('-disk'):
                    self.assertEqual(spec['dedup_storage'], 'disk')
            common = {'case': 'jsonl', 'records': 10, 'commit_every': 1, 'concurrency': 1}
            grouped = summarize([dict(common, status='completed', run_seconds=2,
                                      records_per_second=5, peak_rss_bytes=100),
                                 dict(common, status='failed')])[0]
            self.assertEqual((grouped['successful'], grouped['failed']), (1, 1))
            self.assertEqual(grouped['run_seconds']['median'], 2)

    result = unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromTestCase(BenchmarkTests))
    return 0 if result.wasSuccessful() else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', default='target/release/synthflow')
    parser.add_argument('--records', nargs='+', default=['1000'], help='record counts, space/comma separated')
    parser.add_argument('--cases', nargs='+', default=['jsonl'], help=f'case names or all: {", ".join(CASES)}')
    parser.add_argument('--commit-intervals', nargs='+', default=['1', '128'])
    parser.add_argument('--concurrency', nargs='+', default=['1'], help='provider concurrency values, 1..1024')
    parser.add_argument('--repeat', type=int, default=1)
    parser.add_argument('--dedup-max-mib', type=int, default=64, help='memory index accounting budget; disk ignores it')
    parser.add_argument('--text-bytes', type=int, default=64, help='unique generated text length, at least 20')
    parser.add_argument('--timeout', type=float, default=3600, help='seconds allowed for each run/resume/inspect')
    parser.add_argument('--recovery', action='store_true', help='also SIGKILL a separate run and verify resume')
    parser.add_argument('--recovery-timeout', type=float, default=60, help='seconds to wait for a checkpoint')
    parser.add_argument('--recovery-after', type=int, help='minimum committed position before SIGKILL; default one interval')
    parser.add_argument('--allow-failures', action='store_true', help='report failed capacity cases without a nonzero exit')
    parser.add_argument('--output', type=Path, help='also save JSON, updated after each observation')
    parser.add_argument('--work-dir', type=Path, help='parent directory for temporary data, to choose the tested disk')
    parser.add_argument('--self-test', action='store_true')
    parser.add_argument('--measure', nargs=3, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    if any(not math.isfinite(value) or value <= 0 for value in (args.timeout, args.recovery_timeout)):
        parser.error('timeouts must be positive')
    if args.measure:
        print(json.dumps(measure_child(args.measure, args.timeout)))
        return 0
    if sys.platform not in ('darwin', 'linux'):
        parser.error('peak RSS and SIGKILL measurements require macOS or Linux')
    try:
        counts = positive_list(args.records, '--records')
        intervals = positive_list(args.commit_intervals, '--commit-intervals')
        concurrencies = positive_list(args.concurrency, '--concurrency', 1024)
    except ValueError as error:
        parser.error(str(error))
    cases = [case for value in args.cases for case in value.split(',')]
    if cases == ['all']:
        cases = list(CASES)
    if any(case not in CASES for case in cases):
        parser.error(f'--cases must be names from {CASES}, or all')
    cases = list(dict.fromkeys(cases))
    if args.repeat <= 0 or args.dedup_max_mib <= 0 or args.text_bytes < 20:
        parser.error('--repeat and --dedup-max-mib must be positive; --text-bytes must be at least 20')
    if args.recovery_after is not None and args.recovery_after <= 0:
        parser.error('--recovery-after must be positive')
    binary = Path(args.binary).resolve()
    if not binary.is_file():
        parser.error(f'binary not found: {binary}; run cargo build --release --workspace --locked')
    if args.work_dir and not args.work_dir.is_dir():
        parser.error('--work-dir must be an existing directory')
    observations = []
    result = {'schema_version': 2, 'started_at_utc': datetime.now(timezone.utc).isoformat(),
              'environment': fingerprint(binary), 'platform': sys.platform, 'binary': str(binary),
              'settings': {'records': counts, 'cases': cases, 'commit_intervals': intervals,
                           'concurrency': concurrencies, 'repeat': args.repeat,
                           'dedup_max_bytes': args.dedup_max_mib * 1024 * 1024,
                           'text_bytes': args.text_bytes, 'timeout_seconds': args.timeout,
                           'recovery': args.recovery, 'recovery_timeout_seconds': args.recovery_timeout,
                           'recovery_after': args.recovery_after},
              'measurements': observations}
    def save():
        result['summary'] = summarize(observations)
        if args.output:
            temporary = args.output.with_name(args.output.name + '.tmp')
            temporary.write_text(json.dumps(result, indent=2) + '\n')
            temporary.replace(args.output)
    with tempfile.TemporaryDirectory(prefix='synthflow-bench-', dir=args.work_dir) as temp:
        root = Path(temp)
        result['environment']['temporary_parent'] = str(root.parent)
        for records in counts:
            source = root / f'input-{records}.jsonl'
            write_source(source, records, args.text_bytes)
            for case, interval, concurrency, repeat in itertools.product(
                    cases, intervals, concurrencies, range(1, args.repeat + 1)):
                observation = {'case': case, 'records': records, 'commit_every': interval,
                               'concurrency': concurrency, 'repeat': repeat, 'status': 'failed'}
                print(f'{case} rows={records} commit={interval} concurrency={concurrency} repeat={repeat}',
                      file=sys.stderr, flush=True)
                with tempfile.TemporaryDirectory(prefix='case-', dir=root) as case_temp:
                    trial = Path(case_temp)
                    output = trial / ('output.parquet' if case == 'parquet' else 'output.jsonl')
                    config = trial / 'pipeline.json'
                    spec = make_spec(source, output, case, interval, concurrency,
                                     args.dedup_max_mib * 1024 * 1024)
                    config.write_text(json.dumps(spec))
                    try:
                        full = measure(binary, 'run', config, args.timeout)
                        observation.update({'run_seconds': full['seconds'],
                                            'peak_rss_bytes': full['peak_rss_bytes'],
                                            'returncode': full['returncode'],
                                            'timed_out': full['timed_out']})
                        if full['report']:
                            observation['sink_state'] = full['report'].get('sink_state')
                            observation['report_errors'] = full['report'].get('errors', [])
                        check_report(full, records)
                        observation['verification'] = verify_output(binary, output, records,
                                                                     args.text_bytes, case, args.timeout)
                        observation['records_per_second'] = records / full['seconds']
                        if args.recovery:
                            spec['output']['path'] = str(trial / ('recovery.parquet' if case == 'parquet' else 'recovery.jsonl'))
                            config.write_text(json.dumps(spec))
                            observation['recovery'] = recover(binary, config, spec, records, interval, case, args)
                        observation['status'] = 'completed'
                    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
                        observation['error'] = str(error)
                        print(f'  failed: {error}', file=sys.stderr, flush=True)
                observations.append(observation)
                save()
            source.unlink()
    result['finished_at_utc'] = datetime.now(timezone.utc).isoformat()
    result['failed_measurements'] = sum(item['status'] != 'completed' for item in observations)
    save()
    print(json.dumps(result, indent=2))
    return 0 if not result['failed_measurements'] or args.allow_failures else 1


if __name__ == '__main__':
    raise SystemExit(main())
