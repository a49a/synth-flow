#!/usr/bin/env python3
"""Execute the workbook's actual bash blocks, including their assertions."""
import os
from pathlib import Path
import re
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
text = (ROOT / 'docs/LABS.md').read_text()
blocks = re.findall(r'^```bash\n(.*?)^```', text, flags=re.M | re.S)
assert blocks, 'No executable workbook blocks found'
# The final fmt/clippy/workspace-test block is covered by separate CI steps.
assert 'cargo fmt --all -- --check' in blocks[-1]
with tempfile.TemporaryDirectory(prefix='synthflow-labs-') as temporary:
    for number, block in enumerate(blocks[:-1], 1):
        expected = block.count('-- --exact')
        print(f'Workbook block {number}/{len(blocks)-1}', flush=True)
        result = subprocess.run(['bash', '-euo', 'pipefail', '-c', block], cwd=ROOT,
                                env={**os.environ, 'TMPDIR': temporary},
                                text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        print(result.stdout, end='')
        if result.returncode:
            raise SystemExit(f'Workbook block {number} failed ({result.returncode})')
        if expected:
            # Cargo exits successfully even when a renamed test matches nothing.
            assert result.stdout.count('test result: ok. 1 passed;') == expected, (
                f'Workbook block {number}: expected {expected} single-test executions')
print('All executable lab blocks passed.')
