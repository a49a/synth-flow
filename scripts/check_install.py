#!/usr/bin/env python3
"""Install the CLI in an isolated prefix and run an offline end-to-end smoke test."""
import json
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def main():
    with tempfile.TemporaryDirectory(prefix="synthflow-install-") as temporary:
        root = Path(temporary)
        subprocess.run([
            "cargo", "install", "--path", str(ROOT / "crates/synthflow-cli"),
            "--locked", "--debug", "--root", str(root / "install"),
        ], cwd=ROOT, env={**os.environ, "CARGO_TARGET_DIR": str(ROOT / "target")}, check=True)
        binary = root / "install/bin/synthflow"
        subprocess.run([str(binary), "--version"], check=True)
        spec = {
            "version": 1, "dataset": {"name": "installation-smoke"},
            "source": {"type": "inline", "records": [{"topic": "installation"}]},
            "providers": {"mock": {"type": "mock", "response": '{"answer":"ok"}'}},
            "generate": {"provider": "mock", "prompt": "p", "output_schema": {"type": "object"}},
            "output": {"format": "jsonl", "path": str(root / "data.jsonl")},
        }
        config = root / "pipeline.json"
        config.write_text(json.dumps(spec), encoding="utf-8")
        subprocess.run([str(binary), "validate", str(config)], check=True)
        report = json.loads(subprocess.check_output([str(binary), "run", str(config)], text=True))
        assert report["status"] == "completed"
        assert report["sink_state"]["committed_accepted_records"] == 1
        summary = json.loads(subprocess.check_output([
            str(binary), "inspect", str(root / "data.jsonl"), "--json"], text=True))
        assert summary["rows"] == 1
    print("Isolated installation and offline pipeline passed.")


if __name__ == "__main__":
    main()
