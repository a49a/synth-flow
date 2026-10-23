#!/usr/bin/env python3
"""Bundle an existing release executable and its license; never publishes it."""
import hashlib
from pathlib import Path
import platform
import subprocess
import tarfile

ROOT = Path(__file__).resolve().parents[1]


def main():
    binary = ROOT / "target/release/synthflow"
    version = subprocess.check_output([str(binary), "--version"], text=True).strip().split()[-1]
    stem = f"synthflow-{version}-{platform.system().lower()}-{platform.machine().lower()}"
    output = ROOT / "dist"
    output.mkdir(exist_ok=True)
    archive = output / f"{stem}.tar.gz"
    with tarfile.open(archive, "w:gz") as bundle:
        for path, name in [(binary, "synthflow"), (ROOT / "LICENSE", "LICENSE"),
                           (ROOT / "README.md", "README.md")]:
            bundle.add(path, arcname=f"{stem}/{name}")
    digest = hashlib.sha256()
    with archive.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    checksum = digest.hexdigest()
    archive.with_suffix(archive.suffix + ".sha256").write_text(
        f"{checksum}  {archive.name}\n", encoding="utf-8")
    print(archive)


if __name__ == "__main__":
    main()
