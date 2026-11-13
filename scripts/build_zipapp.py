#!/usr/bin/env python3
"""Builds a clean, reproducible, self-contained zipapp (.pyz) of dataproc-gateway-diagnostics.

Strictly excludes __pycache__, *.pyc, and test/development files so the resulting
zipapp matches production release distribution requirements (~33 KB).
"""

from __future__ import annotations

import hashlib
import os
import shutil
import sys
import tempfile
import zipapp

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_PACKAGE = os.path.join(ROOT_DIR, "src", "dataproc_gateway_diagnostics")
OUTPUT_PYZ = os.path.join(ROOT_DIR, "dataproc_gateway_diagnostics.pyz")


def build_zipapp(output_path: str = OUTPUT_PYZ) -> str:
    if not os.path.isdir(SRC_PACKAGE):
        raise FileNotFoundError(f"Source package not found at: {SRC_PACKAGE}")

    with tempfile.TemporaryDirectory(prefix="zipapp_build_") as tmpdir:
        staging_pkg = os.path.join(tmpdir, "dataproc_gateway_diagnostics")

        def _ignore_pycache(path: str, names: list[str]) -> set[str]:
            ignored = set()
            for name in names:
                if (
                    name == "__pycache__"
                    or name.endswith(".pyc")
                    or name.endswith(".pyo")
                    or name == ".DS_Store"
                ):
                    ignored.add(name)
            return ignored

        shutil.copytree(SRC_PACKAGE, staging_pkg, ignore=_ignore_pycache)

        # Create top-level __main__.py inside zipapp root for direct execution
        main_py = os.path.join(tmpdir, "__main__.py")
        with open(main_py, "w", encoding="utf-8") as f:
            f.write(
                "import sys\n"
                "from dataproc_gateway_diagnostics.cli import main\n"
                "if __name__ == '__main__':\n"
                "    sys.exit(main())\n"
            )

        if os.path.exists(output_path):
            os.remove(output_path)

        zipapp.create_archive(
            tmpdir,
            target=output_path,
            interpreter="/usr/bin/env python3",
            compressed=True,
        )

    # Compute size and sha256
    size_bytes = os.path.getsize(output_path)
    sha256 = hashlib.sha256()
    with open(output_path, "rb") as f:
        while chunk := f.read(65536):
            sha256.update(chunk)
    digest = sha256.hexdigest()

    print(f"Built clean zipapp: {output_path}")
    print(f"Size: {size_bytes} bytes ({size_bytes / 1024:.1f} KB)")
    print(f"SHA-256: {digest}")
    return output_path


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else OUTPUT_PYZ
    build_zipapp(out)
