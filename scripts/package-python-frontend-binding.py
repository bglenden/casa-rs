#!/usr/bin/env python3
"""Adapt generated UniFFI Python bindings to the packaged PyO3 extension.

The casars wheel deliberately contains one native module, ``casars._core``.
That module links the generated frontend-service FFI symbols alongside the
NumPy-oriented PyO3 object API.  UniFFI's generated loader normally looks for
a second adjacent ``libcasars_frontend_services`` library; replace only that
loader while leaving the generated contract and checksum code untouched.
"""

from __future__ import annotations

import argparse
from pathlib import Path


GENERATED_LOADER = '''def _uniffi_load_indirect():
    """
    This is how we find and load the dynamic library provided by the component.
    For now we just look it up by name.
    """
    if sys.platform == "darwin":
        libname = "lib{}.dylib"
    elif sys.platform.startswith("win"):
        # As of python3.8, ctypes does not seem to search $PATH when loading DLLs.
        # We could use `os.add_dll_directory` to configure the search path, but
        # it doesn't feel right to mess with application-wide settings. Let's
        # assume that the `.dll` is next to the `.py` file and load by full path.
        libname = os.path.join(
            os.path.dirname(__file__),
            "{}.dll",
        )
    else:
        # Anything else must be an ELF platform - Linux, *BSD, Solaris/illumos
        libname = "lib{}.so"

    libname = libname.format("casars_frontend_services")
    path = os.path.join(os.path.dirname(__file__), libname)
    lib = ctypes.cdll.LoadLibrary(path)
    return lib
'''

PACKAGED_LOADER = '''def _uniffi_load_indirect():
    """Load frontend-service FFI symbols from the packaged native extension."""
    import importlib.util

    spec = importlib.util.find_spec("casars._core")
    if spec is None or spec.origin is None:
        raise ImportError("casars._core is required by the generated frontend binding")
    return ctypes.cdll.LoadLibrary(spec.origin)
'''


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()

    generated = args.source.read_text(encoding="utf-8")
    if generated.count(GENERATED_LOADER) != 1:
        raise SystemExit("generated UniFFI loader shape changed; update the packaging adapter")
    packaged = generated.replace(GENERATED_LOADER, PACKAGED_LOADER)
    args.destination.parent.mkdir(parents=True, exist_ok=True)
    args.destination.write_text(packaged, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
