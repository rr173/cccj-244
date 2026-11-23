#!/usr/bin/env python3
"""Top-level entry point so the CLI runs under ``python3 -I narc_tool.py``."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from narc.cli import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
