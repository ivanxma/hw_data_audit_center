#!/usr/bin/env python3
"""Run the private OL9 image builder."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from modules.image_build_worker import main  # noqa: E402

if __name__ == "__main__":
    main()
