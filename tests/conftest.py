"""Shared pytest fixtures and path setup for the Virtual Waiting Room tests.

Ensures the project root is importable so ``import waiting_room`` works when
pytest is invoked without an editable install.
"""

import os
import sys

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
