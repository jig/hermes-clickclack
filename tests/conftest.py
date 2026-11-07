"""Test setup: put a Hermes Agent checkout on sys.path and load adapter.py as a module.

The checkout defaults to ~/.hermes/hermes-agent; override with HERMES_AGENT_DIR.
Run with the Hermes venv's interpreter so its dependencies (aiohttp, ...) resolve.
"""

import importlib.util
import os
import sys
from pathlib import Path

import pytest

HERMES_AGENT_DIR = Path(os.environ.get("HERMES_AGENT_DIR", Path.home() / ".hermes" / "hermes-agent"))
REPO_DIR = Path(__file__).resolve().parent.parent

if str(HERMES_AGENT_DIR) not in sys.path:
    sys.path.insert(0, str(HERMES_AGENT_DIR))


@pytest.fixture(scope="session")
def adapter_mod():
    spec = importlib.util.spec_from_file_location("clickclack_adapter", REPO_DIR / "adapter.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module
