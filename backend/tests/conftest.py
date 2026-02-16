from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import app.main as app_main


@pytest.fixture(autouse=True)
def reset_batch_manager() -> None:
    if app_main.batch_manager is not None:
        app_main.batch_manager.stop()
    app_main.batch_manager = None

    yield

    if app_main.batch_manager is not None:
        app_main.batch_manager.stop()
    app_main.batch_manager = None
