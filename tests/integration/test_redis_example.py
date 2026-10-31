from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.integration
def test_redis_shared_example_runs_as_a_standalone_script() -> None:
    redis_url = os.getenv("TEST_REDIS_URL")
    if not redis_url:
        pytest.skip("TEST_REDIS_URL is not set")

    environment = os.environ.copy()
    environment["REDIS_URL"] = redis_url
    result = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "examples" / "redis_shared.py")],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "redis shared-store example passed" in result.stdout
