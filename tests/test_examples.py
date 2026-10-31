from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    "example",
    [
        "basic.py",
        "conditional_hydration.py",
        "document_pipeline.py",
        "local_persistence.py",
        "long_conversation.py",
        "message_keep.py",
        "multi_thread_isolation.py",
        "multiple_channels.py",
        "parallel_tool_workflow.py",
        "resilient_io.py",
        "state_validation.py",
    ],
)
def test_example_runs_as_a_standalone_script(example: str) -> None:
    result = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "examples" / example)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "example passed" in result.stdout
