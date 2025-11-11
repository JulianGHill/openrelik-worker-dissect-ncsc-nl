"""Tests for the dedicated target-info task."""

from __future__ import annotations

import pytest

from src import target_info


class DummyTask:
    def __init__(self):
        self.events = []

    def send_event(self, event, data=None):  # pragma: no cover - trivial forwarding
        self.events.append((event, data))


def test_target_info_runs_with_fixed_arguments(monkeypatch):
    captured = {}

    def fake_run(
        task,
        *,
        pipe_result,
        input_files,
        output_path,
        workflow_id,
        task_config,
    ):
        captured.update(
            {
                "task": task,
                "pipe_result": pipe_result,
                "input_files": input_files,
                "output_path": output_path,
                "workflow_id": workflow_id,
                "task_config": task_config,
            }
        )
        return "ok"

    monkeypatch.setattr(target_info, "_run_query", fake_run)

    # Celery decorates the task, so call the underlying function directly for testing.
    run_impl = target_info.run_target_info.run.__func__

    result = run_impl(
        DummyTask(),
        pipe_result="pipe",
        input_files=[{"path": "disk.E01"}],
        output_path="/tmp/out",
        workflow_id="wf-789",
    )

    assert result == "ok"
    assert captured["task_config"] == {"query": target_info.DEFAULT_QUERY, "arguments": ""}
    assert captured["output_path"] == "/tmp/out"
    assert captured["workflow_id"] == "wf-789"
    assert captured["input_files"] == [{"path": "disk.E01"}]
