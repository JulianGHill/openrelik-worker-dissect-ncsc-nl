"""Tests for the target-query bundle task."""

from __future__ import annotations

import pytest

from src import target_query_bundle as bundle


class DummyTask:
    """Minimal Celery-like task stub."""

    def __init__(self):
        self.events = []

    def send_event(self, event, data=None):  # pragma: no cover - trivial forwarding
        self.events.append((event, data))


def _make_output_file(tmp_path, name):
    class _OutputFile:
        def __init__(self, file_path):
            self.path = str(file_path)
            self.display_name = file_path.name

        def to_dict(self):
            return {"path": self.path, "display_name": self.display_name}

    return _OutputFile(tmp_path / name)


@pytest.fixture(autouse=True)
def set_bundle(monkeypatch):
    presets = [
        {"name": "mft_timeline", "arguments": ["-f", "mft_timeline"], "output_suffix": "mft"},
        {"name": "evtx", "arguments": ["-f", "evtx"], "output_suffix": "evtx"},
    ]
    monkeypatch.setattr(bundle, "TARGET_QUERY_BUNDLE", presets)


def test_bundle_success(monkeypatch, tmp_path):
    input_file = {"path": "/cases/disk.E01", "display_name": "disk.E01"}
    monkeypatch.setattr(bundle, "get_input_files", lambda pipe_result, files: [input_file])

    outputs = {}

    def fake_create_output_file(output_path, display_name, extension, data_type):
        file_obj = _make_output_file(tmp_path, f"{display_name}.{extension}")
        outputs[display_name] = file_obj
        return file_obj

    monkeypatch.setattr(bundle, "create_output_file", fake_create_output_file)

    calls = []

    def fake_invoke(script, args, stdin_data=None, decode_stdout=True):
        calls.append((script, list(args), stdin_data, decode_stdout))
        if script == "target-query":
            if decode_stdout:
                return 0, f"records-{args[-1]}", ""
            return 0, f"records-{args[-1]}".encode(), b""
        if script == "rdump":
            stdin_repr = stdin_data.decode() if isinstance(stdin_data, (bytes, bytearray)) else str(stdin_data)
            return 0, f"csv-from-{stdin_repr}", ""
        raise AssertionError("unexpected script")

    monkeypatch.setattr(bundle, "invoke_console_script", fake_invoke)

    captured_result = {}

    def fake_create_task_result(**kwargs):
        captured_result.update(kwargs)
        return "bundle-result"

    monkeypatch.setattr(bundle, "create_task_result", fake_create_task_result)

    result = bundle._run_bundle(
        DummyTask(),
        pipe_result=None,
        input_files=None,
        output_path=str(tmp_path),
        workflow_id="wf-321",
    )

    assert result == "bundle-result"
    assert len(calls) == 4  # 2 presets * (target-query + rdump)

    written = sorted(tmp_path.iterdir())
    assert any(f.name.endswith("mft.csv") for f in written)
    assert any(f.name.endswith("evtx.csv") for f in written)

    assert captured_result["workflow_id"] == "wf-321"
    assert captured_result["output_files"] == [outputs["disk-mft"].to_dict(), outputs["disk-evtx"].to_dict()]
    assert captured_result["meta"]["presets"] == ["mft_timeline", "evtx"]
    assert {meta["preset"] for meta in captured_result["meta"]["results"]} == {"mft_timeline", "evtx"}


def test_bundle_target_query_failure(monkeypatch, tmp_path):
    input_file = {"path": "/cases/disk.E01", "display_name": "disk.E01"}
    monkeypatch.setattr(bundle, "get_input_files", lambda pipe_result, files: [input_file])
    monkeypatch.setattr(bundle, "create_output_file", lambda *args, **kwargs: _make_output_file(tmp_path, "dummy.csv"))

    def failing_invoke(script, args, stdin_data=None, decode_stdout=True):
        if script == "target-query":
            return 1, b"", b"boom"
        return 0, "", ""

    monkeypatch.setattr(bundle, "invoke_console_script", failing_invoke)

    with pytest.raises(RuntimeError) as exc:
        bundle._run_bundle(
            DummyTask(),
            pipe_result=None,
            input_files=None,
            output_path=str(tmp_path),
            workflow_id="wf-321",
        )

    assert "boom" in str(exc.value)
