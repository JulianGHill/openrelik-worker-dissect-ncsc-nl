"""Unit tests for the Dissect Celery task."""

from __future__ import annotations

from pathlib import Path
import zipfile

import pytest

from src import tasks


class DummyTask:
    """Minimal Celery-like task used for unit testing."""

    def __init__(self):
        self.events = []

    def send_event(self, event, data=None):  # pragma: no cover - trivial forwarding
        self.events.append((event, data))


@pytest.fixture(autouse=True)
def reset_defaults(monkeypatch):
    """Ensure environment derived defaults are stable in tests."""

    monkeypatch.setattr(tasks, "DEFAULT_QUERY", "target-info")


@pytest.fixture
def tmp_output(tmp_path):
    return tmp_path


def make_output_file(tmp_path, name="result.txt"):
    class _OutputFile:
        def __init__(self, file_path):
            self.path = str(file_path)
            self.display_name = file_path.name

        def to_dict(self):
            return {"path": self.path, "display_name": self.display_name}

    return _OutputFile(tmp_path / name)


def test_run_query_success(monkeypatch, tmp_output):
    input_file = {"path": "/cases/disk.E01", "display_name": "disk.E01"}

    monkeypatch.setattr(tasks, "get_input_files", lambda pipe_result, files, filter=None: [input_file])

    fake_output = make_output_file(tmp_output, "disk-target-info.txt")
    monkeypatch.setattr(tasks, "create_output_file", lambda *args, **kwargs: fake_output)

    captured_invocations = []

    def fake_invoke(script, args, decode_stdout=True):
        assert decode_stdout is True
        captured_invocations.append((script, args))
        return 0, "analysis", ""

    monkeypatch.setattr(tasks, "_invoke_query", fake_invoke)

    result_payload = {}

    def fake_create_task_result(**kwargs):
        result_payload.update(kwargs)
        return "encoded-result"

    monkeypatch.setattr(tasks, "create_task_result", fake_create_task_result)

    dummy_task = DummyTask()
    return_value = tasks._run_query(
        dummy_task,
        pipe_result=None,
        input_files=None,
        output_path=str(tmp_output),
        workflow_id="wf-123",
        task_config={
            "query": "target-info",
            "arguments": "--flag value",
        },
    )

    assert return_value == "encoded-result"
    assert captured_invocations == [("target-info", ["--flag", "value", "/cases/disk.E01"])]

    with open(fake_output.path, "r", encoding="utf-8") as handle:
        assert handle.read() == "analysis"

    assert result_payload["workflow_id"] == "wf-123"
    assert result_payload["command"] == "target-info --flag value"
    assert result_payload["meta"]["query"] == "target-info"
    assert result_payload["meta"]["arguments"] == ["--flag", "value"]
    assert result_payload["output_files"] == [fake_output.to_dict()]


def test_run_query_extracts_zip_inputs(monkeypatch, tmp_output, tmp_path):
    archive_root = tmp_path / "TestCollect"
    archive_root.mkdir()
    evidence_file = archive_root / "disk.E01"
    evidence_file.write_text("dummy-data", encoding="utf-8")

    zip_path = tmp_path / "TestCollect.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.write(evidence_file, arcname="TestCollect/disk.E01")

    input_file = {"path": str(zip_path), "display_name": "TestCollect.zip"}
    monkeypatch.setattr(tasks, "get_input_files", lambda pipe_result, files, filter=None: [input_file])

    fake_output = make_output_file(tmp_output, "TestCollect-target-info.txt")
    monkeypatch.setattr(tasks, "create_output_file", lambda *args, **kwargs: fake_output)

    captured_invocations = []

    def fake_invoke(script, args, decode_stdout=True):
        assert decode_stdout is True
        extracted_path = Path(args[-1])
        assert extracted_path.name == "TestCollect"
        assert extracted_path.joinpath("disk.E01").exists()
        captured_invocations.append((script, args))
        return 0, "zip-analysis", ""

    monkeypatch.setattr(tasks, "_invoke_query", fake_invoke)
    monkeypatch.setattr(tasks, "create_task_result", lambda **kwargs: "ok")

    return_value = tasks._run_query(
        DummyTask(),
        pipe_result=None,
        input_files=None,
        output_path=str(tmp_output),
        workflow_id="wf-zip",
        task_config={"query": "target-info"},
    )

    assert return_value == "ok"
    assert len(captured_invocations) == 1
    assert captured_invocations[0][1][-1].endswith("TestCollect")


def test_run_query_target_query_runs_rdump(monkeypatch, tmp_output):
    input_file = {"path": "/cases/disk.E01", "display_name": "disk.E01"}

    monkeypatch.setattr(tasks, "get_input_files", lambda pipe_result, files, filter=None: [input_file])

    fake_output = make_output_file(tmp_output, "disk-target-query.csv")

    def fake_create_output_file(*args, **kwargs):
        assert kwargs["extension"] == "csv"
        assert kwargs["data_type"] == tasks.TARGET_QUERY_RESULT_TYPE
        return fake_output

    monkeypatch.setattr(tasks, "create_output_file", fake_create_output_file)

    captured_queries = []

    def fake_invoke_query(script, args, decode_stdout=True):
        captured_queries.append({"script": script, "args": args, "decode": decode_stdout})
        return 0, b"record-data", ""

    monkeypatch.setattr(tasks, "_invoke_query", fake_invoke_query)

    rdump_calls = []

    def fake_invoke_console(script, args, stdin_data=None, decode_stdout=True):
        assert script == "rdump"
        rdump_calls.append({"args": args, "stdin": stdin_data, "decode": decode_stdout})
        return 0, "csv-output", ""

    monkeypatch.setattr(tasks, "invoke_console_script", fake_invoke_console)

    result_payload = {}

    def fake_create_task_result(**kwargs):
        result_payload["result"] = kwargs
        return "ok"

    monkeypatch.setattr(tasks, "create_task_result", fake_create_task_result)

    return_value = tasks._run_query(
        DummyTask(),
        pipe_result=None,
        input_files=None,
        output_path=str(tmp_output),
        workflow_id="wf-target-query",
        task_config={"query": "target-query"},
    )

    assert return_value == "ok"
    assert captured_queries[0]["decode"] is False
    assert rdump_calls[0]["stdin"] == b"record-data"

    with open(fake_output.path, "r", encoding="utf-8") as handle:
        assert handle.read() == "csv-output"

    meta_entry = result_payload["result"]["meta"]["results"][0]
    assert meta_entry["rdump_command"].startswith("rdump ")
    assert meta_entry["stderr"] == ""
def test_run_query_failure(monkeypatch, tmp_output):
    input_file = {"path": "/cases/disk.E01", "display_name": "disk.E01"}

    monkeypatch.setattr(tasks, "get_input_files", lambda pipe_result, files, filter=None: [input_file])

    fake_output = make_output_file(tmp_output, "disk-target-info.txt")
    monkeypatch.setattr(tasks, "create_output_file", lambda *args, **kwargs: fake_output)

    monkeypatch.setattr(tasks, "_invoke_query", lambda script, args, decode_stdout=True: (1, "", "boom"))

    with pytest.raises(RuntimeError) as exc:
        tasks._run_query(
            DummyTask(),
            pipe_result=None,
            input_files=None,
            output_path=str(tmp_output),
            workflow_id="wf-123",
            task_config={"query": "target-info"},
        )

    assert "boom" in str(exc.value)

    # File should not contain partial output on failure
    assert not tmp_output.joinpath(fake_output.display_name).exists()


def test_run_query_requires_query(monkeypatch, tmp_output):
    monkeypatch.setattr(tasks, "get_input_files", lambda pipe_result, files, filter=None: [])

    with pytest.raises(RuntimeError) as exc:
        tasks._run_query(
            DummyTask(),
            pipe_result=None,
            input_files=None,
            output_path=str(tmp_output),
            workflow_id="wf-000",
            task_config={},
        )

    assert "No Dissect console script provided" in str(exc.value)
