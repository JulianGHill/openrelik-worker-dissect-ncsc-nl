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


@pytest.mark.parametrize("writer_source", ["config", "env"])
def test_run_query_target_query_runs_writer(monkeypatch, tmp_output, writer_source):
    input_file = {"path": "/cases/disk.E01", "display_name": "disk.E01"}
    monkeypatch.setattr(tasks, "get_input_files", lambda pipe_result, files, filter=None: [input_file])

    fake_output = make_output_file(tmp_output, "disk-target-query.csv")
    monkeypatch.setattr(tasks, "create_output_file", lambda *args, **kwargs: fake_output)

    monkeypatch.setattr(tasks, "_invoke_query", lambda script, args, decode_stdout=True: (0, b"record-data", ""))

    rdump_calls = []

    def fake_rdump(script, args, stdin_data=None, decode_stdout=True):
        assert script == "rdump"
        rdump_calls.append({"args": args, "stdin": stdin_data})
        if args and args[0] == "-w":
            return 0, "", ""
        return 0, "csv-output", ""

    monkeypatch.setattr(tasks, "invoke_console_script", fake_rdump)
    monkeypatch.setattr(tasks, "create_task_result", lambda **kwargs: "ok")

    writer_uri = "elastic+http://elastic:9200?index=target-query"
    task_config = {"query": "target-query"}
    if writer_source == "config":
        task_config["elastic_writer"] = writer_uri
    else:
        monkeypatch.setenv("DISSECT_ELASTIC_WRITER_URI", writer_uri)
        task_config["enable_record_writer"] = True

    result = tasks._run_query(
        DummyTask(),
        pipe_result=None,
        input_files=None,
        output_path=str(tmp_output),
        workflow_id="wf-target-query",
        task_config=task_config,
    )

    assert result == "ok"
    assert len(rdump_calls) == 2
    assert rdump_calls[0]["args"] == tasks.TARGET_QUERY_RDUMP_ARGS
    assert rdump_calls[0]["stdin"] == b"record-data"
    assert rdump_calls[1]["args"] == ["-w", writer_uri]
    assert rdump_calls[1]["stdin"] == b"record-data"

    with open(fake_output.path, "r", encoding="utf-8") as handle:
        assert handle.read() == "csv-output"


@pytest.mark.parametrize("writer_source", ["config", "env"])
@pytest.mark.parametrize("query_name", ["target-info", "target-reg"])
def test_run_query_record_capture_streams_writer(monkeypatch, tmp_output, writer_source, query_name):
    input_file = {"path": "/cases/disk.E01", "display_name": "disk.E01"}

    monkeypatch.setattr(tasks, "get_input_files", lambda pipe_result, files, filter=None: [input_file])

    fake_output = make_output_file(tmp_output, f"disk-{query_name}.txt")
    monkeypatch.setattr(tasks, "create_output_file", lambda *args, **kwargs: fake_output)

    invoke_calls = []

    def fake_invoke_query(script, args, decode_stdout=True):
        invoke_calls.append({"script": script, "args": args, "decode": decode_stdout})
        if decode_stdout:
            return 0, "text-output", ""
        return 0, b"record-output", ""

    monkeypatch.setattr(tasks, "_invoke_query", fake_invoke_query)

    writer_calls = []

    def fake_rdump(script, args, stdin_data=None, decode_stdout=True):
        assert script == "rdump"
        writer_calls.append({"args": args, "stdin": stdin_data})
        return 0, "", ""

    monkeypatch.setattr(tasks, "invoke_console_script", fake_rdump)
    monkeypatch.setattr(tasks, "create_task_result", lambda **kwargs: "ok")

    writer_uri = f"elastic+http://elastic:9200?index={query_name}"
    task_config = {"query": query_name}
    if writer_source == "config":
        task_config["elastic_writer"] = writer_uri
    else:
        monkeypatch.setenv("DISSECT_ELASTIC_WRITER_URI", writer_uri)
        task_config["enable_record_writer"] = True

    result = tasks._run_query(
        DummyTask(),
        pipe_result=None,
        input_files=None,
        output_path=str(tmp_output),
        workflow_id="wf-writer",
        task_config=task_config,
    )

    assert result == "ok"
    assert len(invoke_calls) == 2
    assert invoke_calls[0]["decode"] is True
    assert invoke_calls[1]["decode"] is False
    assert "-r" in invoke_calls[1]["args"]
    assert writer_calls == [
        {"args": ["-w", writer_uri], "stdin": b"record-output"}
    ]

    with open(fake_output.path, "r", encoding="utf-8") as handle:
        assert handle.read() == "text-output"


def test_env_writer_disabled_without_toggle(monkeypatch, tmp_output):
    input_file = {"path": "/cases/disk.E01", "display_name": "disk.E01"}
    monkeypatch.setattr(tasks, "get_input_files", lambda pipe_result, files, filter=None: [input_file])

    fake_output = make_output_file(tmp_output, "disk-target-info.txt")
    monkeypatch.setattr(tasks, "create_output_file", lambda *args, **kwargs: fake_output)

    monkeypatch.setattr(tasks, "_invoke_query", lambda script, args, decode_stdout=True: (0, "text", ""))
    monkeypatch.setattr(tasks, "create_task_result", lambda **kwargs: "ok")
    rdump_calls = []
    monkeypatch.setattr(tasks, "invoke_console_script", lambda *args, **kwargs: rdump_calls.append(args) or (0, "", ""))

    monkeypatch.setenv("DISSECT_ELASTIC_WRITER_URI", "elastic+http://elastic:9200?index=records")

    result = tasks._run_query(
        DummyTask(),
        pipe_result=None,
        input_files=None,
        output_path=str(tmp_output),
        workflow_id="wf-no-export",
        task_config={"query": "target-info"},
    )

    assert result == "ok"
    assert rdump_calls == []


def test_run_query_writer_rejected_for_unsupported_queries(monkeypatch, tmp_output):
    monkeypatch.setattr(tasks, "get_input_files", lambda pipe_result, files, filter=None: [{"path": "/cases/a", "display_name": "a"}])

    with pytest.raises(RuntimeError) as exc:
        tasks._run_query(
            DummyTask(),
            pipe_result=None,
            input_files=None,
            output_path=str(tmp_output),
            workflow_id="wf-bad",
            task_config={
                "query": "target-shell",
                "elastic_writer": "elastic+http://elastic:9200?index=bad",
            },
        )

    assert "currently supported" in str(exc.value)


def test_run_query_requires_writer_uri_when_enabled(monkeypatch, tmp_output):
    monkeypatch.setattr(tasks, "get_input_files", lambda pipe_result, files, filter=None: [{"path": "/cases/a", "display_name": "a"}])

    with pytest.raises(RuntimeError) as exc:
        tasks._run_query(
            DummyTask(),
            pipe_result=None,
            input_files=None,
            output_path=str(tmp_output),
            workflow_id="wf-missing-writer",
            task_config={
                "query": "target-info",
                "enable_record_writer": True,
            },
        )

    assert "no writer URI" in str(exc.value)


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
