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
        {
            "name": "mft_timeline",
            "arguments": ["-f", "mft.records"],
            "output_suffix": "mft",
            "categories": [bundle.CATEGORY_APPLICATION_EXECUTION],
        },
        {
            "name": "evtx",
            "arguments": ["-f", "evtx"],
            "output_suffix": "evtx",
            "categories": [bundle.CATEGORY_APPLICATION_EXECUTION],
        },
    ]
    monkeypatch.setattr(bundle, "TARGET_QUERY_BUNDLE", presets)
    bundle._plugin_available.cache_clear()


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
        task_config={},
    )

    assert result == "bundle-result"
    assert len(calls) == 4  # 2 presets * (target-query + rdump)

    written = sorted(tmp_path.iterdir())
    assert any(f.name.endswith("mft.csv") for f in written)
    assert any(f.name.endswith("evtx.csv") for f in written)

    assert captured_result["workflow_id"] == "wf-321"
    assert captured_result["output_files"] == [outputs["disk-mft"].to_dict(), outputs["disk-evtx"].to_dict()]
    assert captured_result["meta"]["presets"] == ["mft_timeline", "evtx"]
    assert captured_result["meta"]["skipped_presets"] == []
    assert {meta["preset"] for meta in captured_result["meta"]["results"]} == {"mft_timeline", "evtx"}
    assert {meta["plugin"] for meta in captured_result["meta"]["results"]} == {"mft.records", "evtx"}
    assert captured_result["meta"]["selection"] == [bundle.CATEGORY_EVERYTHING]
    assert (
        captured_result["meta"]["selection_label"]
        == [bundle.SCOPE_VALUE_TO_LABEL[bundle.CATEGORY_EVERYTHING]]
    )


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
            task_config={},
        )

    assert "boom" in str(exc.value)


def test_bundle_skips_missing_plugins(monkeypatch, tmp_path):
    input_file = {"path": "/cases/disk.E01", "display_name": "disk.E01"}
    monkeypatch.setattr(bundle, "get_input_files", lambda pipe_result, files: [input_file])

    presets = [
        {
            "name": "evtx",
            "arguments": ["-f", "evtx"],
            "output_suffix": "evtx",
            "categories": [bundle.CATEGORY_APPLICATION_EXECUTION],
        },
        {
            "name": "missing",
            "arguments": ["-f", "missing"],
            "output_suffix": "missing",
            "categories": [bundle.CATEGORY_APPLICATION_EXECUTION],
        },
    ]
    monkeypatch.setattr(bundle, "TARGET_QUERY_BUNDLE", presets)

    outputs = {}

    def fake_create_output_file(output_path, display_name, extension, data_type):
        file_obj = _make_output_file(tmp_path, f"{display_name}.{extension}")
        outputs[display_name] = file_obj
        return file_obj

    monkeypatch.setattr(bundle, "create_output_file", fake_create_output_file)

    calls = []

    def fake_invoke(script, args, stdin_data=None, decode_stdout=True):
        calls.append((script, list(args)))
        assert not (script == "target-query" and args[-1] == "missing")  # missing preset must be skipped
        if script == "target-query":
            return 0, b"records", b""
        if script == "rdump":
            return 0, "csv", ""
        raise AssertionError("unexpected script")

    monkeypatch.setattr(bundle, "invoke_console_script", fake_invoke)

    class _Match:
        def __init__(self, name):
            self.name = name

    def fake_find_functions(function_name):
        if function_name == "missing":
            return [], None
        return ([_Match(function_name)], None)

    monkeypatch.setattr(bundle, "find_functions", fake_find_functions)
    bundle._plugin_available.cache_clear()

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
        workflow_id="wf-987",
        task_config={"bundle_scopes": [bundle.CATEGORY_EVERYTHING]},
    )

    assert result == "bundle-result"
    assert len(calls) == 2  # evtx preset -> target-query + rdump
    assert all("missing" not in arg for _, args in calls for arg in args)
    assert list(captured_result["meta"]["presets"]) == ["evtx"]
    assert captured_result["meta"]["skipped_presets"] == [{"preset": "missing", "plugin": "missing"}]
    assert captured_result["output_files"] == [outputs["disk-evtx"].to_dict()]
    assert {meta["plugin"] for meta in captured_result["meta"]["results"]} == {"evtx"}
    assert captured_result["meta"]["selection"] == [bundle.CATEGORY_EVERYTHING]
    assert (
        captured_result["meta"]["selection_label"]
        == [bundle.SCOPE_VALUE_TO_LABEL[bundle.CATEGORY_EVERYTHING]]
    )


def test_bundle_scope_filters_presets(monkeypatch, tmp_path):
    input_file = {"path": "/cases/disk.E01", "display_name": "disk.E01"}
    monkeypatch.setattr(bundle, "get_input_files", lambda pipe_result, files: [input_file])

    presets = [
        {
            "name": "evtx",
            "arguments": ["-f", "evtx"],
            "output_suffix": "evtx",
            "categories": [bundle.CATEGORY_APPLICATION_EXECUTION],
        },
        {
            "name": "mru.opensave",
            "arguments": ["-f", "mru.opensave"],
            "output_suffix": "mru_opensave",
            "categories": [bundle.CATEGORY_FILE_FOLDER_OPENING],
        },
    ]
    monkeypatch.setattr(bundle, "TARGET_QUERY_BUNDLE", presets)
    bundle._plugin_available.cache_clear()

    monkeypatch.setattr(bundle, "create_output_file", lambda *args, **kwargs: _make_output_file(tmp_path, "dummy.csv"))

    def fake_invoke(script, args, stdin_data=None, decode_stdout=True):
        if script == "target-query":
            return 0, b"records", b""
        if script == "rdump":
            return 0, "csv", ""
        raise AssertionError("unexpected script")

    monkeypatch.setattr(bundle, "invoke_console_script", fake_invoke)

    captured_result = {}

    def fake_create_task_result(**kwargs):
        captured_result.update(kwargs)
        return "bundle-result"

    monkeypatch.setattr(bundle, "create_task_result", fake_create_task_result)

    bundle._run_bundle(
        DummyTask(),
        pipe_result=None,
        input_files=None,
        output_path=str(tmp_path),
        workflow_id="wf-555",
        task_config={"bundle_scopes": [bundle.CATEGORY_FILE_FOLDER_OPENING]},
    )

    assert captured_result["meta"]["presets"] == ["mru.opensave"]
    assert captured_result["meta"]["selection"] == [bundle.CATEGORY_FILE_FOLDER_OPENING]
    assert (
        captured_result["meta"]["selection_label"]
        == [bundle.SCOPE_VALUE_TO_LABEL[bundle.CATEGORY_FILE_FOLDER_OPENING]]
    )


def test_bundle_scope_all_event_logs(monkeypatch, tmp_path):
    input_file = {"path": "/cases/disk.E01", "display_name": "disk.E01"}
    monkeypatch.setattr(bundle, "get_input_files", lambda pipe_result, files: [input_file])

    presets = [
        {
            "name": "All event logs",
            "arguments": ["-f", "evtx"],
            "output_suffix": "evtx",
            "categories": [bundle.CATEGORY_ALL_EVENT_LOGS],
        },
        {
            "name": "evtx (other)",
            "arguments": ["-f", "evtx"],
            "output_suffix": "evtx-other",
            "categories": [bundle.CATEGORY_APPLICATION_EXECUTION],
        },
    ]
    monkeypatch.setattr(bundle, "TARGET_QUERY_BUNDLE", presets)
    bundle._plugin_available.cache_clear()

    def fake_invoke(script, args, stdin_data=None, decode_stdout=True):
        if script == "target-query":
            return 0, b"records", b""
        if script == "rdump":
            return 0, "csv", ""
        raise AssertionError("unexpected script")

    monkeypatch.setattr(bundle, "invoke_console_script", fake_invoke)

    captured_result = {}

    def fake_create_task_result(**kwargs):
        captured_result.update(kwargs)
        return "bundle-result"

    monkeypatch.setattr(bundle, "create_task_result", fake_create_task_result)

    bundle._run_bundle(
        DummyTask(),
        pipe_result=None,
        input_files=None,
        output_path=str(tmp_path),
        workflow_id="wf-554",
        task_config={"bundle_scopes": [bundle.CATEGORY_ALL_EVENT_LOGS]},
    )

    assert captured_result["meta"]["presets"] == ["All event logs"]
    assert captured_result["meta"]["selection"] == [bundle.CATEGORY_ALL_EVENT_LOGS]
    assert (
        captured_result["meta"]["selection_label"]
        == [bundle.SCOPE_VALUE_TO_LABEL[bundle.CATEGORY_ALL_EVENT_LOGS]]
    )


def test_bundle_scope_mft_timeline(monkeypatch, tmp_path):
    input_file = {"path": "/cases/disk.E01", "display_name": "disk.E01"}
    monkeypatch.setattr(bundle, "get_input_files", lambda pipe_result, files: [input_file])

    presets = [
        {
            "name": "Generate a MFT Timeline",
            "arguments": ["-f", "mft.records"],
            "output_suffix": "mft_timeline",
            "categories": [bundle.CATEGORY_MFT_TIMELINE],
        },
        {
            "name": "All event logs",
            "arguments": ["-f", "evtx"],
            "output_suffix": "evtx",
            "categories": [bundle.CATEGORY_ALL_EVENT_LOGS],
        },
    ]
    monkeypatch.setattr(bundle, "TARGET_QUERY_BUNDLE", presets)
    bundle._plugin_available.cache_clear()

    def fake_invoke(script, args, stdin_data=None, decode_stdout=True):
        if script == "target-query":
            return 0, b"records", b""
        if script == "rdump":
            return 0, "csv", ""
        raise AssertionError("unexpected script")

    monkeypatch.setattr(bundle, "invoke_console_script", fake_invoke)

    captured_result = {}

    def fake_create_task_result(**kwargs):
        captured_result.update(kwargs)
        return "bundle-result"

    monkeypatch.setattr(bundle, "create_task_result", fake_create_task_result)

    bundle._run_bundle(
        DummyTask(),
        pipe_result=None,
        input_files=None,
        output_path=str(tmp_path),
        workflow_id="wf-554b",
        task_config={"bundle_scopes": [bundle.CATEGORY_MFT_TIMELINE]},
    )

    assert captured_result["meta"]["presets"] == ["Generate a MFT Timeline"]
    assert captured_result["meta"]["selection"] == [bundle.CATEGORY_MFT_TIMELINE]
    assert (
        captured_result["meta"]["selection_label"]
        == [bundle.SCOPE_VALUE_TO_LABEL[bundle.CATEGORY_MFT_TIMELINE]]
    )





def test_bundle_scope_deleted_items(monkeypatch, tmp_path):
    input_file = {"path": "/cases/disk.E01", "display_name": "disk.E01"}
    monkeypatch.setattr(bundle, "get_input_files", lambda pipe_result, files: [input_file])

    presets = [
        {
            "name": "evtx",
            "arguments": ["-f", "evtx"],
            "output_suffix": "evtx",
            "categories": [bundle.CATEGORY_APPLICATION_EXECUTION],
        },
        {
            "name": "Shortcut (LNK) Files",
            "arguments": ["-f", "lnk"],
            "output_suffix": "lnk",
            "categories": [bundle.CATEGORY_FILE_FOLDER_OPENING, bundle.CATEGORY_DELETED_ITEMS],
        },
    ]
    monkeypatch.setattr(bundle, "TARGET_QUERY_BUNDLE", presets)
    bundle._plugin_available.cache_clear()

    monkeypatch.setattr(bundle, "create_output_file", lambda *args, **kwargs: _make_output_file(tmp_path, "dummy.csv"))

    def fake_invoke(script, args, stdin_data=None, decode_stdout=True):
        if script == "target-query":
            return 0, b"records", b""
        if script == "rdump":
            return 0, "csv", ""
        raise AssertionError("unexpected script")

    monkeypatch.setattr(bundle, "invoke_console_script", fake_invoke)

    captured_result = {}

    def fake_create_task_result(**kwargs):
        captured_result.update(kwargs)
        return "bundle-result"

    monkeypatch.setattr(bundle, "create_task_result", fake_create_task_result)

    bundle._run_bundle(
        DummyTask(),
        pipe_result=None,
        input_files=None,
        output_path=str(tmp_path),
        workflow_id="wf-556",
        task_config={"bundle_scopes": [bundle.CATEGORY_DELETED_ITEMS]},
    )

    assert captured_result["meta"]["presets"] == ["Shortcut (LNK) Files"]
    assert captured_result["meta"]["selection"] == [bundle.CATEGORY_DELETED_ITEMS]
    assert (
        captured_result["meta"]["selection_label"]
        == [bundle.SCOPE_VALUE_TO_LABEL[bundle.CATEGORY_DELETED_ITEMS]]
    )


def test_bundle_scope_browser_activity(monkeypatch, tmp_path):
    input_file = {"path": "/cases/disk.E01", "display_name": "disk.E01"}
    monkeypatch.setattr(bundle, "get_input_files", lambda pipe_result, files: [input_file])

    presets = [
        {
            "name": "Browser History",
            "arguments": ["-f", "browser.history"],
            "output_suffix": "browser_history",
            "categories": [bundle.CATEGORY_BROWSER_ACTIVITY],
        },
        {
            "name": "evtx",
            "arguments": ["-f", "evtx"],
            "output_suffix": "evtx",
            "categories": [bundle.CATEGORY_APPLICATION_EXECUTION],
        },
    ]
    monkeypatch.setattr(bundle, "TARGET_QUERY_BUNDLE", presets)
    bundle._plugin_available.cache_clear()

    monkeypatch.setattr(bundle, "create_output_file", lambda *args, **kwargs: _make_output_file(tmp_path, "dummy.csv"))

    def fake_invoke(script, args, stdin_data=None, decode_stdout=True):
        if script == "target-query":
            return 0, b"records", b""
        if script == "rdump":
            return 0, "csv", ""
        raise AssertionError("unexpected script")

    monkeypatch.setattr(bundle, "invoke_console_script", fake_invoke)

    captured_result = {}

    def fake_create_task_result(**kwargs):
        captured_result.update(kwargs)
        return "bundle-result"

    monkeypatch.setattr(bundle, "create_task_result", fake_create_task_result)

    bundle._run_bundle(
        DummyTask(),
        pipe_result=None,
        input_files=None,
        output_path=str(tmp_path),
        workflow_id="wf-557",
        task_config={"bundle_scopes": [bundle.SCOPE_VALUE_TO_LABEL[bundle.CATEGORY_BROWSER_ACTIVITY]]},
    )

    assert captured_result["meta"]["presets"] == ["Browser History"]
    assert captured_result["meta"]["selection"] == [bundle.CATEGORY_BROWSER_ACTIVITY]
    assert (
        captured_result["meta"]["selection_label"]
        == [bundle.SCOPE_VALUE_TO_LABEL[bundle.CATEGORY_BROWSER_ACTIVITY]]
    )


def test_bundle_scope_external_device(monkeypatch, tmp_path):
    input_file = {"path": "/cases/disk.E01", "display_name": "disk.E01"}
    monkeypatch.setattr(bundle, "get_input_files", lambda pipe_result, files: [input_file])

    custom_rdump = ["-C", "--multi-timestamp", "-s", "expr"]

    presets = [
        {
            "name": "USB history (registry)",
            "arguments": ["-f", "usb"],
            "output_suffix": "usb",
            "categories": [bundle.CATEGORY_EXTERNAL_DEVICE],
        },
        {
            "name": "Removable device activity",
            "arguments": ["-f", "evtx"],
            "output_suffix": "evtx_removable",
            "categories": [bundle.CATEGORY_EXTERNAL_DEVICE],
            "rdump_args": custom_rdump,
        },
    ]
    monkeypatch.setattr(bundle, "TARGET_QUERY_BUNDLE", presets)
    bundle._plugin_available.cache_clear()

    monkeypatch.setattr(bundle, "create_output_file", lambda *args, **kwargs: _make_output_file(tmp_path, "dummy.csv"))

    calls = []

    def fake_invoke(script, args, stdin_data=None, decode_stdout=True):
        calls.append((script, list(args)))

        if script == "target-query":
            return 0, b"records", b""
        if script == "rdump":
            # Ensure rdump receives our customised argument list when handling the EVTX preset.
            if calls[-2][0] == "target-query" and calls[-2][1][-1] == "evtx":
                assert args == custom_rdump
            return 0, "csv", ""
        raise AssertionError("unexpected script")

    monkeypatch.setattr(bundle, "invoke_console_script", fake_invoke)

    captured_result = {}

    def fake_create_task_result(**kwargs):
        captured_result.update(kwargs)
        return "bundle-result"

    monkeypatch.setattr(bundle, "create_task_result", fake_create_task_result)

    bundle._run_bundle(
        DummyTask(),
        pipe_result=None,
        input_files=None,
        output_path=str(tmp_path),
        workflow_id="wf-558",
        task_config={"bundle_scopes": [bundle.CATEGORY_EXTERNAL_DEVICE]},
    )

    assert captured_result["meta"]["presets"] == ["USB history (registry)", "Removable device activity"]
    assert captured_result["meta"]["selection"] == [bundle.CATEGORY_EXTERNAL_DEVICE]
    assert (
        captured_result["meta"]["selection_label"]
        == [bundle.SCOPE_VALUE_TO_LABEL[bundle.CATEGORY_EXTERNAL_DEVICE]]
    )


def test_yara_rule_executes_when_provided(monkeypatch, tmp_path):
    input_file = {"path": "/cases/disk.E01", "display_name": "disk.E01"}
    monkeypatch.setattr(bundle, "get_input_files", lambda pipe_result, files: [input_file])

    presets = [
        {
            "name": "All event logs",
            "arguments": ["-f", "evtx"],
            "output_suffix": "evtx",
            "categories": [bundle.CATEGORY_ALL_EVENT_LOGS],
        },
    ]
    monkeypatch.setattr(bundle, "TARGET_QUERY_BUNDLE", presets)

    class _Match:
        def __init__(self, name):
            self.name = name

    def fake_find_functions(function_name):
        return ([_Match(function_name)], None)

    monkeypatch.setattr(bundle, "find_functions", fake_find_functions)
    bundle._plugin_available.cache_clear()

    outputs = {}

    def fake_create_output_file(output_path, display_name, extension, data_type):
        file_obj = _make_output_file(tmp_path, f"{display_name}.{extension}")
        outputs.setdefault(display_name, []).append(file_obj)
        return file_obj

    monkeypatch.setattr(bundle, "create_output_file", fake_create_output_file)

    calls = []

    def fake_invoke(script, args, stdin_data=None, decode_stdout=True):
        calls.append((script, list(args)))
        if script == "target-query" and "yara" in args:
            return 0, "yara-results", ""
        if script == "target-query":
            return 0, b"records", b""
        if script == "rdump":
            return 0, "csv", ""
        raise AssertionError("unexpected script")

    monkeypatch.setattr(bundle, "invoke_console_script", fake_invoke)

    captured_result = {}

    def fake_create_task_result(**kwargs):
        captured_result.update(kwargs)
        return "bundle-result"

    monkeypatch.setattr(bundle, "create_task_result", fake_create_task_result)

    bundle._run_bundle(
        DummyTask(),
        pipe_result=None,
        input_files=None,
        output_path=str(tmp_path),
        workflow_id="wf-559",
        task_config={
            "bundle_scopes": [bundle.CATEGORY_ALL_EVENT_LOGS],
            "yara_rule": "rule r { strings: $a = \"test\" condition: $a }",
        },
    )

    assert "All event logs" in captured_result["meta"]["presets"]
    assert "Yara (custom rule)" in captured_result["meta"]["presets"]
    assert captured_result["meta"]["selection"] == [bundle.CATEGORY_ALL_EVENT_LOGS]
    assert (
        captured_result["meta"]["selection_label"]
        == [bundle.SCOPE_VALUE_TO_LABEL[bundle.CATEGORY_ALL_EVENT_LOGS]]
    )
    assert any("yara" in name.lower() for name in outputs)
    assert any(
        call[0] == "rdump"
        and index > 0
        and calls[index - 1][0] == "target-query"
        and "yara" in calls[index - 1][1]
    for index, call in enumerate(calls)
    )


@pytest.mark.parametrize("scopes_value", ([], None))
def test_yara_only_runs_when_scope_selection_empty(monkeypatch, tmp_path, scopes_value):
    input_file = {"path": "/cases/disk.E01", "display_name": "disk.E01"}
    monkeypatch.setattr(bundle, "get_input_files", lambda pipe_result, files: [input_file])

    def fake_create_output_file(output_path, display_name, extension, data_type):
        return _make_output_file(tmp_path, f"{display_name}.{extension}")

    monkeypatch.setattr(bundle, "create_output_file", fake_create_output_file)

    target_query_calls = []

    def fake_invoke(script, args, stdin_data=None, decode_stdout=True):
        if script == "target-query":
            target_query_calls.append(list(args))
            plugin = args[args.index("-f") + 1]
            assert plugin == "yara", "Only the Yara preset should execute"
            return 0, b"yara-results", b""
        if script == "rdump":
            return 0, "csv-content", ""
        raise AssertionError("unexpected script")

    monkeypatch.setattr(bundle, "invoke_console_script", fake_invoke)

    captured_result = {}

    def fake_create_task_result(**kwargs):
        captured_result.update(kwargs)
        return "bundle-result"

    monkeypatch.setattr(bundle, "create_task_result", fake_create_task_result)

    bundle._run_bundle(
        DummyTask(),
        pipe_result=None,
        input_files=None,
        output_path=str(tmp_path),
        workflow_id="wf-560",
        task_config={
            "bundle_scopes": scopes_value,
            "yara_rule": "rule r { strings: $a = \\\"test\\\" condition: $a }",
        },
    )

    assert len(target_query_calls) == 1
    assert target_query_calls[0][0] == input_file["path"]
    assert captured_result["meta"]["presets"] == ["Yara (custom rule)"]
    assert captured_result["meta"]["selection"] == []
    assert captured_result["meta"]["selection_label"] == []


def test_yara_rule_paths_execute_when_provided(monkeypatch, tmp_path):
    input_file = {"path": "/cases/disk.E01", "display_name": "disk.E01"}
    monkeypatch.setattr(bundle, "get_input_files", lambda pipe_result, files: [input_file])

    monkeypatch.setattr(bundle, "TARGET_QUERY_BUNDLE", [])

    class _Match:
        def __init__(self, name):
            self.name = name

    def fake_find_functions(function_name):
        return ([_Match(function_name)], None)

    monkeypatch.setattr(bundle, "find_functions", fake_find_functions)
    bundle._plugin_available.cache_clear()

    def fake_create_output_file(output_path, display_name, extension, data_type):
        return _make_output_file(tmp_path, f"{display_name}.{extension}")

    monkeypatch.setattr(bundle, "create_output_file", fake_create_output_file)

    target_query_calls = []

    def fake_invoke(script, args, stdin_data=None, decode_stdout=True):
        if script == "target-query":
            target_query_calls.append(list(args))
            assert "-r" in args
            idx = args.index("-r")
            assert args[idx + 1 :] == ["/rules/all"]
            return 0, b"yara-results", b""
        if script == "rdump":
            return 0, "csv", ""
        raise AssertionError("unexpected script")

    monkeypatch.setattr(bundle, "invoke_console_script", fake_invoke)

    captured_result = {}

    def fake_create_task_result(**kwargs):
        captured_result.update(kwargs)
        return "bundle-result"

    monkeypatch.setattr(bundle, "create_task_result", fake_create_task_result)

    bundle._run_bundle(
        DummyTask(),
        pipe_result=None,
        input_files=None,
        output_path=str(tmp_path),
        workflow_id="wf-561",
        task_config={
            "bundle_scopes": [],
            "yara_rule_paths": "/rules/all",
        },
    )

    assert len(target_query_calls) == 1
    assert captured_result["meta"]["presets"] == ["Yara (custom rule)"]
    assert captured_result["meta"]["selection"] == []
    assert captured_result["meta"]["selection_label"] == []


def test_yara_rule_inline_and_paths(monkeypatch, tmp_path):
    input_file = {"path": "/cases/disk.E01", "display_name": "disk.E01"}
    monkeypatch.setattr(bundle, "get_input_files", lambda pipe_result, files: [input_file])

    presets = [
        {
            "name": "All event logs",
            "arguments": ["-f", "evtx"],
            "output_suffix": "evtx",
            "categories": [bundle.CATEGORY_ALL_EVENT_LOGS],
        },
    ]
    monkeypatch.setattr(bundle, "TARGET_QUERY_BUNDLE", presets)

    class _Match:
        def __init__(self, name):
            self.name = name

    def fake_find_functions(function_name):
        return ([_Match(function_name)], None)

    monkeypatch.setattr(bundle, "find_functions", fake_find_functions)
    bundle._plugin_available.cache_clear()

    outputs = {}

    def fake_create_output_file(output_path, display_name, extension, data_type):
        file_obj = _make_output_file(tmp_path, f"{display_name}.{extension}")
        outputs.setdefault(display_name, []).append(file_obj)
        return file_obj

    monkeypatch.setattr(bundle, "create_output_file", fake_create_output_file)

    target_query_calls = []

    def fake_invoke(script, args, stdin_data=None, decode_stdout=True):
        if script == "target-query" and "yara" in args:
            target_query_calls.append(list(args))
            idx = args.index("-r")
            yara_args = args[idx + 1 :]
            assert len(yara_args) == 2
            assert yara_args[1] == "/rules/dir"
            assert yara_args[0].endswith(".yar")
            return 0, "yara-results", ""
        if script == "target-query":
            target_query_calls.append(list(args))
            return 0, b"records", b""
        if script == "rdump":
            return 0, "csv", ""
        raise AssertionError("unexpected script")

    monkeypatch.setattr(bundle, "invoke_console_script", fake_invoke)

    captured_result = {}

    def fake_create_task_result(**kwargs):
        captured_result.update(kwargs)
        return "bundle-result"

    monkeypatch.setattr(bundle, "create_task_result", fake_create_task_result)

    bundle._run_bundle(
        DummyTask(),
        pipe_result=None,
        input_files=None,
        output_path=str(tmp_path),
        workflow_id="wf-562",
        task_config={
            "bundle_scopes": [bundle.CATEGORY_ALL_EVENT_LOGS],
            "yara_rule": "rule r { strings: $a = \"test\" condition: $a }",
            "yara_rule_paths": ["/rules/dir"],
        },
    )

    yara_calls = [call for call in target_query_calls if "yara" in call]
    assert len(yara_calls) == 1
    assert "All event logs" in captured_result["meta"]["presets"]
    assert "Yara (custom rule)" in captured_result["meta"]["presets"]
