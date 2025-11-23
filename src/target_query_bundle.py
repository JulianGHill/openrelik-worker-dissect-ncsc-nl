"""Celery task that runs a predefined set of Dissect target-query commands."""

from __future__ import annotations

import re
import tempfile
from functools import lru_cache
from pathlib import Path
from typing import Iterable

from celery import signals
from celery.utils.log import get_task_logger
from dissect.target.plugin import find_functions

from openrelik_worker_common.file_utils import create_output_file
from openrelik_worker_common.logging import Logger
from openrelik_worker_common.task_utils import create_task_result, get_input_files

from .app import celery
from .tasks import (
    _export_records_with_writer,
    _normalize_writer_uri,
    _resolve_default_writer,
    invoke_console_script,
    materialize_target_path,
    quote_command,
    send_progress,
)

TASK_NAME = "openrelik-worker-dissect-ncsc-nl.tasks.run_target_query_bundle"

CATEGORY_EVERYTHING = "everything"
CATEGORY_ALL_EVENT_LOGS = "all_event_logs"
CATEGORY_MFT_TIMELINE = "mft_timeline"
CATEGORY_APPLICATION_EXECUTION = "application_execution"
CATEGORY_FILE_FOLDER_OPENING = "file_folder_opening"
CATEGORY_DELETED_ITEMS = "deleted_items_file_existence"
CATEGORY_BROWSER_ACTIVITY = "browser_activity"
CATEGORY_EXTERNAL_DEVICE = "external_device_usage"
CATEGORY_USER_INFORMATION = "user_information"

SCOPE_ORDER = [
    CATEGORY_EVERYTHING,
    CATEGORY_ALL_EVENT_LOGS,
    CATEGORY_MFT_TIMELINE,
    CATEGORY_APPLICATION_EXECUTION,
    CATEGORY_FILE_FOLDER_OPENING,
    CATEGORY_DELETED_ITEMS,
    CATEGORY_BROWSER_ACTIVITY,
    CATEGORY_EXTERNAL_DEVICE,
    CATEGORY_USER_INFORMATION,
]

SCOPE_VALUE_TO_LABEL = {
    CATEGORY_EVERYTHING: "Everything",
    CATEGORY_ALL_EVENT_LOGS: "All event logs",
    CATEGORY_MFT_TIMELINE: "MFT timeline",
    CATEGORY_APPLICATION_EXECUTION: "Application execution",
    CATEGORY_FILE_FOLDER_OPENING: "File & folder opening",
    CATEGORY_DELETED_ITEMS: "Deleted items & file existence",
    CATEGORY_BROWSER_ACTIVITY: "Browser activity",
    CATEGORY_EXTERNAL_DEVICE: "External device & USB usage",
    CATEGORY_USER_INFORMATION: "User information",
}

SCOPE_LABEL_TO_VALUE = {label: value for value, label in SCOPE_VALUE_TO_LABEL.items()}

TASK_METADATA = {
    "display_name": "Dissect target-query bundle",
    "description": "Run a curated list of target-query functions and convert results to CSV using rdump.",
    "task_config": [
        {
            "name": "bundle_scopes",
            "label": "Preset selections",
            "description": (
                "Choose one or more preset groups to run. 'Everything' selects all available presets."
            ),
            "type": "autocomplete",
            "items": [SCOPE_VALUE_TO_LABEL[value] for value in SCOPE_ORDER],
            "default": [SCOPE_VALUE_TO_LABEL[CATEGORY_EVERYTHING]],
            "required": False,
        },
        {
            "name": "yara_rule",
            "label": "Custom Yara rule",
            "description": "Optional Yara rule contents applied to each input. Leave empty to skip running Yara.",
            "type": "textarea",
            "required": False,
        },
        {
            "name": "yara_rule_paths",
            "label": "Yara rule directories/files",
            "description": (
                "Optional path(s) to existing Yara rule files or directories. Provide multiple entries "
                "comma or newline separated."
            ),
            "type": "textarea",
            "required": False,
        },
        {
            "name": "elastic_writer",
            "label": "Record writer URI",
            "description": (
                "Optional Dissect writer URI (e.g. elastic+http://elastic:9200?index=dissect-records). "
                "When set, the worker streams record-formatted output for each preset to that writer."
            ),
            "type": "text",
            "required": False,
        },
        {
            "name": "case_name",
            "label": "Case name",
            "description": "Optional case identifier included in exported records (Elastic only).",
            "type": "text",
            "required": False,
        },
        {
            "name": "enable_record_writer",
            "label": "Export to record writer",
            "description": (
                "Toggle streaming record-formatted output to the configured writer URI (either supplied above "
                "or via the DISSECT_ELASTIC_WRITER_URI environment variable)."
            ),
            "type": "checkbox",
            "default": False,
            "required": False,
        },
    ],
}

log_root = Logger()
logger = log_root.get_logger(__name__, get_task_logger(__name__))

RDUMP_ARGS = ["-C", "--multi-timestamp"]

TARGET_QUERY_BUNDLE = [
    {
        "name": "Local users (target-query)",
        "arguments": ["-f", "users"],
        "output_suffix": "users",
        "categories": [CATEGORY_USER_INFORMATION],
    },
    {
        "name": "SAM user names (target-reg)",
        "command": "target-reg",
        "arguments": [
            "-k",
            r"HKEY_LOCAL_MACHINE\SAM\SAM\Domains\Account\Users\Names",
            "-d",
            "2",
        ],
        "output_suffix": "sam_user_names",
        "rdump_args": None,
        "data_type": "openrelik:dissect:target-reg:text",
        "decode_stdout": True,
        "categories": [CATEGORY_USER_INFORMATION],
    },
    {
        "name": "All event logs",
        "arguments": ["-f", "evtx"],
        "output_suffix": "evtx",
        "categories": [CATEGORY_ALL_EVENT_LOGS],
    },
    {
        "name": "Generate a MFT Timeline",
        "arguments": ["-f", "mft.records"],
        "output_suffix": "mft_timeline",
        "categories": [CATEGORY_MFT_TIMELINE],
    },
    {
        "name": "Shimcache",
        "arguments": ["-f", "shimcache"],
        "output_suffix": "shimcache",
        "categories": [CATEGORY_APPLICATION_EXECUTION],
    },
    {
        "name": "Task Bar Feature Usage",
        "arguments": ["-f", "featureusage"],
        "output_suffix": "featureusage",
        "categories": [CATEGORY_APPLICATION_EXECUTION],
    },
    {
        "name": "Amcache.hve",
        "arguments": ["-f", "amcache"],
        "output_suffix": "amcache",
        "categories": [CATEGORY_APPLICATION_EXECUTION],
    },
    {
        "name": "Jump Lists",
        "arguments": ["-f", "jumplist"],
        "output_suffix": "jumplist",
        "categories": [CATEGORY_APPLICATION_EXECUTION],
    },
    {
        "name": "Open/Save MRU",
        "arguments": ["-f", "mru.opensave"],
        "output_suffix": "mru_opensave",
        "categories": [CATEGORY_FILE_FOLDER_OPENING],
    },
    {
        "name": "Recent Files (MRU)",
        "arguments": ["-f", "mru.recentdocs"],
        "output_suffix": "mru_recentdocs",
        "categories": [CATEGORY_FILE_FOLDER_OPENING],
    },
    {
        "name": "Shortcut (LNK) Files",
        "arguments": ["-f", "lnk"],
        "output_suffix": "lnk",
        "categories": [CATEGORY_FILE_FOLDER_OPENING, CATEGORY_DELETED_ITEMS, CATEGORY_EXTERNAL_DEVICE],
    },
    {
        "name": "Shell Bags",
        "arguments": ["-f", "shellbags"],
        "output_suffix": "shellbags",
        "categories": [CATEGORY_FILE_FOLDER_OPENING, CATEGORY_DELETED_ITEMS],
    },
    {
        "name": "Office Recent Files",
        "arguments": ["-f", "mru.msoffice"],
        "output_suffix": "mru_msoffice",
        "categories": [CATEGORY_FILE_FOLDER_OPENING],
    },
    {
        "name": "Office Trust Records",
        "arguments": ["-f", "trusteddocs"],
        "output_suffix": "trusteddocs",
        "categories": [CATEGORY_FILE_FOLDER_OPENING],
    },
    {
        "name": "Last Visited MRU",
        "arguments": ["-f", "mru"],
        "output_suffix": "mru",
        "categories": [CATEGORY_APPLICATION_EXECUTION],
    },
    {
        "name": "RunMRU",
        "arguments": ["-f", "runkeys"],
        "output_suffix": "runkeys",
        "categories": [CATEGORY_APPLICATION_EXECUTION],
    },
    {
        "name": "Windows 10 Timeline (ActivitiesCache.db)",
        "arguments": ["-f", "activitiescache"],
        "output_suffix": "activitiescache",
        "categories": [CATEGORY_APPLICATION_EXECUTION],
    },
    {
        "name": "BAM/DAM",
        "arguments": ["-f", "bam"],
        "output_suffix": "bam",
        "categories": [CATEGORY_APPLICATION_EXECUTION],
    },
    {
        "name": "SRUM (System Resource Usage Monitor)",
        "arguments": ["-f", "sru"],
        "output_suffix": "sru",
        "categories": [CATEGORY_APPLICATION_EXECUTION],
    },
    {
        "name": "Prefetch",
        "arguments": ["-f", "prefetch"],
        "output_suffix": "prefetch",
        "categories": [CATEGORY_APPLICATION_EXECUTION],
    },
    {
        "name": "CapabilityAccessManager",
        "arguments": ["-f", "cam"],
        "output_suffix": "cam",
        "categories": [CATEGORY_APPLICATION_EXECUTION],
    },
    {
        "name": "UserAssist",
        "arguments": ["-f", "userassist"],
        "output_suffix": "userassist",
        "categories": [CATEGORY_APPLICATION_EXECUTION],
    },
    {
        "name": "Installed Services",
        "arguments": ["-f", "services"],
        "output_suffix": "services",
        "categories": [CATEGORY_APPLICATION_EXECUTION],
    },
    {
        "name": "Recycle Bin",
        "arguments": ["-f", "recyclebin"],
        "output_suffix": "recyclebin",
        "categories": [CATEGORY_DELETED_ITEMS],
    },
    {
        "name": "Thumbcache",
        "arguments": ["-f", "thumbcache"],
        "output_suffix": "thumbcache",
        "categories": [CATEGORY_DELETED_ITEMS],
    },
    {
        "name": "Internet Explorer file:/// History",
        "arguments": ["-f", "iexplore.history"],
        "output_suffix": "iexplore_history",
        "categories": [CATEGORY_DELETED_ITEMS],
    },
    {
        "name": "Search - WordWheelQuery",
        "arguments": ["-f", "mru.acmru"],
        "output_suffix": "mru_acmru",
        "categories": [CATEGORY_DELETED_ITEMS],
    },
    {
        "name": "USB history (registry)",
        "arguments": ["-f", "usb"],
        "output_suffix": "usb",
        "categories": [CATEGORY_EXTERNAL_DEVICE],
    },
    {
        "name": "Removable device activity",
        "arguments": ["-f", "evtx"],
        "output_suffix": "evtx_removable_devices",
        "categories": [CATEGORY_EXTERNAL_DEVICE],
        "rdump_args": [
            "-C",
            "--multi-timestamp",
            "-s",
            '(r.EventID in [4663,4656,6416] and r.Channel == "Security") '
            'or (r.EventID in [20001,20003] and r.Channel == "System") '
            'or (r.EventID in [1006])',
        ],
    },
    {
        "name": "Browser (all below)",
        "arguments": ["-f", "browser"],
        "output_suffix": "browser",
        "categories": [CATEGORY_BROWSER_ACTIVITY],
    },
    {
        "name": "Browser Cookies",
        "arguments": ["-f", "browser.cookies"],
        "output_suffix": "browser_cookies",
        "categories": [CATEGORY_BROWSER_ACTIVITY],
    },
    {
        "name": "Browser Downloads",
        "arguments": ["-f", "browser.downloads"],
        "output_suffix": "browser_downloads",
        "categories": [CATEGORY_BROWSER_ACTIVITY],
    },
    {
        "name": "Browser Extensions",
        "arguments": ["-f", "browser.extensions"],
        "output_suffix": "browser_extensions",
        "categories": [CATEGORY_BROWSER_ACTIVITY],
    },
    {
        "name": "Browser History",
        "arguments": ["-f", "browser.history"],
        "output_suffix": "browser_history",
        "categories": [CATEGORY_BROWSER_ACTIVITY],
    },
    {
        "name": "Browser Passwords",
        "arguments": ["-f", "browser.passwords"],
        "output_suffix": "browser_passwords",
        "categories": [CATEGORY_BROWSER_ACTIVITY],
    },
]


def _extract_plugin_from_arguments(arguments: Iterable[str]) -> str | None:
    """Return the plugin/function name provided to ``target-query``."""

    arguments = list(arguments)
    try:
        index = arguments.index("-f")
    except ValueError:
        return None

    if index + 1 >= len(arguments):  # Defensive guard for malformed presets.
        return None

    return arguments[index + 1]


@lru_cache(maxsize=None)
def _plugin_available(function_name: str | None) -> bool:
    """Check whether a ``target-query`` function is available locally."""

    if not function_name:
        return True

    matches, _ = find_functions(function_name)
    return any(match.name == function_name for match in matches)


def _normalise_scope(scope: str | None) -> str:
    if scope is None:
        return CATEGORY_EVERYTHING

    raw = str(scope).strip()
    if not raw:
        return CATEGORY_EVERYTHING

    if raw in SCOPE_LABEL_TO_VALUE:
        return SCOPE_LABEL_TO_VALUE[raw]

    lower = raw.lower()

    if lower in {CATEGORY_EVERYTHING, "all", "everything"}:
        return CATEGORY_EVERYTHING
    if lower in {CATEGORY_ALL_EVENT_LOGS, "all-event-logs", "all_event_logs", "evtx"}:
        return CATEGORY_ALL_EVENT_LOGS
    if lower in {CATEGORY_MFT_TIMELINE, "mft", "mft-timeline", "mft_timeline"}:
        return CATEGORY_MFT_TIMELINE
    if lower in {CATEGORY_APPLICATION_EXECUTION, "application", "application-execution"}:
        return CATEGORY_APPLICATION_EXECUTION
    if lower in {CATEGORY_FILE_FOLDER_OPENING, "file-folder", "file_folder_opening", "file"}:
        return CATEGORY_FILE_FOLDER_OPENING
    if lower in {CATEGORY_DELETED_ITEMS, "deleted", "deleted-items", "deleted_items", "file-existence"}:
        return CATEGORY_DELETED_ITEMS
    if lower in {CATEGORY_BROWSER_ACTIVITY, "browser", "browser-activity", "browser_activity"}:
        return CATEGORY_BROWSER_ACTIVITY
    if lower in {CATEGORY_EXTERNAL_DEVICE, "external", "external-device", "external_device", "usb", "usb-usage", "usb_usage"}:
        return CATEGORY_EXTERNAL_DEVICE

    for value, label in SCOPE_VALUE_TO_LABEL.items():
        if lower == label.lower():
            return value

    return CATEGORY_EVERYTHING


def _normalise_scopes(raw_scopes, *, default_to_everything: bool = True) -> list[str]:
    if raw_scopes is None:
        return [CATEGORY_EVERYTHING] if default_to_everything else []

    if isinstance(raw_scopes, (list, tuple, set)):
        if len(raw_scopes) == 0:
            return []
        raw_items = list(raw_scopes)
    else:
        raw_items = [raw_scopes]

    normalised: list[str] = []
    for item in raw_items:
        if item is None:
            continue
        if isinstance(item, str):
            parts = [part.strip() for part in item.split(",") if part.strip()] if "," in item else [item.strip()]
        else:
            parts = [str(item).strip()]

        for part in parts:
            if not part:
                continue
            scope_value = _normalise_scope(part)
            if scope_value not in normalised:
                normalised.append(scope_value)

    if not normalised:
        return []

    if CATEGORY_EVERYTHING in normalised:
        return [CATEGORY_EVERYTHING]

    return normalised


def _normalise_yara_rule_paths(raw_paths) -> list[str]:
    if raw_paths is None:
        return []

    if isinstance(raw_paths, (list, tuple, set)):
        raw_items = list(raw_paths)
    else:
        raw_items = [raw_paths]

    normalised: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        if item is None:
            continue
        text = str(item).strip()
        if not text:
            continue
        segments = re.split(r"[\n,]", text)
        for segment in segments:
            cleaned = segment.strip()
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            normalised.append(cleaned)

    return normalised


def _resolve_presets_for_scope(scope: str) -> list[dict]:
    if scope == CATEGORY_EVERYTHING:
        return list(TARGET_QUERY_BUNDLE)

    desired_categories = {scope}

    def _preset_categories(preset: dict) -> list[str]:
        categories = preset.get("categories")
        if categories is None:
            category = preset.get("category")
            if category is not None:
                categories = [category]
        if categories is None:
            return []
        if isinstance(categories, str):
            return [categories]
        return list(categories)

    return [
        preset
        for preset in TARGET_QUERY_BUNDLE
        if any(category in desired_categories for category in _preset_categories(preset))
    ]


def _classify_presets(presets: Iterable[dict]) -> tuple[list[tuple[dict, str | None]], list[tuple[dict, str | None]]]:
    """Split presets into available and unavailable buckets based on plugin presence."""

    available: list[tuple[dict, str | None]] = []
    unavailable: list[tuple[dict, str | None]] = []

    for preset in presets:
        plugin_name = _extract_plugin_from_arguments(preset.get("arguments", []))
        bucket = available if _plugin_available(plugin_name) else unavailable
        bucket.append((preset, plugin_name))

    return available, unavailable


def _bundle_command_display(source_path: str, arguments: Iterable[str], command: str = "target-query") -> str:
    base = [command, source_path, *arguments]
    return quote_command(base)


def _run_bundle(
    task,
    *,
    pipe_result: str | None,
    input_files: list | None,
    output_path: str | None,
    workflow_id: str | None,
    task_config: dict | None,
) -> str:
    if not output_path:
        raise RuntimeError("Output path is required for Dissect results")

    log_root.bind(workflow_id=workflow_id)
    logger.info("Starting Dissect target-query bundle", extra={"workflow_id": workflow_id})

    resolved_inputs = get_input_files(pipe_result, input_files or [])
    if not resolved_inputs:
        raise RuntimeError("No input files available for Dissect")

    config = task_config or {}

    if "bundle_scopes" in config:
        raw_scopes = config.get("bundle_scopes")
        scopes = _normalise_scopes(raw_scopes, default_to_everything=False)
    elif "bundle_scope" in config:
        raw_scopes = config.get("bundle_scope")
        scopes = _normalise_scopes(raw_scopes)
    else:
        scopes = _normalise_scopes(None)

    if not scopes and "bundle_scopes" not in config and "bundle_scope" not in config:
        scopes = [CATEGORY_EVERYTHING]
    yara_rule = (config.get("yara_rule") or "").strip()
    yara_rule_paths = _normalise_yara_rule_paths(config.get("yara_rule_paths"))

    if CATEGORY_EVERYTHING in scopes:
        selected_presets = list(TARGET_QUERY_BUNDLE)
    else:
        selected_presets = []
        seen_ids: set[int] = set()
        for scope in scopes:
            for preset in _resolve_presets_for_scope(scope):
                key = id(preset)
                if key not in seen_ids:
                    seen_ids.add(key)
                    selected_presets.append(preset)

    if not selected_presets and not (yara_rule or yara_rule_paths):
        raise RuntimeError("No target-query presets match the selected scope")

    available_presets, unavailable_presets = _classify_presets(selected_presets)

    if not available_presets and not (yara_rule or yara_rule_paths):
        raise RuntimeError("No target-query presets are available on this worker")

    writer_uri_from_config = _normalize_writer_uri(
        config.get("elastic_writer") or config.get("record_writer")
    )
    writer_uri = writer_uri_from_config or _resolve_default_writer()
    writer_toggle = config.get("enable_record_writer")
    if writer_toggle is None:
        writer_enabled = writer_uri_from_config is not None
    else:
        writer_enabled = bool(writer_toggle)
    case_name = (config.get("case_name") or "").strip() or None

    if writer_enabled and not writer_uri:
        raise RuntimeError(
            "Record writer export was requested but no writer URI is configured."
        )

    for preset, plugin_name in unavailable_presets:
        logger.warning(
            "Skipping target-query preset because plugin is unavailable",
            extra={
                "preset": preset.get("name"),
                "plugin": plugin_name,
            },
        )

    output_files = []
    meta_entries = []
    skipped_meta = [
        {"preset": preset.get("name"), "plugin": plugin_name}
        for preset, plugin_name in unavailable_presets
    ]

    yara_rule_path: str | None = None
    yara_plugin_available = False
    yara_arguments: list[str] = []

    if yara_rule:
        with tempfile.NamedTemporaryFile("w", suffix=".yar", delete=False) as rule_handle:
            rule_handle.write(yara_rule)
            rule_handle.flush()
            yara_rule_path = rule_handle.name
        yara_arguments.append(yara_rule_path)

    if yara_rule_paths:
        yara_arguments.extend(yara_rule_paths)

    if yara_arguments:
        if _plugin_available("yara"):
            yara_plugin_available = True
            available_presets.append(
                (
                    {
                        "name": "Yara (custom rule)",
                        "arguments": ["-f", "yara", "-r", *yara_arguments],
                        "output_suffix": "yara",
                        "rdump_args": RDUMP_ARGS,
                        "categories": [],
                    },
                    "yara",
                )
            )
        else:
            logger.warning("Skipping custom Yara execution because plugin is unavailable")
            skipped_meta.append({"preset": "Yara (custom rule)", "plugin": "yara"})

    for entry in resolved_inputs:
        source_path = entry.get("path")
        if not source_path:
            logger.warning("Skipping entry without a path", extra={"entry": entry})
            continue

        display_name = entry.get("display_name") or Path(source_path).name
        base_name = Path(display_name).stem

        with materialize_target_path(source_path, log=logger) as prepared_source_path:
            for preset, plugin_name in available_presets:
                command_name = preset.get("command", "target-query")
                preset_args = preset.get("arguments", [])
                command_args = [prepared_source_path, *preset_args]
                command_display = _bundle_command_display(
                    prepared_source_path, preset_args, command=command_name
                )
                logger.info(
                    "Running target bundle command", extra={"command": command_display}
                )

                decode_stdout = preset.get(
                    "decode_stdout", command_name != "target-query"
                )
                exit_code, query_stdout, query_stderr = invoke_console_script(
                    command_name, command_args, decode_stdout=decode_stdout
                )

                send_progress(task)

                if exit_code != 0:
                    logger.error(
                        "bundle preset failed",
                        extra={
                            "command": command_display,
                            "exit_code": exit_code,
                            "stderr": query_stderr,
                        },
                    )
                    stderr_text = (
                        query_stderr.decode("utf-8", errors="replace")
                        if isinstance(query_stderr, bytes)
                        else query_stderr
                    )
                    raise RuntimeError(
                        stderr_text.strip()
                        or f"bundle preset '{preset['name']}' failed for {display_name}"
                    )

                record_bytes: bytes | None = None
                if writer_enabled and writer_uri and command_name == "target-query":
                    if isinstance(query_stdout, (bytes, bytearray)):
                        record_bytes = query_stdout
                    else:
                        record_bytes = str(query_stdout).encode("utf-8")

                default_rdump = RDUMP_ARGS if command_name == "target-query" else None
                rdump_args = preset.get("rdump_args", default_rdump)
                if rdump_args is None:
                    rdump_stdout = (
                        query_stdout.decode("utf-8", errors="replace")
                        if isinstance(query_stdout, (bytes, bytearray))
                        else str(query_stdout)
                    )
                    rdump_stderr = ""
                    rdump_command_display = ""
                    output_extension = preset.get("output_extension", "txt")
                    data_type = preset.get(
                        "data_type", "openrelik:dissect:target-query:text"
                    )
                else:
                    rdump_exit, rdump_stdout, rdump_stderr = invoke_console_script(
                        "rdump",
                        rdump_args,
                        stdin_data=query_stdout,
                    )

                    if rdump_exit != 0:
                        logger.error(
                            "rdump failed",
                            extra={
                                "command": quote_command(["rdump", *rdump_args]),
                                "stderr": rdump_stderr,
                            },
                        )
                        raise RuntimeError(
                            rdump_stderr.strip()
                            or f"rdump failed for preset '{preset['name']}'"
                        )

                    rdump_command_display = quote_command(["rdump", *rdump_args])
                    output_extension = preset.get("output_extension", "csv")
                    data_type = preset.get(
                        "data_type", "openrelik:dissect:target-query:csv"
                    )

                output_file = create_output_file(
                    output_path,
                    display_name=f"{base_name}-{preset['output_suffix']}",
                    extension=output_extension,
                    data_type=data_type,
                )

                with open(output_file.path, "w", encoding="utf-8") as handle:
                    handle.write(rdump_stdout)

                writer_used = False
                if writer_enabled and writer_uri and record_bytes is not None:
                    _export_records_with_writer(
                        record_bytes,
                        writer_uri,
                        query_name=command_name,
                        display_name=display_name,
                        case_name=case_name,
                    )
                    writer_used = True

                output_files.append(output_file.to_dict())
                entry_meta = {
                    "input": display_name,
                    "preset": preset["name"],
                    "plugin": plugin_name,
                    "target_query_command": command_display,
                    "rdump_command": rdump_command_display,
                    "target_query_stderr": (
                        query_stderr.decode("utf-8", errors="replace")
                        if isinstance(query_stderr, bytes)
                        else query_stderr
                    ).strip(),
                    "rdump_stderr": rdump_stderr.strip(),
                }
                if writer_used:
                    entry_meta["record_writer"] = writer_uri
                meta_entries.append(entry_meta)

    if yara_rule_path is not None:
        Path(yara_rule_path).unlink(missing_ok=True)

    if not output_files:
        raise RuntimeError("No target-query bundle outputs were generated")

    executed_presets: list[str] = []
    seen_names: set[str] = set()
    for preset, _ in available_presets:
        name = preset.get("name")
        if name and name not in seen_names:
            seen_names.add(name)
            executed_presets.append(name)

    if yara_plugin_available and "Yara (custom rule)" not in seen_names and yara_rule:
        executed_presets.append("Yara (custom rule)")

    selection_labels = [SCOPE_VALUE_TO_LABEL.get(scope, scope) for scope in scopes]

    return create_task_result(
        output_files=output_files,
        workflow_id=workflow_id,
        command=f"target-query presets: {', '.join(executed_presets)}",
        meta={
            "presets": executed_presets,
            "results": meta_entries,
            "skipped_presets": skipped_meta,
            "selection": scopes,
            "selection_label": selection_labels,
        },
    )


@celery.task(bind=True, name=TASK_NAME, metadata=TASK_METADATA)
def run_target_query_bundle(
    self,
    pipe_result: str | None = None,
    input_files: list | None = None,
    output_path: str | None = None,
    workflow_id: str | None = None,
    task_config: dict | None = None,
) -> str:
    """Run the predefined target-query bundle against every provided input file."""

    return _run_bundle(
        self,
        pipe_result=pipe_result,
        input_files=input_files,
        output_path=output_path,
        workflow_id=workflow_id,
        task_config=task_config,
    )


@signals.task_prerun.connect
def on_task_prerun(sender, task_id, task, args, kwargs, **_):
    if sender.name != TASK_NAME:
        return

    log_root.bind(
        task_id=task_id,
        task_name=task.name,
        worker_name=TASK_METADATA.get("display_name"),
    )
